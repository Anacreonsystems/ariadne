"""Per-input streaming execution inside a native Prefect TaskRun."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from contextlib import aclosing
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable
from uuid import UUID, uuid4

from prefect._flow_run_suspension import raise_if_flow_run_suspension_requested
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.states import Failed
from prefect.streams import (
    SourceContext,
    SourceRecord,
    SourceRunResult,
    Stream,
    StreamContext,
    StreamProtocolError,
    StreamRunResult,
    StreamSourceConfig,
    _PartitionMetadata,  # pyright: ignore[reportPrivateUsage]
)
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.utilities.callables import call_with_parameters

if TYPE_CHECKING:
    from prefect.task_engine import AsyncTaskRunEngine, SyncTaskRunEngine


@runtime_checkable
class NativeStreamStorage(Protocol):
    """Internal lifecycle authority for the platform HTTP backend."""

    @property
    def flow_run_id(self) -> UUID: ...

    async def register_task_run(self, task: Any, task_run: Any) -> Any: ...

    async def reserve_output(
        self, stream_id: str, task_run_id: UUID, *, source: bool = False
    ) -> None: ...

    async def reject_output(
        self, stream_id: str, task_run_id: UUID, error: str, *, source: bool = False
    ) -> None: ...

    async def finish_consumer(self, binding: Any, error: str | None = None) -> None: ...

    async def fail_task_output(
        self,
        stream_id: str,
        task_run_id: UUID,
        attempt: int,
        error: str,
        *,
        source: bool = False,
    ) -> None: ...


@runtime_checkable
class NativeSourceStorage(NativeStreamStorage, Protocol):
    """Positioned source publication for the platform HTTP backend."""

    async def open_source(
        self, stream_id: str, task_run_id: UUID, owner_id: UUID, attempt: int
    ) -> Any: ...

    async def append_source(self, binding: Any, position: int, value: Any) -> None: ...

    async def close_source(self, binding: Any, expected_position: int) -> None: ...


class _StreamRun:
    """Opaque submission metadata; dependency traversal must not inspect it."""

    def __init__(
        self, output: Stream[Any], task_run: Any = None, *, source: bool = False
    ) -> None:
        self.output = output
        self.task_run = task_run
        self.registration_verified = False
        self.source = source
        self.partition = _PartitionMetadata()


def _fail_unstarted_run(run: _StreamRun, error: str) -> None:
    """Compensate a confirmed Pending registration through normal task events."""
    # utilities.engine imports Task; defer until Task initialization is complete.
    from prefect._internal.stream_execution import get_stream_subtask_execution
    from prefect.utilities.engine import emit_task_run_state_change_event

    task_run = run.task_run
    if (
        not run.registration_verified
        or task_run is None
        or task_run.run_count != 0
        or task_run.state is None
        or not task_run.state.is_pending()
    ):
        return
    initial = task_run.state
    failed = Failed(message=f"Stream startup failed: {error}")
    failed.state_details.task_run_id = task_run.id
    failed.state_details.flow_run_id = task_run.flow_run_id
    physical = get_stream_subtask_execution()
    if physical is not None:
        failed.state_details.retriable = False
    task_run.state = failed
    task_run.state_id = failed.id
    task_run.state_type = failed.type
    task_run.state_name = failed.name
    if physical is None:
        emit_task_run_state_change_event(
            task_run=task_run, initial_state=initial, validated_state=failed
        )
    else:
        from prefect._internal.stream_subtask_entrypoint import emit_physical_task_event
        from prefect.client.orchestration import get_client
        from prefect.utilities.engine import build_task_run_state_change_event

        event = build_task_run_state_change_event(task_run, initial, failed)
        with get_client(sync_client=True) as client:
            emit_physical_task_event(client, event)


async def reject_stream_submission(run: _StreamRun, error: str) -> None:
    storage = run.output.storage
    if isinstance(storage, NativeStreamStorage):
        _fail_unstarted_run(run, error)
    status = await storage.status(run.output.stream_id)
    if status.state != "OPEN":
        return
    if isinstance(storage, NativeStreamStorage):
        if run.task_run is None:
            raise StreamProtocolError("Native output has no reserved TaskRun")
        try:
            await storage.reject_output(
                run.output.stream_id, run.task_run.id, error, source=run.source
            )
        except Exception as refused:
            if (
                getattr(refused, "code", None) != "STALE_OWNER"
                or run.task_run.run_count < 1
            ):
                raise
            await storage.fail_task_output(
                run.output.stream_id,
                run.task_run.id,
                run.task_run.run_count,
                error,
                source=run.source,
            )
    else:
        await storage.finish(run.output.stream_id, error)


async def _finish_output(storage: Any, binding: Any, stream_id: str) -> None:
    if isinstance(storage, NativeStreamStorage):
        await storage.finish_consumer(binding)
    else:
        await storage.finish(stream_id)


def validate_stream_parameters(task: Any, parameters: dict[str, Any]) -> _StreamRun:
    config = task.stream
    run = parameters.get("__prefect_stream_run__")
    if not isinstance(run, _StreamRun) or (
        not isinstance(config, StreamSourceConfig)
        and not isinstance(parameters.get(config.input), Stream)
    ):
        raise StreamProtocolError("Stream tasks require native stream submission")
    return run


def _state_copy(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise StreamProtocolError("Managed state must be a JSON dictionary")
    try:
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise StreamProtocolError("Managed state must be a JSON dictionary") from exc


def _parameters(engine: Any, record: Any, consumer_id: str) -> dict[str, Any]:
    config = engine.task.stream
    parameters = {
        key: value
        for key, value in engine.parameters.items()
        if key != "__prefect_stream_run__"
    }
    parameters[config.input] = record.value
    parameters[config.context] = StreamContext(
        task_run_id=engine.task_run.id,
        consumer_id=consumer_id,
        input_sequence=record.sequence,
        attempt=engine.task_run.run_count,
        managed_state=_state_copy(record.managed_state),
        _partition=validate_stream_parameters(engine.task, engine.parameters).partition,
    )
    return parameters


def _check_execution(engine: Any) -> None:
    raise_if_flow_run_suspension_requested()
    if engine.is_cancelled():
        raise asyncio.CancelledError("Stream task cancelled by its task runner")


async def _input_drained(upstream: Stream[Any]) -> bool:
    status = await upstream.storage.status(upstream.stream_id)
    if status.state == "FAILED":
        raise StreamProtocolError(f"Upstream stream failed: {status.error}")
    return status.state == "COMPLETED"


def execute_stream_sync(
    engine: SyncTaskRunEngine[Any, Any],
) -> StreamRunResult | SourceRunResult:
    config, parameters, task_run = (
        engine.task.stream,
        engine.parameters,
        engine.task_run,
    )
    if config is None or parameters is None or task_run is None:
        raise StreamProtocolError("Stream execution requires an initialized TaskRun")
    if isinstance(config, StreamSourceConfig):
        return execute_source_sync(engine)
    run = validate_stream_parameters(engine.task, parameters)
    upstream = parameters[config.input]
    storage = upstream.storage
    consumer_id = str(task_run.id)
    binding = run_coro_as_sync(
        storage.open_consumer(
            upstream.stream_id,
            run.output.stream_id,
            consumer_id,
            uuid4().hex,
            task_run.run_count,
        )
    )
    while True:
        _check_execution(engine)
        record = run_coro_as_sync(storage.read(binding))
        if record is None:
            if run_coro_as_sync(_input_drained(upstream)):
                # EOF was observed after the first empty read. Check once more
                # so the committed tail preceding EOF cannot be skipped.
                record = run_coro_as_sync(storage.read(binding))
                if record is None:
                    run_coro_as_sync(
                        _finish_output(storage, binding, run.output.stream_id)
                    )
                    return StreamRunResult(consumer_id, run.output.stream_id)
            else:
                time.sleep(0.01)
                continue
        parameters = _parameters(engine, record, consumer_id)
        context = parameters[config.context]
        generator = None
        kind: Literal["YIELD", "RETURN"] = "RETURN"
        try:
            value: Any = call_with_parameters(engine.task.fn, parameters)
            if inspect.isgenerator(value):
                generator = value
                try:
                    value = next(generator)
                    kind = "YIELD"
                except StopIteration as stopped:
                    value = stopped.value
            if kind == "RETURN":
                # The validated RETURN contract has no output envelope. An
                # optional returned dictionary is the terminal managed state.
                state = context.managed_state if value is None else value
                value = None
            else:
                state = context.managed_state
            _check_execution(engine)
            run_coro_as_sync(
                storage.complete(binding, record, kind, value, _state_copy(state))
            )
        finally:
            if generator is not None:
                generator.close()


async def execute_stream_async(
    engine: AsyncTaskRunEngine[Any, Any],
) -> StreamRunResult | SourceRunResult:
    config, parameters, task_run = (
        engine.task.stream,
        engine.parameters,
        engine.task_run,
    )
    if config is None or parameters is None or task_run is None:
        raise StreamProtocolError("Stream execution requires an initialized TaskRun")
    if isinstance(config, StreamSourceConfig):
        return await execute_source_async(engine)
    run = validate_stream_parameters(engine.task, parameters)
    upstream = parameters[config.input]
    storage = upstream.storage
    consumer_id = str(task_run.id)
    binding = await storage.open_consumer(
        upstream.stream_id,
        run.output.stream_id,
        consumer_id,
        uuid4().hex,
        task_run.run_count,
    )
    while True:
        _check_execution(engine)
        record = await storage.read(binding)
        if record is None:
            if await _input_drained(upstream):
                record = await storage.read(binding)
                if record is None:
                    await _finish_output(storage, binding, run.output.stream_id)
                    return StreamRunResult(consumer_id, run.output.stream_id)
            else:
                await asyncio.sleep(0.01)
                continue
        parameters = _parameters(engine, record, consumer_id)
        context = parameters[config.context]
        generator = None
        kind: Literal["YIELD", "RETURN"] = "RETURN"
        try:
            value: Any = call_with_parameters(engine.task.fn, parameters)
            if inspect.isasyncgen(value):
                generator = value
                try:
                    value = await anext(generator)
                    kind = "YIELD"
                except StopAsyncIteration:
                    value = None
            elif inspect.isawaitable(value):
                value = await value
            else:
                raise StreamProtocolError(
                    "Async stream tasks require an awaitable or async generator"
                )
            if kind == "RETURN":
                state = context.managed_state if value is None else value
                value = None
            else:
                state = context.managed_state
            _check_execution(engine)
            await storage.complete(binding, record, kind, value, _state_copy(state))
        finally:
            if generator is not None:
                await generator.aclose()


def _source_execution(
    engine: Any,
) -> tuple[Any, NativeSourceStorage, SourceContext, dict[str, Any]]:
    run = validate_stream_parameters(engine.task, engine.parameters)
    storage = run.output.storage
    if not run.source or not isinstance(storage, NativeSourceStorage):
        raise StreamProtocolError("A source requires native platform storage")
    parameters = {
        key: value
        for key, value in engine.parameters.items()
        if key != "__prefect_stream_run__"
    }
    context = SourceContext(
        engine.task_run.id,
        run.output.stream_id,
        engine.task_run.run_count,
        0,
        _partition=run.partition,
    )
    parameters[engine.task.stream.context] = context
    return run, storage, context, parameters


def _source_record(value: Any, accepted_position: int) -> SourceRecord[Any]:
    if not isinstance(value, SourceRecord) or value.position != accepted_position + 1:
        raise StreamProtocolError(
            "Source yields require the next contiguous SourceRecord"
        )
    return value


def execute_source_sync(engine: SyncTaskRunEngine[Any, Any]) -> SourceRunResult:
    task_run = engine.task_run
    if task_run is None:
        raise StreamProtocolError("Source execution requires an initialized TaskRun")
    run, storage, context, parameters = _source_execution(engine)
    _check_execution(engine)
    admission = run_coro_as_sync(
        storage.open_source(
            run.output.stream_id, task_run.id, uuid4(), task_run.run_count
        )
    )
    accepted = admission.accepted_source_position
    context.accepted_source_position = accepted
    if admission.state == "COMPLETED":
        return SourceRunResult(task_run.id, run.output.stream_id, accepted)
    generator = call_with_parameters(engine.task.fn, parameters)
    if not inspect.isgenerator(generator):
        raise StreamProtocolError("A sync source must produce a generator")
    try:
        while True:
            _check_execution(engine)
            try:
                value = next(generator)
            except StopIteration as stopped:
                if stopped.value is not None:
                    raise StreamProtocolError(
                        "A source generator cannot return a value"
                    )
                _check_execution(engine)
                run_coro_as_sync(storage.close_source(admission.binding, accepted))
                return SourceRunResult(task_run.id, run.output.stream_id, accepted)
            record = _source_record(value, accepted)
            _check_execution(engine)
            try:
                run_coro_as_sync(
                    storage.append_source(
                        admission.binding, record.position, record.value
                    )
                )
            except StreamRequestError as refusal:
                if refusal.code != "SOURCE_DRAIN_REQUESTED":
                    raise
                # A drain rejects only the next unpublished position. The
                # accepted prefix remains durable and ends at its real EOF.
                run_coro_as_sync(storage.close_source(admission.binding, accepted))
                return SourceRunResult(task_run.id, run.output.stream_id, accepted)
            accepted = record.position
            context.accepted_source_position = accepted
    finally:
        generator.close()


async def execute_source_async(engine: AsyncTaskRunEngine[Any, Any]) -> SourceRunResult:
    task_run = engine.task_run
    if task_run is None:
        raise StreamProtocolError("Source execution requires an initialized TaskRun")
    run, storage, context, parameters = _source_execution(engine)
    _check_execution(engine)
    admission = await storage.open_source(
        run.output.stream_id, task_run.id, uuid4(), task_run.run_count
    )
    accepted = admission.accepted_source_position
    context.accepted_source_position = accepted
    if admission.state == "COMPLETED":
        return SourceRunResult(task_run.id, run.output.stream_id, accepted)
    generator = call_with_parameters(engine.task.fn, parameters)
    if not inspect.isasyncgen(generator):
        raise StreamProtocolError("An async source must produce an async generator")
    async with aclosing(generator):
        while True:
            _check_execution(engine)
            try:
                value = await anext(generator)
            except StopAsyncIteration:
                _check_execution(engine)
                await storage.close_source(admission.binding, accepted)
                return SourceRunResult(task_run.id, run.output.stream_id, accepted)
            record = _source_record(value, accepted)
            _check_execution(engine)
            try:
                await storage.append_source(
                    admission.binding, record.position, record.value
                )
            except StreamRequestError as refusal:
                if refusal.code != "SOURCE_DRAIN_REQUESTED":
                    raise
                await storage.close_source(admission.binding, accepted)
                return SourceRunResult(task_run.id, run.output.stream_id, accepted)
            accepted = record.position
            context.accepted_source_position = accepted


async def reject_stream_execution(engine: Any, error: str) -> None:
    """Close a rejected execution without replacing its original exception."""
    run = (engine.parameters or {}).get("__prefect_stream_run__")
    if not isinstance(run, _StreamRun):
        return
    try:
        await reject_stream_submission(run, error)
    except Exception:
        engine.logger.warning("Unable to close a rejected stream execution")


def finish_stream_sync(engine: Any) -> None:
    run = (engine.parameters or {}).get("__prefect_stream_run__")
    if isinstance(run, _StreamRun) and (
        engine.state.is_failed()
        or engine.state.is_crashed()
        or engine.state.is_cancelled()
    ):
        error = engine.state.name
        run_coro_as_sync(reject_stream_execution(engine, error))


async def finish_stream_async(engine: Any) -> None:
    run = (engine.parameters or {}).get("__prefect_stream_run__")
    if isinstance(run, _StreamRun) and (
        engine.state.is_failed()
        or engine.state.is_crashed()
        or engine.state.is_cancelled()
    ):
        error = engine.state.name
        await reject_stream_execution(engine, error)
