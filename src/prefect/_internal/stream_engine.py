"""Per-input streaming execution inside a native Prefect TaskRun."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from prefect._flow_run_suspension import raise_if_flow_run_suspension_requested
from prefect.streams import (
    Stream,
    StreamContext,
    StreamProtocolError,
    StreamRunResult,
    _StreamRun,
)
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.utilities.callables import call_with_parameters

if TYPE_CHECKING:
    from prefect.task_engine import AsyncTaskRunEngine, SyncTaskRunEngine


def validate_stream_parameters(task: Any, parameters: dict[str, Any]) -> _StreamRun:
    config = task.stream
    run = parameters.get("__prefect_stream_run__")
    if not isinstance(run, _StreamRun) or not isinstance(
        parameters.get(config.input), Stream
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


def execute_stream_sync(engine: SyncTaskRunEngine[Any, Any]) -> StreamRunResult:
    run = validate_stream_parameters(engine.task, engine.parameters)
    upstream = engine.parameters[engine.task.stream.input]
    storage = upstream.storage
    consumer_id = str(engine.task_run.id)
    binding = run_coro_as_sync(
        storage.open_consumer(
            upstream.stream_id,
            run.output.stream_id,
            consumer_id,
            uuid4().hex,
            engine.task_run.run_count,
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
                    run_coro_as_sync(storage.finish(run.output.stream_id))
                    return StreamRunResult(consumer_id, run.output.stream_id)
            else:
                time.sleep(0.01)
                continue
        parameters = _parameters(engine, record, consumer_id)
        context = parameters[engine.task.stream.context]
        generator = None
        kind: Literal["YIELD", "RETURN"] = "RETURN"
        try:
            value = call_with_parameters(engine.task.fn, parameters)
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
) -> StreamRunResult:
    run = validate_stream_parameters(engine.task, engine.parameters)
    upstream = engine.parameters[engine.task.stream.input]
    storage = upstream.storage
    consumer_id = str(engine.task_run.id)
    binding = await storage.open_consumer(
        upstream.stream_id,
        run.output.stream_id,
        consumer_id,
        uuid4().hex,
        engine.task_run.run_count,
    )
    while True:
        _check_execution(engine)
        record = await storage.read(binding)
        if record is None:
            if await _input_drained(upstream):
                record = await storage.read(binding)
                if record is None:
                    await storage.finish(run.output.stream_id)
                    return StreamRunResult(consumer_id, run.output.stream_id)
            else:
                await asyncio.sleep(0.01)
                continue
        parameters = _parameters(engine, record, consumer_id)
        context = parameters[engine.task.stream.context]
        generator = None
        kind: Literal["YIELD", "RETURN"] = "RETURN"
        try:
            value = call_with_parameters(engine.task.fn, parameters)
            if inspect.isasyncgen(value):
                generator = value
                try:
                    value = await anext(generator)
                    kind = "YIELD"
                except StopAsyncIteration:
                    value = None
            else:
                value = await value
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


async def reject_stream_execution(engine: Any, error: str) -> None:
    """Close a rejected execution without replacing its original exception."""
    run = (engine.parameters or {}).get("__prefect_stream_run__")
    if not isinstance(run, _StreamRun):
        return
    try:
        status = await run.output.storage.status(run.output.stream_id)
        if status.state == "OPEN":
            await run.output.storage.finish(run.output.stream_id, error)
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
