"""Reconstruct a bounded native task graph from durable submission identities.

The flow body declares the graph again. No task enters user code before its
complete declaration is sealed or reconciled by the server.
"""

from __future__ import annotations

# This coordinator intentionally composes private SDK, runner and storage SPI.
# Keep argument/member checking while allowing those internal peer boundaries.
# pyright: reportPrivateUsage=false
import inspect
import json
import threading
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from anyio.to_thread import run_sync

from prefect._internal.stream_code import NativeTaskCodeError, native_task_code_identity
from prefect._internal.stream_execution import get_stream_execution
from prefect.exceptions import PrefectSignal
from prefect.streams import (
    SourceRunResult,
    Stream,
    StreamProtocolError,
    StreamRunResult,
    StreamSourceConfig,
    StreamTaskConfig,
)
from prefect.utilities.asyncutils import run_coro_as_sync


class _UnstartedStreamGraph(PrefectSignal):
    """Release held workers without failing their durable reservations."""


@dataclass
class _Submission:
    manifest: Any
    run: Any
    future: Any = None
    completed_result: Any = None


class StreamReconstruction:
    def __init__(self, execution: Any, flow_attempt: int) -> None:
        self.execution = execution
        self.flow_attempt = flow_attempt
        self.entries: dict[str, _Submission] = {}
        self.finite_entries: dict[str, Any] = {}
        self.ready = threading.Event()
        self.lock = threading.RLock()
        self.phase = "OPEN"
        self.admission_attempted = False
        self.operation_id = uuid5(
            NAMESPACE_URL,
            f"prefect-stream-graph-v1/{execution.flow_run_id}/{execution.generation}",
        )

    def witnesses(self) -> list[dict[str, str]]:
        return [
            {"submission_key": key, "manifest_digest": entry.manifest.manifest_digest}
            for key, entry in self.entries.items()
        ]

    def seal(self) -> None:
        from prefect.client.stream_storage import PrefectStreamStorage

        with self.lock:
            if self.phase == "READY":
                return
            if self.phase != "OPEN":
                raise StreamProtocolError("The native graph declaration was aborted")
            if not self.entries:
                raise StreamProtocolError(
                    "A native execution requires a declared graph"
                )
            try:
                self.admission_attempted = True
                storage = PrefectStreamStorage(self.execution.flow_run_id)
                plan = run_coro_as_sync(
                    storage._request(
                        "read_stream_finite_plan",
                        flow_run_id=self.execution.flow_run_id,
                    )
                )
                recovering = self.execution.generation > 1 and plan.manifest_sealed
                method = (
                    "prepare_stream_recovery" if recovering else "seal_stream_manifest"
                )
                arguments: dict[str, Any] = {}
                if recovering and plan.items:
                    arguments["finite_manifests"] = [
                        {
                            "submission_key": item.submission_key,
                            "manifest_digest": item.manifest_digest,
                            "phase": item.phase,
                        }
                        for item in plan.items
                    ]
                elif self.finite_entries:
                    prefix = next(
                        entry
                        for entry in self.finite_entries.values()
                        if entry.manifest.phase == "PREFIX"
                    )
                    if prefix.receipt is None:
                        raise StreamProtocolError(
                            "Stream seal requires the finite prefix receipt"
                        )
                    arguments["finite_prefix"] = {
                        "submission_key": prefix.manifest.submission_key,
                        "manifest_digest": prefix.manifest.manifest_digest,
                        "phase": "PREFIX",
                        "task_run_id": str(prefix.manifest.task_run_id),
                        "result_digest": prefix.receipt.result_digest,
                    }
                run_coro_as_sync(
                    storage._request(
                        method,
                        flow_run_id=self.execution.flow_run_id,
                        operation_id=self.operation_id,
                        manifests=self.witnesses(),
                        **arguments,
                    )
                )
                for entry in self.entries.values():
                    task_run = run_coro_as_sync(
                        storage._request(
                            "read_task_run", task_run_id=entry.manifest.task_run_id
                        )
                    )
                    info = run_coro_as_sync(
                        storage._request(
                            "read_stream", stream_id=entry.manifest.stream_id
                        )
                    )
                    if info.state == "FAILED":
                        raise StreamProtocolError(
                            "A failed stream requires a new FlowRun"
                        )
                    entry.run.task_run = task_run
                    if info.state == "COMPLETED":
                        if task_run.state is None or not task_run.state.is_completed():
                            raise StreamProtocolError(
                                "Durable EOF lacks reconciled task success"
                            )
                        entry.completed_result = (
                            SourceRunResult(
                                task_run.id, str(info.id), info.producer_position
                            )
                            if entry.run.source
                            else StreamRunResult(str(task_run.id), str(info.id))
                        )
                self.phase = "READY"
                self.ready.set()
            except BaseException:
                self.abort()
                raise

    def abort(self) -> None:
        with self.lock:
            if self.phase == "OPEN":
                self.phase = "ABORTED"
                self.ready.set()

    def wait_for_entry(
        self, task_run_id: UUID, cancel_event: threading.Event
    ) -> _Submission:
        while not self.ready.wait(0.1):
            if cancel_event.is_set():
                raise _UnstartedStreamGraph("Unstarted graph execution cancelled")
        if self.phase != "READY":
            raise _UnstartedStreamGraph("The flow did not seal its graph")
        for entry in self.entries.values():
            if entry.manifest.task_run_id == task_run_id:
                return entry
        raise StreamProtocolError("TaskRun is not a declared native submission")

    def finish(self) -> None:
        self.seal()
        for entry in self.entries.values():
            if entry.future is None:
                raise StreamProtocolError("Native submission has no completion future")
            entry.future.result()


def reconstruction() -> StreamReconstruction | None:
    execution = get_stream_execution()
    return (
        getattr(execution, "_reconstruction", None) if execution is not None else None
    )


def _begin(flow_run: Any) -> StreamReconstruction | None:
    execution = get_stream_execution()
    if execution is None:
        return None
    if flow_run is None or execution.flow_run_id != flow_run.id:
        raise StreamProtocolError("A native execution cannot enter another FlowRun")
    previous = reconstruction()
    if previous is not None and previous.flow_attempt == flow_run.run_count:
        return previous
    if previous is not None:
        if previous.phase != "ABORTED":
            raise StreamProtocolError(
                "A sealed graph cannot retry in its old execution generation"
            )
        for entry in previous.entries.values():
            if entry.future is None:
                continue
            try:
                entry.future.result()
            except _UnstartedStreamGraph:
                pass
    graph = StreamReconstruction(execution, flow_run.run_count)
    object.__setattr__(execution, "_reconstruction", graph)
    return graph


@contextmanager
def stream_flow_scope(flow_run: Any):
    graph = _begin(flow_run)
    try:
        yield
        if graph is not None:
            graph.finish()
    except BaseException:
        if graph is not None:
            graph.abort()
        raise


@asynccontextmanager
async def async_stream_flow_scope(flow_run: Any):
    graph = _begin(flow_run)
    try:
        yield
        if graph is not None:
            await run_sync(graph.finish)
    except BaseException:
        if graph is not None:
            graph.abort()
        raise


def seal_declared_graph() -> None:
    graph = reconstruction()
    if graph is not None:
        graph.seal()


def submit_reconstructed_stream(
    task: Any, args: tuple[Any, ...], kwargs: dict[str, Any], wait_for: Any
) -> Stream[Any]:
    from prefect._internal.stream_engine import _StreamRun
    from prefect.client.schemas.objects import TaskRunPolicy
    from prefect.client.stream_storage import PrefectStreamStorage
    from prefect.context import FlowRunContext, TagsContext, TaskRunContext
    from prefect.task_runners import ThreadPoolTaskRunner
    from prefect.utilities.collections import visit_collection

    graph = reconstruction()
    context = FlowRunContext.get()
    if (
        graph is None
        or context is None
        or context.flow_run is None
        or context.flow is None
    ):
        raise StreamProtocolError(
            "Keyed stream submission requires its native flow execution"
        )
    if TaskRunContext.get() is not None or not isinstance(
        context.task_runner, ThreadPoolTaskRunner
    ):
        raise StreamProtocolError(
            "Native graph declaration requires the flow thread-pool runner"
        )
    runner = cast(ThreadPoolTaskRunner[Any], cast(Any, context).task_runner)
    if runner._max_workers < 2:
        raise StreamProtocolError(
            "Native reconstruction requires concurrent task-runner slots"
        )
    if wait_for is not None:
        raise StreamProtocolError(
            "Native reconstruction does not support wait_for dependencies"
        )
    if "submission_key" in inspect.signature(task.fn).parameters:
        raise StreamProtocolError("submission_key is reserved in a reconstructed graph")
    kwargs = dict(kwargs)
    key = kwargs.pop("submission_key", None)
    if not isinstance(key, str) or not key:
        raise StreamProtocolError(
            "Every reconstructed stream requires an explicit submission_key"
        )
    if not task.version or not context.flow.version:
        raise StreamProtocolError(
            "Native reconstruction requires task and flow versions"
        )

    try:
        code_identity = native_task_code_identity(task.fn)
    except NativeTaskCodeError as exc:
        raise StreamProtocolError(str(exc)) from exc
    config = task.stream
    parameters = config.bind_parameters(task.fn, args, kwargs)
    source = isinstance(config, StreamSourceConfig)
    input_key = config.input if isinstance(config, StreamTaskConfig) else None
    upstream = parameters.get(input_key) if input_key else None
    side_parameters = {
        name: value
        for name, value in parameters.items()
        if name not in (input_key, config.context)
    }

    def reject_dependency(value: Any) -> None:
        from prefect.futures import PrefectFuture

        if isinstance(value, (Stream, PrefectFuture)):
            raise StreamProtocolError(
                "Native side parameters require finite JSON and durable references"
            )

    visit_collection(side_parameters, visit_fn=reject_dependency, return_data=False)

    # Round-trip only strict JSON values. Pydantic must not silently turn tuples,
    # UUIDs, dates, bytes, custom mappings or non-finite values into another input.
    def strict_json(value: Any) -> None:
        if value is None or type(value) in (str, bool, int):
            return
        if type(value) is float:
            try:
                json.dumps(value, allow_nan=False)
            except ValueError as exc:
                raise StreamProtocolError(
                    "Native side parameters require strict finite JSON"
                ) from exc
            return
        if type(value) is list:
            for item in cast(list[Any], value):
                strict_json(item)
            return
        if type(value) is dict:
            mapping = cast(dict[object, Any], value)
            if any(type(key) is not str for key in mapping):
                raise StreamProtocolError(
                    "Native side parameters require string JSON keys"
                )
            for item in mapping.values():
                strict_json(item)
            return
        raise StreamProtocolError("Native side parameters require strict finite JSON")

    strict_json(side_parameters)
    upstream_id: UUID | None = None
    if not source:
        if not isinstance(upstream, Stream) or not isinstance(
            upstream.storage, PrefectStreamStorage
        ):
            raise StreamProtocolError(
                "A reconstructed operator requires its declared native upstream"
            )
        upstream_id = UUID(upstream.stream_id)
        if not any(
            entry.manifest.stream_id == upstream_id for entry in graph.entries.values()
        ):
            raise StreamProtocolError(
                "The upstream must belong to this exact graph declaration"
            )
    with graph.lock:
        if graph.phase != "OPEN":
            raise StreamProtocolError(
                "No submission may be added after the graph barrier"
            )
        if key in graph.entries or key in graph.finite_entries:
            raise StreamProtocolError(
                "A submission_key must occur once in the flow declaration"
            )
        if len(graph.entries) >= runner._max_workers:
            raise StreamProtocolError(
                "The entire bounded graph must fit the concurrent runner slots"
            )
        storage = PrefectStreamStorage(context.flow_run.id)
        manifest = run_coro_as_sync(
            storage._request(
                "reserve_stream_submission",
                flow_run_id=context.flow_run.id,
                submission_key=key,
                role="SOURCE" if source else "OPERATOR",
                protocol=config.protocol,
                task_name=task.name,
                task_key=code_identity.task_key,
                artifact_digest=code_identity.artifact_digest,
                task_version=task.version,
                flow_version=context.flow.version,
                deployment_id=context.flow_run.deployment_id,
                deployment_version=context.flow_run.deployment_version,
                parameters=side_parameters,
                upstream_stream_id=upstream_id,
                input_key=input_key,
                task_policy=TaskRunPolicy(
                    retries=task.retries,
                    retry_delay=task.retry_delay_seconds,
                    retry_jitter_factor=task.retry_jitter_factor,
                ),
                tags=sorted(set(task.tags).union(TagsContext.get().current_tags or [])),
            )
        )
        graph.execution.register_manifest(
            manifest.task_run_id, manifest.manifest_digest
        )
        output = Stream[Any](
            storage, str(manifest.stream_id), producer_task_run_id=manifest.task_run_id
        )
        run = _StreamRun(output, source=source)
        run.registration_verified = True
        entry = _Submission(manifest, run)
        graph.entries[key] = entry
        parameters["__prefect_stream_run__"] = run
        try:
            run.task_run = run_coro_as_sync(
                storage._request("read_task_run", task_run_id=manifest.task_run_id)
            )
            future = runner.submit(task, parameters)
            entry.future = future
            output._bind_completion(future)
        except BaseException:
            graph.abort()
            raise
        from contextvars import copy_context

        from prefect._internal.stream_engine import reject_stream_submission

        completion_context = copy_context()

        def on_completion(completed: Any) -> None:
            try:
                completed.result()
            except PrefectSignal:
                return
            except BaseException as exc:
                try:
                    run_coro_as_sync(reject_stream_submission(run, type(exc).__name__))
                except Exception:
                    # The flow failure remains authoritative if the API is lost.
                    import logging

                    logging.getLogger("prefect.streams").warning(
                        "Unable to close a failed native stream task"
                    )

        future.add_done_callback(
            lambda completed: completion_context.copy().run(on_completion, completed)
        )
        return output


def prepare_flow_state(state: Any) -> None:
    """A sealed graph permits task retries and explicit generation takeover only."""
    graph = reconstruction()
    if (
        graph is not None
        and (
            graph.admission_attempted
            or graph.finite_entries
            or graph.execution.generation > 1
        )
        and state.is_failed()
    ):
        # CoreFlowPolicy honors this ordinary StateDetails input. No force or
        # policy bypass is used, and infrastructure takeover remains server-owned.
        state.state_details.retriable = False


def reconcile_unstarted_graph(flow_run: Any) -> None:
    graph = reconstruction()
    if (
        graph is None
        or graph.phase != "ABORTED"
        or not graph.entries
        or graph.execution.generation > 1
        or graph.admission_attempted
        or graph.finite_entries
    ):
        return
    if flow_run is None or flow_run.state is None or not flow_run.state.is_final():
        return
    from prefect.client.stream_storage import PrefectStreamStorage

    run_coro_as_sync(
        PrefectStreamStorage(flow_run.id)._request(
            "abort_unsealed_stream_execution",
            flow_run_id=flow_run.id,
            operation_id=uuid5(
                NAMESPACE_URL,
                f"prefect-stream-graph-abort-v1/{flow_run.id}/{graph.execution.generation}",
            ),
            manifests=graph.witnesses(),
            error="Flow ended before native graph admission",
        )
    )
