"""Trusted coordinator for the finite, same-host fixed-P2 process profile."""

from __future__ import annotations

# This module deliberately composes private task, storage and runner interfaces.
# pyright: reportPrivateUsage=false
import concurrent.futures
import inspect
import json
import math
import re
import threading
import time
from contextvars import copy_context
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx

from prefect._internal.stream_code import NativeTaskCodeError, native_task_code_identity
from prefect._internal.stream_execution import get_stream_execution
from prefect._internal.stream_subtask_entrypoint import (
    subtask_environment as subtask_environment,
)
from prefect._internal.stream_subtask_protocol import (
    METADATA_POLL_INTERVAL_SECONDS,
    STARTUP_ABORT_ERROR,
    startup_abort_operation_id,
)
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.client.schemas.objects import TaskRunPolicy
from prefect.client.schemas.stream_subtasks import StreamOperatorSpec
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.context import FlowRunContext, TagsContext, TaskRunContext
from prefect.futures import PrefectConcurrentFuture
from prefect.streams import (
    PartitionedStream,
    SourceRunResult,
    Stream,
    StreamPartitionCall,
    StreamProtocolError,
    StreamRunResult,
    StreamSourceConfig,
    StreamTaskConfig,
)
from prefect.utilities.asyncutils import run_coro_as_sync


def validate_partitioned_task(task: Any) -> None:
    """Refuse unsupported task options before metadata or task user code."""
    config = task.stream
    if not isinstance(config, (StreamSourceConfig, StreamTaskConfig)):
        raise StreamProtocolError("Partitioned submission requires a stream task")
    config.__post_init__()
    config.validate_function(task.fn)
    if type(task.retries) is not int or not 1 <= task.retries <= 63:
        raise StreamProtocolError("Physical tasks require 1..63 explicit retries")
    delays = task.retry_delay_seconds
    numbers = (list(delays) if isinstance(delays, list) else [delays]) + [
        task.retry_jitter_factor
    ]
    if any(
        number is not None
        and (
            type(number) not in (int, float) or not math.isfinite(number) or number < 0
        )
        for number in numbers
    ):
        raise StreamProtocolError(
            "Physical retry delay and jitter require finite nonnegative numbers"
        )
    if (
        task.cache_policy != NO_CACHE
        or task.cache_key_fn is not None
        or task.persist_result is not False
        or task.result_storage is not None
        or task.result_storage_key is not None
        or task.result_serializer is not None
        or task.cache_expiration is not None
        or task.refresh_cache is not None
        or task.retry_condition_fn is not None
        or task.on_commit_hooks
        or task.on_rollback_hooks
        or task.on_running_hooks
        or task.on_completion_hooks
        or task.on_failure_hooks
        or task.asset_deps
        or task.task_run_name is not None
    ):
        raise StreamProtocolError(
            "The fixed-P2 profile does not support caching, result persistence, custom retry conditions, hooks or assets"
        )


def _direct_task_witness(task: Any) -> bytes:
    """Freeze direct Task semantics independently of ambient contextual tags."""
    validate_partitioned_task(task)
    if not task.version:
        raise StreamProtocolError("Partitioned tasks require explicit versions")
    try:
        identity = native_task_code_identity(task.fn)
    except NativeTaskCodeError as exc:
        raise StreamProtocolError(str(exc)) from exc
    module = inspect.getmodule(task.fn)
    if (
        module is None
        or not module.__file__
        or vars(module).get(task.fn.__name__) is not task
    ):
        raise StreamProtocolError(
            "Physical tasks require their exact module-level Task binding"
        )
    return json.dumps(
        {
            "stream_role": "SOURCE"
            if isinstance(task.stream, StreamSourceConfig)
            else "OPERATOR",
            "stream": asdict(task.stream),
            "entrypoint": f"{Path(module.__file__).resolve()}:{task.fn.__name__}",
            "task_name": task.name,
            "task_key": identity.task_key,
            "task_version": task.version,
            "artifact_digest": identity.artifact_digest,
            "task_policy": TaskRunPolicy(
                retries=task.retries,
                retry_delay=task.retry_delay_seconds,
                retry_jitter_factor=task.retry_jitter_factor,
            ).model_dump(mode="json"),
            "timeout_seconds": task.timeout_seconds,
            "direct_tags": sorted(set(task.tags)),
        },
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def bind_partition_calls(
    task: Any,
    partitions: tuple[StreamPartitionCall, StreamPartitionCall],
) -> list[dict[str, Any]]:
    """Bind strict snapshots without invoking user code or allocating metadata."""
    if (
        type(partitions) is not tuple
        or len(partitions) != 2
        or any(type(partition) is not StreamPartitionCall for partition in partitions)
    ):
        raise StreamProtocolError(
            "Exactly two explicit StreamPartitionCall values are required"
        )
    snapshots = [
        bind_partition_parameters(task, partition.parameters)
        for partition in partitions
    ]
    if isinstance(task.stream, StreamSourceConfig) and (
        not all(snapshots)
        or StreamPartitionCall(parameters=snapshots[0])
        == StreamPartitionCall(parameters=snapshots[1])
    ):
        raise StreamProtocolError(
            "A source requires two distinct nonempty bound split snapshots"
        )
    return snapshots


def bind_partition_parameters(task: Any, parameters: dict[str, Any]) -> dict[str, Any]:
    """Bind one own split; preflight never needs a sibling's parameters."""
    validate_partitioned_task(task)
    config = task.stream
    excluded = {config.context}
    if isinstance(config, StreamTaskConfig):
        excluded.add(config.input)
    signature = inspect.signature(task.fn)
    signature = signature.replace(
        parameters=[
            parameter
            for name, parameter in signature.parameters.items()
            if name not in excluded
        ]
    )
    values = StreamPartitionCall(parameters=parameters).parameters
    if excluded.intersection(values):
        raise StreamProtocolError(
            "Injected context and stream input are not partition parameters"
        )
    try:
        bound = signature.bind(**values)
        bound.apply_defaults()
    except TypeError as exc:
        raise StreamProtocolError(
            "Partition parameters do not match the task signature"
        ) from exc
    return StreamPartitionCall(parameters=dict(bound.arguments)).parameters


class PartitionedStreamReconstruction:
    """Declare and observe worker-owned fixed-P2 processes through durable metadata."""

    profile = "partitioned-fixed-v1"

    def __init__(self, execution: Any, flow_run: Any, flow: Any) -> None:
        self.execution = execution
        self.flow_run = flow_run
        self.flow = flow
        self.flow_attempt = flow_run.run_count
        self.phase = "OPEN"
        self.admission_attempted = False
        self.lock = threading.RLock()
        self.ready = threading.Event()
        self.entries: dict[str, Any] = {}
        self._task_witnesses: dict[str, bytes] = {}
        self.finite_entries: dict[str, Any] = {}
        self.handles: list[PartitionedStream[Any]] = []
        self.specifications: list[dict[str, Any]] = []
        self.completions: dict[UUID, concurrent.futures.Future[Any]] = {}
        self.plan: Any = None
        self._monitor: threading.Thread | None = None
        self._stopping = threading.Event()
        self._activated = False
        self.storage = PrefectStreamStorage(execution.flow_run_id, execution.api_url)

    def request(self, method: str, **kwargs: Any) -> Any:
        return run_coro_as_sync(self.storage._request(method, **kwargs))

    def declare(
        self,
        task: Any,
        *,
        operator_uid: str,
        partitions: tuple[StreamPartitionCall, StreamPartitionCall],
        upstream: PartitionedStream[Any] | None,
    ) -> PartitionedStream[Any]:
        snapshots = bind_partition_calls(task, partitions)
        if type(operator_uid) is not str or not re.fullmatch(
            r"[A-Za-z0-9_.-]{1,64}", operator_uid
        ):
            raise StreamProtocolError("An explicit stable operator_uid is required")
        if not task.version or not self.flow.version:
            raise StreamProtocolError(
                "Partitioned tasks and flows require explicit versions"
            )
        try:
            identity = native_task_code_identity(task.fn)
        except NativeTaskCodeError as exc:
            raise StreamProtocolError(str(exc)) from exc
        module = inspect.getmodule(task.fn)
        if (
            module is None
            or not module.__file__
            or vars(module).get(task.fn.__name__) is not task
        ):
            raise StreamProtocolError(
                "Physical tasks require their exact module-level Task binding"
            )
        entrypoint = f"{Path(module.__file__).resolve()}:{task.fn.__name__}"
        source = isinstance(task.stream, StreamSourceConfig)
        with self.lock:
            if self.phase != "OPEN" or len(self.specifications) >= 2:
                raise StreamProtocolError(
                    "The fixed graph accepts one source and one operator"
                )
            if operator_uid in self.entries:
                raise StreamProtocolError("operator_uid must be unique in the graph")
            if source:
                if self.specifications or upstream is not None:
                    raise StreamProtocolError("The fixed graph begins with one source")
            elif (
                not self.specifications
                or upstream is not self.handles[0]
                or len(self.specifications) != 1
            ):
                raise StreamProtocolError(
                    "The operator requires this graph's source handle"
                )
            spec = StreamOperatorSpec(
                operator_uid=operator_uid,
                role="SOURCE" if source else "OPERATOR",
                task_name=task.name,
                task_key=identity.task_key,
                task_version=task.version,
                artifact_digest=identity.artifact_digest,
                entrypoint=entrypoint,
                parameters=snapshots,
                input_key=None if source else task.stream.input,
                context_key=task.stream.context,
                upstream_operator_uid=None
                if source
                else self.specifications[0]["operator_uid"],
                task_policy=TaskRunPolicy(
                    retries=task.retries,
                    retry_delay=task.retry_delay_seconds,
                    retry_jitter_factor=task.retry_jitter_factor,
                ),
                timeout_seconds=task.timeout_seconds,
                tags=sorted(set(task.tags) | TagsContext.get().current_tags),
            )
            witness = _direct_task_witness(task)
            handle: PartitionedStream[Any] = PartitionedStream(_ready=self.ready)
            self.entries[operator_uid] = task
            self._task_witnesses[operator_uid] = witness
            self.specifications.append(spec.model_dump(mode="json"))
            self.handles.append(handle)
            if not source:
                self.seal()
            return handle

    def _bind_handles(self) -> None:
        if (
            self.plan.flow_run_id != self.flow_run.id
            or len({item.manifest.task_run_id for item in self.plan.subtasks}) != 4
        ):
            raise StreamProtocolError(
                "Sealed graph has an incompatible TaskRun identity set"
            )
        placement: dict[int, UUID] = {}
        for item in self.plan.subtasks:
            if item.assigned_worker_id is None:
                raise StreamProtocolError(
                    "Physical graph requires assigned registered workers"
                )
            previous = placement.setdefault(item.subtask_index, item.assigned_worker_id)
            if previous != item.assigned_worker_id:
                raise StreamProtocolError(
                    "Fixed-P2 placement must preserve same-index worker ownership"
                )
        if (
            set(placement) != {0, 1}
            or len(set(placement.values())) != 2
            or self.plan.placement_digest is None
        ):
            raise StreamProtocolError(
                "Fixed-P2 execution requires two distinct assigned workers"
            )
        for index, spec in enumerate(self.specifications):
            assignments = sorted(
                [
                    item
                    for item in self.plan.subtasks
                    if item.operator_uid == spec["operator_uid"]
                ],
                key=lambda item: item.subtask_index,
            )
            if len(assignments) != 2 or [
                item.subtask_index for item in assignments
            ] != [0, 1]:
                raise StreamProtocolError(
                    "Sealed operator does not contain its two physical partitions"
                )
            physical: list[Stream[Any]] = []
            for item in assignments:
                if (
                    item.manifest.role != spec["role"]
                    or item.manifest.flow_run_id != self.flow_run.id
                ):
                    raise StreamProtocolError(
                        "Physical role or FlowRun differs from the declaration"
                    )
                completion: concurrent.futures.Future[Any] = concurrent.futures.Future()
                self.completions[item.manifest.task_run_id] = completion
                stream = Stream(
                    self.storage,
                    str(item.manifest.stream_id),
                    producer_task_run_id=item.manifest.task_run_id,
                )
                stream._bind_completion(
                    PrefectConcurrentFuture(item.manifest.task_run_id, completion)
                )
                physical.append(stream)
            self.handles[index]._bind((physical[0], physical[1]))

    def seal(self) -> None:
        with self.lock:
            if self.phase == "READY":
                return
            if self.phase != "OPEN" or len(self.specifications) != 2:
                raise StreamProtocolError(
                    "The partitioned graph requires its operator declaration"
                )
            if self.flow_run.deployment_id is None:
                raise StreamProtocolError(
                    "The fixed-P2 profile requires its enrolled deployment and two workers"
                )
            for operator_uid, task in self.entries.items():
                if _direct_task_witness(task) != self._task_witnesses[operator_uid]:
                    raise StreamProtocolError(
                        "Task definition changed after its partitioned declaration"
                    )
            try:
                self.admission_attempted = True
                self.plan = self.request(
                    "seal_stream_graph",
                    flow_run_id=self.flow_run.id,
                    operation_id=uuid5(NAMESPACE_URL, f"fixed-p2/{self.flow_run.id}"),
                    flow_version=self.flow.version,
                    deployment_id=self.flow_run.deployment_id,
                    deployment_version=self.flow_run.deployment_version,
                    working_directory=str(Path.cwd().resolve()),
                    operators=self.specifications,
                )
                self.plan = self.request(
                    "admit_stream_region",
                    graph_revision_id=self.plan.id,
                    operation_id=uuid5(self.plan.id, "admit"),
                    graph_digest=self.plan.graph_digest,
                    metadata_lag=True,
                )
                self._bind_handles()
                context = copy_context()
                self._monitor = threading.Thread(
                    target=context.run, args=(self._supervise,), daemon=True
                )
                self._monitor.start()
                deadline = time.monotonic() + 120
                while self.plan.startup_completed_at is None:
                    if self.plan.state in {"ABORTING", "RELEASED"}:
                        raise StreamProtocolError(
                            "Assigned workers refused the physical startup barrier"
                        )
                    grants = [item.current_grant_id for item in self.plan.subtasks]
                    if all(grant is not None for grant in grants):
                        try:
                            self.plan = self.request(
                                "activate_stream_graph",
                                graph_revision_id=self.plan.id,
                                operation_id=uuid5(self.plan.id, "activate"),
                                graph_digest=self.plan.graph_digest,
                                grant_ids=grants,
                            )
                        except StreamRequestError as refusal:
                            if (
                                refusal.code != "METADATA_NOT_READY"
                                or not refusal.retryable
                            ):
                                raise
                    if self.plan.startup_completed_at is not None:
                        break
                    if time.monotonic() >= deadline:
                        raise StreamProtocolError(
                            "Assigned workers did not complete physical registration"
                        )
                    time.sleep(METADATA_POLL_INTERVAL_SECONDS)
                    self.plan = self.request(
                        "read_stream_graph", graph_revision_id=self.plan.id
                    )
                self._activated = True
                self.phase = "READY"
                self.ready.set()
            except BaseException:
                if not self._activated:
                    self.abort()
                raise

    def _observe(self) -> bool:
        self.plan = self.request("read_stream_graph", graph_revision_id=self.plan.id)
        for item in self.plan.subtasks:
            completion = self.completions[item.manifest.task_run_id]
            if completion.done():
                continue
            task_run = self.request(
                "read_task_run", task_run_id=item.manifest.task_run_id
            )
            if task_run.state is None or not task_run.state.is_final():
                continue
            info = self.request("read_stream", stream_id=item.manifest.stream_id)
            if (
                info.id != item.manifest.stream_id
                or info.flow_run_id != self.flow_run.id
            ):
                raise StreamProtocolError(
                    "Physical output identity differs from its manifest"
                )
            if not item.physical_slot_released:
                continue
            if task_run.state.is_completed():
                if info.state != "COMPLETED":
                    raise StreamProtocolError(
                        "Completed TaskRun has no matching durable EOF"
                    )
                completion.set_result(
                    SourceRunResult(task_run.id, str(info.id), info.producer_position)
                    if item.manifest.role == "SOURCE"
                    else StreamRunResult(str(task_run.id), str(info.id))
                )
            else:
                if info.state == "OPEN":
                    raise StreamProtocolError(
                        "Released physical slot retains an unfinished output"
                    )
                completion.set_exception(
                    StreamProtocolError(f"Physical task ended in {task_run.state.name}")
                )
        return all(completion.done() for completion in self.completions.values())

    def _supervise(self) -> None:
        while not self.ready.wait(METADATA_POLL_INTERVAL_SECONDS):
            if self._stopping.is_set():
                return
        while not self._stopping.is_set():
            try:
                if self._observe():
                    return
            except httpx.TransportError:
                # An unavailable control API cannot prove a child is stopped.
                # Retain authority and resume observation when it is reachable.
                pass
            except StreamRequestError as refusal:
                if not refusal.retryable:
                    for completion in self.completions.values():
                        if not completion.done():
                            completion.set_exception(refusal)
                    return
            except BaseException as exc:
                for completion in self.completions.values():
                    if not completion.done():
                        completion.set_exception(exc)
                return
            self._stopping.wait(METADATA_POLL_INTERVAL_SECONDS)

    def finish(self) -> None:
        self.seal()
        failures: list[Exception] = []
        for handle in self.handles:
            try:
                handle.wait()
            except Exception as exc:
                failures.append(exc)
        if self._monitor is not None:
            self._monitor.join()
        if failures:
            raise failures[0]

    def abort(self) -> None:
        with self.lock:
            if self._activated:
                self.finish()
                return
            self.phase = "ABORTED"
            self._stopping.set()
            if self.plan is not None:
                deadline = time.monotonic() + 120
                while True:
                    self.plan = self.request(
                        "abort_stream_graph",
                        graph_revision_id=self.plan.id,
                        operation_id=startup_abort_operation_id(self.plan.id),
                        graph_digest=self.plan.graph_digest,
                        error=STARTUP_ABORT_ERROR,
                    )
                    if self.plan.state == "RELEASED":
                        break
                    if time.monotonic() >= deadline:
                        raise StreamProtocolError(
                            "Startup abort awaits assigned-worker stop receipts"
                        )
                    time.sleep(METADATA_POLL_INTERVAL_SECONDS)
            if (
                self._monitor is not None
                and self._monitor is not threading.current_thread()
            ):
                self._monitor.join()


def submit_partitioned_stream(
    task: Any,
    *,
    operator_uid: str,
    partitions: tuple[StreamPartitionCall, StreamPartitionCall],
    upstream: PartitionedStream[Any] | None,
) -> PartitionedStream[Any]:
    execution = get_stream_execution()
    context = FlowRunContext.get()
    if (
        execution is None
        or context is None
        or context.flow is None
        or TaskRunContext.get() is not None
    ):
        raise StreamProtocolError(
            "Partitioned submission requires its native Flow coordinator"
        )
    graph = getattr(execution, "_reconstruction", None)
    if not isinstance(graph, PartitionedStreamReconstruction):
        raise StreamProtocolError(
            "submit_partitioned_stream requires partitioned-fixed-v1"
        )
    return graph.declare(
        task, operator_uid=operator_uid, partitions=partitions, upstream=upstream
    )
