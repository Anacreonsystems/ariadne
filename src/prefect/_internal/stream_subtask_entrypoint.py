"""Private physical task bootstrap and exact ordinary-event HTTP transport."""

from __future__ import annotations

# The physical launcher intentionally composes private engine/context boundaries.
# pyright: reportPrivateUsage=false
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import anyio
import httpx

from prefect._internal.stream_execution import (
    SubtaskExecutionContext,
    get_stream_subtask_execution,
    stream_subtask_execution_context,
)
from prefect._internal.stream_subtask_protocol import (
    BOOTSTRAP_MAX_BYTES,
    PREFLIGHT_ERROR,
    PREFLIGHT_MAX_BYTES,
    PREFLIGHT_READY,
)
from prefect.client.orchestration import get_client
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.events.schemas.events import Event
from prefect.exceptions import PrefectSignal
from prefect.runner._starter_native import linux_process_identity
from prefect.streams import StreamProtocolError


class PhysicalStreamExecutionStopped(PrefectSignal):
    """No further user work or new state event is safe for this physical grant."""


def subtask_environment(home: Path, api_url: str) -> dict[str, str]:
    """Drop inherited platform credentials and profile providers before import."""
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("PREFECT_") and key != "ARIADNE_NATIVE_STREAM_TEST_DSN"
    }
    environment.update(
        {
            "PREFECT_API_URL": api_url,
            "PREFECT_HOME": str(home),
            "PREFECT_PROFILES_PATH": str(home / "profiles.toml"),
            "PREFECT_LOGGING_TO_API_ENABLED": "false",
            "PREFECT_SERVER_ALLOW_EPHEMERAL_MODE": "false",
        }
    )
    return environment


def _validate_event_receipt(event: Event, result: Any) -> None:
    execution = get_stream_subtask_execution()
    if execution is None or (
        not result.accepted
        or result.event_id != event.id
        or result.task_run_id != execution.task_run_id
        or result.run_count != event.payload["task_run"]["run_count"]
        or result.state_type.value != event.payload["validated_state"]["type"]
    ):
        raise PhysicalStreamExecutionStopped(
            "Physical TaskRun event was fenced or its receipt differs"
        )


def _event_is_recorded(event: Event, task_run: Any) -> bool:
    return (
        str(task_run.id) == event.resource["prefect.resource.id"].rsplit(".", 1)[1]
        and task_run.state is not None
        and task_run.state.id == event.id
        and task_run.run_count == event.payload["task_run"]["run_count"]
        and task_run.state.type.value == event.payload["validated_state"]["type"]
        and task_run.state.name == event.payload["validated_state"]["name"]
    )


def emit_physical_task_event(
    client: Any, event: Event, *, timeout: float = 20.0
) -> Event:
    """Replay exactly one signed event across ACK loss; never invent Failed."""
    execution = get_stream_subtask_execution()
    if execution is None:
        raise PhysicalStreamExecutionStopped("Physical task event has no grant")
    payload = event.model_dump(mode="json")
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = client.emit_stream_subtask_event(
                subtask_id=execution.subtask_id, event=payload
            )
            _validate_event_receipt(event, result)
            return event
        except httpx.TransportError as exc:
            if time.monotonic() >= deadline:
                try:
                    task_run = client.read_stream_subtask_task_run(
                        subtask_id=execution.subtask_id
                    )
                    if _event_is_recorded(event, task_run):
                        return event
                except (httpx.TransportError, StreamRequestError):
                    pass
                raise PhysicalStreamExecutionStopped(
                    "Physical event acknowledgement remains unknown"
                ) from exc
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        except StreamRequestError as exc:
            raise PhysicalStreamExecutionStopped(
                "Physical event transport refused this grant"
            ) from exc
        except PhysicalStreamExecutionStopped:
            raise
        except BaseException as exc:
            raise PhysicalStreamExecutionStopped(
                "Physical event delivery has no verified receipt"
            ) from exc


async def aemit_physical_task_event(
    client: Any, event: Event, *, timeout: float = 20.0
) -> Event:
    execution = get_stream_subtask_execution()
    if execution is None:
        raise PhysicalStreamExecutionStopped("Physical task event has no grant")
    payload = event.model_dump(mode="json")
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = await client.emit_stream_subtask_event(
                subtask_id=execution.subtask_id, event=payload
            )
            _validate_event_receipt(event, result)
            return event
        except httpx.TransportError as exc:
            if time.monotonic() >= deadline:
                try:
                    task_run = await client.read_stream_subtask_task_run(
                        subtask_id=execution.subtask_id
                    )
                    if _event_is_recorded(event, task_run):
                        return event
                except (httpx.TransportError, StreamRequestError):
                    pass
                raise PhysicalStreamExecutionStopped(
                    "Physical event acknowledgement remains unknown"
                ) from exc
            await anyio.sleep(min(0.05, max(0, deadline - time.monotonic())))
        except StreamRequestError as exc:
            raise PhysicalStreamExecutionStopped(
                "Physical event transport refused this grant"
            ) from exc
        except PhysicalStreamExecutionStopped:
            raise
        except BaseException as exc:
            raise PhysicalStreamExecutionStopped(
                "Physical event delivery has no verified receipt"
            ) from exc


def read_bootstrap(descriptor: int) -> SubtaskExecutionContext:
    """Verify private delivery against the actual kernel identity before context."""
    with os.fdopen(descriptor, "rb") as pipe:
        raw = pipe.read(BOOTSTRAP_MAX_BYTES + 1)
    if not raw or len(raw) > BOOTSTRAP_MAX_BYTES:
        raise StreamProtocolError("Physical bootstrap is absent or oversized")
    value = json.loads(raw)
    _, _, ticks = linux_process_identity(os.getpid())
    if type(value.get("child_process_id")) is not int or (
        value["child_process_id"] != os.getpid()
        or value.get("process_start_ticks") != ticks
    ):
        raise StreamProtocolError(
            "Physical bootstrap was delivered to a different process"
        )
    for name in (
        "flow_run_id",
        "graph_revision_id",
        "subtask_id",
        "grant_id",
        "holder_id",
        "task_run_id",
        "output_stream_id",
    ):
        value[name] = UUID(value[name])
    if value["input_stream_id"] is not None:
        value["input_stream_id"] = UUID(value["input_stream_id"])
    value["event_private_key"] = bytes.fromhex(value["event_private_key"])
    return SubtaskExecutionContext(**value)


@dataclass(frozen=True)
class _Preflight:
    task: Any
    role: str
    configuration: bytes


def _configuration_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def read_preflight(descriptor: int) -> _Preflight:
    """Resolve and retain this exact Task with no platform execution authority."""
    from prefect._internal.stream_code import native_task_code_identity
    from prefect._internal.stream_execution import get_stream_execution
    from prefect._internal.stream_partitioned import (
        bind_partition_parameters,
        validate_partitioned_task,
    )
    from prefect.client.schemas.objects import TaskRunPolicy
    from prefect.client.schemas.stream_subtasks import StreamWorkerTaskConfiguration
    from prefect.streams import (
        StreamPartitionCall,
        StreamSourceConfig,
        StreamTaskConfig,
    )
    from prefect.tasks import Task
    from prefect.utilities.importtools import import_object

    if get_stream_execution() is not None or get_stream_subtask_execution() is not None:
        raise StreamProtocolError("Task preflight must not inherit platform authority")
    with os.fdopen(descriptor, "rb") as pipe:
        raw = pipe.read(PREFLIGHT_MAX_BYTES + 1)
    if not raw or len(raw) > PREFLIGHT_MAX_BYTES:
        raise StreamProtocolError("Physical preflight is absent or oversized")
    payload = json.loads(raw)
    if (
        type(payload) is not dict
        or set(payload) != {"configuration", "role"}
        or payload["role"] not in {"SOURCE", "OPERATOR"}
    ):
        raise StreamProtocolError(
            "Physical preflight requires one own task configuration and role"
        )
    configuration = StreamWorkerTaskConfiguration.model_validate(
        payload["configuration"]
    )
    task = import_object(configuration.entrypoint)
    if not isinstance(task, Task) or task.stream is None:
        raise StreamProtocolError("Physical entrypoint must resolve a declared Task")
    validate_partitioned_task(task)
    identity = native_task_code_identity(task.fn)
    source = isinstance(task.stream, StreamSourceConfig)
    policy = TaskRunPolicy(
        retries=task.retries,
        retry_delay=task.retry_delay_seconds,
        retry_jitter_factor=task.retry_jitter_factor,
    )
    parameters = bind_partition_parameters(task, configuration.parameters)
    if (
        configuration.role != payload["role"]
        or identity.task_key != configuration.task_key
        or identity.artifact_digest != configuration.artifact_digest
        or task.version != configuration.task_version
        or task.timeout_seconds != configuration.timeout_seconds
        or task.stream.context != configuration.context_key
        or policy != configuration.task_policy
        or not set(task.tags).issubset(configuration.effective_tags)
        or source != (payload["role"] == "SOURCE")
        or (
            not source
            and (
                not isinstance(task.stream, StreamTaskConfig)
                or task.stream.input != configuration.input_key
            )
        )
        or StreamPartitionCall(parameters=parameters)
        != StreamPartitionCall(parameters=configuration.parameters)
        or source
        and not parameters
    ):
        raise StreamProtocolError(
            "Physical preflight differs from the sealed task definition"
        )
    return _Preflight(
        task,
        payload["role"],
        _configuration_bytes(configuration.model_dump(mode="json")),
    )


def validate_bootstrap_task(task: Any, bootstrap: Any) -> None:
    """Compare the imported definition with its own immutable server witnesses."""
    from prefect._internal.stream_code import native_task_code_identity
    from prefect._internal.stream_partitioned import validate_partitioned_task
    from prefect.client.schemas.objects import TaskRunPolicy
    from prefect.streams import StreamSourceConfig, StreamTaskConfig
    from prefect.tasks import Task

    if not isinstance(task, Task) or task.stream is None:
        raise StreamProtocolError("Physical entrypoint must resolve a declared Task")
    validate_partitioned_task(task)
    identity = native_task_code_identity(task.fn)
    policy = TaskRunPolicy(
        retries=task.retries,
        retry_delay=task.retry_delay_seconds,
        retry_jitter_factor=task.retry_jitter_factor,
    )
    source = isinstance(task.stream, StreamSourceConfig)
    if (
        identity.artifact_digest != bootstrap.artifact_digest
        or identity.task_key != bootstrap.task_run.task_key
        or task.version != bootstrap.task_version
        or task.timeout_seconds != bootstrap.timeout_seconds
        or task.stream.context != bootstrap.context_key
        or policy != bootstrap.task_policy
        or bootstrap.task_run.empirical_policy != bootstrap.task_policy
        or sorted(set(bootstrap.task_run.tags)) != bootstrap.effective_tags
        or not set(task.tags).issubset(bootstrap.effective_tags)
        or source != (bootstrap.assignment.manifest.role == "SOURCE")
        or (
            not source
            and (
                not isinstance(task.stream, StreamTaskConfig)
                or task.stream.input != bootstrap.input_key
            )
        )
    ):
        raise StreamProtocolError(
            "Physical task code or options differ from the sealed manifest"
        )


def execute_bootstrap(
    execution: SubtaskExecutionContext, preflight: _Preflight | None = None
) -> None:
    # Task imports task_engine; keep these after bootstrap identity validation.
    from prefect._internal.stream_engine import _StreamRun, reject_stream_submission
    from prefect.client.stream_storage import PrefectStreamStorage
    from prefect.streams import (
        Stream,
        _PartitionMetadata,
    )
    from prefect.task_engine import run_task_async, run_task_sync
    from prefect.utilities.asyncutils import run_coro_as_sync
    from prefect.utilities.importtools import import_object

    with stream_subtask_execution_context(execution):
        with get_client(sync_client=True) as client:
            bootstrap = client.read_stream_subtask_bootstrap(
                subtask_id=execution.subtask_id
            )
        if bootstrap.task_run.flow_run_run_count != bootstrap.flow_run_run_count:
            raise StreamProtocolError(
                "Physical TaskRun does not match its Flow attempt"
            )
    source = bootstrap.assignment.manifest.role == "SOURCE"
    storage = PrefectStreamStorage(execution.flow_run_id, execution.api_url)
    output = Stream(
        storage,
        str(execution.output_stream_id),
        producer_task_run_id=execution.task_run_id,
    )
    run = _StreamRun(output, bootstrap.task_run, source=source)
    run.registration_verified = True
    try:
        task = (
            preflight.task
            if preflight is not None
            else import_object(bootstrap.entrypoint)
        )
        validate_bootstrap_task(task, bootstrap)
        if preflight is not None:
            configuration = {
                "entrypoint": bootstrap.entrypoint,
                "task_key": bootstrap.task_run.task_key,
                "role": bootstrap.assignment.manifest.role,
                "task_version": bootstrap.task_version,
                "artifact_digest": bootstrap.artifact_digest,
                "context_key": bootstrap.context_key,
                "timeout_seconds": bootstrap.timeout_seconds,
                "task_policy": bootstrap.task_policy.model_dump(mode="json"),
                "effective_tags": bootstrap.effective_tags,
                "input_key": bootstrap.input_key,
                "parameters": bootstrap.parameters,
            }
            if (
                preflight.role != bootstrap.assignment.manifest.role
                or preflight.configuration != _configuration_bytes(configuration)
            ):
                raise StreamProtocolError(
                    "Physical bootstrap changed after same-child preflight"
                )
    except PhysicalStreamExecutionStopped:
        raise
    except BaseException as exc:
        with stream_subtask_execution_context(execution):
            run_coro_as_sync(reject_stream_submission(run, type(exc).__name__))
        raise
    with stream_subtask_execution_context(execution):
        run.partition = _PartitionMetadata(
            bootstrap.assignment.subtask_index,
            2,
            bootstrap.assignment.operator_uid,
            execution.assignment_epoch,
        )
        parameters: dict[str, Any] = dict(bootstrap.parameters)
        parameters[task.stream.context] = None
        parameters["__prefect_stream_run__"] = run
        if not source:
            assert bootstrap.input_key is not None
            parameters[bootstrap.input_key] = Stream(
                storage, str(execution.input_stream_id)
            )
        with get_client(sync_client=True) as lease_client:
            lease_client.renew_stream_subtask_lease(subtask_id=execution.subtask_id)
            transport_stopped = False
            try:
                if task.isasync:
                    asyncio.run(
                        run_task_async(
                            task, task_run=bootstrap.task_run, parameters=parameters
                        )
                    )
                else:
                    run_task_sync(
                        task, task_run=bootstrap.task_run, parameters=parameters
                    )
            except PhysicalStreamExecutionStopped:
                transport_stopped = True
                raise
            finally:
                if not transport_stopped:
                    final = lease_client.read_stream_subtask_task_run(
                        subtask_id=execution.subtask_id
                    )
                    info = lease_client.read_stream(execution.output_stream_id)
                    if (
                        final.state is not None
                        and final.state.is_final()
                        and info.state != "OPEN"
                    ):
                        lease_client.release_stream_subtask_lease(
                            subtask_id=execution.subtask_id
                        )


def main() -> None:
    if len(sys.argv) not in (3, 5):
        raise StreamProtocolError(
            "Physical entrypoint requires private bootstrap and GO descriptors"
        )
    preflight = None
    if len(sys.argv) == 5:
        with os.fdopen(int(sys.argv[4]), "wb", buffering=0) as ready:
            try:
                preflight = read_preflight(int(sys.argv[3]))
            except BaseException:
                ready.write(PREFLIGHT_ERROR)
                raise
            ready.write(PREFLIGHT_READY)
    execution = read_bootstrap(int(sys.argv[1]))
    with os.fdopen(int(sys.argv[2]), "rb", buffering=0) as pipe:
        if pipe.read(1) != b"G":
            raise StreamProtocolError(
                "The coordinator refused the physical startup barrier"
            )
    execute_bootstrap(execution, preflight)


if __name__ == "__main__":
    # TaskEngine imports this module for its fencing signal and transport.
    # Keep one class identity when Python executes this entrypoint via -m.
    canonical = sys.modules.get("prefect._internal.stream_subtask_entrypoint")
    if canonical is None:
        sys.modules["prefect._internal.stream_subtask_entrypoint"] = sys.modules[
            __name__
        ]
        main()
    else:
        canonical.main()
