"""Ordinary finite Task engines bound to native prefix/tail receipts."""

from __future__ import annotations

import hashlib
import json

# This SDK coordinator composes the internal storage transport, like graph reconstruction.
# pyright: reportPrivateUsage=false
import time
from dataclasses import dataclass
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

import anyio

from prefect._internal.stream_code import NativeTaskCodeError, native_task_code_identity
from prefect._internal.stream_reconstruction import reconstruction
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.client.schemas.objects import TaskRunPolicy
from prefect.client.schemas.streams import StreamFiniteManifest, StreamFiniteReceipt
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.context import FlowRunContext, TagsContext, TaskRunContext
from prefect.streams import StreamCollection, StreamProtocolError
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.utilities.callables import get_call_parameters


@dataclass
class _FiniteRun:
    manifest: StreamFiniteManifest
    task: Any
    parameters: dict[str, Any]
    task_run: Any
    receipt: StreamFiniteReceipt | None = None


def finite_run(engine: Any) -> _FiniteRun | None:
    graph = reconstruction()
    if graph is None or engine.task_run is None:
        return None
    return next(
        (
            entry
            for entry in graph.finite_entries.values()
            if entry.manifest.task_run_id == engine.task_run.id
        ),
        None,
    )


async def _resolve_parameter(value: Any) -> tuple[Any, dict[str, Any]]:
    if isinstance(value, StreamCollection):
        resolved = await value.resolve()
        digest = hashlib.sha256(
            json.dumps(
                resolved,
                sort_keys=True,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return resolved, {
            "kind": "STREAM_COLLECTION",
            "stream_id": value.stream.stream_id,
            "limit": value.limit,
            "result_digest": digest,
        }
    if type(value) is dict:
        resolved_mapping: dict[str, Any] = {}
        witnesses: dict[str, Any] = {}
        for key, item in cast(dict[Any, Any], value).items():
            if type(key) is not str:
                raise StreamProtocolError(
                    "Finite task parameters require string JSON keys"
                )
            resolved_mapping[key], witnesses[key] = await _resolve_parameter(item)
        return resolved_mapping, {"kind": "OBJECT", "values": witnesses}
    if type(value) is list:
        pairs = [await _resolve_parameter(item) for item in cast(list[Any], value)]
        return [pair[0] for pair in pairs], {
            "kind": "ARRAY",
            "values": [pair[1] for pair in pairs],
        }
    return value, {"kind": "JSON", "value": value}


async def _read_receipt(
    storage: PrefectStreamStorage, task_run_id: UUID
) -> StreamFiniteReceipt | None:
    try:
        return await storage._request(
            "read_stream_finite_receipt", task_run_id=task_run_id
        )
    except StreamRequestError as exc:
        if exc.code != "NOT_FOUND":
            raise
        return None


async def _reserve(
    task: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> _FiniteRun:
    # utilities.engine imports Task; defer until the finite call is requested.
    from prefect.utilities.engine import refuse_stream_graph_dependencies

    refuse_stream_graph_dependencies((args, kwargs))
    context = FlowRunContext.get()
    if (
        context is not None
        and context.flow is not None
        and context.flow.streaming is not None
        and context.flow.streaming.profile == "partitioned-dag-v1"
    ):
        raise StreamProtocolError(
            "Finite Task prefix/tail execution is unsupported in the topology-only DAG profile"
        )
    graph = reconstruction()
    if (
        graph is None
        or context is None
        or context.flow_run is None
        or context.flow is None
    ):
        raise StreamProtocolError("A finite native call requires its active flow")
    if TaskRunContext.get() is not None:
        raise StreamProtocolError("Nested finite tasks are outside the native profile")
    if task.stream is not None or task.isgenerator:
        raise StreamProtocolError("Use submit_stream() for a native stream task")
    if not task.stream_key or not task.version or not context.flow.version:
        raise StreamProtocolError(
            "A finite native task requires stream_key and explicit task/flow versions"
        )
    if (
        task.cache_key_fn is not None
        or task.persist_result is True
        or task.result_storage is not None
        or task.result_storage_key is not None
        or task.result_serializer is not None
    ):
        raise StreamProtocolError(
            "Native finite tasks use bounded JSON receipts for result persistence"
        )
    try:
        code = native_task_code_identity(task.fn)
    except NativeTaskCodeError as exc:
        raise StreamProtocolError(str(exc)) from exc
    parameters, parameter_witness = await _resolve_parameter(
        get_call_parameters(task.fn, args, kwargs)
    )
    if graph.phase == "OPEN" and not graph.entries:
        phase = "PREFIX"
    elif graph.phase == "READY":
        phase = "TAIL"
    else:
        raise StreamProtocolError(
            "Finite calls require the prefix boundary or a sealed, completed graph"
        )
    if any(entry.manifest.phase == phase for entry in graph.finite_entries.values()):
        raise StreamProtocolError("The bounded graph permits one finite task per phase")
    if task.stream_key in graph.entries:
        raise StreamProtocolError("A submission key must identify one graph step")
    if phase == "TAIL" and not graph.finite_entries:
        raise StreamProtocolError("A mixed native graph requires its finite prefix")
    storage = PrefectStreamStorage(context.flow_run.id)
    plan = await storage._request(
        "read_stream_finite_plan", flow_run_id=context.flow_run.id
    )
    manifest = await storage._request(
        "reserve_stream_finite_submission",
        flow_run_id=context.flow_run.id,
        phase=phase,
        submission_key=task.stream_key,
        task_name=task.name,
        task_key=code.task_key,
        artifact_digest=code.artifact_digest,
        task_version=task.version,
        flow_version=context.flow.version,
        deployment_id=context.flow_run.deployment_id,
        deployment_version=context.flow_run.deployment_version,
        parameters={
            "schema": "native-finite-parameters-v1",
            "binding": parameter_witness,
        },
        task_policy=TaskRunPolicy(
            retries=task.retries,
            retry_delay=task.retry_delay_seconds,
            retry_jitter_factor=task.retry_jitter_factor,
        ).model_dump(mode="json"),
        tags=sorted(set(task.tags).union(TagsContext.get().current_tags or [])),
    )
    graph.execution.register_manifest(manifest.task_run_id, manifest.manifest_digest)
    if (
        graph.execution.generation > 1
        and phase == "PREFIX"
        and not plan.manifest_sealed
    ):
        # A lost process may have reserved only its prefix. Prepare that old
        # binding once, then use a distinct first seal for newly declared streams.
        await storage._request(
            "prepare_stream_recovery",
            flow_run_id=context.flow_run.id,
            operation_id=uuid5(
                NAMESPACE_URL,
                f"prefect-finite-prefix-recovery-v1/{context.flow_run.id}/{graph.execution.generation}",
            ),
            manifests=[],
            finite_manifests=[
                {
                    "submission_key": item.submission_key,
                    "manifest_digest": item.manifest_digest,
                    "phase": item.phase,
                }
                for item in plan.items
            ],
        )
    receipt = await _read_receipt(storage, manifest.task_run_id)
    if phase == "PREFIX" and plan.manifest_sealed and receipt is None:
        raise StreamProtocolError("A sealed mixed graph lacks its durable prefix")
    registered = await storage._request(
        "read_task_run", task_run_id=manifest.task_run_id
    )
    bound_task = task.with_options(cache_policy=NO_CACHE, persist_result=False)
    entry = _FiniteRun(manifest, bound_task, parameters, registered, receipt)
    graph.finite_entries[task.stream_key] = entry
    return entry


async def replay_finite_result(engine: Any) -> StreamFiniteReceipt | None:
    entry = finite_run(engine)
    if entry is None:
        return None
    receipt = await _read_receipt(
        PrefectStreamStorage(entry.manifest.flow_run_id), entry.manifest.task_run_id
    )
    if receipt is not None:
        entry.receipt = receipt
        # A local retry after an uncertain ACK must use the durable attempt,
        # rather than publish success for an unaccepted later local attempt.
        engine.task_run = await PrefectStreamStorage(
            entry.manifest.flow_run_id
        )._request("read_task_run", task_run_id=entry.manifest.task_run_id)
    return receipt


async def admit_finite_attempt(engine: Any) -> None:
    """Wait for ordinary signed Running metadata before entering user code."""
    entry = finite_run(engine)
    if entry is None:
        return
    storage = PrefectStreamStorage(entry.manifest.flow_run_id)
    deadline = time.monotonic() + storage.admission_timeout
    while True:
        task = await storage._request(
            "read_task_run", task_run_id=entry.manifest.task_run_id
        )
        if (
            task.state is not None
            and task.state.is_running()
            and task.run_count == engine.task_run.run_count
        ):
            return
        if time.monotonic() >= deadline:
            raise StreamProtocolError("Finite task Running metadata was not accepted")
        await anyio.sleep(storage.poll_interval)


async def complete_finite_result(engine: Any, result: Any) -> None:
    entry = finite_run(engine)
    if entry is None:
        return
    entry.receipt = await PrefectStreamStorage(entry.manifest.flow_run_id)._request(
        "complete_stream_finite_task",
        flow_run_id=entry.manifest.flow_run_id,
        task_run_id=entry.manifest.task_run_id,
        operation_id=uuid5(
            NAMESPACE_URL,
            f"prefect-finite-result-v1/{entry.manifest.task_run_id}/{engine.task_run.run_count}",
        ),
        run_count=engine.task_run.run_count,
        result=result,
        metadata_lag=True,
    )


def call_finite_task(
    task: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    return_state: bool,
    wait_for: Any,
) -> Any:
    # Task imports its engines lazily; this inverse boundary must stay lazy too.
    from prefect.task_engine import run_task_async, run_task_sync

    if return_state or wait_for is not None:
        raise StreamProtocolError(
            "Native finite direct calls do not support return_state or wait_for"
        )
    if task.isasync:

        async def call() -> Any:
            entry = await _reserve(task, args, kwargs)
            if entry.receipt is not None:
                return entry.receipt.result
            return await run_task_async(
                task=entry.task,
                task_run=entry.task_run,
                parameters=entry.parameters,
            )

        return call()
    entry = run_coro_as_sync(_reserve(task, args, kwargs))
    if entry.receipt is not None:
        return entry.receipt.result
    return run_task_sync(
        task=entry.task, task_run=entry.task_run, parameters=entry.parameters
    )
