"""Public DAG execution and scoped child boundaries, independent of a database."""

import dataclasses
import hashlib
import inspect
import json
import pickle
import sys
from types import ModuleType, SimpleNamespace
from typing import Any
from uuid import UUID

import anyio
import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prefect._internal.stream_event_auth import (
    canonical_dag_bootstrap_ready_witness,
    dag_worker_generation_digest,
    sign_dag_bootstrap_ready,
    sign_task_event,
    verify_dag_bootstrap_ready,
)
from prefect._internal.stream_execution import (
    DAGSubtaskExecutionContext,
    SubtaskExecutionContext,
    stream_subtask_execution_context,
    stream_subtask_headers,
)
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect.exceptions import CancelledRun
from prefect.streams import RunningStreamGraph, SealedStreamGraph, StreamProtocolError


def ready_material() -> dict[str, Any]:
    return {
        "operation_id": UUID(int=10),
        "flow_run_id": UUID(int=1),
        "flow_run_run_count": 1,
        "flow_generation": 1,
        "flow_holder_id": UUID(int=11),
        "graph_revision_id": UUID(int=2),
        "graph_digest": "a" * 64,
        "routing_digest": "b" * 64,
        "assignment_epoch": 1,
        "subtask_id": UUID(int=3),
        "task_run_id": UUID(int=6),
        "physical_epoch": 1,
        "grant_id": UUID(int=4),
        "holder_id": UUID(int=5),
        "scope_digest": "c" * 64,
        "event_key_id": "d" * 64,
        "child_process_id": 100,
        "process_start_ticks": 200,
        "bootstrap_digest": "e" * 64,
    }


def worker_generation_material() -> dict[str, Any]:
    return dict(
        worker_id=UUID(int=1),
        work_pool_id=UUID(int=2),
        host_id=UUID(int=3),
        boot_id=UUID(int=4),
        launcher_process_id=11,
        launcher_start_ticks=13,
    )


def test_worker_generation_matches_independent_canonical_bytes():
    canonical = (
        b'{"boot_id":"00000000-0000-0000-0000-000000000004",'
        b'"host_id":"00000000-0000-0000-0000-000000000003",'
        b'"launcher_process_id":11,"launcher_start_ticks":13,'
        b'"schema":"prefect-dag-worker-generation-v1",'
        b'"work_pool_id":"00000000-0000-0000-0000-000000000002",'
        b'"worker_id":"00000000-0000-0000-0000-000000000001"}'
    )
    material = worker_generation_material()
    actual = dag_worker_generation_digest(**material)
    assert actual == hashlib.sha256(canonical).hexdigest()
    assert len(actual) == 64 and actual == actual.lower()
    assert set(inspect.signature(dag_worker_generation_digest).parameters) == set(
        material
    )
    assert (
        dag_worker_generation_digest(**dict(reversed(list(material.items())))) == actual
    )


@pytest.mark.parametrize(
    "field",
    [
        "worker_id",
        "work_pool_id",
        "host_id",
        "boot_id",
        "launcher_process_id",
        "launcher_start_ticks",
    ],
)
def test_worker_generation_binds_each_identity_field(field: str):
    original = worker_generation_material()
    changed = dict(original)
    changed[field] = (
        UUID(int=99) if field.endswith("_id") and field != "launcher_process_id" else 99
    )
    assert dag_worker_generation_digest(**changed) != dag_worker_generation_digest(
        **original
    )


@pytest.mark.parametrize("field", ["worker_id", "work_pool_id", "host_id", "boot_id"])
@pytest.mark.parametrize(
    "value",
    [
        None,
        1,
        True,
        b"00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000001",
    ],
)
def test_worker_generation_refuses_coerced_uuid(field: str, value: object):
    with pytest.raises(ValueError, match="exact UUID"):
        dag_worker_generation_digest(**(worker_generation_material() | {field: value}))


@pytest.mark.parametrize("field", ["launcher_process_id", "launcher_start_ticks"])
@pytest.mark.parametrize("value", [None, True, False, 0, -1, 1.0, "1"])
def test_worker_generation_refuses_coerced_process_identity(field: str, value: object):
    with pytest.raises(ValueError, match="exact positive"):
        dag_worker_generation_digest(**(worker_generation_material() | {field: value}))


def test_worker_generation_never_stringifies_hostile_identity():
    class HostileIdentity:
        def __str__(self) -> str:
            raise AssertionError("coercion must not run")

    with pytest.raises(ValueError, match="exact UUID"):
        dag_worker_generation_digest(
            **(worker_generation_material() | {"worker_id": HostileIdentity()})
        )


def test_bootstrap_ready_has_distinct_canonical_domain_and_signature_wire():
    material = ready_material()
    canonical = canonical_dag_bootstrap_ready_witness(**material)
    expected = {"schema": "prefect-dag-bootstrap-ready-v1"} | {
        key: str(value) if type(value) is UUID else value
        for key, value in material.items()
    }
    assert (
        canonical
        == json.dumps(expected, sort_keys=True, separators=(",", ":")).encode()
    )
    private = b"s" * 32
    public = (
        Ed25519PrivateKey.from_private_bytes(private)
        .public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    )
    signature = sign_dag_bootstrap_ready(private, canonical)
    assert len(signature) == 128 and signature == signature.lower()
    assert verify_dag_bootstrap_ready(public, canonical, signature)
    assert not verify_dag_bootstrap_ready(
        public, canonical, sign_task_event(private, canonical)
    )
    assert not verify_dag_bootstrap_ready(public, canonical, signature.upper())
    assert not verify_dag_bootstrap_ready(public, canonical + b" ", signature)


@pytest.mark.parametrize("field", tuple(ready_material()))
def test_ready_signature_binds_every_immutable_witness_field(field: str):
    material = ready_material()
    private = b"s" * 32
    public = (
        Ed25519PrivateKey.from_private_bytes(private)
        .public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    )
    signature = sign_dag_bootstrap_ready(
        private, canonical_dag_bootstrap_ready_witness(**material)
    )
    value = material[field]
    material[field] = (
        UUID(int=99)
        if type(value) is UUID
        else value + 1
        if type(value) is int
        else "f" * 64
    )
    assert not verify_dag_bootstrap_ready(
        public, canonical_dag_bootstrap_ready_witness(**material), signature
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation_id", "00000000-0000-0000-0000-000000000001"),
        ("flow_run_run_count", True),
        ("flow_run_run_count", -1),
        ("flow_generation", 0),
        ("flow_generation", 65),
        ("physical_epoch", True),
        ("assignment_epoch", 0),
        ("child_process_id", 0),
        ("process_start_ticks", 1.0),
        ("bootstrap_digest", "E" * 64),
        ("scope_digest", "bad"),
    ],
)
def test_ready_witness_refuses_coerced_identity_and_noncanonical_digests(
    field: str, value: Any
):
    with pytest.raises(ValueError):
        canonical_dag_bootstrap_ready_witness(**(ready_material() | {field: value}))


@pytest.mark.parametrize("synchronous", [True, False])
async def test_public_handles_delegate_only_explicit_lifecycle_calls(
    monkeypatch: pytest.MonkeyPatch, synchronous: bool
):
    calls = []
    result = SimpleNamespace(phase="TERMINAL", cleanup_complete=True)
    sealed = SealedStreamGraph(graph_revision_id=UUID(int=2), graph_digest="a" * 64)
    running = RunningStreamGraph(
        graph_revision_id=sealed.graph_revision_id,
        graph_digest=sealed.graph_digest,
        routing_digest="b" * 64,
    )
    backend = ModuleType("prefect._internal.stream_dag_execution")

    async def start(handle, *, timeout):
        calls.append(("start", handle, timeout))
        return running

    async def abort(handle):
        calls.append(("abort", handle))
        return result

    async def wait(handle, *, timeout):
        calls.append(("wait", handle, timeout))
        return result

    async def cancel(handle, *, reason):
        calls.append(("cancel", handle, reason))
        raise CancelledRun("Verified graph cancellation")

    backend.start_stream_graph = start
    backend.abort_stream_graph_preview = abort
    backend.wait_stream_graph = wait
    backend.cancel_stream_graph = cancel
    monkeypatch.setitem(sys.modules, backend.__name__, backend)
    assert calls == []
    if synchronous:

        def invoke():
            assert sealed.start(timeout=3) is running
            assert sealed.abort_preview() is result
            assert running.wait(timeout=4) is result
            with pytest.raises(CancelledRun):
                running.cancel(reason="Explicit cancellation")

        await anyio.to_thread.run_sync(invoke)
    else:
        assert await sealed.start(timeout=3) is running
        assert await sealed.abort_preview() is result
        assert await running.wait(timeout=4) is result
        with pytest.raises(CancelledRun):
            await running.cancel(reason="Explicit cancellation")
    assert calls == [
        ("start", sealed, 3),
        ("abort", sealed),
        ("wait", running, 4),
        ("cancel", running, "Explicit cancellation"),
    ]
    assert not hasattr(sealed, "wait")
    assert not hasattr(running, "start")
    assert not hasattr(running, "abort_preview")


async def test_public_cancel_never_silently_returns_on_invalid_backend(
    monkeypatch: pytest.MonkeyPatch,
):
    backend = ModuleType("prefect._internal.stream_dag_execution")

    async def invalid(handle, *, reason):
        return None

    backend.cancel_stream_graph = invalid
    monkeypatch.setitem(sys.modules, backend.__name__, backend)
    running = RunningStreamGraph(
        graph_revision_id=UUID(int=2), graph_digest="a" * 64, routing_digest="b" * 64
    )
    with pytest.raises(StreamProtocolError, match="cancellation signal"):
        await running.cancel()


def child_context(*, dag: bool, source: bool) -> SubtaskExecutionContext:
    context_type = DAGSubtaskExecutionContext if dag else SubtaskExecutionContext
    return context_type(
        flow_run_id=UUID(int=1),
        graph_revision_id=UUID(int=2),
        assignment_epoch=1,
        subtask_id=UUID(int=3),
        grant_id=UUID(int=4),
        physical_epoch=1,
        holder_id=UUID(int=5),
        scope_digest="a" * 64,
        manifest_digest="b" * 64,
        task_run_id=UUID(int=6),
        input_stream_id=None if source else UUID(int=7),
        output_stream_id=UUID(int=8),
        child_process_id=100,
        process_start_ticks=200,
        api_url="http://protocol.test/api",
        capability="c" * 64,
        event_private_key=b"s" * 32,
    )


@pytest.mark.parametrize("source", [True, False])
@pytest.mark.parametrize(
    "method,route,source_allowed,operator_allowed",
    [
        ("GET", "subtasks/{child}/bootstrap", True, True),
        ("GET", "subtasks/{child}/activation", True, True),
        ("GET", "subtasks/{child}/output/status", True, True),
        ("GET", "subtasks/{child}/input/status", False, True),
        ("POST", "subtasks/{child}/bootstrap-ready", True, True),
        ("POST", "sources/open", True, False),
        ("POST", "sources/append", True, False),
        ("POST", "sources/finish", True, False),
        ("POST", "consumers/open", False, True),
        ("POST", "consumers/read", False, True),
        ("POST", "consumers/complete", False, True),
        ("POST", "consumers/finish", False, True),
        ("POST", "{graph}/activate", False, False),
        ("POST", "{graph}/cancel", False, False),
        ("POST", "{graph}/fail", False, False),
        ("GET", "{graph}/execution", False, False),
        ("GET", "subtasks/{sibling}/bootstrap", False, False),
        ("POST", "subtasks/{sibling}/bootstrap-ready", False, False),
    ],
)
def test_dag_child_has_only_own_role_and_bootstrap_routes(
    source: bool,
    method: str,
    route: str,
    source_allowed: bool,
    operator_allowed: bool,
):
    context = child_context(dag=True, source=source)
    path = route.format(
        child=context.subtask_id, sibling=UUID(int=99), graph=context.graph_revision_id
    )
    endpoint = context.api_url + "/streams/v2/dags/" + path
    with stream_subtask_execution_context(context):
        if source_allowed if source else operator_allowed:
            assert stream_subtask_headers(context.api_url, endpoint, method) == {
                "Authorization": "PrefectSubtask " + context.capability,
                "X-Prefect-Subtask-Grant": str(context.grant_id),
            }
        else:
            with pytest.raises(ValueError, match="route"):
                stream_subtask_headers(context.api_url, endpoint, method)


@pytest.mark.parametrize("source", [True, False])
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/streams/v2/subtasks/{child}/bootstrap"),
        ("GET", "/streams/v2/subtasks/{child}/task-run"),
        ("POST", "/streams/v2/subtasks/{child}/lease/renew"),
        ("POST", "/streams/v2/subtasks/{child}/lease/release"),
        ("GET", "/streams/v1/{output}"),
        ("GET", "/streams/v1/{output}/prefix"),
        ("POST", "/streams/v1/sources/claim"),
        ("POST", "/streams/v1/sources/open"),
        ("POST", "/streams/v1/sources/append"),
        ("POST", "/streams/v1/sources/finish"),
        ("POST", "/streams/v1/sources/close"),
        ("POST", "/streams/v1/sources/fail"),
        ("POST", "/streams/v1/sources/reject"),
        ("POST", "/streams/v1/consumers/open"),
        ("POST", "/streams/v1/consumers/read"),
        ("POST", "/streams/v1/consumers/complete"),
        ("POST", "/streams/v1/consumers/finish"),
        ("POST", "/streams/v1/outputs/fail"),
        ("POST", "/streams/v1/outputs/reject"),
        ("POST", "/task_runs/"),
        ("POST", "/logs/"),
        ("POST", "/events"),
        ("GET", "/admin/version"),
    ],
)
def test_dag_scope_refuses_every_legacy_data_lease_and_general_route(
    source: bool, method: str, path: str
):
    context = child_context(dag=True, source=source)
    path = path.format(child=context.subtask_id, output=context.output_stream_id)
    with (
        stream_subtask_execution_context(context),
        pytest.raises(ValueError, match="route"),
    ):
        stream_subtask_headers(context.api_url, context.api_url + path, method)


@pytest.mark.parametrize("path", ["bootstrap", "task-run"])
def test_fixed_profile_keeps_existing_own_metadata_headers(path: str):
    context = child_context(dag=False, source=False)
    url = f"{context.api_url}/streams/v2/subtasks/{context.subtask_id}/{path}"
    with stream_subtask_execution_context(context):
        assert stream_subtask_headers(context.api_url, url, "GET") == {
            "Authorization": "PrefectSubtask " + context.capability,
            "X-Prefect-Subtask-Grant": str(context.grant_id),
        }


@pytest.mark.parametrize("path", ["sources/append", "consumers/complete"])
def test_fixed_profile_keeps_existing_data_routes(path: str):
    context = child_context(dag=False, source=False)
    with stream_subtask_execution_context(context):
        assert (
            stream_subtask_headers(
                context.api_url, context.api_url + "/streams/v1/" + path, "POST"
            )["Authorization"]
            == "PrefectSubtask " + context.capability
        )


@pytest.mark.parametrize("dag", [False, True])
def test_scoped_context_is_frozen_nonserializable_and_secret_redacted(dag: bool):
    context = child_context(dag=dag, source=False)
    assert context.capability not in repr(context)
    assert repr(context.event_private_key) not in repr(context)
    with pytest.raises(dataclasses.FrozenInstanceError):
        context.grant_id = UUID(int=99)
    with pytest.raises(TypeError, match="serialized"):
        pickle.dumps(context)


@pytest.mark.parametrize("synchronous", [True, False])
async def test_dag_transport_strips_broad_credentials_and_denies_before_send(
    synchronous: bool,
):
    context = child_context(dag=True, source=False)
    requests = []

    def handler(request: httpx.Request):
        requests.append(request)
        return httpx.Response(200, json={})

    options = {
        "transport": httpx.MockTransport(handler),
        "headers": {
            "Cookie": "forbidden",
            "X-Prefect-Stream-Execution-Capability": "forbidden",
        },
    }
    own = f"/streams/v2/dags/subtasks/{context.subtask_id}/bootstrap"
    with stream_subtask_execution_context(context):
        if synchronous:
            with SyncPrefectClient(
                context.api_url, api_key="forbidden", httpx_settings=options
            ) as client:
                client._client.get(own)
                with pytest.raises(ValueError, match="route"):
                    client._client.post("/streams/v1/sources/append", json={})
        else:
            async with PrefectClient(
                context.api_url, api_key="forbidden", httpx_settings=options
            ) as client:
                await client._client.get(own)
                with pytest.raises(ValueError, match="route"):
                    await client._client.post("/streams/v1/sources/append", json={})
    assert len(requests) == 1
    headers = requests[0].headers
    assert headers["Authorization"] == "PrefectSubtask " + context.capability
    assert headers["X-Prefect-Subtask-Grant"] == str(context.grant_id)
    assert "Cookie" not in headers
    assert not any(key.startswith("x-prefect-stream-execution-") for key in headers)
