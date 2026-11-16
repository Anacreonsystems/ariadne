"""Separate routed HTTP contracts remain fenced without changing legacy bytes."""

import json
import secrets
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect._internal.stream_route_plan import graph_routing_digest
from prefect.server.api.server import create_api_app, create_app
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.schemas import stream_dags as topology
from prefect.server.schemas import stream_routes as routed
from prefect.server.schemas import stream_subtasks as physical
from prefect.server.schemas import streams as legacy
from prefect.settings import (
    PREFECT_SERVER_API_AUTH_STRING,
    PREFECT_SERVER_CSRF_PROTECTION_ENABLED,
    temporary_settings,
)
from tests.server.api.test_stream_dag_execution_fences import deployed_fixed_graph
from tests.server.api.test_stream_dags import assert_error, deployed_dag_request
from tests.server.api.test_stream_dags import (
    supported_dag_backend as supported_dag_backend,
)

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]
SOURCE_PATH = "/streams/v2/dags/sources/append"
COMPLETE_PATH = "/streams/v2/dags/consumers/complete"


@pytest.fixture
async def routed_api() -> AsyncGenerator[httpx.AsyncClient, None]:
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-dag:test"}):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_api_app(ignore_cache=True)),
            base_url="http://native-api",
            auth=("native-dag", "test"),
        ) as client:
            yield client


def unbound_requests() -> tuple[
    routed.StreamDAGSourceAppendRequest, routed.StreamDAGConsumerCompleteRequest
]:
    flow_id, owner_id = uuid4(), uuid4()
    context = routed.StreamDAGRouteContext(
        graph_revision_id=uuid4(),
        graph_digest="aa" * 32,
        routing_digest=graph_routing_digest("aa" * 32),
        producer_subtask_id=uuid4(),
        operator_uid="A",
        output_port_uid="out",
        subtask_index=0,
        has_keyed_outgoing=False,
    )
    source = legacy.SourceBinding(
        flow_run_id=flow_id,
        stream_id=uuid4(),
        task_run_id=uuid4(),
        owner_id=owner_id,
        attempt=1,
        owner_epoch=1,
    )
    binding = legacy.ConsumerBinding(
        flow_run_id=flow_id,
        consumer_id=uuid4(),
        input_stream_id=uuid4(),
        output_stream_id=uuid4(),
        owner_id=owner_id,
        attempt=1,
        owner_epoch=1,
    )
    token = legacy.DeliveryToken(
        **binding.model_dump(), record_id=uuid4(), sequence=1, delivery_id=uuid4()
    )
    return (
        routed.StreamDAGSourceAppendRequest(
            context=context, binding=source, position=1, value=None
        ),
        routed.StreamDAGConsumerCompleteRequest(
            context=context,
            binding=binding,
            token=token,
            kind="YIELD",
            value=None,
            managed_state={},
        ),
    )


async def route_snapshot(db: PrefectDBInterface) -> dict[str, list[dict[str, Any]]]:
    """Include every native stream table, including new receipts and pins."""
    async with db.session_context() as session:
        snapshot: dict[str, list[dict[str, Any]]] = {}
        for name, table in sorted(orm_models.Base.metadata.tables.items()):
            if name.startswith(("stream_", "native_stream_")) or name in {
                "flow_run",
                "task_run",
                "task_run_state",
                "concurrency_limit_v2",
            }:
                result = await session.execute(
                    sa.select(table).order_by(*table.primary_key.columns)
                )
                snapshot[name] = [dict(row) for row in result.mappings()]
        return snapshot


def request_context(
    plan: topology.StreamDAGPlan, subtask: topology.StreamDAGSubtaskProjection
) -> routed.StreamDAGRouteContext:
    operator = next(row for row in plan.operators if row.id == subtask.operator_run_id)
    return routed.StreamDAGRouteContext(
        graph_revision_id=plan.id,
        graph_digest=plan.graph_digest,
        routing_digest=graph_routing_digest(plan.graph_digest),
        producer_subtask_id=subtask.id,
        operator_uid=operator.operator_uid,
        output_port_uid=operator.output_port.uid,
        subtask_index=subtask.subtask_index,
        has_keyed_outgoing=any(
            edge.exchange == "keyed"
            for edge in plan.edges
            if edge.producer_operator_uid == operator.operator_uid
        ),
    )


def plan_requests(
    plan: topology.StreamDAGPlan,
) -> tuple[
    routed.StreamDAGSourceAppendRequest, routed.StreamDAGConsumerCompleteRequest
]:
    source = next(row for row in plan.subtasks if row.manifest.role == "SOURCE")
    consumer = next(row for row in plan.subtasks if row.manifest.role == "OPERATOR")
    owner_id = uuid4()
    source_binding = legacy.SourceBinding(
        flow_run_id=plan.flow_run_id,
        stream_id=source.output_stream_id,
        task_run_id=source.manifest.task_run_id,
        owner_id=owner_id,
        attempt=1,
        owner_epoch=1,
    )
    partition = next(
        part
        for edge in plan.edges
        for part in edge.partitions
        if part.target_subtask_id == consumer.id
    )
    assert consumer.input_stream_id is not None
    binding = legacy.ConsumerBinding(
        flow_run_id=plan.flow_run_id,
        consumer_id=partition.subscriber_id,
        input_stream_id=consumer.input_stream_id,
        output_stream_id=consumer.output_stream_id,
        owner_id=owner_id,
        attempt=1,
        owner_epoch=1,
    )
    token = legacy.DeliveryToken(
        **binding.model_dump(), record_id=uuid4(), sequence=1, delivery_id=uuid4()
    )
    return (
        routed.StreamDAGSourceAppendRequest(
            context=request_context(plan, source),
            binding=source_binding,
            position=1,
            value={"ref": "object:document-200"},
        ),
        routed.StreamDAGConsumerCompleteRequest(
            context=request_context(plan, consumer),
            binding=binding,
            token=token,
            kind="YIELD",
            value=None,
            managed_state={"processed": 1},
        ),
    )


async def public_plan(
    db: PrefectDBInterface,
    session: AsyncSession,
    api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    *,
    keyed: bool = False,
) -> tuple[topology.StreamDAGPlan, dict[str, str]]:
    request, headers = await deployed_dag_request(db, session, api, deployment)
    body = request.model_dump(mode="json")
    if keyed:
        body["edges"][1].update(
            exchange="keyed",
            key_codec="tagged-key-v1",
            hash_algorithm="sha256",
            key_group_count=16,
        )
    valid = topology.StreamDAGSealRequest.model_validate(body)
    sealed = await api.post(
        "/streams/v2/dags/seal", json=valid.model_dump(mode="json"), headers=headers
    )
    assert sealed.status_code == 200, sealed.text
    return topology.StreamDAGPlan.model_validate(sealed.json()), headers


@pytest.mark.parametrize(
    "authorization", [None, "Basic d3Jvbmc6d3Jvbmc=", "PrefectSubtask " + "ab" * 32]
)
async def test_routed_terminals_require_platform_auth_and_refuse_child_login(
    routed_api: httpx.AsyncClient, authorization: str | None
):
    headers = {} if authorization is None else {"Authorization": authorization}
    for path, request in zip((SOURCE_PATH, COMPLETE_PATH), unbound_requests()):
        response = await routed_api.post(
            path, json=request.model_dump(mode="json"), headers=headers, auth=None
        )
        assert response.status_code == 401, response.text
        assert "ab" * 32 not in response.text


@pytest.mark.parametrize(
    "key",
    [
        {"kind": "B", "value": 1},
        {"kind": "I", "value": "01"},
        {"kind": "I", "value": "-0"},
        {"kind": "I", "value": "9223372036854775808"},
        {"kind": "I", "value": 1},
        {"kind": "S", "value": "\ud800"},
        {"kind": "S", "value": "я" * 2046},
        {"kind": "Y", "value": "YQ"},
        {"kind": "Y", "value": "YR=="},
        {"kind": "Y", "value": "YQ==\n"},
        {"kind": "N", "value": None},
    ],
)
async def test_routed_key_validation_is_strict_and_redacts_request_input(
    routed_api: httpx.AsyncClient, key: dict[str, Any]
):
    secret = secrets.token_hex(32)
    for path, request in zip((SOURCE_PATH, COMPLETE_PATH), unbound_requests()):
        body = request.model_dump(mode="json")
        body["context"]["has_keyed_outgoing"] = True
        body["output_key"] = key
        body["value"] = {"secret": secret}
        response = await routed_api.post(
            path,
            content=json.dumps(body, ensure_ascii=True),
            headers={"Content-Type": "application/json"},
        )
        assert_error(response, 422, "PROTOCOL_ERROR")
        assert secret not in response.text


@pytest.mark.parametrize("mode", ["partial", "generation", "bypass"])
async def test_routed_headers_and_extra_execution_flags_are_redacted(
    routed_api: httpx.AsyncClient, mode: str
):
    secret = secrets.token_hex(32)
    for path, request in zip((SOURCE_PATH, COMPLETE_PATH), unbound_requests()):
        body = request.model_dump(mode="json")
        headers = {"X-Prefect-Stream-Execution-Capability": secret}
        if mode == "generation":
            headers |= {
                "X-Prefect-Stream-Execution-Generation": secret,
                "X-Prefect-Stream-Execution-Holder": str(uuid4()),
            }
        elif mode == "bypass":
            headers = {}
            body["allow_execution"] = secret
        response = await routed_api.post(path, json=body, headers=headers)
        assert_error(response, 422, "PROTOCOL_ERROR")
        assert secret not in response.text


async def test_routed_terminals_keep_ordinary_csrf_protection():
    with temporary_settings(
        {
            PREFECT_SERVER_API_AUTH_STRING: "native-dag:test",
            PREFECT_SERVER_CSRF_PROTECTION_ENABLED: True,
        }
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=create_app(ephemeral=True, ignore_cache=True)
            ),
            base_url="http://native-api/api",
            auth=("native-dag", "test"),
        ) as client:
            for path, request in zip((SOURCE_PATH, COMPLETE_PATH), unbound_requests()):
                response = await client.post(path, json=request.model_dump(mode="json"))
                assert response.status_code == 403, response.text
                assert response.json()["detail"] == "Missing CSRF token."


def test_legacy_terminal_default_body_bytes_remain_unchanged():
    source, consumer = unbound_requests()
    old_source = legacy.StreamSourceAppendRequest(
        binding=source.binding, position=1, value={"kind": "N"}
    )
    old_consumer = legacy.StreamConsumerCompleteRequest(
        binding=consumer.binding,
        token=consumer.token,
        kind="YIELD",
        value=None,
        managed_state={},
    )
    assert old_source.model_dump() == {
        "binding": source.binding.model_dump(),
        "position": 1,
        "value": {"kind": "N"},
    }
    assert old_consumer.model_dump() == {
        "binding": consumer.binding.model_dump(),
        "token": consumer.token.model_dump(),
        "kind": "YIELD",
        "value": None,
        "managed_state": {},
    }
    assert old_source.model_dump(mode="json") == {
        "binding": source.binding.model_dump(mode="json"),
        "position": 1,
        "value": {"kind": "N"},
    }
    assert old_consumer.model_dump(mode="json") == {
        "binding": consumer.binding.model_dump(mode="json"),
        "token": consumer.token.model_dump(mode="json"),
        "kind": "YIELD",
        "value": None,
        "managed_state": {},
    }


async def test_postgres_valid_routed_requests_are_fenced_before_any_mutation(
    db: PrefectDBInterface,
    session: AsyncSession,
    routed_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await public_plan(db, session, routed_api, deployment)
    baseline = await route_snapshot(db)
    for path, request in zip((SOURCE_PATH, COMPLETE_PATH), plan_requests(plan)):
        for _ in range(2):
            response = await routed_api.post(
                path, json=request.model_dump(mode="json"), headers=headers
            )
            assert_error(response, 409, "EXECUTION_PROFILE_NOT_READY")
            assert await route_snapshot(db) == baseline
        unkeyed_body = request.model_dump(mode="json")
        unkeyed_body.pop("output_key")
        response = await routed_api.post(path, json=unkeyed_body, headers=headers)
        assert_error(response, 409, "EXECUTION_PROFILE_NOT_READY")
        assert await route_snapshot(db) == baseline
    request = plan_requests(plan)[1]
    returned = request.model_copy(update={"kind": "RETURN", "value": None})
    response = await routed_api.post(
        COMPLETE_PATH, json=returned.model_dump(mode="json"), headers=headers
    )
    assert_error(response, 409, "EXECUTION_PROFILE_NOT_READY")
    assert await route_snapshot(db) == baseline


async def test_postgres_every_canonical_key_reaches_the_execution_fence(
    db: PrefectDBInterface,
    session: AsyncSession,
    routed_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await public_plan(db, session, routed_api, deployment, keyed=True)
    source = next(row for row in plan.subtasks if row.manifest.role == "SOURCE")
    binding = legacy.SourceBinding(
        flow_run_id=plan.flow_run_id,
        stream_id=source.output_stream_id,
        task_run_id=source.manifest.task_run_id,
        owner_id=uuid4(),
        attempt=1,
        owner_epoch=1,
    )
    baseline = await route_snapshot(db)
    for key in (
        {"kind": "N"},
        {"kind": "B", "value": False},
        {"kind": "I", "value": "-9223372036854775808"},
        {"kind": "S", "value": "é"},
        {"kind": "Y", "value": "AAE="},
    ):
        request = routed.StreamDAGSourceAppendRequest.model_validate(
            {
                "context": request_context(plan, source).model_dump(),
                "binding": binding.model_dump(),
                "position": 1,
                "value": None,
                "output_key": key,
            }
        )
        response = await routed_api.post(
            SOURCE_PATH, json=request.model_dump(mode="json"), headers=headers
        )
        assert_error(response, 409, "EXECUTION_PROFILE_NOT_READY")
        assert await route_snapshot(db) == baseline


async def test_postgres_routed_authentication_and_context_scope_precede_the_fence(
    db: PrefectDBInterface,
    session: AsyncSession,
    routed_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await public_plan(db, session, routed_api, deployment)
    _, foreign = await deployed_dag_request(db, session, routed_api, deployment)
    baseline = await route_snapshot(db)
    for path, request in zip((SOURCE_PATH, COMPLETE_PATH), plan_requests(plan)):
        for auth_headers, code in (
            ({}, "EXECUTION_BINDING_REQUIRED"),
            (
                headers | {"X-Prefect-Stream-Execution-Capability": "00" * 32},
                "STALE_EXECUTION",
            ),
            (
                headers | {"X-Prefect-Stream-Execution-Generation": "2"},
                "STALE_EXECUTION",
            ),
            (foreign, "STALE_EXECUTION"),
        ):
            response = await routed_api.post(
                path, json=request.model_dump(mode="json"), headers=auth_headers
            )
            assert_error(response, 409, code)
            assert await route_snapshot(db) == baseline
        for field, value in (
            ("graph_digest", "00" * 32),
            ("routing_digest", "00" * 32),
            ("producer_subtask_id", str(uuid4())),
            ("output_port_uid", "other-port"),
            ("operator_uid", "foreign-operator"),
            ("subtask_index", 15),
        ):
            body = request.model_dump(mode="json")
            body["context"][field] = value
            response = await routed_api.post(path, json=body, headers=headers)
            assert_error(response, 409, "IDENTITY_CONFLICT")
            assert await route_snapshot(db) == baseline
        binding_fields = (
            ("task_run_id", "stream_id")
            if path == SOURCE_PATH
            else ("consumer_id", "input_stream_id", "output_stream_id")
        )
        for field in binding_fields:
            body = request.model_dump(mode="json")
            body["binding"][field] = str(uuid4())
            if "token" in body:
                body["token"][field] = body["binding"][field]
            response = await routed_api.post(path, json=body, headers=headers)
            assert_error(response, 409, "IDENTITY_CONFLICT")
            assert await route_snapshot(db) == baseline
        body = request.model_dump(mode="json")
        body["binding"]["flow_run_id"] = str(uuid4())
        if "token" in body:
            body["token"]["flow_run_id"] = body["binding"]["flow_run_id"]
        response = await routed_api.post(path, json=body, headers=headers)
        assert_error(response, 409, "IDENTITY_CONFLICT")
        assert await route_snapshot(db) == baseline


async def test_postgres_fixed_profile_cannot_enter_routed_dag_endpoints(
    db: PrefectDBInterface,
    session: AsyncSession,
    routed_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, _, headers = await deployed_fixed_graph(db, session, routed_api, deployment)
    request = unbound_requests()[0].model_dump(mode="json")
    request["context"].update(
        graph_revision_id=str(plan.id),
        graph_digest=plan.graph_digest,
        routing_digest=graph_routing_digest(plan.graph_digest),
    )
    request["binding"]["flow_run_id"] = str(plan.flow_run_id)
    baseline = await route_snapshot(db)
    response = await routed_api.post(SOURCE_PATH, json=request, headers=headers)
    assert_error(response, 422, "UNSUPPORTED_PROFILE")
    assert await route_snapshot(db) == baseline


async def test_postgres_current_fixed_child_cannot_use_routed_dag_allowlist(
    db: PrefectDBInterface,
    session: AsyncSession,
    routed_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, workers, execution_headers = await deployed_fixed_graph(
        db, session, routed_api, deployment
    )
    admitted = await routed_api.post(
        f"/streams/v2/graphs/{plan.id}/admit",
        json={"operation_id": str(uuid4()), "graph_digest": plan.graph_digest},
        headers=execution_headers,
    )
    assert admitted.status_code == 200, admitted.text
    assigned = physical.StreamGraphPlan.model_validate(admitted.json())
    principals = dict(workers)
    grants: dict[UUID, UUID] = {}
    child_headers: dict[str, str] = {}
    for index, subtask in enumerate(assigned.subtasks):
        worker_id = subtask.assigned_worker_id
        assert worker_id is not None
        principal = principals[worker_id]
        capability = secrets.token_hex(32)
        claim = physical.StreamSubtaskClaimRequest(
            operation_id=uuid4(),
            graph_digest=plan.graph_digest,
            holder_id=uuid4(),
            capability=capability,
            event_public_key="aa" * 32,
            launcher_binding_kind="ASSIGNED_WORKER",
            worker_id=worker_id,
            **principal.model_dump(),
        )
        claimed = await routed_api.post(
            f"/streams/v2/workers/{worker_id}/subtasks/{subtask.id}/claim",
            json=claim.model_dump(mode="json"),
        )
        assert claimed.status_code == 200, claimed.text
        grant = physical.StreamSubtaskGrant.model_validate(claimed.json())
        registration = physical.StreamWorkerRegistrationRequest(
            principal=principal,
            registration=physical.StreamSubtaskRegistrationRequest(
                operation_id=uuid4(),
                grant_id=grant.id,
                child_process_id=10000 + index,
                process_start_ticks=20000 + index,
            ),
        )
        registered = await routed_api.post(
            f"/streams/v2/workers/{worker_id}/subtasks/{subtask.id}/register",
            json=registration.model_dump(mode="json"),
        )
        assert registered.status_code == 200, registered.text
        grants[subtask.id] = grant.id
        child_headers = {
            "Authorization": f"PrefectSubtask {capability}",
            "X-Prefect-Subtask-Grant": str(grant.id),
        }
    activated = await routed_api.post(
        f"/streams/v2/graphs/{plan.id}/activate",
        json=physical.StreamGraphActivateRequest(
            operation_id=uuid4(),
            graph_digest=plan.graph_digest,
            grant_ids=[grants[row.id] for row in assigned.subtasks],
        ).model_dump(mode="json"),
        headers=execution_headers,
    )
    assert activated.status_code == 200, activated.text
    # Authentication is a current REGISTERED fixed-P2 child, so an invalid
    # credential cannot accidentally mask a broadened route allowlist.
    baseline = await route_snapshot(db)
    for path, request in zip((SOURCE_PATH, COMPLETE_PATH), unbound_requests()):
        response = await routed_api.post(
            path, json=request.model_dump(mode="json"), headers=child_headers, auth=None
        )
        assert response.status_code == 401, response.text
        assert child_headers["Authorization"].split()[1] not in response.text
        assert await route_snapshot(db) == baseline


async def test_sqlite_routed_terminals_refuse_before_lookup_or_mutation(
    db: PrefectDBInterface, routed_api: httpx.AsyncClient
):
    assert db.dialect.name == "sqlite"
    baseline = await route_snapshot(db)
    for path, request in zip((SOURCE_PATH, COMPLETE_PATH), unbound_requests()):
        response = await routed_api.post(path, json=request.model_dump(mode="json"))
        assert_error(response, 422, "UNSUPPORTED_BACKEND")
        assert await route_snapshot(db) == baseline


async def test_postgres_legacy_terminal_replay_conflict_null_and_return_stay_unchanged(
    db: PrefectDBInterface,
    routed_api: httpx.AsyncClient,
    flow_run: orm_models.FlowRun,
    task_run: orm_models.TaskRun,
):
    running = {"state": {"type": "RUNNING", "name": "Running"}}
    for path in (
        f"/flow_runs/{flow_run.id}/set_state",
        f"/task_runs/{task_run.id}/set_state",
    ):
        response = await routed_api.post(path, json=running)
        assert response.status_code == 201, response.text
    downstream = await routed_api.post(
        "/task_runs/",
        json={
            "flow_run_id": str(flow_run.id),
            "task_key": "legacy-signer-" + uuid4().hex,
            "dynamic_key": "0",
            "state": running["state"],
        },
    )
    assert downstream.status_code == 201, downstream.text
    input_id, output_id = uuid4(), uuid4()
    created = await routed_api.post(
        "/streams/v1/create",
        json={"flow_run_id": str(flow_run.id), "stream_id": str(input_id)},
    )
    assert created.status_code == 200, created.text
    reserved = await routed_api.post(
        "/streams/v1/outputs/reserve",
        json={
            "flow_run_id": str(flow_run.id),
            "stream_id": str(output_id),
            "task_run_id": downstream.json()["id"],
        },
    )
    assert reserved.status_code == 200, reserved.text
    claimed = await routed_api.post(
        "/streams/v1/sources/claim",
        json={
            "stream_id": str(input_id),
            "task_run_id": str(task_run.id),
            "owner_id": str(uuid4()),
            "attempt": 1,
        },
    )
    assert claimed.status_code == 200, claimed.text
    source_binding = claimed.json()
    source_body = {"binding": source_binding, "position": 1, "value": {"kind": "N"}}
    first = await routed_api.post("/streams/v1/sources/append", json=source_body)
    assert first.status_code == 200, first.text
    assert set(first.json()) == {
        "id",
        "stream_id",
        "sequence",
        "producer_position",
        "value",
        "value_digest",
        "size_bytes",
    }
    accepted_source = await route_snapshot(db)
    replay = await routed_api.post("/streams/v1/sources/append", json=source_body)
    assert replay.status_code == 200 and replay.json() == first.json()
    conflict = await routed_api.post(
        "/streams/v1/sources/append",
        json=source_body | {"value": {"kind": "I", "value": "1"}},
    )
    assert_error(conflict, 409, "IDENTITY_CONFLICT")
    assert await route_snapshot(db) == accepted_source
    opened = await routed_api.post(
        "/streams/v1/consumers/open",
        json={
            "input_stream_id": str(input_id),
            "output_stream_id": str(output_id),
            "task_run_id": downstream.json()["id"],
            "owner_id": str(uuid4()),
            "attempt": 1,
        },
    )
    assert opened.status_code == 200, opened.text
    consumer_binding = opened.json()
    delivery = await routed_api.post(
        "/streams/v1/consumers/read", json={"binding": consumer_binding}
    )
    assert delivery.status_code == 200, delivery.text
    assert delivery.json()["value"] == {"kind": "N"}
    terminal = {
        "binding": consumer_binding,
        "token": delivery.json()["token"],
        "kind": "YIELD",
        "value": None,
        "managed_state": {"count": 1},
    }
    completed = await routed_api.post("/streams/v1/consumers/complete", json=terminal)
    assert completed.status_code == 200, completed.text
    assert completed.json() == {"input_sequence": 1, "output_sequence": 1}
    accepted_terminal = await route_snapshot(db)
    replay = await routed_api.post("/streams/v1/consumers/complete", json=terminal)
    assert replay.status_code == 200 and replay.json() == completed.json()
    conflict = await routed_api.post(
        "/streams/v1/consumers/complete",
        json=terminal | {"managed_state": {"count": 2}},
    )
    assert_error(conflict, 409, "IDENTITY_CONFLICT")
    assert await route_snapshot(db) == accepted_terminal
    second = await routed_api.post(
        "/streams/v1/sources/append",
        json={"binding": source_binding, "position": 2, "value": {"ref": "object:2"}},
    )
    assert second.status_code == 200, second.text
    closed = await routed_api.post(
        "/streams/v1/sources/close",
        json={"binding": source_binding, "expected_position": 2},
    )
    assert closed.status_code == 204, closed.text
    second_delivery = await routed_api.post(
        "/streams/v1/consumers/read", json={"binding": consumer_binding}
    )
    assert second_delivery.status_code == 200, second_delivery.text
    returned = await routed_api.post(
        "/streams/v1/consumers/complete",
        json={
            "binding": consumer_binding,
            "token": second_delivery.json()["token"],
            "kind": "RETURN",
            "value": None,
            "managed_state": {"count": 2},
        },
    )
    assert returned.status_code == 200, returned.text
    assert returned.json() == {"input_sequence": 2, "output_sequence": None}
    finished = await routed_api.post(
        "/streams/v1/consumers/finish", json={"binding": consumer_binding}
    )
    assert finished.status_code == 204, finished.text
    output = await routed_api.get(f"/streams/v1/{output_id}")
    assert output.status_code == 200, output.text
    assert output.json()["state"] == "COMPLETED" and output.json()["last_sequence"] == 1
    prefix = await routed_api.get(
        f"/streams/v1/{output_id}/prefix", params={"limit": 10}
    )
    assert prefix.status_code == 200 and [row["value"] for row in prefix.json()] == [
        None
    ]
