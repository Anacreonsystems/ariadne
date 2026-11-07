"""Protected DAG lifecycle and exact child HTTP boundaries on real databases."""

import secrets
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4, uuid5

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect._internal.stream_route_plan import graph_routing_digest
from prefect.server.api._stream_subtask_routes import subtask_route_allowed
from prefect.server.api.server import create_api_app, create_app
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.schemas import stream_dag_executions as s
from prefect.server.schemas import stream_dags as topology
from prefect.server.schemas import stream_subtasks as physical
from prefect.settings import (
    PREFECT_SERVER_API_AUTH_STRING,
    PREFECT_SERVER_CSRF_PROTECTION_ENABLED,
    temporary_settings,
)
from tests.server.api.test_native_deployments import _enrollment_body
from tests.server.api.test_stream_dags import assert_error, deployed_dag_request
from tests.server.api.test_stream_routes import route_snapshot, unbound_requests

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]
PARENT_ACTIONS = ("start", "abort-preview", "cancel", "activate", "fail")


@pytest.fixture
async def execution_api() -> AsyncGenerator[httpx.AsyncClient, None]:
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-dag:test"}):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_api_app(ignore_cache=True)),
            base_url="http://native-api",
            auth=("native-dag", "test"),
        ) as api:
            yield api


def parent_body(graph_id: UUID, action: str, graph_digest: str) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation_id": str(uuid5(graph_id, f"prefect-stream-dag-{action}-v1")),
        "graph_digest": graph_digest,
        "routing_digest": graph_routing_digest(graph_digest),
    }
    if action == "cancel":
        body["reason"] = "Cancellation requested for stream graph"
    elif action == "fail":
        body["failure_kind"] = "START_EXCEPTION"
    return body


def child_calls() -> list[tuple[str, str, dict[str, Any] | None]]:
    source, consumer = unbound_requests()
    context = source.context.model_dump(mode="json")
    subtask_id = source.context.producer_subtask_id
    root = f"/streams/v2/dags/subtasks/{subtask_id}"
    return [
        ("GET", root + "/bootstrap", None),
        (
            "POST",
            root + "/bootstrap-ready",
            {
                "operation_id": str(uuid4()),
                "bootstrap_digest": "aa" * 32,
                "signature": "bb" * 64,
            },
        ),
        ("GET", root + "/activation", None),
        ("POST", "/streams/v2/dags/sources/open", {"context": context}),
        (
            "POST",
            "/streams/v2/dags/consumers/open",
            {"context": consumer.context.model_dump(mode="json")},
        ),
        (
            "POST",
            "/streams/v2/dags/consumers/read",
            {
                "context": consumer.context.model_dump(mode="json"),
                "binding": consumer.binding.model_dump(mode="json"),
            },
        ),
        ("GET", root + "/input/status", None),
        ("GET", root + "/output/status", None),
        (
            "POST",
            "/streams/v2/dags/sources/finish",
            {
                "operation_id": str(
                    uuid5(subtask_id, "prefect-stream-dag-source-finish-v1")
                ),
                "context": context,
                "binding": source.binding.model_dump(mode="json"),
                "expected_position": 0,
            },
        ),
        (
            "POST",
            "/streams/v2/dags/consumers/finish",
            {
                "operation_id": str(
                    uuid5(
                        consumer.context.producer_subtask_id,
                        "prefect-stream-dag-consumer-finish-v1",
                    )
                ),
                "context": consumer.context.model_dump(mode="json"),
                "binding": consumer.binding.model_dump(mode="json"),
            },
        ),
    ]


async def sealed_execution_graph(
    db: PrefectDBInterface,
    session: AsyncSession,
    api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
) -> tuple[topology.StreamDAGPlan, dict[str, str]]:
    request, headers = await deployed_dag_request(db, session, api, deployment)
    response = await api.post(
        "/streams/v2/dags/seal", json=request.model_dump(mode="json"), headers=headers
    )
    assert response.status_code == 200, response.text
    return topology.StreamDAGPlan.model_validate(response.json()), headers


async def advertise_graph_workers(
    db: PrefectDBInterface,
    session: AsyncSession,
    api: httpx.AsyncClient,
    flow_run_id: UUID,
) -> list[tuple[UUID, physical.StreamWorkerPrincipal]]:
    """Advertise the actual enrolled worker and a same-host placement peer."""
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity).where(
            db.NativeStreamLaunchIdentity.flow_run_id == flow_run_id
        )
    )
    assert launch is not None
    pool = await session.get(db.WorkPool, launch.work_pool_id)
    assert pool is not None
    identity = physical.StreamWorkerPrincipal(
        work_pool_id=pool.id,
        host_id=launch.launcher_host_id,
        boot_id=launch.launcher_boot_id,
        launcher_process_id=launch.launcher_process_id,
        launcher_start_ticks=launch.launcher_start_ticks,
    )
    peer = await api.post(
        f"/work_pools/{pool.name}/workers/heartbeat",
        json={"name": "dag-peer-" + uuid4().hex, "return_id": True},
    )
    assert peer.status_code == 200, peer.text
    workers = [
        (launch.worker_id, identity),
        (
            UUID(peer.text),
            identity.model_copy(
                update={"launcher_process_id": 101, "launcher_start_ticks": 1001}
            ),
        ),
    ]
    await session.commit()
    for worker_id, principal in workers:
        response = await api.post(
            f"/streams/v2/dags/workers/{worker_id}/poll",
            json=s.StreamDAGWorkerPollRequest(principal=principal).model_dump(
                mode="json"
            ),
        )
        assert response.status_code == 200, response.text
        assert response.json() == []
    return workers


@pytest.mark.parametrize(
    "authorization", [None, "Basic d3Jvbmc6d3Jvbmc=", "PrefectSubtask " + "ab" * 32]
)
async def test_parent_routes_require_platform_login(
    execution_api: httpx.AsyncClient, authorization: str | None
):
    graph_id = uuid4()
    headers = {} if authorization is None else {"Authorization": authorization}
    for action in PARENT_ACTIONS:
        response = await execution_api.post(
            f"/streams/v2/dags/{graph_id}/{action}",
            json=parent_body(graph_id, action, "aa" * 32),
            headers=headers,
            auth=None,
        )
        assert response.status_code == 401, response.text
        assert "ab" * 32 not in response.text
    observed = await execution_api.get(
        f"/streams/v2/dags/{graph_id}/execution", headers=headers, auth=None
    )
    assert observed.status_code == 401, observed.text


@pytest.mark.parametrize("malformed", ["body", "headers"])
async def test_lifecycle_validation_redacts_inputs(
    execution_api: httpx.AsyncClient, malformed: str
):
    graph_id = uuid4()
    secret = secrets.token_hex(32)
    for action in PARENT_ACTIONS:
        body = parent_body(graph_id, action, "aa" * 32)
        headers = {}
        if malformed == "body":
            body["targets"] = secret
        else:
            headers = {"X-Prefect-Stream-Execution-Capability": secret}
        response = await execution_api.post(
            f"/streams/v2/dags/{graph_id}/{action}", json=body, headers=headers
        )
        assert_error(response, 422, "PROTOCOL_ERROR")
        assert secret not in response.text


async def test_child_routes_refuse_basic_and_mixed_flow_credentials(
    execution_api: httpx.AsyncClient,
):
    secret = secrets.token_hex(32)
    for method, path, body in child_calls():
        basic = await execution_api.request(method, path, json=body)
        assert basic.status_code == 401, basic.text
        mixed = await execution_api.request(
            method,
            path,
            json=body,
            auth=None,
            headers={
                "Authorization": "PrefectSubtask " + secret,
                "X-Prefect-Subtask-Grant": str(uuid4()),
                "X-Prefect-Stream-Execution-Capability": secret,
            },
        )
        assert mixed.status_code == 401, mixed.text
        assert secret not in mixed.text


def test_child_route_allowlist_has_exact_methods_and_boundaries():
    calls = child_calls()
    calls.extend(
        [
            ("POST", "/streams/v2/dags/sources/append", None),
            ("POST", "/streams/v2/dags/consumers/complete", None),
        ]
    )
    for method, path, _ in calls:
        assert subtask_route_allowed(method, path)
        assert not subtask_route_allowed("DELETE", path)
        assert not subtask_route_allowed(method, path + "/extra")
    for suffix in ("start", "activate", "cancel", "fail", "wait", "admit"):
        assert not subtask_route_allowed("POST", f"/streams/v2/dags/{uuid4()}/{suffix}")
    assert not subtask_route_allowed("POST", f"/streams/v2/dags/workers/{uuid4()}/poll")


async def test_parent_mutations_keep_csrf_protection():
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
        ) as api:
            graph_id = uuid4()
            for action in PARENT_ACTIONS:
                response = await api.post(
                    f"/streams/v2/dags/{graph_id}/{action}",
                    json=parent_body(graph_id, action, "aa" * 32),
                )
                assert response.status_code == 403, response.text
                assert response.json()["detail"] == "Missing CSRF token."


async def test_preview_read_abort_and_terminal_replay_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await sealed_execution_graph(db, session, execution_api, deployment)
    before = await route_snapshot(db)
    read = await execution_api.get(
        f"/streams/v2/dags/{plan.id}/execution", headers=headers
    )
    assert read.status_code == 200, read.text
    observed = s.StreamDAGExecution.model_validate(read.json())
    assert observed.phase == "PREVIEW" and observed.counts.total == 4
    assert observed.counts.physical_slots_remaining == 0
    assert await route_snapshot(db) == before
    body = parent_body(plan.id, "abort-preview", plan.graph_digest)
    path = f"/streams/v2/dags/{plan.id}/abort-preview"
    aborted = await execution_api.post(path, json=body, headers=headers)
    assert aborted.status_code == 200, aborted.text
    outcome = s.StreamDAGOperationResult.model_validate(aborted.json())
    assert outcome.receipt.kind == "ABORT_PREVIEW"
    assert outcome.execution.phase == "TERMINAL"
    assert outcome.execution.terminal_state == "FAILED"
    assert outcome.execution.cleanup_complete
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, plan.flow_run_id)
        assert flow is not None and flow.state_type.value == "RUNNING"
    final = await route_snapshot(db)
    replay = await execution_api.post(path, json=body, headers=headers)
    assert replay.status_code == 200, replay.text
    assert replay.json() == aborted.json()
    read_terminal = await execution_api.get(
        f"/streams/v2/dags/{plan.id}/execution", headers=headers
    )
    assert read_terminal.status_code == 200, read_terminal.text
    assert read_terminal.json() == aborted.json()["execution"]
    forged_headers = {
        **headers,
        "X-Prefect-Stream-Execution-Capability": secrets.token_hex(32),
    }
    forged_replay = await execution_api.post(path, json=body, headers=forged_headers)
    assert_error(forged_replay, 409, "STALE_EXECUTION")
    late_cancel = await execution_api.post(
        f"/streams/v2/dags/{plan.id}/cancel",
        json=parent_body(plan.id, "cancel", plan.graph_digest),
        headers=headers,
    )
    assert_error(late_cancel, 409, "TERMINAL_CONFLICT")
    assert await route_snapshot(db) == final


@pytest.mark.parametrize("scope", ["missing", "forged", "stale", "foreign"])
async def test_lifecycle_authority_is_checked_before_mutation_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    scope: str,
):
    plan, headers = await sealed_execution_graph(db, session, execution_api, deployment)
    supplied = headers.copy()
    if scope == "missing":
        supplied = {}
    elif scope == "forged":
        supplied["X-Prefect-Stream-Execution-Capability"] = secrets.token_hex(32)
    elif scope == "stale":
        supplied["X-Prefect-Stream-Execution-Generation"] = "2"
    else:
        _, supplied = await deployed_dag_request(db, session, execution_api, deployment)
    before = await route_snapshot(db)
    read = await execution_api.get(
        f"/streams/v2/dags/{plan.id}/execution", headers=supplied
    )
    assert_error(
        read,
        409,
        "EXECUTION_BINDING_REQUIRED" if scope == "missing" else "STALE_EXECUTION",
    )
    assert await route_snapshot(db) == before
    for action in PARENT_ACTIONS:
        response = await execution_api.post(
            f"/streams/v2/dags/{plan.id}/{action}",
            json=parent_body(plan.id, action, plan.graph_digest),
            headers=supplied,
        )
        assert_error(
            response,
            409,
            "EXECUTION_BINDING_REQUIRED" if scope == "missing" else "STALE_EXECUTION",
        )
        assert await route_snapshot(db) == before


async def test_retryable_deployed_flow_cannot_seal_dag_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    template, _ = await deployed_dag_request(db, session, execution_api, deployment)
    first_launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity).where(
            db.NativeStreamLaunchIdentity.flow_run_id == template.flow_run_id
        )
    )
    assert first_launch is not None
    declaration = dict(deployment.job_variables["prefect_native_streaming"])
    declaration["flow_retries"] = 2
    deployment.job_variables = {"prefect_native_streaming": declaration}
    await session.commit()
    created = await execution_api.post(
        f"/deployments/{deployment.id}/create_flow_run", json={}
    )
    assert created.status_code == 201, created.text
    flow_id = UUID(created.json()["id"])
    plan = await execution_api.get(f"/streams/v1/deployments/{flow_id}/plan")
    assert plan.status_code == 200, plan.text
    enrollment = _enrollment_body(
        flow_id,
        first_launch.worker_id,
        first_launch.work_pool_id,
        plan.json()["declaration_digest"],
    )
    enrollment.update(
        launcher_host_id=str(first_launch.launcher_host_id),
        launcher_boot_id=str(first_launch.launcher_boot_id),
        launcher_process_id=first_launch.launcher_process_id,
        launcher_start_ticks=first_launch.launcher_start_ticks,
    )
    enrolled = await execution_api.post(
        "/streams/v1/deployments/enroll", json=enrollment
    )
    assert enrolled.status_code == 200, enrolled.text
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": enrollment["holder_id"],
        "X-Prefect-Stream-Execution-Capability": enrollment["holder_capability"],
    }
    registered = await execution_api.post(
        "/streams/v1/deployments/launches/register-process",
        json={
            "flow_run_id": str(flow_id),
            "operation_id": str(uuid4()),
            "enrollment_operation_id": enrollment["operation_id"],
            "generation": 1,
            "launcher_instance_id": enrollment["launcher_instance_id"],
            "host_id": str(first_launch.launcher_host_id),
            "boot_id": str(first_launch.launcher_boot_id),
            "child_process_id": 4322,
            "process_start_ticks": 987655,
            "launch_nonce_digest": "cc" * 32,
        },
        headers=headers,
    )
    assert registered.status_code == 200, registered.text
    running = await execution_api.post(
        f"/flow_runs/{flow_id}/set_state",
        json={"state": {"type": "RUNNING", "name": "Running"}},
        headers=headers,
    )
    assert running.status_code == 201, running.text
    request = template.model_copy(
        update={"flow_run_id": flow_id, "operation_id": uuid4()}
    )
    before = await route_snapshot(db)
    refused = await execution_api.post(
        "/streams/v2/dags/seal", json=request.model_dump(mode="json"), headers=headers
    )
    assert_error(refused, 409, "UNSUPPORTED_FLOW_RETRY")
    assert await route_snapshot(db) == before
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, flow_id)
        assert flow is not None and flow.empirical_policy.retries == 2


async def test_platform_cancellation_journals_before_exposing_worker_kill_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await sealed_execution_graph(db, session, execution_api, deployment)
    workers = await advertise_graph_workers(
        db, session, execution_api, plan.flow_run_id
    )
    started = await execution_api.post(
        f"/streams/v2/dags/{plan.id}/start",
        json=parent_body(plan.id, "start", plan.graph_digest),
        headers=headers,
    )
    assert started.status_code == 200, started.text
    # The ordinary platform cancellation route is what the worker observes
    # before sending a termination signal. It has no child or Flow capability.
    cancel = await execution_api.post(
        f"/flow_runs/{plan.flow_run_id}/set_state",
        json={"state": {"type": "CANCELLED", "name": "Cancelled"}},
    )
    assert cancel.status_code == 201, cancel.text
    assert cancel.json()["state"]["type"] == "CANCELLING"
    async with db.session_context() as check:
        graph = await check.get(db.StreamGraphRevision, plan.id)
        flow = await check.get(db.FlowRun, plan.flow_run_id)
        assert graph is not None and graph.current_control_operation_id is not None
        assert flow is not None and flow.state_type.value == "CANCELLING"
        control = await check.get(
            db.StreamDAGLifecycleOperation, graph.current_control_operation_id
        )
        assert control is not None and control.kind == "CONTROL"
        assert control.effective_control_kind == "CANCEL"
        assert control.first_cause == {"kind": "PLATFORM_CANCEL"}
        control_id = control.id
        targets = list(
            await check.scalars(
                sa.select(db.StreamDAGControlTarget).where(
                    db.StreamDAGControlTarget.control_operation_id == control_id
                )
            )
        )
        assert len(targets) == 4 and all(row.state == "PENDING" for row in targets)
        assert all(row.grant_id is None and row.physical_epoch == 0 for row in targets)
        assert (
            await check.scalar(
                sa.select(sa.func.count()).select_from(db.StreamDAGLaunchAttempt)
            )
            == 0
        )
        owned = {
            row.id: row.assigned_worker_id
            for row in await check.scalars(
                sa.select(db.StreamPhysicalSubtask).where(
                    db.StreamPhysicalSubtask.graph_revision_id == plan.id
                )
            )
        }
    observed = await execution_api.get(
        f"/streams/v2/dags/{plan.id}/execution", headers=headers
    )
    assert observed.status_code == 200, observed.text
    assert observed.json()["phase"] == "STOPPING"
    assert observed.json()["counts"]["physical_slots_remaining"] == 4
    principals = dict(workers)
    for subtask_id, worker_id in owned.items():
        assert worker_id is not None
        stop = s.StreamDAGWorkerStopRequest(
            principal=principals[worker_id],
            operation_id=uuid5(
                control_id, "prefect-stream-dag-stop-v1/" + str(subtask_id)
            ),
            control_operation_id=control_id,
        )
        stopped = await execution_api.post(
            f"/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/stop",
            json=stop.model_dump(mode="json"),
        )
        assert stopped.status_code == 200, stopped.text
    terminal = await execution_api.get(
        f"/streams/v2/dags/{plan.id}/execution", headers=headers
    )
    assert terminal.status_code == 200, terminal.text
    assert terminal.json()["terminal_state"] == "CANCELLED"
    assert terminal.json()["cleanup_complete"] is True


async def test_lifecycle_routes_refuse_sqlite_without_mutation(
    db: PrefectDBInterface, execution_api: httpx.AsyncClient
):
    assert db.dialect.name == "sqlite"
    before = await route_snapshot(db)
    graph_id = uuid4()
    for action in PARENT_ACTIONS:
        response = await execution_api.post(
            f"/streams/v2/dags/{graph_id}/{action}",
            json=parent_body(graph_id, action, "aa" * 32),
        )
        assert_error(response, 422, "UNSUPPORTED_BACKEND")
    observed = await execution_api.get(f"/streams/v2/dags/{graph_id}/execution")
    assert_error(observed, 422, "UNSUPPORTED_BACKEND")
    assert await route_snapshot(db) == before
