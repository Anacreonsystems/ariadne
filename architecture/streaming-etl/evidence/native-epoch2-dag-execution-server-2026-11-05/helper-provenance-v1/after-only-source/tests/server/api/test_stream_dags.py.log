"""DAG publication through the protected API; execution remains fenced."""

import secrets
from collections.abc import AsyncGenerator, Iterator
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.api.server import create_api_app, create_app
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import stream_dags as dag_models
from prefect.server.schemas import stream_dags as s
from prefect.server.schemas import stream_subtasks as physical
from prefect.server.schemas.streams import StreamExecutionClaimRequest
from prefect.settings import (
    PREFECT_SERVER_API_AUTH_STRING,
    PREFECT_SERVER_CSRF_PROTECTION_ENABLED,
    temporary_settings,
)
from tests.server.api.test_native_deployments import _declaration, _enrollment_body
from tests.server.api.test_stream_workers import new_worker
from tests.server.models.test_stream_dags import dag_request
from tests.server.models.test_stream_executions import _require_postgres_17

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


@pytest.fixture(autouse=True)
async def supported_dag_backend(
    request: pytest.FixtureRequest, db: PrefectDBInterface
) -> None:
    if request.node.name.startswith("test_postgres_"):
        await _require_postgres_17(db)
    elif request.node.name.startswith("test_sqlite_") and db.dialect.name != "sqlite":
        pytest.skip("This API refusal is specific to SQLite")


@pytest.fixture(autouse=True)
def protected_dag_api() -> Iterator[None]:
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-dag:test"}):
        yield


@pytest.fixture
async def dag_api(protected_dag_api: None) -> AsyncGenerator[httpx.AsyncClient, None]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_api_app(ignore_cache=True)),
        base_url="http://native-api",
        auth=("native-dag", "test"),
    ) as client:
        yield client


async def deployed_dag_request(
    db: PrefectDBInterface,
    session: AsyncSession,
    api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    *,
    profile: str = "partitioned-dag-v1",
    event_public_key: str | None = None,
) -> tuple[s.StreamDAGSealRequest, dict[str, str]]:
    """Enroll a deployed Flow before publication, using the actual HTTP boundary."""
    worker_id, principal = await new_worker(api)
    pool = await session.get(db.WorkPool, principal.work_pool_id)
    assert pool is not None
    declaration = _declaration().model_copy(update={"profile": profile})
    if profile == "partitioned-dag-v1":
        declaration = declaration.model_copy(update={"flow_retries": 0})
    deployment.version = "dag-deployment-v1"
    deployment.work_queue_id = pool.default_queue_id
    deployment.job_variables = {
        "prefect_native_streaming": declaration.model_dump(mode="json")
    }
    await session.commit()
    created = await api.post(f"/deployments/{deployment.id}/create_flow_run", json={})
    assert created.status_code == 201, created.text
    flow_id = UUID(created.json()["id"])
    plan = await api.get(f"/streams/v1/deployments/{flow_id}/plan")
    assert plan.status_code == 200, plan.text
    enrollment = _enrollment_body(
        flow_id, worker_id, principal.work_pool_id, plan.json()["declaration_digest"]
    )
    enrollment.update(
        launcher_host_id=str(principal.host_id),
        launcher_boot_id=str(principal.boot_id),
        launcher_process_id=principal.launcher_process_id,
        launcher_start_ticks=principal.launcher_start_ticks,
    )
    if event_public_key is not None:
        enrollment["holder_event_public_key"] = event_public_key
    enrolled = await api.post("/streams/v1/deployments/enroll", json=enrollment)
    assert enrolled.status_code == 200, enrolled.text
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": enrollment["holder_id"],
        "X-Prefect-Stream-Execution-Capability": enrollment["holder_capability"],
    }
    registered = await api.post(
        "/streams/v1/deployments/launches/register-process",
        json={
            "flow_run_id": str(flow_id),
            "operation_id": str(uuid4()),
            "enrollment_operation_id": enrollment["operation_id"],
            "generation": 1,
            "launcher_instance_id": enrollment["launcher_instance_id"],
            "host_id": str(principal.host_id),
            "boot_id": str(principal.boot_id),
            "child_process_id": 4321,
            "process_start_ticks": 987654,
            "launch_nonce_digest": "bb" * 32,
        },
        headers=headers,
    )
    assert registered.status_code == 200, registered.text
    running = await api.post(
        f"/flow_runs/{flow_id}/set_state",
        json={"state": {"type": "RUNNING", "name": "Running"}},
        headers=headers,
    )
    assert running.status_code == 201, running.text
    request = dag_request(flow_id).model_copy(
        update={
            "deployment_id": deployment.id,
            "deployment_version": deployment.version,
            "flow_version": declaration.flow_version,
            "working_directory": "/tmp",
        }
    )
    return request, headers


async def topology_counts(db: PrefectDBInterface) -> dict[str, int]:
    async with db.session_context() as session:
        return {
            model.__tablename__: int(
                await session.scalar(sa.select(sa.func.count()).select_from(model)) or 0
            )
            for model in (
                db.StreamGraphRevision,
                db.StreamOperatorRun,
                db.StreamGraphPort,
                db.StreamGraphEdge,
                db.StreamGraphEdgePartition,
                db.StreamPhysicalSubtask,
                db.TaskRun,
                db.StreamSubmission,
                db.StreamPartition,
                db.StreamRecord,
                db.StreamSubtaskGrant,
                db.StreamPhysicalSlotAllocation,
            )
        }


def assert_error(response: httpx.Response, status: int, code: str) -> None:
    assert response.status_code == status, response.text
    assert response.json()["detail"]["code"] == code, response.text
    assert response.json()["detail"]["retryable"] is False


@pytest.mark.parametrize(
    "authorization", [None, "Basic d3Jvbmc6d3Jvbmc=", "PrefectSubtask " + "aa" * 32]
)
async def test_dag_routes_require_platform_auth_and_refuse_child_login(
    dag_api: httpx.AsyncClient, authorization: str | None
):
    headers = {} if authorization is None else {"Authorization": authorization}
    for method, path, body in (
        ("POST", "/streams/v2/dags/seal", dag_request(uuid4()).model_dump(mode="json")),
        ("GET", f"/streams/v2/dags/{uuid4()}", None),
    ):
        response = await dag_api.request(
            method, path, json=body, headers=headers, auth=None
        )
        assert response.status_code == 401, response.text
        assert "aa" * 32 not in response.text


@pytest.mark.parametrize("malformed", ["body", "header", "partial"])
async def test_dag_validation_redacts_input_secrets(
    dag_api: httpx.AsyncClient, malformed: str
):
    secret = secrets.token_hex(32)
    body = dag_request(uuid4()).model_dump(mode="json")
    headers: dict[str, str] = {}
    if malformed == "body":
        body["metadata_only"] = secret
    elif malformed == "header":
        headers = {
            "X-Prefect-Stream-Execution-Generation": secret,
            "X-Prefect-Stream-Execution-Holder": str(uuid4()),
            "X-Prefect-Stream-Execution-Capability": secret,
        }
    else:
        headers = {"X-Prefect-Stream-Execution-Capability": secret}
    response = await dag_api.post("/streams/v2/dags/seal", json=body, headers=headers)
    assert_error(response, 422, "PROTOCOL_ERROR")
    assert secret not in response.text


async def test_dag_surface_contains_only_metadata_and_fenced_terminal_routes(
    dag_api: httpx.AsyncClient,
):
    response = await dag_api.get("/openapi.json")
    assert response.status_code == 200, response.text
    paths = response.json()["paths"]
    expected = dict(
        (
            ("/streams/v2/dags/seal", "post"),
            ("/streams/v2/dags/{graph_id}", "get"),
            ("/streams/v2/dags/{graph_id}/start", "post"),
            ("/streams/v2/dags/{graph_id}/execution", "get"),
            ("/streams/v2/dags/{graph_id}/abort-preview", "post"),
            ("/streams/v2/dags/{graph_id}/cancel", "post"),
            ("/streams/v2/dags/{graph_id}/activate", "post"),
            ("/streams/v2/dags/{graph_id}/fail", "post"),
            ("/streams/v2/dags/subtasks/{subtask_id}/bootstrap", "get"),
            ("/streams/v2/dags/subtasks/{subtask_id}/bootstrap-ready", "post"),
            ("/streams/v2/dags/subtasks/{subtask_id}/activation", "get"),
            ("/streams/v2/dags/sources/open", "post"),
            ("/streams/v2/dags/consumers/open", "post"),
            ("/streams/v2/dags/consumers/read", "post"),
            ("/streams/v2/dags/subtasks/{subtask_id}/input/status", "get"),
            ("/streams/v2/dags/subtasks/{subtask_id}/output/status", "get"),
            ("/streams/v2/dags/sources/append", "post"),
            ("/streams/v2/dags/consumers/complete", "post"),
            ("/streams/v2/dags/sources/finish", "post"),
            ("/streams/v2/dags/consumers/finish", "post"),
            ("/streams/v2/dags/workers/{worker_id}/poll", "post"),
            (
                "/streams/v2/dags/workers/{worker_id}/graphs/{graph_id}/startup-abort",
                "post",
            ),
            (
                "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/prepare",
                "post",
            ),
            (
                "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/record",
                "post",
            ),
            (
                "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/close",
                "post",
            ),
            ("/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/stop", "post"),
        )
    )
    assert {path for path in paths if path.startswith("/streams/v2/dags")} == set(
        expected
    )
    for path, method in expected.items():
        assert set(paths[path]) == {method}
    for path in ("/streams/v2/dags/seal", "/streams/v2/dags/{graph_id}"):
        for operation in paths[path].values():
            assert all(
                value["name"] != "metadata_only"
                for value in operation.get("parameters", [])
            )
    for suffix in (
        "admit",
        "wait",
        "claim",
        "begin",
        "run",
        "start-extra",
        "activate-extra",
    ):
        refused = await dag_api.post(f"/streams/v2/dags/{uuid4()}/{suffix}", json={})
        assert refused.status_code == 404, refused.text


async def test_dag_seal_keeps_csrf_protection():
    with temporary_settings({PREFECT_SERVER_CSRF_PROTECTION_ENABLED: True}):
        app = create_app(ephemeral=True, ignore_cache=True)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://native-api/api",
            auth=("native-dag", "test"),
        ) as client:
            response = await client.post(
                "/streams/v2/dags/seal",
                json=dag_request(uuid4()).model_dump(mode="json"),
            )
    assert response.status_code == 403, response.text
    assert response.json()["detail"] == "Missing CSRF token."


async def test_sqlite_public_dag_routes_cannot_enable_metadata_mode(
    db: PrefectDBInterface, dag_api: httpx.AsyncClient
):
    assert db.dialect.name == "sqlite"
    for query in ("", "?metadata_only=true"):
        sealed = await dag_api.post(
            "/streams/v2/dags/seal" + query,
            json=dag_request(uuid4()).model_dump(mode="json"),
        )
        assert_error(sealed, 422, "UNSUPPORTED_BACKEND")
        read = await dag_api.get(f"/streams/v2/dags/{uuid4()}" + query)
        assert_error(read, 422, "UNSUPPORTED_BACKEND")
    assert not any((await topology_counts(db)).values())


async def test_postgres_seal_and_read_replay_keep_stable_unassigned_topology(
    db: PrefectDBInterface,
    session: AsyncSession,
    dag_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    assert db.dialect.name == "postgresql"
    request, headers = await deployed_dag_request(db, session, dag_api, deployment)
    body = request.model_dump(mode="json")
    sealed = await dag_api.post("/streams/v2/dags/seal", json=body, headers=headers)
    assert sealed.status_code == 200, sealed.text
    plan = s.StreamDAGPlan.model_validate(sealed.json())
    assert plan.state == "SEALED" and plan.execution_ready is False
    assert plan.leaf_operator_uids == ["B", "C"]
    assert len(plan.subtasks) == 4
    assert all(
        row.assigned_worker_id is None and not row.physical_slot_reserved
        for row in plan.subtasks
    )
    replay = await dag_api.post("/streams/v2/dags/seal", json=body, headers=headers)
    read = await dag_api.get(f"/streams/v2/dags/{plan.id}", headers=headers)
    assert replay.status_code == read.status_code == 200
    assert replay.json() == read.json() == sealed.json()
    assert headers["X-Prefect-Stream-Execution-Capability"] not in sealed.text
    counts = await topology_counts(db)
    assert counts == {
        "stream_graph_revision": 1,
        "stream_operator_run": 3,
        "stream_graph_port": 5,
        "stream_graph_edge": 2,
        "stream_graph_edge_partition": 3,
        "stream_physical_subtask": 4,
        "task_run": 4,
        "stream_submission": 4,
        "stream_partition": 7,
        "stream_record": 0,
        "stream_subtask_grant": 0,
        "stream_physical_slot_allocation": 0,
    }
    changed = request.model_dump(mode="json")
    changed["operators"][0]["parameters"][0]["split"] = 999
    for changed_intent in (changed, body | {"operation_id": str(uuid4())}):
        refused = await dag_api.post(
            "/streams/v2/dags/seal", json=changed_intent, headers=headers
        )
        assert_error(refused, 409, "IDENTITY_CONFLICT")
        assert await topology_counts(db) == counts


async def test_postgres_seal_and_read_fence_missing_forged_and_foreign_flow_credentials(
    db: PrefectDBInterface,
    session: AsyncSession,
    dag_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    request, headers = await deployed_dag_request(db, session, dag_api, deployment)
    _, foreign = await deployed_dag_request(db, session, dag_api, deployment)
    for credentials, code in (
        ({}, "EXECUTION_BINDING_REQUIRED"),
        (
            headers | {"X-Prefect-Stream-Execution-Capability": "00" * 32},
            "STALE_EXECUTION",
        ),
        (headers | {"X-Prefect-Stream-Execution-Generation": "2"}, "STALE_EXECUTION"),
        (foreign, "STALE_EXECUTION"),
    ):
        response = await dag_api.post(
            "/streams/v2/dags/seal",
            json=request.model_dump(mode="json"),
            headers=credentials,
        )
        assert_error(response, 409, code)
    assert not any((await topology_counts(db)).values())
    sealed = await dag_api.post(
        "/streams/v2/dags/seal", json=request.model_dump(mode="json"), headers=headers
    )
    assert sealed.status_code == 200, sealed.text
    for credentials, code in (
        ({}, "EXECUTION_BINDING_REQUIRED"),
        (foreign, "STALE_EXECUTION"),
    ):
        read = await dag_api.get(
            f"/streams/v2/dags/{sealed.json()['id']}", headers=credentials
        )
        assert_error(read, 409, code)
        assert headers["X-Prefect-Stream-Execution-Capability"] not in read.text


@pytest.mark.parametrize(
    "mismatch",
    ["deployment", "deployment_version", "flow_version", "working_directory"],
)
async def test_postgres_dag_seal_requires_exact_deployment_snapshot(
    db: PrefectDBInterface,
    session: AsyncSession,
    dag_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    mismatch: str,
):
    request, headers = await deployed_dag_request(db, session, dag_api, deployment)
    body = request.model_dump(mode="json")
    if mismatch == "deployment":
        body["deployment_id"] = str(uuid4())
    elif mismatch == "working_directory":
        body[mismatch] = None
    else:
        body[mismatch] = "different-version"
    response = await dag_api.post("/streams/v2/dags/seal", json=body, headers=headers)
    assert_error(response, 409, "IDENTITY_CONFLICT")
    assert not any((await topology_counts(db)).values())


async def test_postgres_fixed_profile_cannot_publish_dag(
    db: PrefectDBInterface,
    session: AsyncSession,
    dag_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    request, headers = await deployed_dag_request(
        db, session, dag_api, deployment, profile="partitioned-fixed-v1"
    )
    response = await dag_api.post(
        "/streams/v2/dags/seal", json=request.model_dump(mode="json"), headers=headers
    )
    assert_error(response, 422, "UNSUPPORTED_PROFILE")
    assert not any((await topology_counts(db)).values())


async def test_postgres_internal_standalone_dag_is_not_public_deployed_metadata(
    db: PrefectDBInterface,
    dag_api: httpx.AsyncClient,
    flow: orm_models.Flow,
):
    created = await dag_api.post(
        "/flow_runs/",
        json={
            "flow_id": str(flow.id),
            "flow_version": "dag-flow-v1",
            "empirical_policy": {"retries": 2},
            "state": {"type": "PENDING", "name": "Pending"},
        },
    )
    assert created.status_code == 201, created.text
    flow_id = UUID(created.json()["id"])
    claim = StreamExecutionClaimRequest(
        flow_run_id=flow_id,
        operation_id=uuid4(),
        holder_id=uuid4(),
        holder_capability=secrets.token_hex(32),
        holder_event_public_key="aa" * 32,
        supervisor_id=uuid4(),
        supervisor_capability=secrets.token_hex(32),
    )
    claimed = await dag_api.post(
        "/streams/v1/executions/claim", json=claim.model_dump(mode="json")
    )
    assert claimed.status_code == 200, claimed.text
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": str(claim.holder_id),
        "X-Prefect-Stream-Execution-Capability": claim.holder_capability,
    }
    async with db.session_context(begin_transaction=True) as session:
        internal = await dag_models.seal_dag_metadata(session, db, dag_request(flow_id))
    baseline = await topology_counts(db)
    public = await dag_api.get(f"/streams/v2/dags/{internal.id}", headers=headers)
    assert_error(public, 422, "UNSUPPORTED_PROFILE")
    async with db.session_context(begin_transaction=True) as session:
        replay = await dag_models.read_dag_plan(
            session, db, internal.id, metadata_only=True
        )
    assert replay.model_dump(mode="json") == internal.model_dump(mode="json")
    assert await topology_counts(db) == baseline


@pytest.mark.parametrize(
    "invalid",
    [
        "cycle",
        "subscriber",
        "schema",
        "fan_in",
        "multiple_roots",
        "disconnected",
        "same_ports",
    ],
)
async def test_postgres_invalid_dag_refused_before_task_or_stream_creation(
    db: PrefectDBInterface,
    session: AsyncSession,
    dag_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    invalid: str,
):
    request, headers = await deployed_dag_request(db, session, dag_api, deployment)
    body = request.model_dump(mode="json")
    operators, edges = body["operators"], body["edges"]
    if invalid == "cycle":
        edges[1]["producer_operator_uid"] = "B"
        edges.append(
            edges[0]
            | {
                "edge_uid": "C-B",
                "producer_operator_uid": "C",
                "subscriber_uid": "other",
                "exchange": "rebalance",
            }
        )
    elif invalid == "subscriber":
        edges[1]["subscriber_uid"] = edges[0]["subscriber_uid"]
    elif invalid == "schema":
        operators[1]["input_port"]["schema_digest"] = "00" * 32
    elif invalid == "fan_in":
        edges.append(
            edges[0]
            | {
                "edge_uid": "C-B",
                "producer_operator_uid": "C",
                "subscriber_uid": "other",
                "exchange": "rebalance",
            }
        )
    elif invalid == "multiple_roots":
        operators[1].update(role="SOURCE", input_port=None, input_key=None)
    elif invalid == "disconnected":
        operators.append(operators[1] | {"operator_uid": "D"})
    else:
        operators[1]["input_port"]["uid"] = "out"
        edges[0]["input_port_uid"] = "out"
    response = await dag_api.post("/streams/v2/dags/seal", json=body, headers=headers)
    assert_error(response, 422, "PROTOCOL_ERROR")
    assert not any((await topology_counts(db)).values())


async def test_postgres_dag_cannot_execute_through_fixed_profile_routes(
    db: PrefectDBInterface,
    session: AsyncSession,
    dag_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    request, headers = await deployed_dag_request(db, session, dag_api, deployment)
    sealed = await dag_api.post(
        "/streams/v2/dags/seal", json=request.model_dump(mode="json"), headers=headers
    )
    assert sealed.status_code == 200, sealed.text
    plan = s.StreamDAGPlan.model_validate(sealed.json())
    baseline = await topology_counts(db)
    common = {"operation_id": str(uuid4()), "graph_digest": plan.graph_digest}
    read = await dag_api.get(f"/streams/v2/graphs/{plan.id}", headers=headers)
    assert_error(read, 409, "EXECUTION_PROFILE_NOT_READY")
    for suffix, body in (
        ("admit", common),
        ("activate", common | {"grant_ids": [str(uuid4()) for _ in range(4)]}),
        ("abort", common | {"error": "DAG execution is unavailable"}),
    ):
        request_type = {
            "admit": physical.StreamRegionAdmissionRequest,
            "activate": physical.StreamGraphActivateRequest,
            "abort": physical.StreamGraphAbortRequest,
        }[suffix]
        request_type.model_validate(body)
        refused = await dag_api.post(
            f"/streams/v2/graphs/{plan.id}/{suffix}", json=body, headers=headers
        )
        assert_error(refused, 409, "EXECUTION_PROFILE_NOT_READY")
    subtask = plan.subtasks[0]
    claim = common | {
        "holder_id": str(uuid4()),
        "capability": secrets.token_hex(32),
        "event_public_key": "aa" * 32,
        "launcher_binding_kind": "FLOW_AUTHORITY_LOCAL",
        "host_id": str(uuid4()),
        "boot_id": str(uuid4()),
        "launcher_process_id": 100,
        "launcher_start_ticks": 1000,
    }
    for suffix, body in (
        ("claim", claim),
        (
            "register",
            {
                "operation_id": str(uuid4()),
                "grant_id": str(uuid4()),
                "child_process_id": 200,
                "process_start_ticks": 2000,
            },
        ),
    ):
        if suffix == "claim":
            physical.StreamSubtaskClaimRequest.model_validate(body)
        else:
            physical.StreamSubtaskRegistrationRequest.model_validate(body)
        refused = await dag_api.post(
            f"/streams/v2/subtasks/{subtask.id}/{suffix}", json=body, headers=headers
        )
        assert_error(refused, 409, "EXECUTION_PROFILE_NOT_READY")
    assert await topology_counts(db) == baseline


async def test_postgres_unstarted_dag_cannot_report_completed_flow(
    db: PrefectDBInterface,
    session: AsyncSession,
    dag_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    request, headers = await deployed_dag_request(db, session, dag_api, deployment)
    sealed = await dag_api.post(
        "/streams/v2/dags/seal", json=request.model_dump(mode="json"), headers=headers
    )
    assert sealed.status_code == 200, sealed.text
    baseline = await topology_counts(db)
    completed = await dag_api.post(
        f"/flow_runs/{request.flow_run_id}/set_state",
        json={"state": {"type": "COMPLETED", "name": "Completed"}},
        headers=headers,
    )
    assert_error(completed, 409, "DAG_NOT_STARTED")
    flow = await dag_api.get(f"/flow_runs/{request.flow_run_id}")
    assert flow.status_code == 200, flow.text
    assert flow.json()["state"]["type"] == "RUNNING"
    read = await dag_api.get(f"/streams/v2/dags/{sealed.json()['id']}", headers=headers)
    assert read.status_code == 200 and read.json() == sealed.json()
    assert await topology_counts(db) == baseline
