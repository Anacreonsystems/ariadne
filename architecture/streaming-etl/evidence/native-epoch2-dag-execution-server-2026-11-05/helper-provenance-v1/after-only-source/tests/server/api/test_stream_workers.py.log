"""Worker-only P2 advertisement and control boundaries on actual databases."""

import asyncio
import base64
from collections.abc import AsyncGenerator, Iterator
from contextlib import suppress
from dataclasses import replace
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.api.server import create_api_app
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import stream_subtasks as core
from prefect.server.models import stream_workers as worker_models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import stream_subtasks as s
from prefect.server.schemas.streams import StreamExecutionCredentials
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING, temporary_settings
from prefect.types._datetime import now
from tests.server.api.test_native_deployments import _declaration, _enrollment_body
from tests.server.models.test_stream_executions import _require_postgres_17
from tests.server.models.test_stream_subtasks import PUBLIC, graph_request

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


@pytest.fixture(autouse=True)
def protected_worker_api() -> Iterator[None]:
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-worker:test"}):
        yield


@pytest.fixture
async def worker_api(
    protected_worker_api: None,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    app = create_api_app(ignore_cache=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://native-api",
        auth=("native-worker", "test"),
    ) as client:
        yield client


def principal(pool_id: UUID) -> s.StreamWorkerPrincipal:
    return s.StreamWorkerPrincipal(
        work_pool_id=pool_id,
        host_id=uuid4(),
        boot_id=uuid4(),
        launcher_process_id=100,
        launcher_start_ticks=1000,
    )


async def new_worker(client: httpx.AsyncClient) -> tuple[UUID, s.StreamWorkerPrincipal]:
    name = "physical-" + uuid4().hex
    pool = await client.post("/work_pools/", json={"name": name, "type": "process"})
    assert pool.status_code == 201, pool.text
    worker = await client.post(
        f"/work_pools/{name}/workers/heartbeat",
        json={"name": "worker-" + uuid4().hex, "return_id": True},
    )
    assert worker.status_code == 200, worker.text
    return UUID(worker.text), principal(UUID(pool.json()["id"]))


async def deployed_graph(
    db: PrefectDBInterface,
    session: AsyncSession,
    api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    *,
    workers: list[tuple[UUID, s.StreamWorkerPrincipal]] | None = None,
    admit: bool = True,
) -> tuple[
    s.StreamGraphPlan, list[tuple[UUID, s.StreamWorkerPrincipal]], dict[str, str]
]:
    """Build production placement through enrollment and the real graph API."""
    first_id, first = await new_worker(api) if workers is None else workers[0]
    pool = await session.get(db.WorkPool, first.work_pool_id)
    assert pool is not None
    if workers is None:
        second_worker = await api.post(
            f"/work_pools/{pool.name}/workers/heartbeat",
            json={"name": "worker-" + uuid4().hex, "return_id": True},
        )
        assert second_worker.status_code == 200, second_worker.text
        second = first.model_copy(
            update={"launcher_process_id": 101, "launcher_start_ticks": 1001}
        )
        workers = [(first_id, first), (UUID(second_worker.text), second)]
    for worker_id, identity in workers:
        response = await api.post(
            f"/streams/v2/workers/{worker_id}/poll",
            json=s.StreamWorkerPollRequest(principal=identity).model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
    declaration = _declaration().model_copy(update={"profile": "partitioned-fixed-v1"})
    deployment.version = "deployment-v1"
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
        flow_id, first_id, first.work_pool_id, plan.json()["declaration_digest"]
    )
    enrollment.update(
        launcher_host_id=str(first.host_id),
        launcher_boot_id=str(first.boot_id),
        launcher_process_id=first.launcher_process_id,
        launcher_start_ticks=first.launcher_start_ticks,
    )
    enrolled = await api.post("/streams/v1/deployments/enroll", json=enrollment)
    assert enrolled.status_code == 200, enrolled.text
    execution_headers = {
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
            "host_id": str(first.host_id),
            "boot_id": str(first.boot_id),
            "child_process_id": 4321,
            "process_start_ticks": 987654,
            "launch_nonce_digest": "bb" * 32,
        },
        headers=execution_headers,
    )
    assert registered.status_code == 200, registered.text
    running = await api.post(
        f"/flow_runs/{flow_id}/set_state",
        json={"state": {"type": "RUNNING", "name": "Running"}},
        headers=execution_headers,
    )
    assert running.status_code == 201, running.text
    request = graph_request(flow_id).model_copy(
        update={
            "deployment_id": deployment.id,
            "deployment_version": deployment.version,
            "flow_version": declaration.flow_version,
            "working_directory": "/tmp",
        }
    )
    sealed = await api.post(
        "/streams/v2/graphs/seal",
        json=request.model_dump(mode="json"),
        headers=execution_headers,
    )
    assert sealed.status_code == 200, sealed.text
    sealed_plan = s.StreamGraphPlan.model_validate(sealed.json())
    if not admit:
        return sealed_plan, workers, execution_headers
    admitted = await api.post(
        f"/streams/v2/graphs/{sealed_plan.id}/admit",
        json={"operation_id": str(uuid4()), "graph_digest": sealed_plan.graph_digest},
        headers=execution_headers,
    )
    assert admitted.status_code == 200, admitted.text
    return s.StreamGraphPlan.model_validate(admitted.json()), workers, execution_headers


async def test_deployed_worker_poll_contains_only_own_assignments_and_parameters(
    db: PrefectDBInterface,
    session: AsyncSession,
    worker_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    await _require_postgres_17(db)
    plan, workers, _ = await deployed_graph(db, session, worker_api, deployment)
    for index, (worker_id, identity) in enumerate(workers):
        response = await worker_api.post(
            f"/streams/v2/workers/{worker_id}/poll",
            json=s.StreamWorkerPollRequest(principal=identity).model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        assignments = [
            s.StreamWorkerAssignment.model_validate(row) for row in response.json()
        ]
        assert len(assignments) == 2
        assert {row.assignment.operator_uid for row in assignments} == {"A", "B"}
        for row in assignments:
            assert row.assignment.assigned_worker_id == worker_id
            assert row.assignment.subtask_index == index
            assert row.assignment.graph_revision_id == plan.id
            assert row.phase == "WAITING"
            assert row.current_grant is None
            assert row.task_state.value == "PENDING" and row.task_run_count == 0
            assert row.output_state == "OPEN" and row.output_attempt == 0
            expected = (
                {"partition": index} if row.task_configuration.role == "SOURCE" else {}
            )
            assert row.task_configuration.parameters == expected
            assert row.task_configuration.task_key in ("source-key", "sign-key")
    changed = workers[0][1].model_copy(update={"launcher_start_ticks": 2222})
    refused = await worker_api.post(
        f"/streams/v2/workers/{workers[0][0]}/poll",
        json=s.StreamWorkerPollRequest(principal=changed).model_dump(mode="json"),
    )
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "WORKER_IDENTITY_IN_USE"


async def test_worker_control_proof_rejects_sibling_session_and_weak_locks(
    db: PrefectDBInterface,
    session: AsyncSession,
    worker_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    await _require_postgres_17(db)
    plan, workers, _ = await deployed_graph(db, session, worker_api, deployment)
    own = next(
        row
        for row in plan.subtasks
        if row.operator_uid == "A" and row.subtask_index == 0
    )
    sibling = next(
        row
        for row in plan.subtasks
        if row.operator_uid == "A" and row.subtask_index == 1
    )
    first_id, first = workers[0]
    async with db.session_context(begin_transaction=True) as locked:
        scope = await worker_models._authorize_worker_subtask(
            locked, db, first_id, own.id, first
        )
        assert (await core._graph(locked, db, plan.id, scope)).id == plan.id
        with pytest.raises(NativeStreamError, match="session or resource"):
            await core._subtask(locked, db, sibling.id, scope, update=True)
        async with db.session_context() as foreign:
            with pytest.raises(NativeStreamError, match="session or resource"):
                await core._graph(foreign, db, plan.id, scope)
        with pytest.raises(NativeStreamError, match="exclusive subtask proof"):
            await core._subtask(
                locked, db, own.id, replace(scope, subtask_update=False), update=True
            )
        with pytest.raises(NativeStreamError, match="exclusive graph proof"):
            await core._graph(locked, db, plan.id, scope, update=True)
        scope.principal.launcher_process_id += 1
        with pytest.raises(NativeStreamError, match="principal identity drifted"):
            await core._graph(locked, db, plan.id, scope)
    request = s.StreamSubtaskClaimRequest(
        operation_id=uuid4(),
        graph_digest=plan.graph_digest,
        holder_id=uuid4(),
        capability="cc" * 32,
        event_public_key=PUBLIC,
        launcher_binding_kind="ASSIGNED_WORKER",
        worker_id=workers[1][0],
        **workers[1][1].model_dump(),
    )
    refused = await worker_api.post(
        f"/streams/v2/workers/{workers[1][0]}/subtasks/{own.id}/claim",
        json=request.model_dump(mode="json"),
    )
    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"]["code"] == "SUBTASK_SCOPE_DENIED"


async def test_startup_abort_and_competing_admission_lock_workers_in_one_order(
    db: PrefectDBInterface,
    session: AsyncSession,
    worker_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    await _require_postgres_17(db)
    active, workers, _ = await deployed_graph(db, session, worker_api, deployment)
    contender, _, execution_headers = await deployed_graph(
        db, session, worker_api, deployment, workers=workers, admit=False
    )
    ordered = sorted(workers, key=lambda item: item[0])
    # Choose the higher-ID caller: locking its row first would invert admission's
    # lower-ID-first order and create an actual PostgreSQL deadlock here.
    caller_id, caller = ordered[1]
    abort_task: asyncio.Task[httpx.Response] | None = None
    try:
        with pytest.raises(NativeStreamError) as refused:
            async with db.session_context(begin_transaction=True) as holder:
                for model, clause in (
                    (db.FlowRun, db.FlowRun.id == contender.flow_run_id),
                    (
                        db.StreamExecutionAuthority,
                        db.StreamExecutionAuthority.flow_run_id
                        == contender.flow_run_id,
                    ),
                    (db.StreamGraphRevision, db.StreamGraphRevision.id == contender.id),
                ):
                    await holder.execute(
                        sa.select(model.id).where(clause).with_for_update()
                    )
                await holder.execute(
                    sa.select(db.Worker.id)
                    .where(db.Worker.id == ordered[0][0])
                    .with_for_update()
                )
                abort_task = asyncio.create_task(
                    worker_api.post(
                        f"/streams/v2/workers/{caller_id}/graphs/{active.id}/startup-abort",
                        json=s.StreamWorkerStartupAbortRequest(
                            principal=caller
                        ).model_dump(mode="json"),
                    )
                )
                async with db.session_context() as observer:
                    async with asyncio.timeout(5):
                        while True:
                            await observer.execute(
                                sa.text("SELECT pg_stat_clear_snapshot()")
                            )
                            if await observer.scalar(
                                sa.text(
                                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                    "WHERE datname=current_database() AND pid<>pg_backend_pid() "
                                    "AND wait_event_type='Lock' AND query LIKE '%FROM worker%' "
                                    "AND query LIKE '%FOR SHARE%')"
                                )
                            ):
                                break
                        assert not abort_task.done(), (
                            await abort_task if abort_task.done() else ""
                        )
                        await asyncio.sleep(0.01)
                assert not abort_task.done()
                await core.admit_region(
                    holder,
                    db,
                    contender.id,
                    s.StreamRegionAdmissionRequest(
                        operation_id=uuid4(), graph_digest=contender.graph_digest
                    ),
                    StreamExecutionCredentials(
                        generation=1,
                        holder_id=UUID(
                            execution_headers["X-Prefect-Stream-Execution-Holder"]
                        ),
                        capability=execution_headers[
                            "X-Prefect-Stream-Execution-Capability"
                        ],
                    ),
                )
        assert refused.value.code == "REGION_CAPACITY"
        assert abort_task is not None
        aborted = await asyncio.wait_for(abort_task, 5)
        assert aborted.status_code == 200, aborted.text
        assert aborted.json()["state"] == "RELEASED"
        async with db.session_context() as check:
            unchanged = await check.get(db.StreamGraphRevision, contender.id)
            assert unchanged is not None and unchanged.state == "SEALED"
            assert unchanged.admission_operation_id is None
            physical = (
                await check.scalars(
                    sa.select(db.StreamPhysicalSubtask).where(
                        db.StreamPhysicalSubtask.graph_revision_id == active.id
                    )
                )
            ).all()
            assert len(physical) == 4 and all(
                row.physical_slot_released for row in physical
            )
            assert (
                await check.scalar(
                    sa.select(sa.func.count()).select_from(db.StreamSubtaskGrant)
                )
                == 0
            )
    finally:
        if abort_task is not None and not abort_task.done():
            abort_task.cancel()
            with suppress(asyncio.CancelledError):
                await abort_task


async def test_worker_poll_advertises_p2_without_assignments(
    db: PrefectDBInterface, worker_api: httpx.AsyncClient
):
    await _require_postgres_17(db)
    worker_id, identity = await new_worker(worker_api)
    body = s.StreamWorkerPollRequest(principal=identity).model_dump(mode="json")
    first = await worker_api.post(f"/streams/v2/workers/{worker_id}/poll", json=body)
    assert first.status_code == 200, first.text
    assert first.json() == []
    async with db.session_context() as session:
        capability = await session.scalar(
            sa.select(db.StreamWorkerCapability).where(
                db.StreamWorkerCapability.worker_id == worker_id
            )
        )
        assert capability is not None
        assert capability.protocol == "partitioned-fixed-v1"
        assert capability.protocol_version == 1
        assert capability.work_pool_id == identity.work_pool_id
        assert capability.host_id == identity.host_id
        assert capability.boot_id == identity.boot_id
        assert capability.launcher_process_id == identity.launcher_process_id
        assert capability.launcher_start_ticks == identity.launcher_start_ticks
        original_id, original_time = capability.id, capability.last_renewed_at
    second = await worker_api.post(f"/streams/v2/workers/{worker_id}/poll", json=body)
    assert second.status_code == 200, second.text
    async with db.session_context() as session:
        capability = await session.scalar(
            sa.select(db.StreamWorkerCapability).where(
                db.StreamWorkerCapability.worker_id == worker_id
            )
        )
        assert capability is not None
        assert capability.id == original_id
        assert capability.last_renewed_at >= original_time
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamGraphRevision)
            )
            == 0
        )
        assert (
            await session.scalar(sa.select(sa.func.count()).select_from(db.TaskRun))
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskGrant)
            )
            == 0
        )


@pytest.mark.parametrize(
    "authentication", ["missing", "wrong-basic", "child", "bearer"]
)
async def test_worker_poll_rejects_nonworker_auth_without_mutation(
    db: PrefectDBInterface, worker_api: httpx.AsyncClient, authentication: str
):
    worker_id = uuid4()
    secret = "ab" * 32
    body = s.StreamWorkerPollRequest(principal=principal(uuid4())).model_dump(
        mode="json"
    )
    headers = {
        "missing": {},
        "wrong-basic": {
            "Authorization": "Basic " + base64.b64encode(b"wrong:secret").decode()
        },
        "child": {
            "Authorization": "PrefectSubtask " + secret,
            "X-Prefect-Subtask-Grant": str(uuid4()),
        },
        "bearer": {"Authorization": "Bearer " + secret},
    }[authentication]
    response = await worker_api.post(
        f"/streams/v2/workers/{worker_id}/poll", json=body, headers=headers, auth=None
    )
    assert response.status_code == 401, response.text
    assert secret not in response.text
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamWorkerCapability)
            )
            == 0
        )


async def test_worker_poll_wrong_pool_never_registers_capability(
    db: PrefectDBInterface, worker_api: httpx.AsyncClient
):
    await _require_postgres_17(db)
    worker_id, _ = await new_worker(worker_api)
    response = await worker_api.post(
        f"/streams/v2/workers/{worker_id}/poll",
        json=s.StreamWorkerPollRequest(principal=principal(uuid4())).model_dump(
            mode="json"
        ),
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "WORKER_NOT_READY"
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamWorkerCapability)
            )
            == 0
        )


@pytest.mark.parametrize("limit", [True, 1.0, 0, 129])
async def test_worker_poll_limit_is_strict_and_validation_is_secret_free(
    db: PrefectDBInterface, worker_api: httpx.AsyncClient, limit: object
):
    secret = "ab" * 32
    body = {
        "principal": principal(uuid4()).model_dump(mode="json"),
        "limit": limit,
        "capability": secret,
    }
    response = await worker_api.post(f"/streams/v2/workers/{uuid4()}/poll", json=body)
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "PROTOCOL_ERROR"
    assert secret not in response.text
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamWorkerCapability)
            )
            == 0
        )


@pytest.mark.parametrize(
    "operation",
    [
        "poll",
        "claim",
        "register",
        "renew",
        "authorize",
        "confirm",
        "retire",
        "stop",
        "abort",
    ],
)
async def test_sqlite_worker_controls_refuse_before_locator_or_write(
    db: PrefectDBInterface, worker_api: httpx.AsyncClient, operation: str
):
    if db.dialect.name == "postgresql":
        pytest.skip("SQLite fail-closed control")
    worker_id, subtask_id, graph_id, grant_id = uuid4(), uuid4(), uuid4(), uuid4()
    identity = principal(uuid4()).model_dump(mode="json")
    witness = {
        "kind": "WAITPID",
        "observed_at": now("UTC").isoformat(),
        "host_id": identity["host_id"],
        "boot_id": identity["boot_id"],
        "child_process_id": 200,
        "process_start_ticks": 2000,
        "exit_code": -9,
    }
    replacement = {
        "operation_id": str(uuid4()),
        "challenge_capability": "aa" * 32,
        "death_witness": witness,
    }
    route, body = {
        "poll": ("poll", {"principal": identity}),
        "claim": (
            f"subtasks/{subtask_id}/claim",
            {
                "operation_id": str(uuid4()),
                "graph_digest": "aa" * 32,
                "holder_id": str(uuid4()),
                "capability": "bb" * 32,
                "event_public_key": "cc" * 32,
                "launcher_binding_kind": "ASSIGNED_WORKER",
                "worker_id": str(worker_id),
                **identity,
            },
        ),
        "register": (
            f"subtasks/{subtask_id}/register",
            {
                "principal": identity,
                "registration": {
                    "operation_id": str(uuid4()),
                    "grant_id": str(grant_id),
                    "child_process_id": 200,
                    "process_start_ticks": 2000,
                },
            },
        ),
        "renew": (
            f"subtasks/{subtask_id}/renew",
            {"principal": identity, "grant_id": str(grant_id)},
        ),
        "authorize": (
            f"subtasks/{subtask_id}/replacements/authorize",
            {
                "principal": identity,
                "replacement": {
                    "operation_id": str(uuid4()),
                    "expected_grant_id": str(grant_id),
                    "expected_physical_epoch": 1,
                    "host_id": identity["host_id"],
                    "challenge_capability": "aa" * 32,
                },
            },
        ),
        "confirm": (
            f"subtasks/{subtask_id}/replacements/confirm",
            {
                "principal": identity,
                "replacement": {
                    **replacement,
                    "new_holder_id": str(uuid4()),
                    "new_capability": "dd" * 32,
                    "new_event_public_key": "ee" * 32,
                    "new_child_process_id": 201,
                    "new_process_start_ticks": 2001,
                    "host_id": identity["host_id"],
                    "boot_id": identity["boot_id"],
                },
            },
        ),
        "retire": (
            f"subtasks/{subtask_id}/replacements/retire",
            {
                "principal": identity,
                "retirement": {**replacement, "error": "verified stop"},
            },
        ),
        "stop": (
            f"subtasks/{subtask_id}/startup-stop",
            {
                "principal": identity,
                "stop": {
                    "operation_id": str(uuid4()),
                    "abort_operation_id": str(uuid4()),
                    "grant_id": str(grant_id),
                    "expected_physical_epoch": 1,
                    "unstarted": True,
                },
            },
        ),
        "abort": (f"graphs/{graph_id}/startup-abort", {"principal": identity}),
    }[operation]
    response = await worker_api.post(
        f"/streams/v2/workers/{worker_id}/{route}", json=body
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "UNSUPPORTED_BACKEND"
    async with db.session_context() as session:
        for model in (
            db.StreamWorkerCapability,
            db.StreamPhysicalSubtask,
            db.StreamSubtaskGrant,
            db.TaskRun,
        ):
            assert (
                await session.scalar(sa.select(sa.func.count()).select_from(model)) == 0
            )
