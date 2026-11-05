"""Production P2 metadata controls on PostgreSQL; OS acceptance is separate."""

from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.schemas import stream_subtasks as s
from prefect.types._datetime import now
from tests.server.api.test_stream_workers import (
    deployed_graph,
)
from tests.server.api.test_stream_workers import (
    protected_worker_api as protected_worker_api,
)
from tests.server.api.test_stream_workers import (
    worker_api as worker_api,
)
from tests.server.models.test_stream_executions import _require_postgres_17
from tests.server.models.test_stream_subtasks import PUBLIC

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def register_region(
    api: httpx.AsyncClient,
    plan: s.StreamGraphPlan,
    workers: list[tuple[UUID, s.StreamWorkerPrincipal]],
) -> list[UUID]:
    grants = []
    for index, subtask in enumerate(plan.subtasks):
        worker_id, identity = workers[subtask.subtask_index]
        claim = s.StreamSubtaskClaimRequest(
            operation_id=uuid4(),
            graph_digest=plan.graph_digest,
            holder_id=uuid4(),
            capability=uuid4().hex + uuid4().hex,
            event_public_key=PUBLIC,
            launcher_binding_kind="ASSIGNED_WORKER",
            worker_id=worker_id,
            **identity.model_dump(),
        )
        response = await api.post(
            f"/streams/v2/workers/{worker_id}/subtasks/{subtask.id}/claim",
            json=claim.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        grant = s.StreamSubtaskGrant.model_validate(response.json())
        registration = s.StreamWorkerRegistrationRequest(
            principal=identity,
            registration=s.StreamSubtaskRegistrationRequest(
                operation_id=uuid4(),
                grant_id=grant.id,
                child_process_id=2000 + index,
                process_start_ticks=3000 + index,
            ),
        )
        response = await api.post(
            f"/streams/v2/workers/{worker_id}/subtasks/{subtask.id}/register",
            json=registration.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        assert response.json()["state"] == "REGISTERED"
        grants.append(grant.id)
    return grants


async def test_cancel_and_raw_cancelling_are_refused_without_any_partial_state(
    db: PrefectDBInterface,
    session: AsyncSession,
    worker_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    await _require_postgres_17(db)
    plan, _, headers = await deployed_graph(db, session, worker_api, deployment)
    response = await worker_api.post(
        "/streams/v1/deployments/control/cancel",
        json={
            "flow_run_id": str(plan.flow_run_id),
            "operation_id": str(uuid4()),
            "expected_generation": 1,
            "reason": "must fail closed before any stop proof",
        },
        headers=headers,
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "PHYSICAL_STOP_REQUIRED"
    proposed = await worker_api.post(
        f"/flow_runs/{plan.flow_run_id}/set_state",
        json={"state": {"type": "CANCELLING", "name": "Cancelling"}},
        headers=headers,
    )
    assert proposed.status_code == 409, proposed.text
    assert proposed.json()["detail"]["code"] == "PHYSICAL_STOP_REQUIRED"
    async with db.session_context() as check:
        authority = await check.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == plan.flow_run_id
            )
        )
        assert (
            authority.state == "ACTIVE"
            and not authority.cancel_requested
            and not authority.source_admission_closed
        )
        assert (
            await check.get(db.FlowRun, plan.flow_run_id)
        ).state_type.value == "RUNNING"
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.NativeStreamControlOperation)
                .where(db.NativeStreamControlOperation.flow_run_id == plan.flow_run_id)
            )
            == 0
        )
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRun)
                .where(
                    db.TaskRun.flow_run_id == plan.flow_run_id,
                    db.TaskRun.state_type == "PENDING",
                )
            )
            == 4
        )


async def test_assigned_history_prevents_deleting_worker(
    db: PrefectDBInterface,
    session: AsyncSession,
    worker_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    await _require_postgres_17(db)
    plan, workers, _ = await deployed_graph(db, session, worker_api, deployment)
    async with db.session_context(begin_transaction=True) as check:
        with pytest.raises(IntegrityError) as error:
            async with check.begin_nested():
                await check.execute(
                    sa.delete(db.Worker).where(db.Worker.id == workers[0][0])
                )
        assert "fk_stream_physical_subtask__assigned_worker_id__worker" in str(
            error.value.orig
        )
        assert await check.get(db.Worker, workers[0][0]) is not None
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamPhysicalSubtask)
                .where(
                    db.StreamPhysicalSubtask.graph_revision_id == plan.id,
                    db.StreamPhysicalSubtask.physical_slot_released.is_(False),
                )
            )
            == 4
        )


@pytest.mark.parametrize("fault", ["stale-capability", "offline-worker"])
async def test_activation_revalidates_worker_after_all_registrations(
    db: PrefectDBInterface,
    session: AsyncSession,
    worker_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    fault: str,
):
    await _require_postgres_17(db)
    plan, workers, headers = await deployed_graph(db, session, worker_api, deployment)
    grant_ids = await register_region(worker_api, plan, workers)
    async with db.session_context(begin_transaction=True) as change:
        if fault == "stale-capability":
            cap = await change.scalar(
                sa.select(db.StreamWorkerCapability).where(
                    db.StreamWorkerCapability.worker_id == workers[1][0]
                )
            )
            cap.last_renewed_at = now("UTC") - timedelta(seconds=121)
        else:
            (await change.get(db.Worker, workers[1][0])).status = "OFFLINE"
    response = await worker_api.post(
        f"/streams/v2/graphs/{plan.id}/activate",
        json={
            "operation_id": str(uuid4()),
            "graph_digest": plan.graph_digest,
            "grant_ids": [str(value) for value in grant_ids],
        },
        headers=headers,
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "NATIVE_WORKERS_UNAVAILABLE"
    async with db.session_context() as check:
        assert (
            await check.get(db.StreamGraphRevision, plan.id)
        ).startup_completed_at is None
        assert (
            await check.scalar(sa.select(sa.func.count()).select_from(db.StreamRecord))
            == 0
        )
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRun)
                .where(
                    db.TaskRun.flow_run_id == plan.flow_run_id,
                    db.TaskRun.run_count != 0,
                )
            )
            == 0
        )


async def test_whole_flow_legacy_recovery_cannot_take_over_assigned_children(
    db: PrefectDBInterface,
    session: AsyncSession,
    worker_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    await _require_postgres_17(db)
    plan, workers, headers = await deployed_graph(db, session, worker_api, deployment)
    grant_ids = await register_region(worker_api, plan, workers)
    worker_id, identity = workers[0]
    async with db.session_context(begin_transaction=True) as change:
        launch = await change.scalar(
            sa.select(db.NativeStreamLaunchIdentity).where(
                db.NativeStreamLaunchIdentity.flow_run_id == plan.flow_run_id
            )
        )
        assert launch is not None
        launch.last_renewed_at = now("UTC") - timedelta(seconds=121)
        enrollment_id = launch.enrollment_operation_id
        child_pid = launch.child_process_id
        child_ticks = launch.process_start_ticks
    candidates = await worker_api.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker_id),
            "work_pool_id": str(identity.work_pool_id),
            "host_id": str(identity.host_id),
        },
    )
    assert candidates.status_code == 200, candidates.text
    assert candidates.json() == []
    operation_id = uuid4()
    challenge = uuid4().hex + uuid4().hex
    witness = {
        "kind": "IDENTITY_GONE",
        "observed_at": now("UTC").isoformat(),
        "host_id": str(identity.host_id),
        "boot_id": str(identity.boot_id),
        "child_process_id": child_pid,
        "process_start_ticks": child_ticks,
    }
    authorization = {
        "flow_run_id": str(plan.flow_run_id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": str(enrollment_id),
        "worker_id": str(worker_id),
        "work_pool_id": str(identity.work_pool_id),
        "host_id": str(identity.host_id),
        "identity_kind": "CHILD",
        "challenge_capability": challenge,
    }
    confirmation = {
        "flow_run_id": str(plan.flow_run_id),
        "operation_id": str(operation_id),
        "challenge_capability": challenge,
        "launcher_instance_id": str(uuid4()),
        "launcher_host_id": str(identity.host_id),
        "launcher_boot_id": str(identity.boot_id),
        "launcher_process_id": identity.launcher_process_id,
        "launcher_start_ticks": identity.launcher_start_ticks,
        "new_holder_id": str(uuid4()),
        "new_holder_capability": uuid4().hex + uuid4().hex,
        "new_holder_event_public_key": PUBLIC,
        "new_supervisor_id": str(uuid4()),
        "new_supervisor_capability": uuid4().hex + uuid4().hex,
        "death_witness": witness,
    }
    retirement = {
        "flow_run_id": str(plan.flow_run_id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": str(enrollment_id),
        "challenge_capability": challenge,
        "death_witness": witness,
        "error": "P2 cannot use bounded whole-Flow retirement",
    }
    for path, request in (
        ("authorize", authorization),
        ("confirm-death", confirmation),
        ("retire-verified", retirement),
    ):
        result = await worker_api.post(
            f"/streams/v1/deployments/replacements/{path}",
            json=request,
            headers=headers,
        )
        assert result.status_code == 422, result.text
        assert result.json()["detail"]["code"] == "UNSUPPORTED_PROFILE"
    async with db.session_context() as check:
        authority = await check.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == plan.flow_run_id
            )
        )
        assert authority is not None
        assert authority.generation == 1 and authority.state == "ACTIVE"
        assert str(authority.holder_id) == headers["X-Prefect-Stream-Execution-Holder"]
        assert (
            await check.get(db.FlowRun, plan.flow_run_id)
        ).state_type.value == "RUNNING"
        graph = await check.get(db.StreamGraphRevision, plan.id)
        assert graph is not None and graph.state == "ADMITTED"
        assert graph.startup_completed_at is None
        subtasks = list(
            (
                await check.scalars(
                    sa.select(db.StreamPhysicalSubtask).where(
                        db.StreamPhysicalSubtask.graph_revision_id == plan.id
                    )
                )
            ).all()
        )
        assert len(subtasks) == 4
        assert {row.current_grant_id for row in subtasks} == set(grant_ids)
        assert all(
            row.physical_epoch == 1 and not row.physical_slot_released
            for row in subtasks
        )
        grants = list(
            (
                await check.scalars(
                    sa.select(db.StreamSubtaskGrant).where(
                        db.StreamSubtaskGrant.id.in_(grant_ids)
                    )
                )
            ).all()
        )
        assert len(grants) == 4 and all(row.state == "REGISTERED" for row in grants)
        assert (
            await check.scalar(
                sa.select(sa.func.count()).select_from(
                    db.NativeStreamReplacementOperation
                )
            )
            == 0
        )
        assert (
            await check.scalar(
                sa.select(sa.func.count()).select_from(db.NativeStreamControlOperation)
            )
            == 0
        )
        assert (
            await check.scalar(sa.select(sa.func.count()).select_from(db.StreamRecord))
            == 0
        )
