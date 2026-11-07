"""Recovery discovery shares Worker identity without weakening mutation fences."""

import asyncio
from datetime import timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import native_deployments as m
from prefect.server.models import stream_dag_executions, workers
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import actions
from prefect.server.schemas.native_deployments import (
    NativeStreamProcessRegistrationRequest,
    NativeStreamRecoveryCandidate,
    NativeStreamReplacementAuthorization,
    NativeStreamReplacementAuthorizeRequest,
    NativeStreamWorkerEnrollRequest,
)
from prefect.server.schemas.statuses import WorkerStatus
from prefect.server.schemas.streams import StreamExecutionCredentials
from prefect.types._datetime import now
from tests.server.api.test_native_deployments import (
    _enrollment_body,
    _native_flow_run,
)
from tests.server.api.test_stream_dag_executions import (
    execution_api as execution_api,  # noqa: PLC0414 -- pytest fixture re-export
)
from tests.server.models.test_stream_dag_executions import ready_deployed_region

pytestmark = pytest.mark.service("database")


@pytest.fixture(autouse=True)
async def postgres17(db: PrefectDBInterface) -> None:
    async with db.session_context() as probe:
        connection = await probe.connection()
        if (
            connection.dialect.name != "postgresql"
            or (connection.dialect.server_version_info or (0,))[0] != 17
        ):
            pytest.skip("Recovery discovery lock semantics require PostgreSQL 17")


async def _online_worker(db: PrefectDBInterface) -> tuple[UUID, UUID]:
    async with db.session_context(begin_transaction=True) as transaction:
        pool = await workers.create_work_pool(
            session=transaction,
            work_pool=actions.WorkPoolCreate(
                name="recovery-discovery-" + uuid4().hex, type="process"
            ),
        )
        await workers.worker_heartbeat(
            session=transaction,
            work_pool_id=pool.id,
            worker_name="discovery-worker-" + uuid4().hex,
        )
        worker = await transaction.scalar(
            sa.select(db.Worker).where(db.Worker.work_pool_id == pool.id)
        )
        assert worker is not None and worker.status == WorkerStatus.ONLINE
        return worker.id, pool.id


async def _enrolled_launch(
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
) -> NativeStreamWorkerEnrollRequest:
    flow, worker, pool_id = await _native_flow_run(session, db, deployment)
    plan = await m.read_plan(session, db, flow.id)
    request = NativeStreamWorkerEnrollRequest.model_validate(
        _enrollment_body(flow.id, worker.id, pool_id, plan.declaration_digest)
    )
    await m.enroll_worker(session, db, request)
    await session.commit()
    return request


async def _worker_blocker(
    db: PrefectDBInterface,
    waiter: int,
    holder: int,
    pending: asyncio.Task[Any],
    *,
    clause: str = "",
) -> None:
    async with db.session_context() as observer:
        async with asyncio.timeout(10):
            while True:
                await observer.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
                row = (
                    await observer.execute(
                        sa.text(
                            "SELECT query, pg_blocking_pids(pid) AS blockers "
                            "FROM pg_stat_activity WHERE pid=:pid "
                            "AND datname=current_database()"
                        ),
                        {"pid": waiter},
                    )
                ).one()
                if holder in row.blockers:
                    assert "worker" in row.query and clause in row.query
                    return
                if pending.done():
                    await pending
                    pytest.fail("Worker operation completed before its required fence")
                await asyncio.sleep(0.01)


async def test_recovery_candidates_coexist_with_the_active_dag_data_prefix(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    _, _, children = await ready_deployed_region(db, session, execution_api, deployment)
    child = children[0]
    started: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def discover() -> list[NativeStreamRecoveryCandidate]:
        async with db.session_context(begin_transaction=True) as discovery:
            pid = await discovery.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            started.set_result(pid)
            return await m.recovery_candidates(
                discovery,
                db,
                worker_id=child.worker_id,
                work_pool_id=child.principal.work_pool_id,
                host_id=child.principal.host_id,
            )

    async with db.session_context(begin_transaction=True) as data:
        await stream_dag_executions.lock_dag_child(
            data, db, child.credentials, ready=True, active=True
        )
        holder_pid = await data.scalar(sa.text("SELECT pg_backend_pid()"))
        assert isinstance(holder_pid, int)
        pending = asyncio.create_task(discover())
        try:
            async with asyncio.timeout(10):
                waiter_pid = await started
            try:
                result = await asyncio.wait_for(asyncio.shield(pending), timeout=5)
            except TimeoutError:
                await _worker_blocker(
                    db, waiter_pid, holder_pid, pending, clause="FOR UPDATE"
                )
                pytest.fail(
                    "Read-only recovery discovery blocked on the DAG DATA Worker lock"
                )
            # DAG workers have no bounded-linear recovery candidates. The empty
            # result must still complete while the real DATA prefix remains held.
            assert result == []
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("identity_kind", ["LAUNCHER", "CHILD"])
async def test_recovery_candidates_keep_the_exact_bounded_launch_dto(
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
    identity_kind: Literal["LAUNCHER", "CHILD"],
):
    enrollment = await _enrolled_launch(db, session, deployment)
    host_id, boot_id = enrollment.launcher_host_id, enrollment.launcher_boot_id
    process_id, start_ticks = (
        enrollment.launcher_process_id,
        enrollment.launcher_start_ticks,
    )
    if identity_kind == "CHILD":
        registration = NativeStreamProcessRegistrationRequest(
            flow_run_id=enrollment.flow_run_id,
            operation_id=uuid4(),
            enrollment_operation_id=enrollment.operation_id,
            generation=1,
            launcher_instance_id=enrollment.launcher_instance_id,
            host_id=uuid4(),
            boot_id=uuid4(),
            child_process_id=4321,
            process_start_ticks=9876,
            launch_nonce_digest="b" * 64,
        )
        await m.register_process(
            session,
            db,
            registration,
            StreamExecutionCredentials(
                generation=1,
                holder_id=enrollment.holder_id,
                capability=enrollment.holder_capability,
            ),
        )
        host_id, boot_id = registration.host_id, registration.boot_id
        process_id, start_ticks = (
            registration.child_process_id,
            registration.process_start_ticks,
        )
    stale = now("UTC") - m.LAUNCH_RENEWAL_STALE_AFTER - timedelta(seconds=1)
    await session.execute(
        sa.update(db.NativeStreamLaunchIdentity)
        .where(db.NativeStreamLaunchIdentity.flow_run_id == enrollment.flow_run_id)
        .values(last_renewed_at=stale)
    )
    await session.commit()
    candidates = await m.recovery_candidates(
        session,
        db,
        worker_id=enrollment.worker_id,
        work_pool_id=enrollment.work_pool_id,
        host_id=host_id,
    )
    assert candidates == [
        NativeStreamRecoveryCandidate(
            flow_run_id=enrollment.flow_run_id,
            deployment_id=deployment.id,
            generation=1,
            work_pool_id=enrollment.work_pool_id,
            worker_id=enrollment.worker_id,
            launcher_instance_id=enrollment.launcher_instance_id,
            enrollment_operation_id=enrollment.operation_id,
            launch_profile=enrollment.launch_profile,
            identity_kind=identity_kind,
            host_id=host_id,
            boot_id=boot_id,
            child_process_id=process_id,
            process_start_ticks=start_ticks,
            last_renewed_at=stale,
            verification_not_before=stale + m.LAUNCH_RENEWAL_STALE_AFTER,
        )
    ]
    assert (
        await m.recovery_candidates(
            session,
            db,
            worker_id=enrollment.worker_id,
            work_pool_id=enrollment.work_pool_id,
            host_id=uuid4(),
        )
        == []
    )


@pytest.mark.parametrize("change", ["status", "pool", "delete"])
async def test_recovery_candidates_refuse_fresh_worker_identity_drift(
    db: PrefectDBInterface, change: str
):
    worker_id, pool_id = await _online_worker(db)
    _, other_pool_id = await _online_worker(db)
    async with db.session_context() as discovery:
        cached = await discovery.get(db.Worker, worker_id)
        assert cached is not None and cached.status == WorkerStatus.ONLINE
        await discovery.commit()
        async with db.session_context(begin_transaction=True) as mutation:
            statement = (
                sa.delete(db.Worker)
                if change == "delete"
                else sa.update(db.Worker).values(
                    **(
                        {"status": WorkerStatus.OFFLINE}
                        if change == "status"
                        else {"work_pool_id": other_pool_id}
                    )
                )
            )
            await mutation.execute(statement.where(db.Worker.id == worker_id))
        # This session deliberately retains the previously online identity.
        assert cached.status == WorkerStatus.ONLINE and cached.work_pool_id == pool_id
        with pytest.raises(NativeStreamError, match="Worker is not online") as refusal:
            await m.recovery_candidates(
                discovery,
                db,
                worker_id=worker_id,
                work_pool_id=pool_id,
                host_id=uuid4(),
            )
        assert refusal.value.code == "IDENTITY_CONFLICT"


async def test_authorize_replacement_keeps_the_default_exclusive_worker_fence(
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
):
    enrollment = await _enrolled_launch(db, session, deployment)
    started: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    request = NativeStreamReplacementAuthorizeRequest(
        flow_run_id=enrollment.flow_run_id,
        operation_id=uuid4(),
        expected_generation=1,
        enrollment_operation_id=enrollment.operation_id,
        worker_id=enrollment.worker_id,
        work_pool_id=enrollment.work_pool_id,
        host_id=enrollment.launcher_host_id,
        identity_kind="LAUNCHER",
        challenge_capability="c" * 64,
    )

    async def authorize() -> NativeStreamReplacementAuthorization:
        async with db.session_context(begin_transaction=True) as mutation:
            pid = await mutation.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            started.set_result(pid)
            return await m.authorize_replacement(
                mutation,
                db,
                request,
                StreamExecutionCredentials(
                    generation=1,
                    holder_id=enrollment.holder_id,
                    capability=enrollment.holder_capability,
                ),
            )

    pending = None
    try:
        async with db.session_context(begin_transaction=True) as holder:
            await holder.execute(
                sa.select(db.Worker.id)
                .where(db.Worker.id == enrollment.worker_id)
                .with_for_update(read=True)
            )
            holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            pending = asyncio.create_task(authorize())
            async with asyncio.timeout(10):
                waiter_pid = await started
            await _worker_blocker(
                db, waiter_pid, holder_pid, pending, clause="FOR UPDATE"
            )
            assert not pending.done()
        async with asyncio.timeout(10):
            result = await pending
        assert result.state == "AUTHORIZED"
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("change", ["status", "name", "pool", "delete"])
async def test_recovery_candidate_share_blocks_worker_mutation(
    db: PrefectDBInterface, change: str
):
    worker_id, pool_id = await _online_worker(db)
    _, other_pool_id = await _online_worker(db)
    started: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def mutate() -> None:
        async with db.session_context(begin_transaction=True) as mutation:
            pid = await mutation.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            started.set_result(pid)
            statement = (
                sa.delete(db.Worker)
                if change == "delete"
                else sa.update(db.Worker).values(
                    **{
                        "status": {"status": WorkerStatus.OFFLINE},
                        "name": {"name": "renamed-discovery-worker"},
                        "pool": {"work_pool_id": other_pool_id},
                    }[change]
                )
            )
            await mutation.execute(statement.where(db.Worker.id == worker_id))

    pending = None
    try:
        async with db.session_context(begin_transaction=True) as discovery:
            assert (
                await m.recovery_candidates(
                    discovery,
                    db,
                    worker_id=worker_id,
                    work_pool_id=pool_id,
                    host_id=uuid4(),
                )
                == []
            )
            holder_pid = await discovery.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            pending = asyncio.create_task(mutate())
            async with asyncio.timeout(10):
                waiter_pid = await started
            await _worker_blocker(db, waiter_pid, holder_pid, pending)
            assert not pending.done()
        async with asyncio.timeout(10):
            await pending
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
