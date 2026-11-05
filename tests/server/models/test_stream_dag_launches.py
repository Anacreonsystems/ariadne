"""Server-recorded process births and exact replay under a winning stop fence."""

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import stream_dag_executions as lifecycle
from prefect.server.models import stream_dag_launches as m
from prefect.server.models import stream_workers
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import stream_dag_executions as s
from prefect.server.schemas import stream_subtasks as physical
from prefect.server.schemas.native_deployments import NativeStreamProcessDeathWitness
from prefect.server.services.task_run_recorder import dispatch_stream_subtask_event
from prefect.types._datetime import now
from tests.server.api.test_native_deployments import _observe_heartbeat_blocker
from tests.server.api.test_stream_dag_executions import (
    execution_api as execution_api,
)
from tests.server.api.test_stream_dag_executions import (
    parent_body,
)
from tests.server.models.test_stream_dag_executions import (
    produce_and_finish_region,
    ready_deployed_region,
    signed_child_event,
    start_deployed_region,
)

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def _renewal_grant_row(db: PrefectDBInterface, grant_id: UUID) -> dict[str, Any]:
    async with db.session_context() as check:
        row = (
            (
                await check.execute(
                    sa.select(db.StreamSubtaskGrant.__table__).where(
                        db.StreamSubtaskGrant.id == grant_id
                    )
                )
            )
            .mappings()
            .one()
        )
        return dict(row)


async def _hold_data_reader_prefix(
    holder: AsyncSession, db: PrefectDBInterface, flow_id: UUID, graph_id: UUID
) -> None:
    for model, condition in (
        (db.FlowRun, db.FlowRun.id == flow_id),
        (
            db.StreamExecutionAuthority,
            db.StreamExecutionAuthority.flow_run_id == flow_id,
        ),
        (db.StreamGraphRevision, db.StreamGraphRevision.id == graph_id),
    ):
        await holder.execute(
            sa.select(model.id).where(condition).with_for_update(read=True)
        )


async def test_worker_renewal_commits_under_data_reader_prefix_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    tmp_path: Path,
):
    plan, _, children = await ready_deployed_region(
        db, session, execution_api, deployment
    )
    child = children[0]
    subtask_id, grant_id = child.bootstrap.assignment.id, child.bootstrap.grant.id
    request = physical.StreamWorkerRenewRequest(
        principal=child.principal, grant_id=grant_id
    )
    started: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def rows() -> dict[str, list[Any]]:
        async with db.session_context() as check:
            return {
                model.__tablename__: (
                    await check.execute(sa.select(model.__table__).order_by(model.id))
                ).all()
                for model in (
                    db.FlowRun,
                    db.StreamExecutionAuthority,
                    db.StreamGraphRevision,
                    db.StreamPhysicalSubtask,
                    db.StreamDAGLaunchAttempt,
                    db.StreamPhysicalSlotAllocation,
                    db.StreamSubtaskAllocation,
                    db.Worker,
                    db.StreamWorkerCapability,
                )
            }

    async def renew() -> physical.StreamSubtaskGrant:
        async with db.session_context(begin_transaction=True) as transaction:
            pid = await transaction.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            started.set_result(pid)
            return await stream_workers.renew_subtask(
                transaction, db, child.worker_id, subtask_id, request
            )

    before = await rows()
    original_grant = await _renewal_grant_row(db, grant_id)
    task: asyncio.Task[physical.StreamSubtaskGrant] | None = None
    try:
        async with db.session_context(begin_transaction=True) as holder:
            holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            await _hold_data_reader_prefix(holder, db, plan.flow_run_id, plan.id)
            task = asyncio.create_task(renew())
            waiter_pid = await asyncio.wait_for(asyncio.shield(started), timeout=5)
            try:
                result = await asyncio.wait_for(asyncio.shield(task), timeout=5)
                assert holder.in_transaction()
                assert result.id == grant_id
                assert result.state == "REGISTERED"
            except TimeoutError:
                async with db.session_context() as observer:
                    blocked = (
                        (
                            await observer.execute(
                                sa.text(
                                    "SELECT pg_blocking_pids(pid) AS blockers, "
                                    "wait_event_type, wait_event, "
                                    "query LIKE 'SELECT flow_run.%' AS selecting_flow "
                                    "FROM pg_stat_activity "
                                    "WHERE pid = :waiter AND datname = current_database()"
                                ),
                                {"waiter": waiter_pid},
                            )
                        )
                        .mappings()
                        .one()
                    )
                witness = {
                    "holder_pid": holder_pid,
                    "waiter_pid": waiter_pid,
                    "holder_still_open": holder.in_transaction(),
                    "test_deadline_seconds": 5,
                    "platform_timeout_unchanged": True,
                    **dict(blocked),
                }
                (tmp_path / "renewal-lock-witness.json").write_text(
                    json.dumps(witness, indent=2) + "\n"
                )
                assert holder_pid in blocked["blockers"]
                assert blocked["selecting_flow"]
                raise
            finally:
                await holder.commit()
                await asyncio.wait_for(task, timeout=5)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert await rows() == before
    renewed = await _renewal_grant_row(db, grant_id)
    assert renewed["last_renewed_at"] > original_grant["last_renewed_at"]
    unchanged = set(renewed) - {"last_renewed_at", "updated"}
    assert {key: renewed[key] for key in unchanged} == {
        key: original_grant[key] for key in unchanged
    }


async def test_worker_renewal_http_commits_under_reader_prefix_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, _, children = await ready_deployed_region(
        db, session, execution_api, deployment
    )
    child = children[0]
    request = physical.StreamWorkerRenewRequest(
        principal=child.principal, grant_id=child.bootstrap.grant.id
    )
    transport = execution_api._transport
    assert isinstance(transport, httpx.ASGITransport)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=transport.app, raise_app_exceptions=False),
        base_url=execution_api.base_url,
        auth=execution_api.auth,
    ) as api:
        async with db.session_context(begin_transaction=True) as holder:
            await _hold_data_reader_prefix(holder, db, plan.flow_run_id, plan.id)
            response = await asyncio.wait_for(
                api.post(
                    f"/streams/v2/workers/{child.worker_id}/subtasks/"
                    f"{child.bootstrap.assignment.id}/renew",
                    json=request.model_dump(mode="json"),
                ),
                timeout=5,
            )
            assert holder.in_transaction()
            assert response.status_code == 200, response.text
            result = physical.StreamSubtaskGrant.model_validate(response.json())
            assert result.id == child.bootstrap.grant.id
            assert result.state == "REGISTERED"


@pytest.mark.parametrize("held_lock", ["flow", "authority", "graph"])
async def test_worker_renewal_waits_for_exclusive_parent_fence_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    held_lock: str,
):
    plan, _, children = await ready_deployed_region(
        db, session, execution_api, deployment
    )
    child = children[0]
    request = physical.StreamWorkerRenewRequest(
        principal=child.principal, grant_id=child.bootstrap.grant.id
    )
    before = await _renewal_grant_row(db, request.grant_id)
    started: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def renew() -> physical.StreamSubtaskGrant:
        async with db.session_context(begin_transaction=True) as transaction:
            pid = await transaction.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            started.set_result(pid)
            return await stream_workers.renew_subtask(
                transaction, db, child.worker_id, child.bootstrap.assignment.id, request
            )

    task: asyncio.Task[physical.StreamSubtaskGrant] | None = None
    try:
        async with db.session_context(begin_transaction=True) as holder:
            holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            for name, model, condition in (
                ("flow", db.FlowRun, db.FlowRun.id == plan.flow_run_id),
                (
                    "authority",
                    db.StreamExecutionAuthority,
                    db.StreamExecutionAuthority.flow_run_id == plan.flow_run_id,
                ),
                ("graph", db.StreamGraphRevision, db.StreamGraphRevision.id == plan.id),
            ):
                await holder.execute(
                    sa.select(model.id)
                    .where(condition)
                    .with_for_update(read=name != held_lock)
                )
                if name == held_lock:
                    table = model.__tablename__
                    break
            task = asyncio.create_task(renew())
            waiter_pid = await asyncio.wait_for(asyncio.shield(started), timeout=5)
            await _observe_heartbeat_blocker(db, waiter_pid, holder_pid, table)
            assert not task.done()
            assert await _renewal_grant_row(db, request.grant_id) == before
            await holder.commit()
            response = await asyncio.wait_for(task, timeout=5)
            assert response.id == request.grant_id
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("fault", ["worker", "principal", "grant", "control"])
async def test_worker_renewal_preserves_current_control_and_identity_fences_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    fault: str,
):
    plan, credentials, children = await ready_deployed_region(
        db, session, execution_api, deployment
    )
    child = children[0]
    principal = child.principal
    grant_id = child.bootstrap.grant.id
    worker_id = child.worker_id
    if fault == "worker":
        worker_id = next(
            item.worker_id for item in children if item.worker_id != worker_id
        )
    elif fault == "principal":
        principal = principal.model_copy(
            update={"launcher_start_ticks": principal.launcher_start_ticks + 1}
        )
    elif fault == "grant":
        grant_id = uuid4()
    else:
        async with db.session_context(begin_transaction=True) as controller:
            await lifecycle.cancel_stream_dag(
                controller,
                db,
                plan.id,
                s.StreamDAGCancelRequest.model_validate(
                    parent_body(plan.id, "cancel", plan.graph_digest)
                ),
                credentials,
            )
    before = await _renewal_grant_row(db, child.bootstrap.grant.id)
    async with db.session_context(begin_transaction=True) as transaction:
        with pytest.raises(NativeStreamError) as error:
            await stream_workers.renew_subtask(
                transaction,
                db,
                worker_id,
                child.bootstrap.assignment.id,
                physical.StreamWorkerRenewRequest(
                    principal=principal, grant_id=grant_id
                ),
            )
        assert error.value.code == (
            "SUBTASK_SCOPE_DENIED"
            if fault in {"worker", "principal"}
            else "SUBTASK_FENCED"
        )
    assert await _renewal_grant_row(db, child.bootstrap.grant.id) == before


@pytest.mark.parametrize("new_replacement", [True, False])
async def test_terminal_replacement_prepare_and_accepted_replay_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    new_replacement: bool,
):
    plan, _, children = await ready_deployed_region(
        db, session, execution_api, deployment
    )
    child = next(
        item for item in children if item.bootstrap.context.operator_uid == "B"
    )
    subtask_id, grant = child.bootstrap.assignment.id, child.bootstrap.grant
    start_id = uuid5(plan.id, "prefect-stream-dag-start-v1")
    initial = s.StreamDAGLaunchPrepareRequest(
        principal=child.principal,
        operation_id=uuid5(subtask_id, "prefect-stream-dag-launch-v1/" + str(start_id)),
        start_operation_id=start_id,
        replacement_operation_id=None,
    )
    async with db.session_context(begin_transaction=True) as transaction:
        accepted = await m.prepare_worker_stream_dag_launch(
            transaction, db, child.worker_id, subtask_id, initial
        )
    await produce_and_finish_region(db, children, documents=1)
    async with db.session_context(begin_transaction=True) as transaction:
        # Keep the normal signed child event/recorder path: no Task state SQL.
        completed = await dispatch_stream_subtask_event(
            transaction,
            db,
            subtask_id,
            signed_child_event(child, "COMPLETED"),
            child.credentials,
        )
        assert completed.accepted and completed.state_type.value == "COMPLETED"
        request = initial
        if new_replacement:
            replacement_id = uuid4()
            await m.authorize_replacement(
                transaction,
                db,
                child.worker_id,
                subtask_id,
                physical.StreamWorkerReplacementAuthorizeRequest(
                    principal=child.principal,
                    replacement=physical.StreamSubtaskReplacementAuthorizeRequest(
                        operation_id=replacement_id,
                        expected_grant_id=grant.id,
                        expected_physical_epoch=grant.physical_epoch,
                        host_id=grant.host_id,
                        challenge_capability="ab" * 32,
                    ),
                ),
            )
            assert (
                grant.child_process_id is not None
                and grant.process_start_ticks is not None
            )
            await m.close_worker_stream_dag_launch(
                transaction,
                db,
                child.worker_id,
                subtask_id,
                child.bootstrap.launch_attempt_id,
                s.StreamDAGLaunchCloseRequest(
                    principal=child.principal,
                    operation_id=uuid5(
                        child.bootstrap.launch_attempt_id, "prefect-stream-dag-close-v1"
                    ),
                    proof=s.StreamDAGLaunchDeathProof(
                        kind="DEATH",
                        death_witness=NativeStreamProcessDeathWitness(
                            kind="WAITPID",
                            observed_at=now("UTC"),
                            host_id=grant.host_id,
                            boot_id=grant.boot_id,
                            child_process_id=grant.child_process_id,
                            process_start_ticks=grant.process_start_ticks,
                            exit_code=0,
                        ),
                    ),
                ),
            )
            request = s.StreamDAGLaunchPrepareRequest(
                principal=child.principal,
                operation_id=uuid5(
                    subtask_id, "prefect-stream-dag-launch-v1/" + str(replacement_id)
                ),
                start_operation_id=start_id,
                replacement_operation_id=replacement_id,
            )
        tables = [
            db.TaskRun.__table__,
            db.TaskRunState.__table__,
            db.StreamGraphRevision.__table__,
            db.StreamPhysicalSubtask.__table__,
            db.StreamPartition.__table__,
            db.StreamSubtaskGrant.__table__,
            db.StreamSubtaskReplacement.__table__,
            db.StreamDAGLaunchAttempt.__table__,
            db.StreamSubtaskAllocation.__table__,
            db.StreamPhysicalSlotAllocation.__table__,
        ]
        before = [
            (await transaction.execute(sa.select(table).order_by(table.c.id))).all()
            for table in tables
        ]
        if new_replacement:
            with pytest.raises(NativeStreamError) as error:
                await m.prepare_worker_stream_dag_launch(
                    transaction, db, child.worker_id, subtask_id, request
                )
            assert error.value.code == "TERMINAL_RECONCILIATION_REQUIRED"
        else:
            replay = await m.prepare_worker_stream_dag_launch(
                transaction, db, child.worker_id, subtask_id, request
            )
            assert replay.receipt == accepted.receipt
            assert replay.current == accepted.current
        after = [
            (await transaction.execute(sa.select(table).order_by(table.c.id))).all()
            for table in tables
        ]
        assert after == before


async def test_startup_abort_worker_prefix_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, _, workers = await start_deployed_region(
        db, session, execution_api, deployment
    )
    async with db.session_context() as check:
        assigned_workers = list(
            (
                await check.scalars(
                    sa.select(db.StreamPhysicalSubtask.assigned_worker_id).where(
                        db.StreamPhysicalSubtask.graph_revision_id == plan.id
                    )
                )
            ).all()
        )
    assert len(assigned_workers) == 4
    assert set(assigned_workers) == {worker_id for worker_id, _ in workers}
    assert all(assigned_workers.count(worker_id) == 2 for worker_id, _ in workers)
    early, late = sorted(workers, key=lambda row: row[0])
    started: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def abort() -> s.StreamDAGWorkerStartupAbortResult:
        async with db.session_context(begin_transaction=True) as transaction:
            pid = await transaction.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            started.set_result(pid)
            return await m.abort_worker_stream_dag_startup(
                transaction,
                db,
                late[0],
                plan.id,
                s.StreamDAGWorkerStartupAbortRequest(
                    principal=late[1],
                    start_operation_id=parent_body(plan.id, "start", plan.graph_digest)[
                        "operation_id"
                    ],
                    graph_digest=plan.graph_digest,
                    routing_digest=parent_body(plan.id, "start", plan.graph_digest)[
                        "routing_digest"
                    ],
                ),
            )

    task: asyncio.Task[s.StreamDAGWorkerStartupAbortResult] | None = None
    try:
        async with db.session_context(begin_transaction=True) as holder:
            holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            await holder.execute(
                sa.select(db.Worker.id)
                .where(db.Worker.id == early[0])
                .with_for_update()
            )
            task = asyncio.create_task(abort())
            waiter = await asyncio.wait_for(asyncio.shield(started), timeout=5)
            await _observe_heartbeat_blocker(db, waiter, holder_pid, "worker")
            async with db.session_context(begin_transaction=True) as probe:
                await probe.execute(
                    sa.select(db.Worker.id)
                    .where(db.Worker.id == late[0])
                    .with_for_update(nowait=True)
                )
            assert not task.done()
        result = await asyncio.wait_for(asyncio.shield(task), timeout=5)
        assert (
            result.execution.phase == "STOPPING"
            and result.execution.control is not None
        )
        assert (
            result.execution.control.kind == "STARTUP_ABORT"
            and result.execution.counts.physical_slots_remaining == 4
        )
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with db.session_context() as check:
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamDAGControlTarget)
                .where(db.StreamDAGControlTarget.graph_revision_id == plan.id)
            )
            == 4
        )


async def test_active_graph_refuses_startup_abort_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, _, children = await ready_deployed_region(
        db, session, execution_api, deployment
    )
    child = children[0]
    async with db.session_context(begin_transaction=True) as transaction:
        with pytest.raises(NativeStreamError) as error:
            await m.abort_worker_stream_dag_startup(
                transaction,
                db,
                child.worker_id,
                plan.id,
                s.StreamDAGWorkerStartupAbortRequest(
                    principal=child.principal,
                    start_operation_id=parent_body(plan.id, "start", plan.graph_digest)[
                        "operation_id"
                    ],
                    graph_digest=plan.graph_digest,
                    routing_digest=child.bootstrap.context.routing_digest,
                ),
            )
        assert error.value.code == "ACTIVATION_ALREADY_COMMITTED"
        graph = await transaction.get(db.StreamGraphRevision, plan.id)
        assert graph is not None and graph.current_control_operation_id is None


async def test_launch_lost_ack_and_delayed_record_after_stop_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, credentials, workers = await start_deployed_region(
        db, session, execution_api, deployment
    )
    worker_id, principal = workers[0]
    async with db.session_context(begin_transaction=True) as transaction:
        assignments = await m.poll_worker_stream_dag_subtasks(
            transaction,
            db,
            worker_id,
            s.StreamDAGWorkerPollRequest(principal=principal),
        )
        target = assignments[0]
        prepare = s.StreamDAGLaunchPrepareRequest(
            principal=principal,
            operation_id=uuid5(
                target.assignment.id,
                "prefect-stream-dag-launch-v1/" + str(target.start_operation_id),
            ),
            start_operation_id=target.start_operation_id,
            replacement_operation_id=None,
        )
        first = await m.prepare_worker_stream_dag_launch(
            transaction, db, worker_id, target.assignment.id, prepare
        )
        assert (
            first.current.state == "PREPARED" and first.disposition == "SPAWN_ALLOWED"
        )
        assert (
            await m.prepare_worker_stream_dag_launch(
                transaction, db, worker_id, target.assignment.id, prepare
            )
            == first
        )
    async with db.session_context(begin_transaction=True) as transaction:
        stopped = await lifecycle.fail_stream_dag(
            transaction,
            db,
            plan.id,
            s.StreamDAGFailRequest.model_validate(
                parent_body(plan.id, "fail", plan.graph_digest)
            ),
            credentials,
        )
        assert stopped.execution.phase == "STOPPING"
        assert stopped.execution.counts.physical_slots_remaining == 4
        record = s.StreamDAGLaunchRecordRequest(
            principal=principal,
            operation_id=uuid5(first.current.id, "prefect-stream-dag-record-v1"),
            child_process_id=4545,
            process_start_ticks=99999,
            local_journal_digest="11" * 32,
        )
        recorded = await m.record_worker_stream_dag_launch(
            transaction, db, worker_id, target.assignment.id, first.current.id, record
        )
        assert recorded.disposition == "STOP_REQUIRED"
        assert recorded.current.state == "PID_RECORDED"
        assert (
            await m.record_worker_stream_dag_launch(
                transaction,
                db,
                worker_id,
                target.assignment.id,
                first.current.id,
                record,
            )
            == recorded
        )
        with pytest.raises(NativeStreamError) as error:
            await m.record_worker_stream_dag_launch(
                transaction,
                db,
                worker_id,
                target.assignment.id,
                first.current.id,
                record.model_copy(update={"child_process_id": 4546}),
            )
        assert error.value.code == "IDENTITY_CONFLICT"
        with pytest.raises(NativeStreamError) as error:
            await m.close_worker_stream_dag_launch(
                transaction,
                db,
                worker_id,
                target.assignment.id,
                first.current.id,
                s.StreamDAGLaunchCloseRequest(
                    principal=principal,
                    operation_id=uuid5(first.current.id, "prefect-stream-dag-close-v1"),
                    proof=s.StreamDAGLaunchUnstartedProof(
                        kind="CLOSED_UNSTARTED",
                        local_journal_digest="22" * 32,
                        recovery_complete=True,
                        spawn_fence_closed=True,
                    ),
                ),
            )
        assert error.value.code == "PHYSICAL_STOP_REQUIRED"
        assert (
            await lifecycle.read_stream_dag_execution(
                transaction, db, plan.id, credentials
            )
        ).counts.physical_slots_remaining == 4


@pytest.mark.parametrize("record_pid", [False, True])
async def test_all_attempts_close_before_last_stop_commits_failed_flow_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    record_pid: bool,
):
    plan, credentials, workers = await start_deployed_region(
        db, session, execution_api, deployment
    )
    worker_id, principal = workers[0]
    async with db.session_context(begin_transaction=True) as transaction:
        assignment = (
            await m.poll_worker_stream_dag_subtasks(
                transaction,
                db,
                worker_id,
                s.StreamDAGWorkerPollRequest(principal=principal),
            )
        )[0]
        target_id = assignment.assignment.id
        prepared = await m.prepare_worker_stream_dag_launch(
            transaction,
            db,
            worker_id,
            target_id,
            s.StreamDAGLaunchPrepareRequest(
                principal=principal,
                operation_id=uuid5(
                    target_id,
                    "prefect-stream-dag-launch-v1/"
                    + str(assignment.start_operation_id),
                ),
                start_operation_id=assignment.start_operation_id,
                replacement_operation_id=None,
            ),
        )
        if record_pid:
            await m.record_worker_stream_dag_launch(
                transaction,
                db,
                worker_id,
                target_id,
                prepared.current.id,
                s.StreamDAGLaunchRecordRequest(
                    principal=principal,
                    operation_id=uuid5(
                        prepared.current.id, "prefect-stream-dag-record-v1"
                    ),
                    child_process_id=5555,
                    process_start_ticks=12345,
                    local_journal_digest="ab" * 32,
                ),
            )
        failed = await lifecycle.fail_stream_dag(
            transaction,
            db,
            plan.id,
            s.StreamDAGFailRequest.model_validate(
                parent_body(plan.id, "fail", plan.graph_digest)
            ),
            credentials,
        )
        assert failed.execution.control is not None
        control_id = failed.execution.control.operation_id
        stop = s.StreamDAGWorkerStopRequest(
            principal=principal,
            operation_id=uuid5(
                control_id, "prefect-stream-dag-stop-v1/" + str(target_id)
            ),
            control_operation_id=control_id,
        )
        with pytest.raises(NativeStreamError) as error:
            await m.stop_worker_stream_dag_subtask(
                transaction, db, worker_id, target_id, stop
            )
        assert error.value.code == "PHYSICAL_STOP_REQUIRED"
        flow = await transaction.get(db.FlowRun, plan.flow_run_id)
        assert flow is not None and flow.state_type.value == "RUNNING"
        proof: s.StreamDAGLaunchDeathProof | s.StreamDAGLaunchUnstartedProof
        if record_pid:
            proof = s.StreamDAGLaunchDeathProof(
                kind="DEATH",
                death_witness=NativeStreamProcessDeathWitness(
                    kind="WAITPID",
                    observed_at=now("UTC"),
                    host_id=principal.host_id,
                    boot_id=principal.boot_id,
                    child_process_id=5555,
                    process_start_ticks=12345,
                    exit_code=0,
                ),
            )
        else:
            proof = s.StreamDAGLaunchUnstartedProof(
                kind="CLOSED_UNSTARTED",
                local_journal_digest="cd" * 32,
                recovery_complete=True,
                spawn_fence_closed=True,
            )
        close = s.StreamDAGLaunchCloseRequest(
            principal=principal,
            operation_id=uuid5(prepared.current.id, "prefect-stream-dag-close-v1"),
            proof=proof,
        )
        receipt = await m.close_worker_stream_dag_launch(
            transaction, db, worker_id, target_id, prepared.current.id, close
        )
        assert (
            await m.close_worker_stream_dag_launch(
                transaction, db, worker_id, target_id, prepared.current.id, close
            )
            == receipt
        )
    stop_receipts = []
    for own_worker, identity in workers:
        async with db.session_context(begin_transaction=True) as transaction:
            assignments = await m.poll_worker_stream_dag_subtasks(
                transaction,
                db,
                own_worker,
                s.StreamDAGWorkerPollRequest(principal=identity),
            )
            for own in assignments:
                request = s.StreamDAGWorkerStopRequest(
                    principal=identity,
                    operation_id=uuid5(
                        control_id,
                        "prefect-stream-dag-stop-v1/" + str(own.assignment.id),
                    ),
                    control_operation_id=control_id,
                )
                stopped = await m.stop_worker_stream_dag_subtask(
                    transaction, db, own_worker, own.assignment.id, request
                )
                assert stopped.physical_slot_released and stopped.task_state == "FAILED"
                assert (
                    await m.stop_worker_stream_dag_subtask(
                        transaction, db, own_worker, own.assignment.id, request
                    )
                    == stopped
                )
                stop_receipts.append(stopped)
    async with db.session_context(begin_transaction=True) as transaction:
        terminal = await lifecycle.read_stream_dag_execution(
            transaction, db, plan.id, credentials
        )
        assert terminal.phase == "TERMINAL" and terminal.terminal_state == "FAILED"
        assert (
            terminal.counts.stopped == 4
            and terminal.counts.physical_slots_remaining == 0
        )
        assert terminal.counts.terminal_tasks == 4 and terminal.cleanup_complete
        flow = await transaction.get(db.FlowRun, plan.flow_run_id)
        assert flow is not None and flow.state_type.value == "FAILED"
        assert flow.state.state_details.retriable is False
        assert (
            await transaction.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRunState)
                .where(db.TaskRunState.type == "FAILED")
            )
            == 4
        )
