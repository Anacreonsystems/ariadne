"""Actual claimed-child cleanup through the SDK, HTTP server and PostgreSQL."""

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.client.orchestration import PrefectClient
from prefect.logging import get_logger
from prefect.runner._starter_native import linux_process_identity
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.schemas.stream_subtasks import StreamWorkerPrincipal
from prefect.streams import StreamProtocolError
from prefect.workers import _stream_subtasks as worker_subtasks
from prefect.workers._stream_subtasks import (
    NativeStreamSubtaskSupervisor,
    _SubtaskProcess,
)
from tests.server.api.test_stream_workers import deployed_graph, new_worker
from tests.test_native_execution_http import execution_api as execution_api
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI

pytestmark = [
    pytest.mark.clear_db,
    pytest.mark.service("database"),
    pytest.mark.skipif(
        sys.platform != "linux", reason="Physical startup requires Linux pidfd"
    ),
]


@pytest.mark.timeout(90)
async def test_claimed_startup_keeps_slot_until_real_child_is_reaped(
    execution_api: RestartableAPI,
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    server = execution_api
    host, boot, ticks = linux_process_identity(os.getpid())
    async with httpx.AsyncClient(
        base_url=server.url, auth=tuple(server.auth.split(":")), timeout=20
    ) as api:
        worker_id, seed = await new_worker(api)
        pool = await session.get(db.WorkPool, seed.work_pool_id)
        assert pool is not None
        other = await api.post(
            f"/work_pools/{pool.name}/workers/heartbeat",
            json={"name": "placement-peer-" + uuid4().hex, "return_id": True},
        )
        assert other.status_code == 200, other.text
        identity = StreamWorkerPrincipal(
            work_pool_id=pool.id,
            host_id=host,
            boot_id=boot,
            launcher_process_id=os.getpid(),
            launcher_start_ticks=ticks,
        )
        # Only the managed Task child is real in this focused control. The
        # second placement and Flow coordinator use the metadata fixture;
        # this test does not claim full two-worker execution acceptance.
        peer = identity.model_copy(
            update={
                "launcher_process_id": os.getpid() + 100000,
                "launcher_start_ticks": ticks + 1,
            }
        )
        plan, _, _ = await deployed_graph(
            db,
            session,
            api,
            deployment,
            workers=[(worker_id, identity), (UUID(other.text), peer)],
        )

    target_id = next(
        row.id
        for row in plan.subtasks
        if row.operator_uid == "A" and row.subtask_index == 0
    )
    real_wait = subprocess.Popen.wait
    process_seen: subprocess.Popen[bytes] | None = None
    target_pid: int | None = None
    waits = 0
    child: _SubtaskProcess | None = None

    def fail_identity_after_popen(pid: int) -> tuple[UUID, UUID, int]:
        nonlocal target_pid
        target_pid = pid
        raise StreamProtocolError("Injected identity setup failure after Popen")

    def unknown_first_wait(
        process: subprocess.Popen[bytes], timeout: float | None = None
    ) -> int:
        nonlocal process_seen, waits
        if process.pid == target_pid:
            process_seen = process
            waits += 1
            if waits == 1:
                raise subprocess.TimeoutExpired(process.args, timeout)
        return real_wait(process, timeout=timeout)

    try:
        async with PrefectClient(server.url, auth_string=server.auth) as client:
            supervisor = NativeStreamSubtaskSupervisor(
                SimpleNamespace(
                    backend_id=worker_id,
                    work_pool=SimpleNamespace(id=identity.work_pool_id),
                    client=client,
                    _logger=get_logger("tests.stream-startup-cleanup"),
                )
            )
            items = await client.poll_worker_stream_subtasks(
                worker_id=worker_id, principal=supervisor.principal
            )
            item = next(row for row in items if row.assignment.id == target_id)
            assert item.phase == "WAITING" and item.current_grant is None
            with monkeypatch.context() as injection:
                injection.setattr(
                    worker_subtasks, "linux_process_identity", fail_identity_after_popen
                )
                injection.setattr(subprocess.Popen, "wait", unknown_first_wait)
                await supervisor._drive_safely(item)
                run = supervisor.runs[target_id]
                child = run.child
                assert child is not None and child.scope is not None
                assert child.process.pid == target_pid
                assert process_seen is child.process
                assert waits == 1 and child.process.returncode is None
                assert child.home.exists()
                os.fstat(child.bootstrap_fd)
                os.fstat(child.go_fd)
                assert run.startup_failed and run.stop is None
                async with db.session_context() as check:
                    graph = await check.get(db.StreamGraphRevision, plan.id)
                    physical = await check.get(db.StreamPhysicalSubtask, target_id)
                    assert graph is not None and graph.state == "ABORTING"
                    assert graph.startup_completed_at is None
                    assert physical is not None
                    assert not physical.physical_slot_released
                    assert physical.startup_stop_operation_id is None
                    grant = await check.get(
                        db.StreamSubtaskGrant, physical.current_grant_id
                    )
                    assert grant is not None and grant.state == "CLAIMED"
                    assert (
                        grant.child_process_id is None
                        and grant.process_start_ticks is None
                    )
                    assert (
                        await check.scalar(
                            sa.select(sa.func.count())
                            .select_from(db.StreamPhysicalSubtask)
                            .where(
                                db.StreamPhysicalSubtask.graph_revision_id == plan.id,
                                db.StreamPhysicalSubtask.physical_slot_released.is_(
                                    False
                                ),
                            )
                        )
                        == 4
                    )
                    assert (
                        await check.scalar(
                            sa.select(sa.func.count())
                            .select_from(db.StreamRecord)
                            .join(
                                db.StreamPartition,
                                db.StreamPartition.id == db.StreamRecord.stream_id,
                            )
                            .where(db.StreamPartition.flow_run_id == plan.flow_run_id)
                        )
                        == 0
                    )
                items = await client.poll_worker_stream_subtasks(
                    worker_id=worker_id,
                    principal=supervisor.principal,
                    active_subtask_ids=[target_id],
                )
                aborting = next(row for row in items if row.assignment.id == target_id)
                assert aborting.phase == "ABORTING"
                assert run.stop is None
                await supervisor._drive_safely(aborting)
                assert waits >= 2 and child.process.returncode is not None
                assert target_id not in supervisor.runs
                assert not Path(f"/proc/{child.process.pid}").exists()
                assert not child.home.exists()
                assert child.bootstrap_fd == child.go_fd == -1
                assert run.stop is not None and run.stop["unstarted"] is True
                assert run.stop["death_witness"] is None
            replay = await client.acknowledge_worker_stream_subtask_startup_stop(
                worker_id=worker_id,
                subtask_id=target_id,
                principal=supervisor.principal,
                stop=run.stop,
            )
            assert replay.graph_state == "RELEASED"
            async with db.session_context() as check:
                graph = await check.get(db.StreamGraphRevision, plan.id)
                physical = await check.get(db.StreamPhysicalSubtask, target_id)
                assert graph is not None and graph.state == "RELEASED"
                assert physical is not None and physical.physical_slot_released
                assert physical.startup_stop_operation_id == replay.operation_id
                assert (
                    await check.scalar(
                        sa.select(sa.func.count())
                        .select_from(db.StreamPhysicalSubtask)
                        .where(
                            db.StreamPhysicalSubtask.graph_revision_id == plan.id,
                            db.StreamPhysicalSubtask.startup_stop_operation_id.is_not(
                                None
                            ),
                        )
                    )
                    == 1
                )
                tasks = (
                    await check.scalars(
                        sa.select(db.TaskRun).where(
                            db.TaskRun.flow_run_id == plan.flow_run_id
                        )
                    )
                ).all()
                assert len(tasks) == 4
                assert all(
                    row.run_count == 0 and row.state_type.value == "FAILED"
                    for row in tasks
                )
                assert (
                    await check.scalar(
                        sa.select(sa.func.count())
                        .select_from(db.StreamPhysicalSubtask)
                        .where(
                            db.StreamPhysicalSubtask.graph_revision_id == plan.id,
                            db.StreamPhysicalSubtask.physical_slot_released.is_(False),
                        )
                    )
                    == 0
                )
    finally:
        if child is not None:
            child.close()
        elif process_seen is not None:
            if process_seen.poll() is None:
                process_seen.kill()
            real_wait(process_seen, timeout=10)
