"""Native lifecycle acceptance through real CLI workers, HTTP and PostgreSQL.

The tests use client-controlled source pauses, actual worker SIGKILL and the
real renewal horizon. They do not advance a database clock or enrol a synthetic
launcher in place of a Prefect worker.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import anyio
import pytest
import sqlalchemy as sa

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.native_deployments import NativeStreamProcessDeathWitness
from prefect.deployments.runner import RunnerDeployment
from prefect.flows import load_flow_from_entrypoint
from prefect.runner._native_process import NativePidfdScope, verify_native_process_death
from prefect.runner._starter_native import linux_process_identity
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_URL,
    temporary_settings,
)
from prefect.workers.process import ProcessWorker
from tests.test_native_execution_http import execution_api as execution_api
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI


@dataclass
class _DeploymentCase:
    server: RestartableAPI
    directory: Path
    coordination: Path
    pool: str
    deployment_id: UUID
    flow_run_id: UUID
    db: Any
    processes: list[subprocess.Popen[bytes]] = field(default_factory=list)
    logs: list[Path] = field(default_factory=list)
    child_scope: NativePidfdScope | None = None

    def environment(self) -> dict[str, str]:
        environment = dict(os.environ)
        for name in (
            "PREFECT_SERVER_DATABASE_CONNECTION_URL",
            "PREFECT_API_DATABASE_CONNECTION_URL",
        ):
            environment.pop(name, None)
        environment.update(
            PREFECT_API_URL=self.server.url,
            PREFECT_API_AUTH_STRING=self.server.auth,
            PREFECT_LOGGING_LEVEL="INFO",
            PREFECT_LOGGING_INTERNAL_LEVEL="WARNING",
            PREFECT_WORKER_QUERY_SECONDS="0.5",
            PREFECT_WORKER_HEARTBEAT_SECONDS="5",
        )
        return environment

    def start_worker(
        self, *, run_once: bool, pause_after_enrollment: bool = False
    ) -> subprocess.Popen[bytes]:
        command = [sys.executable, "-m", "prefect"]
        if pause_after_enrollment:
            # The real worker commits enrollment, then loses its process before
            # it can spawn a flow child. All API and slot ownership stay real.
            code = """
import os
import sys
from pathlib import Path
import anyio
from prefect.runner._starter_native import NativeProcessStarter

marker = Path(sys.argv.pop(1))
original = NativeProcessStarter.enroll
async def enroll_then_pause(self):
    await original(self)
    marker.write_text(str(os.getpid()))
    await anyio.Event().wait()
NativeProcessStarter.enroll = enroll_then_pause
from prefect.cli import app
app()
"""
            command = [
                sys.executable,
                "-c",
                code,
                str(self.coordination / "enrolled"),
            ]
        command.extend(
            [
                "worker",
                "start",
                "--pool",
                self.pool,
                "--type",
                "process",
                "--name",
                "native-acceptance-" + uuid4().hex,
            ]
        )
        if run_once:
            command.append("--run-once")
        log = self.directory / f"worker-{len(self.logs) + 1}.log"
        with log.open("wb") as output:
            process = subprocess.Popen(
                command,
                env=self.environment(),
                stdout=output,
                stderr=subprocess.STDOUT,
            )
        self.logs.append(log)
        self.processes.append(process)
        return process

    def diagnostic(self) -> str:
        return "\n".join(path.read_text()[-12000:] for path in self.logs)

    async def wait_paused(self) -> int:
        marker = self.coordination / "source-paused"
        deadline = time.monotonic() + 150
        while True:
            try:
                pid = int(marker.read_text())
            except (FileNotFoundError, ValueError):
                # Existence can become visible before write_text has flushed
                # the marker. Wait for its complete bounded PID value.
                pass
            else:
                break
            assert all(process.poll() is None for process in self.processes), (
                self.diagnostic()
            )
            assert time.monotonic() < deadline, self.diagnostic()
            await anyio.sleep(0.1)
        # The pause records its actual engine PID; pin it before killing its
        # parent, so cleanup never signals a later process that reuses the PID.
        self.child_scope = NativePidfdScope(pid)
        return pid

    async def wait_final(self, *, timeout: float = 180) -> Any:
        deadline = time.monotonic() + timeout
        with self.server.client() as client:
            while True:
                observed = client.read_flow_run(self.flow_run_id)
                if observed.state is not None and observed.state.is_final():
                    return observed
                assert time.monotonic() < deadline, self.diagnostic()
                await anyio.sleep(0.2)

    async def close(self) -> None:
        (self.coordination / "release").touch()
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        deadline = time.monotonic() + 15
        for process in self.processes:
            while process.poll() is None and time.monotonic() < deadline:
                await anyio.sleep(0.1)
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
        if self.child_scope is not None:
            # pidfd retains the original identity even if /proc now describes a
            # reused PID. Only this test's pinned child can receive the signal.
            self.child_scope.hard_kill(self.child_scope.pid)
            self.child_scope.close()
        # A failed assertion may interrupt a newly registered replacement.
        # Inspect only this FlowRun's recorded identities and use exact kernel
        # verification to leave no orphan after test cleanup.
        async with self.db.session_context() as session:
            rows = (
                (
                    await session.execute(
                        sa.text(
                            "SELECT host_id, boot_id, child_process_id, process_start_ticks "
                            "FROM native_stream_launch_identity WHERE flow_run_id=:flow "
                            "AND child_process_id IS NOT NULL"
                        ),
                        {"flow": self.flow_run_id},
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            await anyio.to_thread.run_sync(
                verify_native_process_death, SimpleNamespace(**row)
            )


async def _deploy(
    server: RestartableAPI,
    directory: Path,
    db: Any,
    *,
    fault: str,
    entrypoint: str = "tests/fixtures/native_stream_mixed_flow.py:deployed_mixed_flow",
) -> _DeploymentCase:
    dataset, coordination = directory / "dataset", directory / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for number in range(1, 501):
        (dataset / f"{number:04}.json").write_text(json.dumps({"number": number}))
    definition = load_flow_from_entrypoint(entrypoint)
    pool = "native-acceptance-" + uuid4().hex
    with temporary_settings(
        {PREFECT_API_URL: server.url, PREFECT_API_AUTH_STRING: server.auth}
    ):
        async with get_client() as client:
            await client.create_work_pool(
                WorkPoolCreate(
                    name=pool,
                    type="process",
                    base_job_template=ProcessWorker.get_default_base_job_template(),
                )
            )
            deployment = RunnerDeployment.from_flow(
                definition,
                name="mixed-lifecycle",
                work_pool_name=pool,
                concurrency_limit=1,
                job_variables={"working_dir": str(Path.cwd()), "stream_output": True},
            )
            deployment_id = await deployment.aapply()
            run = await client.create_flow_run_from_deployment(
                deployment_id,
                parameters={
                    "dataset": str(dataset),
                    "coordination": str(coordination),
                    "count": 500,
                    "fault": fault,
                },
            )
    return _DeploymentCase(
        server, directory, coordination, pool, deployment_id, run.id, db
    )


async def _slot_count(db: Any, case: _DeploymentCase) -> int:
    async with db.session_context() as session:
        value = await session.scalar(
            sa.text(
                "SELECT c.active_slots FROM concurrency_limit_v2 c "
                "JOIN deployment d ON d.concurrency_limit_id=c.id WHERE d.id=:id"
            ),
            {"id": case.deployment_id},
        )
    assert type(value) is int
    return value


async def _wait_completed_prefix(db: Any, case: _DeploymentCase) -> None:
    deadline = time.monotonic() + 90
    while True:
        async with db.session_context() as session:
            completed = await session.scalar(
                sa.text(
                    "SELECT completed_through FROM stream_consumer "
                    "WHERE flow_run_id=:flow"
                ),
                {"flow": case.flow_run_id},
            )
        if completed == 200:
            return
        assert time.monotonic() < deadline, case.diagnostic()
        await anyio.sleep(0.1)


@pytest.mark.timeout(420)
async def test_fresh_worker_recovers_live_orphan_after_real_renewal_horizon(
    execution_api: RestartableAPI, tmp_path: Path, db: Any
):
    case = await _deploy(execution_api, tmp_path, db, fault="orphan")
    try:
        first = case.start_worker(run_once=True)
        original_pid = await case.wait_paused()
        original_identity = linux_process_identity(original_pid)
        await _wait_completed_prefix(db, case)
        assert case.child_scope is not None and case.child_scope.is_alive(original_pid)
        assert len(list((case.coordination / "signed").glob("*.json"))) == 200
        assert not (case.coordination / "tail-bodies").exists()
        with execution_api.client() as client:
            before = client.read_flow_run(case.flow_run_id)
            plan = client.read_native_stream_deployment_plan(
                flow_run_id=case.flow_run_id
            )
        assert before.state.is_running()
        assert before.state.state_details.deployment_concurrency_lease_id is not None
        lease = before.state.state_details.deployment_concurrency_lease_id
        assert plan.current_launch is not None
        assert plan.current_launch.generation == 1
        assert plan.current_launch.last_renewed_at is not None
        verification_not_before = plan.current_launch.last_renewed_at + timedelta(
            seconds=120
        )
        async with db.session_context() as session:
            task_ids = set(
                await session.scalars(
                    sa.text("SELECT id FROM task_run WHERE flow_run_id=:flow"),
                    {"flow": case.flow_run_id},
                )
            )
            stream_ids = set(
                await session.scalars(
                    sa.text("SELECT id FROM stream_partition WHERE flow_run_id=:flow"),
                    {"flow": case.flow_run_id},
                )
            )
        assert len(task_ids) == 3 and len(stream_ids) == 2
        assert await _slot_count(db, case) == 1
        first.kill()
        assert first.wait(timeout=10) == -signal.SIGKILL
        assert case.child_scope.is_alive(original_pid)
        second = case.start_worker(run_once=False)
        competing_worker = case.start_worker(run_once=False)
        deadline = time.monotonic() + 240
        generation = 1
        with execution_api.client() as client:
            while generation == 1:
                assert second.poll() is None, case.diagnostic()
                assert competing_worker.poll() is None, case.diagnostic()
                current = client.read_flow_run(case.flow_run_id)
                assert not current.state.is_final(), case.diagnostic()
                assert await _slot_count(db, case) == 1
                current_plan = client.read_native_stream_deployment_plan(
                    flow_run_id=case.flow_run_id
                )
                assert current_plan.current_launch is not None
                generation = current_plan.current_launch.generation
                if generation == 1 and datetime.now(UTC) < verification_not_before:
                    assert case.child_scope.is_alive(original_pid)
                assert time.monotonic() < deadline, case.diagnostic()
                await anyio.sleep(0.5)
        # The real server renewal horizon elapsed. No test has rewritten its
        # timestamps, invoked the replacement endpoint, or delivered credentials.
        assert datetime.now(UTC) >= verification_not_before
        assert generation == 2
        assert not case.child_scope.is_alive(original_pid)
        death_observed_at = datetime.now(UTC)
        final = await case.wait_final()
        assert final.state.is_completed(), case.diagnostic()
        assert final.empirical_policy.retries == 2
        assert final.run_count == 2
        assert final.state.state_details.deployment_concurrency_lease_id == lease
        assert await _slot_count(db, case) == 0
        async with db.session_context() as session:
            final_tasks = set(
                await session.scalars(
                    sa.text("SELECT id FROM task_run WHERE flow_run_id=:flow"),
                    {"flow": case.flow_run_id},
                )
            )
            final_streams = set(
                await session.scalars(
                    sa.text("SELECT id FROM stream_partition WHERE flow_run_id=:flow"),
                    {"flow": case.flow_run_id},
                )
            )
            replacement = (
                (
                    await session.execute(
                        sa.text(
                            "SELECT expected_generation, new_generation, worker_id, "
                            "host_id, identity_kind, death_witness "
                            "FROM native_stream_replacement_operation "
                            "WHERE flow_run_id=:flow AND state='CONFIRMED'"
                        ),
                        {"flow": case.flow_run_id},
                    )
                )
                .mappings()
                .one()
            )
            original_launch = (
                (
                    await session.execute(
                        sa.text(
                            "SELECT host_id, boot_id, child_process_id, process_start_ticks, "
                            "last_renewed_at FROM native_stream_launch_identity "
                            "WHERE flow_run_id=:flow AND generation=1"
                        ),
                        {"flow": case.flow_run_id},
                    )
                )
                .mappings()
                .one()
            )
            successor_launch = (
                (
                    await session.execute(
                        sa.text(
                            "SELECT worker_id, launcher_process_id "
                            "FROM native_stream_launch_identity "
                            "WHERE flow_run_id=:flow AND generation=2"
                        ),
                        {"flow": case.flow_run_id},
                    )
                )
                .mappings()
                .one()
            )
        assert task_ids < final_tasks and len(final_tasks) == 4
        assert final_streams == stream_ids
        # The worker that wins the generation CAS need not be the worker that
        # signalled the old child: another verifier can observe its identity
        # gone after the first worker's pidfd kill. Both proofs must refer to
        # the exact original process, after the real durable renewal horizon.
        witness = NativeStreamProcessDeathWitness.model_validate(
            replacement["death_witness"]
        )
        assert witness.kind in {"PIDFD_EXIT", "IDENTITY_GONE"}
        assert original_identity == (
            original_launch["host_id"],
            original_launch["boot_id"],
            original_launch["process_start_ticks"],
        )
        assert witness.host_id == original_launch["host_id"]
        assert witness.boot_id == original_launch["boot_id"]
        assert (
            witness.child_process_id
            == original_launch["child_process_id"]
            == original_pid
        )
        assert witness.process_start_ticks == original_launch["process_start_ticks"]
        assert (
            original_launch["last_renewed_at"] + timedelta(seconds=120)
            <= witness.observed_at
            <= death_observed_at
        )
        assert replacement["identity_kind"] == "CHILD"
        assert replacement["host_id"] == witness.host_id
        assert replacement["expected_generation"] == 1
        assert replacement["new_generation"] == 2
        assert replacement["worker_id"] == successor_launch["worker_id"]
        assert successor_launch["launcher_process_id"] in (
            second.pid,
            competing_worker.pid,
        )
        assert (case.coordination / "prefix-bodies").read_text().splitlines() == [
            "prepare"
        ]
        assert (case.coordination / "tail-bodies").read_text().splitlines() == [
            "summarize"
        ]
        assert (case.coordination / "operator-inputs").read_text().splitlines() == [
            str(number) for number in range(1, 501)
        ]
        assert len(list((case.coordination / "signed").glob("*.json"))) == 500
    finally:
        await case.close()


@pytest.mark.timeout(240)
async def test_drain_survives_server_restart_and_finishes_only_accepted_prefix(
    execution_api: RestartableAPI, tmp_path: Path, db: Any
):
    case = await _deploy(execution_api, tmp_path, db, fault="drain")
    try:
        worker = case.start_worker(run_once=True)
        await case.wait_paused()
        await _wait_completed_prefix(db, case)
        assert await _slot_count(db, case) == 1
        command = {
            "flow_run_id": case.flow_run_id,
            "operation_id": uuid4(),
            "expected_generation": 1,
            "reason": "Drain the real accepted document prefix",
        }
        with execution_api.client() as client:
            result = client.drain_native_stream(**command)
        assert result.source_admission_closed and not result.cancel_requested
        execution_api.restart()
        with execution_api.client() as client:
            replay = client.drain_native_stream(**command)
        assert replay == result
        assert await _slot_count(db, case) == 1
        (case.coordination / "release").touch()
        final = await case.wait_final()
        assert final.state.is_completed(), case.diagnostic()
        assert worker.wait(timeout=30) == 0, case.diagnostic()
        with execution_api.client() as client:
            assert client.drain_native_stream(**command) == result
        async with db.session_context() as session:
            partitions = (
                await session.execute(
                    sa.text(
                        "SELECT state, last_sequence, tail_sequence FROM stream_partition "
                        "WHERE flow_run_id=:flow"
                    ),
                    {"flow": case.flow_run_id},
                )
            ).all()
            consumers = await session.scalar(
                sa.text("SELECT count(*) FROM stream_consumer WHERE flow_run_id=:flow"),
                {"flow": case.flow_run_id},
            )
        assert len(partitions) == 2
        assert all(tuple(row) == ("COMPLETED", 200, 200) for row in partitions)
        # The 200-record durable cursor was observed before terminal release.
        # Public channel metadata survives; hot consumers and payloads do not.
        assert consumers == 0
        assert len(list((case.coordination / "signed").glob("*.json"))) == 200
        assert (case.coordination / "tail-bodies").read_text().splitlines() == [
            "summarize"
        ]
        assert await _slot_count(db, case) == 0
    finally:
        await case.close()


@pytest.mark.timeout(240)
@pytest.mark.parametrize("interface", ["sdk", "cli"])
async def test_cancel_kills_current_child_and_replays_after_terminal_state(
    execution_api: RestartableAPI, tmp_path: Path, db: Any, interface: str
):
    case = await _deploy(execution_api, tmp_path, db, fault="cancel")
    try:
        worker = case.start_worker(run_once=True)
        child_pid = await case.wait_paused()
        await _wait_completed_prefix(db, case)
        assert await _slot_count(db, case) == 1
        operation = uuid4()
        reason = "Cancel the current native worker generation"
        if interface == "cli":
            cli = await anyio.run_process(
                [
                    sys.executable,
                    "-m",
                    "prefect",
                    "flow-run",
                    "cancel",
                    str(case.flow_run_id),
                ],
                env=case.environment(),
                check=False,
            )
            output = cli.stdout + cli.stderr
            (tmp_path / "cli-cancel.log").write_bytes(output)
            assert cli.returncode == 0, output.decode(errors="replace")
            assert b"successfully scheduled for cancellation" in b" ".join(
                cli.stdout.split()
            )
            operation = uuid5(
                NAMESPACE_URL, f"prefect-native-cli-cancel/{case.flow_run_id}/1"
            )
            reason = "Cancellation requested through the flow-run CLI"
        command = {
            "flow_run_id": case.flow_run_id,
            "operation_id": operation,
            "expected_generation": 1,
            "reason": reason,
        }
        with execution_api.client() as client:
            result = client.cancel_native_stream(**command)
        assert result.source_admission_closed and result.cancel_requested
        final = await case.wait_final(timeout=60)
        assert final.state.is_cancelled(), case.diagnostic()
        assert worker.wait(timeout=30) == 0, case.diagnostic()
        assert case.child_scope is not None and not case.child_scope.is_alive(child_pid)
        with execution_api.client() as client:
            assert client.cancel_native_stream(**command) == result
        async with db.session_context() as session:
            authority = (
                await session.execute(
                    sa.text(
                        "SELECT state, source_admission_closed, cancel_requested "
                        "FROM stream_execution_authority WHERE flow_run_id=:flow"
                    ),
                    {"flow": case.flow_run_id},
                )
            ).one()
            finalizations = await session.scalar(
                sa.text(
                    "SELECT count(*) FROM native_stream_control_operation "
                    "WHERE flow_run_id=:flow AND kind='CANCEL_FINALIZED'"
                ),
                {"flow": case.flow_run_id},
            )
        assert tuple(authority) == ("RELEASED", True, True)
        assert finalizations == 1
        assert not (case.coordination / "tail-bodies").exists()
        assert len(list((case.coordination / "signed").glob("*.json"))) == 200
        assert await _slot_count(db, case) == 0
    finally:
        await case.close()


@pytest.mark.timeout(420)
async def test_fresh_worker_finalizes_orphan_cancellation_without_a_new_child(
    execution_api: RestartableAPI, tmp_path: Path, db: Any
):
    case = await _deploy(execution_api, tmp_path, db, fault="cancel")
    try:
        first = case.start_worker(run_once=True)
        original_pid = await case.wait_paused()
        await _wait_completed_prefix(db, case)
        with execution_api.client() as client:
            plan = client.read_native_stream_deployment_plan(
                flow_run_id=case.flow_run_id
            )
        assert plan.current_launch is not None
        assert plan.current_launch.last_renewed_at is not None
        verification_not_before = plan.current_launch.last_renewed_at + timedelta(
            seconds=120
        )
        assert await _slot_count(db, case) == 1
        first.kill()
        assert first.wait(timeout=10) == -signal.SIGKILL
        assert case.child_scope is not None and case.child_scope.is_alive(original_pid)
        command = {
            "flow_run_id": case.flow_run_id,
            "operation_id": uuid4(),
            "expected_generation": 1,
            "reason": "Cancel the abandoned native generation before replacement",
        }
        with execution_api.client() as client:
            cancellation = client.cancel_native_stream(**command)
            assert client.read_flow_run(case.flow_run_id).state.is_cancelling()
        second = case.start_worker(run_once=False)
        deadline = time.monotonic() + 240
        with execution_api.client() as client:
            while True:
                assert second.poll() is None, case.diagnostic()
                observed = client.read_flow_run(case.flow_run_id)
                if observed.state.is_final():
                    break
                assert observed.state.is_cancelling(), case.diagnostic()
                assert await _slot_count(db, case) == 1
                if datetime.now(UTC) < verification_not_before:
                    assert case.child_scope.is_alive(original_pid)
                assert time.monotonic() < deadline, case.diagnostic()
                await anyio.sleep(0.5)
            assert client.cancel_native_stream(**command) == cancellation
            final_plan = client.read_native_stream_deployment_plan(
                flow_run_id=case.flow_run_id
            )
        assert datetime.now(UTC) >= verification_not_before
        assert observed.state.is_cancelled(), case.diagnostic()
        assert observed.run_count == 1
        assert not case.child_scope.is_alive(original_pid)
        assert final_plan.current_launch is not None
        assert final_plan.current_launch.generation == 1
        assert final_plan.current_launch.state == "RELEASED"
        assert final_plan.state == "RELEASED"
        assert await _slot_count(db, case) == 0
        async with db.session_context() as session:
            launches = await session.scalar(
                sa.text(
                    "SELECT count(*) FROM native_stream_launch_identity "
                    "WHERE flow_run_id=:flow"
                ),
                {"flow": case.flow_run_id},
            )
            finalizations = await session.scalar(
                sa.text(
                    "SELECT count(*) FROM native_stream_control_operation "
                    "WHERE flow_run_id=:flow AND kind='CANCEL_FINALIZED'"
                ),
                {"flow": case.flow_run_id},
            )
            task_states = list(
                await session.scalars(
                    sa.text("SELECT state_type FROM task_run WHERE flow_run_id=:flow"),
                    {"flow": case.flow_run_id},
                )
            )
        assert launches == finalizations == 1
        assert sorted(task_states) == ["CANCELLED", "CANCELLED", "COMPLETED"]
        assert not (case.coordination / "tail-bodies").exists()
        assert len(list((case.coordination / "signed").glob("*.json"))) == 200
    finally:
        await case.close()


@pytest.mark.timeout(420)
async def test_fresh_worker_retires_enrolled_launcher_without_spawning_a_child(
    execution_api: RestartableAPI, tmp_path: Path, db: Any
):
    case = await _deploy(execution_api, tmp_path, db, fault="none")
    try:
        first = case.start_worker(run_once=False, pause_after_enrollment=True)
        marker = case.coordination / "enrolled"
        deadline = time.monotonic() + 90
        while True:
            try:
                enrolled_pid = int(marker.read_text())
            except (FileNotFoundError, ValueError):
                pass
            else:
                break
            assert first.poll() is None, case.diagnostic()
            assert time.monotonic() < deadline, case.diagnostic()
            await anyio.sleep(0.1)
        assert enrolled_pid == first.pid
        with execution_api.client() as client:
            enrolled = client.read_flow_run(case.flow_run_id)
            plan = client.read_native_stream_deployment_plan(
                flow_run_id=case.flow_run_id
            )
        assert enrolled.state.is_pending()
        assert enrolled.run_count == 0
        assert plan.current_launch is not None
        assert plan.current_launch.state == "ENROLLED"
        assert plan.current_launch.process_registered_at is None
        anchor = plan.current_launch.last_renewed_at or plan.current_launch.created_at
        verification_not_before = anchor + timedelta(seconds=120)
        assert await _slot_count(db, case) == 1
        second = case.start_worker(run_once=False)
        deadline = time.monotonic() + 240
        # Renewal expiry must not kill a live whole-worker process. Observe two
        # complete verifier refusals from real polling after the real horizon.
        while True:
            assert first.poll() is None, case.diagnostic()
            assert second.poll() is None, case.diagnostic()
            with execution_api.client() as client:
                observed = client.read_flow_run(case.flow_run_id)
            assert observed.state.is_pending(), case.diagnostic()
            assert await _slot_count(db, case) == 1
            verifier_refusals = re.findall(
                r"(?m)^[^\n]*StreamProtocolError: [^\n]*still alive[^\n]*$",
                case.logs[-1].read_text(),
            )
            if len(verifier_refusals) >= 2:
                break
            assert time.monotonic() < deadline, case.diagnostic()
            await anyio.sleep(0.2)
        assert datetime.now(UTC) >= verification_not_before
        async with db.session_context() as session:
            heartbeat = await session.scalar(
                sa.text("SELECT last_heartbeat_time FROM worker WHERE id=:id"),
                {"id": plan.current_launch.worker_id},
            )
        assert heartbeat >= datetime.now(UTC) - timedelta(seconds=20)
        first.kill()
        assert first.wait(timeout=10) == -signal.SIGKILL
        final = await case.wait_final(timeout=60)
        assert final.state.is_failed(), case.diagnostic()
        assert final.run_count == 0
        assert await _slot_count(db, case) == 0
        with execution_api.client() as client:
            final_plan = client.read_native_stream_deployment_plan(
                flow_run_id=case.flow_run_id
            )
        assert final_plan.state == "RELEASED"
        assert final_plan.current_launch is not None
        assert final_plan.current_launch.generation == 1
        assert final_plan.current_launch.state == "RELEASED"
        assert final_plan.current_launch.process_registered_at is None
        async with db.session_context() as session:
            counts = (
                await session.execute(
                    sa.text(
                        "SELECT "
                        "(SELECT count(*) FROM task_run WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM stream_partition WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM stream_finite_submission WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM native_stream_launch_identity WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM native_stream_control_operation "
                        " WHERE flow_run_id=:flow AND kind='SPAWN_FAILED'), "
                        "(SELECT count(*) FROM native_stream_replacement_operation "
                        " WHERE flow_run_id=:flow)"
                    ),
                    {"flow": case.flow_run_id},
                )
            ).one()
        assert tuple(counts) == (0, 0, 0, 1, 1, 1)
        assert not (case.coordination / "prefix-bodies").exists()
        assert not (case.coordination / "source-bodies").exists()
        assert not (case.coordination / "tail-bodies").exists()
    finally:
        await case.close()


@pytest.mark.timeout(120)
async def test_worker_retires_child_that_exits_running_before_first_task_reservation(
    execution_api: RestartableAPI, tmp_path: Path, db: Any
):
    case = await _deploy(
        execution_api,
        tmp_path,
        db,
        fault="none",
        entrypoint=(
            "tests/fixtures/native_stream_mixed_flow.py:deployed_pre_manifest_failure_flow"
        ),
    )
    try:
        worker = case.start_worker(run_once=True)
        final = await case.wait_final(timeout=80)
        assert final.state.is_failed(), case.diagnostic()
        assert final.run_count == 1
        assert final.empirical_policy.retries == 2
        assert worker.wait(timeout=20) == 0, case.diagnostic()
        marker = case.coordination / "pre-manifest-exit"
        child_pid = int(marker.read_text())
        assert await _slot_count(db, case) == 0
        with execution_api.client() as client:
            plan = client.read_native_stream_deployment_plan(
                flow_run_id=case.flow_run_id
            )
        assert plan.state == "RELEASED"
        assert plan.current_launch is not None
        assert plan.current_launch.generation == 1
        assert plan.current_launch.state == "RELEASED"
        assert plan.current_launch.process_registered_at is not None
        async with db.session_context() as session:
            counts = (
                await session.execute(
                    sa.text(
                        "SELECT "
                        "(SELECT count(*) FROM task_run WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM stream_partition WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM stream_finite_submission WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM native_stream_launch_identity WHERE flow_run_id=:flow), "
                        "(SELECT count(*) FROM native_stream_control_operation "
                        " WHERE flow_run_id=:flow AND kind='VERIFIED_RETIRED'), "
                        "(SELECT count(*) FROM flow_run_state "
                        " WHERE flow_run_id=:flow AND type='RUNNING')"
                    ),
                    {"flow": case.flow_run_id},
                )
            ).one()
            witness = (
                await session.execute(
                    sa.text(
                        "SELECT death_witness->>'kind', "
                        "death_witness->>'child_process_id', "
                        "death_witness->>'exit_code' "
                        "FROM native_stream_replacement_operation "
                        "WHERE flow_run_id=:flow"
                    ),
                    {"flow": case.flow_run_id},
                )
            ).one()
        assert tuple(counts) == (0, 0, 0, 1, 1, 1)
        assert tuple(witness) == ("WAITPID", str(child_pid), "77")
        assert not (case.coordination / "prefix-bodies").exists()
        assert not (case.coordination / "source-bodies").exists()
        assert not (case.coordination / "tail-bodies").exists()
    finally:
        await case.close()
