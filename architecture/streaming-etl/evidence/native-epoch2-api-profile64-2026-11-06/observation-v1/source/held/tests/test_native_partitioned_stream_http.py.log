"""Discovery and acceptance on migrated PostgreSQL and a real ProcessWorker."""

import json
import os
import subprocess
import sys
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate, WorkPoolCreate
from prefect.deployments.runner import RunnerDeployment
from prefect.exceptions import ObjectAlreadyExists
from prefect.flows import load_flow_from_entrypoint
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_URL,
    temporary_settings,
)
from prefect.workers.process import ProcessWorker
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI


@pytest.fixture
def execution_api(
    native_backend: None, tmp_path: Path
) -> Generator[RestartableAPI, None, None]:
    server = RestartableAPI(tmp_path)
    server.environment.update(
        PREFECT_DEBUG_MODE="false",
        PREFECT_LOGGING_LEVEL="INFO",
        PREFECT_SERVER_LOGGING_LEVEL="INFO",
        PREFECT_LOGGING_INTERNAL_LEVEL="WARNING",
        PREFECT_SERVER_SERVICES_TASK_RUN_RECORDER_ENABLED="true",
        PREFECT_API_SERVICES_TASK_RUN_RECORDER_ENABLED="true",
    )
    try:
        server.start()
        yield server
    finally:
        server.stop()
        server.socket.close()


def _phase_observation(
    directory: Path, ready_ns: int | None, final_ns: int | None
) -> dict[str, Any]:
    traces = {
        path.stem: [json.loads(line) for line in path.read_text().splitlines()]
        for path in directory.glob("calls-*.jsonl")
    }
    all_calls = [item for values in traces.values() for item in values]
    first_data = [
        int(path.read_text()) for path in directory.glob("source-first-accepted-*.time")
    ]
    steady = {}
    for name, values in traces.items():
        bounded = [item for item in values if 1 <= item["sequence"] <= 199]
        if len(bounded) >= 199:
            seconds = (bounded[-1]["monotonic_ns"] - bounded[0]["monotonic_ns"]) / 1e9
            steady[name] = {
                "records": 198,
                "seconds": seconds,
                "records_per_second": 198 / seconds,
            }
    fault = (
        json.loads((directory / "operator-loss").read_text())
        if (directory / "operator-loss").exists()
        else None
    )
    recovered = traces.get("calls-0-2", [])
    return {
        "clock_scope": "Same Linux host monotonic clock; functional timing, no throughput SLO",
        "workers_ready_to_first_accepted_data_seconds": (min(first_data) - ready_ns)
        / 1e9
        if first_data and ready_ns is not None
        else None,
        "steady_inputs_1_through_199": steady,
        "fault_to_recovered_input_seconds": (
            recovered[0]["monotonic_ns"] - fault["monotonic_ns"]
        )
        / 1e9
        if recovered and fault
        else None,
        "recovered_first_sequence": recovered[0]["sequence"] if recovered else None,
        "recovered_suffix_seconds": (
            recovered[-1]["monotonic_ns"] - recovered[0]["monotonic_ns"]
        )
        / 1e9
        if recovered
        else None,
        "last_input_to_terminal_and_physical_stop_observation_seconds": (
            final_ns - max(item["monotonic_ns"] for item in all_calls)
        )
        / 1e9
        if all_calls and final_ns is not None
        else None,
        "workers_ready_to_terminal_seconds": (final_ns - ready_ns) / 1e9
        if final_ns is not None and ready_ns is not None
        else None,
    }


async def _deployment_slots(db: Any, deployment_id: UUID) -> int:
    async with db.session_context() as session:
        slots = await session.scalar(
            sa.select(db.ConcurrencyLimitV2.active_slots)
            .join(
                db.Deployment,
                db.Deployment.concurrency_limit_id == db.ConcurrencyLimitV2.id,
            )
            .where(db.Deployment.id == deployment_id)
        )
        assert slots is not None
        return slots


@pytest.mark.parametrize(
    "count,fault,asynchronous",
    [
        pytest.param(
            count,
            fault,
            asynchronous,
            marks=pytest.mark.timeout(3600 if count == 5000 else 900),
        )
        for count, fault, asynchronous in [
            (8, "none", False),
            (8, "none", True),
            (500, "none", False),
            (500, "operator-loss-200", False),
            (500, "operator-before-yield-200", False),
            (500, "operator-loss-200", True),
            (5000, "none", False),
            (5000, "none", True),
            (8, "before-declaration", False),
            (8, "after-activation", False),
            (8, "import-probe", False),
            (8, "import-exit", False),
            (8, "terminal-exit-stall", False),
        ]
    ],
)
async def test_actual_process_worker_runs_four_partition_tasks(
    execution_api: RestartableAPI,
    tmp_path: Path,
    db: Any,
    count: int,
    fault: str,
    asynchronous: bool,
):
    server = execution_api
    for number in range(1, count + 1):
        (tmp_path / f"document-{number}.json").write_text(
            json.dumps({"number": number, "document_id": f"invoice-{number}"})
        )
    definition = load_flow_from_entrypoint(
        "tests/fixtures/native_partitioned_stream_flow.py:"
        + ("partitioned_documents_async" if asynchronous else "partitioned_documents")
    )
    pool = "partitioned-" + uuid4().hex
    worker_names = ["left-" + uuid4().hex, "right-" + uuid4().hex]
    with temporary_settings(
        {PREFECT_API_URL: server.url, PREFECT_API_AUTH_STRING: server.auth}
    ):
        async with get_client() as client:
            try:
                await client.create_global_concurrency_limit(
                    GlobalConcurrencyLimitCreate(
                        name="tag:partitioned-fixed-capacity", limit=4
                    )
                )
            except ObjectAlreadyExists:
                existing = await client.read_global_concurrency_limit_by_name(
                    "tag:partitioned-fixed-capacity"
                )
                assert existing.limit == 4 and existing.active_slots == 0
            template = ProcessWorker.get_default_base_job_template()
            template["prefect_native_streaming"] = {
                "schema_version": 1,
                "profile": "bounded-linear-v1",
            }
            work_pool = await client.create_work_pool(
                WorkPoolCreate(name=pool, type="process", base_job_template=template)
            )
            deployment = RunnerDeployment.from_flow(
                definition,
                name="partitioned",
                work_pool_name=pool,
                concurrency_limit=1,
                job_variables={"working_dir": str(Path.cwd()), "stream_output": True},
            )
            deployment_id = await deployment.aapply()
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "PREFECT_SERVER_DATABASE_CONNECTION_URL",
            "PREFECT_API_DATABASE_CONNECTION_URL",
        }
    }
    environment.update(
        PREFECT_API_URL=server.url,
        PREFECT_API_AUTH_STRING=server.auth,
        PREFECT_LOGGING_LEVEL="INFO",
        PREFECT_LOGGING_INTERNAL_LEVEL="WARNING",
        PREFECT_DEBUG_MODE="false",
        PREFECT_WORKER_QUERY_SECONDS="1",
        PREFECT_WORKER_HEARTBEAT_SECONDS="1",
    )
    if fault in {"import-probe", "import-exit"}:
        environment.update(
            NATIVE_PARTITIONED_IMPORT_PROBE="exit"
            if fault == "import-exit"
            else "observe",
            NATIVE_PARTITIONED_IMPORT_DIRECTORY=str(tmp_path),
        )
    logs = [tmp_path / "worker-left.log", tmp_path / "worker-right.log"]
    workers = []
    for name, log in zip(worker_names, logs):
        with log.open("wb") as output:
            workers.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "prefect",
                        "worker",
                        "start",
                        "--pool",
                        pool,
                        "--type",
                        "process",
                        "--name",
                        name,
                        "--limit",
                        "1",
                    ],
                    env=environment,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
            )

    def diagnostics() -> str:
        return "\n".join(path.name + "\n" + path.read_text()[-24000:] for path in logs)

    run = None
    registered = []
    ready_ns = None
    final_ns = None
    passed = False
    lifecycle_checks: dict[str, Any] = {}
    try:
        deadline = time.monotonic() + 40
        while True:
            assert all(worker.poll() is None for worker in workers), diagnostics()
            with server.client() as client:
                registered = [
                    worker
                    for worker in client.read_workers_for_work_pool(pool)
                    if worker.name in worker_names and worker.status.value == "ONLINE"
                ]
            if len(registered) == 2:
                async with db.session_context() as session:
                    capabilities = (
                        await session.scalars(
                            sa.select(db.StreamWorkerCapability.worker_id).where(
                                db.StreamWorkerCapability.worker_id.in_(
                                    [worker.id for worker in registered]
                                ),
                                db.StreamWorkerCapability.protocol
                                == "partitioned-fixed-v1",
                                db.StreamWorkerCapability.protocol_version == 1,
                                db.StreamWorkerCapability.work_pool_id == work_pool.id,
                            )
                        )
                    ).all()
                if set(capabilities) == {worker.id for worker in registered}:
                    ready_ns = time.monotonic_ns()
                    break
            assert time.monotonic() < deadline, diagnostics()
            time.sleep(0.1)
        with server.client() as client:
            run = client.create_flow_run_from_deployment(
                deployment_id,
                parameters={"directory": str(tmp_path), "count": count, "fault": fault},
            )
        deadline = time.monotonic() + (3540 if count == 5000 else 840)
        observed = False
        while True:
            assert all(worker.poll() is None for worker in workers), diagnostics()
            if (
                fault == "before-declaration"
                and not observed
                and (tmp_path / "before-declaration").exists()
            ):
                async with db.session_context() as session:
                    for model in (
                        db.TaskRun,
                        db.StreamGraphRevision,
                        db.StreamPartition,
                    ):
                        assert (
                            await session.scalar(
                                sa.select(sa.func.count())
                                .select_from(model)
                                .where(model.flow_run_id == run.id)
                            )
                            == 0
                        )
                (tmp_path / "declaration-observed").touch()
                observed = True
            if (
                fault in {"operator-loss-200", "operator-before-yield-200"}
                and not observed
                and (tmp_path / "operator-loss").exists()
            ):
                for name in ("source-0-1.pid", "source-1-1.pid", "operator-1-1.pid"):
                    os.kill(int((tmp_path / name).read_text()), 0)
                with server.client() as client:
                    assert (
                        client.read_global_concurrency_limit_by_name(
                            "tag:partitioned-fixed-capacity"
                        ).active_slots
                        == 4
                    )
                    cut_flow = client.read_flow_run(run.id)
                assert cut_flow.state is not None and cut_flow.state.is_running()
                lease_id = cut_flow.state.state_details.deployment_concurrency_lease_id
                assert lease_id is not None
                assert await _deployment_slots(db, deployment_id) == 1
                lifecycle_checks["fault_cut_flow_lease_id"] = str(lease_id)
                lifecycle_checks["fault_cut_flow_slots"] = 1
                cut = json.loads((tmp_path / "operator-loss").read_text())
                async with db.session_context() as session:
                    cursor = await session.get(
                        db.StreamConsumer, UUID(cut["task_run_id"])
                    )
                    assert cursor is not None
                    assert cursor.completed_through == (
                        199 if fault == "operator-before-yield-200" else 200
                    )
                    assert cursor.inflight_sequence == cursor.completed_through + 1
                (tmp_path / "cut-observed").touch()
                observed = True
            if (
                fault == "terminal-exit-stall"
                and not observed
                and (tmp_path / "terminal-stall-entered").exists()
            ):
                stall_pid = int((tmp_path / "terminal-stall-entered").read_text())
                os.kill(stall_pid, 0)
                with server.client() as client:
                    held_flow = client.read_flow_run(run.id)
                assert held_flow.state is not None and held_flow.state.is_running()
                lease_id = held_flow.state.state_details.deployment_concurrency_lease_id
                assert lease_id is not None
                assert await _deployment_slots(db, deployment_id) == 1
                async with db.session_context() as session:
                    row = (
                        await session.execute(
                            sa.select(
                                db.StreamPhysicalSubtask.id,
                                db.StreamPhysicalSubtask.physical_slot_released,
                                db.TaskRun.state_type,
                                db.StreamPartition.state,
                                db.StreamSubtaskAllocation.released,
                            )
                            .join(
                                db.StreamOperatorRun,
                                db.StreamOperatorRun.id
                                == db.StreamPhysicalSubtask.operator_run_id,
                            )
                            .join(
                                db.StreamGraphRevision,
                                db.StreamGraphRevision.id
                                == db.StreamPhysicalSubtask.graph_revision_id,
                            )
                            .join(
                                db.TaskRun,
                                db.TaskRun.id == db.StreamPhysicalSubtask.task_run_id,
                            )
                            .join(
                                db.StreamPartition,
                                db.StreamPartition.id
                                == db.StreamPhysicalSubtask.output_stream_id,
                            )
                            .join(
                                db.StreamSubtaskAllocation,
                                db.StreamSubtaskAllocation.subtask_id
                                == db.StreamPhysicalSubtask.id,
                            )
                            .where(
                                db.StreamGraphRevision.flow_run_id == run.id,
                                db.StreamOperatorRun.operator_uid == "operator.sign",
                                db.StreamPhysicalSubtask.subtask_index == 0,
                            )
                        )
                    ).one()
                    assert row.state_type.value == "COMPLETED"
                    assert row.state == "COMPLETED"
                    assert row.physical_slot_released is False
                    assert row.released is True
                    pending = (
                        sa.select(sa.func.count())
                        .select_from(db.StreamSubtaskAllocation)
                        .where(
                            db.StreamSubtaskAllocation.concurrency_limit_id
                            == db.ConcurrencyLimitV2.id,
                            db.StreamSubtaskAllocation.released.is_(False),
                        )
                        .correlate(db.ConcurrencyLimitV2)
                        .scalar_subquery()
                    )
                    tag_slots, allocations = (
                        await session.execute(
                            sa.select(
                                db.ConcurrencyLimitV2.active_slots, pending
                            ).where(
                                db.ConcurrencyLimitV2.name
                                == "tag:partitioned-fixed-capacity"
                            )
                        )
                    ).one()
                    assert tag_slots == allocations
                lifecycle_checks["terminal_stall"] = {
                    "subtask_id": str(row.id),
                    "pid": stall_pid,
                    "task_state": "COMPLETED",
                    "output_state": "COMPLETED",
                    "physical_slot_released": False,
                    "logical_tag_allocation_released": True,
                    "flow_state": "RUNNING",
                    "flow_lease_id": str(lease_id),
                    "flow_slots": 1,
                    "tag_slots": tag_slots,
                    "unreleased_tag_allocations": allocations,
                }
                observed = True
            with server.client() as client:
                completed = client.read_flow_run(run.id)
            if completed.state is not None and completed.state.is_final():
                final_ns = time.monotonic_ns()
                break
            assert time.monotonic() < deadline, diagnostics()
            time.sleep(1)
        if fault in {"after-activation", "import-exit"}:
            assert completed.state.is_failed(), diagnostics()
        else:
            assert completed.state.is_completed(), diagnostics()
        assert completed.run_count == (2 if fault == "before-declaration" else 1)
        assert await _deployment_slots(db, deployment_id) == 0
        lifecycle_checks["final_flow_slots"] = 0
        with server.client() as client:
            assert (
                client.read_global_concurrency_limit_by_name(
                    "tag:partitioned-fixed-capacity"
                ).active_slots
                == 0
            )
        pids = [int(path.read_text()) for path in tmp_path.glob("*.pid")]
        assert (
            len(pids)
            == len(set(pids))
            == (
                0
                if fault == "import-exit"
                else (
                    5
                    if fault in {"operator-loss-200", "operator-before-yield-200"}
                    else 4
                )
            )
        )
        signed = [
            json.loads(path.read_text()) for path in tmp_path.glob("signed-*.json")
        ]
        assert len(signed) == (0 if fault == "import-exit" else count)
        assert all(item["partition"] == (item["number"] - 1) % 2 for item in signed)
        if fault in {"import-probe", "import-exit"}:
            imports = [
                json.loads(path.read_text()) for path in tmp_path.glob("import-*.json")
            ]
            assert (
                (1 <= len(imports) <= 4)
                if fault == "import-exit"
                else len(imports) == 4
            )
            assert all(
                item
                == {
                    "physical_context": False,
                    "flow_context": False,
                    "headers_present": False,
                    "auth_present": False,
                    "http_status": 401,
                    "import_count": 1,
                }
                for item in imports
            )
        if fault in {"operator-loss-200", "operator-before-yield-200"}:
            assert observed
            replay = 200 if fault == "operator-before-yield-200" else 201
            assert (tmp_path / "replacement-started").read_text() == str(replay)
            first = [
                json.loads(line)
                for line in (tmp_path / "calls-0-1.jsonl").read_text().splitlines()
            ]
            second = [
                json.loads(line)
                for line in (tmp_path / "calls-0-2.jsonl").read_text().splitlines()
            ]
            assert [item["sequence"] for item in first] == list(range(1, 201))
            assert [item["sequence"] for item in second] == list(
                range(replay, count // 2 + 1)
            )
            assert {item["task_run_id"] for item in first} == {
                item["task_run_id"] for item in second
            }
            assert len(list(tmp_path.glob("source-*.pid"))) == 2
            assert len(list(tmp_path.glob("operator-1-*.pid"))) == 1
        async with db.session_context() as session:
            task_runs = (
                await session.scalars(
                    sa.select(db.TaskRun).where(db.TaskRun.flow_run_id == run.id)
                )
            ).all()
            assert len(task_runs) == 4
            for task_run in task_runs:
                states = (
                    await session.scalars(
                        sa.select(db.TaskRunState.type)
                        .where(db.TaskRunState.task_run_id == task_run.id)
                        .order_by(db.TaskRunState.timestamp)
                    )
                ).all()
                assert states[0].value == "PENDING"
                if fault == "import-exit":
                    assert {state.value for state in states} == {"PENDING", "FAILED"}
                    assert task_run.run_count == 0
                else:
                    assert {state.value for state in states} >= {
                        "PENDING",
                        "RUNNING",
                        "COMPLETED",
                    }
                    assert states[-1].value == "COMPLETED"
                    assert task_run.run_count == (
                        2
                        if fault in {"operator-loss-200", "operator-before-yield-200"}
                        and task_run.name.startswith("partitioned_operator")
                        and task_run.name.endswith("[0]")
                        else 1
                    )
            assert (
                await session.scalar(
                    sa.select(sa.func.count())
                    .select_from(db.StreamPartition)
                    .where(db.StreamPartition.flow_run_id == run.id)
                )
                == 4
            )
            placement = (
                await session.execute(
                    sa.select(
                        db.StreamPhysicalSubtask.subtask_index,
                        db.StreamOperatorRun.operator_uid,
                        db.StreamPhysicalSubtask.assigned_worker_id,
                        db.StreamPhysicalSubtask.physical_slot_released,
                    )
                    .join(
                        db.StreamOperatorRun,
                        db.StreamOperatorRun.id
                        == db.StreamPhysicalSubtask.operator_run_id,
                    )
                    .join(
                        db.StreamGraphRevision,
                        db.StreamGraphRevision.id
                        == db.StreamPhysicalSubtask.graph_revision_id,
                    )
                    .where(db.StreamGraphRevision.flow_run_id == run.id)
                )
            ).all()
            assert len(placement) == 4
            assert {item.assigned_worker_id for item in placement} == {
                worker.id for worker in registered
            }
            assert all(item.physical_slot_released for item in placement)
            if fault == "terminal-exit-stall":
                assert observed
                assert not (tmp_path / "terminal-stall-natural-exit").exists()
                stall = lifecycle_checks["terminal_stall"]
                with pytest.raises(ProcessLookupError):
                    os.kill(stall["pid"], 0)
                retirement = (
                    await session.scalars(
                        sa.select(db.StreamSubtaskReplacement).where(
                            db.StreamSubtaskReplacement.subtask_id
                            == UUID(stall["subtask_id"])
                        )
                    )
                ).one()
                assert retirement.state == "RETIRED"
                assert retirement.death_witness["child_process_id"] == stall["pid"]
                assert retirement.retirement_outcome["task_state"] == "COMPLETED"
                assert (
                    retirement.retirement_outcome["outcome_kind"]
                    == "COMPLETED_RECONCILED"
                )
                lifecycle_checks["terminal_stall_retirement"] = {
                    "state": retirement.state,
                    "death_witness_kind": retirement.death_witness["kind"],
                    "outcome_kind": retirement.retirement_outcome["outcome_kind"],
                    "physical_slot_released": True,
                    "natural_exit_marker_present": False,
                }
            worker_pids = {
                worker.id: workers[worker_names.index(worker.name)].pid
                for worker in registered
            }
            if fault != "import-exit":
                for item in placement:
                    prefix = (
                        "source"
                        if item.operator_uid == "source.documents"
                        else "operator"
                    )
                    assert (
                        int(
                            (
                                tmp_path / f"{prefix}-{item.subtask_index}-1.ppid"
                            ).read_text()
                        )
                        == worker_pids[item.assigned_worker_id]
                    )
        passed = True
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.terminate()
        for worker in workers:
            try:
                worker.wait(timeout=15)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
        try:
            observation = {
                "status": "PASS" if passed else "FAIL",
                "count": count,
                "fault": fault,
                "asynchronous": asynchronous,
                "flow_run_id": str(run.id) if run is not None else None,
                "workers": [
                    {
                        "worker_id": str(item.id),
                        "name": item.name,
                        "pid": workers[worker_names.index(item.name)].pid,
                    }
                    for item in registered
                ],
                "phase_timings": _phase_observation(tmp_path, ready_ns, final_ns),
                "lifecycle_checks": lifecycle_checks,
            }
        except Exception as timing_error:
            (tmp_path / "case-observation.json").write_text(
                json.dumps(
                    {
                        "status": "TIMING_CAPTURE_FAILED",
                        "error_type": type(timing_error).__name__,
                    }
                )
                + "\n"
            )
            if passed:
                raise
        else:
            (tmp_path / "case-observation.json").write_text(
                json.dumps(observation, indent=2) + "\n"
            )
