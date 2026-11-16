"""Trusted worker DAG journal replay and exact cleanup over real HTTP/SQL."""

import secrets
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.schemas import stream_dag_executions as s
from prefect.server.schemas import stream_subtasks as physical
from prefect.types._datetime import now
from tests.server.api.test_stream_dag_executions import (
    advertise_graph_workers,
    parent_body,
    sealed_execution_graph,
)
from tests.server.api.test_stream_dag_executions import execution_api as execution_api
from tests.server.api.test_stream_dags import assert_error
from tests.server.api.test_stream_routes import route_snapshot
from tests.server.api.test_stream_workers import new_worker

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


def worker_calls() -> list[tuple[str, dict[str, Any]]]:
    worker_id, subtask_id, graph_id, launch_id = (uuid4() for _ in range(4))
    principal = physical.StreamWorkerPrincipal(
        work_pool_id=uuid4(),
        host_id=uuid4(),
        boot_id=uuid4(),
        launcher_process_id=100,
        launcher_start_ticks=1000,
    ).model_dump(mode="json")
    root = f"/streams/v2/dags/workers/{worker_id}"
    launches = root + f"/subtasks/{subtask_id}/launches"
    return [
        (root + "/poll", {"principal": principal}),
        (
            root + f"/graphs/{graph_id}/startup-abort",
            {
                "principal": principal,
                "start_operation_id": str(uuid4()),
                "graph_digest": "aa" * 32,
                "routing_digest": "bb" * 32,
            },
        ),
        (
            launches + "/prepare",
            {
                "principal": principal,
                "operation_id": str(uuid4()),
                "start_operation_id": str(uuid4()),
                "replacement_operation_id": None,
            },
        ),
        (
            launches + f"/{launch_id}/record",
            {
                "principal": principal,
                "operation_id": str(uuid4()),
                "child_process_id": 200,
                "process_start_ticks": 2000,
                "local_journal_digest": "cc" * 32,
            },
        ),
        (
            launches + f"/{launch_id}/close",
            {
                "principal": principal,
                "operation_id": str(uuid4()),
                "proof": {
                    "kind": "CLOSED_UNSTARTED",
                    "local_journal_digest": "dd" * 32,
                    "recovery_complete": True,
                    "spawn_fence_closed": True,
                },
            },
        ),
        (
            root + f"/subtasks/{subtask_id}/stop",
            {
                "principal": principal,
                "operation_id": str(uuid4()),
                "control_operation_id": str(uuid4()),
            },
        ),
    ]


@pytest.mark.parametrize(
    "authorization", [None, "Basic d3Jvbmc6d3Jvbmc=", "PrefectSubtask " + "ab" * 32]
)
async def test_worker_routes_require_platform_basic(
    execution_api: httpx.AsyncClient, authorization: str | None
):
    headers = {} if authorization is None else {"Authorization": authorization}
    for path, body in worker_calls():
        response = await execution_api.post(path, json=body, headers=headers, auth=None)
        assert response.status_code == 401, response.text
        assert "ab" * 32 not in response.text


@pytest.mark.parametrize(
    "field,value",
    [
        ("protocol_version", True),
        ("protocol_version", 1.0),
        ("protocol_version", 2),
        ("limit", True),
        ("limit", 0),
        ("limit", 129),
        ("active_subtask_ids", [str(UUID(int=1)), str(UUID(int=1))]),
    ],
)
async def test_worker_poll_is_strict_and_bounded(
    db: PrefectDBInterface,
    execution_api: httpx.AsyncClient,
    field: str,
    value: Any,
):
    path, body = worker_calls()[0]
    body[field] = value
    before = await route_snapshot(db)
    response = await execution_api.post(path, json=body)
    assert_error(response, 422, "PROTOCOL_ERROR")
    assert await route_snapshot(db) == before


async def test_worker_control_validation_redacts_secrets(
    execution_api: httpx.AsyncClient,
):
    secret = secrets.token_hex(32)
    for path, body in worker_calls():
        body["targets"] = secret
        response = await execution_api.post(path, json=body)
        assert_error(response, 422, "PROTOCOL_ERROR")
        assert secret not in response.text


async def test_dag_and_fixed_advertisements_coexist_postgres(
    db: PrefectDBInterface, execution_api: httpx.AsyncClient
):
    worker_id, principal = await new_worker(execution_api)
    fixed = await execution_api.post(
        f"/streams/v2/workers/{worker_id}/poll",
        json=physical.StreamWorkerPollRequest(principal=principal).model_dump(
            mode="json"
        ),
    )
    assert fixed.status_code == 200 and fixed.content == b"[]", fixed.text
    dag = await execution_api.post(
        f"/streams/v2/dags/workers/{worker_id}/poll",
        json=s.StreamDAGWorkerPollRequest(principal=principal).model_dump(mode="json"),
    )
    assert dag.status_code == 200 and dag.content == b"[]", dag.text
    async with db.session_context() as check:
        capabilities = list(
            await check.scalars(
                sa.select(db.StreamWorkerCapability).where(
                    db.StreamWorkerCapability.worker_id == worker_id
                )
            )
        )
        assert {row.protocol for row in capabilities} == {
            "partitioned-fixed-v1",
            "partitioned-dag-v1",
        }
        assert all(
            row.launcher_process_id == principal.launcher_process_id
            for row in capabilities
        )
    fixed_again = await execution_api.post(
        f"/streams/v2/workers/{worker_id}/poll",
        json=physical.StreamWorkerPollRequest(principal=principal).model_dump(
            mode="json"
        ),
    )
    assert fixed_again.status_code == 200 and fixed_again.content == fixed.content


async def test_worker_journal_controls_refuse_sqlite_without_mutation(
    db: PrefectDBInterface, execution_api: httpx.AsyncClient
):
    assert db.dialect.name == "sqlite"
    before = await route_snapshot(db)
    for path, body in worker_calls():
        response = await execution_api.post(path, json=body)
        assert_error(response, 422, "UNSUPPORTED_BACKEND")
    assert await route_snapshot(db) == before


@pytest.mark.timeout(90)
async def test_cancelled_launch_retains_slot_until_exact_closed_attempt_postgres(
    db: PrefectDBInterface,
    session: AsyncSession,
    execution_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await sealed_execution_graph(db, session, execution_api, deployment)
    workers = await advertise_graph_workers(
        db, session, execution_api, plan.flow_run_id
    )
    start_body = parent_body(plan.id, "start", plan.graph_digest)
    start_path = f"/streams/v2/dags/{plan.id}/start"
    started = await execution_api.post(start_path, json=start_body, headers=headers)
    assert started.status_code == 200, started.text
    start = s.StreamDAGOperationResult.model_validate(started.json())
    assert start.execution.phase == "STARTING"
    assert start.execution.counts.physical_slots_remaining == 4
    replay_start = await execution_api.post(
        start_path, json=start_body, headers=headers
    )
    assert replay_start.status_code == 200, replay_start.text
    assert replay_start.json() == started.json()
    all_assignments: list[
        tuple[UUID, physical.StreamWorkerPrincipal, s.StreamDAGWorkerAssignment]
    ] = []
    for worker_id, principal in workers:
        polled = await execution_api.post(
            f"/streams/v2/dags/workers/{worker_id}/poll",
            json=s.StreamDAGWorkerPollRequest(principal=principal).model_dump(
                mode="json"
            ),
        )
        assert polled.status_code == 200, polled.text
        own = [s.StreamDAGWorkerAssignment.model_validate(row) for row in polled.json()]
        assert len(own) == 2
        for row in own:
            assert row.assignment.assigned_worker_id == worker_id
            assert row.phase == "STARTING" and row.current_grant is None
            assert not row.bootstrap_ready and row.launch_attempts == []
            assert headers["X-Prefect-Stream-Execution-Capability"] not in polled.text
            all_assignments.append((worker_id, principal, row))
    worker_id, principal, own = all_assignments[0]
    subtask_id = own.assignment.id
    prefix = f"/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}"
    prepare = s.StreamDAGLaunchPrepareRequest(
        principal=principal,
        operation_id=uuid5(
            subtask_id, "prefect-stream-dag-launch-v1/" + start_body["operation_id"]
        ),
        start_operation_id=UUID(start_body["operation_id"]),
        replacement_operation_id=None,
    )
    prepared = await execution_api.post(
        prefix + "/launches/prepare", json=prepare.model_dump(mode="json")
    )
    assert prepared.status_code == 200, prepared.text
    candidate = s.StreamDAGLaunchOperationResult.model_validate(prepared.json())
    assert (
        candidate.current.state == "PREPARED" and candidate.current.attempt_index == 1
    )
    assert candidate.disposition == "SPAWN_ALLOWED"
    prepared_again = await execution_api.post(
        prefix + "/launches/prepare", json=prepare.model_dump(mode="json")
    )
    assert prepared_again.status_code == 200, prepared_again.text
    assert prepared_again.json() == prepared.json()

    cancelled = await execution_api.post(
        f"/streams/v2/dags/{plan.id}/cancel",
        json=parent_body(plan.id, "cancel", plan.graph_digest),
        headers=headers,
    )
    assert cancelled.status_code == 200, cancelled.text
    cancel = s.StreamDAGOperationResult.model_validate(cancelled.json())
    assert cancel.execution.phase == "STOPPING"
    assert cancel.execution.control is not None
    assert cancel.execution.control.kind == "CANCEL"
    control_id = cancel.execution.control.operation_id
    after_cancel = await route_snapshot(db)
    changed_cancel = parent_body(plan.id, "cancel", plan.graph_digest)
    changed_cancel["reason"] = "Changed accepted reason"
    rejected_change = await execution_api.post(
        f"/streams/v2/dags/{plan.id}/cancel", json=changed_cancel, headers=headers
    )
    assert_error(rejected_change, 409, "IDENTITY_CONFLICT")
    conflicting_fail = await execution_api.post(
        f"/streams/v2/dags/{plan.id}/fail",
        json=parent_body(plan.id, "fail", plan.graph_digest),
        headers=headers,
    )
    assert_error(conflicting_fail, 409, "CONTROL_CONFLICT")
    assert await route_snapshot(db) == after_cancel
    start_after_cancel = await execution_api.post(
        start_path, json=start_body, headers=headers
    )
    assert start_after_cancel.status_code == 200, start_after_cancel.text
    assert start_after_cancel.json()["receipt"] == started.json()["receipt"]
    assert start_after_cancel.json()["execution"]["phase"] == "STOPPING"
    own_stop = s.StreamDAGWorkerStopRequest(
        principal=principal,
        operation_id=uuid5(control_id, "prefect-stream-dag-stop-v1/" + str(subtask_id)),
        control_operation_id=control_id,
    )
    before_stop = await route_snapshot(db)
    refused = await execution_api.post(
        prefix + "/stop", json=own_stop.model_dump(mode="json")
    )
    assert_error(refused, 409, "PHYSICAL_STOP_REQUIRED")
    assert await route_snapshot(db) == before_stop

    # PID/start ticks and the eventual wait are observed from an actual process.
    # Placement supervisors are metadata fixtures; this is a journal/HTTP gate,
    # not a complete physical worker or Task-body execution acceptance claim.
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        stat = Path(f"/proc/{process.pid}/stat").read_text()
        ticks = int(stat[stat.rfind(")") + 2 :].split()[19])
        launch_id = candidate.current.id
        record = s.StreamDAGLaunchRecordRequest(
            principal=principal,
            operation_id=uuid5(launch_id, "prefect-stream-dag-record-v1"),
            child_process_id=process.pid,
            process_start_ticks=ticks,
            local_journal_digest="cc" * 32,
        )
        recorded = await execution_api.post(
            prefix + f"/launches/{launch_id}/record",
            json=record.model_dump(mode="json"),
        )
        assert recorded.status_code == 200, recorded.text
        current = s.StreamDAGLaunchOperationResult.model_validate(recorded.json())
        assert current.disposition == "STOP_REQUIRED"
        assert current.current.state == "PID_RECORDED"
        assert current.current.child_process_id == process.pid
        process.terminate()
        exit_code = process.wait(timeout=10)
        close = s.StreamDAGLaunchCloseRequest(
            principal=principal,
            operation_id=uuid5(launch_id, "prefect-stream-dag-close-v1"),
            proof=s.StreamDAGLaunchDeathProof(
                kind="DEATH",
                death_witness={
                    "kind": "WAITPID",
                    "observed_at": now("UTC"),
                    "host_id": principal.host_id,
                    "boot_id": principal.boot_id,
                    "child_process_id": process.pid,
                    "process_start_ticks": ticks,
                    "exit_code": exit_code,
                },
            ),
        )
        wrong_close = close.model_dump(mode="json")
        wrong_close["proof"]["death_witness"]["process_start_ticks"] = ticks + 1
        before_close = await route_snapshot(db)
        refused_close = await execution_api.post(
            prefix + f"/launches/{launch_id}/close", json=wrong_close
        )
        assert_error(refused_close, 409, "IDENTITY_CONFLICT")
        assert await route_snapshot(db) == before_close
        closed = await execution_api.post(
            prefix + f"/launches/{launch_id}/close", json=close.model_dump(mode="json")
        )
        assert closed.status_code == 200, closed.text
        closed_again = await execution_api.post(
            prefix + f"/launches/{launch_id}/close", json=close.model_dump(mode="json")
        )
        assert closed_again.status_code == 200 and closed_again.json() == closed.json()
        old_record = await execution_api.post(
            prefix + f"/launches/{launch_id}/record",
            json=record.model_dump(mode="json"),
        )
        assert old_record.status_code == 200, old_record.text
        assert old_record.json()["receipt"] == recorded.json()["receipt"]
        assert old_record.json()["current"]["state"] == "CLOSED"
        assert old_record.json()["disposition"] == "STOP_REQUIRED"
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)

    own_receipt: dict[str, Any] | None = None
    for assigned_worker, identity, assignment in all_assignments:
        target = assignment.assignment.id
        stop = s.StreamDAGWorkerStopRequest(
            principal=identity,
            operation_id=uuid5(control_id, "prefect-stream-dag-stop-v1/" + str(target)),
            control_operation_id=control_id,
        )
        stopped = await execution_api.post(
            f"/streams/v2/dags/workers/{assigned_worker}/subtasks/{target}/stop",
            json=stop.model_dump(mode="json"),
        )
        assert stopped.status_code == 200, stopped.text
        receipt = s.StreamDAGStopReceipt.model_validate(stopped.json())
        assert receipt.physical_slot_released and receipt.grant_id is None
        assert receipt.physical_epoch == 0 and receipt.task_state == "CANCELLED"
        if target == subtask_id:
            own_receipt = stopped.json()
    assert own_receipt is not None
    repeated = await execution_api.post(
        prefix + "/stop", json=own_stop.model_dump(mode="json")
    )
    assert repeated.status_code == 200 and repeated.json() == own_receipt
    terminal = await execution_api.get(
        f"/streams/v2/dags/{plan.id}/execution", headers=headers
    )
    assert terminal.status_code == 200, terminal.text
    final = s.StreamDAGExecution.model_validate(terminal.json())
    assert final.phase == "TERMINAL" and final.terminal_state == "CANCELLED"
    assert final.cleanup_complete and final.counts.physical_slots_remaining == 0
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, plan.flow_run_id)
        assert flow is not None and flow.state_type.value == "CANCELLED"
        assert (
            await check.scalar(sa.select(sa.func.count()).select_from(db.StreamRecord))
            == 0
        )
