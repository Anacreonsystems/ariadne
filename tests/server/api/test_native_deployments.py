"""Native deployment enrollment through the real server API and database."""

import asyncio
import secrets
from copy import deepcopy
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server import models, schemas
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import native_deployments
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas.native_deployments import (
    NativeStreamDeclaration,
    NativeStreamLaunchHeartbeat,
    NativeStreamLaunchHeartbeatRequest,
)
from prefect.server.schemas.streams import StreamExecutionCredentials


def _declaration() -> NativeStreamDeclaration:
    return NativeStreamDeclaration(
        schema_version=1,
        protocol_version=1,
        profile="bounded-linear-v1",
        flow_version="native-deployment-v1",
        flow_code_digest="a" * 64,
        resource_profile="bounded-default-v1",
        flow_retries=2,
        flow_retry_delay_seconds=0,
    )


async def _postgres_17(db: PrefectDBInterface) -> bool:
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    return db.dialect.name == "postgresql" and bool(version) and version[0] == 17


async def _native_flow_run(
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
) -> tuple[orm_models.FlowRun, orm_models.Worker, UUID]:
    declaration = _declaration()
    deployment.version = "deployment-v1"
    deployment.job_variables = {
        "prefect_native_streaming": declaration.model_dump(mode="json")
    }
    assert deployment.work_queue_id is not None
    work_queue = await session.get(db.WorkQueue, deployment.work_queue_id)
    assert work_queue is not None
    work_pool = await session.get(db.WorkPool, work_queue.work_pool_id)
    assert work_pool is not None
    worker = await models.workers.record_worker_heartbeat(
        session=session,
        work_pool=work_pool,
        worker_name=f"native-worker-{uuid4()}",
        heartbeat_interval_seconds=30,
        return_worker=True,
    )
    assert worker is not None
    flow_run = await models.flow_runs.create_flow_run(
        session=session,
        flow_run=schemas.core.FlowRun(
            flow_id=deployment.flow_id,
            deployment_id=deployment.id,
            deployment_version=deployment.version,
            work_queue_id=deployment.work_queue_id,
            empirical_policy=schemas.core.FlowRunPolicy(retries=2),
            state=schemas.states.Scheduled(),
        ),
    )
    await session.commit()
    return flow_run, worker, work_queue.work_pool_id


def _enrollment_body(
    flow_run_id: UUID,
    worker_id: UUID,
    work_pool_id: UUID,
    declaration_digest: str,
) -> dict[str, Any]:
    return {
        "flow_run_id": str(flow_run_id),
        "operation_id": str(uuid4()),
        "expected_generation": 0,
        "declaration_digest": declaration_digest,
        "worker_id": str(worker_id),
        "work_pool_id": str(work_pool_id),
        "launcher_instance_id": str(uuid4()),
        "launcher_host_id": str(uuid4()),
        "launcher_boot_id": str(uuid4()),
        "launcher_process_id": 2468,
        "launcher_start_ticks": 13579,
        "launch_profile": "linux-local-pidfd-v1",
        "holder_id": str(uuid4()),
        "holder_capability": secrets.token_hex(32),
        "holder_event_public_key": secrets.token_hex(32),
        "supervisor_id": str(uuid4()),
        "supervisor_capability": secrets.token_hex(32),
    }


async def _active_launch(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
) -> tuple[
    orm_models.FlowRun,
    orm_models.Worker,
    UUID,
    dict[str, Any],
    dict[str, str],
    dict[str, Any],
]:
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    body = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    enrolled = await client.post("/streams/v1/deployments/enroll", json=body)
    assert enrolled.status_code == 200
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": body["holder_id"],
        "X-Prefect-Stream-Execution-Capability": body["holder_capability"],
    }
    source = await client.post(
        "/streams/v1/submissions/reserve",
        json={
            "flow_run_id": str(flow_run.id),
            "submission_key": "source.documents",
            "role": "SOURCE",
            "protocol": "source-v1",
            "task_name": "documents",
            "task_key": "documents-key",
            "task_version": "task-v1",
            "flow_version": _declaration().flow_version,
            "deployment_id": str(deployment.id),
            "deployment_version": deployment.version,
            "parameters": {"connector": "documents"},
            "task_policy": {"retries": 2},
        },
        headers=headers,
    )
    assert source.status_code == 200
    sealed = await client.post(
        "/streams/v1/executions/seal-manifest",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(uuid4()),
            "manifests": [
                {
                    "submission_key": source.json()["submission_key"],
                    "manifest_digest": source.json()["manifest_digest"],
                }
            ],
        },
        headers=headers,
    )
    assert sealed.status_code == 200
    registration = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": body["operation_id"],
        "generation": 1,
        "launcher_instance_id": body["launcher_instance_id"],
        "host_id": str(uuid4()),
        "boot_id": str(uuid4()),
        "child_process_id": 4321,
        "process_start_ticks": 987654,
        "launch_nonce_digest": "b" * 64,
    }
    registered = await client.post(
        "/streams/v1/deployments/launches/register-process",
        json=registration,
        headers=headers,
    )
    assert registered.status_code == 200
    running = await client.post(
        f"/flow_runs/{flow_run.id}/set_state",
        json={"state": {"type": "RUNNING", "name": "Running"}},
        headers=headers,
    )
    assert running.status_code == 201
    return flow_run, worker, work_pool_id, body, headers, registration


async def test_unstarted_launch_retirement_is_exact_and_leaves_no_child(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL launch cleanup is covered by the PostgreSQL profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    enrollment = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    enrolled = await client.post("/streams/v1/deployments/enroll", json=enrollment)
    assert enrolled.status_code == 200
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": enrollment["holder_id"],
        "X-Prefect-Stream-Execution-Capability": enrollment["holder_capability"],
    }
    request = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": enrollment["operation_id"],
        "error": "child process could not be started",
    }
    retired = await client.post(
        "/streams/v1/deployments/launches/retire-unstarted",
        json=request,
        headers=headers,
    )
    replay = await client.post(
        "/streams/v1/deployments/launches/retire-unstarted",
        json=request,
        headers=headers,
    )
    assert retired.status_code == replay.status_code == 200
    assert retired.json() == replay.json()
    assert retired.json()["kind"] == "SPAWN_FAILED"
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, flow_run.id)
        authority = await check.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        launch = await check.scalar(
            sa.select(db.NativeStreamLaunchIdentity).where(
                db.NativeStreamLaunchIdentity.flow_run_id == flow_run.id
            )
        )
        process_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.NativeStreamLaunchIdentity)
            .where(
                db.NativeStreamLaunchIdentity.flow_run_id == flow_run.id,
                db.NativeStreamLaunchIdentity.child_process_id.is_not(None),
            )
        )
    assert flow is not None and flow.state_type == "FAILED"
    assert authority is not None and authority.state == "RELEASED"
    assert launch is not None and launch.state == "RELEASED"
    assert process_count == 0


async def test_verified_dead_launcher_retires_without_child_or_graph(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL launcher retirement is covered by its PG profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    enrollment = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    enrolled = await client.post("/streams/v1/deployments/enroll", json=enrollment)
    assert enrolled.status_code == 200
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    candidates = await client.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": enrollment["launcher_host_id"],
        },
    )
    assert candidates.status_code == 200
    assert len(candidates.json()) == 1
    candidate = candidates.json()[0]
    assert candidate["identity_kind"] == "LAUNCHER"
    assert candidate["host_id"] == enrollment["launcher_host_id"]
    assert candidate["boot_id"] == enrollment["launcher_boot_id"]
    assert candidate["child_process_id"] == enrollment["launcher_process_id"]
    assert candidate["process_start_ticks"] == enrollment["launcher_start_ticks"]

    operation_id = uuid4()
    challenge = secrets.token_hex(32)
    authorization = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": enrollment["operation_id"],
        "worker_id": str(worker.id),
        "work_pool_id": str(work_pool_id),
        "host_id": enrollment["launcher_host_id"],
        "identity_kind": "LAUNCHER",
        "challenge_capability": challenge,
    }
    authorized = await client.post(
        "/streams/v1/deployments/replacements/authorize", json=authorization
    )
    assert authorized.status_code == 200
    assert authorized.json()["identity_kind"] == "LAUNCHER"
    retirement = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": enrollment["operation_id"],
        "challenge_capability": challenge,
        "death_witness": {
            "kind": "IDENTITY_GONE",
            "observed_at": "2026-11-04T16:30:00Z",
            "host_id": enrollment["launcher_host_id"],
            "boot_id": enrollment["launcher_boot_id"],
            "child_process_id": enrollment["launcher_process_id"] + 1,
            "process_start_ticks": enrollment["launcher_start_ticks"],
        },
        "error": "launcher exited before child creation",
    }
    mismatch = await client.post(
        "/streams/v1/deployments/replacements/retire-verified", json=retirement
    )
    assert mismatch.status_code == 409
    assert mismatch.json()["detail"]["code"] == "STALE_EXECUTION"
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, flow_run.id)
        authority = await check.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        launch = await check.scalar(
            sa.select(db.NativeStreamLaunchIdentity).where(
                db.NativeStreamLaunchIdentity.flow_run_id == flow_run.id
            )
        )
    assert flow is not None and flow.state_type == "SCHEDULED"
    assert authority is not None and authority.state == "CLAIMED"
    assert launch is not None and launch.state == "ENROLLED"

    retirement["death_witness"]["child_process_id"] = enrollment["launcher_process_id"]
    retired = await client.post(
        "/streams/v1/deployments/replacements/retire-verified", json=retirement
    )
    replay = await client.post(
        "/streams/v1/deployments/replacements/retire-verified", json=retirement
    )
    assert retired.status_code == replay.status_code == 200
    assert retired.json() == replay.json()
    assert retired.json()["kind"] == "SPAWN_FAILED"
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, flow_run.id)
        snapshot = await check.scalar(
            sa.select(db.NativeStreamDeploymentSnapshot).where(
                db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run.id
            )
        )
        launch = await check.scalar(
            sa.select(db.NativeStreamLaunchIdentity).where(
                db.NativeStreamLaunchIdentity.flow_run_id == flow_run.id
            )
        )
        task_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.TaskRun)
            .where(db.TaskRun.flow_run_id == flow_run.id)
        )
    assert flow is not None and flow.state_type == "FAILED"
    assert snapshot is not None and snapshot.state == "RELEASED"
    assert launch is not None and launch.state == "RELEASED"
    assert task_count == 0


async def test_process_registration_wins_over_launcher_death_authorization(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL launcher race is covered by its PG profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    enrollment = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    assert (
        await client.post("/streams/v1/deployments/enroll", json=enrollment)
    ).status_code == 200
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": enrollment["holder_id"],
        "X-Prefect-Stream-Execution-Capability": enrollment["holder_capability"],
    }
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    operation_id = uuid4()
    challenge = secrets.token_hex(32)
    authorized = await client.post(
        "/streams/v1/deployments/replacements/authorize",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(operation_id),
            "expected_generation": 1,
            "enrollment_operation_id": enrollment["operation_id"],
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": enrollment["launcher_host_id"],
            "identity_kind": "LAUNCHER",
            "challenge_capability": challenge,
        },
    )
    assert authorized.status_code == 200
    registration = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": enrollment["operation_id"],
        "generation": 1,
        "launcher_instance_id": enrollment["launcher_instance_id"],
        "host_id": enrollment["launcher_host_id"],
        "boot_id": enrollment["launcher_boot_id"],
        "child_process_id": 9999,
        "process_start_ticks": 7777,
        "launch_nonce_digest": "9" * 64,
    }
    registered = await client.post(
        "/streams/v1/deployments/launches/register-process",
        json=registration,
        headers=headers,
    )
    assert registered.status_code == 200
    retirement = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": enrollment["operation_id"],
        "challenge_capability": challenge,
        "death_witness": {
            "kind": "IDENTITY_GONE",
            "observed_at": "2026-11-04T16:40:00Z",
            "host_id": enrollment["launcher_host_id"],
            "boot_id": enrollment["launcher_boot_id"],
            "child_process_id": enrollment["launcher_process_id"],
            "process_start_ticks": enrollment["launcher_start_ticks"],
        },
        "error": "stale launcher observation",
    }
    refused = await client.post(
        "/streams/v1/deployments/replacements/retire-verified", json=retirement
    )
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "STALE_EXECUTION"
    candidates = await client.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
        },
    )
    assert candidates.status_code == 200
    assert len(candidates.json()) == 1
    assert candidates.json()[0]["identity_kind"] == "CHILD"


async def test_enroll_register_and_public_plan_are_exact_and_secret_free(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL enrollment is covered by the PostgreSQL profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    assert flow_run.flow_version == _declaration().flow_version
    assert flow_run.empirical_policy.retries == 2
    assert flow_run.empirical_policy.retry_delay == 0
    assert flow_run.labels["prefect.native-streaming"] == "bounded-linear-v1"
    plan = await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")
    assert plan.status_code == 200
    snapshot = plan.json()
    assert snapshot["declaration"] == _declaration().model_dump(mode="json")
    assert snapshot["current_launch"] is None
    running = {"state": {"type": "RUNNING", "name": "Running"}}
    unclaimed = await client.post(f"/flow_runs/{flow_run.id}/set_state", json=running)
    assert unclaimed.status_code == 409
    assert unclaimed.json()["detail"]["code"] == "RECOVERY_NOT_READY"

    body = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, snapshot["declaration_digest"]
    )
    invalid_body = dict(body, expected_generation=True)
    invalid = await client.post("/streams/v1/deployments/enroll", json=invalid_body)
    assert invalid.status_code == 422
    assert invalid.json()["detail"]["code"] == "PROTOCOL_ERROR"
    assert body["holder_capability"] not in invalid.text
    assert body["supervisor_capability"] not in invalid.text
    first = await client.post("/streams/v1/deployments/enroll", json=body)
    replay = await client.post("/streams/v1/deployments/enroll", json=body)
    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.json()["state"] == "ENROLLED"
    async with db.session_context() as check:
        launch = await check.scalar(
            sa.select(db.NativeStreamLaunchIdentity).where(
                db.NativeStreamLaunchIdentity.flow_run_id == flow_run.id
            )
        )
        assert launch is not None
        assert launch.holder_capability_digest != body["holder_capability"]
        assert launch.supervisor_capability_digest != body["supervisor_capability"]
        assert launch.holder_event_public_key == body["holder_event_public_key"]

    registration = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": body["operation_id"],
        "generation": 1,
        "launcher_instance_id": body["launcher_instance_id"],
        "host_id": str(uuid4()),
        "boot_id": str(uuid4()),
        "child_process_id": 4321,
        "process_start_ticks": 987654,
        "launch_nonce_digest": "b" * 64,
    }
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": body["holder_id"],
        "X-Prefect-Stream-Execution-Capability": body["holder_capability"],
    }
    unregistered = await client.post(
        f"/flow_runs/{flow_run.id}/set_state", json=running, headers=headers
    )
    assert unregistered.status_code == 409
    assert unregistered.json()["detail"]["code"] == "RECOVERY_NOT_READY"
    registered = await client.post(
        "/streams/v1/deployments/launches/register-process",
        json=registration,
        headers=headers,
    )
    registered_replay = await client.post(
        "/streams/v1/deployments/launches/register-process",
        json=registration,
        headers=headers,
    )
    assert registered.status_code == registered_replay.status_code == 200
    assert registered.json() == registered_replay.json()
    assert registered.json()["state"] == "PROCESS_REGISTERED"
    started = await client.post(
        f"/flow_runs/{flow_run.id}/set_state", json=running, headers=headers
    )
    assert started.status_code == 201

    projected = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    encoded = str(projected)
    for secret in (
        body["holder_capability"],
        body["supervisor_capability"],
        body["holder_event_public_key"],
        registration["launch_nonce_digest"],
    ):
        assert secret not in encoded
    for private_field in (
        "child_process_id",
        "process_start_ticks",
        "boot_id",
        "host_id",
    ):
        assert private_field not in encoded


async def test_projection_is_bounded_and_payload_free(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL projection is covered by the PostgreSQL profile")
    ordinary = await client.get(f"/streams/v1/deployments/{uuid4()}/projection")
    assert ordinary.status_code == 404
    flow_run, _, _, enrollment, _, _ = await _active_launch(
        client, session, db, deployment
    )
    projection = await client.get(f"/streams/v1/deployments/{flow_run.id}/projection")
    assert projection.status_code == 200
    body = projection.json()
    assert body["flow_run_id"] == str(flow_run.id)
    assert body["generation"] == 1
    assert body["authority_state"] == "ACTIVE"
    assert body["launch_state"] == "PROCESS_REGISTERED"
    assert body["worker_id"] == enrollment["worker_id"]
    assert len(body["items"]) == 1
    source = body["items"][0]
    assert source["role"] == "SOURCE"
    assert source["input_stream_id"] is None
    assert source["pending_inputs"] is None
    assert source["record_limit"] >= 1
    assert source["byte_limit"] >= 1
    assert body["finite_items"] == []
    encoded = projection.text
    assert enrollment["holder_capability"] not in encoded
    assert enrollment["supervisor_capability"] not in encoded
    assert "child_process_id" not in encoded


async def test_enrollment_drift_and_legacy_takeover_fail_closed(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL enrollment is covered by the PostgreSQL profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    body = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    enrolled = await client.post("/streams/v1/deployments/enroll", json=body)
    assert enrolled.status_code == 200

    changed = dict(body, worker_id=str(uuid4()))
    conflict = await client.post("/streams/v1/deployments/enroll", json=changed)
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "IDENTITY_CONFLICT"

    legacy_claim = await client.post(
        "/streams/v1/executions/claim",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(uuid4()),
            "expected_generation": 0,
            "holder_id": str(uuid4()),
            "holder_capability": secrets.token_hex(32),
            "holder_event_public_key": secrets.token_hex(32),
            "supervisor_id": str(uuid4()),
            "supervisor_capability": secrets.token_hex(32),
            "protocol_version": 1,
        },
    )
    assert legacy_claim.status_code == 422
    assert legacy_claim.json()["detail"]["code"] == "UNSUPPORTED_PROFILE"

    legacy = await client.post(
        "/streams/v1/executions/takeover",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(uuid4()),
            "expected_generation": 1,
            "expected_holder_id": body["holder_id"],
            "supervisor_id": body["supervisor_id"],
            "supervisor_capability": body["supervisor_capability"],
            "new_holder_id": str(uuid4()),
            "new_holder_capability": secrets.token_hex(32),
            "new_holder_event_public_key": secrets.token_hex(32),
            "death_witness": {
                "launcher_instance_id": body["launcher_instance_id"],
                "child_process_id": 4321,
                "exit_code": 77,
                "observed_at": "2026-11-04T00:00:00+00:00",
                "kind": "WAITPID",
            },
        },
    )
    assert legacy.status_code == 409
    assert legacy.json()["detail"]["code"] == "TAKEOVER_NOT_AUTHORIZED"


async def test_sqlite_enrollment_refuses_before_mutation(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if db.dialect.name == "postgresql":
        pytest.skip("SQLite refusal is covered by the SQLite profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    body = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    response = await client.post("/streams/v1/deployments/enroll", json=body)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNSUPPORTED_BACKEND"
    async with db.session_context() as check:
        assert (
            await check.scalar(
                sa.select(sa.func.count()).select_from(db.NativeStreamLaunchIdentity)
            )
            == 0
        )
        assert (
            await check.scalar(
                sa.select(sa.func.count()).select_from(db.StreamExecutionAuthority)
            )
            == 0
        )


async def test_flow_run_cannot_override_deployment_declaration(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    declaration = _declaration().model_dump(mode="json")
    deployment.version = "deployment-v1"
    deployment.job_variables = {"prefect_native_streaming": declaration}
    await session.commit()
    response = await client.post(
        f"/deployments/{deployment.id}/create_flow_run",
        json={"job_variables": {"prefect_native_streaming": declaration}},
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "IDENTITY_CONFLICT"
    async with db.session_context() as check:
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.FlowRun)
                .where(db.FlowRun.deployment_id == deployment.id)
            )
            == 0
        )


async def test_bounded_profile_caps_active_native_runs_per_work_pool(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if db.dialect.name != "postgresql":
        pytest.skip("PostgreSQL work-pool capacity is covered by its PG profile")
    deployment.version = "deployment-v1"
    deployment.job_variables = {
        "prefect_native_streaming": _declaration().model_dump(mode="json")
    }
    assert deployment.work_queue_id is not None
    work_queue = await session.get(db.WorkQueue, deployment.work_queue_id)
    assert work_queue is not None and work_queue.work_pool_id is not None
    work_pool_id = work_queue.work_pool_id
    await session.commit()
    flow_run_ids: list[UUID] = []
    for _ in range(native_deployments.MAX_ACTIVE_NATIVE_RUNS_PER_WORK_POOL):
        response = await client.post(
            f"/deployments/{deployment.id}/create_flow_run", json={}
        )
        assert response.status_code == 201, response.text
        flow_run_ids.append(UUID(response.json()["id"]))
    refused = await client.post(
        f"/deployments/{deployment.id}/create_flow_run", json={}
    )
    assert refused.status_code == 409
    assert refused.json()["detail"] == {
        "code": "CAPACITY_EXCEEDED",
        "message": (
            "bounded-default-v1 permits at most 128 active native FlowRuns "
            "per work pool"
        ),
        "retryable": False,
    }
    async with db.session_context(begin_transaction=True) as check:
        active_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.NativeStreamDeploymentSnapshot)
            .where(
                db.NativeStreamDeploymentSnapshot.state == "ACTIVE",
                db.NativeStreamDeploymentSnapshot.work_pool_id == work_pool_id,
            )
        )
        run_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.FlowRun)
            .where(db.FlowRun.deployment_id == deployment.id)
        )
        first = await check.get(db.FlowRun, flow_run_ids[0])
        assert first is not None
        replay = await native_deployments.bind_flow_run_snapshot(check, db, first)
        assert replay is not None and replay.flow_run_id == first.id
    assert active_count == native_deployments.MAX_ACTIVE_NATIVE_RUNS_PER_WORK_POOL
    assert run_count == native_deployments.MAX_ACTIVE_NATIVE_RUNS_PER_WORK_POOL

    released = await client.post(
        f"/flow_runs/{flow_run_ids[0]}/set_state",
        json={
            "state": {
                "type": "FAILED",
                "name": "Failed",
                "state_details": {"retriable": False},
            }
        },
    )
    assert released.status_code == 201
    admitted = await client.post(
        f"/deployments/{deployment.id}/create_flow_run", json={}
    )
    assert admitted.status_code == 201
    async with db.session_context() as check:
        active_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.NativeStreamDeploymentSnapshot)
            .where(
                db.NativeStreamDeploymentSnapshot.state == "ACTIVE",
                db.NativeStreamDeploymentSnapshot.work_pool_id == work_pool_id,
            )
        )
    assert active_count == native_deployments.MAX_ACTIVE_NATIVE_RUNS_PER_WORK_POOL


async def test_zero_retry_declaration_rolls_back_flow_run(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    declaration = _declaration().model_dump(mode="json")
    declaration["flow_retries"] = 0
    deployment.version = "deployment-v1"
    deployment.job_variables = {"prefect_native_streaming": declaration}
    await session.commit()
    response = await client.post(
        f"/deployments/{deployment.id}/create_flow_run", json={}
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "PROTOCOL_ERROR"
    async with db.session_context() as check:
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.FlowRun)
                .where(db.FlowRun.deployment_id == deployment.id)
            )
            == 0
        )


@pytest.mark.parametrize("unsupported", ["command", "pull_steps"])
async def test_native_local_profile_rejects_custom_launch_configuration(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    unsupported: str,
):
    declaration = _declaration().model_dump(mode="json")
    deployment.version = "deployment-v1"
    deployment.job_variables = {"prefect_native_streaming": declaration}
    if unsupported == "command":
        deployment.job_variables["command"] = "python custom.py"
    else:
        deployment.pull_steps = [{"example.pull": {"repository": "remote"}}]
    await session.commit()
    response = await client.post(
        f"/deployments/{deployment.id}/create_flow_run", json={}
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNSUPPORTED_PROFILE"
    async with db.session_context() as check:
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.FlowRun)
                .where(db.FlowRun.deployment_id == deployment.id)
            )
            == 0
        )


async def test_verified_replacement_requires_exact_process_identity(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL replacement is covered by the PostgreSQL profile")
    (
        flow_run,
        worker,
        work_pool_id,
        body,
        old_headers,
        registration,
    ) = await _active_launch(client, session, db, deployment)
    heartbeat = await client.post(
        "/streams/v1/deployments/launches/heartbeat",
        json={
            "flow_run_id": str(flow_run.id),
            "generation": 1,
            "enrollment_operation_id": body["operation_id"],
        },
        headers=old_headers,
    )
    assert heartbeat.status_code == 200
    fresh = await client.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
        },
    )
    assert fresh.status_code == 200
    assert fresh.json() == []
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    candidates = await client.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
        },
    )
    assert candidates.status_code == 200
    assert len(candidates.json()) == 1
    candidate = candidates.json()[0]
    assert candidate["child_process_id"] == registration["child_process_id"]
    assert candidate["identity_kind"] == "CHILD"

    operation_id = uuid4()
    challenge = secrets.token_hex(32)
    authorization = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": body["operation_id"],
        "worker_id": str(worker.id),
        "work_pool_id": str(work_pool_id),
        "host_id": registration["host_id"],
        "identity_kind": "CHILD",
        "challenge_capability": challenge,
    }
    authorized = await client.post(
        "/streams/v1/deployments/replacements/authorize", json=authorization
    )
    authorized_replay = await client.post(
        "/streams/v1/deployments/replacements/authorize", json=authorization
    )
    assert authorized.status_code == authorized_replay.status_code == 200
    assert authorized.json() == authorized_replay.json()
    assert challenge not in authorized.text

    new_holder = uuid4()
    new_capability = secrets.token_hex(32)
    new_supervisor = uuid4()
    new_supervisor_capability = secrets.token_hex(32)
    new_launcher = uuid4()
    confirmation = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(operation_id),
        "challenge_capability": challenge,
        "launcher_instance_id": str(new_launcher),
        "launcher_host_id": registration["host_id"],
        "launcher_boot_id": registration["boot_id"],
        "launcher_process_id": 2469,
        "launcher_start_ticks": 13580,
        "new_holder_id": str(new_holder),
        "new_holder_capability": new_capability,
        "new_holder_event_public_key": secrets.token_hex(32),
        "new_supervisor_id": str(new_supervisor),
        "new_supervisor_capability": new_supervisor_capability,
        "death_witness": {
            "kind": "IDENTITY_GONE",
            "observed_at": "2026-11-04T14:30:00+00:00",
            "host_id": registration["host_id"],
            "boot_id": registration["boot_id"],
            "child_process_id": registration["child_process_id"],
            "process_start_ticks": registration["process_start_ticks"],
        },
    }
    confirmed = await client.post(
        "/streams/v1/deployments/replacements/confirm-death", json=confirmation
    )
    confirmed_replay = await client.post(
        "/streams/v1/deployments/replacements/confirm-death", json=confirmation
    )
    assert confirmed.status_code == confirmed_replay.status_code == 200
    assert confirmed.json() == confirmed_replay.json()
    async with db.session_context() as check:
        authority = await check.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        attempts = (
            await check.scalars(
                sa.select(db.StreamExecutionAttempt)
                .where(db.StreamExecutionAttempt.flow_run_id == flow_run.id)
                .order_by(db.StreamExecutionAttempt.generation)
            )
        ).all()
        launches = (
            await check.scalars(
                sa.select(db.NativeStreamLaunchIdentity)
                .where(db.NativeStreamLaunchIdentity.flow_run_id == flow_run.id)
                .order_by(db.NativeStreamLaunchIdentity.generation)
            )
        ).all()
    assert authority is not None
    assert authority.generation == 2
    assert [attempt.generation for attempt in attempts] == [1, 2]
    assert [launch.generation for launch in launches] == [1, 2]
    assert confirmed.json()["generation"] == 2
    encoded = confirmed.text
    for secret in (challenge, new_capability, new_supervisor_capability):
        assert secret not in encoded

    old_heartbeat = await client.post(
        "/streams/v1/deployments/launches/heartbeat",
        json={
            "flow_run_id": str(flow_run.id),
            "generation": 1,
            "enrollment_operation_id": body["operation_id"],
        },
        headers=old_headers,
    )
    assert old_heartbeat.status_code == 409
    assert old_heartbeat.json()["detail"]["code"] == "STALE_EXECUTION"
    new_headers = {
        "X-Prefect-Stream-Execution-Generation": "2",
        "X-Prefect-Stream-Execution-Holder": str(new_holder),
        "X-Prefect-Stream-Execution-Capability": new_capability,
    }
    registered = await client.post(
        "/streams/v1/deployments/launches/register-process",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(uuid4()),
            "enrollment_operation_id": str(operation_id),
            "generation": 2,
            "launcher_instance_id": str(new_launcher),
            "host_id": registration["host_id"],
            "boot_id": registration["boot_id"],
            "child_process_id": 5432,
            "process_start_ticks": 987655,
            "launch_nonce_digest": "c" * 64,
        },
        headers=new_headers,
    )
    assert registered.status_code == 200
    assert registered.json()["generation"] == 2


async def test_unclaimed_native_failure_releases_snapshot(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    flow_run, _, _ = await _native_flow_run(session, db, deployment)
    failed = await client.post(
        f"/flow_runs/{flow_run.id}/set_state",
        json={"state": {"type": "FAILED", "name": "Failed"}},
    )
    assert failed.status_code == 201
    async with db.session_context() as check:
        snapshot = await check.scalar(
            sa.select(db.NativeStreamDeploymentSnapshot).where(
                db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run.id
            )
        )
        authority_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.StreamExecutionAuthority)
            .where(db.StreamExecutionAuthority.flow_run_id == flow_run.id)
        )
    assert snapshot is not None and snapshot.state == "RELEASED"
    assert authority_count == 0


async def test_generation_two_unstarted_launch_fails_and_releases_old_graph(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL replacement cleanup is covered by its PG profile")
    flow_run, worker, work_pool_id, enrollment, _, registration = await _active_launch(
        client, session, db, deployment
    )
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    replacement_id = uuid4()
    challenge = secrets.token_hex(32)
    authorized = await client.post(
        "/streams/v1/deployments/replacements/authorize",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(replacement_id),
            "expected_generation": 1,
            "enrollment_operation_id": enrollment["operation_id"],
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
            "identity_kind": "CHILD",
            "challenge_capability": challenge,
        },
    )
    assert authorized.status_code == 200
    new_holder = uuid4()
    new_capability = secrets.token_hex(32)
    launcher_instance = uuid4()
    confirmed = await client.post(
        "/streams/v1/deployments/replacements/confirm-death",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(replacement_id),
            "challenge_capability": challenge,
            "launcher_instance_id": str(launcher_instance),
            "launcher_host_id": registration["host_id"],
            "launcher_boot_id": registration["boot_id"],
            "launcher_process_id": 8642,
            "launcher_start_ticks": 97531,
            "new_holder_id": str(new_holder),
            "new_holder_capability": new_capability,
            "new_holder_event_public_key": secrets.token_hex(32),
            "new_supervisor_id": str(uuid4()),
            "new_supervisor_capability": secrets.token_hex(32),
            "death_witness": {
                "kind": "IDENTITY_GONE",
                "observed_at": "2026-11-04T16:50:00Z",
                "host_id": registration["host_id"],
                "boot_id": registration["boot_id"],
                "child_process_id": registration["child_process_id"],
                "process_start_ticks": registration["process_start_ticks"],
            },
        },
    )
    assert confirmed.status_code == 200
    headers = {
        "X-Prefect-Stream-Execution-Generation": "2",
        "X-Prefect-Stream-Execution-Holder": str(new_holder),
        "X-Prefect-Stream-Execution-Capability": new_capability,
    }
    cleanup = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": str(replacement_id),
        "error": "replacement launcher could not create a child",
    }
    retired = await client.post(
        "/streams/v1/deployments/launches/retire-unstarted",
        json=cleanup,
        headers=headers,
    )
    replay = await client.post(
        "/streams/v1/deployments/launches/retire-unstarted",
        json=cleanup,
        headers=headers,
    )
    assert retired.status_code == replay.status_code == 200, retired.text
    assert retired.json() == replay.json()
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, flow_run.id)
        authority = await check.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        snapshot = await check.scalar(
            sa.select(db.NativeStreamDeploymentSnapshot).where(
                db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run.id
            )
        )
        tasks = (
            await check.scalars(
                sa.select(db.TaskRun).where(db.TaskRun.flow_run_id == flow_run.id)
            )
        ).all()
    assert flow is not None and flow.state_type == "FAILED"
    assert authority is not None and authority.state == "RELEASED"
    assert snapshot is not None and snapshot.state == "RELEASED"
    assert tasks and all(task.state_type == "FAILED" for task in tasks)


async def test_verified_child_before_first_manifest_fails_empty_started_flow(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL empty-start cleanup is covered by its PG profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    enrollment = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    assert (
        await client.post("/streams/v1/deployments/enroll", json=enrollment)
    ).status_code == 200
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": enrollment["holder_id"],
        "X-Prefect-Stream-Execution-Capability": enrollment["holder_capability"],
    }
    registration = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": enrollment["operation_id"],
        "generation": 1,
        "launcher_instance_id": enrollment["launcher_instance_id"],
        "host_id": enrollment["launcher_host_id"],
        "boot_id": enrollment["launcher_boot_id"],
        "child_process_id": 65432,
        "process_start_ticks": 23456,
        "launch_nonce_digest": "7" * 64,
    }
    assert (
        await client.post(
            "/streams/v1/deployments/launches/register-process",
            json=registration,
            headers=headers,
        )
    ).status_code == 200
    assert (
        await client.post(
            f"/flow_runs/{flow_run.id}/set_state",
            json={"state": {"type": "RUNNING", "name": "Running"}},
            headers=headers,
        )
    ).status_code == 201
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    operation_id = uuid4()
    challenge = secrets.token_hex(32)
    assert (
        await client.post(
            "/streams/v1/deployments/replacements/authorize",
            json={
                "flow_run_id": str(flow_run.id),
                "operation_id": str(operation_id),
                "expected_generation": 1,
                "enrollment_operation_id": enrollment["operation_id"],
                "worker_id": str(worker.id),
                "work_pool_id": str(work_pool_id),
                "host_id": registration["host_id"],
                "identity_kind": "CHILD",
                "challenge_capability": challenge,
            },
        )
    ).status_code == 200
    retired = await client.post(
        "/streams/v1/deployments/replacements/retire-verified",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(operation_id),
            "expected_generation": 1,
            "enrollment_operation_id": enrollment["operation_id"],
            "challenge_capability": challenge,
            "death_witness": {
                "kind": "IDENTITY_GONE",
                "observed_at": "2026-11-04T16:55:00Z",
                "host_id": registration["host_id"],
                "boot_id": registration["boot_id"],
                "child_process_id": registration["child_process_id"],
                "process_start_ticks": registration["process_start_ticks"],
            },
            "error": "child exited before the first manifest",
        },
    )
    assert retired.status_code == 200, retired.text
    assert retired.json()["kind"] == "VERIFIED_RETIRED"
    async with db.session_context() as check:
        flow = await check.get(db.FlowRun, flow_run.id)
        snapshot = await check.scalar(
            sa.select(db.NativeStreamDeploymentSnapshot).where(
                db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run.id
            )
        )
        task_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.TaskRun)
            .where(db.TaskRun.flow_run_id == flow_run.id)
        )
        stream_count = await check.scalar(
            sa.select(sa.func.count())
            .select_from(db.StreamPartition)
            .where(db.StreamPartition.flow_run_id == flow_run.id)
        )
    assert flow is not None and flow.state_type == "FAILED"
    assert snapshot is not None and snapshot.state == "RELEASED"
    assert task_count == stream_count == 0


async def test_terminal_child_cleanup_preserves_final_flow_state(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL terminal cleanup is covered by its PG profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    enrollment = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    assert (
        await client.post("/streams/v1/deployments/enroll", json=enrollment)
    ).status_code == 200
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": enrollment["holder_id"],
        "X-Prefect-Stream-Execution-Capability": enrollment["holder_capability"],
    }
    registration = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": enrollment["operation_id"],
        "generation": 1,
        "launcher_instance_id": enrollment["launcher_instance_id"],
        "host_id": enrollment["launcher_host_id"],
        "boot_id": enrollment["launcher_boot_id"],
        "child_process_id": 54321,
        "process_start_ticks": 12345,
        "launch_nonce_digest": "8" * 64,
    }
    assert (
        await client.post(
            "/streams/v1/deployments/launches/register-process",
            json=registration,
            headers=headers,
        )
    ).status_code == 200
    assert (
        await client.post(
            f"/flow_runs/{flow_run.id}/set_state",
            json={"state": {"type": "RUNNING", "name": "Running"}},
            headers=headers,
        )
    ).status_code == 201
    assert (
        await client.post(
            f"/flow_runs/{flow_run.id}/set_state",
            json={
                "state": {
                    "type": "FAILED",
                    "name": "Failed",
                    "state_details": {"retriable": False},
                }
            },
            headers=headers,
        )
    ).status_code == 201
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    candidates = await client.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
        },
    )
    assert candidates.status_code == 200
    assert len(candidates.json()) == 1
    assert candidates.json()[0]["identity_kind"] == "CHILD"
    operation_id = uuid4()
    challenge = secrets.token_hex(32)
    assert (
        await client.post(
            "/streams/v1/deployments/replacements/authorize",
            json={
                "flow_run_id": str(flow_run.id),
                "operation_id": str(operation_id),
                "expected_generation": 1,
                "enrollment_operation_id": enrollment["operation_id"],
                "worker_id": str(worker.id),
                "work_pool_id": str(work_pool_id),
                "host_id": registration["host_id"],
                "identity_kind": "CHILD",
                "challenge_capability": challenge,
            },
        )
    ).status_code == 200
    retired = await client.post(
        "/streams/v1/deployments/replacements/retire-verified",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(operation_id),
            "expected_generation": 1,
            "enrollment_operation_id": enrollment["operation_id"],
            "challenge_capability": challenge,
            "death_witness": {
                "kind": "IDENTITY_GONE",
                "observed_at": "2026-11-04T17:00:00Z",
                "host_id": registration["host_id"],
                "boot_id": registration["boot_id"],
                "child_process_id": registration["child_process_id"],
                "process_start_ticks": registration["process_start_ticks"],
            },
            "error": "supervisor exited after terminal state",
        },
    )
    assert retired.status_code == 200, retired.text
    assert retired.json()["kind"] == "VERIFIED_RETIRED"
    final = await client.get(f"/flow_runs/{flow_run.id}")
    assert final.json()["state_type"] == "FAILED"
    final_plan = await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")
    assert final_plan.json()["state"] == "RELEASED"


async def test_release_waits_for_bound_task_terminal_metadata(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL release metadata gate is covered by its PG profile")
    flow_run, _, _, _, headers, _ = await _active_launch(
        client, session, db, deployment
    )
    assert (
        await client.post(
            f"/flow_runs/{flow_run.id}/set_state",
            json={
                "state": {
                    "type": "FAILED",
                    "name": "Failed",
                    "state_details": {"retriable": False},
                }
            },
            headers=headers,
        )
    ).status_code == 201
    refused = await client.post(
        "/streams/v1/release",
        json={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    assert refused.status_code == 409
    assert refused.json()["detail"] == {
        "code": "METADATA_NOT_READY",
        "message": "Bound TaskRun terminal metadata has not been recorded",
        "retryable": True,
    }
    async with db.session_context() as check:
        task_id = await check.scalar(
            sa.select(db.StreamSubmission.task_run_id).where(
                db.StreamSubmission.flow_run_id == flow_run.id
            )
        )
        snapshot = await check.scalar(
            sa.select(db.NativeStreamDeploymentSnapshot).where(
                db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run.id
            )
        )
    assert task_id is not None
    assert snapshot is not None and snapshot.state == "ACTIVE"


async def test_drain_replays_accepted_append_and_cancel_blocks_writes(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL controls are covered by the PostgreSQL profile")
    flow_run, _, _, enrollment, headers, registration = await _active_launch(
        client, session, db, deployment
    )
    async with db.session_context() as check:
        manifest = await check.scalar(
            sa.select(db.StreamSubmission).where(
                db.StreamSubmission.flow_run_id == flow_run.id
            )
        )
        assert manifest is not None
    task_running = await client.post(
        f"/task_runs/{manifest.task_run_id}/set_state",
        json={"state": {"type": "RUNNING", "name": "Running"}},
        headers=headers,
    )
    assert task_running.status_code == 201
    owner_id = uuid4()
    opened = await client.post(
        "/streams/v1/sources/open",
        json={
            "stream_id": str(manifest.stream_id),
            "task_run_id": str(manifest.task_run_id),
            "owner_id": str(owner_id),
            "attempt": 1,
            "protocol_version": 1,
            "source_protocol": "source-v1",
        },
        headers=headers,
    )
    assert opened.status_code == 200
    binding = opened.json()["binding"]
    append = {"binding": binding, "position": 1, "value": {"document": 1}}
    first = await client.post(
        "/streams/v1/sources/append", json=append, headers=headers
    )
    assert first.status_code == 200
    drain = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "expected_generation": 1,
        "reason": "bounded drain",
    }
    drained = await client.post(
        "/streams/v1/deployments/control/drain", json=drain, headers=headers
    )
    drained_replay = await client.post(
        "/streams/v1/deployments/control/drain", json=drain, headers=headers
    )
    assert drained.status_code == drained_replay.status_code == 200
    assert drained.json() == drained_replay.json()
    accepted_replay = await client.post(
        "/streams/v1/sources/append", json=append, headers=headers
    )
    assert accepted_replay.status_code == 200
    refused = await client.post(
        "/streams/v1/sources/append",
        json={"binding": binding, "position": 2, "value": {"document": 2}},
        headers=headers,
    )
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "SOURCE_DRAIN_REQUESTED"
    cancel = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "expected_generation": 1,
        "reason": "operator cancellation",
    }
    stale_admin = await client.post(
        "/streams/v1/deployments/control/drain",
        json={**cancel, "operation_id": str(uuid4()), "expected_generation": 2},
    )
    assert stale_admin.status_code == 409
    assert stale_admin.json()["detail"]["code"] == "STALE_EXECUTION"
    cancelled = await client.post("/streams/v1/deployments/control/cancel", json=cancel)
    assert cancelled.status_code == 200
    assert cancelled.json()["cancel_requested"] is True
    cancelling_flow = await client.get(f"/flow_runs/{flow_run.id}")
    assert cancelling_flow.status_code == 200
    assert cancelling_flow.json()["state_type"] == "CANCELLING"
    cancelled_replay = await client.post(
        "/streams/v1/sources/append", json=append, headers=headers
    )
    assert cancelled_replay.status_code == 409
    assert cancelled_replay.json()["detail"]["code"] == "CANCEL_REQUESTED"
    finalized_request = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "expected_generation": 1,
        "enrollment_operation_id": enrollment["operation_id"],
        "death_witness": {
            "kind": "WAITPID",
            "observed_at": "2026-11-04T15:00:00Z",
            "host_id": registration["host_id"],
            "boot_id": registration["boot_id"],
            "child_process_id": registration["child_process_id"],
            "process_start_ticks": registration["process_start_ticks"],
            "exit_code": -15,
        },
    }
    finalized = await client.post(
        "/streams/v1/deployments/control/finalize-cancel",
        json=finalized_request,
        headers=headers,
    )
    finalized_replay = await client.post(
        "/streams/v1/deployments/control/finalize-cancel",
        json=finalized_request,
        headers=headers,
    )
    assert finalized.status_code == finalized_replay.status_code == 200, (
        finalized.text,
        finalized_replay.text,
    )
    assert finalized.json() == finalized_replay.json()
    assert finalized.json()["kind"] == "CANCEL_FINALIZED"
    final_flow = await client.get(f"/flow_runs/{flow_run.id}")
    assert final_flow.status_code == 200
    assert final_flow.json()["state_type"] == "CANCELLED"
    final_plan = await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")
    assert final_plan.status_code == 200
    assert final_plan.json()["state"] == "RELEASED"
    assert final_plan.json()["current_launch"]["state"] == "RELEASED"
    cancel_replay_after_release = await client.post(
        "/streams/v1/deployments/control/cancel", json=cancel
    )
    assert cancel_replay_after_release.status_code == 200
    assert cancel_replay_after_release.json() == cancelled.json()
    async with db.session_context() as check:
        task = await check.get(db.TaskRun, manifest.task_run_id)
        stream = await check.get(db.StreamPartition, manifest.stream_id)
    assert task is not None and task.state_type == "CANCELLED"
    assert stream is not None and stream.state == "FAILED" and stream.released


async def test_orphan_cancellation_uses_authorized_death_witness(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL orphan cancellation is covered by its PG profile")
    flow_run, worker, work_pool_id, enrollment, _, registration = await _active_launch(
        client, session, db, deployment
    )
    cancel = await client.post(
        "/streams/v1/deployments/control/cancel",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(uuid4()),
            "expected_generation": 1,
            "reason": "orphan cancellation",
        },
    )
    assert cancel.status_code == 200
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    candidates = await client.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
        },
    )
    assert candidates.status_code == 200
    assert len(candidates.json()) == 1
    operation_id = uuid4()
    challenge = secrets.token_hex(32)
    authorized = await client.post(
        "/streams/v1/deployments/replacements/authorize",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(operation_id),
            "expected_generation": 1,
            "enrollment_operation_id": enrollment["operation_id"],
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
            "identity_kind": "CHILD",
            "challenge_capability": challenge,
        },
    )
    assert authorized.status_code == 200
    finalization = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": enrollment["operation_id"],
        "challenge_capability": challenge,
        "death_witness": {
            "kind": "IDENTITY_GONE",
            "observed_at": "2026-11-04T15:30:00Z",
            "host_id": registration["host_id"],
            "boot_id": registration["boot_id"],
            "child_process_id": registration["child_process_id"],
            "process_start_ticks": registration["process_start_ticks"],
        },
    }
    finalized = await client.post(
        "/streams/v1/deployments/control/finalize-cancel", json=finalization
    )
    finalized_replay = await client.post(
        "/streams/v1/deployments/control/finalize-cancel", json=finalization
    )
    assert finalized.status_code == finalized_replay.status_code == 200, (
        finalized.text,
        finalized_replay.text,
    )
    assert finalized.json() == finalized_replay.json()
    final_flow = await client.get(f"/flow_runs/{flow_run.id}")
    assert final_flow.json()["state_type"] == "CANCELLED"
    async with db.session_context() as check:
        replacement = await check.scalar(
            sa.select(db.NativeStreamReplacementOperation).where(
                db.NativeStreamReplacementOperation.flow_run_id == flow_run.id,
                db.NativeStreamReplacementOperation.operation_id == operation_id,
            )
        )
    assert replacement is not None
    assert replacement.death_witness is not None
    assert replacement.death_witness["kind"] == "IDENTITY_GONE"
    assert (
        replacement.death_witness["child_process_id"]
        == registration["child_process_id"]
    )


async def _launch_heartbeat_rows(
    db: PrefectDBInterface, flow_run_id: UUID
) -> dict[str, dict[str, object]]:
    snapshot: dict[str, dict[str, object]] = {}
    async with db.session_context() as session:
        for name, table in (
            ("flow", db.FlowRun.__table__),
            ("authority", db.StreamExecutionAuthority.__table__),
            ("launch", db.NativeStreamLaunchIdentity.__table__),
        ):
            identity = table.c.id if name == "flow" else table.c.flow_run_id
            row = (
                (await session.execute(sa.select(table).where(identity == flow_run_id)))
                .mappings()
                .one()
            )
            snapshot[name] = {str(key): deepcopy(value) for key, value in row.items()}
    return snapshot


async def _observe_heartbeat_blocker(
    db: PrefectDBInterface, waiter_pid: int, holder_pid: int, expected_table: str
) -> None:
    async with db.session_context() as observer:
        async with asyncio.timeout(10):
            while True:
                await observer.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
                row = (
                    await observer.execute(
                        sa.text(
                            "SELECT pg_blocking_pids(pid), query FROM pg_stat_activity "
                            "WHERE pid = :waiter AND datname = current_database()"
                        ),
                        {"waiter": waiter_pid},
                    )
                ).first()
                if row is not None and holder_pid in row[0]:
                    # pg_stat_activity truncates long ORM SELECTs before FROM;
                    # the selected relation is already explicit in the prefix.
                    assert row[1].startswith(f"SELECT {expected_table}."), row[1]
                    return
                await asyncio.sleep(0.01)


@pytest.mark.parametrize(
    "held_lock", ["flow-share", "flow-update", "authority-update", "launch-update"]
)
async def test_launch_heartbeat_lock_order_postgres(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    held_lock: str,
):
    if not await _postgres_17(db):
        pytest.skip("The heartbeat row-lock regression requires PostgreSQL 17")
    flow_run, _, _, enrollment, _, _ = await _active_launch(
        client, session, db, deployment
    )
    request = NativeStreamLaunchHeartbeatRequest(
        flow_run_id=flow_run.id,
        generation=1,
        enrollment_operation_id=UUID(enrollment["operation_id"]),
    )
    credentials = StreamExecutionCredentials(
        generation=1,
        holder_id=UUID(enrollment["holder_id"]),
        capability=enrollment["holder_capability"],
    )
    before = await _launch_heartbeat_rows(db, flow_run.id)
    started: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def heartbeat() -> NativeStreamLaunchHeartbeat:
        async with db.session_context(begin_transaction=True) as renewal:
            pid = await renewal.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            started.set_result(pid)
            return await native_deployments.renew_launch(
                renewal, db, request, credentials
            )

    async with db.session_context(begin_transaction=True) as holder:
        holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
        assert isinstance(holder_pid, int)
        await holder.execute(
            sa.select(db.FlowRun.id)
            .where(db.FlowRun.id == flow_run.id)
            .with_for_update(read=held_lock != "flow-update")
        )
        if held_lock in {"authority-update", "launch-update"}:
            await holder.execute(
                sa.select(db.StreamExecutionAuthority.id)
                .where(db.StreamExecutionAuthority.flow_run_id == flow_run.id)
                .with_for_update(read=held_lock == "launch-update")
            )
        if held_lock == "launch-update":
            await holder.execute(
                sa.select(db.NativeStreamLaunchIdentity.id)
                .where(db.NativeStreamLaunchIdentity.flow_run_id == flow_run.id)
                .with_for_update()
            )
        task = asyncio.create_task(heartbeat())
        try:
            waiter_pid = await asyncio.wait_for(asyncio.shield(started), timeout=5)
            if held_lock == "flow-share":
                # The DATA reader remains open while the real renewal commits.
                response = await asyncio.wait_for(asyncio.shield(task), timeout=5)
                assert holder.in_transaction()
                assert response.flow_run_id == flow_run.id
            else:
                table = {
                    "flow-update": "flow_run",
                    "authority-update": "stream_execution_authority",
                    "launch-update": "native_stream_launch_identity",
                }[held_lock]
                await _observe_heartbeat_blocker(db, waiter_pid, holder_pid, table)
                assert not task.done()
                assert await _launch_heartbeat_rows(db, flow_run.id) == before
        finally:
            await holder.commit()
            await asyncio.wait_for(task, timeout=5)
    after = await _launch_heartbeat_rows(db, flow_run.id)
    assert after["flow"] == before["flow"]
    assert after["authority"] == before["authority"]
    assert {
        key: value
        for key, value in after["launch"].items()
        if key not in {"last_renewed_at", "updated"}
    } == {
        key: value
        for key, value in before["launch"].items()
        if key not in {"last_renewed_at", "updated"}
    }
    assert after["launch"]["last_renewed_at"] != before["launch"]["last_renewed_at"]


@pytest.mark.parametrize(
    "fault,code",
    [
        ("missing-binding", "EXECUTION_BINDING_REQUIRED"),
        ("stale-generation", "STALE_EXECUTION"),
        ("foreign-flow", "STALE_EXECUTION"),
        ("wrong-capability", "STALE_EXECUTION"),
        ("request-generation", "STALE_EXECUTION"),
        ("unknown-enrollment", "NOT_FOUND"),
        ("released-authority", "RECOVERY_HORIZON_EXPIRED"),
        ("released-launch", "RECOVERY_NOT_READY"),
    ],
)
async def test_launch_heartbeat_refusals_preserve_rows_postgres(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    fault: str,
    code: str,
):
    if not await _postgres_17(db):
        pytest.skip("Native launch heartbeat authority requires PostgreSQL 17")
    flow_run, _, _, enrollment, _, _ = await _active_launch(
        client, session, db, deployment
    )
    request = NativeStreamLaunchHeartbeatRequest(
        flow_run_id=flow_run.id,
        generation=1,
        enrollment_operation_id=UUID(enrollment["operation_id"]),
    )
    credentials: StreamExecutionCredentials | None = StreamExecutionCredentials(
        generation=1,
        holder_id=UUID(enrollment["holder_id"]),
        capability=enrollment["holder_capability"],
    )
    if fault == "missing-binding":
        credentials = None
    elif fault == "stale-generation":
        credentials = credentials.model_copy(update={"generation": 2})
    elif fault == "foreign-flow":
        _, _, _, foreign, _, _ = await _active_launch(client, session, db, deployment)
        credentials = StreamExecutionCredentials(
            generation=1,
            holder_id=UUID(foreign["holder_id"]),
            capability=foreign["holder_capability"],
        )
    elif fault == "wrong-capability":
        credentials = credentials.model_copy(
            update={"capability": secrets.token_hex(32)}
        )
    elif fault == "request-generation":
        request = request.model_copy(update={"generation": 2})
    elif fault == "unknown-enrollment":
        request = request.model_copy(update={"enrollment_operation_id": uuid4()})
    elif fault in {"released-authority", "released-launch"}:
        # Constraint-valid retained lifecycle fixtures, rather than a changed
        # clock or a mock result from the execution credential gate.
        target = (
            db.StreamExecutionAuthority
            if fault == "released-authority"
            else db.NativeStreamLaunchIdentity
        )
        async with db.session_context(begin_transaction=True) as setup:
            await setup.execute(
                sa.update(target)
                .where(target.flow_run_id == flow_run.id)
                .values(state="RELEASED")
            )
    before = await _launch_heartbeat_rows(db, flow_run.id)
    with pytest.raises(NativeStreamError) as error:
        async with db.session_context(begin_transaction=True) as renewal:
            await native_deployments.renew_launch(renewal, db, request, credentials)
    assert error.value.code == code
    assert await _launch_heartbeat_rows(db, flow_run.id) == before


async def test_registered_startup_crash_is_verified_and_retired(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    monkeypatch: pytest.MonkeyPatch,
):
    if not await _postgres_17(db):
        pytest.skip("PostgreSQL verified retirement is covered by its PG profile")
    flow_run, worker, work_pool_id = await _native_flow_run(session, db, deployment)
    plan = (await client.get(f"/streams/v1/deployments/{flow_run.id}/plan")).json()
    enrollment = _enrollment_body(
        flow_run.id, worker.id, work_pool_id, plan["declaration_digest"]
    )
    enrolled = await client.post("/streams/v1/deployments/enroll", json=enrollment)
    assert enrolled.status_code == 200
    headers = {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": enrollment["holder_id"],
        "X-Prefect-Stream-Execution-Capability": enrollment["holder_capability"],
    }
    registration = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "enrollment_operation_id": enrollment["operation_id"],
        "generation": 1,
        "launcher_instance_id": enrollment["launcher_instance_id"],
        "host_id": str(uuid4()),
        "boot_id": str(uuid4()),
        "child_process_id": 6789,
        "process_start_ticks": 456789,
        "launch_nonce_digest": "d" * 64,
    }
    registered = await client.post(
        "/streams/v1/deployments/launches/register-process",
        json=registration,
        headers=headers,
    )
    assert registered.status_code == 200
    monkeypatch.setattr(native_deployments, "LAUNCH_RENEWAL_STALE_AFTER", timedelta(0))
    candidates = await client.get(
        "/streams/v1/deployments/replacements/candidates",
        params={
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
        },
    )
    assert len(candidates.json()) == 1
    operation_id = uuid4()
    challenge = secrets.token_hex(32)
    authorized = await client.post(
        "/streams/v1/deployments/replacements/authorize",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(operation_id),
            "expected_generation": 1,
            "enrollment_operation_id": enrollment["operation_id"],
            "worker_id": str(worker.id),
            "work_pool_id": str(work_pool_id),
            "host_id": registration["host_id"],
            "identity_kind": "CHILD",
            "challenge_capability": challenge,
        },
    )
    assert authorized.status_code == 200
    retirement = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(operation_id),
        "expected_generation": 1,
        "enrollment_operation_id": enrollment["operation_id"],
        "challenge_capability": challenge,
        "death_witness": {
            "kind": "IDENTITY_GONE",
            "observed_at": "2026-11-04T16:00:00Z",
            "host_id": registration["host_id"],
            "boot_id": registration["boot_id"],
            "child_process_id": registration["child_process_id"],
            "process_start_ticks": registration["process_start_ticks"],
        },
        "error": "registered child exited before bootstrap",
    }
    retired = await client.post(
        "/streams/v1/deployments/replacements/retire-verified", json=retirement
    )
    retired_replay = await client.post(
        "/streams/v1/deployments/replacements/retire-verified", json=retirement
    )
    assert retired.status_code == retired_replay.status_code == 200, retired.text
    assert retired.json() == retired_replay.json()
    assert retired.json()["kind"] == "VERIFIED_RETIRED"
    final_flow = await client.get(f"/flow_runs/{flow_run.id}")
    assert final_flow.json()["state_type"] == "FAILED"
    async with db.session_context() as check:
        replacement = await check.scalar(
            sa.select(db.NativeStreamReplacementOperation).where(
                db.NativeStreamReplacementOperation.flow_run_id == flow_run.id,
                db.NativeStreamReplacementOperation.operation_id == operation_id,
            )
        )
    assert replacement is not None
    assert replacement.death_witness is not None
    assert replacement.death_witness["kind"] == "IDENTITY_GONE"
    assert (
        replacement.death_witness["child_process_id"]
        == registration["child_process_id"]
    )
