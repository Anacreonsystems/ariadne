"""Server authority for native deployment snapshots and worker launches."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import timedelta
from typing import Any, Literal, cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

import prefect.server.models.flow_runs as flow_run_models
import prefect.server.models.task_runs as task_run_models
import prefect.server.schemas as schemas
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_executions import (
    _apply_takeover_locked,
    _claim_execution,
    require_execution,
)
from prefect.server.orchestration.core_policy import CoreFlowPolicy, CoreTaskPolicy
from prefect.server.schemas.core import FlowRunPolicy
from prefect.server.schemas.native_deployments import (
    NativeStreamCancelFinalizeRequest,
    NativeStreamControlRequest,
    NativeStreamControlResult,
    NativeStreamDeclaration,
    NativeStreamDeploymentPlan,
    NativeStreamFiniteProjection,
    NativeStreamFlowProjection,
    NativeStreamLaunchHeartbeat,
    NativeStreamLaunchHeartbeatRequest,
    NativeStreamLaunchProjection,
    NativeStreamProcessRegistrationRequest,
    NativeStreamRecoveryCandidate,
    NativeStreamReplacementAuthorization,
    NativeStreamReplacementAuthorizeRequest,
    NativeStreamReplacementConfirmRequest,
    NativeStreamTaskProjection,
    NativeStreamUnstartedRetireRequest,
    NativeStreamVerifiedRetireRequest,
    NativeStreamWorkerEnrollment,
    NativeStreamWorkerEnrollRequest,
)
from prefect.server.schemas.statuses import WorkerStatus
from prefect.server.schemas.streams import (
    StreamExecutionClaimRequest,
    StreamExecutionCredentials,
    StreamLimits,
)
from prefect.types import DateTime
from prefect.types._datetime import now

NATIVE_DECLARATION_JOB_VARIABLE = "prefect_native_streaming"
NATIVE_WORK_POOL_MARKER = {
    "schema_version": 1,
    "profile": "bounded-linear-v1",
}
LAUNCH_RENEWAL_STALE_AFTER = timedelta(seconds=120)
MAX_ACTIVE_NATIVE_RUNS_PER_WORK_POOL = 128


def _canonical(value: Any) -> tuple[bytes, str]:
    encoded = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode()
    return encoded, hashlib.sha256(encoded).hexdigest()


def _secret_digest(value: str) -> str:
    return hashlib.sha256(bytes.fromhex(value)).hexdigest()


def _declaration_from_job_variables(
    job_variables: dict[str, Any] | None,
) -> NativeStreamDeclaration | None:
    if not job_variables or NATIVE_DECLARATION_JOB_VARIABLE not in job_variables:
        return None
    try:
        return NativeStreamDeclaration.model_validate(
            job_variables[NATIVE_DECLARATION_JOB_VARIABLE]
        )
    except ValueError as exc:
        raise NativeStreamError(
            "PROTOCOL_ERROR", "Invalid native deployment declaration"
        ) from exc


async def bind_flow_run_snapshot(
    session: AsyncSession,
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
) -> orm_models.NativeStreamDeploymentSnapshot | None:
    """Bind a deployment declaration to a newly created FlowRun."""

    override = _declaration_from_job_variables(flow_run.job_variables)
    if override is not None:
        raise NativeStreamError(
            "IDENTITY_CONFLICT",
            "FlowRun job variables cannot override prefect_native_streaming",
        )
    if flow_run.deployment_id is None:
        return None
    deployment = await session.get(db.Deployment, flow_run.deployment_id)
    if deployment is None:
        raise NativeStreamError("NOT_FOUND", "Native deployment is missing")
    declaration = _declaration_from_job_variables(deployment.job_variables)
    if declaration is None:
        return None
    if deployment.pull_steps:
        raise NativeStreamError(
            "UNSUPPORTED_PROFILE",
            "The first native deployment profile requires local code without pull steps",
        )
    if deployment.job_variables.get("command") is not None:
        raise NativeStreamError(
            "UNSUPPORTED_PROFILE",
            "The first native deployment profile does not accept a custom command",
        )
    if (
        flow_run.flow_version is not None
        and flow_run.flow_version != declaration.flow_version
    ):
        raise NativeStreamError("IDENTITY_CONFLICT", "Flow version drift")
    flow_run.flow_version = declaration.flow_version
    flow_run.empirical_policy = FlowRunPolicy(
        retries=declaration.flow_retries,
        retry_delay=declaration.flow_retry_delay_seconds,
        retry_delay_seconds=declaration.flow_retry_delay_seconds or 0,
    )
    if flow_run.work_queue_id is None:
        raise NativeStreamError(
            "PROTOCOL_ERROR", "Native deployment requires a work queue"
        )
    work_queue = await session.get(db.WorkQueue, flow_run.work_queue_id)
    if work_queue is None or work_queue.work_pool_id is None:
        raise NativeStreamError(
            "PROTOCOL_ERROR", "Native deployment requires a work pool"
        )
    work_pool = await session.scalar(
        sa.select(db.WorkPool)
        .where(db.WorkPool.id == work_queue.work_pool_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if work_pool is None:
        raise NativeStreamError("NOT_FOUND", "Native deployment work pool is missing")
    base_job_template = dict(work_pool.base_job_template or {})
    marker = base_job_template.get(NATIVE_DECLARATION_JOB_VARIABLE)
    if marker is not None and marker != NATIVE_WORK_POOL_MARKER:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Work pool native capability marker conflicts"
        )
    base_job_template[NATIVE_DECLARATION_JOB_VARIABLE] = dict(NATIVE_WORK_POOL_MARKER)
    work_pool.base_job_template = base_job_template
    _, declaration_digest = _canonical(declaration.model_dump(mode="json"))
    existing = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot).where(
            db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run.id
        )
    )
    if existing is not None:
        if existing.declaration_digest != declaration_digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Native deployment snapshot drift"
            )
        return existing
    active_snapshots = await session.scalar(
        sa.select(sa.func.count())
        .select_from(db.NativeStreamDeploymentSnapshot)
        .where(
            db.NativeStreamDeploymentSnapshot.work_pool_id == work_queue.work_pool_id,
            db.NativeStreamDeploymentSnapshot.state == "ACTIVE",
        )
    )
    if (
        active_snapshots is None
        or active_snapshots >= MAX_ACTIVE_NATIVE_RUNS_PER_WORK_POOL
    ):
        raise NativeStreamError(
            "CAPACITY_EXCEEDED",
            "bounded-default-v1 permits at most 128 active native FlowRuns per work pool",
        )
    snapshot = db.NativeStreamDeploymentSnapshot(
        flow_run_id=flow_run.id,
        deployment_id=deployment.id,
        deployment_version=deployment.version,
        work_pool_id=work_queue.work_pool_id,
        declaration=declaration.model_dump(mode="json"),
        declaration_digest=declaration_digest,
        protocol_version=declaration.protocol_version,
        profile=declaration.profile,
        flow_version=declaration.flow_version,
        flow_code_digest=declaration.flow_code_digest,
        resource_profile=declaration.resource_profile,
        state="ACTIVE",
    )
    labels = dict(flow_run.labels or {})
    labels["prefect.native-streaming"] = declaration.profile
    flow_run.labels = labels
    session.add(snapshot)
    await session.flush()
    return snapshot


def _enrollment_material(request: NativeStreamWorkerEnrollRequest) -> dict[str, Any]:
    return {
        "schema": "native-stream-worker-enrollment-v1",
        "flow_run_id": str(request.flow_run_id),
        "operation_id": str(request.operation_id),
        "expected_generation": request.expected_generation,
        "declaration_digest": request.declaration_digest,
        "worker_id": str(request.worker_id),
        "work_pool_id": str(request.work_pool_id),
        "launcher_instance_id": str(request.launcher_instance_id),
        "launcher_host_id": str(request.launcher_host_id),
        "launcher_boot_id": str(request.launcher_boot_id),
        "launcher_process_id": request.launcher_process_id,
        "launcher_start_ticks": request.launcher_start_ticks,
        "launch_profile": request.launch_profile,
        "holder_id": str(request.holder_id),
        "holder_capability_digest": _secret_digest(request.holder_capability),
        "holder_event_public_key": request.holder_event_public_key,
        "supervisor_id": str(request.supervisor_id),
        "supervisor_capability_digest": _secret_digest(request.supervisor_capability),
    }


def _enrollment_from_launch(
    snapshot: orm_models.NativeStreamDeploymentSnapshot,
    launch: orm_models.NativeStreamLaunchIdentity,
) -> NativeStreamWorkerEnrollment:
    return NativeStreamWorkerEnrollment(
        flow_run_id=launch.flow_run_id,
        deployment_id=snapshot.deployment_id,
        deployment_version=snapshot.deployment_version,
        work_pool_id=launch.work_pool_id,
        worker_id=launch.worker_id,
        snapshot_digest=snapshot.declaration_digest,
        generation=launch.generation,
        holder_id=launch.holder_id,
        operation_id=launch.enrollment_operation_id,
        launcher_instance_id=launch.launcher_instance_id,
        launch_profile=cast(Literal["linux-local-pidfd-v1"], launch.launch_profile),
        state=cast(Literal["ENROLLED", "PROCESS_REGISTERED", "RELEASED"], launch.state),
        created_at=launch.created,
    )


async def enroll_worker(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamWorkerEnrollRequest,
) -> NativeStreamWorkerEnrollment:
    """Atomically enrol a real worker and claim generation one."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native deployments require PostgreSQL 17"
        )
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    snapshot = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot)
        .where(db.NativeStreamDeploymentSnapshot.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    if snapshot is None:
        raise NativeStreamError(
            "UNSUPPORTED_PROFILE", "FlowRun has no native deployment snapshot"
        )
    if not hmac.compare_digest(snapshot.declaration_digest, request.declaration_digest):
        raise NativeStreamError("IDENTITY_CONFLICT", "Declaration digest drift")
    _, request_digest = _canonical(_enrollment_material(request))
    existing = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity).where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.enrollment_operation_id
            == request.operation_id,
        )
    )
    if existing is not None:
        if existing.enrollment_request_digest != request_digest:
            raise NativeStreamError("IDENTITY_CONFLICT", "Enrollment identity drift")
        return _enrollment_from_launch(snapshot, existing)

    worker = await session.scalar(
        sa.select(db.Worker).where(db.Worker.id == request.worker_id).with_for_update()
    )
    if worker is None or worker.work_pool_id != request.work_pool_id:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Worker is not registered in the requested work pool"
        )
    if worker.status != WorkerStatus.ONLINE:
        raise NativeStreamError("METADATA_NOT_READY", "Worker is not online")
    if snapshot.work_pool_id != request.work_pool_id:
        raise NativeStreamError("IDENTITY_CONFLICT", "Work pool identity drift")

    binding = await _claim_execution(
        session,
        db,
        StreamExecutionClaimRequest(
            flow_run_id=request.flow_run_id,
            operation_id=request.operation_id,
            expected_generation=0,
            holder_id=request.holder_id,
            holder_capability=request.holder_capability,
            holder_event_public_key=request.holder_event_public_key,
            supervisor_id=request.supervisor_id,
            supervisor_capability=request.supervisor_capability,
            protocol_version=1,
        ),
    )
    launch = db.NativeStreamLaunchIdentity(
        flow_run_id=request.flow_run_id,
        snapshot_id=snapshot.id,
        generation=binding.generation,
        enrollment_operation_id=request.operation_id,
        enrollment_request_digest=request_digest,
        enrollment_outcome_digest="0" * 64,
        work_pool_id=request.work_pool_id,
        worker_id=request.worker_id,
        launcher_instance_id=request.launcher_instance_id,
        launcher_host_id=request.launcher_host_id,
        launcher_boot_id=request.launcher_boot_id,
        launcher_process_id=request.launcher_process_id,
        launcher_start_ticks=request.launcher_start_ticks,
        launch_profile=request.launch_profile,
        holder_id=request.holder_id,
        holder_capability_digest=_secret_digest(request.holder_capability),
        holder_event_public_key=request.holder_event_public_key,
        supervisor_id=request.supervisor_id,
        supervisor_capability_digest=_secret_digest(request.supervisor_capability),
        state="ENROLLED",
        last_renewed_at=cast(DateTime, now("UTC")),
    )
    session.add(launch)
    await session.flush()
    result = _enrollment_from_launch(snapshot, launch)
    _, launch.enrollment_outcome_digest = _canonical(result.model_dump(mode="json"))
    await session.flush()
    return result


def _projection(
    snapshot: orm_models.NativeStreamDeploymentSnapshot,
    launch: orm_models.NativeStreamLaunchIdentity,
) -> NativeStreamLaunchProjection:
    return NativeStreamLaunchProjection(
        flow_run_id=launch.flow_run_id,
        deployment_id=snapshot.deployment_id,
        work_pool_id=launch.work_pool_id,
        worker_id=launch.worker_id,
        snapshot_digest=snapshot.declaration_digest,
        generation=launch.generation,
        enrollment_operation_id=launch.enrollment_operation_id,
        launcher_instance_id=launch.launcher_instance_id,
        launch_profile=cast(Literal["linux-local-pidfd-v1"], launch.launch_profile),
        state=cast(Literal["ENROLLED", "PROCESS_REGISTERED", "RELEASED"], launch.state),
        created_at=launch.created,
        process_registered_at=launch.process_registered_at,
        last_renewed_at=launch.last_renewed_at,
    )


async def register_process(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamProcessRegistrationRequest,
    execution: StreamExecutionCredentials | None,
) -> NativeStreamLaunchProjection:
    """Persist exact Linux child identity before releasing its bootstrap pipe."""

    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    snapshot = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot)
        .where(db.NativeStreamDeploymentSnapshot.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.enrollment_operation_id
            == request.enrollment_operation_id,
        )
        .with_for_update()
    )
    if snapshot is None or launch is None:
        raise NativeStreamError("NOT_FOUND", "Unknown native launch")
    if snapshot.state != "ACTIVE":
        raise NativeStreamError("REPLAY_HORIZON_EXPIRED", "Snapshot was released")
    if execution is None:
        raise NativeStreamError("PROTOCOL_ERROR", "Execution headers are required")
    if (
        execution.generation != launch.generation
        or execution.holder_id != launch.holder_id
        or request.generation != launch.generation
        or request.launcher_instance_id != launch.launcher_instance_id
    ):
        raise NativeStreamError("STALE_EXECUTION", "Launch identity changed")
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    if authority is None or not hmac.compare_digest(
        authority.holder_capability_digest,
        hashlib.sha256(bytes.fromhex(execution.capability)).hexdigest(),
    ):
        raise NativeStreamError("STALE_EXECUTION", "Execution capability changed")
    material = request.model_dump(mode="json")
    _, request_digest = _canonical(material)
    if launch.process_registration_operation_id is not None:
        if (
            launch.process_registration_operation_id != request.operation_id
            or launch.process_registration_request_digest != request_digest
        ):
            raise NativeStreamError("IDENTITY_CONFLICT", "Process identity drift")
        return _projection(snapshot, launch)
    launch.process_registration_operation_id = request.operation_id
    launch.process_registration_request_digest = request_digest
    launch.process_registration_outcome_digest = "0" * 64
    launch.host_id = request.host_id
    launch.boot_id = request.boot_id
    launch.child_process_id = request.child_process_id
    launch.process_start_ticks = request.process_start_ticks
    launch.launch_nonce_digest = request.launch_nonce_digest
    launch.process_registered_at = cast(DateTime, now("UTC"))
    launch.last_renewed_at = launch.process_registered_at
    launch.state = "PROCESS_REGISTERED"
    await session.flush()
    result = _projection(snapshot, launch)
    _, launch.process_registration_outcome_digest = _canonical(
        result.model_dump(mode="json")
    )
    await session.flush()
    return result


async def read_plan(
    session: AsyncSession,
    db: PrefectDBInterface,
    flow_run_id: UUID,
) -> NativeStreamDeploymentPlan:
    snapshot = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot).where(
            db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run_id
        )
    )
    if snapshot is None:
        raise NativeStreamError("NOT_FOUND", "Unknown native deployment snapshot")
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(db.NativeStreamLaunchIdentity.flow_run_id == flow_run_id)
        .order_by(db.NativeStreamLaunchIdentity.generation.desc())
        .limit(1)
    )
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority).where(
            db.StreamExecutionAuthority.flow_run_id == flow_run_id
        )
    )
    declaration = NativeStreamDeclaration.model_validate(snapshot.declaration)
    return NativeStreamDeploymentPlan(
        flow_run_id=flow_run_id,
        deployment_id=snapshot.deployment_id,
        deployment_version=snapshot.deployment_version,
        work_pool_id=snapshot.work_pool_id,
        declaration=declaration,
        declaration_digest=snapshot.declaration_digest,
        state=cast(Literal["ACTIVE", "RELEASED"], snapshot.state),
        created_at=snapshot.created,
        current_launch=None if launch is None else _projection(snapshot, launch),
        source_admission_closed=(
            False if authority is None else authority.source_admission_closed
        ),
        cancel_requested=False if authority is None else authority.cancel_requested,
    )


async def read_projection(
    session: AsyncSession,
    db: PrefectDBInterface,
    flow_run_id: UUID,
) -> NativeStreamFlowProjection:
    """Read a bounded, payload-free native execution projection."""

    flow = await session.get(db.FlowRun, flow_run_id)
    snapshot = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot).where(
            db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run_id
        )
    )
    if flow is None or snapshot is None:
        raise NativeStreamError("NOT_FOUND", "Unknown native FlowRun")
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority).where(
            db.StreamExecutionAuthority.flow_run_id == flow_run_id
        )
    )
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(db.NativeStreamLaunchIdentity.flow_run_id == flow_run_id)
        .order_by(db.NativeStreamLaunchIdentity.generation.desc())
        .limit(1)
    )
    manifests = (
        await session.scalars(
            sa.select(db.StreamSubmission)
            .where(db.StreamSubmission.flow_run_id == flow_run_id)
            .order_by(db.StreamSubmission.submission_key)
            .limit(129)
        )
    ).all()
    finite = (
        await session.scalars(
            sa.select(db.StreamFiniteSubmission)
            .where(db.StreamFiniteSubmission.flow_run_id == flow_run_id)
            .order_by(db.StreamFiniteSubmission.phase)
            .limit(3)
        )
    ).all()
    if len(manifests) > 128 or len(finite) > 2:
        raise NativeStreamError("CAPACITY", "Native projection exceeds its bound")
    stream_ids = {manifest.stream_id for manifest in manifests}
    stream_ids.update(
        manifest.upstream_stream_id
        for manifest in manifests
        if manifest.upstream_stream_id is not None
    )
    streams = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamPartition).where(
                    db.StreamPartition.id.in_(stream_ids)
                )
            )
        ).all()
    }
    consumers = {
        row.output_stream_id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamConsumer).where(
                    db.StreamConsumer.flow_run_id == flow_run_id
                )
            )
        ).all()
    }
    task_ids = {manifest.task_run_id for manifest in manifests}
    task_ids.update(item.task_run_id for item in finite)
    tasks = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.TaskRun).where(db.TaskRun.id.in_(task_ids))
            )
        ).all()
    }
    receipt_ids = set(
        (
            await session.scalars(
                sa.select(db.StreamFiniteReceipt.finite_submission_id).where(
                    db.StreamFiniteReceipt.finite_submission_id.in_(
                        [item.id for item in finite]
                    )
                )
            )
        ).all()
    )
    items: list[NativeStreamTaskProjection] = []
    for manifest in manifests:
        stream = streams.get(manifest.stream_id)
        task = tasks.get(manifest.task_run_id)
        if stream is None or task is None:
            raise NativeStreamError("IDENTITY_CONFLICT", "Projection row is missing")
        limits = StreamLimits.model_validate(stream.limits)
        consumer = consumers.get(stream.id)
        input_stream = (
            streams.get(consumer.input_stream_id) if consumer is not None else None
        )
        input_tail = input_stream.last_sequence if input_stream is not None else None
        completed = consumer.completed_through if consumer is not None else None
        items.append(
            NativeStreamTaskProjection(
                task_run_id=manifest.task_run_id,
                submission_key=manifest.submission_key,
                role=cast(Literal["SOURCE", "OPERATOR"], manifest.role),
                protocol=cast(Literal["source-v1", "yield-v2"], manifest.protocol),
                task_state=(
                    task.state_type.value if task.state_type is not None else "PENDING"
                ),
                run_count=task.run_count,
                input_stream_id=manifest.upstream_stream_id,
                output_stream_id=stream.id,
                stream_state=cast(Literal["OPEN", "COMPLETED", "FAILED"], stream.state),
                record_count=stream.record_count,
                byte_count=stream.byte_count,
                record_limit=limits.max_records,
                byte_limit=limits.max_bytes,
                last_sequence=stream.last_sequence,
                tail_sequence=stream.tail_sequence,
                completed_through=completed,
                inflight_sequence=(
                    consumer.inflight_sequence if consumer is not None else None
                ),
                input_tail_sequence=input_tail,
                pending_inputs=(
                    max(0, input_tail - completed)
                    if input_tail is not None and completed is not None
                    else None
                ),
            )
        )
    finite_items: list[NativeStreamFiniteProjection] = []
    for item in finite:
        task = tasks.get(item.task_run_id)
        if task is None:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Projection TaskRun is missing"
            )
        task_state = task.state_type
        finite_items.append(
            NativeStreamFiniteProjection(
                task_run_id=item.task_run_id,
                submission_key=item.submission_key,
                phase=cast(Literal["PREFIX", "TAIL"], item.phase),
                task_state=task_state.value if task_state is not None else "PENDING",
                run_count=task.run_count,
                result_committed=item.id in receipt_ids,
            )
        )
    flow_state = flow.state_type
    return NativeStreamFlowProjection(
        flow_run_id=flow_run_id,
        flow_state=flow_state.value if flow_state is not None else "PENDING",
        observed_at=cast(DateTime, now("UTC")),
        generation=authority.generation if authority is not None else None,
        authority_state=authority.state if authority is not None else None,
        prepared_generation=(
            authority.prepared_generation if authority is not None else None
        ),
        manifest_sealed=(
            authority is not None and authority.manifest_set_digest is not None
        ),
        source_admission_closed=(
            authority.source_admission_closed if authority is not None else False
        ),
        cancel_requested=(
            authority.cancel_requested if authority is not None else False
        ),
        launch_state=(
            cast(
                Literal["ENROLLED", "PROCESS_REGISTERED", "RELEASED"],
                launch.state,
            )
            if launch is not None
            else None
        ),
        worker_id=launch.worker_id if launch is not None else None,
        items=items,
        finite_items=finite_items,
    )


async def renew_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamLaunchHeartbeatRequest,
    execution: StreamExecutionCredentials | None,
) -> NativeStreamLaunchHeartbeat:
    """Renew the current holder's registered launch without asserting liveness."""

    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    authority = await require_execution(
        session,
        db,
        request.flow_run_id,
        execution,
        allowed_states=("ACTIVE",),
        update=True,
    )
    if authority is None or authority.generation != request.generation:
        raise NativeStreamError("STALE_EXECUTION", "Launch generation changed")
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.generation == request.generation,
            db.NativeStreamLaunchIdentity.enrollment_operation_id
            == request.enrollment_operation_id,
        )
        .with_for_update()
    )
    if launch is None:
        raise NativeStreamError("NOT_FOUND", "Unknown native launch")
    if launch.state not in {"ENROLLED", "PROCESS_REGISTERED"}:
        raise NativeStreamError("RECOVERY_NOT_READY", "Launch is not renewable")
    launch.last_renewed_at = cast(DateTime, now("UTC"))
    await session.flush()
    return NativeStreamLaunchHeartbeat(
        flow_run_id=launch.flow_run_id,
        generation=launch.generation,
        enrollment_operation_id=launch.enrollment_operation_id,
        last_renewed_at=cast(DateTime, launch.last_renewed_at),
    )


async def _worker(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    work_pool_id: UUID,
) -> orm_models.Worker:
    worker = await session.scalar(
        sa.select(db.Worker).where(db.Worker.id == worker_id).with_for_update()
    )
    if (
        worker is None
        or worker.work_pool_id != work_pool_id
        or worker.status != WorkerStatus.ONLINE
    ):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Worker is not online in the requested work pool"
        )
    return worker


async def recovery_candidates(
    session: AsyncSession,
    db: PrefectDBInterface,
    *,
    worker_id: UUID,
    work_pool_id: UUID,
    host_id: UUID,
) -> list[NativeStreamRecoveryCandidate]:
    """List stale launches eligible for exact same-host identity verification."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native replacements require PostgreSQL 17"
        )
    await _worker(session, db, worker_id, work_pool_id)
    stale_before = now("UTC") - LAUNCH_RENEWAL_STALE_AFTER
    rows = (
        await session.execute(
            sa.select(
                db.NativeStreamDeploymentSnapshot,
                db.NativeStreamLaunchIdentity,
            )
            .join(
                db.NativeStreamLaunchIdentity,
                db.NativeStreamLaunchIdentity.snapshot_id
                == db.NativeStreamDeploymentSnapshot.id,
            )
            .join(
                db.StreamExecutionAuthority,
                db.StreamExecutionAuthority.flow_run_id
                == db.NativeStreamLaunchIdentity.flow_run_id,
            )
            .where(
                db.NativeStreamDeploymentSnapshot.work_pool_id == work_pool_id,
                db.NativeStreamDeploymentSnapshot.state == "ACTIVE",
                db.NativeStreamLaunchIdentity.last_renewed_at <= stale_before,
                db.NativeStreamLaunchIdentity.generation
                == db.StreamExecutionAuthority.generation,
                sa.or_(
                    sa.and_(
                        db.NativeStreamLaunchIdentity.state == "PROCESS_REGISTERED",
                        db.NativeStreamLaunchIdentity.host_id == host_id,
                        db.StreamExecutionAuthority.state.in_(
                            ("CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL")
                        ),
                    ),
                    sa.and_(
                        db.NativeStreamLaunchIdentity.state == "ENROLLED",
                        db.NativeStreamLaunchIdentity.launcher_host_id == host_id,
                        db.StreamExecutionAuthority.state.in_(
                            ("CLAIMED", "RECOVERING")
                        ),
                    ),
                ),
            )
            .order_by(db.NativeStreamLaunchIdentity.last_renewed_at)
            .limit(128)
        )
    ).all()
    result: list[NativeStreamRecoveryCandidate] = []
    for snapshot, launch in rows:
        if launch.last_renewed_at is None:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Launch renewal witness is missing"
            )
        if launch.state == "ENROLLED":
            identity_kind: Literal["LAUNCHER", "CHILD"] = "LAUNCHER"
            identity_host = launch.launcher_host_id
            identity_boot = launch.launcher_boot_id
            identity_process = launch.launcher_process_id
            identity_start = launch.launcher_start_ticks
        else:
            identity_kind = "CHILD"
            identity_host = launch.host_id
            identity_boot = launch.boot_id
            identity_process = launch.child_process_id
            identity_start = launch.process_start_ticks
        if (
            identity_host is None
            or identity_boot is None
            or identity_process is None
            or identity_start is None
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Launch has incomplete verification identity"
            )
        result.append(
            NativeStreamRecoveryCandidate(
                flow_run_id=launch.flow_run_id,
                deployment_id=snapshot.deployment_id,
                generation=launch.generation,
                work_pool_id=launch.work_pool_id,
                worker_id=launch.worker_id,
                launcher_instance_id=launch.launcher_instance_id,
                enrollment_operation_id=launch.enrollment_operation_id,
                launch_profile=launch.launch_profile,
                identity_kind=identity_kind,
                host_id=identity_host,
                boot_id=identity_boot,
                child_process_id=identity_process,
                process_start_ticks=identity_start,
                last_renewed_at=launch.last_renewed_at,
                verification_not_before=launch.last_renewed_at
                + LAUNCH_RENEWAL_STALE_AFTER,
            )
        )
    return result


def _replacement_authorization_material(
    request: NativeStreamReplacementAuthorizeRequest,
) -> dict[str, Any]:
    material = request.model_dump(mode="json", exclude={"challenge_capability"})
    material["schema"] = "native-stream-replacement-authorization-v1"
    material["challenge_capability_digest"] = _secret_digest(
        request.challenge_capability
    )
    return material


def _replacement_authorization(
    row: orm_models.NativeStreamReplacementOperation,
) -> NativeStreamReplacementAuthorization:
    return NativeStreamReplacementAuthorization(
        flow_run_id=row.flow_run_id,
        operation_id=row.operation_id,
        expected_generation=row.expected_generation,
        worker_id=row.worker_id,
        work_pool_id=row.work_pool_id,
        host_id=row.host_id,
        identity_kind=cast(Literal["LAUNCHER", "CHILD"], row.identity_kind),
        state=cast(Literal["AUTHORIZED", "CONFIRMED", "RELEASED"], row.state),
        created_at=row.created,
    )


async def authorize_replacement(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamReplacementAuthorizeRequest,
    execution: StreamExecutionCredentials | None,
) -> NativeStreamReplacementAuthorization:
    """Authorize death verification without treating renewal expiry as death."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native replacements require PostgreSQL 17"
        )
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    snapshot = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot)
        .where(db.NativeStreamDeploymentSnapshot.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.generation == request.expected_generation,
        )
        .with_for_update()
    )
    if authority is None or snapshot is None or launch is None:
        raise NativeStreamError("NOT_FOUND", "Unknown native deployment launch")
    _, request_digest = _canonical(_replacement_authorization_material(request))
    existing = await session.scalar(
        sa.select(db.NativeStreamReplacementOperation)
        .where(
            db.NativeStreamReplacementOperation.flow_run_id == request.flow_run_id,
            db.NativeStreamReplacementOperation.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if existing is not None:
        if existing.authorization_request_digest != request_digest:
            raise NativeStreamError("IDENTITY_CONFLICT", "Replacement identity drift")
        return _replacement_authorization(existing)
    await _worker(session, db, request.worker_id, request.work_pool_id)
    launch_matches_identity = (
        request.identity_kind == "CHILD"
        and launch.state == "PROCESS_REGISTERED"
        and launch.host_id == request.host_id
        and authority.state in {"CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL"}
    ) or (
        request.identity_kind == "LAUNCHER"
        and launch.state == "ENROLLED"
        and launch.launcher_host_id == request.host_id
        and authority.state in {"CLAIMED", "RECOVERING"}
    )
    if (
        snapshot.state != "ACTIVE"
        or snapshot.work_pool_id != request.work_pool_id
        or authority.generation != request.expected_generation
        or launch.enrollment_operation_id != request.enrollment_operation_id
        or not launch_matches_identity
    ):
        raise NativeStreamError("STALE_EXECUTION", "Replacement candidate changed")
    current_holder = False
    if execution is not None:
        supplied = _secret_digest(execution.capability)
        current_holder = (
            execution.generation == authority.generation
            and execution.holder_id == authority.holder_id
            and hmac.compare_digest(authority.holder_capability_digest, supplied)
        )
    if not current_holder:
        if launch.last_renewed_at is None:
            raise NativeStreamError("IDENTITY_CONFLICT", "Launch renewal is missing")
        if launch.last_renewed_at > now("UTC") - LAUNCH_RENEWAL_STALE_AFTER:
            raise NativeStreamError(
                "METADATA_NOT_READY",
                "Launch renewal must become stale before external verification",
            )
    row = db.NativeStreamReplacementOperation(
        flow_run_id=request.flow_run_id,
        operation_id=request.operation_id,
        expected_generation=request.expected_generation,
        worker_id=request.worker_id,
        work_pool_id=request.work_pool_id,
        host_id=request.host_id,
        identity_kind=request.identity_kind,
        challenge_capability_digest=_secret_digest(request.challenge_capability),
        authorization_request_digest=request_digest,
        state="AUTHORIZED",
    )
    session.add(row)
    await session.flush()
    return _replacement_authorization(row)


def _confirm_material(request: NativeStreamReplacementConfirmRequest) -> dict[str, Any]:
    material = request.model_dump(
        mode="json",
        exclude={
            "challenge_capability",
            "new_holder_capability",
            "new_supervisor_capability",
        },
    )
    material["schema"] = "native-stream-replacement-confirmation-v1"
    material["challenge_capability_digest"] = _secret_digest(
        request.challenge_capability
    )
    material["new_holder_capability_digest"] = _secret_digest(
        request.new_holder_capability
    )
    material["new_supervisor_capability_digest"] = _secret_digest(
        request.new_supervisor_capability
    )
    return material


async def confirm_replacement(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamReplacementConfirmRequest,
) -> NativeStreamWorkerEnrollment:
    """Fence a process only after an authorized exact kernel death witness."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native replacements require PostgreSQL 17"
        )
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    snapshot = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot)
        .where(db.NativeStreamDeploymentSnapshot.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    operation = await session.scalar(
        sa.select(db.NativeStreamReplacementOperation)
        .where(
            db.NativeStreamReplacementOperation.flow_run_id == request.flow_run_id,
            db.NativeStreamReplacementOperation.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if authority is None or snapshot is None or operation is None:
        raise NativeStreamError("NOT_FOUND", "Unknown replacement authorization")
    supplied = _secret_digest(request.challenge_capability)
    if not hmac.compare_digest(operation.challenge_capability_digest, supplied):
        raise NativeStreamError(
            "TAKEOVER_NOT_AUTHORIZED", "Replacement challenge is invalid"
        )
    if operation.identity_kind != "CHILD":
        raise NativeStreamError(
            "TAKEOVER_NOT_AUTHORIZED",
            "A launcher-only death witness can retire but cannot replace a child",
        )
    if operation.expected_generation >= 64:
        raise NativeStreamError(
            "RECOVERY_POLICY_EXHAUSTED",
            "Native execution generation limit is exhausted",
        )
    _, confirm_digest = _canonical(_confirm_material(request))
    if operation.state == "CONFIRMED":
        if operation.confirm_request_digest != confirm_digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Replacement confirmation drift"
            )
        launch = await session.scalar(
            sa.select(db.NativeStreamLaunchIdentity).where(
                db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
                db.NativeStreamLaunchIdentity.generation == operation.new_generation,
            )
        )
        if launch is None:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Replacement launch is missing"
            )
        return _enrollment_from_launch(snapshot, launch)
    old_launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.generation == operation.expected_generation,
        )
        .with_for_update()
    )
    witness = request.death_witness
    if (
        old_launch is None
        or authority.generation != operation.expected_generation
        or authority.state != "ACTIVE"
        or old_launch.state != "PROCESS_REGISTERED"
        or old_launch.host_id != witness.host_id
        or old_launch.boot_id != witness.boot_id
        or old_launch.child_process_id != witness.child_process_id
        or old_launch.process_start_ticks != witness.process_start_ticks
    ):
        raise NativeStreamError("STALE_EXECUTION", "Verified process identity changed")
    binding = await _apply_takeover_locked(
        session,
        db,
        flow=flow,
        authority=authority,
        operation_id=operation.operation_id,
        request_digest=confirm_digest,
        new_holder_id=request.new_holder_id,
        new_holder_capability=request.new_holder_capability,
        new_holder_event_public_key=request.new_holder_event_public_key,
        new_supervisor_id=request.new_supervisor_id,
        new_supervisor_capability_digest=_secret_digest(
            request.new_supervisor_capability
        ),
        death_witness=witness.model_dump(mode="json"),
    )
    launch = db.NativeStreamLaunchIdentity(
        flow_run_id=request.flow_run_id,
        snapshot_id=snapshot.id,
        generation=binding.generation,
        enrollment_operation_id=operation.operation_id,
        enrollment_request_digest=confirm_digest,
        enrollment_outcome_digest="0" * 64,
        work_pool_id=operation.work_pool_id,
        worker_id=operation.worker_id,
        launcher_instance_id=request.launcher_instance_id,
        launcher_host_id=request.launcher_host_id,
        launcher_boot_id=request.launcher_boot_id,
        launcher_process_id=request.launcher_process_id,
        launcher_start_ticks=request.launcher_start_ticks,
        launch_profile="linux-local-pidfd-v1",
        holder_id=request.new_holder_id,
        holder_capability_digest=_secret_digest(request.new_holder_capability),
        holder_event_public_key=request.new_holder_event_public_key,
        supervisor_id=request.new_supervisor_id,
        supervisor_capability_digest=_secret_digest(request.new_supervisor_capability),
        state="ENROLLED",
        last_renewed_at=cast(DateTime, now("UTC")),
    )
    session.add(launch)
    operation.state = "CONFIRMED"
    operation.confirm_request_digest = confirm_digest
    operation.confirm_outcome_digest = "0" * 64
    operation.death_witness = witness.model_dump(mode="json")
    operation.new_generation = binding.generation
    await session.flush()
    result = _enrollment_from_launch(snapshot, launch)
    _, launch.enrollment_outcome_digest = _canonical(result.model_dump(mode="json"))
    operation.confirm_outcome_digest = launch.enrollment_outcome_digest
    await session.flush()
    return result


async def apply_control(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamControlRequest,
    execution: StreamExecutionCredentials | None,
    *,
    kind: Literal["DRAIN", "CANCEL"],
) -> NativeStreamControlResult:
    """Journal and apply an idempotent source drain or write cancellation."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native controls require PostgreSQL 17"
        )
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    if execution is None:
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority)
            .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
            .with_for_update()
        )
        if authority is None:
            raise NativeStreamError("NOT_FOUND", "FlowRun is not enrolled")
    else:
        authority = await require_execution(
            session,
            db,
            request.flow_run_id,
            execution,
            allowed_states=("ACTIVE", "TERMINAL", "RELEASED"),
            update=True,
        )
    if authority is None or authority.generation != request.expected_generation:
        raise NativeStreamError("STALE_EXECUTION", "Control generation changed")
    material = request.model_dump(mode="json") | {
        "schema": "native-stream-control-v1",
        "kind": kind,
    }
    _, request_digest = _canonical(material)
    prior = await session.scalar(
        sa.select(db.NativeStreamControlOperation)
        .where(
            db.NativeStreamControlOperation.flow_run_id == request.flow_run_id,
            db.NativeStreamControlOperation.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if prior is not None:
        if prior.request_digest != request_digest or prior.kind != kind:
            raise NativeStreamError("IDENTITY_CONFLICT", "Control operation drift")
        return NativeStreamControlResult.model_validate(prior.outcome)
    if authority.state != "ACTIVE":
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Execution generation is not active"
        )
    authority.source_admission_closed = True
    if kind == "CANCEL":
        authority.cancel_requested = True
        if flow.state_type != schemas.states.StateType.CANCELLING:
            state_result = await flow_run_models.set_flow_run_state(
                session=session,
                flow_run_id=request.flow_run_id,
                state=schemas.states.Cancelling(message=request.reason),
                force=False,
                flow_policy=CoreFlowPolicy,
            )
            if state_result.state is None or not state_result.state.is_cancelling():
                raise NativeStreamError(
                    "TERMINAL_CONFLICT", "FlowRun did not enter Cancelling"
                )
    result = NativeStreamControlResult(
        flow_run_id=request.flow_run_id,
        operation_id=request.operation_id,
        generation=authority.generation,
        kind=kind,
        source_admission_closed=authority.source_admission_closed,
        cancel_requested=authority.cancel_requested,
        created_at=cast(DateTime, now("UTC")),
    )
    outcome = result.model_dump(mode="json")
    _, outcome_digest = _canonical(outcome)
    session.add(
        db.NativeStreamControlOperation(
            flow_run_id=request.flow_run_id,
            operation_id=request.operation_id,
            generation=authority.generation,
            kind=kind,
            reason=request.reason,
            request_digest=request_digest,
            outcome=outcome,
            outcome_digest=outcome_digest,
        )
    )
    await session.flush()
    return result


async def finalize_cancelled_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamCancelFinalizeRequest,
    execution: StreamExecutionCredentials | None,
) -> NativeStreamControlResult:
    """Finalize cancellation only after the current child has exited."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native cancellation requires PostgreSQL 17"
        )
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    if execution is None:
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority)
            .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
            .with_for_update()
        )
    else:
        authority = await require_execution(
            session,
            db,
            request.flow_run_id,
            execution,
            allowed_states=("ACTIVE", "TERMINAL", "RELEASED"),
            update=True,
        )
    if authority is None or authority.generation != request.expected_generation:
        raise NativeStreamError("STALE_EXECUTION", "Cancellation generation changed")
    material = request.model_dump(mode="json", exclude={"challenge_capability"}) | {
        "schema": "native-stream-cancel-finalization-v1"
    }
    if request.challenge_capability is not None:
        material["challenge_capability_digest"] = _secret_digest(
            request.challenge_capability
        )
    _, request_digest = _canonical(material)
    prior = await session.scalar(
        sa.select(db.NativeStreamControlOperation)
        .where(
            db.NativeStreamControlOperation.flow_run_id == request.flow_run_id,
            db.NativeStreamControlOperation.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if prior is not None:
        if prior.kind != "CANCEL_FINALIZED" or prior.request_digest != request_digest:
            raise NativeStreamError("IDENTITY_CONFLICT", "Cancel finalization drift")
        return NativeStreamControlResult.model_validate(prior.outcome)
    if execution is None:
        operation = await session.scalar(
            sa.select(db.NativeStreamReplacementOperation)
            .where(
                db.NativeStreamReplacementOperation.flow_run_id == request.flow_run_id,
                db.NativeStreamReplacementOperation.operation_id
                == request.operation_id,
            )
            .with_for_update()
        )
        if operation is None or request.challenge_capability is None:
            raise NativeStreamError(
                "TAKEOVER_NOT_AUTHORIZED",
                "Cancellation death verification is not authorized",
            )
        supplied = _secret_digest(request.challenge_capability)
        if (
            operation.state != "AUTHORIZED"
            or operation.expected_generation != request.expected_generation
            or not hmac.compare_digest(operation.challenge_capability_digest, supplied)
        ):
            raise NativeStreamError(
                "TAKEOVER_NOT_AUTHORIZED", "Cancellation challenge is invalid"
            )
    elif request.challenge_capability is not None:
        raise NativeStreamError(
            "PROTOCOL_ERROR", "Holder finalization cannot include a challenge"
        )
    if authority.state != "ACTIVE" or not authority.cancel_requested:
        raise NativeStreamError(
            "TERMINAL_CONFLICT", "Cancellation was not requested for this execution"
        )
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.generation == authority.generation,
            db.NativeStreamLaunchIdentity.enrollment_operation_id
            == request.enrollment_operation_id,
        )
        .with_for_update()
    )
    witness = request.death_witness
    if (
        launch is None
        or launch.state != "PROCESS_REGISTERED"
        or launch.host_id != witness.host_id
        or launch.boot_id != witness.boot_id
        or launch.child_process_id != witness.child_process_id
        or launch.process_start_ticks != witness.process_start_ticks
    ):
        raise NativeStreamError("STALE_EXECUTION", "Cancelled process identity changed")
    if execution is None:
        operation.death_witness = witness.model_dump(mode="json")
    task_ids = set(
        (
            await session.scalars(
                sa.select(db.StreamSubmission.task_run_id).where(
                    db.StreamSubmission.flow_run_id == request.flow_run_id
                )
            )
        ).all()
    )
    task_ids.update(
        (
            await session.scalars(
                sa.select(db.StreamFiniteSubmission.task_run_id).where(
                    db.StreamFiniteSubmission.flow_run_id == request.flow_run_id
                )
            )
        ).all()
    )
    tasks = (
        await session.scalars(
            sa.select(db.TaskRun)
            .where(db.TaskRun.id.in_(task_ids))
            .order_by(db.TaskRun.id)
            .with_for_update()
        )
    ).all()
    terminal_types = {
        schemas.states.StateType.COMPLETED,
        schemas.states.StateType.FAILED,
        schemas.states.StateType.CANCELLED,
        schemas.states.StateType.CRASHED,
    }
    for task in tasks:
        if task.state_type in terminal_types:
            continue
        task_result = await task_run_models.set_task_run_state(
            session=session,
            task_run_id=task.id,
            state=schemas.states.Cancelled(message="Native worker process exited"),
            force=False,
            task_policy=CoreTaskPolicy,
        )
        if task_result.state is None or not task_result.state.is_cancelled():
            raise NativeStreamError(
                "TERMINAL_CONFLICT", "TaskRun did not finalize as Cancelled"
            )
    state_result = await flow_run_models.set_flow_run_state(
        session=session,
        flow_run_id=request.flow_run_id,
        state=schemas.states.Cancelled(message="Native worker process exited"),
        force=False,
        flow_policy=CoreFlowPolicy,
    )
    if state_result.state is None or not state_result.state.is_cancelled():
        raise NativeStreamError(
            "TERMINAL_CONFLICT", "FlowRun did not finalize as Cancelled"
        )
    authority.state = "TERMINAL"
    launch.state = "RELEASED"
    result = NativeStreamControlResult(
        flow_run_id=request.flow_run_id,
        operation_id=request.operation_id,
        generation=authority.generation,
        kind="CANCEL_FINALIZED",
        source_admission_closed=True,
        cancel_requested=True,
        created_at=cast(DateTime, now("UTC")),
    )
    outcome = result.model_dump(mode="json")
    _, outcome_digest = _canonical(outcome)
    session.add(
        db.NativeStreamControlOperation(
            flow_run_id=request.flow_run_id,
            operation_id=request.operation_id,
            generation=authority.generation,
            kind="CANCEL_FINALIZED",
            reason="Verified child exit",
            request_digest=request_digest,
            outcome=outcome,
            outcome_digest=outcome_digest,
        )
    )
    await session.flush()
    return result


async def retire_verified_dead_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamVerifiedRetireRequest,
) -> NativeStreamControlResult:
    """Retire a verified dead launch only when no retry remains possible."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native retirement requires PostgreSQL 17"
        )
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    operation = await session.scalar(
        sa.select(db.NativeStreamReplacementOperation)
        .where(
            db.NativeStreamReplacementOperation.flow_run_id == request.flow_run_id,
            db.NativeStreamReplacementOperation.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if flow is None or authority is None or operation is None:
        raise NativeStreamError("NOT_FOUND", "Unknown verified launch retirement")
    material = request.model_dump(mode="json", exclude={"challenge_capability"}) | {
        "schema": "native-stream-verified-retirement-v1",
        "challenge_capability_digest": _secret_digest(request.challenge_capability),
    }
    _, request_digest = _canonical(material)
    expected_kind: Literal["SPAWN_FAILED", "VERIFIED_RETIRED"] = (
        "SPAWN_FAILED" if operation.identity_kind == "LAUNCHER" else "VERIFIED_RETIRED"
    )
    prior = await session.scalar(
        sa.select(db.NativeStreamControlOperation)
        .where(
            db.NativeStreamControlOperation.flow_run_id == request.flow_run_id,
            db.NativeStreamControlOperation.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if prior is not None:
        if prior.kind != expected_kind or prior.request_digest != request_digest:
            raise NativeStreamError("IDENTITY_CONFLICT", "Retirement intent drift")
        return NativeStreamControlResult.model_validate(prior.outcome)
    supplied = _secret_digest(request.challenge_capability)
    if (
        operation.state != "AUTHORIZED"
        or operation.expected_generation != request.expected_generation
        or not hmac.compare_digest(operation.challenge_capability_digest, supplied)
        or authority.generation != request.expected_generation
        or authority.state not in {"CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL"}
    ):
        raise NativeStreamError(
            "TAKEOVER_NOT_AUTHORIZED", "Verified retirement is not authorized"
        )
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.generation == authority.generation,
            db.NativeStreamLaunchIdentity.enrollment_operation_id
            == request.enrollment_operation_id,
        )
        .with_for_update()
    )
    witness = request.death_witness
    if operation.identity_kind == "LAUNCHER":
        identity_matches = (
            launch is not None
            and launch.state == "ENROLLED"
            and launch.launcher_host_id == witness.host_id
            and launch.launcher_boot_id == witness.boot_id
            and launch.launcher_process_id == witness.child_process_id
            and launch.launcher_start_ticks == witness.process_start_ticks
        )
    else:
        identity_matches = (
            launch is not None
            and launch.state == "PROCESS_REGISTERED"
            and launch.host_id == witness.host_id
            and launch.boot_id == witness.boot_id
            and launch.child_process_id == witness.child_process_id
            and launch.process_start_ticks == witness.process_start_ticks
        )
    if not identity_matches or launch is None:
        raise NativeStreamError("STALE_EXECUTION", "Retired process identity changed")
    operation.death_witness = witness.model_dump(mode="json")
    empty_started_graph = False
    if authority.state == "ACTIVE":
        task_count = await session.scalar(
            sa.select(sa.func.count())
            .select_from(db.TaskRun)
            .where(db.TaskRun.flow_run_id == request.flow_run_id)
        )
        stream_count = await session.scalar(
            sa.select(sa.func.count())
            .select_from(db.StreamPartition)
            .where(db.StreamPartition.flow_run_id == request.flow_run_id)
        )
        submission_count = await session.scalar(
            sa.select(sa.func.count())
            .select_from(db.StreamSubmission)
            .where(db.StreamSubmission.flow_run_id == request.flow_run_id)
        )
        finite_count = await session.scalar(
            sa.select(sa.func.count())
            .select_from(db.StreamFiniteSubmission)
            .where(db.StreamFiniteSubmission.flow_run_id == request.flow_run_id)
        )
        empty_started_graph = (
            authority.manifest_set_digest is None
            and not task_count
            and not stream_count
            and not submission_count
            and not finite_count
        )
    flow_is_final = flow.state_type in {
        schemas.states.StateType.COMPLETED,
        schemas.states.StateType.FAILED,
        schemas.states.StateType.CANCELLED,
        schemas.states.StateType.CRASHED,
    }
    if authority.state == "TERMINAL":
        if not flow_is_final or operation.identity_kind != "CHILD":
            raise NativeStreamError(
                "TERMINAL_CONFLICT", "Terminal cleanup requires a final FlowRun child"
            )
    else:
        failed = schemas.states.Failed(message=request.error)
        if (
            authority.state == "CLAIMED"
            or operation.identity_kind == "LAUNCHER"
            or empty_started_graph
        ):
            failed.state_details.retriable = False
        flow_result = await flow_run_models.set_flow_run_state(
            session=session,
            flow_run_id=request.flow_run_id,
            state=failed,
            force=False,
            flow_policy=CoreFlowPolicy,
        )
        if flow_result.state is None or not flow_result.state.is_failed():
            raise NativeStreamError(
                "RECOVERY_NOT_READY", "FlowRun still has a permitted recovery attempt"
            )
    task_ids = set(
        (
            await session.scalars(
                sa.select(db.StreamSubmission.task_run_id).where(
                    db.StreamSubmission.flow_run_id == request.flow_run_id
                )
            )
        ).all()
    )
    task_ids.update(
        (
            await session.scalars(
                sa.select(db.StreamFiniteSubmission.task_run_id).where(
                    db.StreamFiniteSubmission.flow_run_id == request.flow_run_id
                )
            )
        ).all()
    )
    tasks = (
        await session.scalars(
            sa.select(db.TaskRun)
            .where(db.TaskRun.id.in_(task_ids))
            .order_by(db.TaskRun.id)
            .with_for_update()
        )
    ).all()
    for task in tasks if not flow_is_final else []:
        if task.state_type in {
            schemas.states.StateType.COMPLETED,
            schemas.states.StateType.FAILED,
            schemas.states.StateType.CANCELLED,
            schemas.states.StateType.CRASHED,
        }:
            continue
        task_failed = schemas.states.Failed(message=request.error)
        task_failed.state_details.retriable = False
        task_result = await task_run_models.set_task_run_state(
            session=session,
            task_run_id=task.id,
            state=task_failed,
            force=False,
            task_policy=CoreTaskPolicy,
        )
        if task_result.state is None or not task_result.state.is_failed():
            raise NativeStreamError(
                "TERMINAL_CONFLICT", "TaskRun retirement was not accepted"
            )
    authority.state = "TERMINAL"
    authority.source_admission_closed = True
    result = NativeStreamControlResult(
        flow_run_id=request.flow_run_id,
        operation_id=request.operation_id,
        generation=authority.generation,
        kind=expected_kind,
        source_admission_closed=True,
        cancel_requested=authority.cancel_requested,
        created_at=cast(DateTime, now("UTC")),
    )
    outcome = result.model_dump(mode="json")
    _, outcome_digest = _canonical(outcome)
    session.add(
        db.NativeStreamControlOperation(
            flow_run_id=request.flow_run_id,
            operation_id=request.operation_id,
            generation=authority.generation,
            kind=expected_kind,
            reason=request.error,
            request_digest=request_digest,
            outcome=outcome,
            outcome_digest=outcome_digest,
        )
    )
    await session.flush()
    return result


async def retire_unstarted_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: NativeStreamUnstartedRetireRequest,
    execution: StreamExecutionCredentials | None,
) -> NativeStreamControlResult:
    """Fail closed after an authenticated launcher could not start its child."""

    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native launch cleanup requires PostgreSQL 17"
        )
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    authority = await require_execution(
        session,
        db,
        request.flow_run_id,
        execution,
        allowed_states=("CLAIMED", "RECOVERING", "TERMINAL", "RELEASED"),
        update=True,
    )
    if authority is None:
        raise NativeStreamError("NOT_FOUND", "FlowRun is not enrolled")
    material = request.model_dump(mode="json") | {
        "schema": "native-stream-unstarted-retirement-v1"
    }
    _, request_digest = _canonical(material)
    prior = await session.scalar(
        sa.select(db.NativeStreamControlOperation)
        .where(
            db.NativeStreamControlOperation.flow_run_id == request.flow_run_id,
            db.NativeStreamControlOperation.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if prior is not None:
        if prior.kind != "SPAWN_FAILED" or prior.request_digest != request_digest:
            raise NativeStreamError("IDENTITY_CONFLICT", "Launch cleanup drift")
        return NativeStreamControlResult.model_validate(prior.outcome)
    if authority.state not in {"CLAIMED", "RECOVERING"}:
        raise NativeStreamError(
            "TERMINAL_CONFLICT", "Only an unstarted claimed launch may be retired"
        )
    launch = await session.scalar(
        sa.select(db.NativeStreamLaunchIdentity)
        .where(
            db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id,
            db.NativeStreamLaunchIdentity.generation == authority.generation,
            db.NativeStreamLaunchIdentity.enrollment_operation_id
            == request.enrollment_operation_id,
        )
        .with_for_update()
    )
    if launch is None or launch.state != "ENROLLED":
        raise NativeStreamError(
            "TERMINAL_CONFLICT", "A registered child cannot use unstarted cleanup"
        )
    manifest_count = await session.scalar(
        sa.select(sa.func.count())
        .select_from(db.StreamSubmission)
        .where(db.StreamSubmission.flow_run_id == request.flow_run_id)
    )
    finite_count = await session.scalar(
        sa.select(sa.func.count())
        .select_from(db.StreamFiniteSubmission)
        .where(db.StreamFiniteSubmission.flow_run_id == request.flow_run_id)
    )
    if authority.state == "CLAIMED" and (manifest_count or finite_count):
        raise NativeStreamError(
            "TERMINAL_CONFLICT", "Unstarted cleanup requires an empty graph"
        )
    failed = schemas.states.Failed(message=request.error)
    failed.state_details.retriable = False
    state_result = await flow_run_models.set_flow_run_state(
        session=session,
        flow_run_id=request.flow_run_id,
        state=failed,
        force=False,
        flow_policy=CoreFlowPolicy,
    )
    if state_result.state is None or not state_result.state.is_failed():
        raise NativeStreamError(
            "RECOVERY_POLICY_EXHAUSTED", "Launch cleanup did not finalize the FlowRun"
        )
    if authority.state == "RECOVERING":
        task_ids = set(
            (
                await session.scalars(
                    sa.select(db.StreamSubmission.task_run_id).where(
                        db.StreamSubmission.flow_run_id == request.flow_run_id
                    )
                )
            ).all()
        )
        task_ids.update(
            (
                await session.scalars(
                    sa.select(db.StreamFiniteSubmission.task_run_id).where(
                        db.StreamFiniteSubmission.flow_run_id == request.flow_run_id
                    )
                )
            ).all()
        )
        tasks = (
            await session.scalars(
                sa.select(db.TaskRun)
                .where(db.TaskRun.id.in_(task_ids))
                .order_by(db.TaskRun.id)
                .with_for_update()
            )
        ).all()
        for task in tasks:
            if task.state_type in {
                schemas.states.StateType.COMPLETED,
                schemas.states.StateType.FAILED,
                schemas.states.StateType.CANCELLED,
                schemas.states.StateType.CRASHED,
            }:
                continue
            task_failed = schemas.states.Failed(message=request.error)
            task_failed.state_details.retriable = False
            task_result = await task_run_models.set_task_run_state(
                session=session,
                task_run_id=task.id,
                state=task_failed,
                force=False,
                task_policy=CoreTaskPolicy,
            )
            if task_result.state is None or not task_result.state.is_failed():
                raise NativeStreamError(
                    "TERMINAL_CONFLICT", "TaskRun cleanup was not accepted"
                )
    authority.state = "TERMINAL"
    authority.source_admission_closed = True
    authority.cancel_requested = False
    launch.state = "RELEASED"
    result = NativeStreamControlResult(
        flow_run_id=request.flow_run_id,
        operation_id=request.operation_id,
        generation=authority.generation,
        kind="SPAWN_FAILED",
        source_admission_closed=True,
        cancel_requested=False,
        created_at=cast(DateTime, now("UTC")),
    )
    outcome = result.model_dump(mode="json")
    _, outcome_digest = _canonical(outcome)
    session.add(
        db.NativeStreamControlOperation(
            flow_run_id=request.flow_run_id,
            operation_id=request.operation_id,
            generation=authority.generation,
            kind="SPAWN_FAILED",
            reason=request.error,
            request_digest=request_digest,
            outcome=outcome,
            outcome_digest=outcome_digest,
        )
    )
    await session.flush()
    return result
