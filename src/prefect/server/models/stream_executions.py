"""Server authority for bounded native stream execution generations."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Iterable, Literal, cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server import schemas
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import flow_runs as flow_run_models
from prefect.server.models import task_runs as task_run_models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.orchestration.core_policy import CoreFlowPolicy, CoreTaskPolicy
from prefect.server.schemas.responses import SetStateStatus
from prefect.server.schemas.states import TERMINAL_STATES, State, StateType
from prefect.server.schemas.streams import (
    StreamExecutionBinding,
    StreamExecutionClaimRequest,
    StreamExecutionCredentials,
    StreamExecutionTakeoverRequest,
    StreamManifestSealRequest,
    StreamManifestSealResult,
    StreamRecoveryItem,
    StreamRecoveryPrepareRequest,
    StreamRecoveryResult,
    StreamUnsealedAbortItem,
    StreamUnsealedAbortRequest,
    StreamUnsealedAbortResult,
)
from prefect.types._datetime import DateTime, now

MAX_EXECUTION_GENERATIONS = 64


async def _require_backend(session: AsyncSession, db: PrefectDBInterface) -> None:
    if db.dialect.name != "postgresql":
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native streams require PostgreSQL 17"
        )
    connection = await session.connection()
    version = connection.dialect.server_version_info
    if not version or version[0] != 17:
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Native streams require PostgreSQL 17"
        )


def _canonical(value: Any) -> tuple[bytes, str]:
    encoded = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return encoded, hashlib.sha256(encoded).hexdigest()


def _secret_digest(value: str) -> str:
    return hashlib.sha256(bytes.fromhex(value)).hexdigest()


def _event_key_id(value: str) -> str:
    return hashlib.sha256(bytes.fromhex(value)).hexdigest()


def _recovery_outcome_digest(result: StreamRecoveryResult) -> str:
    _, digest = _canonical(
        {
            "schema": "native-stream-recovery-outcome-v1",
            "flow_run_id": str(result.flow_run_id),
            "generation": result.generation,
            "operation_id": str(result.operation_id),
            "items": [item.model_dump(mode="json") for item in result.items],
        }
    )
    return digest


def _claim_material(request: StreamExecutionClaimRequest) -> dict[str, Any]:
    return {
        "schema": "native-stream-execution-claim-v1",
        "flow_run_id": str(request.flow_run_id),
        "operation_id": str(request.operation_id),
        "expected_generation": request.expected_generation,
        "holder_id": str(request.holder_id),
        "holder_capability_digest": _secret_digest(request.holder_capability),
        "holder_event_public_key": request.holder_event_public_key,
        "supervisor_id": str(request.supervisor_id),
        "supervisor_capability_digest": _secret_digest(request.supervisor_capability),
        "protocol_version": request.protocol_version,
    }


def _takeover_material(request: StreamExecutionTakeoverRequest) -> dict[str, Any]:
    return {
        "schema": "native-stream-execution-takeover-v1",
        "flow_run_id": str(request.flow_run_id),
        "operation_id": str(request.operation_id),
        "expected_generation": request.expected_generation,
        "expected_holder_id": str(request.expected_holder_id),
        "supervisor_id": str(request.supervisor_id),
        "supervisor_capability_digest": _secret_digest(request.supervisor_capability),
        "new_holder_id": str(request.new_holder_id),
        "new_holder_capability_digest": _secret_digest(request.new_holder_capability),
        "new_holder_event_public_key": request.new_holder_event_public_key,
        "death_witness": request.death_witness.model_dump(mode="json"),
    }


def _binding_from_attempt(
    attempt: orm_models.StreamExecutionAttempt,
) -> StreamExecutionBinding:
    return StreamExecutionBinding(
        flow_run_id=attempt.flow_run_id,
        generation=attempt.generation,
        holder_id=attempt.holder_id,
        operation_id=attempt.operation_id,
        state=cast(
            Literal["CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL", "RELEASED"],
            attempt.state,
        ),
        flow_run_run_count=attempt.flow_run_run_count,
        created_at=attempt.created,
    )


async def _attempt_for_operation(
    session: AsyncSession,
    db: PrefectDBInterface,
    flow_run_id: UUID,
    operation_id: UUID,
) -> orm_models.StreamExecutionAttempt | None:
    return await session.scalar(
        sa.select(db.StreamExecutionAttempt)
        .where(
            db.StreamExecutionAttempt.flow_run_id == flow_run_id,
            db.StreamExecutionAttempt.operation_id == operation_id,
        )
        .with_for_update()
    )


async def claim_execution(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: StreamExecutionClaimRequest,
) -> StreamExecutionBinding:
    """Claim generation one before the trusted launcher starts its child."""

    await _require_backend(session, db)

    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    _, request_digest = _canonical(_claim_material(request))
    prior = await _attempt_for_operation(
        session, db, request.flow_run_id, request.operation_id
    )
    if prior is not None:
        if prior.kind != "CLAIM" or prior.request_digest != request_digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Execution operation identity drift"
            )
        return _binding_from_attempt(prior)

    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    if authority is not None:
        raise NativeStreamError("STALE_EXECUTION", "FlowRun is already enrolled")
    if flow.state_type in TERMINAL_STATES or flow.state_type not in {
        StateType.PENDING,
        StateType.SCHEDULED,
    }:
        raise NativeStreamError(
            "RECOVERY_NOT_READY",
            "Execution claim must precede the child process Running transition",
        )
    retries = flow.empirical_policy.retries
    if retries is None or retries < 1:
        raise NativeStreamError(
            "PROTOCOL_ERROR", "Recoverable execution requires FlowRun retries"
        )

    capability_digest = _secret_digest(request.holder_capability)
    supervisor_digest = _secret_digest(request.supervisor_capability)
    key_id = _event_key_id(request.holder_event_public_key)
    authority = db.StreamExecutionAuthority(
        flow_run_id=request.flow_run_id,
        generation=1,
        prepared_generation=0,
        holder_id=request.holder_id,
        holder_capability_digest=capability_digest,
        holder_event_public_key=request.holder_event_public_key,
        event_key_id=key_id,
        supervisor_id=request.supervisor_id,
        supervisor_capability_digest=supervisor_digest,
        state="CLAIMED",
        flow_run_run_count=flow.run_count,
    )
    session.add(authority)
    await session.flush()
    attempt = db.StreamExecutionAttempt(
        flow_run_id=request.flow_run_id,
        generation=1,
        operation_id=request.operation_id,
        kind="CLAIM",
        request_digest=request_digest,
        outcome_digest="0" * 64,
        holder_id=request.holder_id,
        holder_event_public_key=request.holder_event_public_key,
        event_key_id=key_id,
        state="CLAIMED",
        flow_run_run_count=flow.run_count,
    )
    session.add(attempt)
    await session.flush()
    binding = _binding_from_attempt(attempt)
    _, attempt.outcome_digest = _canonical(binding.model_dump(mode="json"))
    await session.flush()
    return binding


async def takeover_execution(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: StreamExecutionTakeoverRequest,
) -> StreamExecutionBinding:
    """Fence a confirmed-dead child and govern its FlowRun into retry."""

    await _require_backend(session, db)

    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update()
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    _, request_digest = _canonical(_takeover_material(request))
    prior = await _attempt_for_operation(
        session, db, request.flow_run_id, request.operation_id
    )
    if prior is not None:
        if prior.kind != "TAKEOVER" or prior.request_digest != request_digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Execution operation identity drift"
            )
        return _binding_from_attempt(prior)

    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == request.flow_run_id)
        .with_for_update()
    )
    if authority is None:
        raise NativeStreamError("NOT_FOUND", "FlowRun is not enrolled")
    supplied_supervisor = _secret_digest(request.supervisor_capability)
    if authority.supervisor_id != request.supervisor_id or not hmac.compare_digest(
        authority.supervisor_capability_digest, supplied_supervisor
    ):
        raise NativeStreamError("TAKEOVER_NOT_AUTHORIZED", "Takeover is not authorized")
    if (
        authority.generation != request.expected_generation
        or authority.holder_id != request.expected_holder_id
        or authority.state != "ACTIVE"
    ):
        raise NativeStreamError("STALE_EXECUTION", "Execution generation changed")
    if (
        authority.manifest_set_digest is None
        or authority.manifest_count is None
        or authority.manifest_sealed_at is None
        or authority.manifest_seal_operation_id is None
        or authority.manifest_seal_request_digest is None
    ):
        raise NativeStreamError(
            "MANIFEST_NOT_SEALED",
            "Process takeover requires the complete immutable stream manifest",
        )
    if authority.generation >= MAX_EXECUTION_GENERATIONS:
        raise NativeStreamError(
            "RECOVERY_HORIZON_EXPIRED", "Execution generation limit is exhausted"
        )
    if flow.state_type != StateType.RUNNING:
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Takeover requires the abandoned Running FlowRun"
        )

    result = await flow_run_models.set_flow_run_state(
        session=session,
        flow_run_id=request.flow_run_id,
        state=schemas.states.Failed(message="Confirmed execution process loss"),
        force=False,
        flow_policy=CoreFlowPolicy,
    )
    if (
        result.status != SetStateStatus.REJECT
        or result.state is None
        or result.state.name != "AwaitingRetry"
    ):
        raise NativeStreamError(
            "RECOVERY_POLICY_EXHAUSTED",
            "FlowRun retry policy did not produce AwaitingRetry",
        )

    new_generation = authority.generation + 1
    key_id = _event_key_id(request.new_holder_event_public_key)
    authority.generation = new_generation
    authority.holder_id = request.new_holder_id
    authority.holder_capability_digest = _secret_digest(request.new_holder_capability)
    authority.holder_event_public_key = request.new_holder_event_public_key
    authority.event_key_id = key_id
    authority.state = "RECOVERING"
    authority.flow_run_run_count = flow.run_count
    attempt = db.StreamExecutionAttempt(
        flow_run_id=request.flow_run_id,
        generation=new_generation,
        operation_id=request.operation_id,
        kind="TAKEOVER",
        request_digest=request_digest,
        outcome_digest="0" * 64,
        holder_id=request.new_holder_id,
        holder_event_public_key=request.new_holder_event_public_key,
        event_key_id=key_id,
        previous_generation=request.expected_generation,
        previous_holder_id=request.expected_holder_id,
        death_witness=request.death_witness.model_dump(mode="json"),
        state="RECOVERING",
        flow_run_run_count=flow.run_count,
    )
    session.add(attempt)
    await session.flush()
    binding = _binding_from_attempt(attempt)
    _, attempt.outcome_digest = _canonical(binding.model_dump(mode="json"))
    await session.flush()
    return binding


async def require_execution(
    session: AsyncSession,
    db: PrefectDBInterface,
    flow_run_id: UUID,
    credentials: StreamExecutionCredentials | None,
    *,
    allowed_states: Iterable[str] = ("ACTIVE",),
    update: bool = False,
) -> orm_models.StreamExecutionAuthority | None:
    """Validate an enrolled FlowRun after its parent row has been locked."""

    query = sa.select(db.StreamExecutionAuthority).where(
        db.StreamExecutionAuthority.flow_run_id == flow_run_id
    )
    query = query.with_for_update() if update else query.with_for_update(read=True)
    authority = await session.scalar(query)
    if authority is None:
        if credentials is not None:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Execution binding supplied to unenrolled FlowRun"
            )
        return None
    if credentials is None:
        raise NativeStreamError(
            "EXECUTION_BINDING_REQUIRED", "Enrolled FlowRun requires execution binding"
        )
    supplied = _secret_digest(credentials.capability)
    if (
        authority.generation != credentials.generation
        or authority.holder_id != credentials.holder_id
        or not hmac.compare_digest(authority.holder_capability_digest, supplied)
    ):
        raise NativeStreamError("STALE_EXECUTION", "Execution binding is fenced")
    if authority.state not in set(allowed_states):
        code = (
            "RECOVERY_HORIZON_EXPIRED"
            if authority.state == "RELEASED"
            else "RECOVERY_NOT_READY"
        )
        raise NativeStreamError(code, "Execution generation is not active")
    return authority


async def guard_flow_lifecycle(
    session: AsyncSession,
    db: PrefectDBInterface,
    flow_run_id: UUID,
    credentials: StreamExecutionCredentials | None,
    proposed_state: State[Any],
    *,
    force: bool,
) -> orm_models.StreamExecutionAuthority | None:
    """Fence an enrolled FlowRun state proposal before orchestration."""

    flow = await session.scalar(
        sa.select(db.FlowRun).where(db.FlowRun.id == flow_run_id).with_for_update()
    )
    if flow is None:
        return None
    authority = await require_execution(
        session,
        db,
        flow_run_id,
        credentials,
        allowed_states=("CLAIMED", "ACTIVE", "RECOVERING"),
        update=True,
    )
    if authority is None:
        return None
    if force:
        raise NativeStreamError(
            "PROTOCOL_ERROR", "Enrolled FlowRun lifecycle cannot bypass policy"
        )
    if (
        proposed_state.is_running()
        and authority.state == "ACTIVE"
        and authority.manifest_set_digest is not None
        and flow.state_type != StateType.RUNNING
    ):
        raise NativeStreamError(
            "RECOVERY_NOT_READY",
            "A sealed execution requires an explicit newer-generation takeover",
        )
    if proposed_state.is_completed():
        if authority.prepared_generation != authority.generation:
            raise NativeStreamError(
                "RECOVERY_NOT_READY", "Execution generation is not prepared"
            )
        if authority.manifest_set_digest is None:
            raise NativeStreamError(
                "MANIFEST_NOT_SEALED",
                "Completed FlowRun requires its immutable stream manifest",
            )
        outputs = (
            await session.scalars(
                sa.select(db.StreamPartition)
                .join(
                    db.StreamSubmission,
                    db.StreamSubmission.stream_id == db.StreamPartition.id,
                )
                .where(
                    db.StreamSubmission.flow_run_id == flow_run_id,
                    db.StreamSubmission.state == "ACTIVE",
                )
                .order_by(db.StreamPartition.id)
                .with_for_update(read=True)
            )
        ).all()
        if len(outputs) != authority.manifest_count or any(
            output.state != "COMPLETED" for output in outputs
        ):
            raise NativeStreamError(
                "TERMINAL_CONFLICT",
                "Completed FlowRun requires EOF for every sealed stream",
            )
    if authority.state in {"CLAIMED", "RECOVERING"} and not proposed_state.is_running():
        raise NativeStreamError(
            "RECOVERY_NOT_READY",
            "Claimed or recovering execution must enter Running normally",
        )
    return authority


async def apply_flow_lifecycle_result(
    session: AsyncSession,
    db: PrefectDBInterface,
    authority: orm_models.StreamExecutionAuthority | None,
    result_state: State[Any] | None,
) -> None:
    if authority is None or result_state is None:
        return
    flow = await session.get(db.FlowRun, authority.flow_run_id)
    if flow is None:
        return
    if result_state.is_running():
        authority.state = "ACTIVE"
        authority.flow_run_run_count = flow.run_count
    elif result_state.is_final():
        authority.state = "TERMINAL"


async def guard_task_lifecycle(
    session: AsyncSession,
    db: PrefectDBInterface,
    task_run_id: UUID,
    credentials: StreamExecutionCredentials | None,
    proposed_state: State[Any] | None,
    *,
    force: bool,
) -> orm_models.StreamExecutionAuthority | None:
    """Acquire FlowRun/authority before an enrolled TaskRun state lock."""

    row = await session.execute(
        sa.select(db.TaskRun.flow_run_id).where(db.TaskRun.id == task_run_id)
    )
    flow_run_id = row.scalar_one_or_none()
    if flow_run_id is None:
        return None
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == flow_run_id)
        .with_for_update(read=True)
    )
    if flow is None:
        return None
    authority = await require_execution(
        session,
        db,
        flow_run_id,
        credentials,
        allowed_states=("ACTIVE",),
    )
    if authority is None:
        return None
    if force:
        raise NativeStreamError(
            "PROTOCOL_ERROR", "Enrolled TaskRun lifecycle cannot bypass policy"
        )
    if authority.manifest_set_digest is None:
        raise NativeStreamError(
            "MANIFEST_NOT_SEALED",
            "TaskRun lifecycle cannot start before its manifest is sealed",
        )
    if authority.prepared_generation != authority.generation:
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Execution generation is not prepared"
        )
    task = await session.scalar(
        sa.select(db.TaskRun).where(db.TaskRun.id == task_run_id).with_for_update()
    )
    if task is None:
        return authority
    manifest = await session.scalar(
        sa.select(db.StreamSubmission).where(
            db.StreamSubmission.flow_run_id == flow_run_id,
            db.StreamSubmission.task_run_id == task_run_id,
            db.StreamSubmission.state == "ACTIVE",
        )
    )
    if manifest is None:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Enrolled TaskRun is not bound to a manifest"
        )
    if proposed_state is not None and proposed_state.is_completed():
        output = await session.scalar(
            sa.select(db.StreamPartition)
            .where(db.StreamPartition.id == manifest.stream_id)
            .with_for_update(read=True)
        )
        if (
            output is None
            or output.state != "COMPLETED"
            or output.producer_task_run_id != task_run_id
            or output.attempt != task.run_count
        ):
            raise NativeStreamError(
                "TERMINAL_CONFLICT",
                "TaskRun completion requires its exact durable stream EOF",
            )
    return authority


async def guard_task_creation(
    session: AsyncSession,
    db: PrefectDBInterface,
    flow_run_id: UUID | None,
    credentials: StreamExecutionCredentials | None,
) -> None:
    """Refuse HTTP TaskRun creation inside an enrolled native FlowRun."""

    if flow_run_id is None:
        return
    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == flow_run_id)
        .with_for_update(read=True)
    )
    if flow is None:
        return
    authority = await require_execution(
        session,
        db,
        flow_run_id,
        credentials,
        allowed_states=("CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL"),
    )
    if authority is not None:
        raise NativeStreamError(
            "UNSUPPORTED_TOPOLOGY",
            "Enrolled TaskRuns must be created by an atomic stream manifest",
        )


async def seal_manifest(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: StreamManifestSealRequest,
    credentials: StreamExecutionCredentials | None,
) -> StreamManifestSealResult:
    """Seal the complete ordered linear manifest before task admission."""

    await _require_backend(session, db)

    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update(read=True)
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    authority = await require_execution(
        session,
        db,
        request.flow_run_id,
        credentials,
        allowed_states=("CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL", "RELEASED"),
        update=True,
    )
    assert authority is not None
    witness_material = [item.model_dump(mode="json") for item in request.manifests]
    _, request_digest = _canonical(
        {
            "schema": "native-stream-manifest-seal-v1",
            "flow_run_id": str(request.flow_run_id),
            "operation_id": str(request.operation_id),
            "manifests": witness_material,
        }
    )
    if authority.manifest_set_digest is not None:
        if (
            authority.manifest_seal_operation_id != request.operation_id
            or authority.manifest_seal_request_digest != request_digest
        ):
            raise NativeStreamError("IDENTITY_CONFLICT", "Manifest seal is immutable")
        return StreamManifestSealResult(
            flow_run_id=request.flow_run_id,
            generation=authority.generation,
            operation_id=request.operation_id,
            manifest_set_digest=authority.manifest_set_digest,
            manifest_count=cast(int, authority.manifest_count),
        )
    if authority.state not in {"CLAIMED", "ACTIVE"}:
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Manifest can only be sealed before recovery begins"
        )

    keys = [item.submission_key for item in request.manifests]
    if len(set(keys)) != len(keys):
        raise NativeStreamError("IDENTITY_CONFLICT", "Manifest keys must be unique")
    rows = (
        await session.scalars(
            sa.select(db.StreamSubmission)
            .where(db.StreamSubmission.flow_run_id == request.flow_run_id)
            .order_by(db.StreamSubmission.submission_key)
            .with_for_update()
        )
    ).all()
    if len(rows) != len(request.manifests) or any(
        row.state != "ACTIVE" for row in rows
    ):
        raise NativeStreamError(
            "MANIFEST_NOT_SEALED",
            "Seal must name every active submission exactly once",
        )
    by_key = {row.submission_key: row for row in rows}
    ordered: list[orm_models.StreamSubmission] = []
    for witness in request.manifests:
        row = by_key.get(witness.submission_key)
        if row is None or row.manifest_digest != witness.manifest_digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Manifest witness does not match storage"
            )
        ordered.append(row)
    if ordered[0].role != "SOURCE" or ordered[0].upstream_stream_id is not None:
        raise NativeStreamError(
            "UNSUPPORTED_TOPOLOGY", "The sealed linear graph must begin at its source"
        )
    for previous, current in zip(ordered, ordered[1:]):
        if (
            current.role != "OPERATOR"
            or current.upstream_stream_id != previous.stream_id
        ):
            raise NativeStreamError(
                "UNSUPPORTED_TOPOLOGY",
                "Manifest order must follow the complete linear stream topology",
            )
    task_rows = (
        await session.execute(
            sa.select(db.TaskRun.id, db.TaskRun.state_type, db.TaskRun.run_count).where(
                db.TaskRun.id.in_([row.task_run_id for row in ordered])
            )
        )
    ).all()
    if len(task_rows) != len(ordered) or any(
        state_type != StateType.PENDING or run_count != 0
        for _, state_type, run_count in task_rows
    ):
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Manifest must be sealed before any TaskRun starts"
        )

    _, set_digest = _canonical(
        {
            "schema": "native-stream-manifest-set-v1",
            "flow_run_id": str(request.flow_run_id),
            "manifests": witness_material,
        }
    )
    authority.manifest_set_digest = set_digest
    authority.manifest_count = len(ordered)
    authority.manifest_sealed_at = cast(DateTime, now("UTC"))
    authority.manifest_seal_operation_id = request.operation_id
    authority.manifest_seal_request_digest = request_digest
    authority.prepared_generation = authority.generation
    await session.flush()
    return StreamManifestSealResult(
        flow_run_id=request.flow_run_id,
        generation=authority.generation,
        operation_id=request.operation_id,
        manifest_set_digest=set_digest,
        manifest_count=len(ordered),
    )


async def prepare_recovery(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: StreamRecoveryPrepareRequest,
    credentials: StreamExecutionCredentials | None,
) -> StreamRecoveryResult:
    """Reconcile a fenced generation through normal TaskRun policy transitions."""

    await _require_backend(session, db)

    flow = await session.scalar(
        sa.select(db.FlowRun)
        .where(db.FlowRun.id == request.flow_run_id)
        .with_for_update(read=True)
    )
    if flow is None:
        raise NativeStreamError("NOT_FOUND", "Unknown FlowRun")
    authority = await require_execution(
        session,
        db,
        request.flow_run_id,
        credentials,
        allowed_states=("RECOVERING", "ACTIVE", "TERMINAL"),
        update=True,
    )
    assert authority is not None
    if authority.manifest_set_digest is None:
        raise NativeStreamError(
            "MANIFEST_NOT_SEALED",
            "Whole-worker recovery requires an immutable manifest seal",
        )
    _, supplied_manifest_digest = _canonical(
        {
            "schema": "native-stream-manifest-set-v1",
            "flow_run_id": str(request.flow_run_id),
            "manifests": [item.model_dump(mode="json") for item in request.manifests],
        }
    )
    if supplied_manifest_digest != authority.manifest_set_digest:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Recovery manifest differs from the sealed graph"
        )
    _, request_digest = _canonical(
        {
            "schema": "native-stream-recovery-prepare-v1",
            "flow_run_id": str(request.flow_run_id),
            "generation": authority.generation,
            "operation_id": str(request.operation_id),
            "manifest_set_digest": supplied_manifest_digest,
            "manifests": [item.model_dump(mode="json") for item in request.manifests],
        }
    )
    attempt = await session.scalar(
        sa.select(db.StreamExecutionAttempt)
        .where(
            db.StreamExecutionAttempt.flow_run_id == request.flow_run_id,
            db.StreamExecutionAttempt.generation == authority.generation,
        )
        .with_for_update()
    )
    if attempt is None:
        raise NativeStreamError("NOT_FOUND", "Execution attempt is missing")
    prior_operation = await session.scalar(
        sa.select(db.StreamExecutionAttempt)
        .where(
            db.StreamExecutionAttempt.flow_run_id == request.flow_run_id,
            db.StreamExecutionAttempt.recovery_operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if prior_operation is not None and prior_operation.id != attempt.id:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Recovery operation belongs to another generation"
        )
    if attempt.recovery_operation_id is not None:
        if (
            attempt.recovery_operation_id != request.operation_id
            or attempt.recovery_request_digest != request_digest
            or attempt.recovery_outcome is None
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Recovery prepare is immutable"
            )
        prior_result = StreamRecoveryResult.model_validate(attempt.recovery_outcome)
        prior_digest = _recovery_outcome_digest(prior_result)
        if (
            prior_result.outcome_digest != prior_digest
            or attempt.recovery_outcome_digest != prior_digest
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Recovery outcome witness is inconsistent"
            )
        return prior_result
    if authority.state not in {"RECOVERING", "ACTIVE"}:
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Terminal execution cannot begin recovery"
        )
    if authority.prepared_generation == authority.generation:
        raise NativeStreamError(
            "IDENTITY_CONFLICT",
            "Execution generation already has a preparation witness",
        )
    if attempt.kind != "TAKEOVER":
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Initial execution is prepared by manifest sealing"
        )

    submissions = (
        await session.scalars(
            sa.select(db.StreamSubmission)
            .where(
                db.StreamSubmission.flow_run_id == request.flow_run_id,
                db.StreamSubmission.state == "ACTIVE",
            )
            .order_by(db.StreamSubmission.task_run_id)
            .with_for_update()
        )
    ).all()
    by_key = {row.submission_key: row for row in submissions}
    if len(submissions) != len(request.manifests):
        raise NativeStreamError("IDENTITY_CONFLICT", "Recovery manifest is incomplete")
    ordered_submissions: list[orm_models.StreamSubmission] = []
    for witness in request.manifests:
        submission = by_key.get(witness.submission_key)
        if submission is None or submission.manifest_digest != witness.manifest_digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Recovery manifest differs from storage"
            )
        ordered_submissions.append(submission)

    tasks = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.TaskRun)
                .where(
                    db.TaskRun.id.in_(
                        [submission.task_run_id for submission in submissions]
                    )
                )
                .order_by(db.TaskRun.id)
                .with_for_update()
            )
        ).all()
    }
    streams = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamPartition)
                .where(
                    db.StreamPartition.id.in_(
                        [submission.stream_id for submission in submissions]
                    )
                )
                .order_by(db.StreamPartition.id)
                .with_for_update(read=True)
            )
        ).all()
    }
    if len(tasks) != len(submissions) or len(streams) != len(submissions):
        raise NativeStreamError("IDENTITY_CONFLICT", "Recovery graph is incomplete")

    items: list[StreamRecoveryItem] = []
    for submission in ordered_submissions:
        task = tasks[submission.task_run_id]
        stream = streams[submission.stream_id]
        if stream.state == "COMPLETED":
            if (
                stream.producer_task_run_id != task.id
                or stream.attempt != task.run_count
            ):
                raise NativeStreamError(
                    "TERMINAL_CONFLICT",
                    "Durable stream EOF belongs to another TaskRun attempt",
                )
            if task.state_type != StateType.COMPLETED:
                completed = await task_run_models.set_task_run_state(
                    session=session,
                    task_run_id=task.id,
                    state=schemas.states.Completed(
                        message="Recovered from durable native stream EOF"
                    ),
                    force=False,
                    task_policy=CoreTaskPolicy,
                )
                if (
                    completed.status != SetStateStatus.ACCEPT
                    or completed.state is None
                    or not completed.state.is_completed()
                ):
                    raise NativeStreamError(
                        "RECOVERY_POLICY_EXHAUSTED",
                        "TaskRun policy did not reconcile durable stream EOF",
                    )
                await session.refresh(task)
            next_run_count = max(1, task.run_count)
        else:
            if stream.state != "OPEN":
                raise NativeStreamError(
                    "TERMINAL_CONFLICT", "Failed stream cannot be resumed"
                )
            if task.state_type == StateType.RUNNING:
                failed = await task_run_models.set_task_run_state(
                    session=session,
                    task_run_id=task.id,
                    state=schemas.states.Failed(
                        message="Confirmed execution process loss"
                    ),
                    force=False,
                    task_policy=CoreTaskPolicy,
                )
                if (
                    failed.status != SetStateStatus.REJECT
                    or failed.state is None
                    or failed.state.name != "AwaitingRetry"
                ):
                    raise NativeStreamError(
                        "RECOVERY_POLICY_EXHAUSTED",
                        "TaskRun retry policy did not produce AwaitingRetry",
                    )
                await session.refresh(task)
            elif task.state_type == StateType.COMPLETED:
                raise NativeStreamError(
                    "TERMINAL_CONFLICT", "Completed TaskRun has no durable stream EOF"
                )
            elif task.state_type not in {StateType.PENDING, StateType.SCHEDULED}:
                raise NativeStreamError(
                    "RECOVERY_NOT_READY", "TaskRun is not recoverable"
                )
            next_run_count = task.run_count + 1
        items.append(
            StreamRecoveryItem(
                submission_key=submission.submission_key,
                task_run_id=task.id,
                stream_id=stream.id,
                durable_position=stream.producer_position,
                stream_state=cast(Literal["OPEN", "COMPLETED", "FAILED"], stream.state),
                task_state=cast(str, task.state_name),
                current_run_count=task.run_count,
                next_run_count=next_run_count,
            )
        )

    unsigned_result = StreamRecoveryResult(
        flow_run_id=request.flow_run_id,
        generation=authority.generation,
        operation_id=request.operation_id,
        items=items,
        outcome_digest="0" * 64,
    )
    outcome_digest = _recovery_outcome_digest(unsigned_result)
    result = unsigned_result.model_copy(update={"outcome_digest": outcome_digest})
    attempt.recovery_operation_id = request.operation_id
    attempt.recovery_request_digest = request_digest
    attempt.recovery_outcome = result.model_dump(mode="json")
    attempt.recovery_outcome_digest = outcome_digest
    authority.prepared_generation = authority.generation
    await session.flush()
    return result


def _abort_outcome_digest(result: StreamUnsealedAbortResult) -> str:
    _, digest = _canonical(
        {
            "schema": "native-stream-unsealed-abort-outcome-v1",
            "flow_run_id": str(result.flow_run_id),
            "generation": result.generation,
            "operation_id": str(result.operation_id),
            "state": result.state,
            "items": [item.model_dump(mode="json") for item in result.items],
        }
    )
    return digest


async def abort_unsealed_execution(
    session: AsyncSession,
    db: PrefectDBInterface,
    request: StreamUnsealedAbortRequest,
    credentials: StreamExecutionCredentials | None,
) -> StreamUnsealedAbortResult:
    """Atomically close a terminal, unsealed and entirely unstarted graph."""

    await _require_backend(session, db)
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
        credentials,
        allowed_states=("TERMINAL", "RELEASED"),
        update=True,
    )
    assert authority is not None
    _, request_digest = _canonical(
        {
            "schema": "native-stream-unsealed-abort-v1",
            "flow_run_id": str(request.flow_run_id),
            "generation": authority.generation,
            "operation_id": str(request.operation_id),
            "manifests": [item.model_dump(mode="json") for item in request.manifests],
            "error": request.error,
        }
    )
    attempt = await session.scalar(
        sa.select(db.StreamExecutionAttempt)
        .where(
            db.StreamExecutionAttempt.flow_run_id == request.flow_run_id,
            db.StreamExecutionAttempt.generation == authority.generation,
        )
        .with_for_update()
    )
    if attempt is None:
        raise NativeStreamError("NOT_FOUND", "Execution attempt is missing")
    prior_operation = await session.scalar(
        sa.select(db.StreamExecutionAttempt)
        .where(
            db.StreamExecutionAttempt.flow_run_id == request.flow_run_id,
            db.StreamExecutionAttempt.abort_operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if prior_operation is not None and prior_operation.id != attempt.id:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Abort operation belongs to another generation"
        )
    if attempt.abort_operation_id is not None:
        if (
            attempt.abort_operation_id != request.operation_id
            or attempt.abort_request_digest != request_digest
            or attempt.abort_outcome is None
        ):
            raise NativeStreamError("IDENTITY_CONFLICT", "Unsealed abort is immutable")
        prior_result = StreamUnsealedAbortResult.model_validate(attempt.abort_outcome)
        prior_digest = _abort_outcome_digest(prior_result)
        if (
            prior_result.outcome_digest != prior_digest
            or attempt.abort_outcome_digest != prior_digest
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Abort outcome witness is inconsistent"
            )
        return prior_result
    if authority.state != "TERMINAL" or flow.state_type not in TERMINAL_STATES:
        raise NativeStreamError(
            "RECOVERY_NOT_READY", "Only a terminal FlowRun can abort an unsealed graph"
        )
    if (
        authority.manifest_set_digest is not None
        or authority.manifest_count is not None
        or authority.prepared_generation != 0
    ):
        raise NativeStreamError(
            "MANIFEST_SEALED", "A sealed graph cannot use the unsealed abort path"
        )

    submissions = (
        await session.scalars(
            sa.select(db.StreamSubmission)
            .where(
                db.StreamSubmission.flow_run_id == request.flow_run_id,
                db.StreamSubmission.state == "ACTIVE",
            )
            .order_by(db.StreamSubmission.task_run_id)
            .with_for_update()
        )
    ).all()
    by_key = {row.submission_key: row for row in submissions}
    if len(request.manifests) != len(submissions) or len(by_key) != len(submissions):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Abort must name the complete unsealed manifest set"
        )
    ordered: list[orm_models.StreamSubmission] = []
    for witness in request.manifests:
        row = by_key.get(witness.submission_key)
        if row is None or row.manifest_digest != witness.manifest_digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Abort manifest witness differs from storage"
            )
        ordered.append(row)
    if ordered:
        if ordered[0].role != "SOURCE" or ordered[0].upstream_stream_id is not None:
            raise NativeStreamError("UNSUPPORTED_TOPOLOGY", "Abort graph is not linear")
        for previous, current in zip(ordered, ordered[1:]):
            if (
                current.role != "OPERATOR"
                or current.upstream_stream_id != previous.stream_id
            ):
                raise NativeStreamError(
                    "UNSUPPORTED_TOPOLOGY",
                    "Abort graph is not an ordered linear prefix",
                )

    tasks = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.TaskRun)
                .where(db.TaskRun.id.in_([row.task_run_id for row in submissions]))
                .order_by(db.TaskRun.id)
                .with_for_update()
            )
        ).all()
    }
    streams = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamPartition)
                .where(
                    db.StreamPartition.id.in_([row.stream_id for row in submissions])
                )
                .order_by(db.StreamPartition.id)
                .with_for_update()
            )
        ).all()
    }
    if len(tasks) != len(submissions) or len(streams) != len(submissions):
        raise NativeStreamError("IDENTITY_CONFLICT", "Abort graph is incomplete")
    stream_ids = [row.stream_id for row in submissions]
    consumers = 0
    if stream_ids:
        consumers = cast(
            int,
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamConsumer)
                .where(
                    sa.or_(
                        db.StreamConsumer.input_stream_id.in_(stream_ids),
                        db.StreamConsumer.output_stream_id.in_(stream_ids),
                    )
                )
            ),
        )
    if consumers:
        raise NativeStreamError("TERMINAL_CONFLICT", "Abort graph has consumer state")
    for submission in submissions:
        task = tasks[submission.task_run_id]
        stream = streams[submission.stream_id]
        if task.state_type != StateType.PENDING or task.run_count != 0:
            raise NativeStreamError(
                "TERMINAL_CONFLICT", "Abort TaskRun already started"
            )
        if (
            stream.state != "OPEN"
            or stream.owner_id is not None
            or stream.owner_epoch != 0
            or stream.attempt != 0
            or stream.last_sequence != 0
            or stream.producer_position != 0
            or stream.record_count != 0
            or stream.byte_count != 0
        ):
            raise NativeStreamError(
                "TERMINAL_CONFLICT", "Abort stream already admitted work"
            )

    items: list[StreamUnsealedAbortItem] = []
    for submission in ordered:
        task = tasks[submission.task_run_id]
        result = await task_run_models.set_task_run_state(
            session=session,
            task_run_id=task.id,
            state=schemas.states.Failed(message=request.error),
            force=False,
            task_policy=CoreTaskPolicy,
        )
        if (
            result.status != SetStateStatus.ACCEPT
            or result.state is None
            or not result.state.is_failed()
        ):
            raise NativeStreamError(
                "RECOVERY_POLICY_EXHAUSTED", "Pending TaskRun did not fail terminally"
            )
        stream = streams[submission.stream_id]
        stream.state = "FAILED"
        stream.error = request.error
        stream.tail_sequence = stream.last_sequence
        items.append(
            StreamUnsealedAbortItem(
                submission_key=submission.submission_key,
                task_run_id=task.id,
                stream_id=stream.id,
            )
        )
    unsigned = StreamUnsealedAbortResult(
        flow_run_id=request.flow_run_id,
        generation=authority.generation,
        operation_id=request.operation_id,
        state="TERMINAL",
        items=items,
        outcome_digest="0" * 64,
    )
    digest = _abort_outcome_digest(unsigned)
    result = unsigned.model_copy(update={"outcome_digest": digest})
    attempt.abort_operation_id = request.operation_id
    attempt.abort_request_digest = request_digest
    attempt.abort_outcome = result.model_dump(mode="json")
    attempt.abort_outcome_digest = digest
    await session.flush()
    return result
