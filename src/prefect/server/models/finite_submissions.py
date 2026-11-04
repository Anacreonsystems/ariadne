"""Bounded finite Task authority for mixed native stream executions."""

from __future__ import annotations

import hashlib
from typing import Any, Literal, cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect._internal.uuid7 import uuid7
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import task_runs as task_run_models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models._stream_receipts import (
    canonical_json_digest,
    finite_receipt_outcome_digest,
)
from prefect.server.models.stream_executions import require_execution
from prefect.server.models.streams import (
    MAX_SUBMISSION_BYTES_PER_FLOW,
    MAX_SUBMISSION_CANONICAL_BYTES,
    MAX_SUBMISSION_PARAMETER_BYTES,
    NativeStreamStore,
    _canonical,
)
from prefect.server.schemas import core
from prefect.server.schemas.states import StateType
from prefect.server.schemas.streams import (
    StreamExecutionCredentials,
    StreamFiniteCompleteRequest,
    StreamFiniteLookupRequest,
    StreamFiniteManifest,
    StreamFiniteReceipt,
    StreamFiniteReserveRequest,
)
from prefect.types._datetime import now

MAX_FINITE_RESULT_BYTES = 64 * 1024


def _manifest_response(
    manifest: orm_models.StreamFiniteSubmission, *, completed: bool
) -> StreamFiniteManifest:
    return StreamFiniteManifest(
        flow_run_id=manifest.flow_run_id,
        phase=cast(Literal["PREFIX", "TAIL"], manifest.phase),
        submission_key=manifest.submission_key,
        manifest_digest=manifest.manifest_digest,
        task_run_id=manifest.task_run_id,
        dynamic_key=manifest.dynamic_key,
        state=cast(Literal["ACTIVE", "RELEASED"], manifest.state),
        result_state="COMPLETED" if completed else "PENDING",
        created_at=manifest.created,
    )


def _receipt_response(
    manifest: orm_models.StreamFiniteSubmission,
    receipt: orm_models.StreamFiniteReceipt,
) -> StreamFiniteReceipt:
    result_size, result_digest = canonical_json_digest(receipt.result)
    expected_digest = finite_receipt_outcome_digest(
        flow_run_id=manifest.flow_run_id,
        phase=manifest.phase,
        submission_key=manifest.submission_key,
        manifest_digest=manifest.manifest_digest,
        task_run_id=manifest.task_run_id,
        operation_id=receipt.operation_id,
        run_count=receipt.run_count,
        result_digest=receipt.result_digest,
    )
    if (
        receipt.result_size != result_size
        or receipt.result_digest != result_digest
        or receipt.outcome_digest != expected_digest
    ):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Finite result receipt witness is inconsistent"
        )
    return StreamFiniteReceipt(
        flow_run_id=manifest.flow_run_id,
        phase=cast(Literal["PREFIX", "TAIL"], manifest.phase),
        submission_key=manifest.submission_key,
        task_run_id=manifest.task_run_id,
        operation_id=receipt.operation_id,
        run_count=receipt.run_count,
        result=receipt.result,
        result_digest=receipt.result_digest,
        outcome_digest=receipt.outcome_digest,
        created_at=receipt.created,
    )


class NativeFiniteStore:
    """Finite Task manifests and immutable result receipts."""

    def __init__(
        self,
        db: PrefectDBInterface,
        execution: StreamExecutionCredentials | None = None,
    ) -> None:
        self.db = db
        self.execution = execution
        self.streams = NativeStreamStore(db, execution)

    async def _existing_receipt(
        self, session: AsyncSession, submission_id: UUID
    ) -> orm_models.StreamFiniteReceipt | None:
        return await session.scalar(
            sa.select(self.db.StreamFiniteReceipt)
            .where(self.db.StreamFiniteReceipt.finite_submission_id == submission_id)
            .with_for_update()
        )

    def _material(
        self, request: StreamFiniteReserveRequest
    ) -> tuple[dict[str, Any], bytes, str, str, list[str]]:
        parameters, _, parameters_digest = _canonical(
            request.parameters, MAX_SUBMISSION_PARAMETER_BYTES
        )
        if not isinstance(parameters, dict):
            raise NativeStreamError(
                "PROTOCOL_ERROR", "Finite Task parameters must be a JSON object"
            )
        tags = sorted(set(request.tags))
        material = {
            "schema": "native-finite-submission-v1",
            "flow_run_id": str(request.flow_run_id),
            "phase": request.phase,
            "submission_key": request.submission_key,
            "task_name": request.task_name,
            "task_key": request.task_key,
            "artifact_digest": request.artifact_digest,
            "task_version": request.task_version,
            "flow_version": request.flow_version,
            "deployment_id": (
                str(request.deployment_id) if request.deployment_id else None
            ),
            "deployment_version": request.deployment_version,
            "parameters": parameters,
            "task_policy": request.task_policy.model_dump(mode="json"),
            "tags": tags,
            "labels": request.labels,
        }
        _, encoded, request_digest = _canonical(
            material, MAX_SUBMISSION_CANONICAL_BYTES
        )
        return (
            cast(dict[str, Any], parameters),
            encoded,
            request_digest,
            parameters_digest,
            tags,
        )

    async def reserve(
        self, request: StreamFiniteReserveRequest
    ) -> StreamFiniteManifest:
        parameters, encoded, request_digest, parameters_digest, tags = self._material(
            request
        )
        async with self.streams._session() as session:
            flow = await self.streams._flow(
                session,
                request.flow_run_id,
                update=True,
                allowed_execution_states=("ACTIVE",),
            )
            authority = await require_execution(
                session,
                self.db,
                request.flow_run_id,
                self.execution,
                allowed_states=("ACTIVE",),
                update=True,
            )
            assert authority is not None
            existing = await session.scalar(
                sa.select(self.db.StreamFiniteSubmission)
                .where(
                    self.db.StreamFiniteSubmission.flow_run_id == request.flow_run_id,
                    self.db.StreamFiniteSubmission.submission_key
                    == request.submission_key,
                )
                .with_for_update()
            )
            if existing is not None:
                if existing.request_digest != request_digest:
                    raise NativeStreamError(
                        "IDENTITY_CONFLICT",
                        "Finite submission key is bound to another manifest",
                    )
                if existing.state == "RELEASED":
                    raise NativeStreamError(
                        "REPLAY_HORIZON_EXPIRED",
                        "Finite submission payload has been released",
                    )
                receipt = await self._existing_receipt(session, existing.id)
                return _manifest_response(existing, completed=receipt is not None)

            if flow.state_type != StateType.RUNNING:
                raise NativeStreamError(
                    "RECOVERY_NOT_READY",
                    "Finite Task reservation requires Running FlowRun",
                )
            if (
                flow.flow_version != request.flow_version
                or flow.deployment_id != request.deployment_id
                or flow.deployment_version != request.deployment_version
            ):
                raise NativeStreamError(
                    "IDENTITY_CONFLICT",
                    "Finite submission version differs from the authoritative FlowRun",
                )
            retries = request.task_policy.retries
            if retries is None or retries < 1:
                raise NativeStreamError(
                    "PROTOCOL_ERROR", "Recoverable finite Tasks require retries"
                )

            prior_phase = await session.scalar(
                sa.select(self.db.StreamFiniteSubmission.id).where(
                    self.db.StreamFiniteSubmission.flow_run_id == request.flow_run_id,
                    self.db.StreamFiniteSubmission.phase == request.phase,
                )
            )
            if prior_phase is not None:
                raise NativeStreamError(
                    "UNSUPPORTED_TOPOLOGY",
                    f"The first mixed profile allows one {request.phase.lower()}",
                )

            stream_count = await session.scalar(
                sa.select(sa.func.count())
                .select_from(self.db.StreamSubmission)
                .where(self.db.StreamSubmission.flow_run_id == request.flow_run_id)
            )
            if request.phase == "PREFIX":
                if stream_count or authority.manifest_set_digest is not None:
                    raise NativeStreamError(
                        "UNSUPPORTED_TOPOLOGY",
                        "Finite prefix must precede every stream declaration",
                    )
                if authority.finite_prefix_manifest_digest is not None:
                    raise NativeStreamError(
                        "IDENTITY_CONFLICT", "Finite prefix anchor already exists"
                    )
            else:
                if authority.manifest_set_digest is None:
                    raise NativeStreamError(
                        "MANIFEST_NOT_SEALED",
                        "Finite tail requires the sealed stream graph",
                    )
                if authority.prepared_generation != authority.generation:
                    raise NativeStreamError(
                        "RECOVERY_NOT_READY", "Execution generation is not prepared"
                    )
                prefix = await session.scalar(
                    sa.select(self.db.StreamFiniteSubmission)
                    .where(
                        self.db.StreamFiniteSubmission.flow_run_id
                        == request.flow_run_id,
                        self.db.StreamFiniteSubmission.phase == "PREFIX",
                        self.db.StreamFiniteSubmission.state == "ACTIVE",
                    )
                    .with_for_update(read=True)
                )
                if (
                    prefix is None
                    or await self._existing_receipt(session, prefix.id) is None
                ):
                    raise NativeStreamError(
                        "RECOVERY_NOT_READY", "Finite prefix result is not durable"
                    )
                streams = (
                    await session.scalars(
                        sa.select(self.db.StreamPartition)
                        .where(
                            self.db.StreamPartition.flow_run_id == request.flow_run_id
                        )
                        .order_by(self.db.StreamPartition.id)
                        .with_for_update(read=True)
                    )
                ).all()
                consumers = (
                    await session.scalars(
                        sa.select(self.db.StreamConsumer)
                        .where(
                            self.db.StreamConsumer.flow_run_id == request.flow_run_id
                        )
                        .order_by(self.db.StreamConsumer.id)
                        .with_for_update(read=True)
                    )
                ).all()
                if not streams or any(
                    stream.state != "COMPLETED" for stream in streams
                ):
                    raise NativeStreamError(
                        "TERMINAL_CONFLICT", "Finite tail requires every stream EOF"
                    )
                if any(
                    consumer.inflight_sequence is not None for consumer in consumers
                ):
                    raise NativeStreamError(
                        "TERMINAL_CONFLICT", "Finite tail requires zero in-flight work"
                    )

            finite_bytes = await session.scalar(
                sa.select(
                    sa.func.coalesce(
                        sa.func.sum(self.db.StreamFiniteSubmission.canonical_size), 0
                    )
                ).where(
                    self.db.StreamFiniteSubmission.flow_run_id == request.flow_run_id
                )
            )
            stream_bytes = await session.scalar(
                sa.select(
                    sa.func.coalesce(
                        sa.func.sum(self.db.StreamSubmission.canonical_size), 0
                    )
                ).where(self.db.StreamSubmission.flow_run_id == request.flow_run_id)
            )
            if (finite_bytes or 0) + (stream_bytes or 0) + len(
                encoded
            ) > MAX_SUBMISSION_BYTES_PER_FLOW:
                raise NativeStreamError(
                    "CAPACITY", "FlowRun manifest metadata capacity is exhausted"
                )

            dynamic_key = (
                "finite-"
                + hashlib.sha256(request.submission_key.encode("utf-8")).hexdigest()[
                    :32
                ]
            )
            collision = await session.scalar(
                sa.select(self.db.TaskRun.id).where(
                    self.db.TaskRun.flow_run_id == request.flow_run_id,
                    self.db.TaskRun.task_key == request.task_key,
                    self.db.TaskRun.dynamic_key == dynamic_key,
                )
            )
            if collision is not None:
                raise NativeStreamError(
                    "IDENTITY_CONFLICT",
                    "Finite TaskRun identity exists without its manifest",
                )
            task_run_id = uuid7()
            task = core.TaskRun(
                id=task_run_id,
                name=request.task_name,
                flow_run_id=request.flow_run_id,
                task_key=request.task_key,
                dynamic_key=dynamic_key,
                task_version=request.task_version,
                empirical_policy=request.task_policy,
                tags=tags,
                labels=request.labels,
                flow_run_run_count=flow.run_count,
                state=core.states.Pending(),
            )
            created = await task_run_models.create_task_run(
                session=session, task_run=task
            )
            if created.id != task_run_id:
                raise NativeStreamError(
                    "IDENTITY_CONFLICT",
                    "Finite TaskRun identity was claimed concurrently",
                )
            manifest_digest = hashlib.sha256(
                b"native-finite-manifest-v1\0"
                + request_digest.encode("ascii")
                + task_run_id.bytes
            ).hexdigest()
            manifest = self.db.StreamFiniteSubmission(
                flow_run_id=request.flow_run_id,
                phase=request.phase,
                submission_key=request.submission_key,
                request_digest=request_digest,
                manifest_digest=manifest_digest,
                canonical_size=len(encoded),
                parameters=parameters,
                parameters_digest=parameters_digest,
                task_run_id=task_run_id,
                task_name=request.task_name,
                task_key=request.task_key,
                task_version=request.task_version,
                flow_version=request.flow_version,
                deployment_id=request.deployment_id,
                deployment_version=request.deployment_version,
                artifact_digest=request.artifact_digest,
                dynamic_key=dynamic_key,
                task_policy=request.task_policy.model_dump(mode="json"),
                tags=tags,
                labels=request.labels,
            )
            session.add(manifest)
            await session.flush()
            if request.phase == "PREFIX":
                authority.finite_prefix_manifest_digest = manifest_digest
                authority.finite_prefix_task_run_id = task_run_id
                authority.finite_prefix_sealed_at = now("UTC")
                authority.prepared_generation = authority.generation
                await session.flush()
            return _manifest_response(manifest, completed=False)

    async def lookup(self, request: StreamFiniteLookupRequest) -> StreamFiniteManifest:
        async with self.streams._session() as session:
            manifest = await session.scalar(
                sa.select(self.db.StreamFiniteSubmission).where(
                    self.db.StreamFiniteSubmission.flow_run_id == request.flow_run_id,
                    self.db.StreamFiniteSubmission.submission_key
                    == request.submission_key,
                )
            )
            if manifest is None:
                raise NativeStreamError("NOT_FOUND", "Unknown finite submission")
            if manifest.state == "RELEASED":
                raise NativeStreamError(
                    "REPLAY_HORIZON_EXPIRED", "Finite submission was released"
                )
            receipt = await session.scalar(
                sa.select(self.db.StreamFiniteReceipt.id).where(
                    self.db.StreamFiniteReceipt.finite_submission_id == manifest.id
                )
            )
            return _manifest_response(manifest, completed=receipt is not None)

    async def complete(
        self, request: StreamFiniteCompleteRequest
    ) -> StreamFiniteReceipt:
        result, encoded, result_digest = _canonical(
            request.result, MAX_FINITE_RESULT_BYTES
        )
        material = {
            "schema": "native-finite-completion-v1",
            "flow_run_id": str(request.flow_run_id),
            "task_run_id": str(request.task_run_id),
            "operation_id": str(request.operation_id),
            "run_count": request.run_count,
            "result": result,
        }
        _, _, request_digest = _canonical(material, MAX_FINITE_RESULT_BYTES + 4096)
        async with self.streams._session() as session:
            flow = await self.streams._flow(
                session,
                request.flow_run_id,
                update=True,
                allowed_execution_states=("ACTIVE",),
            )
            authority = await require_execution(
                session,
                self.db,
                request.flow_run_id,
                self.execution,
                allowed_states=("ACTIVE",),
                update=True,
            )
            assert authority is not None
            if authority.prepared_generation != authority.generation:
                raise NativeStreamError(
                    "RECOVERY_NOT_READY", "Execution generation is not prepared"
                )
            manifest = await session.scalar(
                sa.select(self.db.StreamFiniteSubmission)
                .where(
                    self.db.StreamFiniteSubmission.task_run_id == request.task_run_id,
                    self.db.StreamFiniteSubmission.flow_run_id == request.flow_run_id,
                )
                .with_for_update()
            )
            if manifest is None:
                raise NativeStreamError("NOT_FOUND", "Unknown finite submission")
            if manifest.state == "RELEASED":
                raise NativeStreamError(
                    "REPLAY_HORIZON_EXPIRED", "Finite submission was released"
                )
            existing = await self._existing_receipt(session, manifest.id)
            if existing is not None:
                if (
                    existing.operation_id != request.operation_id
                    or existing.request_digest != request_digest
                ):
                    raise NativeStreamError(
                        "IDENTITY_CONFLICT", "Finite result receipt is immutable"
                    )
                return _receipt_response(manifest, existing)
            task = await session.scalar(
                sa.select(self.db.TaskRun)
                .where(self.db.TaskRun.id == request.task_run_id)
                .with_for_update()
            )
            if task is None or task.flow_run_id != flow.id:
                raise NativeStreamError(
                    "IDENTITY_CONFLICT", "Finite TaskRun is missing"
                )
            if (
                task.run_count != request.run_count
                or task.state_type != StateType.RUNNING
            ):
                raise NativeStreamError(
                    "STALE_OWNER",
                    "Finite result attempt is not current Running TaskRun",
                )
            if manifest.phase == "PREFIX" and (
                authority.finite_prefix_manifest_digest != manifest.manifest_digest
                or authority.finite_prefix_task_run_id != manifest.task_run_id
            ):
                raise NativeStreamError(
                    "IDENTITY_CONFLICT", "Finite prefix anchor drift"
                )
            outcome_digest = finite_receipt_outcome_digest(
                flow_run_id=manifest.flow_run_id,
                phase=manifest.phase,
                submission_key=manifest.submission_key,
                manifest_digest=manifest.manifest_digest,
                task_run_id=manifest.task_run_id,
                operation_id=request.operation_id,
                run_count=request.run_count,
                result_digest=result_digest,
            )
            receipt = self.db.StreamFiniteReceipt(
                finite_submission_id=manifest.id,
                task_run_id=manifest.task_run_id,
                operation_id=request.operation_id,
                request_digest=request_digest,
                outcome_digest=outcome_digest,
                run_count=request.run_count,
                result=result,
                result_digest=result_digest,
                result_size=len(encoded),
            )
            session.add(receipt)
            await session.flush()
            return _receipt_response(manifest, receipt)

    async def receipt(self, task_run_id: UUID) -> StreamFiniteReceipt:
        async with self.streams._session() as session:
            manifest = await session.scalar(
                sa.select(self.db.StreamFiniteSubmission).where(
                    self.db.StreamFiniteSubmission.task_run_id == task_run_id
                )
            )
            if manifest is None:
                raise NativeStreamError("NOT_FOUND", "Unknown finite submission")
            if manifest.state == "RELEASED":
                raise NativeStreamError(
                    "REPLAY_HORIZON_EXPIRED", "Finite submission was released"
                )
            receipt = await session.scalar(
                sa.select(self.db.StreamFiniteReceipt).where(
                    self.db.StreamFiniteReceipt.finite_submission_id == manifest.id
                )
            )
            if receipt is None:
                raise NativeStreamError("NOT_FOUND", "Finite result is not complete")
            return _receipt_response(manifest, receipt)
