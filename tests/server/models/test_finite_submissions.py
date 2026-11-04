"""Bounded finite Task authority for mixed native stream executions."""

from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa

from prefect.server import models, schemas
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models.finite_submissions import NativeFiniteStore
from prefect.server.models.stream_executions import (
    apply_flow_lifecycle_result,
    claim_execution,
    guard_flow_lifecycle,
    guard_task_lifecycle,
    prepare_recovery,
    seal_manifest,
    takeover_execution,
)
from prefect.server.models.streams import (
    MAX_SUBMISSION_BYTES_PER_FLOW,
    NativeStreamError,
    NativeStreamStore,
)
from prefect.server.orchestration.core_policy import CoreFlowPolicy, CoreTaskPolicy
from prefect.server.schemas.streams import (
    StreamExecutionClaimRequest,
    StreamExecutionCredentials,
    StreamExecutionDeathWitness,
    StreamExecutionTakeoverRequest,
    StreamFiniteCompleteRequest,
    StreamFiniteManifestWitness,
    StreamFiniteReceiptWitness,
    StreamFiniteReserveRequest,
    StreamManifestSealRequest,
    StreamManifestWitness,
    StreamRecoveryPrepareRequest,
    StreamSubmissionReserveRequest,
)
from prefect.types._datetime import now

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def _require_postgres_17(db: PrefectDBInterface) -> None:
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        pytest.skip("Native finite Task authority requires PostgreSQL 17")


@pytest.fixture(autouse=True)
async def postgres_17(db: PrefectDBInterface) -> None:
    await _require_postgres_17(db)


def _claim(flow_run_id: UUID) -> StreamExecutionClaimRequest:
    return StreamExecutionClaimRequest(
        flow_run_id=flow_run_id,
        operation_id=uuid4(),
        holder_id=uuid4(),
        holder_capability="11" * 32,
        holder_event_public_key="22" * 32,
        supervisor_id=uuid4(),
        supervisor_capability="33" * 32,
    )


def _credentials(request: StreamExecutionClaimRequest) -> StreamExecutionCredentials:
    return StreamExecutionCredentials(
        generation=1,
        holder_id=request.holder_id,
        capability=request.holder_capability,
    )


def _prefix(flow_run_id: UUID) -> StreamFiniteReserveRequest:
    return StreamFiniteReserveRequest(
        flow_run_id=flow_run_id,
        phase="PREFIX",
        submission_key="finite.prefix",
        task_name="finite-prefix",
        task_key="finite-prefix-key",
        artifact_digest="44" * 32,
        task_version="task-v1",
        flow_version="flow-v1",
        parameters={"value": -0.0},
        task_policy=schemas.core.TaskRunPolicy(retries=2),
    )


async def _claimed_running_flow(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
) -> tuple[StreamExecutionClaimRequest, StreamExecutionCredentials]:
    async with db.session_context(begin_transaction=True) as session:
        row = await session.get(db.FlowRun, flow_run.id)
        row.flow_version = "flow-v1"
        row.empirical_policy = schemas.core.FlowRunPolicy(retries=2)
        await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Pending(),
            flow_policy=CoreFlowPolicy,
        )
    claim = _claim(flow_run.id)
    async with db.session_context(begin_transaction=True) as session:
        await claim_execution(session, db, claim)
    credentials = _credentials(claim)
    async with db.session_context(begin_transaction=True) as session:
        authority = await guard_flow_lifecycle(
            session,
            db,
            flow_run.id,
            credentials,
            schemas.states.Running(),
            force=False,
        )
        result = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Running(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, result.state)
    return claim, credentials


async def _set_task_running(
    db: PrefectDBInterface,
    task_run_id: UUID,
    credentials: StreamExecutionCredentials,
) -> None:
    async with db.session_context(begin_transaction=True) as session:
        await guard_task_lifecycle(
            session,
            db,
            task_run_id,
            credentials,
            schemas.states.Running(),
            force=False,
        )
        result = await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=task_run_id,
            state=schemas.states.Running(),
            task_policy=CoreTaskPolicy,
        )
        assert result.state is not None and result.state.is_running()


async def _set_task_completed(
    db: PrefectDBInterface,
    task_run_id: UUID,
    credentials: StreamExecutionCredentials,
) -> None:
    async with db.session_context(begin_transaction=True) as session:
        await guard_task_lifecycle(
            session,
            db,
            task_run_id,
            credentials,
            schemas.states.Completed(),
            force=False,
        )
        result = await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=task_run_id,
            state=schemas.states.Completed(),
            task_policy=CoreTaskPolicy,
        )
        assert result.state is not None and result.state.is_completed()


async def test_prefix_reserve_and_null_receipt_are_exact_and_immutable(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, credentials = await _claimed_running_flow(db, flow_run)
    store = NativeFiniteStore(db, credentials)
    request = _prefix(flow_run.id)
    first = await store.reserve(request)
    second = await store.reserve(request)
    assert first == second
    assert first.result_state == "PENDING"

    await _set_task_running(db, first.task_run_id, credentials)
    complete = StreamFiniteCompleteRequest(
        flow_run_id=flow_run.id,
        task_run_id=first.task_run_id,
        operation_id=uuid4(),
        run_count=1,
        result=None,
    )
    receipt = await store.complete(complete)
    assert receipt == await store.complete(complete)
    assert receipt.result is None
    assert (await store.lookup(request)).result_state == "COMPLETED"
    assert await store.receipt(first.task_run_id) == receipt

    with pytest.raises(NativeStreamError) as late_failure:
        async with db.session_context(begin_transaction=True) as session:
            await guard_task_lifecycle(
                session,
                db,
                first.task_run_id,
                credentials,
                schemas.states.Failed(message="lost completion acknowledgement"),
                force=False,
            )
    assert late_failure.value.code == "TERMINAL_CONFLICT"
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, first.task_run_id)
        assert task is not None and task.state_type == schemas.states.StateType.RUNNING
        assert task.run_count == 1
    await _set_task_completed(db, first.task_run_id, credentials)

    with pytest.raises(NativeStreamError) as drift:
        await store.complete(complete.model_copy(update={"result": {"changed": True}}))
    assert drift.value.code == "IDENTITY_CONFLICT"
    async with db.session_context() as session:
        assert (
            await session.scalar(sa.select(sa.func.count(db.StreamFiniteReceipt.id)))
            == 1
        )
    async with db.session_context(begin_transaction=True) as session:
        stored = await session.scalar(sa.select(db.StreamFiniteReceipt))
        assert stored is not None
        stored.result = {"tampered": True}
    with pytest.raises(NativeStreamError) as corrupted:
        await store.receipt(first.task_run_id)
    assert corrupted.value.code == "IDENTITY_CONFLICT"


async def test_prefix_only_takeover_prepares_same_task_retry(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    claim, credentials = await _claimed_running_flow(db, flow_run)
    prefix = await NativeFiniteStore(db, credentials).reserve(_prefix(flow_run.id))
    await _set_task_running(db, prefix.task_run_id, credentials)

    takeover_request = StreamExecutionTakeoverRequest(
        flow_run_id=flow_run.id,
        operation_id=uuid4(),
        expected_generation=1,
        expected_holder_id=claim.holder_id,
        supervisor_id=claim.supervisor_id,
        supervisor_capability=claim.supervisor_capability,
        new_holder_id=uuid4(),
        new_holder_capability="55" * 32,
        new_holder_event_public_key="66" * 32,
        death_witness=StreamExecutionDeathWitness(
            launcher_instance_id=uuid4(),
            child_process_id=4321,
            exit_code=77,
            observed_at=now("UTC"),
            kind="WAITPID",
        ),
    )
    async with db.session_context(begin_transaction=True) as session:
        takeover = await takeover_execution(session, db, takeover_request)
    assert takeover.state == "RECOVERING"
    recovered = StreamExecutionCredentials(
        generation=2,
        holder_id=takeover_request.new_holder_id,
        capability=takeover_request.new_holder_capability,
    )
    witness = StreamFiniteManifestWitness(
        submission_key=prefix.submission_key,
        manifest_digest=prefix.manifest_digest,
        phase="PREFIX",
    )
    request = StreamRecoveryPrepareRequest(
        flow_run_id=flow_run.id,
        operation_id=uuid4(),
        manifests=[],
        finite_manifests=[witness],
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await prepare_recovery(session, db, request, recovered)
    assert result.items == []
    assert result.finite_items is not None and len(result.finite_items) == 1
    item = result.finite_items[0]
    assert item.task_run_id == prefix.task_run_id
    assert item.task_state == "AwaitingRetry"
    assert item.current_run_count == 1
    assert item.next_run_count == 2
    assert item.receipt is None

    async with db.session_context(begin_transaction=True) as session:
        replay = await prepare_recovery(session, db, request, recovered)
    assert replay == result
    with pytest.raises(NativeStreamError) as stale:
        await NativeFiniteStore(db, credentials).complete(
            StreamFiniteCompleteRequest(
                flow_run_id=flow_run.id,
                task_run_id=prefix.task_run_id,
                operation_id=uuid4(),
                run_count=1,
                result="late",
            )
        )
    assert stale.value.code == "STALE_EXECUTION"


async def test_prefix_only_recovery_refuses_exhausted_retry_without_mutation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    claim, credentials = await _claimed_running_flow(db, flow_run)
    prefix = await NativeFiniteStore(db, credentials).reserve(
        _prefix(flow_run.id).model_copy(
            update={"task_policy": schemas.core.TaskRunPolicy(retries=1)}
        )
    )
    await _set_task_running(db, prefix.task_run_id, credentials)
    async with db.session_context(begin_transaction=True) as session:
        failed = await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=prefix.task_run_id,
            state=schemas.states.Failed(message="local retry"),
            task_policy=CoreTaskPolicy,
        )
        assert failed.state is not None and failed.state.name == "AwaitingRetry"
        running = await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=prefix.task_run_id,
            state=schemas.states.Running(),
            task_policy=CoreTaskPolicy,
        )
        assert running.state is not None and running.state.is_running()

    takeover_request = StreamExecutionTakeoverRequest(
        flow_run_id=flow_run.id,
        operation_id=uuid4(),
        expected_generation=1,
        expected_holder_id=claim.holder_id,
        supervisor_id=claim.supervisor_id,
        supervisor_capability=claim.supervisor_capability,
        new_holder_id=uuid4(),
        new_holder_capability="55" * 32,
        new_holder_event_public_key="66" * 32,
        death_witness=StreamExecutionDeathWitness(
            launcher_instance_id=uuid4(),
            child_process_id=4321,
            exit_code=77,
            observed_at=now("UTC"),
            kind="WAITPID",
        ),
    )
    async with db.session_context(begin_transaction=True) as session:
        await takeover_execution(session, db, takeover_request)
    recovered = StreamExecutionCredentials(
        generation=2,
        holder_id=takeover_request.new_holder_id,
        capability=takeover_request.new_holder_capability,
    )
    request = StreamRecoveryPrepareRequest(
        flow_run_id=flow_run.id,
        operation_id=uuid4(),
        finite_manifests=[
            StreamFiniteManifestWitness(
                submission_key=prefix.submission_key,
                manifest_digest=prefix.manifest_digest,
                phase="PREFIX",
            )
        ],
    )
    with pytest.raises(NativeStreamError) as exhausted:
        async with db.session_context(begin_transaction=True) as session:
            await prepare_recovery(session, db, request, recovered)
    assert exhausted.value.code == "RECOVERY_POLICY_EXHAUSTED"
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, prefix.task_run_id)
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
    assert task is not None and task.state_type == schemas.states.StateType.RUNNING
    assert task.run_count == 2
    assert authority is not None and authority.prepared_generation == 1


async def test_tail_receipt_and_all_eof_allow_mixed_flow_completion(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, credentials = await _claimed_running_flow(db, flow_run)
    finite = NativeFiniteStore(db, credentials)
    prefix = await finite.reserve(_prefix(flow_run.id))
    await _set_task_running(db, prefix.task_run_id, credentials)
    prefix_receipt = await finite.complete(
        StreamFiniteCompleteRequest(
            flow_run_id=flow_run.id,
            task_run_id=prefix.task_run_id,
            operation_id=uuid4(),
            run_count=1,
            result={"input": "ready"},
        )
    )
    await _set_task_completed(db, prefix.task_run_id, credentials)

    streams = NativeStreamStore(db, credentials)
    source = await streams.reserve_submission(
        StreamSubmissionReserveRequest(
            flow_run_id=flow_run.id,
            submission_key="source.documents",
            role="SOURCE",
            protocol="source-v1",
            task_name="documents",
            task_key="documents-key",
            task_version="task-v1",
            flow_version="flow-v1",
            parameters={"source": "documents"},
            task_policy=schemas.core.TaskRunPolicy(retries=2),
        )
    )
    async with db.session_context(begin_transaction=True) as session:
        await seal_manifest(
            session,
            db,
            StreamManifestSealRequest(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                manifests=[
                    StreamManifestWitness(
                        submission_key=source.submission_key,
                        manifest_digest=source.manifest_digest,
                    )
                ],
                finite_prefix=StreamFiniteReceiptWitness(
                    submission_key=prefix.submission_key,
                    manifest_digest=prefix.manifest_digest,
                    phase="PREFIX",
                    task_run_id=prefix.task_run_id,
                    result_digest=prefix_receipt.result_digest,
                ),
            ),
            credentials,
        )
    await _set_task_running(db, source.task_run_id, credentials)
    admission = await streams.open_source(
        source.stream_id, source.task_run_id, uuid4(), 1, metadata_lag=True
    )
    await streams.close_source(admission.binding, expected_position=0)
    await _set_task_completed(db, source.task_run_id, credentials)

    tail = await finite.reserve(
        _prefix(flow_run.id).model_copy(
            update={
                "phase": "TAIL",
                "submission_key": "finite.tail",
                "task_name": "finite-tail",
                "task_key": "finite-tail-key",
                "artifact_digest": "77" * 32,
            }
        )
    )
    await _set_task_running(db, tail.task_run_id, credentials)
    await finite.complete(
        StreamFiniteCompleteRequest(
            flow_run_id=flow_run.id,
            task_run_id=tail.task_run_id,
            operation_id=uuid4(),
            run_count=1,
            result={"count": 0},
        )
    )
    await _set_task_completed(db, tail.task_run_id, credentials)

    async with db.session_context(begin_transaction=True) as session:
        authority = await guard_flow_lifecycle(
            session,
            db,
            flow_run.id,
            credentials,
            schemas.states.Completed(),
            force=False,
        )
        result = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Completed(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, result.state)
    assert result.state is not None and result.state.is_completed()


async def test_finite_payload_and_combined_manifest_quotas_fail_before_mutation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, credentials = await _claimed_running_flow(db, flow_run)
    finite = NativeFiniteStore(db, credentials)
    prefix = await finite.reserve(_prefix(flow_run.id))
    await _set_task_running(db, prefix.task_run_id, credentials)
    with pytest.raises(NativeStreamError) as oversized:
        await finite.complete(
            StreamFiniteCompleteRequest(
                flow_run_id=flow_run.id,
                task_run_id=prefix.task_run_id,
                operation_id=uuid4(),
                run_count=1,
                result="x" * (64 * 1024 + 1),
            )
        )
    assert oversized.value.code == "PAYLOAD_TOO_LARGE"

    async with db.session_context(begin_transaction=True) as session:
        manifest = await session.scalar(
            sa.select(db.StreamFiniteSubmission).where(
                db.StreamFiniteSubmission.task_run_id == prefix.task_run_id
            )
        )
        assert manifest is not None
        manifest.canonical_size = MAX_SUBMISSION_BYTES_PER_FLOW
    with pytest.raises(NativeStreamError) as capacity:
        await NativeStreamStore(db, credentials).reserve_submission(
            StreamSubmissionReserveRequest(
                flow_run_id=flow_run.id,
                submission_key="source.capacity",
                role="SOURCE",
                protocol="source-v1",
                task_name="capacity-source",
                task_key="capacity-source-key",
                task_version="task-v1",
                flow_version="flow-v1",
                parameters={},
                task_policy=schemas.core.TaskRunPolicy(retries=2),
            )
        )
    assert capacity.value.code == "CAPACITY"
    async with db.session_context() as session:
        assert (
            await session.scalar(sa.select(sa.func.count(db.StreamFiniteReceipt.id)))
            == 0
        )
        assert (
            await session.scalar(sa.select(sa.func.count(db.StreamSubmission.id))) == 0
        )


async def test_release_removes_result_and_retains_finite_horizon_witness(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, credentials = await _claimed_running_flow(db, flow_run)
    finite = NativeFiniteStore(db, credentials)
    prefix = await finite.reserve(_prefix(flow_run.id))
    await _set_task_running(db, prefix.task_run_id, credentials)
    await finite.complete(
        StreamFiniteCompleteRequest(
            flow_run_id=flow_run.id,
            task_run_id=prefix.task_run_id,
            operation_id=uuid4(),
            run_count=1,
            result={"temporary": "bounded"},
        )
    )

    async with db.session_context(begin_transaction=True) as session:
        authority = await guard_flow_lifecycle(
            session,
            db,
            flow_run.id,
            credentials,
            schemas.states.Cancelled(),
            force=False,
        )
        result = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Cancelled(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, result.state)
    assert result.state is not None and result.state.is_cancelled()
    store = NativeStreamStore(db, credentials)
    with pytest.raises(NativeStreamError) as metadata_lag:
        await store.release_flow(flow_run.id)
    assert metadata_lag.value.code == "METADATA_NOT_READY"
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await session.scalar(sa.select(sa.func.count(db.StreamFiniteReceipt.id)))
            == 1
        )
        task_result = await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=prefix.task_run_id,
            state=schemas.states.Cancelled(message="bounded finite release"),
            task_policy=CoreTaskPolicy,
        )
        assert task_result.state is not None and task_result.state.is_cancelled()
    assert await store.release_flow(flow_run.id) == 0
    with pytest.raises(NativeStreamError) as released:
        await finite.receipt(prefix.task_run_id)
    assert released.value.code == "REPLAY_HORIZON_EXPIRED"
    async with db.session_context() as session:
        manifest = await session.scalar(
            sa.select(db.StreamFiniteSubmission).where(
                db.StreamFiniteSubmission.task_run_id == prefix.task_run_id
            )
        )
        assert manifest is not None
        assert manifest.state == "RELEASED" and manifest.released_at is not None
        assert (
            await session.scalar(sa.select(sa.func.count(db.StreamFiniteReceipt.id)))
            == 0
        )
