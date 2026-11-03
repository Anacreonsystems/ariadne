"""Bounded execution-generation authority for native stream FlowRuns."""

from datetime import timedelta, timezone
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prefect._internal.stream_event_auth import (
    canonical_event_witness,
    event_key_id,
    sign_task_event,
)
from prefect.server import models, schemas
from prefect.server.database import PrefectDBInterface
from prefect.server.events.schemas.events import ReceivedEvent
from prefect.server.models.stream_executions import (
    apply_flow_lifecycle_result,
    claim_execution,
    guard_flow_lifecycle,
    prepare_recovery,
    seal_manifest,
    takeover_execution,
)
from prefect.server.models.streams import NativeStreamError, NativeStreamStore
from prefect.server.orchestration.core_policy import CoreFlowPolicy, CoreTaskPolicy
from prefect.server.schemas.streams import (
    StreamExecutionClaimRequest,
    StreamExecutionCredentials,
    StreamExecutionDeathWitness,
    StreamExecutionTakeoverRequest,
    StreamManifestSealRequest,
    StreamManifestWitness,
    StreamRecoveryPrepareRequest,
    StreamSubmissionReserveRequest,
)
from prefect.server.services.task_run_recorder import (
    _authorized_stream_events,
    record_bulk_task_run_events,
)
from prefect.types._datetime import now

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def _require_postgres_17(db: PrefectDBInterface) -> None:
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        pytest.skip("Native stream execution authority requires PostgreSQL 17")


@pytest.fixture(autouse=True)
async def postgres_17(db: PrefectDBInterface) -> None:
    await _require_postgres_17(db)


async def _recoverable_flow(db: PrefectDBInterface, flow_run) -> None:
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


def _claim(flow_run_id, *, public_key: str = "22" * 32):
    return StreamExecutionClaimRequest(
        flow_run_id=flow_run_id,
        operation_id=uuid4(),
        holder_id=uuid4(),
        holder_capability="11" * 32,
        holder_event_public_key=public_key,
        supervisor_id=uuid4(),
        supervisor_capability="33" * 32,
    )


def _credentials(request: StreamExecutionClaimRequest) -> StreamExecutionCredentials:
    return StreamExecutionCredentials(
        generation=1,
        holder_id=request.holder_id,
        capability=request.holder_capability,
    )


def _source(flow_run_id) -> StreamSubmissionReserveRequest:
    return StreamSubmissionReserveRequest(
        flow_run_id=flow_run_id,
        submission_key="source.documents",
        role="SOURCE",
        protocol="source-v1",
        task_name="documents",
        task_key="documents-key",
        task_version="task-v1",
        flow_version="flow-v1",
        parameters={"connector": "documents"},
        task_policy=schemas.core.TaskRunPolicy(retries=2),
    )


async def _claim_and_manifest(db, flow_run, *, claim=None, include_operator=True):
    await _recoverable_flow(db, flow_run)
    claim = claim or _claim(flow_run.id)
    async with db.session_context(begin_transaction=True) as session:
        binding = await claim_execution(session, db, claim)
    credentials = _credentials(claim)
    store = NativeStreamStore(db, credentials)
    source = await store.reserve_submission(_source(flow_run.id))
    operator = None
    if include_operator:
        operator = await store.reserve_submission(
            StreamSubmissionReserveRequest(
                flow_run_id=flow_run.id,
                submission_key="operator.transform",
                role="OPERATOR",
                protocol="yield-v2",
                task_name="transform",
                task_key="transform-key",
                task_version="task-v1",
                flow_version="flow-v1",
                parameters={"field": "document"},
                upstream_stream_id=source.stream_id,
                input_key="document",
                task_policy=schemas.core.TaskRunPolicy(retries=2),
            )
        )
    witnesses = [
        StreamManifestWitness(
            submission_key=item.submission_key,
            manifest_digest=item.manifest_digest,
        )
        for item in (source, operator)
        if item is not None
    ]
    return claim, binding, credentials, store, source, operator, witnesses


async def test_claim_is_exact_and_operation_drift_is_read_only(db, flow_run):
    await _recoverable_flow(db, flow_run)
    request = _claim(flow_run.id)
    async with db.session_context(begin_transaction=True) as session:
        first = await claim_execution(session, db, request)
    async with db.session_context(begin_transaction=True) as session:
        second = await claim_execution(session, db, request)
    assert first == second

    drift = request.model_copy(update={"holder_id": uuid4()})
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(NativeStreamError) as error:
            await claim_execution(session, db, drift)
    assert error.value.code == "IDENTITY_CONFLICT"
    async with db.session_context() as session:
        assert (
            await session.scalar(sa.select(sa.func.count(db.StreamExecutionAttempt.id)))
            == 1
        )


async def test_manifest_seal_is_ordered_complete_and_immutable(db, flow_run):
    await _require_postgres_17(db)
    (
        claim,
        _,
        credentials,
        store,
        source,
        operator,
        witnesses,
    ) = await _claim_and_manifest(db, flow_run)
    assert operator is not None
    seal_request = StreamManifestSealRequest(
        flow_run_id=flow_run.id, operation_id=uuid4(), manifests=witnesses
    )
    reversed_request = seal_request.model_copy(
        update={"operation_id": uuid4(), "manifests": list(reversed(witnesses))}
    )
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(NativeStreamError) as wrong_order:
            await seal_manifest(session, db, reversed_request, credentials)
    assert wrong_order.value.code == "UNSUPPORTED_TOPOLOGY"

    async with db.session_context(begin_transaction=True) as session:
        first = await seal_manifest(session, db, seal_request, credentials)
    async with db.session_context(begin_transaction=True) as session:
        second = await seal_manifest(session, db, seal_request, credentials)
    assert first == second
    assert first.manifest_count == 2

    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(NativeStreamError) as immutable:
            await seal_manifest(session, db, reversed_request, credentials)
    assert immutable.value.code == "IDENTITY_CONFLICT"

    with pytest.raises(NativeStreamError) as late:
        await store.reserve_submission(
            _source(flow_run.id).model_copy(
                update={
                    "submission_key": "source.late",
                    "task_key": "late-source-key",
                }
            )
        )
    assert late.value.code == "MANIFEST_SEALED"
    assert source.stream_id != operator.stream_id
    assert claim.flow_run_id == first.flow_run_id


async def test_manifest_seal_is_required_before_task_admission(db, flow_run):
    await _require_postgres_17(db)
    claim, _, credentials, _, source, _, witnesses = await _claim_and_manifest(
        db, flow_run
    )
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
    assert result.state is not None and result.state.is_running()

    with pytest.raises(NativeStreamError) as admission:
        await NativeStreamStore(db, credentials).open_source(
            source.stream_id, source.task_run_id, uuid4(), 1, metadata_lag=True
        )
    assert admission.value.code == "MANIFEST_NOT_SEALED"

    async with db.session_context(begin_transaction=True) as session:
        await seal_manifest(
            session,
            db,
            StreamManifestSealRequest(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                manifests=witnesses,
            ),
            credentials,
        )


async def test_takeover_uses_retry_policy_and_fences_old_holder(db, flow_run):
    await _require_postgres_17(db)
    claim, _, credentials, store, source, _, witnesses = await _claim_and_manifest(
        db, flow_run
    )
    async with db.session_context(begin_transaction=True) as session:
        await seal_manifest(
            session,
            db,
            StreamManifestSealRequest(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                manifests=witnesses,
            ),
            credentials,
        )
        authority = await guard_flow_lifecycle(
            session, db, flow_run.id, credentials, schemas.states.Running(), force=False
        )
        result = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Running(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, result.state)
        task_running = await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=source.task_run_id,
            state=schemas.states.Running(),
            task_policy=CoreTaskPolicy,
        )
        assert task_running.state is not None and task_running.state.is_running()

    request = StreamExecutionTakeoverRequest(
        flow_run_id=flow_run.id,
        operation_id=uuid4(),
        expected_generation=1,
        expected_holder_id=claim.holder_id,
        supervisor_id=claim.supervisor_id,
        supervisor_capability=claim.supervisor_capability,
        new_holder_id=uuid4(),
        new_holder_capability="44" * 32,
        new_holder_event_public_key="55" * 32,
        death_witness=StreamExecutionDeathWitness(
            launcher_instance_id=uuid4(),
            child_process_id=1234,
            exit_code=77,
            observed_at=now("UTC"),
            kind="WAITPID",
        ),
    )
    async with db.session_context(begin_transaction=True) as session:
        takeover = await takeover_execution(session, db, request)
    assert takeover.generation == 2
    assert takeover.state == "RECOVERING"
    async with db.session_context() as session:
        flow = await session.get(db.FlowRun, flow_run.id)
        assert flow.state_name == "AwaitingRetry"

    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(NativeStreamError) as stale:
            await prepare_recovery(
                session,
                db,
                StreamRecoveryPrepareRequest(
                    flow_run_id=flow_run.id,
                    operation_id=uuid4(),
                    manifests=witnesses,
                ),
                credentials,
            )
    assert stale.value.code == "STALE_EXECUTION"

    recovered_credentials = StreamExecutionCredentials(
        generation=2,
        holder_id=request.new_holder_id,
        capability=request.new_holder_capability,
    )
    with pytest.raises(NativeStreamError) as unprepared:
        await store.open_source(
            source.stream_id,
            source.task_run_id,
            uuid4(),
            1,
            metadata_lag=True,
        )
    assert unprepared.value.code == "STALE_EXECUTION"

    recovered_store = NativeStreamStore(db, recovered_credentials)
    with pytest.raises(NativeStreamError) as unprepared_current:
        await recovered_store.open_source(
            source.stream_id,
            source.task_run_id,
            uuid4(),
            1,
            metadata_lag=True,
        )
    assert unprepared_current.value.code == "RECOVERY_NOT_READY"

    recovery_request = StreamRecoveryPrepareRequest(
        flow_run_id=flow_run.id,
        operation_id=uuid4(),
        manifests=witnesses,
    )
    async with db.session_context(begin_transaction=True) as session:
        first = await prepare_recovery(
            session, db, recovery_request, recovered_credentials
        )
    async with db.session_context(begin_transaction=True) as session:
        replay = await prepare_recovery(
            session, db, recovery_request, recovered_credentials
        )
    assert replay == first
    assert first.generation == 2
    source_item = next(
        item for item in first.items if item.task_run_id == source.task_run_id
    )
    assert source_item.task_state == "AwaitingRetry"
    assert source_item.current_run_count == 1
    assert source_item.next_run_count == 2
    async with db.session_context() as session:
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        assert authority is not None and authority.prepared_generation == 2

    drift = recovery_request.model_copy(update={"operation_id": uuid4()})
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(NativeStreamError) as immutable:
            await prepare_recovery(session, db, drift, recovered_credentials)
    assert immutable.value.code == "IDENTITY_CONFLICT"


async def test_recovery_reconciles_durable_eof_before_new_attempt(db, flow_run):
    claim, _, credentials, store, source, _, witnesses = await _claim_and_manifest(
        db, flow_run, include_operator=False
    )
    async with db.session_context(begin_transaction=True) as session:
        await seal_manifest(
            session,
            db,
            StreamManifestSealRequest(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                manifests=witnesses,
            ),
            credentials,
        )
        authority = await guard_flow_lifecycle(
            session, db, flow_run.id, credentials, schemas.states.Running(), force=False
        )
        flow_running = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Running(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, flow_running.state)
        task_running = await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=source.task_run_id,
            state=schemas.states.Running(),
            task_policy=CoreTaskPolicy,
        )
        assert task_running.state is not None and task_running.state.is_running()

    admission = await store.open_source(
        source.stream_id, source.task_run_id, uuid4(), 1, metadata_lag=True
    )
    await store.close_source(admission.binding, expected_position=0)
    takeover_request = StreamExecutionTakeoverRequest(
        flow_run_id=flow_run.id,
        operation_id=uuid4(),
        expected_generation=1,
        expected_holder_id=claim.holder_id,
        supervisor_id=claim.supervisor_id,
        supervisor_capability=claim.supervisor_capability,
        new_holder_id=uuid4(),
        new_holder_capability="44" * 32,
        new_holder_event_public_key="55" * 32,
        death_witness=StreamExecutionDeathWitness(
            launcher_instance_id=uuid4(),
            child_process_id=1234,
            exit_code=77,
            observed_at=now("UTC"),
            kind="WAITPID",
        ),
    )
    async with db.session_context(begin_transaction=True) as session:
        await takeover_execution(session, db, takeover_request)
    recovered_credentials = StreamExecutionCredentials(
        generation=2,
        holder_id=takeover_request.new_holder_id,
        capability=takeover_request.new_holder_capability,
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await prepare_recovery(
            session,
            db,
            StreamRecoveryPrepareRequest(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                manifests=witnesses,
            ),
            recovered_credentials,
        )
    assert len(result.items) == 1
    assert result.items[0].stream_state == "COMPLETED"
    assert result.items[0].task_state == "Completed"
    assert result.items[0].current_run_count == 1
    assert result.items[0].next_run_count == 1
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert (
            task is not None and task.state_type == schemas.states.StateType.COMPLETED
        )


async def test_task_event_signature_is_verified_before_recorder_upsert(db, flow_run):
    private_key = bytes(range(32))
    public_key = (
        Ed25519PrivateKey.from_private_bytes(private_key)
        .public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    )
    claim = _claim(flow_run.id, public_key=public_key.hex())
    claim, _, credentials, _, source, _, witnesses = await _claim_and_manifest(
        db, flow_run, claim=claim, include_operator=False
    )
    async with db.session_context(begin_transaction=True) as session:
        await seal_manifest(
            session,
            db,
            StreamManifestSealRequest(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                manifests=witnesses,
            ),
            credentials,
        )
        authority = await guard_flow_lifecycle(
            session, db, flow_run.id, credentials, schemas.states.Running(), force=False
        )
        result = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Running(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, result.state)

    occurred = now("UTC").astimezone(timezone.utc)
    event_id = uuid4()
    task_payload = {
        "name": "documents",
        "task_key": "documents-key",
        "task_version": "task-v1",
        "dynamic_key": source.dynamic_key,
        "empirical_policy": {"retries": 2},
        "tags": [],
        "task_inputs": {},
        "run_count": 1,
        "flow_run_run_count": 1,
    }
    validated_state = {"type": "RUNNING", "name": "Running", "message": ""}
    witness = canonical_event_witness(
        event_id=event_id,
        event="prefect.task-run.Running",
        occurred=occurred,
        flow_run_id=flow_run.id,
        task_run_id=source.task_run_id,
        generation=1,
        holder_id=claim.holder_id,
        manifest_digest=source.manifest_digest,
        run_count=1,
        task_run=task_payload,
        validated_state=validated_state,
    )
    event = ReceivedEvent(
        id=event_id,
        occurred=occurred,
        event="prefect.task-run.Running",
        resource={
            "prefect.resource.id": f"prefect.task-run.{source.task_run_id}",
            "prefect.run-count": "1",
            "prefect.stream-execution-generation": "1",
            "prefect.stream-execution-holder": str(claim.holder_id),
            "prefect.stream-submission-digest": source.manifest_digest,
            "prefect.stream-event-key-id": event_key_id(public_key),
            "prefect.stream-event-signature": sign_task_event(private_key, witness),
        },
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
                "prefect.resource.role": "flow-run",
            }
        ],
        payload={
            "intended": {"from": "PENDING", "to": "RUNNING"},
            "initial_state": None,
            "validated_state": validated_state,
            "task_run": task_payload,
        },
        account=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        workspace=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        received=occurred,
    )
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [event]))[0] == [event]
    wrong_flow_attempt = event.model_copy(deep=True)
    wrong_flow_attempt.id = uuid4()
    wrong_flow_attempt.payload["task_run"]["flow_run_run_count"] = 0
    wrong_witness = canonical_event_witness(
        event_id=wrong_flow_attempt.id,
        event=wrong_flow_attempt.event,
        occurred=wrong_flow_attempt.occurred,
        flow_run_id=flow_run.id,
        task_run_id=source.task_run_id,
        generation=1,
        holder_id=claim.holder_id,
        manifest_digest=source.manifest_digest,
        run_count=1,
        task_run=wrong_flow_attempt.payload["task_run"],
        validated_state=wrong_flow_attempt.payload["validated_state"],
    )
    wrong_flow_attempt.resource["prefect.stream-event-signature"] = sign_task_event(
        private_key, wrong_witness
    )
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [wrong_flow_attempt]))[
            0
        ] == []
    await record_bulk_task_run_events([event])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task.state_type == schemas.states.StateType.RUNNING
        state_count = await session.scalar(
            sa.select(sa.func.count(db.TaskRunState.id)).where(
                db.TaskRunState.task_run_id == source.task_run_id
            )
        )
        assert task.empirical_policy.retries == 2

    policy_drift = event.model_copy(deep=True)
    policy_drift.id = uuid4()
    policy_drift.occurred = (occurred + timedelta(seconds=100)).replace(
        tzinfo=timezone.utc
    )
    policy_drift.payload["task_run"]["empirical_policy"] = {"retries": 1_000_000}
    policy_witness = canonical_event_witness(
        event_id=policy_drift.id,
        event=policy_drift.event,
        occurred=policy_drift.occurred,
        flow_run_id=flow_run.id,
        task_run_id=source.task_run_id,
        generation=1,
        holder_id=claim.holder_id,
        manifest_digest=source.manifest_digest,
        run_count=1,
        task_run=policy_drift.payload["task_run"],
        validated_state=policy_drift.payload["validated_state"],
    )
    policy_drift.resource["prefect.stream-event-signature"] = sign_task_event(
        private_key, policy_witness
    )
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [policy_drift]))[0] == []
    await record_bulk_task_run_events([policy_drift])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task is not None
        assert task.state_type == schemas.states.StateType.RUNNING
        assert task.empirical_policy.retries == 2
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.TaskRunState.id)).where(
                    db.TaskRunState.task_run_id == source.task_run_id
                )
            )
            == state_count
        )

    tag_drift = event.model_copy(deep=True)
    tag_drift.id = uuid4()
    tag_drift.occurred = (occurred + timedelta(seconds=100)).replace(
        tzinfo=timezone.utc
    )
    tag_drift.payload["task_run"]["tags"] = ["different-concurrency-scope"]
    tag_witness = canonical_event_witness(
        event_id=tag_drift.id,
        event=tag_drift.event,
        occurred=tag_drift.occurred,
        flow_run_id=flow_run.id,
        task_run_id=source.task_run_id,
        generation=1,
        holder_id=claim.holder_id,
        manifest_digest=source.manifest_digest,
        run_count=1,
        task_run=tag_drift.payload["task_run"],
        validated_state=tag_drift.payload["validated_state"],
    )
    tag_drift.resource["prefect.stream-event-signature"] = sign_task_event(
        private_key, tag_witness
    )
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [tag_drift]))[0] == []
    await record_bulk_task_run_events([tag_drift])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task is not None
        assert task.tags == []
        assert task.state_type == schemas.states.StateType.RUNNING
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.TaskRunState.id)).where(
                    db.TaskRunState.task_run_id == source.task_run_id
                )
            )
            == state_count
        )

    foreign_task_id = uuid4()
    foreign_flow_id = uuid4()
    reserved_task_fields = event.model_copy(deep=True)
    reserved_task_fields.id = uuid4()
    reserved_task_fields.occurred = (occurred + timedelta(seconds=101)).replace(
        tzinfo=timezone.utc
    )
    reserved_task_fields.payload["task_run"].update(
        {
            "id": str(foreign_task_id),
            "flow_run_id": str(foreign_flow_id),
            "state": {"type": "COMPLETED", "name": "Completed"},
        }
    )
    reserved_task_witness = canonical_event_witness(
        event_id=reserved_task_fields.id,
        event=reserved_task_fields.event,
        occurred=reserved_task_fields.occurred,
        flow_run_id=flow_run.id,
        task_run_id=source.task_run_id,
        generation=1,
        holder_id=claim.holder_id,
        manifest_digest=source.manifest_digest,
        run_count=1,
        task_run=reserved_task_fields.payload["task_run"],
        validated_state=reserved_task_fields.payload["validated_state"],
    )
    reserved_task_fields.resource["prefect.stream-event-signature"] = sign_task_event(
        private_key, reserved_task_witness
    )

    reserved_state_fields = event.model_copy(deep=True)
    reserved_state_fields.id = uuid4()
    reserved_state_fields.occurred = (occurred + timedelta(seconds=102)).replace(
        tzinfo=timezone.utc
    )
    reserved_state_fields.payload["validated_state"].update(
        {
            "id": str(uuid4()),
            "timestamp": (occurred + timedelta(days=1)).isoformat(),
        }
    )
    reserved_state_witness = canonical_event_witness(
        event_id=reserved_state_fields.id,
        event=reserved_state_fields.event,
        occurred=reserved_state_fields.occurred,
        flow_run_id=flow_run.id,
        task_run_id=source.task_run_id,
        generation=1,
        holder_id=claim.holder_id,
        manifest_digest=source.manifest_digest,
        run_count=1,
        task_run=reserved_state_fields.payload["task_run"],
        validated_state=reserved_state_fields.payload["validated_state"],
    )
    reserved_state_fields.resource["prefect.stream-event-signature"] = sign_task_event(
        private_key, reserved_state_witness
    )
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await _authorized_stream_events(
                session, db, [reserved_task_fields, reserved_state_fields]
            )
        )[0] == []
    await record_bulk_task_run_events([reserved_task_fields, reserved_state_fields])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task is not None
        assert task.state_type == schemas.states.StateType.RUNNING
        assert task.empirical_policy.retries == 2
        assert await session.get(db.TaskRun, foreign_task_id) is None
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.TaskRunState.id)).where(
                    db.TaskRunState.task_run_id == source.task_run_id
                )
            )
            == state_count
        )
    completed = event.model_copy(deep=True)
    completed.id = uuid4()
    completed.occurred = (occurred + timedelta(seconds=1)).replace(tzinfo=timezone.utc)
    completed.event = "prefect.task-run.Completed"
    completed.payload["validated_state"] = {
        "type": "COMPLETED",
        "name": "Completed",
        "message": "",
    }
    completed_witness = canonical_event_witness(
        event_id=completed.id,
        event=completed.event,
        occurred=completed.occurred,
        flow_run_id=flow_run.id,
        task_run_id=source.task_run_id,
        generation=1,
        holder_id=claim.holder_id,
        manifest_digest=source.manifest_digest,
        run_count=1,
        task_run=completed.payload["task_run"],
        validated_state=completed.payload["validated_state"],
    )
    completed.resource["prefect.stream-event-signature"] = sign_task_event(
        private_key, completed_witness
    )
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [completed]))[0] == []

    tampered = event.model_copy(deep=True)
    tampered.id = uuid4()
    tampered.event = "prefect.task-run.Completed"
    tampered.payload["validated_state"] = {
        "type": "COMPLETED",
        "name": "Completed",
        "message": "tampered",
    }
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [tampered]))[0] == []
    await record_bulk_task_run_events([tampered])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task.state_type == schemas.states.StateType.RUNNING

    store = NativeStreamStore(db, credentials)
    admission = await store.open_source(
        source.stream_id, source.task_run_id, uuid4(), 1, metadata_lag=True
    )
    await store.close_source(admission.binding, expected_position=0)
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
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [completed]))[0] == [
            completed
        ]
    await record_bulk_task_run_events([completed])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task.state_type == schemas.states.StateType.COMPLETED
    assert await store.release_flow(flow_run.id) == 1
    assert await store.release_flow(flow_run.id) == 0
    async with db.session_context() as session:
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        assert authority.state == "RELEASED"


async def test_recorder_rejects_future_timestamp_from_older_local_attempt(db, flow_run):
    private_key = bytes(reversed(range(32)))
    public_key = (
        Ed25519PrivateKey.from_private_bytes(private_key)
        .public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    )
    claim = _claim(flow_run.id, public_key=public_key.hex())
    claim, _, credentials, _, source, _, witnesses = await _claim_and_manifest(
        db, flow_run, claim=claim, include_operator=False
    )
    async with db.session_context(begin_transaction=True) as session:
        await seal_manifest(
            session,
            db,
            StreamManifestSealRequest(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                manifests=witnesses,
            ),
            credentials,
        )
        authority = await guard_flow_lifecycle(
            session, db, flow_run.id, credentials, schemas.states.Running(), force=False
        )
        result = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Running(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, result.state)
    async with db.session_context() as session:
        authority_row = await session.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        flow_run_count = authority_row.flow_run_run_count

    base_time = now("UTC").astimezone(timezone.utc)

    def signed_event(
        *, run_count: int, state_type: str, state_name: str, offset: int
    ) -> ReceivedEvent:
        event_id = uuid4()
        occurred = (base_time + timedelta(seconds=offset)).replace(tzinfo=timezone.utc)
        task_payload = {
            "name": "documents",
            "task_key": "documents-key",
            "task_version": "task-v1",
            "dynamic_key": source.dynamic_key,
            "empirical_policy": {"retries": 2},
            "tags": [],
            "task_inputs": {},
            "run_count": run_count,
            "flow_run_run_count": flow_run_count,
        }
        state_payload = {"type": state_type, "name": state_name, "message": ""}
        event_name = f"prefect.task-run.{state_name}"
        witness = canonical_event_witness(
            event_id=event_id,
            event=event_name,
            occurred=occurred,
            flow_run_id=flow_run.id,
            task_run_id=source.task_run_id,
            generation=1,
            holder_id=claim.holder_id,
            manifest_digest=source.manifest_digest,
            run_count=run_count,
            task_run=task_payload,
            validated_state=state_payload,
        )
        return ReceivedEvent(
            id=event_id,
            occurred=occurred,
            event=event_name,
            resource={
                "prefect.resource.id": f"prefect.task-run.{source.task_run_id}",
                "prefect.run-count": str(run_count),
                "prefect.stream-execution-generation": "1",
                "prefect.stream-execution-holder": str(claim.holder_id),
                "prefect.stream-submission-digest": source.manifest_digest,
                "prefect.stream-event-key-id": event_key_id(public_key),
                "prefect.stream-event-signature": sign_task_event(private_key, witness),
            },
            related=[
                {
                    "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
                    "prefect.resource.role": "flow-run",
                }
            ],
            payload={
                "intended": {"from": None, "to": state_type},
                "initial_state": None,
                "validated_state": state_payload,
                "task_run": task_payload,
            },
            account=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            workspace=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            received=occurred,
        )

    running_one = signed_event(
        run_count=1, state_type="RUNNING", state_name="Running", offset=1
    )
    failed_one = signed_event(
        run_count=1, state_type="FAILED", state_name="Failed", offset=2
    )
    running_two = signed_event(
        run_count=2, state_type="RUNNING", state_name="Running", offset=3
    )
    await record_bulk_task_run_events([running_two, failed_one, running_one])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task.run_count == 2
        assert task.state_type == schemas.states.StateType.RUNNING

    stale_failed = signed_event(
        run_count=1, state_type="FAILED", state_name="Failed", offset=100
    )
    await record_bulk_task_run_events([stale_failed])
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, source.task_run_id)
        assert task.run_count == 2
        assert task.state_type == schemas.states.StateType.RUNNING
        assert await session.get(db.TaskRunState, stale_failed.id) is None
