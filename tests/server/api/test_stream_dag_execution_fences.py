"""Topology-only DAGs cannot escape through existing task or stream APIs."""

import asyncio
import secrets
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import sqlalchemy as sa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy.ext.asyncio import AsyncSession

from prefect._internal.stream_event_auth import (
    canonical_event_witness,
    event_key_id,
    sign_task_event,
    verify_task_event,
)
from prefect.server.api.server import create_api_app
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.events.schemas.events import Event, ReceivedEvent
from prefect.server.models import stream_dags as dag_models
from prefect.server.models import stream_subtasks as fixed_models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_worker_capacity import physical_worker_occupancy
from prefect.server.schemas import actions, core, states
from prefect.server.schemas import stream_dags as dag
from prefect.server.schemas import stream_subtasks as physical
from prefect.server.schemas import streams as wire
from prefect.server.services.task_run_recorder import (
    authorized_stream_events,
    record_bulk_task_run_events,
)
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING, temporary_settings
from prefect.types._datetime import now
from tests.server.api.test_stream_dags import (
    assert_error,
    deployed_dag_request,
    topology_counts,
)
from tests.server.models.test_stream_dags import dag_request
from tests.server.models.test_stream_subtasks import graph_request

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


@pytest.fixture
async def fenced_api() -> AsyncGenerator[httpx.AsyncClient, None]:
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-dag:test"}):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_api_app(ignore_cache=True)),
            base_url="http://native-api",
            auth=("native-dag", "test"),
        ) as client:
            yield client


async def execution_snapshot(db: PrefectDBInterface) -> dict[str, list[dict[str, Any]]]:
    """Compare full persisted rows, including owners, cursors and state history."""
    async with db.session_context() as session:
        result: dict[str, list[dict[str, Any]]] = {}
        for model in (
            db.FlowRun,
            db.StreamExecutionAuthority,
            db.StreamGraphRevision,
            db.StreamPhysicalSubtask,
            db.TaskRun,
            db.TaskRunState,
            db.StreamSubmission,
            db.StreamPartition,
            db.StreamRecord,
            db.StreamConsumer,
            db.StreamCompletion,
            db.StreamSubtaskGrant,
            db.StreamSubtaskAllocation,
            db.StreamPhysicalSlotAllocation,
            db.ConcurrencyLimitV2,
        ):
            rows = await session.execute(sa.select(model.__table__).order_by(model.id))
            result[model.__tablename__] = [dict(row) for row in rows.mappings()]
        return result


async def sealed_dag(
    db: PrefectDBInterface,
    session: AsyncSession,
    api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    *,
    event_public_key: str | None = None,
) -> tuple[dag.StreamDAGPlan, dict[str, str]]:
    request, headers = await deployed_dag_request(
        db, session, api, deployment, event_public_key=event_public_key
    )
    response = await api.post(
        "/streams/v2/dags/seal", json=request.model_dump(mode="json"), headers=headers
    )
    assert response.status_code == 200, response.text
    return dag.StreamDAGPlan.model_validate(response.json()), headers


async def test_postgres_flow_holder_cannot_advance_dag_task_lifecycle(
    db: PrefectDBInterface,
    session: AsyncSession,
    fenced_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await sealed_dag(db, session, fenced_api, deployment)
    baseline = await execution_snapshot(db)
    proposed = actions.StateCreate(type=states.StateType.RUNNING, name="Running")
    for subtask in plan.subtasks:
        response = await fenced_api.post(
            f"/task_runs/{subtask.manifest.task_run_id}/set_state",
            json={"state": proposed.model_dump(mode="json")},
            headers=headers,
        )
        assert_error(response, 409, "EXECUTION_PROFILE_NOT_READY")
        assert await execution_snapshot(db) == baseline


def signed_running_event(
    task: orm_models.TaskRun,
    manifest_digest: str,
    headers: dict[str, str],
    private_key: bytes,
) -> ReceivedEvent:
    assert task.flow_run_id is not None
    public_key = (
        Ed25519PrivateKey.from_private_bytes(private_key)
        .public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    )
    payload = {
        "name": task.name,
        "task_key": task.task_key,
        "task_version": task.task_version,
        "dynamic_key": task.dynamic_key,
        "empirical_policy": task.empirical_policy.model_dump(mode="json"),
        "tags": task.tags,
        "task_inputs": {},
        "run_count": 1,
        "flow_run_run_count": task.flow_run_run_count,
    }
    occurred = now("UTC")
    event_id = uuid4()
    validated_state = {"type": "RUNNING", "name": "Running", "message": ""}
    witness = canonical_event_witness(
        event_id=event_id,
        event="prefect.task-run.Running",
        occurred=occurred,
        flow_run_id=task.flow_run_id,
        task_run_id=task.id,
        generation=1,
        holder_id=UUID(headers["X-Prefect-Stream-Execution-Holder"]),
        manifest_digest=manifest_digest,
        run_count=1,
        task_run=payload,
        validated_state=validated_state,
    )
    signature = sign_task_event(private_key, witness)
    assert verify_task_event(public_key, witness, signature)
    return Event(
        id=event_id,
        occurred=occurred,
        event="prefect.task-run.Running",
        resource={
            "prefect.resource.id": f"prefect.task-run.{task.id}",
            "prefect.run-count": "1",
            "prefect.stream-execution-generation": "1",
            "prefect.stream-execution-holder": headers[
                "X-Prefect-Stream-Execution-Holder"
            ],
            "prefect.stream-submission-digest": manifest_digest,
            "prefect.stream-event-key-id": event_key_id(public_key),
            "prefect.stream-event-signature": signature,
        },
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{task.flow_run_id}",
                "prefect.resource.role": "flow-run",
            }
        ],
        payload={
            "intended": {"from": "PENDING", "to": "RUNNING"},
            "initial_state": None,
            "validated_state": validated_state,
            "task_run": payload,
        },
    ).receive()


async def test_postgres_valid_signed_flow_event_cannot_execute_dag_task(
    db: PrefectDBInterface,
    session: AsyncSession,
    fenced_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    private_key = bytes(range(32))
    public_key = (
        Ed25519PrivateKey.from_private_bytes(private_key)
        .public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    )
    plan, headers = await sealed_dag(
        db, session, fenced_api, deployment, event_public_key=public_key.hex()
    )
    source = next(row for row in plan.subtasks if row.manifest.role == "SOURCE")
    async with db.session_context() as observed:
        task = await observed.get(db.TaskRun, source.manifest.task_run_id)
        assert task is not None
        event = signed_running_event(
            task, source.manifest.manifest_digest, headers, private_key
        )
    baseline = await execution_snapshot(db)
    # The production recorder is the synchronous state-bearing seam. General
    # event ingestion can acknowledge before its background consumer records.
    async with db.session_context(begin_transaction=True) as observed:
        accepted, _ = await authorized_stream_events(observed, db, [event])
        assert accepted == []
    await record_bulk_task_run_events([event])
    await record_bulk_task_run_events([event])
    assert await execution_snapshot(db) == baseline


async def test_postgres_advanced_dag_task_cannot_mutate_legacy_stream_data(
    db: PrefectDBInterface,
    session: AsyncSession,
    fenced_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, headers = await sealed_dag(db, session, fenced_api, deployment)
    source = next(row for row in plan.subtasks if row.manifest.role == "SOURCE")
    consumer = next(row for row in plan.subtasks if row.manifest.role == "OPERATOR")
    # Deliberate corrupt/legacy fixture: prove the store's explicit profile fence
    # even if Task metadata already appears Running. This is not a lifecycle path.
    async with db.session_context(begin_transaction=True) as corrupt:
        await corrupt.execute(
            sa.update(db.TaskRun)
            .where(
                db.TaskRun.id.in_([row.manifest.task_run_id for row in plan.subtasks])
            )
            .values(state_type=states.StateType.RUNNING, run_count=1)
        )
    owner_id = uuid4()
    binding = wire.SourceBinding(
        flow_run_id=plan.flow_run_id,
        stream_id=source.output_stream_id,
        task_run_id=source.manifest.task_run_id,
        owner_id=owner_id,
        attempt=1,
        owner_epoch=1,
    )
    claim = wire.StreamSourceClaimRequest(
        stream_id=source.output_stream_id,
        task_run_id=source.manifest.task_run_id,
        owner_id=owner_id,
        attempt=1,
    )
    assert consumer.input_stream_id is not None
    consumer_binding = wire.ConsumerBinding(
        flow_run_id=plan.flow_run_id,
        consumer_id=consumer.manifest.task_run_id,
        input_stream_id=consumer.input_stream_id,
        output_stream_id=consumer.output_stream_id,
        owner_id=owner_id,
        attempt=1,
        owner_epoch=1,
    )
    token = wire.DeliveryToken(
        **consumer_binding.model_dump(),
        record_id=uuid4(),
        sequence=1,
        delivery_id=uuid4(),
    )
    operations = [
        (
            "/create",
            wire.StreamCreateRequest(flow_run_id=plan.flow_run_id, stream_id=uuid4()),
        ),
        (
            "/submissions/reserve",
            wire.StreamSubmissionReserveRequest(
                flow_run_id=plan.flow_run_id,
                submission_key="escape-documents",
                role="SOURCE",
                protocol="source-v1",
                task_name="escape-documents",
                task_key="escape-key",
                task_version="dag-task-v1",
                flow_version="native-deployment-v1",
                parameters={},
                task_policy=core.TaskRunPolicy(retries=2),
            ),
        ),
        (
            "/sources/reserve",
            wire.StreamSourceReserveRequest(
                flow_run_id=plan.flow_run_id,
                stream_id=source.output_stream_id,
                task_run_id=source.manifest.task_run_id,
            ),
        ),
        ("/sources/claim", claim),
        ("/sources/open", wire.StreamSourceOpenRequest(**claim.model_dump())),
        (
            "/sources/append",
            wire.StreamSourceAppendRequest(
                binding=binding, position=1, value={"document": 200}
            ),
        ),
        (
            "/sources/close",
            wire.StreamSourceCloseRequest(binding=binding, expected_position=0),
        ),
        ("/sources/finish", wire.StreamSourceFinishRequest(binding=binding)),
        (
            "/outputs/reserve",
            wire.StreamOutputReserveRequest(
                flow_run_id=plan.flow_run_id,
                stream_id=consumer.output_stream_id,
                task_run_id=consumer.manifest.task_run_id,
            ),
        ),
        (
            "/consumers/open",
            wire.StreamConsumerOpenRequest(
                input_stream_id=consumer.input_stream_id,
                output_stream_id=consumer.output_stream_id,
                task_run_id=consumer.manifest.task_run_id,
                owner_id=owner_id,
                attempt=1,
            ),
        ),
        ("/consumers/read", wire.StreamConsumerReadRequest(binding=consumer_binding)),
        (
            "/consumers/complete",
            wire.StreamConsumerCompleteRequest(
                binding=consumer_binding,
                token=token,
                kind="YIELD",
                value={"document": 200},
                managed_state={},
            ),
        ),
        (
            "/consumers/finish",
            wire.StreamConsumerFinishRequest(binding=consumer_binding),
        ),
    ]
    baseline = await execution_snapshot(db)
    for suffix, request in operations:
        # Each object has passed the actual old request DTO before HTTP.
        response = await fenced_api.post(
            "/streams/v1" + suffix,
            json=request.model_dump(mode="json"),
            headers=headers,
        )
        try:
            assert_error(response, 409, "EXECUTION_PROFILE_NOT_READY")
        except AssertionError as error:
            raise AssertionError(f"{suffix}: {response.text}") from error
        assert await execution_snapshot(db) == baseline


async def test_postgres_unbound_task_in_dag_flow_cannot_execute_or_be_aborted(
    db: PrefectDBInterface,
    session: AsyncSession,
    fenced_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    private_key = bytes(range(32))
    public_key = (
        Ed25519PrivateKey.from_private_bytes(private_key)
        .public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        .hex()
    )
    plan, headers = await sealed_dag(
        db, session, fenced_api, deployment, event_public_key=public_key
    )
    # Deliberately inject an unbound row as persisted corruption. Normal task
    # creation/lifecycle is not used to manufacture this adversarial condition.
    async with db.session_context(begin_transaction=True) as corrupt:
        flow = await corrupt.get(db.FlowRun, plan.flow_run_id)
        assert flow is not None
        extra = db.TaskRun(
            id=uuid4(),
            flow_run_id=plan.flow_run_id,
            name="unbound-dag-task",
            task_key="unbound-dag-key",
            dynamic_key="unbound-dag-dynamic",
            task_version="dag-task-v1",
            empirical_policy=core.TaskRunPolicy(retries=2),
            tags=[],
            task_inputs={},
            state_type=states.StateType.PENDING,
            state_name="Pending",
            flow_run_run_count=flow.run_count,
            run_count=0,
        )
        corrupt.add(extra)
        await corrupt.flush()
        extra_id = extra.id
        event = signed_running_event(
            extra, plan.subtasks[0].manifest.manifest_digest, headers, private_key
        )
    baseline = await execution_snapshot(db)
    proposed = actions.StateCreate(type=states.StateType.RUNNING, name="Running")
    refused = await fenced_api.post(
        f"/task_runs/{extra_id}/set_state",
        json={"state": proposed.model_dump(mode="json")},
        headers=headers,
    )
    assert_error(refused, 409, "IDENTITY_CONFLICT")
    assert await execution_snapshot(db) == baseline
    async with db.session_context(begin_transaction=True) as observed:
        assert (await authorized_stream_events(observed, db, [event]))[0] == []
    await record_bulk_task_run_events([event])
    assert await execution_snapshot(db) == baseline
    with pytest.raises(NativeStreamError) as rejected:
        async with db.session_context(begin_transaction=True) as transaction:
            await dag_models.abort_dag_metadata(
                transaction,
                db,
                plan.id,
                physical.StreamGraphAbortRequest(
                    operation_id=uuid4(),
                    graph_digest=plan.graph_digest,
                    error="metadata compensation",
                ),
            )
    assert rejected.value.code == "IDENTITY_CONFLICT"
    assert await execution_snapshot(db) == baseline


async def test_postgres_linear_dag_is_valid_topology_without_execution(
    db: PrefectDBInterface,
    session: AsyncSession,
    fenced_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    request, headers = await deployed_dag_request(db, session, fenced_api, deployment)
    body = request.model_dump(mode="json")
    body["operators"] = body["operators"][:2]
    body["edges"] = body["edges"][:1]
    valid = dag.StreamDAGSealRequest.model_validate(body)
    sealed = await fenced_api.post(
        "/streams/v2/dags/seal", json=valid.model_dump(mode="json"), headers=headers
    )
    assert sealed.status_code == 200, sealed.text
    plan = dag.StreamDAGPlan.model_validate(sealed.json())
    assert len(plan.operators) == len(plan.subtasks) == 2
    assert len(plan.edges) == 1 and plan.leaf_operator_uids == ["B"]
    assert plan.execution_ready is False
    assert (await topology_counts(db))["stream_subtask_grant"] == 0


async def deployed_fixed_graph(
    db: PrefectDBInterface,
    session: AsyncSession,
    api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    *,
    tags: tuple[str, ...] = (),
) -> tuple[
    physical.StreamGraphPlan,
    list[tuple[UUID, physical.StreamWorkerPrincipal]],
    dict[str, str],
]:
    request, headers = await deployed_dag_request(
        db, session, api, deployment, profile="partitioned-fixed-v1"
    )
    async with db.session_context() as observed:
        launch = await observed.scalar(
            sa.select(db.NativeStreamLaunchIdentity).where(
                db.NativeStreamLaunchIdentity.flow_run_id == request.flow_run_id
            )
        )
        assert launch is not None
        pool = await observed.get(db.WorkPool, launch.work_pool_id)
        assert pool is not None
        first = physical.StreamWorkerPrincipal(
            work_pool_id=launch.work_pool_id,
            host_id=launch.launcher_host_id,
            boot_id=launch.launcher_boot_id,
            launcher_process_id=launch.launcher_process_id,
            launcher_start_ticks=launch.launcher_start_ticks,
        )
        first_id, pool_name = launch.worker_id, pool.name
    created = await api.post(
        f"/work_pools/{pool_name}/workers/heartbeat",
        json={"name": "second-" + uuid4().hex, "return_id": True},
    )
    assert created.status_code == 200, created.text
    second = first.model_copy(
        update={
            "launcher_process_id": first.launcher_process_id + 1,
            "launcher_start_ticks": first.launcher_start_ticks + 1,
        }
    )
    workers = [(first_id, first), (UUID(created.text), second)]
    for worker_id, principal in workers:
        polled = await api.post(
            f"/streams/v2/workers/{worker_id}/poll",
            json=physical.StreamWorkerPollRequest(principal=principal).model_dump(
                mode="json"
            ),
        )
        assert polled.status_code == 200, polled.text
    fixed_request = graph_request(request.flow_run_id, tags=tags).model_copy(
        update={
            "flow_version": request.flow_version,
            "deployment_id": request.deployment_id,
            "deployment_version": request.deployment_version,
            "working_directory": request.working_directory,
        }
    )
    sealed = await api.post(
        "/streams/v2/graphs/seal",
        json=fixed_request.model_dump(mode="json"),
        headers=headers,
    )
    assert sealed.status_code == 200, sealed.text
    return physical.StreamGraphPlan.model_validate(sealed.json()), workers, headers


async def test_postgres_fixed_controls_keep_working_with_two_profile_capabilities(
    db: PrefectDBInterface,
    session: AsyncSession,
    fenced_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
):
    plan, workers, headers = await deployed_fixed_graph(
        db, session, fenced_api, deployment
    )
    async with db.session_context(begin_transaction=True) as register:
        for worker_id, principal in workers:
            await dag_models.register_dag_capability_metadata(
                register, db, worker_id, principal
            )
    admitted = await fenced_api.post(
        f"/streams/v2/graphs/{plan.id}/admit",
        json={"operation_id": str(uuid4()), "graph_digest": plan.graph_digest},
        headers=headers,
    )
    assert admitted.status_code == 200, admitted.text
    assigned = physical.StreamGraphPlan.model_validate(admitted.json())
    principals = dict(workers)
    grants = {}
    for worker_id, principal in workers:
        poll = await fenced_api.post(
            f"/streams/v2/workers/{worker_id}/poll",
            json=physical.StreamWorkerPollRequest(principal=principal).model_dump(
                mode="json"
            ),
        )
        assert poll.status_code == 200, poll.text
        assert len(poll.json()) == 2
    for subtask in assigned.subtasks:
        worker_id = subtask.assigned_worker_id
        assert worker_id is not None
        principal = principals[worker_id]
        claim = physical.StreamSubtaskClaimRequest(
            operation_id=uuid4(),
            graph_digest=plan.graph_digest,
            holder_id=uuid4(),
            capability=secrets.token_hex(32),
            event_public_key="aa" * 32,
            launcher_binding_kind="ASSIGNED_WORKER",
            worker_id=worker_id,
            **principal.model_dump(),
        )
        claimed = await fenced_api.post(
            f"/streams/v2/workers/{worker_id}/subtasks/{subtask.id}/claim",
            json=claim.model_dump(mode="json"),
        )
        assert claimed.status_code == 200, claimed.text
        grant = physical.StreamSubtaskGrant.model_validate(claimed.json())
        registered = await fenced_api.post(
            f"/streams/v2/workers/{worker_id}/subtasks/{subtask.id}/register",
            json=physical.StreamWorkerRegistrationRequest(
                principal=principal,
                registration=physical.StreamSubtaskRegistrationRequest(
                    operation_id=uuid4(),
                    grant_id=grant.id,
                    child_process_id=10000 + len(grants),
                    process_start_ticks=20000 + len(grants),
                ),
            ).model_dump(mode="json"),
        )
        assert registered.status_code == 200, registered.text
        grants[subtask.id] = grant.id
    activated = await fenced_api.post(
        f"/streams/v2/graphs/{plan.id}/activate",
        json=physical.StreamGraphActivateRequest(
            operation_id=uuid4(),
            graph_digest=plan.graph_digest,
            grant_ids=[grants[row.id] for row in assigned.subtasks],
        ).model_dump(mode="json"),
        headers=headers,
    )
    assert activated.status_code == 200, activated.text
    assert activated.json()["startup_completed_at"] is not None
    async with db.session_context() as observed:
        capabilities = (
            await observed.scalars(
                sa.select(db.StreamWorkerCapability).where(
                    db.StreamWorkerCapability.worker_id.in_(principals)
                )
            )
        ).all()
        assert len(capabilities) == 4
        assert {row.protocol for row in capabilities} == {
            "partitioned-fixed-v1",
            "partitioned-dag-v1",
        }
    assert (await topology_counts(db))["stream_record"] == 0


async def test_postgres_fixed_and_dag_admission_contend_for_the_same_worker_slots(
    db: PrefectDBInterface,
    session: AsyncSession,
    fenced_api: httpx.AsyncClient,
    deployment: orm_models.Deployment,
    flow_run: orm_models.FlowRun,
):
    tag = "shared-profile-capacity"
    limit = await fenced_api.post(
        "/v2/concurrency_limits/",
        json={
            "name": "tag:" + tag,
            "limit": 8,
            "active": True,
            "slot_decay_per_second": 0,
        },
    )
    assert limit.status_code == 201, limit.text
    fixed, workers, headers = await deployed_fixed_graph(
        db, session, fenced_api, deployment, tags=(tag,)
    )
    request = dag_request(flow_run.id)
    request = request.model_copy(
        update={
            "operators": [
                operator.model_copy(update={"tags": [tag]})
                for operator in request.operators
            ]
        }
    )
    async with db.session_context(begin_transaction=True) as prepare:
        for worker_id, principal in workers:
            await dag_models.register_dag_capability_metadata(
                prepare, db, worker_id, principal
            )
        graph = await dag_models.seal_dag_metadata(prepare, db, request)
    credentials = wire.StreamExecutionCredentials(
        generation=1,
        holder_id=UUID(headers["X-Prefect-Stream-Execution-Holder"]),
        capability=headers["X-Prefect-Stream-Execution-Capability"],
    )

    async def contender(profile: str) -> tuple[str, str]:
        try:
            async with db.session_context(begin_transaction=True) as transaction:
                await transaction.execute(
                    sa.text("SELECT set_config('application_name', :name, true)"),
                    {"name": "ni2-010-race-" + profile},
                )
                if profile == "fixed":
                    result = await fixed_models.admit_region(
                        transaction,
                        db,
                        fixed.id,
                        physical.StreamRegionAdmissionRequest(
                            operation_id=uuid4(), graph_digest=fixed.graph_digest
                        ),
                        credentials,
                    )
                else:
                    result = await dag_models.admit_dag_metadata(
                        transaction,
                        db,
                        graph.id,
                        dag.StreamDAGAdmissionRequest(
                            operation_id=uuid4(), graph_digest=graph.graph_digest
                        ),
                        work_pool_id=workers[0][1].work_pool_id,
                    )
                assert result.state == "ADMITTED"
            return profile, "ADMITTED"
        except NativeStreamError as error:
            return profile, error.code

    pending: list[asyncio.Task[tuple[str, str]]] = []
    try:
        async with db.session_context(begin_transaction=True) as holder:
            await holder.execute(
                sa.select(db.Worker.id)
                .where(db.Worker.id == min(worker_id for worker_id, _ in workers))
                .with_for_update()
            )
            pending = [
                asyncio.create_task(contender(profile)) for profile in ("fixed", "dag")
            ]
            async with db.session_context() as observer:
                async with asyncio.timeout(10):
                    while True:
                        await observer.execute(
                            sa.text("SELECT pg_stat_clear_snapshot()")
                        )
                        waiting = await observer.scalar(
                            sa.text(
                                "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name IN ('ni2-010-race-fixed','ni2-010-race-dag') AND wait_event_type='Lock' AND query LIKE '%FROM worker%' AND query LIKE '%FOR UPDATE%'"
                            )
                        )
                        if waiting == 2:
                            break
                        for task in pending:
                            if task.done():
                                pytest.fail(
                                    f"Admission ended before the shared Worker lock: {task.result()}"
                                )
                        await asyncio.sleep(0.01)
                assert (
                    await physical_worker_occupancy(
                        observer, db, [worker_id for worker_id, _ in workers]
                    )
                    == {}
                )
        outcomes = dict(await asyncio.wait_for(asyncio.gather(*pending), 10))
        assert sorted(outcomes.values()) == ["ADMITTED", "REGION_CAPACITY"]
        winner = next(
            profile for profile, outcome in outcomes.items() if outcome == "ADMITTED"
        )
        ids = {"fixed": fixed.id, "dag": graph.id}
        async with db.session_context() as observed:
            for profile, graph_id in ids.items():
                stored = await observed.get(db.StreamGraphRevision, graph_id)
                assert stored is not None
                rows = (
                    await observed.scalars(
                        sa.select(db.StreamPhysicalSubtask).where(
                            db.StreamPhysicalSubtask.graph_revision_id == graph_id
                        )
                    )
                ).all()
                assert len(rows) == 4
                if profile == winner:
                    assert (
                        stored.state == "ADMITTED"
                        and stored.admission_operation_id is not None
                    )
                    assert all(row.assigned_worker_id is not None for row in rows)
                else:
                    assert (
                        stored.state == "SEALED"
                        and stored.admission_operation_id is None
                    )
                    assert stored.placement_digest is None
                    assert all(
                        row.assigned_worker_id is None and row.current_grant_id is None
                        for row in rows
                    )
                    assert (
                        await observed.scalar(
                            sa.select(sa.func.count())
                            .select_from(db.StreamSubtaskAllocation)
                            .where(
                                db.StreamSubtaskAllocation.subtask_id.in_(
                                    [row.id for row in rows]
                                )
                            )
                        )
                        == 0
                    )
                    assert (
                        await observed.scalar(
                            sa.select(sa.func.count())
                            .select_from(db.StreamPhysicalSlotAllocation)
                            .where(
                                db.StreamPhysicalSlotAllocation.graph_revision_id
                                == graph_id
                            )
                        )
                        == 0
                    )
            occupancy = await physical_worker_occupancy(
                observed, db, [worker_id for worker_id, _ in workers]
            )
            assert occupancy == {worker_id: 2 for worker_id, _ in workers}
            counter = await observed.get(
                db.ConcurrencyLimitV2, UUID(limit.json()["id"])
            )
            assert counter is not None and counter.active_slots == 4
            assert (
                await observed.scalar(
                    sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
                )
                == 4
            )
            assert (
                await observed.scalar(
                    sa.select(sa.func.count()).select_from(db.StreamSubtaskGrant)
                )
                == 0
            )
            assert (
                await observed.scalar(
                    sa.select(sa.func.count()).select_from(db.StreamRecord)
                )
                == 0
            )
    finally:
        for task in pending:
            if not task.done():
                task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def test_sqlite_dag_public_metadata_remains_unsupported(
    db: PrefectDBInterface, fenced_api: httpx.AsyncClient
):
    assert db.dialect.name == "sqlite"
    response = await fenced_api.post(
        "/streams/v2/dags/seal", json=dag_request(uuid4()).model_dump(mode="json")
    )
    assert_error(response, 422, "UNSUPPORTED_BACKEND")
    assert not any((await topology_counts(db)).values())
