"""Real PostgreSQL fixed-region authority and ordinary-counter invariants."""

import asyncio
import json
import os
import subprocess
import sys
from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prefect._internal.stream_event_auth import (
    canonical_physical_event_witness,
    sign_task_event,
)
from prefect.server import models, schemas
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.events.schemas.events import Event
from prefect.server.models import stream_subtasks as m
from prefect.server.models.stream_executions import (
    apply_flow_lifecycle_result,
    claim_execution,
    guard_flow_lifecycle,
)
from prefect.server.models.stream_subtask_authority import require_subtask
from prefect.server.models.streams import NativeStreamError, NativeStreamStore
from prefect.server.orchestration.core_policy import CoreFlowPolicy
from prefect.server.schemas import stream_subtasks as s
from prefect.server.schemas.streams import StreamExecutionCredentials
from prefect.server.services.task_run_recorder import (
    _authorized_stream_events,
    record_bulk_task_run_events,
)
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING, temporary_settings
from prefect.types._datetime import now
from tests.server.models.test_stream_executions import (
    _claim,
    _credentials,
    _recoverable_flow,
    _require_postgres_17,
)

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]
PRIVATE = bytes(range(32))
PUBLIC = (
    Ed25519PrivateKey.from_private_bytes(PRIVATE)
    .public_key()
    .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    .hex()
)


@pytest.fixture(autouse=True)
async def protected_postgres(db: PrefectDBInterface) -> AsyncGenerator[None, None]:
    await _require_postgres_17(db)
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-physical:test"}):
        yield


def graph_request(
    flow_id: UUID, *, tags: tuple[str, ...] = ()
) -> s.StreamGraphSealRequest:
    common = dict(
        task_version="task-v1",
        context_key="ctx",
        artifact_digest="aa" * 32,
        task_policy=schemas.core.TaskRunPolicy(retries=2),
        tags=list(tags),
    )
    return s.StreamGraphSealRequest(
        flow_run_id=flow_id,
        operation_id=uuid4(),
        flow_version="flow-v1",
        operators=[
            s.StreamOperatorSpec(
                operator_uid="A",
                role="SOURCE",
                task_name="documents",
                task_key="source-key",
                entrypoint="example.py:documents",
                parameters=[{"partition": 0}, {"partition": 1}],
                **common,
            ),
            s.StreamOperatorSpec(
                operator_uid="B",
                role="OPERATOR",
                task_name="sign",
                task_key="sign-key",
                entrypoint="example.py:sign",
                parameters=[{}, {}],
                input_key="document",
                upstream_operator_uid="A",
                **common,
            ),
        ],
    )


async def prepare(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, *, tags: tuple[str, ...] = ()
) -> tuple[s.StreamGraphSealRequest, s.StreamGraphPlan, StreamExecutionCredentials]:
    await _recoverable_flow(db, flow_run)
    claim = _claim(flow_run.id)
    credentials = _credentials(claim)
    request = graph_request(flow_run.id, tags=tags)
    async with db.session_context(begin_transaction=True) as session:
        await claim_execution(session, db, claim)
        plan = await m.seal_graph(
            session, db, request, credentials, _allow_local_discovery=True
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
    return request, plan, credentials


async def admit(
    db: PrefectDBInterface,
    plan: s.StreamGraphPlan,
    credentials: StreamExecutionCredentials,
) -> tuple[s.StreamRegionAdmissionRequest, s.StreamGraphPlan]:
    request = s.StreamRegionAdmissionRequest(
        operation_id=uuid4(), graph_digest=plan.graph_digest
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await m.admit_region(
            session, db, plan.id, request, credentials, _allow_local_discovery=True
        )
    return request, result


async def claim_registered(
    db: PrefectDBInterface,
    plan: s.StreamGraphPlan,
    credentials: StreamExecutionCredentials,
    subtask: s.StreamSubtaskProjection | None = None,
    *,
    activate: bool = True,
    registered_count: int = 4,
) -> tuple[
    s.StreamSubtaskProjection,
    s.StreamSubtaskClaimRequest,
    s.StreamSubtaskGrant,
    s.StreamSubtaskCredentials,
]:
    subtask = subtask or next(
        item
        for item in plan.subtasks
        if item.operator_uid == "A" and item.subtask_index == 0
    )
    request = s.StreamSubtaskClaimRequest(
        operation_id=uuid4(),
        graph_digest=plan.graph_digest,
        holder_id=uuid4(),
        capability=uuid4().hex + uuid4().hex,
        event_public_key=PUBLIC,
        launcher_binding_kind="FLOW_AUTHORITY_LOCAL",
        host_id=uuid4(),
        boot_id=uuid4(),
        launcher_process_id=100,
        launcher_start_ticks=1000,
    )
    async with db.session_context(begin_transaction=True) as session:
        grant = await m.claim_subtask(session, db, subtask.id, request, credentials)
        grant = await m.register_subtask(
            session,
            db,
            subtask.id,
            s.StreamSubtaskRegistrationRequest(
                operation_id=uuid4(),
                grant_id=grant.id,
                child_process_id=200,
                process_start_ticks=2000,
            ),
            credentials,
        )
    siblings = [item for item in plan.subtasks if item.id != subtask.id]
    for sibling in siblings[: registered_count - 1]:
        sibling_request = request.model_copy(
            update={
                "operation_id": uuid4(),
                "holder_id": uuid4(),
                "capability": uuid4().hex + uuid4().hex,
            }
        )
        async with db.session_context(begin_transaction=True) as session:
            sibling_grant = await m.claim_subtask(
                session, db, sibling.id, sibling_request, credentials
            )
            await m.register_subtask(
                session,
                db,
                sibling.id,
                s.StreamSubtaskRegistrationRequest(
                    operation_id=uuid4(),
                    grant_id=sibling_grant.id,
                    child_process_id=210 + sibling.subtask_index,
                    process_start_ticks=2100 + sibling.subtask_index,
                ),
                credentials,
            )
    if activate:
        async with db.session_context(begin_transaction=True) as session:
            current = await m.read_graph(session, db, plan.id, credentials)
            await m.activate_graph(
                session,
                db,
                plan.id,
                s.StreamGraphActivateRequest(
                    operation_id=uuid4(),
                    graph_digest=plan.graph_digest,
                    grant_ids=[item.current_grant_id for item in current.subtasks],
                ),
                credentials,
            )
    return (
        subtask,
        request,
        grant,
        s.StreamSubtaskCredentials(grant_id=grant.id, capability=request.capability),
    )


def task_event(
    plan: s.StreamGraphPlan,
    subtask: s.StreamSubtaskProjection,
    grant: s.StreamSubtaskGrant,
    *,
    run_count: int = 1,
    state_type: str = "RUNNING",
    name: str = "Running",
    tags: tuple[str, ...] = (),
    retriable: bool | None = None,
) -> Event:
    timestamp = now("UTC")
    event_id = uuid4()
    payload = {
        "name": "documents[0]",
        "task_key": "source-key",
        "task_version": "task-v1",
        "dynamic_key": subtask.manifest.dynamic_key,
        "empirical_policy": {"retries": 2},
        "tags": list(tags),
        "task_inputs": {},
        "run_count": run_count,
        "flow_run_run_count": 1,
    }
    state = {"type": state_type, "name": name, "message": ""}
    if retriable is not None:
        state["state_details"] = {"retriable": retriable}
    witness = canonical_physical_event_witness(
        event_id=event_id,
        event=f"prefect.task-run.{name}",
        occurred=timestamp,
        flow_run_id=plan.flow_run_id,
        task_run_id=subtask.manifest.task_run_id,
        generation=grant.physical_epoch,
        holder_id=grant.holder_id,
        manifest_digest=subtask.manifest.manifest_digest,
        run_count=run_count,
        task_run=payload,
        validated_state=state,
        graph_revision_id=plan.id,
        assignment_epoch=1,
        subtask_id=subtask.id,
        grant_id=grant.id,
        physical_epoch=grant.physical_epoch,
        scope_digest=grant.scope_digest,
    )
    return Event(
        id=event_id,
        occurred=timestamp,
        event=f"prefect.task-run.{name}",
        resource={
            "prefect.resource.id": f"prefect.task-run.{subtask.manifest.task_run_id}",
            "prefect.run-count": str(run_count),
            "prefect.stream-execution-generation": str(grant.physical_epoch),
            "prefect.stream-execution-holder": str(grant.holder_id),
            "prefect.stream-submission-digest": subtask.manifest.manifest_digest,
            "prefect.stream-event-key-id": grant.event_key_id,
            "prefect.stream-event-signature": sign_task_event(PRIVATE, witness),
            "prefect.stream-graph-revision": str(plan.id),
            "prefect.stream-assignment-epoch": "1",
            "prefect.stream-subtask": str(subtask.id),
            "prefect.stream-subtask-grant": str(grant.id),
            "prefect.stream-physical-epoch": str(grant.physical_epoch),
            "prefect.stream-scope-digest": grant.scope_digest,
        },
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{plan.flow_run_id}",
                "prefect.resource.role": "flow-run",
            }
        ],
        payload={
            "intended": {"from": "PENDING", "to": state_type},
            "initial_state": None,
            "validated_state": state,
            "task_run": payload,
        },
    )


async def test_immutable_p2_graph_reuses_four_ordinary_tasks_and_partitions(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    request, plan, credentials = await prepare(db, flow_run)
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await m.seal_graph(
                session, db, request, credentials, _allow_local_discovery=True
            )
            == plan
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRun)
                .where(db.TaskRun.flow_run_id == flow_run.id)
            )
            == 4
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamPartition)
                .where(db.StreamPartition.flow_run_id == flow_run.id)
            )
            == 4
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamRecord)
            )
            == 0
        )
    sources = {
        item.subtask_index: item for item in plan.subtasks if item.operator_uid == "A"
    }
    assert all(
        item.input_stream_id == sources[item.subtask_index].manifest.stream_id
        for item in plan.subtasks
        if item.operator_uid == "B"
    )
    with pytest.raises(NativeStreamError, match="immutable graph"):
        async with db.session_context(begin_transaction=True) as session:
            await m.seal_graph(
                session,
                db,
                request.model_copy(update={"operation_id": uuid4()}),
                credentials,
                _allow_local_discovery=True,
            )


async def test_region_admission_is_all_or_none_and_exact_replay(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("roomy", "tight"))
    async with db.session_context(begin_transaction=True) as session:
        roomy = db.ConcurrencyLimitV2(name="tag:roomy", limit=8, active=True)
        tight = db.ConcurrencyLimitV2(name="tag:tight", limit=3, active=True)
        session.add_all([roomy, tight])
        await session.flush()
        roomy_id, tight_id = roomy.id, tight.id
    with pytest.raises(NativeStreamError, match="exceeds"):
        await admit(db, plan, credentials)
    async with db.session_context(begin_transaction=True) as session:
        assert (await session.get(db.ConcurrencyLimitV2, roomy_id)).active_slots == 0
        assert (await session.get(db.ConcurrencyLimitV2, tight_id)).active_slots == 0
        assert (await session.get(db.StreamGraphRevision, plan.id)).state == "SEALED"
        (await session.get(db.ConcurrencyLimitV2, tight_id)).limit = 4
    request, admitted = await admit(db, plan, credentials)
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await m.admit_region(
                session, db, plan.id, request, credentials, _allow_local_discovery=True
            )
            == admitted
        )
        assert (await session.get(db.ConcurrencyLimitV2, roomy_id)).active_slots == 4
        assert (await session.get(db.ConcurrencyLimitV2, tight_id)).active_slots == 4
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 8
        )
        assert not await models.concurrency_limits_v2.bulk_increment_active_slots(
            session=session, concurrency_limit_ids=[tight_id], slots=1
        )


async def test_admission_database_rollback_restores_counters_and_graph(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    with pytest.raises(RuntimeError, match="power loss"):
        async with db.session_context(begin_transaction=True) as session:
            await m.admit_region(
                session,
                db,
                plan.id,
                s.StreamRegionAdmissionRequest(
                    operation_id=uuid4(), graph_digest=plan.graph_digest
                ),
                credentials,
                _allow_local_discovery=True,
            )
            raise RuntimeError("power loss before commit")
    async with db.session_context() as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 0
        assert (await session.get(db.StreamGraphRevision, plan.id)).state == "SEALED"
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )


async def test_scoped_source_cannot_read_sibling_or_use_flow_credential(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    sibling = next(
        item
        for item in plan.subtasks
        if item.operator_uid == "A" and item.subtask_index == 1
    )
    async with db.session_context(begin_transaction=True) as session:
        bootstrap = await m.bootstrap_subtask(session, db, subtask.id, scoped)
        assert bootstrap.task_run.id == subtask.manifest.task_run_id
        assert "capability" not in bootstrap.model_dump_json()
    store = NativeStreamStore(db, scoped)
    assert (
        await store.status(subtask.manifest.stream_id)
    ).id == subtask.manifest.stream_id
    with pytest.raises(NativeStreamError, match="outside"):
        await store.status(sibling.manifest.stream_id)
    await record_bulk_task_run_events([task_event(plan, subtask, grant).receive()])
    source = await store.claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    await store.append(source, 1, {"document": 1})
    with pytest.raises(NativeStreamError, match="exact subtask"):
        await NativeStreamStore(db, credentials).claim_source(
            subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
        )
    async with db.session_context(begin_transaction=True) as session:
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == flow_run.id
            )
        )
        authority.cancel_requested = True
        authority.source_admission_closed = True
    with pytest.raises(NativeStreamError, match="no longer permits"):
        await store.append(source, 2, {"document": 2})


async def test_same_graph_admission_two_contenders_has_one_winner(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    results = await asyncio.gather(
        admit(db, plan, credentials),
        admit(db, plan, credentials),
        return_exceptions=True,
    )
    assert sum(isinstance(item, tuple) for item in results) == 1
    assert sum(isinstance(item, NativeStreamError) for item in results) == 1
    async with db.session_context() as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 4


async def test_verified_child_replacement_keeps_task_id_history_and_allocation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, claim, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events(
        [task_event(plan, subtask, grant, tags=("shared",)).receive()]
    )
    authorize = s.StreamSubtaskReplacementAuthorizeRequest(
        operation_id=uuid4(),
        expected_grant_id=grant.id,
        expected_physical_epoch=1,
        host_id=grant.host_id,
        challenge_capability="dd" * 32,
    )
    async with db.session_context(begin_transaction=True) as session:
        authorization = await m.authorize_replacement(
            session, db, subtask.id, authorize, credentials
        )
    confirm = s.StreamSubtaskReplacementConfirmRequest(
        operation_id=authorization.operation_id,
        challenge_capability=authorize.challenge_capability,
        death_witness={
            "kind": "WAITPID",
            "observed_at": now("UTC"),
            "host_id": grant.host_id,
            "boot_id": grant.boot_id,
            "child_process_id": grant.child_process_id,
            "process_start_ticks": grant.process_start_ticks,
            "exit_code": -9,
        },
        new_holder_id=uuid4(),
        new_capability="ee" * 32,
        new_event_public_key=PUBLIC,
        new_child_process_id=201,
        new_process_start_ticks=2001,
        host_id=grant.host_id,
        boot_id=grant.boot_id,
    )
    async with db.session_context(begin_transaction=True) as session:
        replacement = await m.confirm_replacement(
            session, db, subtask.id, confirm, credentials
        )
        assert replacement.physical_epoch == 2
        task = await session.get(db.TaskRun, subtask.manifest.task_run_id)
        assert task.run_count == 1
        assert task.state.name == "AwaitingRetry"
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 4
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await m.confirm_replacement(session, db, subtask.id, confirm, credentials)
            == replacement
        )
        history = (
            await session.scalars(
                sa.select(db.TaskRunState)
                .where(db.TaskRunState.task_run_id == subtask.manifest.task_run_id)
                .order_by(db.TaskRunState.timestamp)
            )
        ).all()
        assert [state.name for state in history] == [
            "Pending",
            "Running",
            "AwaitingRetry",
        ]
    with pytest.raises(NativeStreamError, match="no longer current"):
        async with db.session_context(begin_transaction=True) as session:
            await require_subtask(session, db, scoped)
    await record_bulk_task_run_events(
        [
            task_event(
                plan, subtask, replacement, run_count=2, tags=("shared",)
            ).receive()
        ]
    )
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, subtask.manifest.task_run_id)
        assert (
            task.run_count == 2 and task.state_type == schemas.states.StateType.RUNNING
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRun)
                .where(db.TaskRun.flow_run_id == flow_run.id)
            )
            == 4
        )
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 4


async def test_terminal_event_releases_reserved_tag_exactly_once(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events(
        [task_event(plan, subtask, grant, tags=("shared",)).receive()]
    )
    store = NativeStreamStore(db, scoped)
    binding = await store.claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    await store.finish_source(binding)
    event = task_event(
        plan, subtask, grant, state_type="COMPLETED", name="Completed", tags=("shared",)
    )
    await record_bulk_task_run_events([event.receive()])
    await record_bulk_task_run_events([event.receive()])
    async with db.session_context(begin_transaction=True) as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3
        await m.release_subtask(session, db, subtask.id, scoped)
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3


async def test_graph_lock_blocks_parent_and_refreshes_cached_child_after_release(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, credentials)
    subtask, _, _, scoped = await claim_registered(db, plan, credentials)
    async with db.session_context() as cached:
        preload = await cached.get(db.StreamGraphRevision, plan.id)
        assert preload.state == "ADMITTED"
        await cached.commit()
        async with db.session_context(begin_transaction=True) as child:
            await require_subtask(child, db, scoped)
            parent_pid = asyncio.Future()

            async def parent_release() -> None:
                async with db.session_context(begin_transaction=True) as parent:
                    parent_pid.set_result(
                        await parent.scalar(sa.text("SELECT pg_backend_pid()"))
                    )
                    graph = await m._graph(
                        parent, db, plan.id, credentials, update=True
                    )
                    graph.state = "RELEASED"

            parent_task = asyncio.create_task(parent_release())
            pid = await parent_pid
            async with db.session_context() as observer:
                for _ in range(100):
                    await observer.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
                    blocking = await observer.scalar(
                        sa.text("SELECT pg_blocking_pids(:pid)"), {"pid": pid}
                    )
                    if blocking:
                        break
                    await asyncio.sleep(0.01)
                assert blocking and not parent_task.done()
        await asyncio.wait_for(parent_task, 5)
        with pytest.raises(NativeStreamError, match="no longer current"):
            await require_subtask(cached, db, scoped)
        assert preload.state == "RELEASED"
    for operation in (
        NativeStreamStore(db, scoped).status(subtask.manifest.stream_id),
    ):
        with pytest.raises(NativeStreamError, match="no longer current"):
            await operation


async def test_kernel_exit_before_region_commit_rolls_back_all_slots(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    script = """
import asyncio,json,os,sys
from uuid import UUID
from prefect.server.database import provide_database_interface
from prefect.server.models.stream_subtasks import admit_region
from prefect.server.schemas.stream_subtasks import StreamRegionAdmissionRequest
from prefect.server.schemas.streams import StreamExecutionCredentials
body=json.load(sys.stdin)
async def main():
    db=provide_database_interface()
    async with db.session_context(begin_transaction=True) as session:
        plan=await admit_region(session,db,UUID(body["graph_id"]),StreamRegionAdmissionRequest.model_validate(body["request"]),StreamExecutionCredentials.model_validate(body["credentials"]),_allow_local_discovery=True)
        assert plan.state=="ADMITTED"
        print("REGION_ALLOCATED_BEFORE_COMMIT",flush=True)
        os._exit(77)
asyncio.run(main())
"""
    env = dict(os.environ)
    env["PREFECT_SERVER_DATABASE_CONNECTION_URL"] = db.database_config.connection_url
    env["PREFECT_API_DATABASE_CONNECTION_URL"] = db.database_config.connection_url
    env["PREFECT_SERVER_API_AUTH_STRING"] = "native-physical:test"
    body = {
        "graph_id": str(plan.id),
        "request": {"operation_id": str(uuid4()), "graph_digest": plan.graph_digest},
        "credentials": credentials.model_dump(mode="json"),
    }
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", script],
        input=json.dumps(body),
        text=True,
        capture_output=True,
        env=env,
        timeout=30,
    )
    assert result.returncode == 77, (result.stdout, result.stderr)
    assert result.stdout.strip() == "REGION_ALLOCATED_BEFORE_COMMIT"
    async with db.session_context() as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 0
        assert (await session.get(db.StreamGraphRevision, plan.id)).state == "SEALED"
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )


async def test_recorder_refreshes_cached_graph_and_fences_after_release(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, _ = await claim_registered(db, plan, credentials)
    event = task_event(plan, subtask, grant).receive()
    async with db.session_context() as cached:
        graph = await cached.get(db.StreamGraphRevision, plan.id)
        assert graph.state == "ADMITTED"
        await cached.commit()
        async with db.session_context(begin_transaction=True) as parent:
            current = await m._graph(parent, db, plan.id, credentials, update=True)
            current.state = "RELEASED"
        accepted, _ = await _authorized_stream_events(cached, db, [event])
        assert accepted == []
        assert graph.state == "RELEASED"
        assert (
            await cached.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRunState)
                .where(db.TaskRunState.task_run_id == subtask.manifest.task_run_id)
            )
            == 1
        )


async def test_registered_region_requires_explicit_exact_activation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, credentials)
    subtask, claim, grant, scoped = await claim_registered(
        db, plan, credentials, activate=False
    )
    with pytest.raises(NativeStreamError) as refusal:
        async with db.session_context(begin_transaction=True) as session:
            await m.bootstrap_subtask(session, db, subtask.id, scoped)
    assert refusal.value.code == "METADATA_NOT_READY"
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await _authorized_stream_events(
                session, db, [task_event(plan, subtask, grant).receive()]
            )
        )[0] == []
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRunState)
                .where(db.TaskRunState.task_run_id == subtask.manifest.task_run_id)
            )
            == 1
        )
        current = await m.read_graph(session, db, plan.id, credentials)
        request = s.StreamGraphActivateRequest(
            operation_id=uuid4(),
            graph_digest=plan.graph_digest,
            grant_ids=[item.current_grant_id for item in current.subtasks],
        )
        activated = await m.activate_graph(session, db, plan.id, request, credentials)
        assert activated.startup_completed_at is not None
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await m.activate_graph(session, db, plan.id, request, credentials)
            == activated
        )
        assert (
            await m.bootstrap_subtask(session, db, subtask.id, scoped)
        ).grant.id == grant.id
    with pytest.raises(NativeStreamError, match="drift"):
        async with db.session_context(begin_transaction=True) as session:
            await m.activate_graph(
                session,
                db,
                plan.id,
                request.model_copy(update={"operation_id": uuid4()}),
                credentials,
            )
    with pytest.raises(NativeStreamError, match="Activated graph"):
        async with db.session_context(begin_transaction=True) as session:
            await m.abort_graph(
                session,
                db,
                plan.id,
                s.StreamGraphAbortRequest(
                    operation_id=uuid4(), graph_digest=plan.graph_digest, error="late"
                ),
                credentials,
            )


async def test_partial_startup_abort_is_atomic_exact_and_releases_all_slots(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(
        db, plan, credentials, activate=False, registered_count=3
    )
    with pytest.raises(NativeStreamError):
        await NativeStreamStore(db, scoped).claim_source(
            subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
        )
    request = s.StreamGraphAbortRequest(
        operation_id=uuid4(),
        graph_digest=plan.graph_digest,
        error="fourth child could not start",
    )
    with pytest.raises(RuntimeError, match="rollback"):
        async with db.session_context(begin_transaction=True) as session:
            await m.abort_graph(session, db, plan.id, request, credentials)
            raise RuntimeError("rollback")
    async with db.session_context(begin_transaction=True) as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 4
        assert (await session.get(db.StreamGraphRevision, plan.id)).state == "ADMITTED"
        aborted = await m.abort_graph(session, db, plan.id, request, credentials)
        assert aborted.state == "ABORTING"
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 4
    async with db.session_context() as session:
        current = await m.graph_plan(
            session, db, await session.get(db.StreamGraphRevision, plan.id)
        )
        stopped = []
        for item in current.subtasks:
            if item.current_grant_id is not None:
                row = await session.get(db.StreamSubtaskGrant, item.current_grant_id)
                physical = await session.get(db.StreamPhysicalSubtask, item.id)
                stopped.append((item, m.grant_projection(row, physical)))
    for index, (item, registered) in enumerate(stopped):
        receipt = s.StreamGraphStartupStopRequest(
            operation_id=uuid4(),
            abort_operation_id=request.operation_id,
            grant_id=registered.id,
            expected_physical_epoch=registered.physical_epoch,
            death_witness=death_for(registered),
        )
        async with db.session_context(begin_transaction=True) as session:
            result = await m.acknowledge_startup_stop(
                session, db, item.id, receipt, credentials
            )
            assert result.graph_state == (
                "RELEASED" if index == len(stopped) - 1 else "ABORTING"
            )
            assert (
                await session.get(db.ConcurrencyLimitV2, limit_id)
            ).active_slots == (0 if index == len(stopped) - 1 else 4)
    async with db.session_context(begin_transaction=True) as session:
        aborted = await m.abort_graph(session, db, plan.id, request, credentials)
        assert aborted.state == "RELEASED"
        assert (
            await m.abort_graph(session, db, plan.id, request, credentials) == aborted
        )
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 0
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRun)
                .where(
                    db.TaskRun.flow_run_id == flow_run.id,
                    db.TaskRun.state_type == schemas.states.StateType.FAILED,
                    db.TaskRun.run_count == 0,
                )
            )
            == 4
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamPartition)
                .where(
                    db.StreamPartition.flow_run_id == flow_run.id,
                    db.StreamPartition.state == "FAILED",
                    db.StreamPartition.released.is_(True),
                )
            )
            == 4
        )
    with pytest.raises(NativeStreamError, match="drift"):
        async with db.session_context(begin_transaction=True) as session:
            await m.abort_graph(
                session,
                db,
                plan.id,
                request.model_copy(update={"error": "another failure"}),
                credentials,
            )


async def test_signed_nonretriable_failure_then_parent_reconcile_releases_once(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events(
        [task_event(plan, subtask, grant, tags=("shared",)).receive()]
    )
    await NativeStreamStore(db, scoped).claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    for retry in (None, True):
        frame = task_event(
            plan,
            subtask,
            grant,
            state_type="FAILED",
            name="Failed",
            tags=("shared",),
            retriable=retry,
        ).receive()
        async with db.session_context(begin_transaction=True) as session:
            assert (await _authorized_stream_events(session, db, [frame]))[0] == []
    final = task_event(
        plan,
        subtask,
        grant,
        state_type="FAILED",
        name="Failed",
        tags=("shared",),
        retriable=False,
    ).receive()
    await record_bulk_task_run_events([final])
    async with db.session_context() as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 4
        assert (
            await session.get(db.TaskRun, subtask.manifest.task_run_id)
        ).state_type == schemas.states.StateType.FAILED
    # Simulate loss of the physical client after its event committed: only the
    # trusted current Flow holder remains to reconcile the failed output.
    parent = NativeStreamStore(db, credentials)
    await parent.fail_task_source(
        flow_run.id,
        subtask.manifest.stream_id,
        subtask.manifest.task_run_id,
        1,
        "terminal client failure",
    )
    await parent.fail_task_source(
        flow_run.id,
        subtask.manifest.stream_id,
        subtask.manifest.task_run_id,
        1,
        "terminal client failure",
    )
    async with db.session_context(begin_transaction=True) as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3
        assert (await _authorized_stream_events(session, db, [final]))[0] == [final]
    wrong = task_event(plan, subtask, grant, run_count=2, tags=("shared",)).receive()
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [wrong]))[0] == []


async def test_completed_event_cannot_be_replaced_and_disabled_limit_does_not_leak(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events(
        [task_event(plan, subtask, grant, tags=("shared",)).receive()]
    )
    store = NativeStreamStore(db, scoped)
    binding = await store.claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    await store.finish_source(binding)
    async with db.session_context(begin_transaction=True) as session:
        (await session.get(db.ConcurrencyLimitV2, limit_id)).active = False
    final = task_event(
        plan, subtask, grant, state_type="COMPLETED", name="Completed", tags=("shared",)
    ).receive()
    await record_bulk_task_run_events([final])
    forged = task_event(
        plan,
        subtask,
        grant,
        state_type="FAILED",
        name="Failed",
        tags=("shared",),
        retriable=False,
    ).receive()
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [final]))[0] == [final]
        assert (await _authorized_stream_events(session, db, [forged]))[0] == []
        row = await session.get(db.ConcurrencyLimitV2, limit_id)
        assert row.active_slots == 3
        row.active = True

    async def release() -> None:
        async with db.session_context(begin_transaction=True) as session:
            await m.release_subtask(session, db, subtask.id, scoped)

    await asyncio.gather(release(), release())
    async with db.session_context(begin_transaction=True) as session:
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3
        await session.execute(
            sa.delete(db.ConcurrencyLimitV2).where(db.ConcurrencyLimitV2.id == limit_id)
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )


async def authorization_for(
    db: PrefectDBInterface,
    subtask: s.StreamSubtaskProjection,
    grant: s.StreamSubtaskGrant,
    credentials: StreamExecutionCredentials,
) -> tuple[
    s.StreamSubtaskReplacementAuthorizeRequest, s.StreamSubtaskReplacementAuthorization
]:
    request = s.StreamSubtaskReplacementAuthorizeRequest(
        operation_id=uuid4(),
        expected_grant_id=grant.id,
        expected_physical_epoch=grant.physical_epoch,
        host_id=grant.host_id,
        challenge_capability=uuid4().hex + uuid4().hex,
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await m.authorize_replacement(
            session, db, subtask.id, request, credentials
        )
    return request, result


def death_for(grant: s.StreamSubtaskGrant) -> dict[str, object]:
    return {
        "kind": "WAITPID",
        "observed_at": now("UTC"),
        "host_id": grant.host_id,
        "boot_id": grant.boot_id,
        "child_process_id": grant.child_process_id,
        "process_start_ticks": grant.process_start_ticks,
        "exit_code": -9,
    }


async def test_verified_terminal_retirement_exhaustion_and_exact_replay(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    for count in range(1, 4):
        await record_bulk_task_run_events(
            [
                task_event(
                    plan, subtask, grant, run_count=count, tags=("shared",)
                ).receive()
            ]
        )
        store = NativeStreamStore(db, scoped)
        binding = await store.claim_source(
            subtask.manifest.stream_id,
            subtask.manifest.task_run_id,
            grant.holder_id,
            count,
        )
        await store.append(binding, 1, {"document": 1})
        auth, authorized = await authorization_for(db, subtask, grant, credentials)
        request = s.StreamSubtaskVerifiedRetireRequest(
            operation_id=auth.operation_id,
            challenge_capability=auth.challenge_capability,
            death_witness=death_for(grant),
            error="child crashed",
        )
        if count < 3:
            with pytest.raises(NativeStreamError) as lower:
                async with db.session_context(begin_transaction=True) as session:
                    await m.retire_verified_subtask(
                        session, db, subtask.id, request, credentials
                    )
            assert lower.value.code == "RETRY_NOT_EXHAUSTED"
            capability = uuid4().hex + uuid4().hex
            confirm = s.StreamSubtaskReplacementConfirmRequest(
                operation_id=auth.operation_id,
                challenge_capability=auth.challenge_capability,
                death_witness=death_for(grant),
                new_holder_id=uuid4(),
                new_capability=capability,
                new_event_public_key=PUBLIC,
                new_child_process_id=300 + count,
                new_process_start_ticks=3000 + count,
                host_id=grant.host_id,
                boot_id=grant.boot_id,
            )
            async with db.session_context(begin_transaction=True) as session:
                grant = await m.confirm_replacement(
                    session, db, subtask.id, confirm, credentials
                )
                assert grant.state == "REGISTERED"
            scoped = s.StreamSubtaskCredentials(
                grant_id=grant.id, capability=capability
            )
        else:
            async with db.session_context(begin_transaction=True) as session:
                retired = await m.retire_verified_subtask(
                    session, db, subtask.id, request, credentials
                )
                assert (
                    retired.task_state == "FAILED"
                    and retired.outcome_kind == "RETRY_EXHAUSTED"
                )
                assert (
                    await session.get(db.ConcurrencyLimitV2, limit_id)
                ).active_slots == 3
                assert (
                    await session.get(db.TaskRun, subtask.manifest.task_run_id)
                ).run_count == 3
            async with db.session_context(begin_transaction=True) as session:
                assert (
                    await m.retire_verified_subtask(
                        session, db, subtask.id, request, credentials
                    )
                    == retired
                )
                operation = await session.scalar(
                    sa.select(db.StreamSubtaskReplacement).where(
                        db.StreamSubtaskReplacement.operation_id == auth.operation_id
                    )
                )
                assert (
                    operation.state == "RETIRED"
                    and operation.death_witness["child_process_id"]
                    == grant.child_process_id
                )
                assert (
                    await session.scalar(
                        sa.select(sa.func.count())
                        .select_from(db.TaskRun)
                        .where(db.TaskRun.flow_run_id == flow_run.id)
                    )
                    == 4
                )
            with pytest.raises(NativeStreamError, match="drift"):
                async with db.session_context(begin_transaction=True) as session:
                    await m.retire_verified_subtask(
                        session,
                        db,
                        subtask.id,
                        request.model_copy(update={"error": "changed"}),
                        credentials,
                    )


@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED"])
async def test_verified_retirement_reconciles_output_before_terminal_event(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, terminal: str
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events(
        [task_event(plan, subtask, grant, tags=("shared",)).receive()]
    )
    store = NativeStreamStore(db, scoped)
    binding = await store.claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    await store.append(binding, 1, {"document": 1})
    await store.finish_source(
        binding, None if terminal == "COMPLETED" else "permanent output failure"
    )
    async with db.session_context() as session:
        assert (
            await session.get(db.TaskRun, subtask.manifest.task_run_id)
        ).state_type == schemas.states.StateType.RUNNING
    auth, _ = await authorization_for(db, subtask, grant, credentials)
    request = s.StreamSubtaskVerifiedRetireRequest(
        operation_id=auth.operation_id,
        challenge_capability=auth.challenge_capability,
        death_witness=death_for(grant),
        error="child died after durable terminal output",
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await m.retire_verified_subtask(
            session, db, subtask.id, request, credentials
        )
        assert result.task_state == terminal
        assert result.outcome_kind == terminal + "_RECONCILED"
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3
        task = await session.get(db.TaskRun, subtask.manifest.task_run_id)
        assert task.state_type.value == terminal and task.run_count == 1
    async with db.session_context(begin_transaction=True) as session:
        assert (
            await m.retire_verified_subtask(
                session, db, subtask.id, request, credentials
            )
            == result
        )
        assert (await session.get(db.StreamSubtaskGrant, grant.id)).state == "RETIRED"


async def test_physical_epoch_64_can_retire_before_first_running(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, claim, grant, _ = await claim_registered(db, plan, credentials)
    # Seed the bounded last-attempt boundary through the real grant constructor;
    # this is not evidence of 64 OS executions.
    async with db.session_context(begin_transaction=True) as session:
        graph, row = await m._subtask(session, db, subtask.id, credentials, update=True)
        old = await session.get(db.StreamSubtaskGrant, grant.id)
        old.state = "RETIRED"
        request = claim.model_copy(
            update={
                "operation_id": uuid4(),
                "holder_id": uuid4(),
                "capability": uuid4().hex + uuid4().hex,
            }
        )
        last = await m._new_grant(session, db, graph, row, request, 64)
        grant = await m.register_subtask(
            session,
            db,
            subtask.id,
            s.StreamSubtaskRegistrationRequest(
                operation_id=uuid4(),
                grant_id=last.id,
                child_process_id=640,
                process_start_ticks=6400,
            ),
            credentials,
        )
    auth, _ = await authorization_for(db, subtask, grant, credentials)
    request = s.StreamSubtaskVerifiedRetireRequest(
        operation_id=auth.operation_id,
        challenge_capability=auth.challenge_capability,
        death_witness=death_for(grant),
        error="last process died before Running",
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await m.retire_verified_subtask(
            session, db, subtask.id, request, credentials
        )
        assert (
            result.physical_epoch == 64
            and result.task_state == "FAILED"
            and result.outcome_kind == "ATTEMPT_LIMIT"
        )
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3
        task = await session.get(db.TaskRun, subtask.manifest.task_run_id)
        assert task.run_count == 0 and task.state.state_details.retriable is False


def resign_event(
    event: Event,
    plan: s.StreamGraphPlan,
    subtask: s.StreamSubtaskProjection,
    grant: s.StreamSubtaskGrant,
) -> None:
    witness = canonical_physical_event_witness(
        event_id=event.id,
        event=event.event,
        occurred=event.occurred,
        flow_run_id=plan.flow_run_id,
        task_run_id=subtask.manifest.task_run_id,
        generation=grant.physical_epoch,
        holder_id=grant.holder_id,
        manifest_digest=subtask.manifest.manifest_digest,
        run_count=int(event.resource["prefect.run-count"]),
        task_run=event.payload["task_run"],
        validated_state=event.payload["validated_state"],
        graph_revision_id=plan.id,
        assignment_epoch=1,
        subtask_id=subtask.id,
        grant_id=grant.id,
        physical_epoch=grant.physical_epoch,
        scope_digest=grant.scope_digest,
    )
    event.resource["prefect.stream-event-signature"] = sign_task_event(PRIVATE, witness)


async def test_signed_sibling_natural_key_poison_and_same_id_terminal_drift_are_rejected(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    sibling = next(
        item
        for item in plan.subtasks
        if item.operator_uid == "A" and item.subtask_index == 1
    )
    poison = task_event(plan, subtask, grant)
    poison.payload["task_run"]["dynamic_key"] = sibling.manifest.dynamic_key
    poison.payload["task_run"]["task_key"] = "source-key"
    resign_event(poison, plan, subtask, grant)
    await record_bulk_task_run_events([poison.receive()])
    async with db.session_context() as session:
        assert (
            await session.get(db.TaskRun, subtask.manifest.task_run_id)
        ).run_count == 0
        assert (
            await session.get(db.TaskRun, sibling.manifest.task_run_id)
        ).run_count == 0
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRunState)
                .where(
                    db.TaskRunState.task_run_id.in_(
                        [subtask.manifest.task_run_id, sibling.manifest.task_run_id]
                    )
                )
            )
            == 2
        )
    await record_bulk_task_run_events([task_event(plan, subtask, grant).receive()])
    store = NativeStreamStore(db, scoped)
    binding = await store.claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    await store.finish_source(binding)
    completed = task_event(
        plan, subtask, grant, state_type="COMPLETED", name="Completed"
    )
    await record_bulk_task_run_events([completed.receive()])
    altered = completed.model_copy(deep=True)
    altered.payload["validated_state"]["message"] = (
        "same event id, different signed message"
    )
    resign_event(altered, plan, subtask, grant)
    async with db.session_context(begin_transaction=True) as session:
        assert (await _authorized_stream_events(session, db, [completed.receive()]))[0]
        assert (await _authorized_stream_events(session, db, [altered.receive()]))[
            0
        ] == []
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRunState)
                .where(db.TaskRunState.task_run_id == subtask.manifest.task_run_id)
            )
            == 3
        )


async def test_concurrent_limit_delete_and_terminal_release_has_no_lock_cycle(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events(
        [task_event(plan, subtask, grant, tags=("shared",)).receive()]
    )
    await NativeStreamStore(db, scoped).claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    await record_bulk_task_run_events(
        [
            task_event(
                plan,
                subtask,
                grant,
                state_type="FAILED",
                name="Failed",
                tags=("shared",),
                retriable=False,
            ).receive()
        ]
    )
    backend_pid = asyncio.Future()

    async def release() -> None:
        async with db.session_context(begin_transaction=True) as session:
            backend_pid.set_result(
                await session.scalar(sa.text("SELECT pg_backend_pid()"))
            )
            await NativeStreamStore(db, credentials, session=session).fail_task_source(
                flow_run.id,
                subtask.manifest.stream_id,
                subtask.manifest.task_run_id,
                1,
                "terminal failure",
            )

    async with db.session_context(begin_transaction=True) as deletion:
        deleting_pid = await deletion.scalar(sa.text("SELECT pg_backend_pid()"))
        await deletion.execute(
            sa.delete(db.ConcurrencyLimitV2).where(db.ConcurrencyLimitV2.id == limit_id)
        )
        release_task = asyncio.create_task(release())
        pid = await backend_pid
        async with db.session_context() as observer:
            for _ in range(100):
                await observer.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
                blockers = await observer.scalar(
                    sa.text("SELECT pg_blocking_pids(:pid)"), {"pid": pid}
                )
                if deleting_pid in blockers:
                    break
                await asyncio.sleep(0.01)
            assert deleting_pid in blockers and not release_task.done()
    await asyncio.wait_for(release_task, 5)
    async with db.session_context() as session:
        assert await session.get(db.ConcurrencyLimitV2, limit_id) is None
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )
        assert (
            await session.get(db.StreamPartition, subtask.manifest.stream_id)
        ).state == "FAILED"


async def test_seal_after_running_records_authoritative_flow_attempt(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    await _recoverable_flow(db, flow_run)
    request = _claim(flow_run.id)
    credentials = _credentials(request)
    async with db.session_context(begin_transaction=True) as session:
        await claim_execution(session, db, request)
        authority = await guard_flow_lifecycle(
            session, db, flow_run.id, credentials, schemas.states.Running(), force=False
        )
        state = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Running(),
            flow_policy=CoreFlowPolicy,
        )
        await apply_flow_lifecycle_result(session, db, authority, state.state)
        plan = await m.seal_graph(
            session,
            db,
            graph_request(flow_run.id),
            credentials,
            _allow_local_discovery=True,
        )
        counts = (
            await session.scalars(
                sa.select(db.TaskRun.flow_run_run_count).where(
                    db.TaskRun.flow_run_id == flow_run.id
                )
            )
        ).all()
        assert counts == [1] * 4
    _, plan = await admit(db, plan, credentials)
    subtask, _, _, scoped = await claim_registered(db, plan, credentials)
    async with db.session_context(begin_transaction=True) as session:
        boot = await m.bootstrap_subtask(session, db, subtask.id, scoped)
        assert boot.flow_run_run_count == boot.task_run.flow_run_run_count == 1


@pytest.mark.parametrize("role", ["SOURCE", "OPERATOR"])
async def test_unowned_rejection_is_scoped_and_verified_terminally_reconciled(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, role: str
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    selected = next(
        item
        for item in plan.subtasks
        if item.manifest.role == role and item.subtask_index == 0
    )
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials, selected)
    sibling = next(
        item
        for item in plan.subtasks
        if item.manifest.role == role and item.subtask_index == 1
    )
    store = NativeStreamStore(db, scoped)
    reject = (
        store.reject_reserved_source
        if role == "SOURCE"
        else store.reject_reserved_output
    )
    with pytest.raises(NativeStreamError) as denied:
        await reject(
            flow_run.id,
            sibling.manifest.stream_id,
            sibling.manifest.task_run_id,
            "must not touch sibling",
        )
    assert denied.value.code == "SUBTASK_SCOPE_DENIED"
    await reject(
        flow_run.id,
        subtask.manifest.stream_id,
        subtask.manifest.task_run_id,
        "unowned submission rejected",
    )
    auth, _ = await authorization_for(db, subtask, grant, credentials)
    request = s.StreamSubtaskVerifiedRetireRequest(
        operation_id=auth.operation_id,
        challenge_capability=auth.challenge_capability,
        death_witness=death_for(grant),
        error="reconcile unowned rejection",
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await m.retire_verified_subtask(
            session, db, subtask.id, request, credentials
        )
        assert (
            result.task_state == "FAILED" and result.outcome_kind == "FAILED_RECONCILED"
        )
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3
        assert (
            await session.get(db.StreamPartition, sibling.manifest.stream_id)
        ).state == "OPEN"


async def test_confirm_open_read_then_terminal_output_uses_same_retirement_operation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events([task_event(plan, subtask, grant).receive()])
    store = NativeStreamStore(db, scoped)
    binding = await store.claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    auth, _ = await authorization_for(db, subtask, grant, credentials)
    assert (await store.status(subtask.manifest.stream_id)).state == "OPEN"
    await store.finish_source(binding)
    confirm = s.StreamSubtaskReplacementConfirmRequest(
        operation_id=auth.operation_id,
        challenge_capability=auth.challenge_capability,
        death_witness=death_for(grant),
        new_holder_id=uuid4(),
        new_capability="dd" * 32,
        new_event_public_key=PUBLIC,
        new_child_process_id=999,
        new_process_start_ticks=9999,
        host_id=grant.host_id,
        boot_id=grant.boot_id,
    )
    with pytest.raises(NativeStreamError) as refused:
        async with db.session_context(begin_transaction=True) as session:
            await m.confirm_replacement(session, db, subtask.id, confirm, credentials)
    assert refused.value.code == "TERMINAL_RECONCILIATION_REQUIRED"
    async with db.session_context(begin_transaction=True) as session:
        operation = await session.scalar(
            sa.select(db.StreamSubtaskReplacement).where(
                db.StreamSubtaskReplacement.operation_id == auth.operation_id
            )
        )
        assert operation.state == "AUTHORIZED"
        assert (
            await session.get(db.TaskRun, subtask.manifest.task_run_id)
        ).state_type == schemas.states.StateType.RUNNING
        request = s.StreamSubtaskVerifiedRetireRequest(
            operation_id=auth.operation_id,
            challenge_capability=auth.challenge_capability,
            death_witness=death_for(grant),
            error="EOF committed during recovery decision",
        )
        result = await m.retire_verified_subtask(
            session, db, subtask.id, request, credentials
        )
        assert result.task_state == "COMPLETED" and result.grant_id == grant.id


async def test_signed_nonretriable_crash_releases_after_exact_output_failure(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    _, plan, credentials = await prepare(db, flow_run, tags=("shared",))
    async with db.session_context(begin_transaction=True) as session:
        limit = db.ConcurrencyLimitV2(name="tag:shared", limit=4, active=True)
        session.add(limit)
        await session.flush()
        limit_id = limit.id
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    await record_bulk_task_run_events(
        [task_event(plan, subtask, grant, tags=("shared",)).receive()]
    )
    await NativeStreamStore(db, scoped).claim_source(
        subtask.manifest.stream_id, subtask.manifest.task_run_id, grant.holder_id, 1
    )
    crashed = task_event(
        plan,
        subtask,
        grant,
        state_type="CRASHED",
        name="Crashed",
        tags=("shared",),
        retriable=False,
    ).receive()
    await record_bulk_task_run_events([crashed])
    async with db.session_context() as session:
        assert (
            await session.get(db.TaskRun, subtask.manifest.task_run_id)
        ).state_type == schemas.states.StateType.CRASHED
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 4
    await NativeStreamStore(db, credentials).fail_task_source(
        flow_run.id,
        subtask.manifest.stream_id,
        subtask.manifest.task_run_id,
        1,
        "terminal crash",
    )
    auth, _ = await authorization_for(db, subtask, grant, credentials)
    request = s.StreamSubtaskVerifiedRetireRequest(
        operation_id=auth.operation_id,
        challenge_capability=auth.challenge_capability,
        death_witness=death_for(grant),
        error="verified crashed child",
    )
    async with db.session_context(begin_transaction=True) as session:
        result = await m.retire_verified_subtask(
            session, db, subtask.id, request, credentials
        )
        assert result.task_state == "FAILED"
        assert (await session.get(db.ConcurrencyLimitV2, limit_id)).active_slots == 3
