"""Real PostgreSQL routed-store controls, without a public DAG executor.

The fixture admits metadata workers and uses the ordinary Flow/Task policies.
Only the internal, session-bound discovery authorization permits routed DATA;
no process, user Task body, grant or public execution-ready flag is fabricated.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from prefect._internal.stream_key_wire import encode_tagged_key_wire_v1
from prefect._internal.stream_route_plan import (
    canonical_route_json,
    route_outcome_digest,
)
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import flow_runs, stream_dags, stream_routes, task_runs
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_executions import (
    apply_flow_lifecycle_result,
    claim_execution,
    guard_flow_lifecycle,
)
from prefect.server.orchestration.core_policy import CoreFlowPolicy, CoreTaskPolicy
from prefect.server.schemas import states
from prefect.server.schemas import stream_dags as topology
from prefect.server.schemas import stream_routes as wire
from prefect.server.schemas.streams import (
    ConsumerBinding,
    SourceBinding,
    StreamDelivery,
    StreamExecutionCredentials,
)
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING, temporary_settings
from tests.server.models.test_stream_dags import dag_request, metadata_workers
from tests.server.models.test_stream_executions import (
    _claim,
    _credentials,
    _recoverable_flow,
)

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


@pytest.fixture(autouse=True)
async def postgres_17(
    db: PrefectDBInterface, request: pytest.FixtureRequest
) -> AsyncGenerator[None, None]:
    engine = await db.engine()
    if request.node.name.startswith("test_sqlite_"):
        if db.dialect.name != "sqlite":
            pytest.skip("SQLite backend-refusal control")
    elif (
        db.dialect.name != "postgresql"
        or (engine.dialect.server_version_info or (0,))[0] != 17
    ):
        pytest.skip("DAG routed DATA requires PostgreSQL 17")
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-dag:test"}):
        yield


@dataclass(frozen=True)
class RoutingRegion:
    plan: topology.StreamDAGPlan
    execution: StreamExecutionCredentials

    def subtask(
        self, operator: str, index: int = 0
    ) -> topology.StreamDAGSubtaskProjection:
        return next(
            row
            for row in self.plan.subtasks
            if row.operator_uid == operator and row.subtask_index == index
        )


async def routing_region(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    *,
    shape: Literal["fanout", "linear", "chain"] = "fanout",
    source_p: int = 1,
    target_p: int = 2,
    exchange: Literal["forward", "rebalance", "broadcast", "keyed"] = "broadcast",
) -> RoutingRegion:
    """Build a genuine admitted region; no worker/process execution is claimed."""
    await _recoverable_flow(db, flow_run)
    claim = _claim(flow_run.id)
    execution = _credentials(claim)
    body = dag_request(flow_run.id, source_p=source_p, branch_p=target_p).model_dump(
        mode="json"
    )
    body["flow_version"] = "flow-v1"
    if shape == "chain":
        body["edges"][1].update(edge_uid="B-C", producer_operator_uid="B")
    elif shape == "linear":
        body["operators"] = body["operators"][:2]
        body["operators"][1].update(
            parallelism=target_p, parameters=[{} for _ in range(target_p)]
        )
        body["edges"] = body["edges"][:1]
        body["edges"][0]["exchange"] = exchange
        if exchange == "keyed":
            body["edges"][0].update(
                key_codec="tagged-key-v1", hash_algorithm="sha256", key_group_count=16
            )
    request = topology.StreamDAGSealRequest.model_validate(body)
    async with db.session_context(begin_transaction=True) as session:
        await claim_execution(session, db, claim)
        plan = await stream_dags.seal_dag_metadata(session, db, request)
        pool, _ = await metadata_workers(session, db, math.ceil(len(plan.subtasks) / 2))
        plan = await stream_dags.admit_dag_metadata(
            session,
            db,
            plan.id,
            topology.StreamDAGAdmissionRequest(
                operation_id=uuid4(), graph_digest=plan.graph_digest
            ),
            work_pool_id=pool.id,
        )
        running = states.Running()
        authority = await guard_flow_lifecycle(
            session, db, flow_run.id, execution, running, force=False
        )
        result = await flow_runs.set_flow_run_state(
            session, flow_run.id, running, flow_policy=CoreFlowPolicy
        )
        assert result.state is not None and result.state.is_running()
        await apply_flow_lifecycle_result(session, db, authority, result.state)
        for subtask in plan.subtasks:
            result = await task_runs.set_task_run_state(
                session,
                subtask.manifest.task_run_id,
                states.Running(),
                task_policy=CoreTaskPolicy,
            )
            assert result.state is not None and result.state.is_running()
        plan = await stream_dags.read_dag_plan(session, db, plan.id, metadata_only=True)
    return RoutingRegion(plan, execution)


@asynccontextmanager
async def route_store(
    db: PrefectDBInterface, region: RoutingRegion, operator: str, index: int = 0
) -> AsyncGenerator[
    tuple[AsyncSession, stream_routes.NativeDAGRouteStore, wire.StreamDAGRouteContext],
    None,
]:
    async with db.session_context(begin_transaction=True) as session:
        context = stream_routes.context_for_subtask(
            region.plan, region.subtask(operator, index).id
        )
        authorization = await stream_routes.authorize_dag_routing_discovery(
            session, db, context, region.execution
        )
        yield (
            session,
            stream_routes.NativeDAGRouteStore(session, db, authorization),
            context,
        )


async def claim_source(
    db: PrefectDBInterface, region: RoutingRegion, index: int = 0
) -> SourceBinding:
    async with route_store(db, region, "A", index) as (_, store, _):
        return (await store.claim_source(uuid4())).binding


def source_request(
    region: RoutingRegion,
    binding: SourceBinding,
    position: int,
    value: Any,
    *,
    index: int = 0,
    key: Any = None,
) -> wire.StreamDAGSourceAppendRequest:
    context = stream_routes.context_for_subtask(
        region.plan, region.subtask("A", index).id
    )
    return wire.StreamDAGSourceAppendRequest.model_validate(
        {
            "context": context,
            "binding": binding,
            "position": position,
            "value": value,
            "output_key": encode_tagged_key_wire_v1(key)
            if context.has_keyed_outgoing
            else None,
        }
    )


async def append(
    db: PrefectDBInterface,
    region: RoutingRegion,
    request: wire.StreamDAGSourceAppendRequest,
    *,
    index: int = 0,
) -> wire.StreamDAGRouteOutcome:
    async with route_store(db, region, "A", index) as (_, store, _):
        return await store.append_source(request)


async def open_input(
    db: PrefectDBInterface, region: RoutingRegion, operator: str = "B", index: int = 0
) -> ConsumerBinding:
    async with route_store(db, region, operator, index) as (_, store, _):
        return await store.open_consumer(uuid4())


async def read_input(
    db: PrefectDBInterface,
    region: RoutingRegion,
    binding: ConsumerBinding,
    operator: str = "B",
    index: int = 0,
) -> StreamDelivery | None:
    async with route_store(db, region, operator, index) as (_, store, _):
        return await store.read_consumer(binding)


def consumer_request(
    region: RoutingRegion,
    binding: ConsumerBinding,
    delivery: StreamDelivery,
    *,
    operator: str = "B",
    index: int = 0,
    kind: Literal["YIELD", "RETURN"] = "YIELD",
    value: Any = None,
    state: dict[str, Any] | None = None,
) -> wire.StreamDAGConsumerCompleteRequest:
    context = stream_routes.context_for_subtask(
        region.plan, region.subtask(operator, index).id
    )
    return wire.StreamDAGConsumerCompleteRequest.model_validate(
        {
            "context": context,
            "binding": binding,
            "token": delivery.token,
            "kind": kind,
            "value": value,
            "managed_state": state or {},
            "output_key": encode_tagged_key_wire_v1(None)
            if kind == "YIELD" and context.has_keyed_outgoing
            else None,
        }
    )


async def complete(
    db: PrefectDBInterface,
    region: RoutingRegion,
    request: wire.StreamDAGConsumerCompleteRequest,
    operator: str = "B",
    index: int = 0,
) -> wire.StreamDAGRouteOutcome:
    async with route_store(db, region, operator, index) as (_, store, _):
        return await store.complete_consumer(request)


async def contents(db: PrefectDBInterface, region: RoutingRegion) -> dict[str, Any]:
    """Read actual rows and progress, independently of the returned receipt."""
    async with db.session_context() as session:
        records = (
            await session.execute(
                sa.select(
                    db.StreamRecord.id,
                    db.StreamRecord.stream_id,
                    db.StreamRecord.sequence,
                    db.StreamRecord.record_kind,
                    db.StreamRecord.payload_record_id,
                    db.StreamRecord.value,
                )
                .where(db.StreamRecord.graph_revision_id == region.plan.id)
                .order_by(db.StreamRecord.id)
            )
        ).all()
        headers = (
            await session.execute(
                sa.select(
                    db.StreamPartition.id,
                    db.StreamPartition.last_sequence,
                    db.StreamPartition.producer_position,
                    db.StreamPartition.record_count,
                    db.StreamPartition.byte_count,
                    db.StreamPartition.state,
                    db.StreamPartition.tail_sequence,
                )
                .where(db.StreamPartition.flow_run_id == region.plan.flow_run_id)
                .order_by(db.StreamPartition.id)
            )
        ).all()
        consumers = (
            await session.execute(
                sa.select(
                    db.StreamConsumer.id,
                    db.StreamConsumer.completed_through,
                    db.StreamConsumer.managed_state,
                    db.StreamConsumer.inflight_sequence,
                    db.StreamConsumer.delivery_id,
                )
                .where(db.StreamConsumer.graph_revision_id == region.plan.id)
                .order_by(db.StreamConsumer.id)
            )
        ).all()
        receipts = (
            await session.scalars(
                sa.select(db.StreamRouteReceipt.id)
                .where(db.StreamRouteReceipt.graph_revision_id == region.plan.id)
                .order_by(db.StreamRouteReceipt.id)
            )
        ).all()
        completions = (
            await session.execute(
                sa.select(
                    db.StreamCompletion.consumer_id,
                    db.StreamCompletion.input_sequence,
                    db.StreamCompletion.kind,
                    db.StreamCompletion.route_receipt_id,
                )
                .join(
                    db.StreamConsumer,
                    db.StreamConsumer.id == db.StreamCompletion.consumer_id,
                )
                .where(db.StreamConsumer.graph_revision_id == region.plan.id)
                .order_by(db.StreamCompletion.id)
            )
        ).all()
        return {
            "records": records,
            "headers": headers,
            "consumers": consumers,
            "receipts": receipts,
            "completions": completions,
        }


async def test_postgres_region_fixture_uses_normal_lifecycle_and_no_data(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    assert region.plan.execution_ready is False and len(region.plan.subtasks) == 4
    async with db.session_context() as session:
        runs = (
            await session.scalars(
                sa.select(db.TaskRun).where(db.TaskRun.flow_run_id == flow_run.id)
            )
        ).all()
        assert len(runs) == 4 and all(
            row.state_type == states.StateType.RUNNING and row.run_count == 1
            for row in runs
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskGrant)
            )
            == 0
        )
    assert not (await contents(db, region))["records"]


async def test_postgres_source_payload_fanout_and_lost_ack_exact_replay(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    binding = await claim_source(db, region)
    request = source_request(region, binding, 1, {"ref": "object://documents/1"})
    first = await append(db, region, request)
    before = await contents(db, region)
    replay = await append(db, region, request)
    assert replay.model_dump() == first.model_dump()
    assert await contents(db, region) == before
    assert len(first.references) == 3
    assert len(before["records"]) == 4
    assert sum(row.record_kind == "PAYLOAD" for row in before["records"]) == 1
    assert sum(row.record_kind == "REFERENCE" for row in before["records"]) == 3
    assert {r.payload_record_id for r in first.references} == {first.payload_record_id}
    assert len(before["receipts"]) == 1


async def test_postgres_corrupt_stored_reference_payload_id_refuses_replay_without_allocation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    binding = await claim_source(db, region)
    request = source_request(region, binding, 1, {"value": 1})
    original = await append(db, region, request)
    async with db.session_context(begin_transaction=True) as session:
        receipt = await session.get(db.StreamRouteReceipt, original.receipt_id)
        assert receipt is not None
        corrupted = {
            **receipt.outcome,
            "references": [dict(ref) for ref in receipt.outcome["references"]],
        }
        corrupted["references"][0]["payload_record_id"] = str(uuid4())
        receipt.outcome = corrupted
    before = await contents(db, region)
    with pytest.raises(NativeStreamError) as refused:
        await append(db, region, request)
    assert refused.value.code == "IDENTITY_CONFLICT"
    assert await contents(db, region) == before
    async with db.session_context() as session:
        receipt = await session.get(db.StreamRouteReceipt, original.receipt_id)
        assert receipt is not None and receipt.outcome == corrupted


async def test_postgres_corrupt_outcome_edge_with_valid_digest_refuses_reference_read(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    source = await claim_source(db, region)
    outcome = await append(db, region, source_request(region, source, 1, None))
    reference = outcome.references[0]
    target = next(
        row
        for row in region.plan.subtasks
        if row.input_stream_id == reference.stream_id
    )
    binding = await open_input(db, region, target.operator_uid, target.subtask_index)
    async with db.session_context(begin_transaction=True) as session:
        receipt = await session.get(db.StreamRouteReceipt, outcome.receipt_id)
        assert receipt is not None
        corrupted = {
            **receipt.outcome,
            "references": [dict(item) for item in receipt.outcome["references"]],
        }
        corrupted["references"][0]["edge_id"] = str(uuid4())
        corrupted["references"].sort(
            key=lambda item: (
                UUID(item["edge_id"]).bytes,
                UUID(item["edge_partition_id"]).bytes,
                UUID(item["record_id"]).bytes,
            )
        )
        corrupted["outcome_digest"] = route_outcome_digest(corrupted)
        wire.StreamDAGRouteOutcome.model_validate(corrupted)
        receipt.outcome = corrupted
        receipt.outcome_digest = corrupted["outcome_digest"]
        receipt.outcome_size = len(canonical_route_json(corrupted))
    before = await contents(db, region)
    with pytest.raises(NativeStreamError) as refused:
        await read_input(db, region, binding, target.operator_uid, target.subtask_index)
    assert refused.value.code == "REFERENCE_IDENTITY_CONFLICT"
    assert await contents(db, region) == before
    async with db.session_context() as session:
        receipt = await session.get(db.StreamRouteReceipt, outcome.receipt_id)
        assert receipt is not None and receipt.outcome == corrupted


@pytest.mark.parametrize("part", ["input", "output"])
async def test_postgres_locked_limits_must_match_the_frozen_operator_declaration(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, part: str
):
    region = await routing_region(db, flow_run)
    source = await claim_source(db, region)
    await append(db, region, source_request(region, source, 1, None))
    binding = await open_input(db, region)
    delivery = await read_input(db, region, binding)
    assert delivery is not None
    stream_id = binding.input_stream_id if part == "input" else binding.output_stream_id
    async with db.session_context(begin_transaction=True) as session:
        header = await session.get(db.StreamPartition, stream_id)
        assert header is not None
        changed = {
            **header.limits,
            "max_state_bytes": header.limits["max_state_bytes"] + 1,
        }
        header.limits = changed
    before = await contents(db, region)
    with pytest.raises(NativeStreamError) as refused:
        await complete(
            db,
            region,
            consumer_request(region, binding, delivery, value=None, state={"count": 1}),
        )
    assert refused.value.code == "IDENTITY_CONFLICT"
    assert await contents(db, region) == before
    async with db.session_context() as session:
        header = await session.get(db.StreamPartition, stream_id)
        assert header is not None and header.limits == changed


async def test_sqlite_discovery_authorization_refuses_before_any_mutation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await stream_dags.seal_dag_metadata(
            session, db, dag_request(flow_run.id)
        )
    execution = StreamExecutionCredentials(
        generation=1, holder_id=uuid4(), capability="11" * 32
    )
    region = RoutingRegion(plan, execution)
    before = await contents(db, region)

    async def metadata_rows() -> dict[str, Any]:
        async with db.session_context() as session:
            return {
                model.__tablename__: (
                    await session.execute(
                        sa.select(model.__table__).order_by(model.__table__.c.id)
                    )
                ).all()
                for model in (
                    db.FlowRun,
                    db.FlowRunState,
                    db.TaskRun,
                    db.TaskRunState,
                    db.StreamGraphRevision,
                    db.StreamPhysicalSubtask,
                    db.StreamPhysicalSlotAllocation,
                    db.StreamSubtaskAllocation,
                    db.StreamSubtaskGrant,
                    db.StreamExecutionAuthority,
                )
            }

    metadata_before = await metadata_rows()
    context = stream_routes.context_for_subtask(plan, region.subtask("A").id)
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(NativeStreamError) as refused:
            await stream_routes.authorize_dag_routing_discovery(
                session, db, context, execution
            )
        assert refused.value.code == "UNSUPPORTED_BACKEND"
    assert await contents(db, region) == before
    assert await metadata_rows() == metadata_before


async def test_postgres_retained_source_receipt_replays_after_eof_but_new_data_refuses(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    binding = await claim_source(db, region)
    request = source_request(region, binding, 1, None)
    original = await append(db, region, request)
    async with route_store(db, region, "A") as (_, store, _):
        await store.finish_producer()
    before = await contents(db, region)
    assert (await append(db, region, request)).model_dump() == original.model_dump()
    with pytest.raises(NativeStreamError) as refused:
        await append(db, region, source_request(region, binding, 2, None))
    assert refused.value.code == "TERMINAL_CONFLICT"
    assert await contents(db, region) == before


async def test_postgres_current_retried_source_owner_gets_the_original_route_receipt(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    old_binding = await claim_source(db, region)
    original = await append(
        db, region, source_request(region, old_binding, 1, {"value": 1})
    )
    async with db.session_context(begin_transaction=True) as session:
        retry = await task_runs.set_task_run_state(
            session,
            region.subtask("A").manifest.task_run_id,
            states.Failed(state_details=states.StateDetails(retriable=True)),
            task_policy=CoreTaskPolicy,
        )
        assert retry.state is not None and retry.state.is_scheduled()
        running = await task_runs.set_task_run_state(
            session,
            region.subtask("A").manifest.task_run_id,
            states.Running(),
            task_policy=CoreTaskPolicy,
        )
        assert running.state is not None and running.state.is_running()
    current = await claim_source(db, region)
    assert current.attempt == 2 and current.owner_epoch == old_binding.owner_epoch + 1
    before = await contents(db, region)
    replay = await append(db, region, source_request(region, current, 1, {"value": 1}))
    assert replay.model_dump() == original.model_dump()
    assert await contents(db, region) == before


@pytest.mark.parametrize("field", ["generation", "holder_id", "capability"])
async def test_postgres_current_flow_authority_is_required_even_for_known_receipt(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, field: str
):
    region = await routing_region(db, flow_run)
    binding = await claim_source(db, region)
    await append(db, region, source_request(region, binding, 1, None))
    before = await contents(db, region)
    replacement: Any = (
        2 if field == "generation" else uuid4() if field == "holder_id" else "ff" * 32
    )
    execution = region.execution.model_copy(update={field: replacement})
    context = stream_routes.context_for_subtask(region.plan, region.subtask("A").id)
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(NativeStreamError) as refused:
            await stream_routes.authorize_dag_routing_discovery(
                session, db, context, execution
            )
        assert refused.value.code == "STALE_EXECUTION"
    assert await contents(db, region) == before


@pytest.mark.parametrize("changed", ["value", "key", "routing"])
async def test_postgres_changed_source_semantics_conflict_without_mutation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, changed: str
):
    region = await routing_region(
        db, flow_run, shape="linear", target_p=3, exchange="keyed"
    )
    binding = await claim_source(db, region)
    request = source_request(region, binding, 1, {"value": 1}, key=None)
    await append(db, region, request)
    before = await contents(db, region)
    replacement = source_request(
        region,
        binding,
        1,
        {"value": 2} if changed == "value" else {"value": 1},
        key=False if changed == "key" else None,
    )
    if changed == "routing":
        replacement = replacement.model_copy(
            update={
                "context": replacement.context.model_copy(
                    update={"routing_digest": "f" * 64}
                )
            }
        )
    with pytest.raises(NativeStreamError) as refused:
        await append(db, region, replacement)
    assert refused.value.code == "IDENTITY_CONFLICT"
    assert await contents(db, region) == before


class InjectedRouteFailure(RuntimeError):
    """Failure after a real database INSERT, before the transaction can commit."""


@pytest.mark.parametrize("source", [False, True])
async def test_postgres_failure_after_real_payload_insert_rolls_back_entire_turn(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, source: bool
):
    region = await routing_region(db, flow_run, shape="fanout" if source else "chain")
    binding = await claim_source(db, region)
    source_intent = source_request(region, binding, 1, {"value": 1})
    request = None
    if not source:
        await append(db, region, source_intent)
        consumer = await open_input(db, region)
        delivery = await read_input(db, region, consumer)
        assert delivery is not None
        request = consumer_request(
            region, consumer, delivery, value={"signed": True}, state={"count": 1}
        )
    before = await contents(db, region)
    engine = await db.engine()
    insert_seen: list[str] = []

    def fail_after_insert(
        connection: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if (
            statement.lower().replace('"', "").startswith("insert into stream_record")
            and not insert_seen
        ):
            insert_seen.append("stream_record inserted")
            raise InjectedRouteFailure("Database accepted INSERT before injected loss")

    sa.event.listen(engine.sync_engine, "after_cursor_execute", fail_after_insert)
    try:
        with pytest.raises(InjectedRouteFailure):
            if source:
                await append(db, region, source_intent)
            else:
                assert request is not None
                await complete(db, region, request)
    finally:
        sa.event.remove(engine.sync_engine, "after_cursor_execute", fail_after_insert)
    assert insert_seen == ["stream_record inserted"]
    assert await contents(db, region) == before


async def test_postgres_one_full_branch_rolls_back_payload_every_reference_and_receipt(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    binding = await claim_source(db, region)
    target = next(
        edge for edge in region.plan.edges if edge.edge_uid == "A-C"
    ).partitions[0]
    async with db.session_context(begin_transaction=True) as session:
        row = await session.get(db.StreamPartition, target.stream_id)
        assert row is not None
        row.limits = {**row.limits, "max_records": 1}
    first_request = source_request(region, binding, 1, {"value": 1})
    first = await append(db, region, first_request)
    before = await contents(db, region)
    assert (await append(db, region, first_request)).model_dump() == first.model_dump()
    assert await contents(db, region) == before
    with pytest.raises(NativeStreamError) as refused:
        await append(db, region, source_request(region, binding, 2, {"value": 2}))
    assert refused.value.code == "CAPACITY"
    assert await contents(db, region) == before


async def test_postgres_operator_target_capacity_rolls_back_cursor_state_and_entire_fanout(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run, shape="chain")
    source = await claim_source(db, region)
    for position in (1, 2):
        await append(
            db, region, source_request(region, source, position, {"value": position})
        )
    target = next(
        edge for edge in region.plan.edges if edge.edge_uid == "B-C"
    ).partitions[0]
    async with db.session_context(begin_transaction=True) as session:
        row = await session.get(db.StreamPartition, target.stream_id)
        assert row is not None
        row.limits = {**row.limits, "max_records": 1}
    binding = await open_input(db, region)
    first = await read_input(db, region, binding)
    assert first is not None
    first_request = consumer_request(
        region, binding, first, value={"signed": 1}, state={"count": 1}
    )
    first_outcome = await complete(
        db,
        region,
        first_request,
    )
    assert (
        await complete(db, region, first_request)
    ).model_dump() == first_outcome.model_dump()
    second = await read_input(db, region, binding)
    assert second is not None
    before = await contents(db, region)
    with pytest.raises(NativeStreamError) as refused:
        await complete(
            db,
            region,
            consumer_request(
                region, binding, second, value={"signed": 2}, state={"count": 2}
            ),
        )
    assert refused.value.code == "CAPACITY"
    assert await contents(db, region) == before


@pytest.mark.parametrize("changed", ["value", "state", "kind"])
async def test_postgres_operator_replay_and_changed_semantics(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, changed: str
):
    region = await routing_region(db, flow_run)
    source = await claim_source(db, region)
    await append(db, region, source_request(region, source, 1, None))
    binding = await open_input(db, region)
    delivery = await read_input(db, region, binding)
    assert delivery is not None
    request = consumer_request(
        region, binding, delivery, value=None, state={"count": 1}
    )
    first = await complete(db, region, request)
    before = await contents(db, region)
    assert (await complete(db, region, request)).model_dump() == first.model_dump()
    replacement = consumer_request(
        region,
        binding,
        delivery,
        kind="RETURN" if changed == "kind" else "YIELD",
        value=1 if changed == "value" else None,
        state={"count": 2 if changed == "state" else 1},
    )
    with pytest.raises(NativeStreamError) as refused:
        await complete(db, region, replacement)
    assert refused.value.code == "IDENTITY_CONFLICT"
    assert await contents(db, region) == before


@pytest.mark.parametrize("source", [False, True])
async def test_postgres_current_owner_is_checked_before_accepted_receipt_lookup(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, source: bool
):
    region = await routing_region(db, flow_run)
    source_binding = await claim_source(db, region)
    source_intent = source_request(region, source_binding, 1, None)
    await append(db, region, source_intent)
    if source:
        changed = source_intent.model_copy(
            update={"binding": source_binding.model_copy(update={"owner_id": uuid4()})}
        )
    else:
        binding = await open_input(db, region)
        delivery = await read_input(db, region, binding)
        assert delivery is not None
        request = consumer_request(region, binding, delivery, kind="RETURN")
        await complete(db, region, request)
        owner = uuid4()
        changed = request.model_copy(
            update={
                "binding": binding.model_copy(update={"owner_id": owner}),
                "token": delivery.token.model_copy(update={"owner_id": owner}),
            }
        )
    before = await contents(db, region)
    with pytest.raises(NativeStreamError) as refused:
        if source:
            await append(db, region, changed)
        else:
            await complete(db, region, changed)
    assert refused.value.code == "STALE_OWNER"
    assert await contents(db, region) == before


async def test_postgres_return_and_yield_null_do_not_acknowledge_sibling_cursors(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    source = await claim_source(db, region)
    for position in (1, 2):
        await append(
            db, region, source_request(region, source, position, {"position": position})
        )
    branch_b = await open_input(db, region)
    branch_c = await open_input(db, region, "C")
    first = await read_input(db, region, branch_b)
    assert first is not None
    returned = await complete(
        db,
        region,
        consumer_request(region, branch_b, first, kind="RETURN", state={"count": 1}),
    )
    assert returned.payload_record_id is None and not returned.references
    second = await read_input(db, region, branch_b)
    assert second is not None
    yielded = await complete(
        db,
        region,
        consumer_request(region, branch_b, second, value=None, state={"count": 2}),
    )
    assert yielded.payload_record_id is not None and yielded.producer_sequence == 1
    async with db.session_context() as session:
        b, c = (
            await session.get(db.StreamConsumer, branch_b.consumer_id),
            await session.get(db.StreamConsumer, branch_c.consumer_id),
        )
        assert b is not None and c is not None
        assert b.completed_through == 2 and b.managed_state == {"count": 2}
        assert c.completed_through == 0 and c.managed_state == {}
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamRecord)
                .where(db.StreamRecord.stream_id == branch_c.input_stream_id)
            )
            == 2
        )
    sibling = await read_input(db, region, branch_c, "C")
    assert sibling is not None and sibling.token.sequence == 1


@pytest.mark.parametrize("exchange", ["forward", "rebalance", "broadcast", "keyed"])
async def test_postgres_edge_eof_waits_for_its_exact_contributors_and_empty_targets(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, exchange: Any
):
    region = await routing_region(
        db,
        flow_run,
        shape="linear",
        source_p=2,
        target_p=2 if exchange == "forward" else 3,
        exchange=exchange,
    )
    source = await claim_source(db, region)
    await append(db, region, source_request(region, source, 1, None))
    async with route_store(db, region, "A") as (_, store, _):
        await store.finish_producer()
    parts = region.plan.edges[0].partitions
    async with db.session_context() as session:
        headers = {
            part.id: await session.get(db.StreamPartition, part.stream_id)
            for part in parts
        }
        assert all(row is not None for row in headers.values())
        assert [headers[part.id].state for part in parts] == (
            ["COMPLETED", "OPEN"] if exchange == "forward" else ["OPEN"] * len(parts)
        )
    await claim_source(db, region, 1)
    async with route_store(db, region, "A", 1) as (_, store, _):
        await store.finish_producer()
        await store.finish_producer()
    async with db.session_context() as session:
        rows = (
            await session.scalars(
                sa.select(db.StreamPartition).where(
                    db.StreamPartition.id.in_([p.stream_id for p in parts])
                )
            )
        ).all()
        assert len(rows) == len(parts) and all(
            row.state == "COMPLETED" and row.tail_sequence == row.last_sequence
            for row in rows
        )
        if exchange != "broadcast":
            assert any(row.tail_sequence == 0 for row in rows)


async def test_postgres_concurrent_producers_create_contiguous_independent_reference_logs(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(
        db, flow_run, shape="linear", source_p=2, target_p=3, exchange="broadcast"
    )
    bindings = [await claim_source(db, region, index) for index in range(2)]
    barrier = asyncio.Barrier(2)

    async def produce(index: int) -> list[wire.StreamDAGRouteOutcome]:
        await barrier.wait()
        return [
            await append(
                db,
                region,
                source_request(
                    region,
                    bindings[index],
                    p,
                    {"producer": index, "position": p},
                    index=index,
                ),
                index=index,
            )
            for p in range(1, 5)
        ]

    results = await asyncio.wait_for(asyncio.gather(produce(0), produce(1)), 30)
    assert len({o.receipt_id for lane in results for o in lane}) == 8
    async with db.session_context() as session:
        for part in region.plan.edges[0].partitions:
            records = (
                await session.scalars(
                    sa.select(db.StreamRecord)
                    .where(db.StreamRecord.stream_id == part.stream_id)
                    .order_by(db.StreamRecord.sequence)
                )
            ).all()
            assert [r.sequence for r in records] == list(range(1, 9))
            assert len({r.payload_record_id for r in records}) == 8
    rows = (await contents(db, region))["records"]
    assert sum(r.record_kind == "PAYLOAD" for r in rows) == 8
    assert sum(r.record_kind == "REFERENCE" for r in rows) == 24

    async def finish(index: int) -> None:
        async with route_store(db, region, "A", index) as (_, store, _):
            await store.finish_producer()

    await asyncio.wait_for(asyncio.gather(finish(0), finish(1)), 30)
    async with db.session_context() as session:
        targets = (
            await session.scalars(
                sa.select(db.StreamPartition).where(
                    db.StreamPartition.id.in_(
                        [part.stream_id for part in region.plan.edges[0].partitions]
                    )
                )
            )
        ).all()
        assert len(targets) == 3 and all(
            row.state == "COMPLETED" and row.tail_sequence == 8 for row in targets
        )


async def test_postgres_reference_resolution_rejects_corrupt_payload_and_sql_chains(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    source = await claim_source(db, region)
    outcome = await append(
        db, region, source_request(region, source, 1, {"ref": "object://1"})
    )
    reference = outcome.references[0]
    target = next(
        row
        for row in region.plan.subtasks
        if row.input_stream_id == reference.stream_id
    )
    sibling = next(
        row
        for row in region.plan.subtasks
        if row.input_stream_id is not None
        and row.input_stream_id != reference.stream_id
    )
    before = await contents(db, region)
    async with route_store(db, region, sibling.operator_uid, sibling.subtask_index) as (
        _,
        store,
        _,
    ):
        with pytest.raises(NativeStreamError) as refused:
            await store.resolve_reference(reference.stream_id, reference.sequence)
        assert refused.value.code == "IDENTITY_CONFLICT"
    assert await contents(db, region) == before
    async with route_store(db, region, target.operator_uid, target.subtask_index) as (
        _,
        store,
        _,
    ):
        resolved = await store.resolve_reference(
            reference.stream_id, reference.sequence
        )
        assert resolved.value == {"ref": "object://1"}
        assert resolved.payload_record_id == outcome.payload_record_id
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                row = await session.get(db.StreamRecord, reference.record_id)
                assert row is not None
                row.payload_record_id = outcome.references[1].record_id
                await session.flush()
        payload = await session.get(db.StreamRecord, outcome.payload_record_id)
        assert payload is not None
        payload.value = {"ref": "corrupted-without-matching-digest"}
    async with route_store(db, region, target.operator_uid, target.subtask_index) as (
        _,
        store,
        _,
    ):
        with pytest.raises(NativeStreamError) as refused:
            await store.resolve_reference(reference.stream_id, reference.sequence)
        assert refused.value.code == "REFERENCE_IDENTITY_CONFLICT"


async def test_postgres_business_identity_field_names_remain_opaque_payload_and_state(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(db, flow_run)
    source = await claim_source(db, region)
    business = {
        "worker_id": "business-value",
        "attempt": 7,
        "owner_id": "customer-42",
        "nested": {"grant_id": "business-grant"},
    }
    await append(db, region, source_request(region, source, 1, business))
    binding = await open_input(db, region)
    delivery = await read_input(db, region, binding)
    assert delivery is not None and delivery.value == business
    outcome = await complete(
        db,
        region,
        consumer_request(region, binding, delivery, value=business, state=business),
    )
    async with db.session_context() as session:
        payload = await session.get(db.StreamRecord, outcome.payload_record_id)
        consumer = await session.get(db.StreamConsumer, binding.consumer_id)
        assert payload is not None and payload.value == business
        assert consumer is not None and consumer.managed_state == business


@pytest.mark.parametrize("exchange", ["forward", "rebalance", "broadcast", "keyed"])
async def test_postgres_all_exchange_targets_match_frozen_vectors(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, exchange: Any
):
    target_p = 2 if exchange == "forward" else 3
    region = await routing_region(
        db, flow_run, shape="linear", source_p=2, target_p=target_p, exchange=exchange
    )
    targets = {
        part.stream_id: part.partition_index for part in region.plan.edges[0].partitions
    }
    for index in range(2):
        binding = await claim_source(db, region, index)
        for position in range(1, 4):
            # Frozen key vectors: SHA256(tagged null)%16=11 -> target2;
            # SHA256(tagged False)%16=2 -> target0 for target P3.
            key = None if position % 2 else False
            outcome = await append(
                db,
                region,
                source_request(
                    region,
                    binding,
                    position,
                    {"index": index, "position": position},
                    index=index,
                    key=key,
                ),
                index=index,
            )
            expected = (
                [index]
                if exchange == "forward"
                else [(index + position - 1) % target_p]
                if exchange == "rebalance"
                else list(range(target_p))
                if exchange == "broadcast"
                else [2 if key is None else 0]
            )
            assert sorted(targets[r.stream_id] for r in outcome.references) == expected


async def test_postgres_failed_contributor_does_not_manufacture_completed_eof(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    region = await routing_region(
        db, flow_run, shape="linear", source_p=2, target_p=3, exchange="broadcast"
    )
    first = await claim_source(db, region, 0)
    await claim_source(db, region, 1)
    await append(db, region, source_request(region, first, 1, {"kept": True}))
    async with db.session_context(begin_transaction=True) as session:
        result = await task_runs.set_task_run_state(
            session,
            region.subtask("A", 1).manifest.task_run_id,
            states.Failed(
                message="Source failed",
                state_details=states.StateDetails(retriable=False),
            ),
            task_policy=CoreTaskPolicy,
        )
        assert result.state is not None and result.state.is_failed()
    async with route_store(db, region, "A", 1) as (_, store, _):
        await store.finish_producer(error="Source failed")
    async with db.session_context() as session:
        targets = (
            await session.scalars(
                sa.select(db.StreamPartition).where(
                    db.StreamPartition.id.in_(
                        [p.stream_id for p in region.plan.edges[0].partitions]
                    )
                )
            )
        ).all()
        assert all(row.state == "OPEN" for row in targets)
    async with route_store(db, region, "A") as (_, store, _):
        await store.finish_producer()
    async with db.session_context() as session:
        targets = (
            await session.scalars(
                sa.select(db.StreamPartition).where(
                    db.StreamPartition.id.in_(
                        [p.stream_id for p in region.plan.edges[0].partitions]
                    )
                )
            )
        ).all()
        assert all(row.state == "FAILED" and row.tail_sequence == 1 for row in targets)
    binding = await open_input(db, region)
    delivery = await read_input(db, region, binding)
    assert delivery is not None and delivery.value == {"kept": True}
    await complete(
        db, region, consumer_request(region, binding, delivery, kind="RETURN")
    )
    with pytest.raises(NativeStreamError) as refused:
        await read_input(db, region, binding)
    assert refused.value.code == "UPSTREAM_FAILED"


@pytest.mark.parametrize("same_session", [False, True])
async def test_postgres_discovery_authorization_cannot_cross_session_or_transaction(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, same_session: bool
):
    region = await routing_region(db, flow_run)
    context = stream_routes.context_for_subtask(region.plan, region.subtask("A").id)
    async with db.session_context() as first:
        async with first.begin():
            authorization = await stream_routes.authorize_dag_routing_discovery(
                first, db, context, region.execution
            )
        if same_session:
            async with first.begin():
                with pytest.raises(NativeStreamError) as refused:
                    await stream_routes.NativeDAGRouteStore(
                        first, db, authorization
                    ).claim_source(uuid4())
                assert refused.value.code == "IDENTITY_CONFLICT"
        else:
            async with db.session_context(begin_transaction=True) as other:
                with pytest.raises(NativeStreamError) as refused:
                    await stream_routes.NativeDAGRouteStore(
                        other, db, authorization
                    ).claim_source(uuid4())
                assert refused.value.code == "IDENTITY_CONFLICT"
