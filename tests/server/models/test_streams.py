"""Native storage tests: actual migrations, PostgreSQL transactions and SQL oracle."""

import asyncio
import json
import math
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from prefect.server import models, schemas
from prefect.server.database import PrefectDBInterface
from prefect.server.events.clients import AssertingEventsClient
from prefect.server.models.streams import NativeStreamError, NativeStreamStore
from prefect.server.orchestration.core_policy import CoreFlowPolicy
from prefect.server.schemas.streams import (
    ConsumerBinding,
    SourceBinding,
    StreamLimits,
    StreamSubmissionLookupRequest,
    StreamSubmissionReserveRequest,
)


@dataclass
class Graph:
    store: NativeStreamStore
    source: SourceBinding
    consumer: ConsumerBinding


@pytest.fixture
async def graph(db, session, flow_run, task_run, request) -> Graph:
    connection = await session.connection()
    version = connection.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    await models.flow_runs.set_flow_run_state(
        session=session,
        flow_run_id=flow_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    downstream = await models.task_runs.create_task_run(
        session=session,
        task_run=schemas.core.TaskRun(
            flow_run_id=flow_run.id,
            task_key=f"transform-{uuid4()}",
            dynamic_key="0",
            state=schemas.states.Running(),
        ),
    )
    await session.commit()
    store = NativeStreamStore(db)
    input_id, output_id = uuid4(), uuid4()
    limits = getattr(request, "param", {})
    await store.create(flow_run.id, input_id, limits=limits.get("input"))
    await store.create(flow_run.id, output_id, limits=limits.get("output"))
    source = await store.claim_source(input_id, task_run.id, uuid4(), 1)
    consumer = await store.open_consumer(input_id, output_id, downstream.id, uuid4(), 1)
    return Graph(store, source, consumer)


async def next_attempt(db: PrefectDBInterface, task_id) -> None:
    async with db.session_context(begin_transaction=True) as session:
        await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=task_id,
            state=schemas.states.Running(),
            force=True,
        )


async def witness(db: PrefectDBInterface, graph: Graph) -> dict:
    async with db.session_context() as session:
        consumer = await session.get(db.StreamConsumer, graph.consumer.consumer_id)
        output = await session.get(db.StreamPartition, graph.consumer.output_stream_id)
        receipts = await session.scalar(
            sa.select(sa.func.count())
            .select_from(db.StreamCompletion)
            .where(db.StreamCompletion.consumer_id == graph.consumer.consumer_id)
        )
        records = (
            await session.scalars(
                sa.select(db.StreamRecord)
                .where(db.StreamRecord.stream_id == graph.consumer.output_stream_id)
                .order_by(db.StreamRecord.sequence)
            )
        ).all()
        return {
            "cursor": consumer.completed_through,
            "state": consumer.managed_state,
            "inflight": consumer.inflight_sequence,
            "delivery": consumer.delivery_id,
            "receipts": receipts,
            "values": [row.value for row in records],
            "count": output.record_count,
            "bytes": output.byte_count,
            "position": output.producer_position,
        }


async def require_postgres_17(db: PrefectDBInterface) -> None:
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        pytest.skip("Native stream data plane requires PostgreSQL 17")


async def test_backend_capability_fails_before_sqlite_writes(db, flow_run):
    store = NativeStreamStore(db)
    stream_id = uuid4()
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        with pytest.raises(NativeStreamError, match="PostgreSQL 17") as error:
            await store.create(flow_run.id, stream_id)
        assert error.value.code == "UNSUPPORTED_BACKEND"
        async with db.session_context() as session:
            assert await session.get(db.StreamPartition, stream_id) is None
    else:
        assert (await store.create(flow_run.id, stream_id)).protocol_version == 1


async def test_yield_receipt_lost_response_and_drift(db, graph):
    original = {"document": 200, "ref": "s3://documents/200.pdf"}
    await graph.store.append(graph.source, 1, original)
    original["document"] = 999
    delivery = await graph.store.read(graph.consumer)
    assert delivery.value["document"] == 200
    assert await graph.store.read(graph.consumer) == delivery
    receipt = await graph.store.complete(
        graph.consumer, delivery.token, "YIELD", {"signed": 200}, {"count": 1}
    )
    recreated = NativeStreamStore(db)
    assert (
        await recreated.complete(
            graph.consumer, delivery.token, "YIELD", {"signed": 200}, {"count": 1}
        )
        == receipt
    )
    with pytest.raises(NativeStreamError) as error:
        await recreated.complete(
            graph.consumer, delivery.token, "YIELD", {"signed": 999}, {"count": 1}
        )
    assert error.value.code == "IDENTITY_CONFLICT"
    actual = await witness(db, graph)
    assert actual["cursor"] == actual["receipts"] == actual["count"] == 1
    assert actual["state"] == {"count": 1}
    assert actual["values"] == [{"signed": 200}]
    assert actual["inflight"] is None


@pytest.mark.parametrize("kind", ["YIELD", "RETURN"])
async def test_real_commit_failure_rolls_back_entire_turn(db, graph, kind):
    await graph.store.append(graph.source, 1, 200)
    delivery = await graph.store.read(graph.consumer)
    before = await witness(db, graph)
    name = "native_terminal_fault_" + uuid4().hex
    async with db.session_context(begin_transaction=True) as session:
        await session.execute(
            sa.text(f"""
            CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected native terminal failure' USING ERRCODE='23514'; END $$
        """)
        )
        await session.execute(
            sa.text(f"""
            CREATE CONSTRAINT TRIGGER {name} AFTER UPDATE ON stream_consumer
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            WHEN (NEW.id = '{graph.consumer.consumer_id}'::uuid
                  AND NEW.completed_through > OLD.completed_through)
            EXECUTE FUNCTION {name}()
        """)
        )
    value = {"signed": 200} if kind == "YIELD" else None
    try:
        with pytest.raises(IntegrityError, match="injected native terminal failure"):
            await graph.store.complete(
                graph.consumer, delivery.token, kind, value, {"count": 1}
            )
        assert await witness(db, graph) == before
    finally:
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(sa.text(f"DROP TRIGGER {name} ON stream_consumer"))
            await session.execute(sa.text(f"DROP FUNCTION {name}()"))
    await graph.store.complete(
        graph.consumer, delivery.token, kind, value, {"count": 1}
    )
    actual = await witness(db, graph)
    assert actual["cursor"] == actual["receipts"] == 1
    assert actual["count"] == (1 if kind == "YIELD" else 0)


@pytest.mark.parametrize("accepted", [False, True])
async def test_document_200_recovery_has_real_durable_coordinates(db, graph, accepted):
    for position in range(1, 202):
        await graph.store.append(graph.source, position, {"document": position})
    for position in range(1, 200):
        item = await graph.store.read(graph.consumer)
        await graph.store.complete(
            graph.consumer,
            item.token,
            "YIELD",
            {"signed": position},
            {"count": position},
        )
    item = await graph.store.read(graph.consumer)
    assert item.token.sequence == 200
    if accepted:
        await graph.store.complete(
            graph.consumer, item.token, "YIELD", {"signed": 200}, {"count": 200}
        )
    recovered = NativeStreamStore(db)
    await next_attempt(db, graph.consumer.consumer_id)
    owner = await recovered.open_consumer(
        graph.consumer.input_stream_id,
        graph.consumer.output_stream_id,
        graph.consumer.consumer_id,
        uuid4(),
        2,
    )
    next_item = await recovered.read(owner)
    assert next_item.token.sequence == (201 if accepted else 200)
    assert next_item.managed_state == {"count": 200 if accepted else 199}
    assert owner.consumer_id == graph.consumer.consumer_id
    assert owner.owner_epoch == graph.consumer.owner_epoch + 1
    with pytest.raises(NativeStreamError) as error:
        await graph.store.complete(
            graph.consumer, item.token, "YIELD", {"signed": 200}, {"count": 200}
        )
    assert error.value.code in {"STALE_OWNER", "ATTEMPT_NOT_CURRENT"}
    assert (await witness(db, graph))["count"] == (200 if accepted else 199)


async def test_null_yield_and_return_are_distinct_and_eof_is_immutable(db, graph):
    await graph.store.append(graph.source, 1, None)
    await graph.store.append(graph.source, 2, False)
    first = await graph.store.read(graph.consumer)
    assert first.value is None
    await graph.store.complete(
        graph.consumer, first.token, "RETURN", None, {"skipped": 1}
    )
    second = await graph.store.read(graph.consumer)
    await graph.store.complete(
        graph.consumer, second.token, "YIELD", None, {"skipped": 1}
    )
    with pytest.raises(NativeStreamError):
        await graph.store.finish_consumer(graph.consumer)
    await graph.store.finish_source(graph.source)
    await graph.store.finish_source(graph.source)
    await graph.store.finish_consumer(graph.consumer)
    await graph.store.finish_consumer(graph.consumer)
    assert await graph.store.read(graph.consumer) is None
    assert [
        row.value
        for row in await graph.store.read_prefix(graph.consumer.output_stream_id, 10)
    ] == [None]
    assert (
        await graph.store.status(graph.consumer.output_stream_id)
    ).tail_sequence == 1
    assert (await witness(db, graph))["position"] == 2
    assert (await witness(db, graph))["receipts"] == 2
    with pytest.raises(NativeStreamError):
        await graph.store.append(graph.source, 3, "late")
    with pytest.raises(NativeStreamError):
        await graph.store.finish_source(graph.source, "conflicting failure")
    await next_attempt(db, graph.consumer.consumer_id)
    retry = await graph.store.open_consumer(
        graph.consumer.input_stream_id,
        graph.consumer.output_stream_id,
        graph.consumer.consumer_id,
        uuid4(),
        2,
    )
    await graph.store.finish_consumer(retry)


async def test_concurrent_completion_and_takeover_have_one_progress_authority(
    db, graph
):
    await graph.store.append(graph.source, 1, {"document": 200})
    item = await graph.store.read(graph.consumer)

    async def take_over():
        await next_attempt(db, graph.consumer.consumer_id)
        return await graph.store.open_consumer(
            graph.consumer.input_stream_id,
            graph.consumer.output_stream_id,
            graph.consumer.consumer_id,
            uuid4(),
            2,
        )

    complete, takeover = await asyncio.gather(
        graph.store.complete(
            graph.consumer, item.token, "YIELD", "signed", {"count": 1}
        ),
        take_over(),
        return_exceptions=True,
    )
    assert not isinstance(takeover, BaseException)
    if isinstance(complete, NativeStreamError):
        assert complete.code in {"STALE_OWNER", "ATTEMPT_NOT_CURRENT"}
        new_item = await graph.store.read(takeover)
        await graph.store.complete(
            takeover, new_item.token, "YIELD", "signed", {"count": 1}
        )
    else:
        assert not isinstance(complete, BaseException)
        assert await graph.store.read(takeover) is None
    actual = await witness(db, graph)
    assert actual["cursor"] == actual["receipts"] == actual["count"] == 1


async def test_source_append_close_race_has_no_record_beyond_tail(graph):
    append, closed = await asyncio.gather(
        graph.store.append(graph.source, 1, "payload"),
        graph.store.finish_source(graph.source),
        return_exceptions=True,
    )
    assert closed is None
    info = await graph.store.status(graph.source.stream_id)
    assert info.state == "COMPLETED"
    assert info.tail_sequence == info.last_sequence == info.record_count
    if isinstance(append, NativeStreamError):
        assert append.code == "TERMINAL_CONFLICT"
        assert info.tail_sequence == 0
    else:
        assert not isinstance(append, BaseException)
        assert info.tail_sequence == 1


@pytest.mark.parametrize("claim", ["source", "consumer"])
async def test_future_attempt_cannot_poison_epoch_and_current_retry_can_claim(
    db, graph, claim
):
    before = await witness(db, graph)
    task_id = (
        graph.source.task_run_id if claim == "source" else graph.consumer.consumer_id
    )

    async def acquire(attempt):
        if claim == "source":
            return await graph.store.claim_source(
                graph.source.stream_id, task_id, uuid4(), attempt
            )
        return await graph.store.open_consumer(
            graph.consumer.input_stream_id,
            graph.consumer.output_stream_id,
            task_id,
            uuid4(),
            attempt,
        )

    with pytest.raises(NativeStreamError) as error:
        await acquire(999)
    assert error.value.code == "ATTEMPT_NOT_CURRENT"
    assert await witness(db, graph) == before
    await next_attempt(db, task_id)
    current = await acquire(2)
    assert current.attempt == current.owner_epoch == 2
    with pytest.raises(NativeStreamError) as error:
        await acquire(1)
    assert error.value.code == "ATTEMPT_NOT_CURRENT"


async def test_metadata_attempt_change_fences_old_turn_before_new_owner_claim(
    db, graph
):
    await graph.store.append(graph.source, 1, 200)
    item = await graph.store.read(graph.consumer)
    before = await witness(db, graph)
    await next_attempt(db, graph.consumer.consumer_id)
    with pytest.raises(NativeStreamError) as error:
        await graph.store.complete(graph.consumer, item.token, "YIELD", "signed", {})
    assert error.value.code == "ATTEMPT_NOT_CURRENT"
    assert await witness(db, graph) == before


async def test_same_attempt_cannot_replace_an_owner_and_nonrunning_cannot_publish(
    db, graph
):
    with pytest.raises(NativeStreamError) as error:
        await graph.store.claim_source(
            graph.source.stream_id, graph.source.task_run_id, uuid4(), 1
        )
    assert error.value.code == "STALE_OWNER"
    async with db.session_context(begin_transaction=True) as session:
        await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=graph.source.task_run_id,
            state=schemas.states.Failed(message="source failure"),
            force=True,
        )
    with pytest.raises(NativeStreamError) as error:
        await graph.store.append(graph.source, 1, "late")
    assert error.value.code == "ATTEMPT_NOT_CURRENT"
    await graph.store.finish_source(graph.source, "source failure")
    assert (await graph.store.status(graph.source.stream_id)).state == "FAILED"


@pytest.mark.parametrize(
    "graph",
    [{"output": StreamLimits(max_records=1)}, {"output": StreamLimits(max_bytes=3)}],
    indirect=True,
)
async def test_output_capacity_rolls_back_progress_but_return_needs_no_output_slot(
    db, graph
):
    await graph.store.append(graph.source, 1, 1)
    await graph.store.append(graph.source, 2, 2)
    first = await graph.store.read(graph.consumer)
    await graph.store.complete(graph.consumer, first.token, "YIELD", "a", {"count": 1})
    second = await graph.store.read(graph.consumer)
    before = await witness(db, graph)
    with pytest.raises(NativeStreamError) as error:
        await graph.store.complete(
            graph.consumer, second.token, "YIELD", "b", {"count": 2}
        )
    assert error.value.code == "CAPACITY"
    assert await witness(db, graph) == before
    await graph.store.complete(
        graph.consumer, second.token, "RETURN", None, {"count": 2}
    )
    actual = await witness(db, graph)
    assert actual["cursor"] == actual["receipts"] == 2
    assert actual["count"] == 1
    assert actual["values"] == ["a"]


@pytest.mark.parametrize(
    "graph",
    [
        {"output": StreamLimits(max_record_bytes=3)},
        {"input": StreamLimits(max_state_bytes=2)},
    ],
    indirect=True,
)
async def test_record_and_state_size_refusal_is_atomic(db, graph):
    await graph.store.append(graph.source, 1, 1)
    item = await graph.store.read(graph.consumer)
    before = await witness(db, graph)
    with pytest.raises(NativeStreamError) as error:
        await graph.store.complete(
            graph.consumer, item.token, "YIELD", "too large", {"count": 1}
        )
    assert error.value.code == "PAYLOAD_TOO_LARGE"
    assert await witness(db, graph) == before


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), b"bytes", {1: "integer-key"}, object()]
)
async def test_noncanonical_values_reject_without_storage_changes(graph, value):
    before = await graph.store.status(graph.source.stream_id)
    with pytest.raises(NativeStreamError) as error:
        await graph.store.append(graph.source, 1, value)
    assert error.value.code == "PROTOCOL_ERROR"
    assert await graph.store.status(graph.source.stream_id) == before


async def test_source_positions_and_limits_are_frozen_and_idempotent(graph):
    first = await graph.store.append(graph.source, 1, {"a": 1, "b": 2})
    assert await graph.store.append(graph.source, 1, {"b": 2, "a": 1}) == first
    with pytest.raises(NativeStreamError):
        await graph.store.append(graph.source, 1, {"a": 999})
    with pytest.raises(NativeStreamError):
        await graph.store.append(graph.source, 3, "gap")
    info = await graph.store.status(graph.source.stream_id)
    assert await graph.store.create(info.flow_run_id, info.id) == info
    with pytest.raises(NativeStreamError):
        await graph.store.create(
            info.flow_run_id, info.id, limits=StreamLimits(max_records=1)
        )
    with pytest.raises(NativeStreamError):
        await graph.store.create(info.flow_run_id, uuid4(), protocol_version=2)
    with pytest.raises(NativeStreamError):
        await graph.store.create(info.flow_run_id, uuid4(), protocol_version=True)
    assert await graph.store.status(graph.source.stream_id) == info


async def test_cross_flow_task_ports_and_rebinding_reject_before_mutation(
    db, session, flow, graph
):
    foreign = await models.flow_runs.create_flow_run(
        session=session, flow_run=schemas.core.FlowRun(flow_id=flow.id)
    )
    await session.commit()
    other_stream = uuid4()
    await graph.store.create(foreign.id, other_stream)
    before = await witness(db, graph)
    with pytest.raises(NativeStreamError) as error:
        await graph.store.claim_source(
            other_stream, graph.source.task_run_id, uuid4(), 1
        )
    assert error.value.code == "IDENTITY_CONFLICT"
    with pytest.raises(NativeStreamError):
        await graph.store.open_consumer(
            graph.source.stream_id, other_stream, graph.consumer.consumer_id, uuid4(), 1
        )
    another_output = uuid4()
    await graph.store.create(graph.consumer.flow_run_id, another_output)
    with pytest.raises(NativeStreamError):
        await graph.store.open_consumer(
            graph.source.stream_id,
            another_output,
            graph.consumer.consumer_id,
            uuid4(),
            1,
        )
    assert await witness(db, graph) == before
    async with db.session_context() as verify:
        for stream_id in (other_stream, another_output):
            row = await verify.get(db.StreamPartition, stream_id)
            assert row.producer_kind is row.owner_id is None


async def test_wrong_delivery_or_binding_cannot_advance_progress(db, graph):
    await graph.store.append(graph.source, 1, 1)
    item = await graph.store.read(graph.consumer)
    before = await witness(db, graph)
    for field in ("delivery_id", "record_id"):
        token = item.token.model_copy(update={field: uuid4()})
        with pytest.raises(NativeStreamError):
            await graph.store.complete(graph.consumer, token, "YIELD", 1, {})
        assert await witness(db, graph) == before


@pytest.mark.parametrize(
    "graph",
    [{"input": StreamLimits(max_prefix_records=1, max_consumers=1)}],
    indirect=True,
)
async def test_prefix_and_consumer_admission_are_bounded(db, session, graph):
    await graph.store.append(graph.source, 1, "record")
    assert len(await graph.store.read_prefix(graph.source.stream_id, 1)) == 1
    for limit in (2, 0, -1, True):
        with pytest.raises(NativeStreamError):
            await graph.store.read_prefix(graph.source.stream_id, limit)
    downstream = await models.task_runs.create_task_run(
        session=session,
        task_run=schemas.core.TaskRun(
            flow_run_id=graph.consumer.flow_run_id,
            task_key=f"extra-{uuid4()}",
            dynamic_key="0",
            state=schemas.states.Running(),
        ),
    )
    await session.commit()
    output = uuid4()
    await graph.store.create(graph.consumer.flow_run_id, output)
    with pytest.raises(NativeStreamError) as error:
        await graph.store.open_consumer(
            graph.source.stream_id, output, downstream.id, uuid4(), 1
        )
    assert error.value.code == "CAPACITY"
    assert (
        await graph.store.open_consumer(
            graph.source.stream_id,
            graph.consumer.output_stream_id,
            graph.consumer.consumer_id,
            graph.consumer.owner_id,
            1,
        )
        == graph.consumer
    )


async def test_whole_flow_release_is_final_only_and_retains_horizon_witness(db, graph):
    await graph.store.append(graph.source, 1, "document")
    item = await graph.store.read(graph.consumer)
    await graph.store.complete(
        graph.consumer, item.token, "YIELD", "signed", {"count": 1}
    )
    before = await witness(db, graph)
    with pytest.raises(NativeStreamError):
        await graph.store.release_flow(graph.consumer.flow_run_id)
    assert await witness(db, graph) == before
    async with db.session_context(begin_transaction=True) as session:
        await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=graph.consumer.flow_run_id,
            state=schemas.states.Failed(message="stop"),
            force=True,
        )
    assert await graph.store.release_flow(graph.consumer.flow_run_id) == 2
    assert await graph.store.release_flow(graph.consumer.flow_run_id) == 0
    for stream_id in (graph.source.stream_id, graph.consumer.output_stream_id):
        info = await graph.store.status(stream_id)
        assert info.released and info.state == "FAILED" and info.tail_sequence == 1
        assert info.record_count == info.byte_count == 0
    async with db.session_context() as session:
        assert await session.get(db.StreamConsumer, graph.consumer.consumer_id) is None
        assert not (
            await session.scalars(
                sa.select(db.StreamCompletion).where(
                    db.StreamCompletion.consumer_id == graph.consumer.consumer_id
                )
            )
        ).all()
        assert not (
            await session.scalars(
                sa.select(db.StreamRecord).where(
                    db.StreamRecord.stream_id.in_(
                        [graph.source.stream_id, graph.consumer.output_stream_id]
                    )
                )
            )
        ).all()
    for operation in (
        graph.store.append(graph.source, 2, "late"),
        graph.store.read(graph.consumer),
        graph.store.complete(
            graph.consumer, item.token, "YIELD", "signed", {"count": 1}
        ),
        graph.store.read_prefix(graph.source.stream_id, 1),
        graph.store.create(graph.source.flow_run_id, graph.source.stream_id),
    ):
        with pytest.raises(NativeStreamError) as error:
            await operation
        assert error.value.code == "REPLAY_HORIZON_EXPIRED"


async def test_stream_operations_create_no_per_record_task_states_or_events(db, graph):
    async def counts():
        async with db.session_context() as session:
            task_ids = [graph.source.task_run_id, graph.consumer.consumer_id]
            return (
                await session.scalar(
                    sa.select(sa.func.count())
                    .select_from(db.TaskRun)
                    .where(db.TaskRun.id.in_(task_ids))
                ),
                await session.scalar(
                    sa.select(sa.func.count())
                    .select_from(db.TaskRunState)
                    .where(db.TaskRunState.task_run_id.in_(task_ids))
                ),
                len(AssertingEventsClient.all),
            )

    before = await counts()
    for position in range(1, 21):
        await graph.store.append(graph.source, position, {"document": position})
        item = await graph.store.read(graph.consumer)
        await graph.store.complete(
            graph.consumer, item.token, "YIELD", position, {"count": position}
        )
    assert await counts() == before
    assert before[0] == 2


@pytest.mark.parametrize("accepted", [False, True])
async def test_separate_process_crash_reconstructs_delivery_or_accepted_progress(
    db, graph, accepted
):
    await graph.store.append(graph.source, 1, {"document": 200})
    await graph.store.append(graph.source, 2, {"document": 201})
    command = {
        "operation": "complete" if accepted else "read",
        "connection_url": str(db.database_config.connection_url),
        "binding": graph.consumer.model_dump(mode="json"),
    }
    if accepted:
        item = await graph.store.read(graph.consumer)
        command["token"] = item.token.model_dump(mode="json")
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(Path(__file__).with_name("stream_storage_process.py"))],
        input=json.dumps(command),
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == (74 if accepted else 73), result.stderr
    assert json.loads(result.stdout)
    await next_attempt(db, graph.consumer.consumer_id)
    new_store = NativeStreamStore(db)
    binding = await new_store.open_consumer(
        graph.consumer.input_stream_id,
        graph.consumer.output_stream_id,
        graph.consumer.consumer_id,
        uuid4(),
        2,
    )
    recovered = await new_store.read(binding)
    assert recovered.token.sequence == (2 if accepted else 1)
    assert recovered.value == {"document": 201 if accepted else 200}
    actual = await witness(db, graph)
    assert (
        actual["count"]
        == actual["receipts"]
        == actual["cursor"]
        == (1 if accepted else 0)
    )


async def test_flow_header_quota_bounds_empty_stream_metadata(graph):
    for _ in range(126):
        await graph.store.create(graph.consumer.flow_run_id, uuid4())
    with pytest.raises(NativeStreamError) as error:
        await graph.store.create(graph.consumer.flow_run_id, uuid4())
    assert error.value.code == "CAPACITY"


async def test_release_and_append_race_leaves_no_payload_after_horizon(db, graph):
    async with db.session_context(begin_transaction=True) as session:
        await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=graph.consumer.flow_run_id,
            state=schemas.states.Failed(message="stop"),
            force=True,
        )
    append, released = await asyncio.gather(
        graph.store.append(graph.source, 1, "payload"),
        graph.store.release_flow(graph.consumer.flow_run_id),
        return_exceptions=True,
    )
    assert released == 2
    if isinstance(append, NativeStreamError):
        assert append.code == "REPLAY_HORIZON_EXPIRED"
    else:
        assert not isinstance(append, BaseException)
    info = await graph.store.status(graph.source.stream_id)
    assert info.released and info.record_count == info.byte_count == 0
    async with db.session_context() as session:
        assert not (
            await session.scalars(
                sa.select(db.StreamRecord).where(
                    db.StreamRecord.stream_id == graph.source.stream_id
                )
            )
        ).all()


async def reserve_consumer(db: PrefectDBInterface, graph: Graph) -> tuple[UUID, UUID]:
    async with db.session_context(begin_transaction=True) as session:
        task = await models.task_runs.create_task_run(
            session=session,
            task_run=schemas.core.TaskRun(
                flow_run_id=graph.consumer.flow_run_id,
                task_key=f"first-open-{uuid4()}",
                dynamic_key="0",
                state=schemas.states.Running(),
            ),
        )
    output_id = uuid4()
    await graph.store.create(graph.consumer.flow_run_id, output_id)
    return task.id, output_id


async def wait_for_lock_waiter(db: PrefectDBInterface, holder_pid: int) -> int:
    async def find_waiter() -> int:
        async with db.session_context() as session:
            while True:
                await session.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
                pid = await session.scalar(
                    sa.text(
                        "SELECT pid FROM pg_stat_activity "
                        "WHERE :holder = ANY(pg_blocking_pids(pid)) LIMIT 1"
                    ),
                    {"holder": holder_pid},
                )
                if pid is not None:
                    return pid
                await asyncio.sleep(0.01)

    return await asyncio.wait_for(find_waiter(), timeout=10)


@pytest.mark.parametrize(
    "state",
    [schemas.states.Completed(), schemas.states.Failed(), schemas.states.Cancelled()],
)
async def test_first_consumer_rejects_final_flow_with_running_task(
    db: PrefectDBInterface, graph: Graph, state: schemas.states.State
):
    task_id, output_id = await reserve_consumer(db, graph)
    before = await graph.store.status(output_id)
    async with db.session_context(begin_transaction=True) as session:
        await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=graph.consumer.flow_run_id,
            state=state,
            force=True,
        )
    with pytest.raises(NativeStreamError) as error:
        await graph.store.open_consumer(
            graph.source.stream_id, output_id, task_id, uuid4(), 1
        )
    assert error.value.code == "TERMINAL_CONFLICT"
    assert await graph.store.status(output_id) == before
    async with db.session_context() as session:
        assert await session.get(db.StreamConsumer, task_id) is None


async def test_first_open_waits_for_flow_before_task_and_headers_during_release(
    db: PrefectDBInterface, graph: Graph
):
    task_id, output_id = await reserve_consumer(db, graph)
    tasks = []
    try:
        async with db.session_context(begin_transaction=True) as gate:
            await gate.scalar(
                sa.select(db.FlowRun)
                .where(db.FlowRun.id == graph.consumer.flow_run_id)
                .with_for_update()
            )
            await models.flow_runs.set_flow_run_state(
                session=gate,
                flow_run_id=graph.consumer.flow_run_id,
                state=schemas.states.Failed(),
                force=True,
            )
            holder = await gate.scalar(sa.text("SELECT pg_backend_pid()"))
            opened = asyncio.create_task(
                graph.store.open_consumer(
                    graph.source.stream_id, output_id, task_id, uuid4(), 1
                )
            )
            tasks.append(opened)
            await wait_for_lock_waiter(db, holder)
            # A blocked first-open must own neither child lock. Otherwise its
            # implicit parent FK lock reverses release/delete's lock order.
            async with db.session_context(begin_transaction=True) as observer:
                await observer.scalar(
                    sa.select(db.TaskRun)
                    .where(db.TaskRun.id == task_id)
                    .with_for_update(nowait=True)
                )
                await observer.scalars(
                    sa.select(db.StreamPartition)
                    .where(
                        db.StreamPartition.id.in_([graph.source.stream_id, output_id])
                    )
                    .order_by(db.StreamPartition.id)
                    .with_for_update(nowait=True)
                )
            released = asyncio.create_task(
                graph.store.release_flow(graph.consumer.flow_run_id)
            )
            tasks.append(released)
        open_result, release_result = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=10
        )
        assert isinstance(open_result, NativeStreamError)
        assert open_result.code == "TERMINAL_CONFLICT"
        assert release_result == 3
        async with db.session_context() as session:
            assert await session.get(db.StreamConsumer, task_id) is None
        assert (await graph.store.status(output_id)).released
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_first_open_commits_before_waiting_finalization_and_release(
    db: PrefectDBInterface, graph: Graph
):
    task_id, output_id = await reserve_consumer(db, graph)

    async def finalize_and_release() -> int:
        async with db.session_context(begin_transaction=True) as session:
            await models.flow_runs.set_flow_run_state(
                session=session,
                flow_run_id=graph.consumer.flow_run_id,
                state=schemas.states.Failed(),
                force=True,
            )
        return await graph.store.release_flow(graph.consumer.flow_run_id)

    tasks = []
    try:
        async with db.session_context(begin_transaction=True) as gate:
            await gate.scalar(
                sa.select(db.StreamPartition)
                .where(db.StreamPartition.id == output_id)
                .with_for_update()
            )
            holder = await gate.scalar(sa.text("SELECT pg_backend_pid()"))
            opened = asyncio.create_task(
                graph.store.open_consumer(
                    graph.source.stream_id, output_id, task_id, uuid4(), 1
                )
            )
            tasks.append(opened)
            opener_pid = await wait_for_lock_waiter(db, holder)
            released = asyncio.create_task(finalize_and_release())
            tasks.append(released)
            await wait_for_lock_waiter(db, opener_pid)
        open_result, release_result = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=10
        )
        assert isinstance(open_result, ConsumerBinding)
        assert open_result.consumer_id == task_id
        assert release_result == 3
        async with db.session_context() as session:
            assert await session.get(db.StreamConsumer, task_id) is None
        assert (await graph.store.status(output_id)).released
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_json_roundtrip_preserves_signed_zero_and_managed_state(graph):
    await graph.store.append(graph.source, 1, {"negative_zero": -0.0})
    await graph.store.append(graph.source, 2, {"document": 2})
    item = await graph.store.read(graph.consumer)
    assert math.copysign(1, item.value["negative_zero"]) == -1
    await graph.store.complete(
        graph.consumer, item.token, "YIELD", -0.0, {"negative_zero": -0.0}
    )
    prefix = await graph.store.read_prefix(graph.consumer.output_stream_id, 1)
    assert math.copysign(1, prefix[0].value) == -1
    following = await graph.store.read(graph.consumer)
    assert math.copysign(1, following.managed_state["negative_zero"]) == -1


@pytest.mark.parametrize(
    "graph", [{"input": StreamLimits(max_prefix_bytes=3)}], indirect=True
)
async def test_prefix_byte_budget_is_checked_before_payload_materialization(graph):
    await graph.store.append(graph.source, 1, "a")
    await graph.store.append(graph.source, 2, "b")
    assert [
        row.value for row in await graph.store.read_prefix(graph.source.stream_id, 1)
    ] == ["a"]
    with pytest.raises(NativeStreamError) as error:
        await graph.store.read_prefix(graph.source.stream_id, 2)
    assert error.value.code == "CAPACITY"


async def test_cancellation_between_delivery_and_completion_refuses_progress(db, graph):
    await graph.store.append(graph.source, 1, "document")
    item = await graph.store.read(graph.consumer)
    before = await witness(db, graph)
    async with db.session_context(begin_transaction=True) as session:
        await models.task_runs.set_task_run_state(
            session=session,
            task_run_id=graph.consumer.consumer_id,
            state=schemas.states.Cancelled(),
            force=True,
        )
    with pytest.raises(NativeStreamError) as error:
        await graph.store.complete(graph.consumer, item.token, "YIELD", "late", {})
    assert error.value.code == "ATTEMPT_NOT_CURRENT"
    assert await witness(db, graph) == before
    await graph.store.finish_consumer(graph.consumer, "cancelled")
    assert (await graph.store.status(graph.consumer.output_stream_id)).state == "FAILED"


@pytest.mark.parametrize("value", ["\u0000", {"escaped": "\u0000"}, 2**80])
async def test_json_payload_and_state_roundtrip_are_lossless(graph, value):
    await graph.store.append(graph.source, 1, value)
    await graph.store.append(graph.source, 2, "next")
    item = await graph.store.read(graph.consumer)
    assert item.value == value
    await graph.store.complete(
        graph.consumer, item.token, "YIELD", value, {"value": value}
    )
    assert (await graph.store.read_prefix(graph.consumer.output_stream_id, 1))[
        0
    ].value == value
    assert (await graph.store.read(graph.consumer)).managed_state == {"value": value}


async def test_release_commit_failure_rolls_back_payload_and_horizon_together(
    db, graph
):
    await graph.store.append(graph.source, 1, "document")
    item = await graph.store.read(graph.consumer)
    await graph.store.complete(
        graph.consumer, item.token, "YIELD", "signed", {"count": 1}
    )
    async with db.session_context(begin_transaction=True) as session:
        await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=graph.consumer.flow_run_id,
            state=schemas.states.Failed(message="stop"),
            force=True,
        )
    before = await witness(db, graph)
    header = await graph.store.status(graph.source.stream_id)
    name = "native_release_fault_" + uuid4().hex
    async with db.session_context(begin_transaction=True) as session:
        await session.execute(
            sa.text(f"""
            CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected native release failure' USING ERRCODE='23514'; END $$
        """)
        )
        await session.execute(
            sa.text(f"""
            CREATE CONSTRAINT TRIGGER {name} AFTER UPDATE ON stream_partition
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            WHEN (NEW.flow_run_id = '{graph.consumer.flow_run_id}'::uuid AND NEW.released AND NOT OLD.released)
            EXECUTE FUNCTION {name}()
        """)
        )
    try:
        with pytest.raises(IntegrityError, match="injected native release failure"):
            await graph.store.release_flow(graph.consumer.flow_run_id)
        assert await witness(db, graph) == before
        assert await graph.store.status(graph.source.stream_id) == header
    finally:
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(sa.text(f"DROP TRIGGER {name} ON stream_partition"))
            await session.execute(sa.text(f"DROP FUNCTION {name}()"))
    assert await graph.store.release_flow(graph.consumer.flow_run_id) == 2


async def test_terminal_task_admission_precedes_release_horizon_for_old_mutations(
    db, graph
):
    await graph.store.append(graph.source, 1, "document")
    item = await graph.store.read(graph.consumer)
    await graph.store.complete(graph.consumer, item.token, "YIELD", "signed", {})
    await graph.store.finish_source(graph.source)
    await graph.store.finish_consumer(graph.consumer)
    async with db.session_context(begin_transaction=True) as session:
        for task_id in (graph.source.task_run_id, graph.consumer.consumer_id):
            await models.task_runs.set_task_run_state(
                session=session,
                task_run_id=task_id,
                state=schemas.states.Completed(),
                force=True,
            )
        await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=graph.consumer.flow_run_id,
            state=schemas.states.Completed(),
            force=True,
        )
    assert await graph.store.release_flow(graph.consumer.flow_run_id) == 2
    for operation in (
        graph.store.append(graph.source, 2, "late"),
        graph.store.read(graph.consumer),
        graph.store.complete(graph.consumer, item.token, "YIELD", "signed", {}),
        graph.store.open_consumer(
            graph.consumer.input_stream_id,
            graph.consumer.output_stream_id,
            graph.consumer.consumer_id,
            graph.consumer.owner_id,
            1,
        ),
    ):
        with pytest.raises(NativeStreamError) as error:
            await operation
        assert error.value.code == "ATTEMPT_NOT_CURRENT"
    with pytest.raises(NativeStreamError) as error:
        await graph.store.read_prefix(graph.source.stream_id, 1)
    assert error.value.code == "REPLAY_HORIZON_EXPIRED"
    assert (await graph.store.status(graph.source.stream_id)).released


async def test_http_admission_distinguishes_metadata_lag_from_stale_attempt(
    db, session, flow_run, task_run
):
    await require_postgres_17(db)
    store = NativeStreamStore(db)
    input_id, output_id = uuid4(), uuid4()
    await store.create(flow_run.id, input_id)
    await store.reserve_output(flow_run.id, output_id, task_run.id)

    with pytest.raises(NativeStreamError) as pending:
        await store.open_consumer(
            input_id,
            output_id,
            task_run.id,
            uuid4(),
            1,
            metadata_lag=True,
            require_reserved_output=True,
        )
    assert pending.value.code == "METADATA_NOT_READY"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    with pytest.raises(NativeStreamError) as behind:
        await store.open_consumer(
            input_id,
            output_id,
            task_run.id,
            uuid4(),
            2,
            metadata_lag=True,
            require_reserved_output=True,
        )
    assert behind.value.code == "METADATA_NOT_READY"

    binding = await store.open_consumer(
        input_id,
        output_id,
        task_run.id,
        uuid4(),
        1,
        metadata_lag=True,
        require_reserved_output=True,
    )
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    with pytest.raises(NativeStreamError) as stale:
        await store.open_consumer(
            input_id,
            output_id,
            task_run.id,
            binding.owner_id,
            1,
            metadata_lag=True,
            require_reserved_output=True,
        )
    assert stale.value.code == "ATTEMPT_NOT_CURRENT"

    async with db.session_context() as check:
        output = await check.get(db.StreamPartition, output_id)
        consumer = await check.get(db.StreamConsumer, task_run.id)
        assert output.owner_id == binding.owner_id
        assert output.attempt == consumer.attempt == 1


async def test_output_reservation_missing_task_is_retryable_without_mutation(
    db, flow_run
):
    await require_postgres_17(db)
    store = NativeStreamStore(db)
    output_id, missing_task_id = uuid4(), uuid4()

    with pytest.raises(NativeStreamError) as error:
        await store.reserve_output(
            flow_run.id,
            output_id,
            missing_task_id,
        )
    assert error.value.code == "METADATA_NOT_READY"
    async with db.session_context() as session:
        assert await session.get(db.StreamPartition, output_id) is None


async def test_output_reservation_reject_and_open_have_one_winner(
    db, session, flow_run, task_run
):
    await require_postgres_17(db)
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    store = NativeStreamStore(db)
    input_id, output_id = uuid4(), uuid4()
    await store.create(flow_run.id, input_id)
    reserved = await store.reserve_output(flow_run.id, output_id, task_run.id)
    assert (await store.reserve_output(flow_run.id, output_id, task_run.id)) == reserved

    outcomes = await asyncio.gather(
        store.open_consumer(
            input_id,
            output_id,
            task_run.id,
            uuid4(),
            1,
            metadata_lag=True,
            require_reserved_output=True,
        ),
        store.reject_reserved_output(
            flow_run.id, output_id, task_run.id, "submission failed"
        ),
        return_exceptions=True,
    )
    successes = [value for value in outcomes if not isinstance(value, BaseException)]
    refusals = [value for value in outcomes if isinstance(value, NativeStreamError)]
    assert len(successes) == len(refusals) == 1
    assert refusals[0].code in {"STALE_OWNER", "TERMINAL_CONFLICT"}

    async with db.session_context() as check:
        output = await check.get(db.StreamPartition, output_id)
        consumer = await check.get(db.StreamConsumer, task_run.id)
        if output.state == "FAILED":
            assert output.owner_id is None
            assert consumer is None
        else:
            assert output.state == "OPEN"
            assert output.owner_id is not None
            assert consumer.output_stream_id == output_id


async def test_terminal_task_failure_closes_older_owner_but_not_from_stale_attempt(
    db, session, flow_run, task_run
):
    await require_postgres_17(db)
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    store = NativeStreamStore(db)
    input_id, output_id = uuid4(), uuid4()
    await store.create(flow_run.id, input_id)
    await store.reserve_output(flow_run.id, output_id, task_run.id)
    binding = await store.open_consumer(
        input_id,
        output_id,
        task_run.id,
        uuid4(),
        1,
        metadata_lag=True,
        require_reserved_output=True,
    )

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Failed(message="retry failed before admission"),
        force=True,
    )
    await session.commit()
    with pytest.raises(NativeStreamError) as stale:
        await store.fail_task_output(
            flow_run.id, output_id, task_run.id, 1, "retry failed"
        )
    assert stale.value.code == "ATTEMPT_NOT_CURRENT"
    assert (await store.status(output_id)).state == "OPEN"

    failed = await store.fail_task_output(
        flow_run.id, output_id, task_run.id, 2, "retry failed"
    )
    assert failed.state == "FAILED"
    assert failed.error == "retry failed"
    assert (
        await store.fail_task_output(
            flow_run.id, output_id, task_run.id, 2, "retry failed"
        )
    ) == failed
    assert binding.attempt == 1


async def test_source_task_reservation_position_checked_eof_and_completed_retry(
    db, session, flow_run, task_run
):
    await require_postgres_17(db)
    store = NativeStreamStore(db)
    stream_id = uuid4()
    reserved = await store.reserve_source(flow_run.id, stream_id, task_run.id)
    assert (await store.reserve_source(flow_run.id, stream_id, task_run.id)) == reserved

    with pytest.raises(NativeStreamError) as lag:
        await store.open_source(stream_id, task_run.id, uuid4(), 1, metadata_lag=True)
    assert lag.value.code == "METADATA_NOT_READY"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    first = await store.open_source(
        stream_id, task_run.id, uuid4(), 1, metadata_lag=True
    )
    assert first.state == "OPEN"
    assert first.accepted_source_position == 0
    record = await store.append(first.binding, 1, {"ref": "object:1"})
    assert record.producer_position == 1
    assert await store.append(first.binding, 1, {"ref": "object:1"}) == record

    with pytest.raises(NativeStreamError) as drift:
        await store.close_source(first.binding, 0)
    assert drift.value.code == "IDENTITY_CONFLICT"
    assert (await store.status(stream_id)).state == "OPEN"

    await store.close_source(first.binding, 1)
    await store.close_source(first.binding, 1)
    assert (await store.status(stream_id)).state == "COMPLETED"

    await next_attempt(db, task_run.id)
    recovered = await store.open_source(
        stream_id, task_run.id, uuid4(), 2, metadata_lag=True
    )
    assert recovered.state == "COMPLETED"
    assert recovered.accepted_source_position == 1
    assert recovered.binding.owner_epoch == first.binding.owner_epoch + 1
    with pytest.raises(NativeStreamError) as stale:
        await store.append(first.binding, 2, {"ref": "object:2"})
    assert stale.value.code in {"ATTEMPT_NOT_CURRENT", "STALE_OWNER"}


async def test_source_task_reject_and_open_have_one_winner(
    db, session, flow_run, task_run
):
    await require_postgres_17(db)
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    store = NativeStreamStore(db)
    stream_id = uuid4()
    await store.reserve_source(flow_run.id, stream_id, task_run.id)

    outcomes = await asyncio.gather(
        store.open_source(stream_id, task_run.id, uuid4(), 1, metadata_lag=True),
        store.reject_reserved_source(
            flow_run.id, stream_id, task_run.id, "submission failed"
        ),
        return_exceptions=True,
    )
    successes = [value for value in outcomes if not isinstance(value, BaseException)]
    refusals = [value for value in outcomes if isinstance(value, NativeStreamError)]
    assert len(successes) == len(refusals) == 1
    assert refusals[0].code in {"STALE_OWNER", "TERMINAL_CONFLICT"}

    async with db.session_context() as check:
        stream = await check.get(db.StreamPartition, stream_id)
        assert stream.producer_kind == "SOURCE"
        assert stream.producer_task_run_id == task_run.id
        if stream.state == "FAILED":
            assert stream.owner_id is None
            assert stream.record_count == 0
        else:
            assert stream.state == "OPEN"
            assert stream.owner_id is not None
            assert stream.attempt == stream.owner_epoch == 1


async def test_source_task_final_failure_uses_exact_current_attempt(
    db, session, flow_run, task_run
):
    await require_postgres_17(db)
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    store = NativeStreamStore(db)
    stream_id = uuid4()
    await store.reserve_source(flow_run.id, stream_id, task_run.id)
    admitted = await store.open_source(
        stream_id, task_run.id, uuid4(), 1, metadata_lag=True
    )
    await store.append(admitted.binding, 1, "ref:1")

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Failed(message="retry exhausted"),
        force=True,
    )
    await session.commit()
    with pytest.raises(NativeStreamError) as stale:
        await store.fail_task_source(
            flow_run.id, stream_id, task_run.id, 1, "retry exhausted"
        )
    assert stale.value.code == "ATTEMPT_NOT_CURRENT"
    assert (await store.status(stream_id)).state == "OPEN"

    failed = await store.fail_task_source(
        flow_run.id, stream_id, task_run.id, 2, "retry exhausted"
    )
    assert failed.state == "FAILED"
    assert failed.producer_position == 1
    assert (
        await store.fail_task_source(
            flow_run.id, stream_id, task_run.id, 2, "retry exhausted"
        )
    ) == failed
    with pytest.raises(NativeStreamError) as conflict:
        await store.fail_task_source(
            flow_run.id, stream_id, task_run.id, 2, "different failure"
        )
    assert conflict.value.code == "TERMINAL_CONFLICT"


async def test_source_task_open_future_cross_flow_and_final_are_read_only(
    db, session, flow, flow_run, task_run
):
    await require_postgres_17(db)
    store = NativeStreamStore(db)
    stream_id = uuid4()
    await store.reserve_source(flow_run.id, stream_id, task_run.id)

    with pytest.raises(NativeStreamError) as future:
        await store.open_source(stream_id, task_run.id, uuid4(), 2, metadata_lag=True)
    assert future.value.code == "METADATA_NOT_READY"

    other_flow = await models.flow_runs.create_flow_run(
        session=session,
        flow_run=schemas.core.FlowRun(flow_id=flow.id, flow_version="source-other"),
    )
    other_task = await models.task_runs.create_task_run(
        session=session,
        task_run=schemas.core.TaskRun(
            flow_run_id=other_flow.id,
            task_key=f"source-cross-{uuid4()}",
            dynamic_key="0",
            state=schemas.states.Running(),
        ),
    )
    await session.commit()
    with pytest.raises(NativeStreamError) as cross_flow:
        await store.open_source(stream_id, other_task.id, uuid4(), 1, metadata_lag=True)
    assert cross_flow.value.code == "IDENTITY_CONFLICT"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    unbound_id = uuid4()
    await store.create(flow_run.id, unbound_id)
    with pytest.raises(NativeStreamError) as unbound:
        await store.open_source(unbound_id, task_run.id, uuid4(), 1, metadata_lag=True)
    assert unbound.value.code == "IDENTITY_CONFLICT"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Cancelled(),
        force=True,
    )
    await session.commit()
    with pytest.raises(NativeStreamError) as terminal_task:
        await store.open_source(stream_id, task_run.id, uuid4(), 1, metadata_lag=True)
    assert terminal_task.value.code == "ATTEMPT_NOT_CURRENT"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await models.flow_runs.set_flow_run_state(
        session=session,
        flow_run_id=flow_run.id,
        state=schemas.states.Failed(message="final"),
        force=True,
    )
    await session.commit()
    with pytest.raises(NativeStreamError) as final:
        await store.open_source(stream_id, task_run.id, uuid4(), 2, metadata_lag=True)
    assert final.value.code == "TERMINAL_CONFLICT"

    async with db.session_context() as check:
        stream = await check.get(db.StreamPartition, stream_id)
        assert stream.owner_id is None
        assert stream.attempt == stream.owner_epoch == stream.producer_position == 0
        unbound = await check.get(db.StreamPartition, unbound_id)
        assert unbound.producer_kind is None
        assert unbound.owner_id is None


async def _recoverable_flow(db: PrefectDBInterface, flow_run) -> None:
    async with db.session_context(begin_transaction=True) as session:
        row = await session.get(db.FlowRun, flow_run.id)
        row.flow_version = "flow-v1"
        row.deployment_id = None
        row.deployment_version = None
        row.empirical_policy = schemas.core.FlowRunPolicy(retries=2)


def _source_submission(flow_run, **updates) -> StreamSubmissionReserveRequest:
    values = {
        "flow_run_id": flow_run.id,
        "submission_key": "source.documents",
        "role": "SOURCE",
        "protocol": "source-v1",
        "task_name": "documents-source",
        "task_key": "documents-source-key",
        "task_version": "task-v1",
        "flow_version": "flow-v1",
        "parameters": {"connector": {"block_document_id": "documents-v1"}},
        "limits": StreamLimits(max_records=16),
        "task_policy": schemas.core.TaskRunPolicy(retries=2),
        "tags": ["source", "documents"],
        "labels": {"profile": "bounded"},
    }
    values.update(updates)
    return StreamSubmissionReserveRequest.model_validate(values)


async def test_submission_manifest_exact_replay_is_one_atomic_identity(db, flow_run):
    await require_postgres_17(db)
    await _recoverable_flow(db, flow_run)
    store = NativeStreamStore(db)
    request = _source_submission(flow_run)
    first = await store.reserve_submission(request)
    second = await store.reserve_submission(request)
    lookup = await store.lookup_submission(
        StreamSubmissionLookupRequest(
            flow_run_id=flow_run.id, submission_key=request.submission_key
        )
    )
    assert first == second == lookup
    assert first.state == "ACTIVE"
    assert len(first.manifest_digest) == 64
    async with db.session_context() as session:
        manifest = await session.scalar(
            sa.select(db.StreamSubmission).where(
                db.StreamSubmission.flow_run_id == flow_run.id
            )
        )
        task = await session.get(db.TaskRun, first.task_run_id)
        stream = await session.get(db.StreamPartition, first.stream_id)
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.StreamSubmission.id)).where(
                    db.StreamSubmission.flow_run_id == flow_run.id
                )
            )
            == 1
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.StreamPartition.id)).where(
                    db.StreamPartition.flow_run_id == flow_run.id
                )
            )
            == 1
        )
        assert task.state_type == schemas.states.StateType.PENDING
        assert task.run_count == 0
        assert task.empirical_policy.retries == 2
        assert stream.producer_kind == "SOURCE"
        assert stream.producer_task_run_id == task.id
        assert manifest.request_digest != manifest.manifest_digest
        assert manifest.parameters == request.parameters


async def test_submission_manifest_drift_is_read_only(db, flow_run):
    await require_postgres_17(db)
    await _recoverable_flow(db, flow_run)
    store = NativeStreamStore(db)
    request = _source_submission(flow_run)
    original = await store.reserve_submission(request)
    with pytest.raises(NativeStreamError) as drift:
        await store.reserve_submission(
            _source_submission(
                flow_run,
                parameters={"connector": {"block_document_id": "documents-v2"}},
            )
        )
    assert drift.value.code == "IDENTITY_CONFLICT"
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.StreamSubmission.id)).where(
                    db.StreamSubmission.flow_run_id == flow_run.id
                )
            )
            == 1
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.StreamPartition.id)).where(
                    db.StreamPartition.flow_run_id == flow_run.id
                )
            )
            == 1
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.TaskRun.id)).where(
                    db.TaskRun.id == original.task_run_id
                )
            )
            == 1
        )


async def test_submission_manifest_binds_linear_operator_topology(db, flow_run):
    await require_postgres_17(db)
    await _recoverable_flow(db, flow_run)
    store = NativeStreamStore(db)
    source = await store.reserve_submission(_source_submission(flow_run))
    operator_request = StreamSubmissionReserveRequest(
        flow_run_id=flow_run.id,
        submission_key="operator.sign",
        role="OPERATOR",
        protocol="yield-v2",
        task_name="sign-documents",
        task_key="sign-documents-key",
        task_version="task-v1",
        flow_version="flow-v1",
        parameters={"input": {"submission_key": "source.documents"}},
        upstream_stream_id=source.stream_id,
        input_key="input",
        task_policy=schemas.core.TaskRunPolicy(retries=2),
    )
    operator = await store.reserve_submission(operator_request)
    async with db.session_context() as session:
        task = await session.get(db.TaskRun, operator.task_run_id)
        assert task.task_inputs == {
            "input": [schemas.core.TaskRunResult(id=source.task_run_id)]
        }
    with pytest.raises(NativeStreamError) as fanout:
        await store.reserve_submission(
            operator_request.model_copy(
                update={
                    "submission_key": "operator.fanout",
                    "task_key": "fanout-key",
                }
            )
        )
    assert fanout.value.code == "UNSUPPORTED_TOPOLOGY"


async def test_submission_manifest_rejects_a_second_source_root(db, flow_run):
    await require_postgres_17(db)
    await _recoverable_flow(db, flow_run)
    store = NativeStreamStore(db)
    original = await store.reserve_submission(_source_submission(flow_run))
    with pytest.raises(NativeStreamError) as second_root:
        await store.reserve_submission(
            _source_submission(
                flow_run,
                submission_key="source.second",
                task_key="second-source-key",
            )
        )
    assert second_root.value.code == "UNSUPPORTED_TOPOLOGY"
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.StreamSubmission.id)).where(
                    db.StreamSubmission.flow_run_id == flow_run.id
                )
            )
            == 1
        )
        assert await session.get(db.TaskRun, original.task_run_id) is not None


async def test_release_retains_submission_witness_and_expires_execution(db, flow_run):
    await require_postgres_17(db)
    await _recoverable_flow(db, flow_run)
    store = NativeStreamStore(db)
    request = _source_submission(flow_run)
    manifest = await store.reserve_submission(request)
    async with db.session_context(begin_transaction=True) as session:
        result = await models.flow_runs.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Cancelled(message="bounded release"),
            flow_policy=CoreFlowPolicy,
        )
        assert result.state is not None
        assert result.state.is_cancelled()
    assert await store.release_flow(flow_run.id) == 1
    witness = await store.lookup_submission(
        StreamSubmissionLookupRequest(
            flow_run_id=flow_run.id, submission_key=request.submission_key
        )
    )
    assert witness.state == "RELEASED"
    assert witness.manifest_digest == manifest.manifest_digest
    with pytest.raises(NativeStreamError) as expired:
        await store.reserve_submission(request)
    assert expired.value.code == "REPLAY_HORIZON_EXPIRED"
