from __future__ import annotations

import asyncio
import concurrent.futures
import os
import threading
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import cloudpickle
import pytest

from prefect import Task, flow, task
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import FlowRunContext, TaskRunContext
from prefect.exceptions import Pause
from prefect.streams import Stream, StreamProtocolError, StreamTaskConfig
from prefect.task_runners import ProcessPoolTaskRunner, ThreadPoolTaskRunner
from prefect.utilities.asyncutils import run_coro_as_sync
from tests._native_stream_postgres import (
    NativeStreamAdapterError,
    PostgresNativeStreamStorage,
    StaleNativeStreamOwner,
)


@pytest.fixture
async def stream_storage() -> AsyncGenerator[PostgresNativeStreamStorage, None]:
    dsn = os.environ.get("ARIADNE_NATIVE_STREAM_TEST_DSN")
    if not dsn:
        pytest.skip("Set ARIADNE_NATIVE_STREAM_TEST_DSN to run real PostgreSQL tests")
    storage = await PostgresNativeStreamStorage.open(dsn)
    try:
        yield storage
    finally:
        await storage.close()


def test_stream_task_remains_a_task_and_preserves_options():
    @task(stream=StreamTaskConfig(input="document"), retries=1)
    def transform(document: int, *, ctx: Any):
        yield document

    assert type(transform) is Task
    assert transform.stream == StreamTaskConfig(input="document")
    configured = transform.with_options(retries=2, name="replacement")
    restored = cloudpickle.loads(cloudpickle.dumps(configured))
    assert type(restored) is Task
    assert restored.stream == transform.stream
    assert restored.retries == 2
    assert restored.persist_result is False


@pytest.mark.parametrize(
    "options",
    [
        {"source": True},
        {"output_cardinality": 2},
        {"allow_empty": True},
        {"protocol": "unknown"},
    ],
)
def test_unsupported_profiles_fail_before_task_creation(options: dict[str, Any]):
    with pytest.raises(StreamProtocolError):
        StreamTaskConfig(input="document", **options)


def test_invalid_context_and_finite_caching_fail_at_definition():
    with pytest.raises(StreamProtocolError, match="ctx"):
        task(lambda document: document, stream=StreamTaskConfig(input="document"))
    with pytest.raises(StreamProtocolError, match="caching"):
        task(
            lambda document, ctx: None,
            stream=StreamTaskConfig(input="document"),
            persist_result=True,
        )


def test_mapping_and_deferred_execution_fail_before_submission():
    @task(stream=StreamTaskConfig(input="document"))
    def transform(document: int, *, ctx: Any):
        yield document

    with pytest.raises(StreamProtocolError, match="map"):
        transform.map([1, 2])
    with pytest.raises(StreamProtocolError, match="deferred"):
        transform.delay(document=1)
    with pytest.raises(StreamProtocolError, match="submit_stream"):
        transform(1)
    with pytest.raises(StreamProtocolError, match="submit_stream"):
        transform.submit(1)


def test_collection_needs_a_finite_explicit_bound():
    stream = Stream(None, "unopened")  # type: ignore[arg-type]
    for limit in (0, -1, True):
        with pytest.raises(StreamProtocolError, match="positive"):
            stream.collect(limit=limit)
    assert stream.dependency_kind == "stream"
    assert stream.collect(limit=10).dependency_kind == "stream_collection"


def test_ordinary_generator_behavior_is_preserved():
    @task
    def ordinary():
        yield 1
        yield 2

    @flow
    def pipeline():
        return list(ordinary())

    assert pipeline() == [1, 2]


def test_native_early_delivery_and_mixed_finite_dependencies(
    stream_storage: PostgresNativeStreamStorage,
    events_pipeline: Any,
    sync_prefect_client: Any,
):
    storage = stream_storage
    run_coro_as_sync(storage.create_stream("source"))
    downstream_accepted = threading.Event()
    producer_finished = threading.Event()
    identities: dict[str, UUID] = {}

    @task
    def source():
        identities["source"] = TaskRunContext.get().task_run.id
        run_coro_as_sync(storage.append("source", 1))
        assert downstream_accepted.wait(15), "downstream waited for producer completion"
        run_coro_as_sync(storage.append("source", 2))
        run_coro_as_sync(storage.finish("source"))
        producer_finished.set()

    @task
    def offset():
        return 10

    @task(stream=StreamTaskConfig(input="document"))
    def transform(document: int, increment: int, *, ctx: Any):
        identities["transform"] = ctx.task_run_id
        ctx.managed_state["count"] = ctx.managed_state.get("count", 0) + 1
        yield document + increment

    @task
    def first(values: list[int]):
        assert values == [11]
        assert not producer_finished.is_set()
        downstream_accepted.set()
        return values[0]

    @task
    def total(values: list[int]):
        return sum(values)

    @flow(task_runner=ThreadPoolTaskRunner(max_workers=6))
    def pipeline():
        producer = source.submit()
        incoming = Stream(storage, "source", producer_task_run_id=producer.task_run_id)
        output = transform.submit_stream(incoming, offset.submit())
        first_result = first.submit(output.collect(limit=1))
        result = total.submit(output.collect(limit=10)).result()
        first_result.result()
        output.wait()
        producer.result()
        return result

    assert pipeline() == 23
    position = run_coro_as_sync(storage.inspect_consumer(str(identities["transform"])))
    assert position["completed_through"] == 2
    assert position["managed_state"] == {"count": 2}
    events_pipeline.process_events(_sync=True)
    transform_run = sync_prefect_client.read_task_run(identities["transform"])
    assert transform_run.state.is_completed()
    assert {i.id for i in transform_run.task_inputs["document"]} == {
        identities["source"]
    }


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("fault", ["before_commit", "lost_ack"])
def test_real_postgres_terminal_failure_and_native_retry(
    stream_storage: PostgresNativeStreamStorage,
    mode: str,
    fault: str,
    events_pipeline: Any,
    sync_prefect_client: Any,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [200, 201]))
    if fault == "before_commit":
        storage.fail_before_commit()
    else:
        storage.lose_completion_ack()
    calls: list[tuple[UUID, int, int]] = []

    def observe(document: int, ctx: Any) -> None:
        calls.append((ctx.task_run_id, ctx.attempt, document))
        ctx.managed_state["count"] = ctx.managed_state.get("count", 0) + 1

    def sync_body(document: int, *, ctx: Any):
        observe(document, ctx)
        yield document * 2

    async def async_body(document: int, *, ctx: Any):
        observe(document, ctx)
        await asyncio.sleep(0)
        yield document * 2

    operator = task(
        sync_body if mode == "sync" else async_body,
        stream=StreamTaskConfig(input="document"),
        retries=1,
    )

    @flow(task_runner=ThreadPoolTaskRunner(max_workers=4))
    def pipeline():
        output = operator.submit_stream(Stream(storage, "source"))
        result = output.wait()
        return output.stream_id, result.consumer_id

    output_id, consumer_id = pipeline()
    assert run_coro_as_sync(storage.inspect_output(output_id)) == [400, 402]
    position = run_coro_as_sync(storage.inspect_consumer(consumer_id))
    assert position["completed_through"] == 2
    assert position["managed_state"] == {"count": 2}
    assert len({c[0] for c in calls}) == 1
    assert calls[-1][1] == 2
    assert [c[2] for c in calls] == (
        [200, 200, 201] if fault == "before_commit" else [200, 201]
    )
    assert position["replay_count"] == (1 if fault == "before_commit" else 0)
    assert storage.fault_counts[fault] == 1
    events_pipeline.process_events(_sync=True)
    native_run = sync_prefect_client.read_task_run(UUID(consumer_id))
    assert native_run.run_count == 2
    assert native_run.state.is_completed()


def test_terminal_acceptance_freezes_state_before_generator_cleanup(
    stream_storage: PostgresNativeStreamStorage,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1, 2]))

    @task(stream=StreamTaskConfig(input="document"))
    def transform(document: int, *, ctx: Any):
        ctx.managed_state["count"] = ctx.managed_state.get("count", 0) + 1
        try:
            yield document
            raise AssertionError("generator resumed after terminal yield")
        finally:
            ctx.managed_state["count"] = -100

    @flow
    def pipeline():
        return transform.submit_stream(Stream(storage, "source")).wait()

    result = pipeline()
    position = run_coro_as_sync(storage.inspect_consumer(result.consumer_id))
    assert position["managed_state"] == {"count": 2}


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_terminal_return_preserves_state_without_an_output_envelope(
    stream_storage: PostgresNativeStreamStorage,
    mode: str,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1, 2]))

    def sync_body(document: int, *, ctx: Any):
        ctx.managed_state["total"] = ctx.managed_state.get("total", 0) + document

    async def async_body(document: int, *, ctx: Any):
        ctx.managed_state["total"] = ctx.managed_state.get("total", 0) + document

    operator = task(
        sync_body if mode == "sync" else async_body,
        stream=StreamTaskConfig(input="document"),
    )

    @flow
    def pipeline():
        return operator.submit_stream(Stream(storage, "source")).wait()

    result = pipeline()
    assert run_coro_as_sync(storage.inspect_output(result.output_stream_id)) == []
    position = run_coro_as_sync(storage.inspect_consumer(result.consumer_id))
    assert position["managed_state"] == {"total": 3}
    assert position["completed_through"] == 2


@pytest.mark.parametrize("fault", ["body", "before_commit", "lost_ack"])
@pytest.mark.timeout(120)
def test_document_200_recovery_uses_its_actual_durable_sequence(
    stream_storage: PostgresNativeStreamStorage,
    fault: str,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", list(range(1, 202))))
    calls: list[tuple[UUID, int, int]] = []

    @task(stream=StreamTaskConfig(input="document"), retries=1)
    def anna(document: int, *, ctx: Any):
        calls.append((ctx.task_run_id, ctx.attempt, ctx.input_sequence))
        if ctx.input_sequence == 200 and ctx.attempt == 1:
            position = run_coro_as_sync(storage.inspect_consumer(ctx.consumer_id))
            assert position["completed_through"] == 199
            assert ctx.managed_state == {"count": 199}
            if fault == "body":
                raise OSError("Anna is unavailable before yield")
            if fault == "before_commit":
                storage.fail_before_commit()
            else:
                storage.lose_completion_ack()
        ctx.managed_state["count"] = ctx.managed_state.get("count", 0) + 1
        yield document

    @flow
    def pipeline():
        return anna.submit_stream(Stream(storage, "source")).wait()

    result = pipeline()
    position = run_coro_as_sync(storage.inspect_consumer(result.consumer_id))
    assert position["completed_through"] == 201
    assert position["managed_state"] == {"count": 201}
    assert run_coro_as_sync(storage.inspect_output(result.output_stream_id)) == list(
        range(1, 202)
    )
    assert len({task_id for task_id, _, _ in calls}) == 1
    assert [seq for _, attempt, seq in calls if attempt == 2] == (
        [201] if fault == "lost_ack" else [200, 201]
    )


@pytest.mark.parametrize("count", [1, 1000])
@pytest.mark.timeout(150)
def test_record_count_does_not_create_task_runs_or_state_events(
    stream_storage: PostgresNativeStreamStorage,
    count: int,
    sync_prefect_client: Any,
    events_pipeline: Any,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", list(range(count))))
    identifiers: dict[str, UUID] = {}

    @task(stream=StreamTaskConfig(input="document"))
    def transform(document: int, *, ctx: Any):
        yield document + 1

    @task
    def total(values: list[int]):
        return sum(values)

    @flow(task_runner=ThreadPoolTaskRunner(max_workers=4))
    def pipeline():
        identifiers["flow"] = FlowRunContext.get().flow_run.id
        first = transform.submit_stream(Stream(storage, "source"))
        second = transform.submit_stream(first)
        finite = total.submit(second.collect(limit=count))
        result = finite.result()
        first.wait()
        second.wait()
        assert len(FlowRunContext.get().run_results) < 10
        return result

    assert pipeline() == sum(i + 2 for i in range(count))
    events_pipeline.process_events(_sync=True)
    runs = sync_prefect_client.read_task_runs(
        flow_run_filter=FlowRunFilter(id={"any_": [identifiers["flow"]]})
    )
    assert len(runs) == 3
    assert all(run.run_count == 1 and run.state.is_completed() for run in runs)
    for run in runs:
        assert len(sync_prefect_client.read_task_run_states(run.id)) <= 4
    counts = run_coro_as_sync(storage.counts())
    assert counts["bindings"] == counts["consumers"] == 2
    assert counts["completions"] == count * 2


async def test_stale_and_conflicting_terminals_rejected_by_real_storage(
    stream_storage: PostgresNativeStreamStorage,
    prefect_client: Any,
):
    storage = stream_storage
    await storage.seed("source", [200])
    await storage.create_stream("output")
    native_run = await prefect_client.create_task_run(
        task(lambda: None), flow_run_id=None, dynamic_key=uuid4().hex
    )
    consumer = str(native_run.id)
    old = await storage.open_consumer("source", "output", consumer, "anna", 1)
    old_record = await storage.read(old)
    replacement = await storage.open_consumer("source", "output", consumer, "tanya", 2)
    with pytest.raises(StaleNativeStreamOwner):
        await storage.complete(old, old_record, "YIELD", 999, {"count": 999})
    assert await storage.inspect_output("output") == []
    position = await storage.inspect_consumer(consumer)
    assert position["completed_through"] == 0
    record = await storage.read(replacement)
    await storage.complete(replacement, record, "YIELD", 400, {"count": 1})
    before = await storage.counts()
    await storage.complete(replacement, record, "YIELD", 400, {"count": 1})
    with pytest.raises(NativeStreamAdapterError):
        await storage.complete(replacement, record, "YIELD", 401, {"count": 1})
    assert await storage.counts() == before
    assert await storage.inspect_output("output") == [400]


def test_finite_input_and_unsupported_runner_fail_before_consuming(
    stream_storage: PostgresNativeStreamStorage,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1]))

    @task
    def finite(value: Any):
        raise AssertionError("unbounded stream entered a finite task")

    @task(stream=StreamTaskConfig(input="document"))
    def transform(document: int, *, ctx: Any):
        yield document

    @flow
    def invalid_finite():
        return finite(Stream(storage, "source"))

    with pytest.raises(StreamProtocolError, match="collect"):
        invalid_finite()

    @flow(task_runner=ProcessPoolTaskRunner(max_workers=2))
    def invalid_runner():
        return transform.submit_stream(Stream(storage, "source"))

    with pytest.raises(StreamProtocolError, match="ThreadPool"):
        invalid_runner()
    counts = run_coro_as_sync(storage.counts())
    assert counts["consumers"] == counts["completions"] == 0
    assert counts["streams"] == 1


def test_finite_future_cannot_hide_an_unbounded_stream(
    stream_storage: PostgresNativeStreamStorage,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1]))

    @task(persist_result=False)
    def existing_handle():
        return Stream(storage, "source")

    @task
    def finite(value: Any):
        raise AssertionError("A future bypassed stream dependency validation")

    @flow
    def pipeline():
        return finite(existing_handle.submit())

    with pytest.raises(StreamProtocolError, match="collect"):
        pipeline()
    assert run_coro_as_sync(storage.counts())["consumers"] == 0


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_completed_tail_reconciles_after_lost_finish_ack(
    stream_storage: PostgresNativeStreamStorage,
    mode: str,
    sync_prefect_client: Any,
    events_pipeline: Any,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1, 2]))
    storage.lose_finish_ack()
    invoked: list[int] = []

    def sync_body(document: int, *, ctx: Any):
        invoked.append(document)
        yield document

    async def async_body(document: int, *, ctx: Any):
        invoked.append(document)
        yield document

    operator = task(
        sync_body if mode == "sync" else async_body,
        stream=StreamTaskConfig(input="document"),
        retries=1,
    )

    @flow
    def pipeline():
        return operator.submit_stream(Stream(storage, "source")).wait()

    result = pipeline()
    assert invoked == [1, 2]
    assert run_coro_as_sync(storage.inspect_output(result.output_stream_id)) == [1, 2]
    assert (
        run_coro_as_sync(storage.status(result.output_stream_id)).state == "COMPLETED"
    )
    events_pipeline.process_events(_sync=True)
    actual = sync_prefect_client.read_task_run(UUID(result.consumer_id))
    assert actual.run_count == 2 and actual.state.is_completed()


@pytest.mark.parametrize("boundary", ["submit", "before_task_run", "executor"])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_start_failure_closes_output_and_preserves_original_error(
    stream_storage: PostgresNativeStreamStorage,
    boundary: str,
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1]))
    outputs: list[Stream[Any]] = []

    def sync_body(document: int, *, ctx: Any):
        yield document

    async def async_body(document: int, *, ctx: Any):
        yield document

    transform = task(
        sync_body if mode == "sync" else async_body,
        stream=StreamTaskConfig(input="document"),
    )

    if boundary == "submit":

        def fail_submit(
            self: Any, task: Any, parameters: dict[str, Any], *args: Any, **kwargs: Any
        ):
            outputs.append(parameters["__prefect_stream_run__"].output)
            raise ValueError("declared submit failure")

        monkeypatch.setattr(ThreadPoolTaskRunner, "submit", fail_submit)
    elif boundary == "before_task_run":

        def fail_start(*args: Any, **kwargs: Any):
            raise ValueError("declared before_task_run failure")

        if mode == "sync":
            monkeypatch.setattr(
                "prefect.task_engine._create_task_run_locally", fail_start
            )
        else:
            monkeypatch.setattr(Task, "create_local_run", fail_start)

    @flow
    def pipeline():
        if boundary == "executor":

            def fail_executor(*args: Any, **kwargs: Any):
                future = concurrent.futures.Future()
                future.set_exception(ValueError("declared executor failure"))
                return future

            monkeypatch.setattr(
                FlowRunContext.get().task_runner._executor, "submit", fail_executor
            )
        output = transform.submit_stream(Stream(storage, "source"))
        outputs.append(output)
        output.wait()

    with pytest.raises(ValueError, match=f"declared {boundary} failure"):
        pipeline()
    assert len(outputs) == 1
    assert run_coro_as_sync(storage.status(outputs[0].stream_id)).state == "FAILED"
    counts = run_coro_as_sync(storage.counts())
    assert counts["consumers"] == counts["completions"] == 0


def test_nested_stream_side_input_rejected_before_output_or_task_run(
    stream_storage: PostgresNativeStreamStorage,
    sync_prefect_client: Any,
    events_pipeline: Any,
):
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1]))
    identifiers: dict[str, UUID] = {}

    @task(stream=StreamTaskConfig(input="document"))
    def transform(document: int, side: Any, *, ctx: Any):
        yield document

    @flow
    def pipeline():
        identifiers["flow"] = FlowRunContext.get().flow_run.id
        stream = Stream(storage, "source")
        with pytest.raises(StreamProtocolError, match="declared stream input"):
            transform.submit_stream(stream, side={"nested": [stream]})

    pipeline()
    events_pipeline.process_events(_sync=True)
    assert (
        sync_prefect_client.read_task_runs(
            flow_run_filter=FlowRunFilter(id={"any_": [identifiers["flow"]]})
        )
        == []
    )
    counts = run_coro_as_sync(storage.counts())
    assert counts["streams"] == 1 and counts["consumers"] == 0


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_pause_control_signal_preserves_open_output(
    stream_storage: PostgresNativeStreamStorage,
    mode: str,
):
    """Exercise native control propagation; full server resume is a later gate."""
    storage = stream_storage
    run_coro_as_sync(storage.seed("source", [1]))

    def sync_body(document: int, *, ctx: Any):
        raise Pause("declared control signal")

    async def async_body(document: int, *, ctx: Any):
        raise Pause("declared control signal")

    operator = task(
        sync_body if mode == "sync" else async_body,
        stream=StreamTaskConfig(input="document"),
    )

    @flow
    def pipeline():
        output = operator.submit_stream(Stream(storage, "source"))
        with pytest.raises(Pause):
            output.wait()
        return output.stream_id

    output_id = pipeline()
    assert run_coro_as_sync(storage.status(output_id)).state == "OPEN"
    assert run_coro_as_sync(storage.counts())["completions"] == 0
