"""Actual native SDK -> HTTP API -> migrated PostgreSQL integration."""

import concurrent.futures
import threading
import time
from collections.abc import AsyncGenerator, Callable
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from prefect import Task, flow, task
from prefect.client.orchestration import get_client
from prefect.client.orchestration._streams.client import StreamAsyncClient, StreamClient
from prefect.client.schemas.filters import FlowRunFilter
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.context import FlowRunContext
from prefect.settings import (
    PREFECT_API_SERVICES_TASK_RUN_RECORDER_ENABLED,
    temporary_settings,
)
from prefect.states import Completed, Running
from prefect.streams import Stream, StreamTaskConfig
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.testing.fixtures import hosted_api_server as hosted_api_fixture
from prefect.utilities.asyncutils import run_coro_as_sync


@pytest.fixture(scope="module")
async def hosted_api_server(
    unused_tcp_port_factory: Callable[[], int],
    test_database_connection_url: str,
) -> AsyncGenerator[str, None]:
    # Root tests intentionally disable the recorder. Native admission needs the
    # actual service, not a forced state or a test adapter's metadata bridge.
    with temporary_settings(
        updates={PREFECT_API_SERVICES_TASK_RUN_RECORDER_ENABLED: True}
    ):
        server = hosted_api_fixture.__wrapped__(
            unused_tcp_port_factory, test_database_connection_url
        )
        try:
            yield await server.__anext__()
        finally:
            await server.aclose()


def native_source(
    values: list[Any], *, finish: bool = True, binding_out: list[Any] | None = None
) -> Stream[Any]:
    """Seed the already-defined low-level source protocol, not a source decorator."""
    context = FlowRunContext.get()
    assert context is not None and context.flow_run is not None
    flow_id = context.flow_run.id
    with get_client(sync_client=True) as client:
        source_task = task(lambda: None, name="native-input")
        source = client.create_task_run(
            task=source_task,
            flow_run_id=flow_id,
            dynamic_key=uuid4().hex,
            id=uuid4(),
            state=Running(),
        )
        stream_id = uuid4()
        client.create_stream(flow_run_id=flow_id, stream_id=stream_id)
        binding = client.claim_stream_source(
            stream_id=stream_id,
            task_run_id=source.id,
            owner_id=uuid4(),
            attempt=source.run_count,
        )
        for position, value in enumerate(values, 1):
            client.append_stream_source(
                **binding.model_dump(), position=position, value=value
            )
        if binding_out is not None:
            binding_out.append(binding)
        if finish:
            client.finish_stream_source(**binding.model_dump())
            client.set_task_run_state(source.id, Completed())
    return Stream(
        PrefectStreamStorage(flow_id), str(stream_id), producer_task_run_id=source.id
    )


@pytest.fixture(autouse=True)
async def native_backend(db: Any, session: Any) -> None:
    connection = await session.connection()
    version = connection.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        pytest.skip("Native HTTP integration requires PostgreSQL 17")


def test_sync_task_uses_native_http_storage():
    @task(stream=StreamTaskConfig(input="document"))
    def sign(document: int, *, ctx: Any):
        ctx.managed_state["count"] = ctx.managed_state.get("count", 0) + 1
        yield {"document": document, "signed": True}

    @flow
    def pipeline():
        output = sign.submit_stream(native_source([1, 2, 3]))
        output.wait()
        return run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10))

    assert pipeline() == [
        {"document": 1, "signed": True},
        {"document": 2, "signed": True},
        {"document": 3, "signed": True},
    ]


def stream_operator(
    mode: str, invoke: Callable[[int, Any], Any], **options: Any
) -> Task[Any, Any]:
    def sync_body(document: int, *, ctx: Any):
        yield invoke(document, ctx)

    async def async_body(document: int, *, ctx: Any):
        yield invoke(document, ctx)

    return task(
        sync_body if mode == "sync" else async_body,
        stream=StreamTaskConfig(input="document"),
        **options,
    )


def recorded_task(task_id: UUID, predicate: Callable[[Any], bool]) -> Any:
    deadline = time.monotonic() + 15
    with get_client(sync_client=True) as client:
        while True:
            run = client.read_task_run(task_id)
            if predicate(run):
                return run
            assert time.monotonic() < deadline, (
                f"TaskRun metadata did not converge: {run}"
            )
            time.sleep(0.05)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_final_task_failure_closes_owned_native_output(mode: str):
    def invoke(document: int, ctx: Any):
        if document == 200:
            raise ValueError("declared terminal failure")
        return document

    operator = stream_operator(mode, invoke)
    outputs: list[Stream[Any]] = []

    @flow
    def pipeline():
        output = operator.submit_stream(native_source([199, 200, 201]))
        outputs.append(output)
        output.wait()

    with pytest.raises(ValueError, match="declared terminal failure"):
        pipeline()
    output = outputs[0]
    recorded_task(output.producer_task_run_id, lambda run: run.state.is_failed())
    assert run_coro_as_sync(output.storage.status(output.stream_id)).state == "FAILED"
    assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [199]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_native_retry_repeats_only_unaccepted_input(mode: str):
    calls: list[tuple[UUID, int, int, int]] = []

    def invoke(document: int, ctx: Any):
        count = ctx.managed_state.get("count", 0)
        calls.append((ctx.task_run_id, ctx.attempt, document, count))
        if document == 200 and ctx.attempt == 1:
            raise ValueError("declared failure before yield")
        ctx.managed_state["count"] = count + 1
        return document

    operator = stream_operator(mode, invoke, retries=1)

    @flow
    def pipeline():
        output = operator.submit_stream(native_source([199, 200, 201]))
        output.wait()
        return output

    output = pipeline()
    assert len({row[0] for row in calls}) == 1
    assert [row[1:] for row in calls] == [
        (1, 199, 0),
        (1, 200, 1),
        (2, 200, 1),
        (2, 201, 2),
    ]
    assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
        199,
        200,
        201,
    ]
    recorded = recorded_task(
        output.producer_task_run_id, lambda run: run.state.is_completed()
    )
    assert recorded.run_count == 2
    with get_client(sync_client=True) as client:
        runs = client.read_task_runs(
            flow_run_filter=FlowRunFilter(id={"any_": [recorded.flow_run_id]})
        )
    assert (
        len(runs) == 2
    )  # One source, one operator; records create neither runs nor states.


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    "operation",
    [
        "reserve_stream_output",
        "open_stream_consumer",
        "read_stream_consumer",
        "complete_stream_input",
        "finish_stream_consumer",
    ],
)
def test_lost_http_response_reuses_exact_request(
    mode: str, operation: str, monkeypatch: pytest.MonkeyPatch
):
    lost = threading.Event()
    retried: list[dict[str, Any]] = []
    originals = (
        getattr(StreamClient, operation),
        getattr(StreamAsyncClient, operation),
    )

    def synchronous(self: Any, **kwargs: Any):
        result = originals[0](self, **kwargs)
        if not lost.is_set():
            retried.append(kwargs)
            lost.set()
            raise httpx.ReadError("declared lost response after real HTTP acceptance")
        if len(retried) == 1:
            retried.append(kwargs)
        return result

    async def asynchronous(self: Any, **kwargs: Any):
        result = await originals[1](self, **kwargs)
        if not lost.is_set():
            retried.append(kwargs)
            lost.set()
            raise httpx.ReadError("declared lost response after real HTTP acceptance")
        if len(retried) == 1:
            retried.append(kwargs)
        return result

    monkeypatch.setattr(StreamClient, operation, synchronous)
    monkeypatch.setattr(StreamAsyncClient, operation, asynchronous)
    invoked: list[int] = []

    def invoke(document: int, ctx: Any):
        invoked.append(document)
        return document

    operator = stream_operator(mode, invoke)

    @flow
    def pipeline():
        output = operator.submit_stream(native_source([200, 201]))
        output.wait()
        return output

    output = pipeline()
    assert lost.is_set() and len(retried) == 2
    assert retried[0] == retried[1]
    assert invoked == [200, 201]
    assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
        200,
        201,
    ]
    assert (
        run_coro_as_sync(output.storage.status(output.stream_id)).state == "COMPLETED"
    )


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("boundary", ["submit", "worker_entry", "executor"])
def test_native_start_failure_closes_reserved_output_and_registered_task(
    mode: str, boundary: str, monkeypatch: pytest.MonkeyPatch
):
    operator = stream_operator(mode, lambda document, ctx: document)
    outputs: list[Stream[Any]] = []
    release_entry = threading.Event()

    if boundary == "submit":

        def fail_submit(
            self: Any, task: Any, parameters: dict[str, Any], *args: Any, **kwargs: Any
        ):
            outputs.append(parameters["__prefect_stream_run__"].output)
            raise ValueError("declared submit failure")

        monkeypatch.setattr(ThreadPoolTaskRunner, "submit", fail_submit)
    elif boundary == "worker_entry":

        def fail_entry(*args: Any, **kwargs: Any):
            assert release_entry.wait(15), "Callback was not registered"
            raise ValueError("declared worker_entry failure")

        async def fail_async_entry(*args: Any, **kwargs: Any):
            assert release_entry.wait(15), "Callback was not registered"
            raise ValueError("declared worker_entry failure")

        monkeypatch.setattr(
            "prefect.task_engine.run_task_sync"
            if mode == "sync"
            else "prefect.task_engine.run_task_async",
            fail_entry if mode == "sync" else fail_async_entry,
        )

    @flow
    def pipeline():
        source = native_source([1])
        if boundary == "executor":

            def fail_executor(*args: Any, **kwargs: Any):
                future: concurrent.futures.Future[Any] = concurrent.futures.Future()
                future.set_exception(ValueError("declared executor failure"))
                return future

            monkeypatch.setattr(
                FlowRunContext.get().task_runner._executor, "submit", fail_executor
            )
        output = operator.submit_stream(source)
        outputs.append(output)
        release_entry.set()
        output.wait()

    with pytest.raises(ValueError, match=f"declared {boundary} failure"):
        pipeline()
    assert len(outputs) == 1
    output = outputs[0]
    assert run_coro_as_sync(output.storage.status(output.stream_id)).state == "FAILED"
    failed = recorded_task(
        output.producer_task_run_id, lambda run: run.state.is_failed()
    )
    assert failed.run_count == 0
    assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == []


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_retry_failing_before_admission_closes_previous_owner_output(
    mode: str, monkeypatch: pytest.MonkeyPatch
):
    original = PrefectStreamStorage.open_consumer

    async def open_consumer(
        self: Any,
        input_id: str,
        output_id: str,
        consumer_id: str,
        owner_id: str,
        attempt: int,
    ):
        if attempt == 2:
            raise ValueError("declared retry admission failure")
        return await original(self, input_id, output_id, consumer_id, owner_id, attempt)

    monkeypatch.setattr(PrefectStreamStorage, "open_consumer", open_consumer)

    def invoke(document: int, ctx: Any):
        if document == 200:
            raise ValueError("declared first-attempt failure")
        return document

    operator = stream_operator(mode, invoke, retries=1)
    outputs: list[Stream[Any]] = []

    @flow
    def pipeline():
        output = operator.submit_stream(native_source([199, 200]))
        outputs.append(output)
        output.wait()

    with pytest.raises(ValueError, match="declared retry admission failure"):
        pipeline()
    output = outputs[0]
    failed = recorded_task(
        output.producer_task_run_id, lambda run: run.state.is_failed()
    )
    assert failed.run_count == 2
    assert run_coro_as_sync(output.storage.status(output.stream_id)).state == "FAILED"
    assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [199]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_downstream_receives_first_record_before_source_finishes(mode: str):
    source_bindings: list[Any] = []
    operator = stream_operator(mode, lambda document, ctx: document + 10)

    @task
    def first(values: list[int]):
        assert values == [11]
        return values[0]

    @flow(task_runner=ThreadPoolTaskRunner(max_workers=4))
    def pipeline():
        source = native_source([1], finish=False, binding_out=source_bindings)
        output = operator.submit_stream(source)
        assert first.submit(output.collect(limit=1)).result() == 11
        assert run_coro_as_sync(source.storage.status(source.stream_id)).state == "OPEN"
        assert run_coro_as_sync(output.storage.status(output.stream_id)).state == "OPEN"
        with get_client(sync_client=True) as client:
            client.append_stream_source(
                **source_bindings[0].model_dump(), position=2, value=2
            )
            client.finish_stream_source(**source_bindings[0].model_dump())
            client.set_task_run_state(source.producer_task_run_id, Completed())
        output.wait()
        return output

    output = pipeline()
    assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
        11,
        12,
    ]
