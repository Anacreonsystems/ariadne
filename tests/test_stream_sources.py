"""Native source tasks use the real task engine, HTTP API and PostgreSQL."""

import concurrent.futures
import threading
from collections.abc import Callable
from typing import Any
from uuid import UUID

import cloudpickle
import httpx
import pytest

from prefect import Task, flow, task
from prefect.client.orchestration import get_client
from prefect.client.orchestration._streams.client import (
    StreamAsyncClient,
    StreamClient,
    StreamRequestError,
)
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import FlowRunContext
from prefect.exceptions import Pause
from prefect.streams import (
    SourceContext,
    SourceRecord,
    SourceRunResult,
    Stream,
    StreamProtocolError,
    StreamSourceConfig,
    StreamTaskConfig,
)
from prefect.task_runners import ProcessPoolTaskRunner, ThreadPoolTaskRunner
from prefect.utilities.asyncutils import run_coro_as_sync
from tests.test_native_stream_http import hosted_api_server as hosted_api_server
from tests.test_native_stream_http import recorded_task


def test_source_declaration_remains_an_ordinary_task():
    @task(stream=StreamSourceConfig(), retries=1)
    def source(*, ctx: SourceContext):
        yield SourceRecord(
            ctx.accepted_source_position + 1, {"ref": "object://documents/200"}
        )

    restored = cloudpickle.loads(cloudpickle.dumps(source.with_options(retries=2)))
    assert type(restored) is Task
    assert restored.stream == StreamSourceConfig()
    assert restored.retries == 2 and restored.persist_result is False
    with pytest.raises(StreamProtocolError, match="submit_stream"):
        source.submit()


@pytest.mark.parametrize(
    "options",
    [{"protocol": "yield-v2"}, {"bounded": False}, {"bounded": 1}, {"context": ""}],
)
def test_unsupported_source_config_refuses_at_declaration(options: dict[str, Any]):
    with pytest.raises(StreamProtocolError):
        StreamSourceConfig(**options)


@pytest.mark.parametrize("position", [0, -1, True, 1.0])
def test_source_position_is_strictly_positive(position: Any):
    with pytest.raises(StreamProtocolError):
        SourceRecord(position, None)


def test_source_function_requires_generator_and_context():
    with pytest.raises(StreamProtocolError, match="generator"):
        task(lambda ctx: None, stream=StreamSourceConfig())

    def missing():
        yield SourceRecord(1, None)

    with pytest.raises(StreamProtocolError, match="context"):
        task(missing, stream=StreamSourceConfig())


@pytest.mark.parametrize("action", ["__call__", "submit", "map", "delay"])
def test_source_requires_explicit_stream_submission(action: str):
    source = positioned_source("sync", [])
    with pytest.raises(StreamProtocolError):
        getattr(source, action)(1)


def test_source_rejects_finite_result_caching():
    def source(*, ctx: SourceContext):
        yield SourceRecord(1, None)

    with pytest.raises(StreamProtocolError, match="caching"):
        task(source, stream=StreamSourceConfig(), persist_result=True)


@pytest.fixture
async def source_backend(db: Any, session: Any) -> None:
    connection = await session.connection()
    version = connection.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        pytest.skip("Native source data plane requires PostgreSQL 17")


def positioned_source(
    mode: str, observations: list[tuple[UUID, int, int]], **options: Any
) -> Task[Any, Any]:
    def sync_source(count: int, *, ctx: SourceContext):
        observations.append(
            (ctx.task_run_id, ctx.attempt, ctx.accepted_source_position)
        )
        for position in range(ctx.accepted_source_position + 1, count + 1):
            yield SourceRecord(position, {"ref": f"object://documents/{position}"})
            assert ctx.accepted_source_position == position

    async def async_source(count: int, *, ctx: SourceContext):
        observations.append(
            (ctx.task_run_id, ctx.attempt, ctx.accepted_source_position)
        )
        for position in range(ctx.accepted_source_position + 1, count + 1):
            yield SourceRecord(position, {"ref": f"object://documents/{position}"})
            assert ctx.accepted_source_position == position

    return task(
        sync_source if mode == "sync" else async_source,
        stream=StreamSourceConfig(),
        **options,
    )


def intercept_http(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    fault: Callable[[dict[str, Any]], None],
    *,
    before: bool = False,
) -> None:
    synchronous = getattr(StreamClient, method)
    asynchronous = getattr(StreamAsyncClient, method)

    def sync_call(self: Any, **kwargs: Any):
        if before:
            fault(kwargs)
        result = synchronous(self, **kwargs)
        if not before:
            fault(kwargs)
        return result

    async def async_call(self: Any, **kwargs: Any):
        if before:
            fault(kwargs)
        result = await asynchronous(self, **kwargs)
        if not before:
            fault(kwargs)
        return result

    monkeypatch.setattr(StreamClient, method, sync_call)
    monkeypatch.setattr(StreamAsyncClient, method, async_call)


@pytest.mark.usefixtures("source_backend")
class TestSourceHTTP:
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_source_emits_multiple_records_from_one_native_task(self, mode: str):
        observations: list[tuple[UUID, int, int]] = []
        source = positioned_source(mode, observations)

        @task(stream=StreamTaskConfig(input="document"))
        def sign(document: Any, *, ctx: Any):
            yield {**document, "signed": True}

        @flow
        def pipeline():
            incoming = source.submit_stream(3)
            output = sign.submit_stream(incoming)
            output.wait()
            source_result = incoming.wait()
            return incoming, output, source_result

        incoming, output, result = pipeline()
        assert isinstance(incoming, Stream) and isinstance(result, SourceRunResult)
        assert result.accepted_source_position == 3
        assert observations == [(result.task_run_id, 1, 0)]
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
            {"ref": f"object://documents/{position}", "signed": True}
            for position in range(1, 4)
        ]
        actual = recorded_task(result.task_run_id, lambda run: run.state.is_completed())
        assert actual.run_count == 1
        with get_client(sync_client=True) as client:
            runs = client.read_task_runs(
                flow_run_filter=FlowRunFilter(id={"any_": [actual.flow_run_id]})
            )
        assert len(runs) == 2

    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_empty_source_has_eof_at_zero(self, mode: str):
        source = positioned_source(mode, [])

        @flow
        def pipeline():
            output = source.submit_stream(0)
            return output, output.wait()

        output, result = pipeline()
        assert result.accepted_source_position == 0
        assert (
            run_coro_as_sync(output.storage.status(output.stream_id)).state
            == "COMPLETED"
        )
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == []

    @pytest.mark.parametrize("mode", ["sync", "async"])
    @pytest.mark.parametrize("accepted", [False, True])
    def test_task_retry_resumes_before_or_after_document_200(
        self, mode: str, accepted: bool, monkeypatch: pytest.MonkeyPatch
    ):
        observations: list[tuple[UUID, int, int]] = []
        source = positioned_source(mode, observations, retries=1)
        failed = False

        def fault(request: dict[str, Any]):
            nonlocal failed
            if request["position"] == 200 and not failed:
                failed = True
                raise ValueError("declared append boundary failure")

        intercept_http(monkeypatch, "append_stream_source", fault, before=not accepted)

        @flow
        def pipeline():
            output = source.submit_stream(201)
            return output, output.wait()

        output, result = pipeline()
        assert failed
        assert observations == [
            (result.task_run_id, 1, 0),
            (result.task_run_id, 2, 200 if accepted else 199),
        ]
        assert result.accepted_source_position == 201
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 250)) == [
            {"ref": f"object://documents/{position}"} for position in range(1, 202)
        ]
        actual = recorded_task(result.task_run_id, lambda run: run.state.is_completed())
        assert actual.run_count == 2

    @pytest.mark.parametrize("mode", ["sync", "async"])
    @pytest.mark.parametrize(
        "operation",
        [
            "reserve_stream_source",
            "open_stream_source",
            "append_stream_source",
            "close_stream_source",
        ],
    )
    def test_lost_response_reuses_frozen_source_request(
        self, mode: str, operation: str, monkeypatch: pytest.MonkeyPatch
    ):
        requests: list[dict[str, Any]] = []

        def lose_reply(request: dict[str, Any]):
            if len(requests) < 2:
                requests.append(request)
            if len(requests) == 1:
                raise httpx.ReadError("declared source HTTP response loss")

        intercept_http(monkeypatch, operation, lose_reply)
        observations: list[tuple[UUID, int, int]] = []
        source = positioned_source(mode, observations)

        @flow
        def pipeline():
            output = source.submit_stream(2)
            return output, output.wait()

        output, result = pipeline()
        assert len(requests) == 2 and requests[0] == requests[1]
        assert observations == [(result.task_run_id, 1, 0)]
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
            {"ref": "object://documents/1"},
            {"ref": "object://documents/2"},
        ]

    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_retry_after_durable_eof_does_not_reenter_source(
        self, mode: str, monkeypatch: pytest.MonkeyPatch
    ):
        observations: list[tuple[UUID, int, int]] = []
        source = positioned_source(mode, observations, retries=1)
        failed = False

        def lose_eof(request: dict[str, Any]):
            nonlocal failed
            if not failed:
                failed = True
                raise ValueError("declared failure after durable EOF")

        intercept_http(monkeypatch, "close_stream_source", lose_eof)

        @flow
        def pipeline():
            output = source.submit_stream(3)
            return output.wait()

        result = pipeline()
        assert observations == [(result.task_run_id, 1, 0)]
        assert result.accepted_source_position == 3
        actual = recorded_task(result.task_run_id, lambda run: run.state.is_completed())
        assert actual.run_count == 2

    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_early_downstream_delivery_before_source_eof(self, mode: str):
        received = threading.Event()
        source_finished = threading.Event()

        def sync_source(*, ctx: SourceContext):
            yield SourceRecord(1, "first")
            assert received.wait(15), "Source completion blocked early delivery"
            yield SourceRecord(2, "tail")
            source_finished.set()

        async def async_source(*, ctx: SourceContext):
            yield SourceRecord(1, "first")
            assert received.wait(15), "Source completion blocked early delivery"
            yield SourceRecord(2, "tail")
            source_finished.set()

        source = task(
            sync_source if mode == "sync" else async_source, stream=StreamSourceConfig()
        )

        @task(stream=StreamTaskConfig(input="document"))
        def anna(document: str, *, ctx: Any):
            if document == "first":
                assert not source_finished.is_set()
                received.set()
            yield document

        @flow(task_runner=ThreadPoolTaskRunner(max_workers=4))
        def pipeline():
            incoming = source.submit_stream()
            outgoing = anna.submit_stream(incoming)
            outgoing.wait()
            incoming.wait()
            return outgoing

        output = pipeline()
        assert received.is_set() and source_finished.is_set()
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
            "first",
            "tail",
        ]

    @pytest.mark.parametrize("mode", ["sync", "async"])
    @pytest.mark.parametrize("boundary", ["submit", "worker_entry", "executor"])
    def test_start_failure_fails_exact_source_reservation(
        self, mode: str, boundary: str, monkeypatch: pytest.MonkeyPatch
    ):
        source = positioned_source(mode, [])
        outputs: list[Stream[Any]] = []
        release = threading.Event()
        if boundary == "submit":

            def reject(
                self: Any,
                task: Any,
                parameters: dict[str, Any],
                *args: Any,
                **kwargs: Any,
            ):
                outputs.append(parameters["__prefect_stream_run__"].output)
                raise ValueError("declared submit failure")

            monkeypatch.setattr(ThreadPoolTaskRunner, "submit", reject)
        elif boundary == "worker_entry":

            def reject_sync(*args: Any, **kwargs: Any):
                assert release.wait(15)
                raise ValueError("declared worker_entry failure")

            async def reject_async(*args: Any, **kwargs: Any):
                assert release.wait(15)
                raise ValueError("declared worker_entry failure")

            monkeypatch.setattr(
                "prefect.task_engine.run_task_sync"
                if mode == "sync"
                else "prefect.task_engine.run_task_async",
                reject_sync if mode == "sync" else reject_async,
            )

        @flow
        def pipeline():
            if boundary == "executor":

                def reject_executor(*args: Any, **kwargs: Any):
                    future: concurrent.futures.Future[Any] = concurrent.futures.Future()
                    future.set_exception(ValueError("declared executor failure"))
                    return future

                monkeypatch.setattr(
                    FlowRunContext.get().task_runner._executor,
                    "submit",
                    reject_executor,
                )
            output = source.submit_stream(1)
            outputs.append(output)
            release.set()
            output.wait()

        with pytest.raises(ValueError, match=f"declared {boundary} failure"):
            pipeline()
        assert len(outputs) == 1
        output = outputs[0]
        assert (
            run_coro_as_sync(output.storage.status(output.stream_id)).state == "FAILED"
        )
        actual = recorded_task(
            output.producer_task_run_id, lambda run: run.state.is_failed()
        )
        assert actual.run_count == 0

    @pytest.mark.parametrize(
        "fault", ["gap", "wrong_event", "non_json", "non_none_return"]
    )
    def test_invalid_source_never_creates_successful_eof(self, fault: str):
        @task(stream=StreamSourceConfig())
        def source(*, ctx: SourceContext):
            yield SourceRecord(1, "accepted")
            if fault == "gap":
                yield SourceRecord(3, "gap")
            elif fault == "wrong_event":
                yield "unpositioned"
            elif fault == "non_json":
                yield SourceRecord(2, {object()})
            else:
                return {"invalid": "return value"}

        outputs: list[Stream[Any]] = []

        @flow
        def pipeline():
            output = source.submit_stream()
            outputs.append(output)
            output.wait()

        with pytest.raises(StreamProtocolError):
            pipeline()
        output = outputs[0]
        assert (
            run_coro_as_sync(output.storage.status(output.stream_id)).state == "FAILED"
        )
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
            "accepted"
        ]

    def test_record_count_does_not_expand_task_state_history(self):
        source = positioned_source("sync", [])
        counts: list[tuple[int, int]] = []

        @flow
        def pipeline(count: int):
            output = source.submit_stream(count)
            return output.wait()

        for count in (1, 64):
            result = pipeline(count)
            actual = recorded_task(
                result.task_run_id, lambda run: run.state.is_completed()
            )
            with get_client(sync_client=True) as client:
                runs = client.read_task_runs(
                    flow_run_filter=FlowRunFilter(id={"any_": [actual.flow_run_id]})
                )
                states = client.read_task_run_states(actual.id)
            counts.append((len(runs), len(states)))
        assert counts[0] == counts[1]
        assert counts[0][0] == 1 and counts[0][1] <= 4

    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_pause_preserves_open_source_and_accepted_prefix(self, mode: str):
        def sync_source(*, ctx: SourceContext):
            yield SourceRecord(1, "accepted")
            raise Pause("declared source pause")

        async def async_source(*, ctx: SourceContext):
            yield SourceRecord(1, "accepted")
            raise Pause("declared source pause")

        source = task(
            sync_source if mode == "sync" else async_source, stream=StreamSourceConfig()
        )

        @flow
        def pipeline():
            output = source.submit_stream()
            with pytest.raises(Pause):
                output.wait()
            return output

        output = pipeline()
        assert run_coro_as_sync(output.storage.status(output.stream_id)).state == "OPEN"
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
            "accepted"
        ]

    @pytest.mark.parametrize(
        "runner",
        [ThreadPoolTaskRunner(max_workers=1), ProcessPoolTaskRunner(max_workers=2)],
    )
    def test_unsupported_runner_refuses_before_registering_source(self, runner: Any):
        source = positioned_source("sync", [])

        @flow(task_runner=runner)
        def pipeline():
            flow_id = FlowRunContext.get().flow_run.id
            with pytest.raises(StreamProtocolError):
                source.submit_stream(1)
            return flow_id

        flow_id = pipeline()
        with get_client(sync_client=True) as client:
            assert (
                client.read_task_runs(
                    flow_run_filter=FlowRunFilter(id={"any_": [flow_id]})
                )
                == []
            )

    def test_frozen_source_capacity_is_a_permanent_bounded_refusal(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        def freeze_limits(request: dict[str, Any]):
            request["limits"] = {"max_records": 1}

        intercept_http(monkeypatch, "reserve_stream_source", freeze_limits, before=True)
        attempts: list[dict[str, Any]] = []
        intercept_http(
            monkeypatch, "append_stream_source", attempts.append, before=True
        )
        source = positioned_source("sync", [])
        outputs: list[Stream[Any]] = []

        @flow
        def pipeline():
            output = source.submit_stream(2)
            outputs.append(output)
            output.wait()

        with pytest.raises(StreamRequestError) as refused:
            pipeline()
        assert refused.value.code == "CAPACITY"
        assert [request["position"] for request in attempts] == [1, 2]
        output = outputs[0]
        assert (
            run_coro_as_sync(output.storage.status(output.stream_id)).state == "FAILED"
        )
        assert run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10)) == [
            {"ref": "object://documents/1"}
        ]
