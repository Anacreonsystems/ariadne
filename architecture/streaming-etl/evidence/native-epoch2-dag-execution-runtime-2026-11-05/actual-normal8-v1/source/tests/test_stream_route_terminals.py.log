"""Internal DAG terminal controls, without enabling public DAG execution.

Transport controls retain the real adapter and mirrored request validation.
The injected engine controls run the existing per-input sync/async loops, not
a production DAG Flow or its still-fenced process bootstrap.
"""

import asyncio
import copy
import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest

from prefect import task
from prefect._internal import stream_routed_storage
from prefect._internal.stream_engine import (
    _StreamRun,
    execute_source_async,
    execute_source_sync,
    execute_stream_async,
    execute_stream_sync,
)
from prefect._internal.stream_execution import stream_subtask_execution_context
from prefect._internal.stream_routed_storage import PrefectRoutedStreamStorage
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.client.schemas.stream_routes import StreamDAGRouteContext
from prefect.client.schemas.streams import (
    ConsumerBinding,
    DeliveryToken,
    SourceAdmission,
    SourceBinding,
    StreamDelivery,
    StreamInfo,
    StreamLimits,
)
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.context import AsyncClientContext, SyncClientContext
from prefect.settings import PREFECT_CLIENT_CSRF_SUPPORT_ENABLED, temporary_settings
from prefect.streams import (
    KeyedStreamOutput,
    SourceContext,
    SourceRecord,
    SourceRunResult,
    Stream,
    StreamContext,
    StreamProtocolError,
    StreamRecord,
    StreamRunResult,
    StreamSourceConfig,
    StreamTaskConfig,
)
from tests.test_stream_dag_bootstrap import owned_storage


def route_context(keyed: bool = True) -> StreamDAGRouteContext:
    return StreamDAGRouteContext(
        graph_revision_id=uuid4(),
        graph_digest="a" * 64,
        routing_digest="b" * 64,
        producer_subtask_id=uuid4(),
        operator_uid="source.documents",
        output_port_uid="documents",
        subtask_index=0,
        has_keyed_outgoing=keyed,
    )


def bindings(
    keyed: bool = True,
) -> tuple[
    PrefectRoutedStreamStorage, SourceBinding, ConsumerBinding, StreamRecord[Any]
]:
    storage = PrefectRoutedStreamStorage(
        uuid4(),
        "http://prefect.test/api",
        route_context=route_context(keyed),
        poll_interval=0.001,
    )
    source = SourceBinding(
        flow_run_id=storage.flow_run_id,
        stream_id=uuid4(),
        task_run_id=uuid4(),
        owner_id=uuid4(),
        attempt=2,
        owner_epoch=3,
    )
    consumer = ConsumerBinding(
        flow_run_id=storage.flow_run_id,
        consumer_id=source.task_run_id,
        input_stream_id=uuid4(),
        output_stream_id=source.stream_id,
        owner_id=source.owner_id,
        attempt=source.attempt,
        owner_epoch=source.owner_epoch,
    )
    token = DeliveryToken(
        **consumer.model_dump(), record_id=uuid4(), sequence=200, delivery_id=uuid4()
    )
    return storage, source, consumer, StreamRecord(200, {"input": True}, {}, token)


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    calls: list[tuple[str, dict[str, Any]]] = []

    async def dispatch(
        self: PrefectRoutedStreamStorage, method: str, **kwargs: Any
    ) -> None:
        calls.append((method, copy.deepcopy(kwargs)))

    monkeypatch.setattr(PrefectRoutedStreamStorage, "_dispatch", dispatch)
    return calls


@pytest.mark.parametrize("source", [False, True])
@pytest.mark.parametrize(
    "key,wire",
    [
        (None, {"kind": "N"}),
        (False, {"kind": "B", "value": False}),
        (-(1 << 63), {"kind": "I", "value": "-9223372036854775808"}),
        ("é", {"kind": "S", "value": "é"}),
        (b"\x00\xff", {"kind": "Y", "value": "AP8="}),
    ],
)
async def test_key_is_separate_from_business_payload_and_source_position(
    source: bool, key: Any, wire: dict[str, Any], captured: Any
):
    storage, source_binding, consumer, record = bindings()
    value = {"key": "business field", "ref": "object://documents/200"}
    wrapped = KeyedStreamOutput(value, key)
    if source:
        await storage.append_source(source_binding, 200, wrapped)
    else:
        await storage.complete(consumer, record, "YIELD", wrapped, {"count": 200})
    method, sent = captured[0]
    assert method == (
        "append_stream_dag_source" if source else "complete_stream_dag_consumer"
    )
    assert sent["value"] == value
    assert sent["output_key"] == wire
    assert sent["context"] == storage.route_context.model_dump(mode="json")
    assert sent["position" if source else "sequence"] == 200
    assert not {"targets", "producer_sequence", "references"} & sent.keys()


@pytest.mark.parametrize("source", [False, True])
async def test_plain_null_payload_has_no_output_key(source: bool, captured: Any):
    storage, source_binding, consumer, record = bindings(False)
    if source:
        await storage.append_source(source_binding, 1, None)
    else:
        await storage.complete(consumer, record, "YIELD", None, {})
    sent = captured[0][1]
    assert sent["value"] is None and sent["output_key"] is None
    if not source:
        assert sent["kind"] == "YIELD"


@pytest.mark.parametrize("keyed", [False, True])
async def test_return_is_state_only_even_with_keyed_outgoing(
    keyed: bool, captured: Any
):
    storage, _, consumer, record = bindings(keyed)
    await storage.complete(consumer, record, "RETURN", None, {"count": 200})
    sent = captured[0][1]
    assert sent["kind"] == "RETURN"
    assert sent["value"] is None and sent["output_key"] is None
    assert sent["managed_state"] == {"count": 200}


@pytest.mark.parametrize("source", [False, True])
@pytest.mark.parametrize("keyed", [False, True])
async def test_wrapper_presence_must_match_sealed_keyed_edges(
    source: bool, keyed: bool, captured: Any
):
    storage, source_binding, consumer, record = bindings(keyed)
    value = {"value": 1} if keyed else KeyedStreamOutput({"value": 1}, None)
    with pytest.raises(StreamProtocolError, match="keyed|KeyedStreamOutput"):
        if source:
            await storage.append_source(source_binding, 1, value)
        else:
            await storage.complete(consumer, record, "YIELD", value, {})
    assert captured == []


@pytest.mark.parametrize("value", [KeyedStreamOutput(None, None), {}, 1])
async def test_return_cannot_smuggle_a_payload_or_key(value: Any, captured: Any):
    storage, _, consumer, record = bindings()
    with pytest.raises(StreamProtocolError, match="RETURN"):
        await storage.complete(consumer, record, "RETURN", value, {})
    assert captured == []


@pytest.mark.parametrize("source", [False, True])
@pytest.mark.parametrize(
    "value", [{"bad": (1, 2)}, {1: "bad"}, b"bad", float("nan"), "\ud800"]
)
async def test_invalid_json_refuses_before_transport(
    source: bool, value: Any, captured: Any
):
    storage, source_binding, consumer, record = bindings(False)
    with pytest.raises(StreamProtocolError, match="finite JSON"):
        if source:
            await storage.append_source(source_binding, 1, value)
        else:
            await storage.complete(consumer, record, "YIELD", value, {})
    assert captured == []


async def test_cyclic_value_and_invalid_state_refuse_before_transport(captured: Any):
    storage, source_binding, consumer, record = bindings(False)
    cyclic: list[Any] = []
    cyclic.append(cyclic)
    with pytest.raises(StreamProtocolError):
        await storage.append_source(source_binding, 1, cyclic)
    with pytest.raises(StreamProtocolError):
        await storage.complete(consumer, record, "RETURN", None, {"bad": (1, 2)})
    assert captured == []


@pytest.mark.parametrize("source", [False, True])
async def test_mutated_wrapper_key_is_revalidated_once_before_transport(
    source: bool, captured: Any
):
    storage, source_binding, consumer, record = bindings()
    value = KeyedStreamOutput({}, None)
    object.__setattr__(value, "key", 1.5)
    with pytest.raises(StreamProtocolError, match="tagged-key-v1"):
        if source:
            await storage.append_source(source_binding, 1, value)
        else:
            await storage.complete(consumer, record, "YIELD", value, {})
    assert captured == []


@pytest.mark.parametrize("source", [False, True])
@pytest.mark.parametrize("keyed", [False, True])
async def test_custom_wrapper_subclass_cannot_execute_field_callbacks(
    source: bool, keyed: bool, captured: Any
):
    entered: list[str] = []

    class CallbackOutput(KeyedStreamOutput[Any]):
        @property
        def key(self):
            entered.append("key")
            raise AssertionError("Custom wrapper key callback executed")

        @property
        def value(self):
            entered.append("value")
            raise AssertionError("Custom wrapper value callback executed")

    wrapper = object.__new__(CallbackOutput)
    storage, source_binding, consumer, record = bindings(keyed)
    with pytest.raises(StreamProtocolError, match="exact KeyedStreamOutput"):
        if source:
            await storage.append_source(source_binding, 1, wrapper)
        else:
            await storage.complete(consumer, record, "YIELD", wrapper, {})
    assert entered == [] and captured == []


def test_routing_context_is_private_detached_and_client_safe():
    context = route_context()
    storage = PrefectRoutedStreamStorage(
        uuid4(), "http://prefect.test/api", route_context=context
    )
    before = storage.route_context.model_dump()
    object.__setattr__(context, "operator_uid", "changed")
    projection = storage.route_context
    object.__setattr__(projection, "has_keyed_outgoing", False)
    assert storage.route_context.model_dump() == before
    with pytest.raises(FrozenInstanceError):
        storage.flow_run_id = uuid4()
    assert "source.documents" not in repr(storage)


@pytest.mark.parametrize("changed", ["flow", "token_binding", "record_sequence"])
async def test_wrong_delivery_or_flow_refuses_without_transport(
    changed: str, captured: Any
):
    storage, _, consumer, record = bindings()
    if changed == "flow":
        consumer = consumer.model_copy(update={"flow_run_id": uuid4()})
    elif changed == "token_binding":
        record = StreamRecord(
            record.sequence,
            record.value,
            {},
            record.token.model_copy(update={"owner_id": uuid4()}),
        )
    else:
        record = StreamRecord(201, record.value, {}, record.token)
    with pytest.raises(StreamProtocolError):
        await storage.complete(
            consumer, record, "YIELD", KeyedStreamOutput(None, None), {}
        )
    assert captured == []


@pytest.mark.parametrize("source", [False, True])
async def test_unknown_ack_replays_frozen_intent_despite_concurrent_mutation(
    source: bool, monkeypatch: pytest.MonkeyPatch
):
    storage, source_binding, consumer, record = bindings()
    original_context = route_context()
    storage = PrefectRoutedStreamStorage(
        storage.flow_run_id,
        storage.api_url,
        route_context=original_context,
        poll_interval=0.001,
    )
    value = {"ids": [1]}
    state = {"count": 200}
    wrapper = KeyedStreamOutput(value, b"original")
    started, release = asyncio.Event(), asyncio.Event()
    calls: list[tuple[str, dict[str, Any]]] = []

    async def dispatch(self: Any, method: str, **arguments: Any) -> None:
        calls.append((method, copy.deepcopy(arguments)))
        if len(calls) == 1:
            started.set()
            await release.wait()
            # A client callback must not mutate the retained retry intent.
            arguments["value"]["ids"].append(999)
            arguments["context"]["operator_uid"] = "mutated-client"
            arguments["output_key"]["value"] = "different"
            raise httpx.ReadTimeout("accepted terminal acknowledgement lost")

    monkeypatch.setattr(PrefectRoutedStreamStorage, "_dispatch", dispatch)
    coroutine = (
        storage.append_source(source_binding, 200, wrapper)
        if source
        else storage.complete(consumer, record, "YIELD", wrapper, state)
    )
    running = asyncio.create_task(coroutine)
    await started.wait()
    value["ids"].append(2)
    state["count"] = 999
    object.__setattr__(wrapper, "key", None)
    object.__setattr__(original_context, "operator_uid", "mutated-caller")
    object.__setattr__(record.token, "delivery_id", uuid4())
    release.set()
    await running
    assert len(calls) == 2 and calls[0] == calls[1]
    bodies = [
        json.dumps(arguments, sort_keys=True, default=str, separators=(",", ":"))
        for _, arguments in calls
    ]
    assert bodies[0] == bodies[1]
    assert calls[0][1]["value"] == {"ids": [1]}
    assert calls[0][1]["output_key"] == {"kind": "Y", "value": "b3JpZ2luYWw="}
    if not source:
        assert calls[0][1]["managed_state"] == {"count": 200}


async def test_typed_execution_fence_is_not_retried(monkeypatch: pytest.MonkeyPatch):
    storage, source_binding, _, _ = bindings()
    calls = 0

    async def dispatch(self: Any, method: str, **arguments: Any) -> None:
        nonlocal calls
        calls += 1
        raise StreamRequestError(
            "EXECUTION_PROFILE_NOT_READY", "not ready", retryable=False
        )

    monkeypatch.setattr(PrefectRoutedStreamStorage, "_dispatch", dispatch)
    with pytest.raises(StreamRequestError) as refused:
        await storage.append_source(source_binding, 1, KeyedStreamOutput(None, None))
    assert refused.value.code == "EXECUTION_PROFILE_NOT_READY"
    assert calls == 1


async def test_unknown_ack_stops_at_the_owned_deadline(monkeypatch: pytest.MonkeyPatch):
    storage, source_binding, _, _ = bindings()
    clock = iter([0.0, 30.0])
    monkeypatch.setattr(
        stream_routed_storage, "time", SimpleNamespace(monotonic=lambda: next(clock))
    )
    calls = 0

    async def dispatch(self: Any, method: str, **arguments: Any) -> None:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("acknowledgement remains unknown")

    monkeypatch.setattr(PrefectRoutedStreamStorage, "_dispatch", dispatch)
    with pytest.raises(httpx.ReadTimeout):
        await storage.append_source(source_binding, 1, KeyedStreamOutput(None, None))
    assert calls == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "source,returns", [(True, False), (False, False), (False, True)]
)
async def test_existing_sync_async_terminal_loops_use_the_injected_dag_adapter(
    asynchronous: bool, source: bool, returns: bool, monkeypatch: pytest.MonkeyPatch
):
    storage, bootstrap, execution, source_binding, consumer = owned_storage(
        source=source
    )
    record = StreamRecord(
        200,
        {"input": True},
        {},
        DeliveryToken(
            **consumer.model_dump(),
            record_id=uuid4(),
            sequence=200,
            delivery_id=uuid4(),
        ),
    )
    emitted: list[tuple[str, dict[str, Any]]] = []
    reads = 0

    async def dispatch(self: Any, method: str, **kwargs: Any) -> Any:
        nonlocal reads
        if method == "open_stream_dag_source":
            return SourceAdmission(
                binding=source_binding, state="OPEN", accepted_source_position=199
            )
        if method == "open_stream_dag_consumer":
            return consumer
        if method == "read_stream_dag_consumer":
            reads += 1
            return (
                StreamDelivery(token=record.token, value=record.value, managed_state={})
                if reads == 1
                else None
            )
        if method in {"read_stream_dag_input_status", "read_stream_dag_output_status"}:
            return StreamInfo(
                id=consumer.input_stream_id
                if method == "read_stream_dag_input_status"
                else source_binding.stream_id,
                flow_run_id=storage.flow_run_id,
                protocol_version=1,
                state="COMPLETED",
                error=None,
                last_sequence=200,
                tail_sequence=200,
                producer_position=200,
                record_count=200,
                byte_count=200,
                limits=StreamLimits(),
                released=False,
            )
        emitted.append((method, copy.deepcopy(kwargs)))

    monkeypatch.setattr(PrefectRoutedStreamStorage, "_dispatch", dispatch)

    def source_sync(ctx: SourceContext):
        assert ctx.accepted_source_position == 199
        yield SourceRecord(200, KeyedStreamOutput(None, None))

    async def source_async(ctx: SourceContext):
        assert ctx.accepted_source_position == 199
        yield SourceRecord(200, KeyedStreamOutput(None, None))

    def operator_sync(ctx: StreamContext, document: Any):
        ctx.managed_state["count"] = ctx.input_sequence
        yield KeyedStreamOutput(document, None)

    async def operator_async(ctx: StreamContext, document: Any):
        ctx.managed_state["count"] = ctx.input_sequence
        yield KeyedStreamOutput(document, None)

    def operator_return_sync(ctx: StreamContext, document: Any):
        return {"filtered": ctx.input_sequence}

    async def operator_return_async(ctx: StreamContext, document: Any):
        return {"filtered": ctx.input_sequence}

    definition = task(
        (source_async if asynchronous else source_sync)
        if source
        else (
            (operator_return_async if asynchronous else operator_return_sync)
            if returns
            else (operator_async if asynchronous else operator_sync)
        ),
        stream=StreamSourceConfig() if source else StreamTaskConfig(input="document"),
    )
    output = Stream(storage, str(source_binding.stream_id))
    run = _StreamRun(
        output,
        bootstrap.task_run,
        source=source,
    )
    parameters: dict[str, Any] = {"ctx": None, "__prefect_stream_run__": run}
    if not source:
        parameters["document"] = Stream(storage, str(consumer.input_stream_id))
    engine = SimpleNamespace(
        task=definition,
        parameters=parameters,
        task_run=run.task_run,
        is_cancelled=lambda: False,
    )
    with stream_subtask_execution_context(execution):
        if asynchronous:
            result = await (
                execute_source_async(engine) if source else execute_stream_async(engine)
            )
        else:
            result = await asyncio.to_thread(
                execute_source_sync if source else execute_stream_sync, engine
            )
    assert isinstance(result, SourceRunResult if source else StreamRunResult)
    if not source:
        assert result.consumer_id == str(consumer.consumer_id)
        assert consumer.consumer_id != bootstrap.task_run.id
    method = "append_stream_dag_source" if source else "complete_stream_dag_consumer"
    terminals = [arguments for name, arguments in emitted if name == method]
    assert len(terminals) == 1
    assert terminals[0]["position" if source else "sequence"] == 200
    assert terminals[0]["output_key"] == (None if returns else {"kind": "N"})
    if source:
        assert terminals[0]["value"] is None
    else:
        assert terminals[0]["kind"] == ("RETURN" if returns else "YIELD")
        assert terminals[0]["managed_state"] == (
            {"filtered": 200} if returns else {"count": 200}
        )
        if returns:
            assert terminals[0]["value"] is None


def test_dag_storage_overrides_lifecycle_without_enabling_legacy_stream_start():
    assert (
        PrefectRoutedStreamStorage.open_source is not PrefectStreamStorage.open_source
    )
    assert PrefectRoutedStreamStorage.read is not PrefectStreamStorage.read
    assert (
        PrefectRoutedStreamStorage.close_source is not PrefectStreamStorage.close_source
    )
    assert not hasattr(Stream, "start")


@asynccontextmanager
async def wire_client(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
    asynchronous: bool,
) -> AsyncGenerator[None, None]:
    """Real paired SDK clients; only the external HTTP transport is substituted."""
    settings = {"transport": httpx.MockTransport(handler)}
    with temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}):
        if asynchronous:
            async with PrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                monkeypatch.setattr(
                    AsyncClientContext,
                    "get",
                    classmethod(lambda cls: SimpleNamespace(client=client)),
                )
                monkeypatch.setattr(
                    SyncClientContext, "get", classmethod(lambda cls: None)
                )
                yield
        else:
            with SyncPrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                monkeypatch.setattr(
                    AsyncClientContext, "get", classmethod(lambda cls: None)
                )
                monkeypatch.setattr(
                    SyncClientContext,
                    "get",
                    classmethod(lambda cls: SimpleNamespace(client=client)),
                )
                yield


def route_outcome(body: dict[str, Any], source: bool) -> dict[str, Any]:
    """A response fixture, not a route planner/store implementation."""
    return {
        "receipt_id": str(uuid4()),
        "graph_revision_id": body["context"]["graph_revision_id"],
        "routing_digest": body["context"]["routing_digest"],
        "semantic_digest": "c" * 64,
        "outcome_digest": "d" * 64,
        "producer_stream_id": body["binding"][
            "stream_id" if source else "output_stream_id"
        ],
        "producer_position": body["position"] if source else body["token"]["sequence"],
        "producer_role": "SOURCE" if source else "OPERATOR",
        "kind": "YIELD",
        "input_stream_id": None if source else body["binding"]["input_stream_id"],
        "input_record_id": None if source else body["token"]["record_id"],
        "input_payload_record_id": None if source else str(uuid4()),
        "input_payload_value_digest": None if source else "e" * 64,
        "input_sequence": None if source else body["token"]["sequence"],
        "consumer_id": None if source else body["binding"]["consumer_id"],
        "producer_sequence": 1,
        "output_ordinal": 0,
        "payload_record_id": str(uuid4()),
        "payload_value_digest": "f" * 64,
        "payload_size_bytes": 4,
        "references": [],
    }


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("source", [False, True])
@pytest.mark.parametrize("wrong_receipt", [False, True])
async def test_adapter_uses_paired_sdk_paths_and_validates_receipt_identity(
    asynchronous: bool,
    source: bool,
    wrong_receipt: bool,
    monkeypatch: pytest.MonkeyPatch,
):
    storage, source_binding, consumer, record = bindings()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/admin/version":
            return httpx.Response(200, json="3.0.0")
        requests.append(request)
        outcome = route_outcome(json.loads(request.content), source)
        if wrong_receipt:
            outcome["producer_stream_id"] = str(uuid4())
        return httpx.Response(200, json=outcome)

    async with wire_client(monkeypatch, handler, asynchronous):
        operation = (
            storage.append_source(source_binding, 200, KeyedStreamOutput(None, None))
            if source
            else storage.complete(
                consumer, record, "YIELD", KeyedStreamOutput(None, None), {}
            )
        )
        if wrong_receipt:
            with pytest.raises(StreamRequestError) as refused:
                await operation
            assert refused.value.code == "IDENTITY_CONFLICT"
        else:
            await operation
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert request.url.path == (
        "/api/streams/v2/dags/sources/append"
        if source
        else "/api/streams/v2/dags/consumers/complete"
    )
    body = json.loads(request.content)
    assert body["value"] is None and body["output_key"] == {"kind": "N"}
    assert body["position"] == 200 if source else body["token"]["sequence"] == 200
    assert body["context"] == storage.route_context.model_dump(mode="json")
    assert "targets" not in body and "producer_sequence" not in body


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_legacy_sdk_terminal_json_bytes_have_no_new_route_fields(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch
):
    routed, source, consumer, record = bindings(False)
    legacy = PrefectStreamStorage(routed.flow_run_id, routed.api_url)
    requests: list[httpx.Request] = []
    value = {"ref": "object://documents/200"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/admin/version":
            return httpx.Response(200, json="3.0.0")
        requests.append(request)
        if request.url.path.endswith("/sources/append"):
            return httpx.Response(
                200,
                json={
                    "id": str(uuid4()),
                    "stream_id": str(source.stream_id),
                    "sequence": 200,
                    "producer_position": 200,
                    "value": value,
                    "value_digest": "f" * 64,
                    "size_bytes": 1,
                },
            )
        return httpx.Response(200, json={"input_sequence": 200, "output_sequence": 1})

    async with wire_client(monkeypatch, handler, asynchronous):
        await legacy.append_source(source, 200, value)
        await legacy.complete(consumer, record, "YIELD", None, {"count": 200})
    expected = [
        {"binding": source.model_dump(mode="json"), "position": 200, "value": value},
        {
            "binding": consumer.model_dump(mode="json"),
            "token": record.token.model_dump(mode="json"),
            "kind": "YIELD",
            "value": None,
            "managed_state": {"count": 200},
        },
    ]
    assert [str(request.url.path) for request in requests] == [
        "/api/streams/v1/sources/append",
        "/api/streams/v1/consumers/complete",
    ]
    for request, body in zip(requests, expected):
        literal = json.dumps(
            body, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode()
        assert request.content == literal


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("stream_role", ["input", "output"])
@pytest.mark.parametrize("forged_response", [False, True])
async def test_own_status_preserves_uuid_through_json_freeze_and_sdk_ack(
    asynchronous: bool,
    stream_role: str,
    forged_response: bool,
    monkeypatch: pytest.MonkeyPatch,
):
    storage, bootstrap, execution, _, consumer = owned_storage()
    stream_id = (
        consumer.input_stream_id
        if stream_role == "input"
        else consumer.output_stream_id
    )
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        result = StreamInfo(
            id=uuid4() if forged_response else stream_id,
            flow_run_id=bootstrap.flow_run_id,
            protocol_version=1,
            state="COMPLETED",
            error=None,
            last_sequence=8,
            tail_sequence=8,
            producer_position=8,
            record_count=8,
            byte_count=128,
            limits=StreamLimits(),
            released=False,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    with stream_subtask_execution_context(execution):
        async with wire_client(monkeypatch, handler, asynchronous):
            if forged_response:
                with pytest.raises(StreamRequestError) as refused:
                    await storage.status(str(stream_id))
                assert refused.value.code == "IDENTITY_CONFLICT"
            else:
                result = await storage.status(str(stream_id))
                assert result.state == "COMPLETED" and result.error is None
    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert (
        requests[0].url.path
        == f"/api/streams/v2/dags/subtasks/{execution.subtask_id}/{stream_role}/status"
    )
