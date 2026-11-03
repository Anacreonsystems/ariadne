"""Client-only contract tests for native stream HTTP storage."""

import asyncio
import inspect
import json
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import cloudpickle
import httpx
import pytest
from pydantic import ValidationError

from prefect.client.orchestration import PrefectClient
from prefect.client.orchestration._streams.client import (
    StreamAsyncClient,
    StreamClient,
    StreamRequestError,
)
from prefect.client.schemas import streams as client_schemas
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.context import AsyncClientContext, SyncClientContext
from prefect.server.schemas import streams as server_schemas
from prefect.settings import PREFECT_CLIENT_CSRF_SUPPORT_ENABLED, temporary_settings
from prefect.streams import StreamProtocolError, StreamRecord

REQUEST_SCHEMA_NAMES = (
    "StreamSubmissionReserveRequest",
    "StreamSubmissionLookupRequest",
    "StreamCreateRequest",
    "StreamOutputReserveRequest",
    "StreamOutputRejectRequest",
    "StreamOutputFailRequest",
    "StreamConsumerOpenRequest",
    "StreamConsumerReadRequest",
    "StreamConsumerCompleteRequest",
    "StreamConsumerFinishRequest",
    "StreamSourceClaimRequest",
    "StreamSourceAppendRequest",
    "StreamSourceFinishRequest",
    "StreamSourceReserveRequest",
    "StreamSourceRejectRequest",
    "StreamSourceFailRequest",
    "StreamSourceOpenRequest",
    "StreamSourceCloseRequest",
    "StreamReleaseRequest",
)

PUBLIC_WIRE_SCHEMA_NAMES = (
    "StreamSubmissionManifest",
    "StreamLimits",
    "SourceBinding",
    "ConsumerBinding",
    "DeliveryToken",
    "StreamDelivery",
    "StreamEnvelope",
    "StreamInfo",
    "StreamCompletionResult",
    "SourceAdmission",
    *REQUEST_SCHEMA_NAMES,
    "StreamReleaseResult",
)

PUBLIC_STREAM_METHODS = (
    "reserve_stream_submission",
    "read_stream_submission",
    "create_stream",
    "reserve_stream_output",
    "reject_stream_output",
    "fail_stream_task_output",
    "open_stream_consumer",
    "read_stream_consumer",
    "complete_stream_input",
    "finish_stream_consumer",
    "claim_stream_source",
    "append_stream_source",
    "finish_stream_source",
    "reserve_stream_source",
    "reject_stream_source",
    "fail_stream_source",
    "open_stream_source",
    "close_stream_source",
    "release_stream_flow",
    "read_stream",
    "read_stream_prefix",
)


@pytest.mark.parametrize("name", PUBLIC_WIRE_SCHEMA_NAMES)
def test_client_schema_matches_server_wire_contract(name: str):
    client_model = getattr(client_schemas, name)
    server_model = getattr(server_schemas, name)

    assert client_model.model_json_schema() == server_model.model_json_schema()


@pytest.mark.parametrize("name", PUBLIC_STREAM_METHODS)
def test_sync_and_async_public_method_signatures_match(name: str):
    assert inspect.signature(getattr(StreamClient, name)) == inspect.signature(
        getattr(StreamAsyncClient, name)
    )


def test_backend_roundtrip_contains_only_safe_config():
    flow_run_id = uuid4()
    storage = PrefectStreamStorage(
        flow_run_id,
        "https://prefect.test/api",
        admission_timeout=7,
        poll_interval=0.2,
    )

    restored = cloudpickle.loads(cloudpickle.dumps(storage))

    assert restored == storage
    assert vars(restored) == {
        "flow_run_id": flow_run_id,
        "api_url": "https://prefect.test/api",
        "admission_timeout": 7,
        "poll_interval": 0.2,
    }
    assert not any("client" in name or "token" in name for name in vars(restored))

    with pytest.raises(StreamProtocolError, match="without credentials"):
        PrefectStreamStorage(flow_run_id, "https://user:secret@prefect.test/api")


@pytest.mark.parametrize(
    "model",
    [client_schemas.StreamCreateRequest, client_schemas.StreamConsumerOpenRequest],
)
def test_protocol_version_rejects_bool_before_transport(model: type[Any]):
    payload: dict[str, Any] = {"protocol_version": True}
    if model is client_schemas.StreamCreateRequest:
        payload.update(flow_run_id=uuid4(), stream_id=uuid4())
    else:
        payload.update(
            input_stream_id=uuid4(),
            output_stream_id=uuid4(),
            task_run_id=uuid4(),
            owner_id=uuid4(),
            attempt=1,
        )

    with pytest.raises(ValidationError, match="protocol_version must be the integer 1"):
        model.model_validate(payload)


async def test_async_client_serializes_route_and_parses_structured_refusal():
    flow_run_id, stream_id, task_run_id = uuid4(), uuid4(), uuid4()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/admin/version":
            return httpx.Response(200, request=request, json="3.0.0")
        requests.append(request)
        return httpx.Response(
            409,
            request=request,
            json={
                "detail": {
                    "code": "METADATA_NOT_READY",
                    "message": "TaskRun metadata is not recorded yet",
                    "retryable": True,
                }
            },
        )

    with temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}):
        async with PrefectClient(
            "http://prefect.test/api",
            httpx_settings={"transport": httpx.MockTransport(handler)},
        ) as client:
            with pytest.raises(StreamRequestError) as raised:
                await client.reserve_stream_output(
                    flow_run_id=flow_run_id,
                    stream_id=stream_id,
                    task_run_id=task_run_id,
                )

    assert raised.value.code == "METADATA_NOT_READY"
    assert raised.value.retryable is True
    assert raised.value.status_code == 409
    assert len(requests) == 1
    request = requests[0]
    assert request.url.path == "/api/streams/v1/outputs/reserve"
    assert request.method == "POST"
    assert request.headers["content-type"] == "application/json"
    assert json.loads(request.content) == {
        "flow_run_id": str(flow_run_id),
        "stream_id": str(stream_id),
        "protocol_version": 1,
        "limits": None,
        "task_run_id": str(task_run_id),
    }


async def test_storage_reuses_async_context_on_owning_loop(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id, expected_stream_id = uuid4(), uuid4()

    class Client:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = asyncio.get_running_loop()
        calls = 0

        async def read_stream(self, stream_id: Any) -> Any:
            self.calls += 1
            assert stream_id == expected_stream_id
            return SimpleNamespace(state="OPEN", error=None)

    client = Client()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=client)),
    )
    monkeypatch.setattr(SyncClientContext, "get", classmethod(lambda cls: None))
    storage = PrefectStreamStorage(flow_run_id, "http://prefect.test/api")

    status = await storage.status(str(expected_stream_id))

    assert status.state == "OPEN"
    assert client.calls == 1


async def test_storage_does_not_reuse_async_client_from_another_loop(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id, expected_stream_id = uuid4(), uuid4()

    class WrongLoopClient:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = object()

        async def read_stream(self, stream_id: Any) -> Any:
            raise AssertionError("client from another loop was reused")

    class SyncClient:
        api_url = httpx.URL("http://prefect.test/api")
        calls = 0

        def read_stream(self, stream_id: Any) -> Any:
            self.calls += 1
            assert stream_id == expected_stream_id
            return SimpleNamespace(state="OPEN", error=None)

    synchronous = SyncClient()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=WrongLoopClient())),
    )
    monkeypatch.setattr(
        SyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=synchronous)),
    )
    storage = PrefectStreamStorage(flow_run_id, "http://prefect.test/api")

    status = await storage.status(str(expected_stream_id))

    assert status.state == "OPEN"
    assert synchronous.calls == 1


async def test_open_retries_only_explicit_metadata_lag(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id = uuid4()
    binding = client_schemas.ConsumerBinding(
        flow_run_id=flow_run_id,
        consumer_id=uuid4(),
        input_stream_id=uuid4(),
        output_stream_id=uuid4(),
        owner_id=uuid4(),
        attempt=1,
        owner_epoch=1,
    )

    class Client:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = asyncio.get_running_loop()
        calls = 0

        async def open_stream_consumer(self, **kwargs: Any) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise StreamRequestError(
                    "METADATA_NOT_READY", "metadata lag", retryable=True
                )
            return binding

    client = Client()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=client)),
    )
    monkeypatch.setattr(SyncClientContext, "get", classmethod(lambda cls: None))
    storage = PrefectStreamStorage(
        flow_run_id,
        "http://prefect.test/api",
        admission_timeout=1,
        poll_interval=0.001,
    )

    result = await storage.open_consumer(
        str(binding.input_stream_id),
        str(binding.output_stream_id),
        str(binding.consumer_id),
        str(binding.owner_id),
        1,
    )

    assert result == binding
    assert client.calls == 2

    client.calls = 0

    async def permanent_refusal(**kwargs: Any) -> Any:
        client.calls += 1
        raise StreamRequestError("STALE_OWNER", "fenced", retryable=False)

    client.open_stream_consumer = permanent_refusal  # type: ignore[method-assign]
    with pytest.raises(StreamRequestError, match="fenced"):
        await storage.open_consumer(
            str(binding.input_stream_id),
            str(binding.output_stream_id),
            str(binding.consumer_id),
            str(binding.owner_id),
            1,
        )
    assert client.calls == 1


async def test_reservation_does_not_retry_permanent_capacity_refusal(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id = uuid4()

    class Client:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = asyncio.get_running_loop()
        calls = 0

        async def reserve_stream_output(self, **kwargs: Any) -> Any:
            self.calls += 1
            raise StreamRequestError("CAPACITY", "frozen limit reached")

    client = Client()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=client)),
    )
    monkeypatch.setattr(SyncClientContext, "get", classmethod(lambda cls: None))
    storage = PrefectStreamStorage(flow_run_id, "http://prefect.test/api")

    with pytest.raises(StreamRequestError) as raised:
        await storage.reserve_output(str(uuid4()), uuid4())

    assert raised.value.code == "CAPACITY"
    assert client.calls == 1


async def test_completion_rejects_token_from_another_binding_before_transport(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id = uuid4()
    binding = client_schemas.ConsumerBinding(
        flow_run_id=flow_run_id,
        consumer_id=uuid4(),
        input_stream_id=uuid4(),
        output_stream_id=uuid4(),
        owner_id=uuid4(),
        attempt=1,
        owner_epoch=1,
    )
    wrong_token = client_schemas.DeliveryToken(
        **binding.model_copy(update={"owner_id": uuid4()}).model_dump(),
        record_id=uuid4(),
        sequence=1,
        delivery_id=uuid4(),
    )

    class Client:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = asyncio.get_running_loop()
        calls = 0

        async def complete_stream_input(self, **kwargs: Any) -> None:
            self.calls += 1

    client = Client()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=client)),
    )
    monkeypatch.setattr(SyncClientContext, "get", classmethod(lambda cls: None))
    storage = PrefectStreamStorage(flow_run_id, "http://prefect.test/api")
    record = StreamRecord(1, "input", {}, wrong_token)

    with pytest.raises(StreamProtocolError, match="another binding"):
        await storage.complete(binding, record, "YIELD", "output", {})

    assert client.calls == 0


async def test_terminal_intent_is_frozen_before_transport_wait(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id = uuid4()
    binding = client_schemas.ConsumerBinding(
        flow_run_id=flow_run_id,
        consumer_id=uuid4(),
        input_stream_id=uuid4(),
        output_stream_id=uuid4(),
        owner_id=uuid4(),
        attempt=1,
        owner_epoch=2,
    )
    token = client_schemas.DeliveryToken(
        **binding.model_dump(),
        record_id=uuid4(),
        sequence=7,
        delivery_id=uuid4(),
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    captured: dict[str, Any] = {}

    class Client:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = asyncio.get_running_loop()

        async def complete_stream_input(self, **kwargs: Any) -> None:
            captured.update(kwargs)
            entered.set()
            await release.wait()

    client = Client()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=client)),
    )
    monkeypatch.setattr(SyncClientContext, "get", classmethod(lambda cls: None))
    storage = PrefectStreamStorage(flow_run_id, "http://prefect.test/api")
    value = {"items": [1]}
    state = {"seen": [7]}
    record = StreamRecord(7, "input", {}, token)

    completion = asyncio.create_task(
        storage.complete(binding, record, "YIELD", value, state)
    )
    await entered.wait()
    value["items"].append(2)
    state["seen"].append(8)
    release.set()
    await completion

    assert captured["value"] == {"items": [1]}
    assert captured["managed_state"] == {"seen": [7]}


async def test_source_append_intent_is_frozen_before_transport_wait(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id = uuid4()
    binding = client_schemas.SourceBinding(
        flow_run_id=flow_run_id,
        stream_id=uuid4(),
        task_run_id=uuid4(),
        owner_id=uuid4(),
        attempt=1,
        owner_epoch=2,
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    captured: dict[str, Any] = {}

    class Client:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = asyncio.get_running_loop()

        async def append_stream_source(self, **kwargs: Any) -> None:
            captured.update(kwargs)
            entered.set()
            await release.wait()

    client = Client()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=client)),
    )
    monkeypatch.setattr(SyncClientContext, "get", classmethod(lambda cls: None))
    storage = PrefectStreamStorage(flow_run_id, "http://prefect.test/api")
    value = {"reference": {"parts": [1]}}

    append = asyncio.create_task(storage.append_source(binding, 8, value))
    await entered.wait()
    value["reference"]["parts"].append(2)
    release.set()
    await append

    assert captured["position"] == 8
    assert captured["value"] == {"reference": {"parts": [1]}}


async def test_source_records_reuse_one_context_client(
    monkeypatch: pytest.MonkeyPatch,
):
    flow_run_id = uuid4()
    binding = client_schemas.SourceBinding(
        flow_run_id=flow_run_id,
        stream_id=uuid4(),
        task_run_id=uuid4(),
        owner_id=uuid4(),
        attempt=1,
        owner_epoch=1,
    )

    class Client:
        api_url = httpx.URL("http://prefect.test/api")
        _loop = asyncio.get_running_loop()

        def __init__(self) -> None:
            self.calls: list[tuple[str, int | None]] = []

        async def open_stream_source(self, **kwargs: Any) -> Any:
            self.calls.append(("open", None))
            return client_schemas.SourceAdmission(
                binding=binding,
                state="OPEN",
                accepted_source_position=0,
            )

        async def append_stream_source(self, **kwargs: Any) -> None:
            self.calls.append(("append", kwargs["position"]))

        async def close_stream_source(self, **kwargs: Any) -> None:
            self.calls.append(("close", kwargs["expected_position"]))

    client = Client()
    monkeypatch.setattr(
        AsyncClientContext,
        "get",
        classmethod(lambda cls: SimpleNamespace(client=client)),
    )
    monkeypatch.setattr(SyncClientContext, "get", classmethod(lambda cls: None))

    async def no_one_off_client() -> Any:
        raise AssertionError("source record publication opened a one-off client")

    monkeypatch.setattr("prefect.client.stream_storage.get_client", no_one_off_client)
    storage = PrefectStreamStorage(flow_run_id, "http://prefect.test/api")

    admission = await storage.open_source(
        str(binding.stream_id), binding.task_run_id, binding.owner_id, 1
    )
    await storage.append_source(admission.binding, 1, {"reference": "one"})
    await storage.append_source(admission.binding, 2, {"reference": "two"})
    await storage.close_source(admission.binding, 2)

    assert client.calls == [
        ("open", None),
        ("append", 1),
        ("append", 2),
        ("close", 2),
    ]
