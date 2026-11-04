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

from prefect.client.orchestration import PrefectClient, SyncPrefectClient
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
    "StreamFiniteReserveRequest",
    "StreamFiniteLookupRequest",
    "StreamFiniteCompleteRequest",
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
    "StreamFinitePlan",
    "StreamFiniteManifest",
    "StreamFiniteReceipt",
    "StreamFiniteManifestWitness",
    "StreamFiniteReceiptWitness",
    "StreamFiniteRecoveryItem",
    "StreamExecutionCredentials",
    "StreamExecutionDeathWitness",
    "StreamExecutionClaimRequest",
    "StreamExecutionTakeoverRequest",
    "StreamExecutionBinding",
    "StreamManifestWitness",
    "StreamManifestSealRequest",
    "StreamManifestSealResult",
    "StreamRecoveryPrepareRequest",
    "StreamRecoveryItem",
    "StreamRecoveryResult",
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
    "enroll_native_stream_deployment",
    "register_native_stream_process",
    "read_native_stream_deployment_plan",
    "renew_native_stream_launch",
    "read_native_stream_recovery_candidates",
    "authorize_native_stream_replacement",
    "confirm_native_stream_replacement",
    "drain_native_stream",
    "cancel_native_stream",
    "retire_unstarted_native_stream_launch",
    "finalize_native_stream_cancellation",
    "read_native_stream_flow_projection",
    "retire_verified_native_stream_launch",
    "read_stream_finite_plan",
    "reserve_stream_finite_submission",
    "read_stream_finite_submission",
    "complete_stream_finite_task",
    "read_stream_finite_receipt",
    "claim_stream_execution",
    "takeover_stream_execution",
    "seal_stream_manifest",
    "prepare_stream_recovery",
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


NATIVE_LIFECYCLE_METHODS = (
    "enroll_native_stream_deployment",
    "renew_native_stream_launch",
    "read_native_stream_recovery_candidates",
    "authorize_native_stream_replacement",
    "confirm_native_stream_replacement",
    "drain_native_stream",
    "cancel_native_stream",
    "retire_unstarted_native_stream_launch",
    "finalize_native_stream_cancellation",
    "read_native_stream_flow_projection",
    "retire_verified_native_stream_launch",
)


def _native_lifecycle_wire_case(
    method_name: str,
) -> tuple[dict[str, Any], str, dict[str, Any] | list[dict[str, Any]]]:
    flow_id, operation_id, enrollment_id = uuid4(), uuid4(), uuid4()
    worker_id, pool_id, host_id = uuid4(), uuid4(), uuid4()
    timestamp = "2026-11-04T12:00:00Z"
    if method_name == "enroll_native_stream_deployment":
        holder_id, launcher_id = uuid4(), uuid4()
        return (
            {
                "flow_run_id": flow_id,
                "operation_id": operation_id,
                "declaration_digest": "e" * 64,
                "worker_id": worker_id,
                "work_pool_id": pool_id,
                "launcher_instance_id": launcher_id,
                "launcher_host_id": host_id,
                "launcher_boot_id": uuid4(),
                "launcher_process_id": 45678,
                "launcher_start_ticks": 23456,
                "holder_id": holder_id,
                "holder_capability": "b" * 64,
                "holder_event_public_key": "c" * 64,
                "supervisor_id": uuid4(),
                "supervisor_capability": "d" * 64,
            },
            "enroll",
            {
                "flow_run_id": str(flow_id),
                "deployment_id": str(uuid4()),
                "deployment_version": "v1",
                "work_pool_id": str(pool_id),
                "worker_id": str(worker_id),
                "snapshot_digest": "e" * 64,
                "generation": 1,
                "holder_id": str(holder_id),
                "operation_id": str(operation_id),
                "launcher_instance_id": str(launcher_id),
                "launch_profile": "linux-local-pidfd-v1",
                "state": "ENROLLED",
                "created_at": timestamp,
            },
        )
    if method_name == "renew_native_stream_launch":
        arguments = {
            "flow_run_id": flow_id,
            "generation": 2,
            "enrollment_operation_id": enrollment_id,
        }
        return (
            arguments,
            "launches/heartbeat",
            {
                "flow_run_id": str(flow_id),
                "generation": 2,
                "enrollment_operation_id": str(enrollment_id),
                "last_renewed_at": timestamp,
            },
        )
    if method_name == "read_native_stream_flow_projection":
        return (
            {"flow_run_id": flow_id},
            f"{flow_id}/projection",
            {
                "flow_run_id": str(flow_id),
                "flow_state": "Running",
                "observed_at": timestamp,
                "generation": 2,
                "authority_state": "ACTIVE",
                "prepared_generation": 2,
                "manifest_sealed": True,
                "source_admission_closed": False,
                "cancel_requested": False,
                "launch_state": "PROCESS_REGISTERED",
                "worker_id": str(worker_id),
                "items": [],
                "finite_items": [],
            },
        )
    if method_name == "read_native_stream_recovery_candidates":
        return (
            {"worker_id": worker_id, "work_pool_id": pool_id, "host_id": host_id},
            "replacements/candidates",
            [
                {
                    "flow_run_id": str(flow_id),
                    "deployment_id": str(uuid4()),
                    "generation": 2,
                    "work_pool_id": str(pool_id),
                    "worker_id": str(worker_id),
                    "launcher_instance_id": str(uuid4()),
                    "enrollment_operation_id": str(enrollment_id),
                    "launch_profile": "linux-local-pidfd-v1",
                    "identity_kind": "CHILD",
                    "host_id": str(host_id),
                    "boot_id": str(uuid4()),
                    "child_process_id": 34567,
                    "process_start_ticks": 12345,
                    "last_renewed_at": timestamp,
                    "verification_not_before": "2026-11-04T12:02:00Z",
                }
            ],
        )
    if method_name == "authorize_native_stream_replacement":
        return (
            {
                "flow_run_id": flow_id,
                "operation_id": operation_id,
                "expected_generation": 2,
                "enrollment_operation_id": enrollment_id,
                "worker_id": worker_id,
                "work_pool_id": pool_id,
                "host_id": host_id,
                "challenge_capability": "a" * 64,
                "identity_kind": "CHILD",
            },
            "replacements/authorize",
            {
                "flow_run_id": str(flow_id),
                "operation_id": str(operation_id),
                "expected_generation": 2,
                "worker_id": str(worker_id),
                "work_pool_id": str(pool_id),
                "host_id": str(host_id),
                "state": "AUTHORIZED",
                "identity_kind": "CHILD",
                "created_at": timestamp,
            },
        )
    if method_name == "confirm_native_stream_replacement":
        holder_id, launcher_id = uuid4(), uuid4()
        return (
            {
                "flow_run_id": flow_id,
                "operation_id": operation_id,
                "challenge_capability": "a" * 64,
                "launcher_instance_id": launcher_id,
                "launcher_host_id": host_id,
                "launcher_boot_id": uuid4(),
                "launcher_process_id": 45678,
                "launcher_start_ticks": 23456,
                "new_holder_id": holder_id,
                "new_holder_capability": "b" * 64,
                "new_holder_event_public_key": "c" * 64,
                "new_supervisor_id": uuid4(),
                "new_supervisor_capability": "d" * 64,
                "death_witness": {
                    "kind": "WAITPID",
                    "observed_at": timestamp,
                    "host_id": str(host_id),
                    "boot_id": str(uuid4()),
                    "child_process_id": 34567,
                    "process_start_ticks": 12345,
                    "exit_code": 77,
                },
            },
            "replacements/confirm-death",
            {
                "flow_run_id": str(flow_id),
                "deployment_id": str(uuid4()),
                "deployment_version": "v1",
                "work_pool_id": str(pool_id),
                "worker_id": str(worker_id),
                "snapshot_digest": "e" * 64,
                "generation": 3,
                "holder_id": str(holder_id),
                "operation_id": str(operation_id),
                "launcher_instance_id": str(launcher_id),
                "launch_profile": "linux-local-pidfd-v1",
                "state": "ENROLLED",
                "created_at": timestamp,
            },
        )
    if method_name == "finalize_native_stream_cancellation":
        return (
            {
                "flow_run_id": flow_id,
                "operation_id": operation_id,
                "expected_generation": 2,
                "enrollment_operation_id": enrollment_id,
                "death_witness": {
                    "kind": "WAITPID",
                    "observed_at": timestamp,
                    "host_id": str(host_id),
                    "boot_id": str(uuid4()),
                    "child_process_id": 34567,
                    "process_start_ticks": 12345,
                    "exit_code": -9,
                },
            },
            "control/finalize-cancel",
            {
                "flow_run_id": str(flow_id),
                "operation_id": str(operation_id),
                "generation": 2,
                "kind": "CANCEL_FINALIZED",
                "source_admission_closed": True,
                "cancel_requested": True,
                "created_at": timestamp,
            },
        )
    if method_name == "retire_verified_native_stream_launch":
        return (
            {
                "flow_run_id": flow_id,
                "operation_id": operation_id,
                "expected_generation": 2,
                "enrollment_operation_id": enrollment_id,
                "challenge_capability": "a" * 64,
                "death_witness": {
                    "kind": "WAITPID",
                    "observed_at": timestamp,
                    "host_id": str(host_id),
                    "boot_id": str(uuid4()),
                    "child_process_id": 34567,
                    "process_start_ticks": 12345,
                    "exit_code": 1,
                },
                "error": "Launch stopped before reconstruction completed",
            },
            "replacements/retire-verified",
            {
                "flow_run_id": str(flow_id),
                "operation_id": str(operation_id),
                "generation": 2,
                "kind": "VERIFIED_RETIRED",
                "source_admission_closed": True,
                "cancel_requested": True,
                "created_at": timestamp,
            },
        )
    if method_name == "retire_unstarted_native_stream_launch":
        return (
            {
                "flow_run_id": flow_id,
                "operation_id": operation_id,
                "enrollment_operation_id": enrollment_id,
                "error": "Unable to open child executable",
            },
            "launches/retire-unstarted",
            {
                "flow_run_id": str(flow_id),
                "operation_id": str(operation_id),
                "generation": 2,
                "kind": "SPAWN_FAILED",
                "source_admission_closed": True,
                "cancel_requested": True,
                "created_at": timestamp,
            },
        )
    kind = "DRAIN" if method_name == "drain_native_stream" else "CANCEL"
    return (
        {
            "flow_run_id": flow_id,
            "operation_id": operation_id,
            "expected_generation": 2,
            "reason": "Requested by the operator",
        },
        f"control/{kind.lower()}",
        {
            "flow_run_id": str(flow_id),
            "operation_id": str(operation_id),
            "generation": 2,
            "kind": kind,
            "source_admission_closed": True,
            "cancel_requested": kind == "CANCEL",
            "created_at": timestamp,
        },
    )


@pytest.mark.parametrize(
    "method_name,identity_kind",
    [
        (method_name, identity_kind)
        for method_name in NATIVE_LIFECYCLE_METHODS
        for identity_kind in (
            ["LAUNCHER", "CHILD"]
            if method_name
            in (
                "read_native_stream_recovery_candidates",
                "authorize_native_stream_replacement",
            )
            else [None]
        )
    ],
)
@pytest.mark.parametrize("synchronous", [False, True])
async def test_native_lifecycle_http_payload_and_response(
    method_name: str, identity_kind: str | None, synchronous: bool
):
    # The HTTP boundary protects UUID/datetime encoding, methods and route names,
    # not just schema construction or matching signatures.
    arguments, route, response_body = _native_lifecycle_wire_case(method_name)
    if identity_kind is not None:
        if method_name == "read_native_stream_recovery_candidates":
            assert isinstance(response_body, list)
            response_body[0]["identity_kind"] = identity_kind
        else:
            assert isinstance(response_body, dict)
            arguments["identity_kind"] = identity_kind
            response_body["identity_kind"] = identity_kind
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/admin/version":
            return httpx.Response(200, request=request, json="3.0.0")
        requests.append(request)
        return httpx.Response(200, request=request, json=response_body)

    with temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}):
        settings = {"transport": httpx.MockTransport(handler)}
        if synchronous:
            with SyncPrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                result = getattr(client, method_name)(**arguments)
        else:
            async with PrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                result = await getattr(client, method_name)(**arguments)

    assert len(requests) == 1
    request = requests[0]
    assert request.url.path == f"/api/streams/v1/deployments/{route}"
    if method_name == "read_native_stream_recovery_candidates":
        assert request.method == "GET"
        assert dict(request.url.params) == {
            key: str(value) for key, value in arguments.items()
        }
        assert [item.model_dump(mode="json") for item in result] == response_body
    elif method_name == "read_native_stream_flow_projection":
        assert request.method == "GET"
        assert not request.url.params
        assert result.model_dump(mode="json") == response_body
    else:
        assert request.method == "POST"
        expected_body = json.loads(json.dumps(arguments, default=str))
        if method_name == "enroll_native_stream_deployment":
            expected_body["launch_profile"] = "linux-local-pidfd-v1"
            expected_body["expected_generation"] = 0
        if method_name in (
            "confirm_native_stream_replacement",
            "finalize_native_stream_cancellation",
            "retire_verified_native_stream_launch",
            "read_native_stream_flow_projection",
            "retire_verified_native_stream_launch",
        ):
            expected_body["death_witness"]["observed_boot_id"] = None
        if method_name == "finalize_native_stream_cancellation":
            expected_body["challenge_capability"] = None
        assert json.loads(request.content) == expected_body
        assert result.model_dump(mode="json") == response_body


@pytest.mark.parametrize(
    "method_name,integer_key",
    [
        ("renew_native_stream_launch", "generation"),
        ("authorize_native_stream_replacement", "expected_generation"),
        ("drain_native_stream", "expected_generation"),
        ("cancel_native_stream", "expected_generation"),
        ("enroll_native_stream_deployment", "launcher_process_id"),
        ("enroll_native_stream_deployment", "launcher_start_ticks"),
        ("confirm_native_stream_replacement", "launcher_process_id"),
        ("confirm_native_stream_replacement", "launcher_start_ticks"),
    ],
)
@pytest.mark.parametrize("synchronous", [False, True])
async def test_native_lifecycle_rejects_boolean_metadata_before_transport(
    method_name: str, integer_key: str, synchronous: bool
):
    arguments, _, _ = _native_lifecycle_wire_case(method_name)
    arguments[integer_key] = True

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/admin/version":
            return httpx.Response(200, request=request, json="3.0.0")
        raise AssertionError("Invalid generation reached the HTTP transport")

    with temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}):
        settings = {"transport": httpx.MockTransport(handler)}
        if synchronous:
            with SyncPrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                with pytest.raises(ValidationError, match="valid integer"):
                    getattr(client, method_name)(**arguments)
        else:
            async with PrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                with pytest.raises(ValidationError, match="valid integer"):
                    await getattr(client, method_name)(**arguments)
