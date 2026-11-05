"""DAG routed wire contracts; these component tests do not enable execution."""

import copy
import inspect
import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, get_args
from uuid import UUID

import httpx
import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from prefect._internal.stream_execution import (
    SubtaskExecutionContext,
    stream_subtask_execution_context,
)
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect.client.orchestration._streams.client import (
    StreamAsyncClient,
    StreamClient,
    StreamRequestError,
)
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import stream_routes as client_schemas
from prefect.server.schemas import stream_routes as server_schemas
from prefect.settings import (
    PREFECT_CLIENT_CSRF_SUPPORT_ENABLED,
    PREFECT_CLIENT_MAX_RETRIES,
    PREFECT_CLIENT_RETRY_JITTER_FACTOR,
    temporary_settings,
)

MODELS = tuple(
    name
    for name, value in vars(server_schemas).items()
    if isinstance(value, type)
    and issubclass(value, BaseModel)
    and value.__module__ == server_schemas.__name__
    and not name.startswith("_")
)
METHODS = ("append_stream_dag_source", "complete_stream_dag_consumer")
KEYS = (
    {"kind": "N"},
    {"kind": "B", "value": False},
    {"kind": "I", "value": "-9223372036854775808"},
    {"kind": "S", "value": "документ"},
    {"kind": "Y", "value": "AA=="},
)


def _arguments(name: str) -> dict[str, Any]:
    common: dict[str, Any] = {
        "context": {
            "graph_revision_id": str(UUID(int=5)),
            "graph_digest": "1" * 64,
            "routing_digest": "2" * 64,
            "producer_subtask_id": str(UUID(int=6)),
            "operator_uid": "A" if name == METHODS[0] else "B",
            "output_port_uid": "out",
            "subtask_index": 0,
            "has_keyed_outgoing": True,
        },
        "flow_run_id": UUID(int=1),
        "owner_id": UUID(int=4),
        "attempt": 1,
        "owner_epoch": 1,
        "value": {"refs": ["object://documents/200"]},
        "output_key": {"kind": "I", "value": "200"},
    }
    if name == METHODS[0]:
        return common | {
            "stream_id": UUID(int=2),
            "task_run_id": UUID(int=3),
            "position": 200,
        }
    return common | {
        "consumer_id": UUID(int=3),
        "input_stream_id": UUID(int=8),
        "output_stream_id": UUID(int=2),
        "record_id": UUID(int=9),
        "sequence": 200,
        "delivery_id": UUID(int=10),
        "kind": "YIELD",
        "managed_state": {"counts": [200]},
    }


def _body(name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
    args = copy.deepcopy(_arguments(name) if arguments is None else arguments)
    binding_fields = (
        (
            "flow_run_id",
            "stream_id",
            "task_run_id",
            "owner_id",
            "attempt",
            "owner_epoch",
        )
        if name == METHODS[0]
        else (
            "flow_run_id",
            "consumer_id",
            "input_stream_id",
            "output_stream_id",
            "owner_id",
            "attempt",
            "owner_epoch",
        )
    )
    binding = {field: args.pop(field) for field in binding_fields}
    if name == METHODS[0]:
        model = server_schemas.StreamDAGSourceAppendRequest.model_validate(
            args | {"binding": binding}
        )
    else:
        token = binding | {
            field: args.pop(field) for field in ("record_id", "sequence", "delivery_id")
        }
        model = server_schemas.StreamDAGConsumerCompleteRequest.model_validate(
            args | {"binding": binding, "token": token}
        )
    return model.model_dump(mode="json")


def _outcome(name: str, *, returned: bool = False, count: int = 2) -> dict[str, Any]:
    source = name == METHODS[0]
    payload = str(UUID(int=20))
    return {
        "receipt_id": str(UUID(int=21)),
        "graph_revision_id": str(UUID(int=5)),
        "routing_digest": "2" * 64,
        "semantic_digest": "b" * 64,
        "outcome_digest": "c" * 64,
        "producer_stream_id": str(UUID(int=2)),
        "producer_position": 200,
        "producer_role": "SOURCE" if source else "OPERATOR",
        "kind": "RETURN" if returned else "YIELD",
        "input_stream_id": None if source else str(UUID(int=8)),
        "input_record_id": None if source else str(UUID(int=9)),
        "input_payload_record_id": None if source else str(UUID(int=11)),
        "input_payload_value_digest": None if source else "d" * 64,
        "input_sequence": None if source else 200,
        "consumer_id": None if source else str(UUID(int=3)),
        "producer_sequence": None if returned else 170,
        "output_ordinal": None if returned else 0,
        "payload_record_id": None if returned else payload,
        "payload_value_digest": None if returned else "e" * 64,
        "payload_size_bytes": None if returned else 39,
        "references": []
        if returned
        else [
            {
                "edge_id": str(UUID(int=100 + index)),
                "edge_partition_id": str(UUID(int=200 + index)),
                "stream_id": str(UUID(int=300 + index)),
                "sequence": 1,
                "record_id": str(UUID(int=400 + index)),
                "payload_record_id": payload,
            }
            for index in range(count)
        ],
    }


async def _invoke(
    name: str,
    arguments: dict[str, Any],
    synchronous: bool,
    handler: Callable[[httpx.Request], httpx.Response],
) -> Any:
    with temporary_settings(
        {
            PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False,
            PREFECT_CLIENT_MAX_RETRIES: 1,
            PREFECT_CLIENT_RETRY_JITTER_FACTOR: 0,
        }
    ):
        options = {"transport": httpx.MockTransport(handler)}
        if synchronous:
            with SyncPrefectClient(
                "http://protocol.test/api", httpx_settings=options
            ) as client:
                return getattr(client, name)(**arguments)
        async with PrefectClient(
            "http://protocol.test/api", httpx_settings=options
        ) as client:
            return await getattr(client, name)(**arguments)


@contextmanager
def _peer(body: dict[str, Any]) -> Iterator[SimpleNamespace]:
    requests: list[tuple[str, str, bytes]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def do_POST(self) -> None:
            requests.append(
                (
                    self.command,
                    self.path,
                    self.rfile.read(int(self.headers["Content-Length"])),
                )
            )
            encoded = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield SimpleNamespace(
            url=f"http://127.0.0.1:{server.server_port}/api", requests=requests
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        assert not thread.is_alive()


@pytest.mark.parametrize("name", MODELS)
def test_routed_model_json_schema_matches_server_exactly(name: str):
    assert (
        getattr(client_schemas, name).model_json_schema()
        == getattr(server_schemas, name).model_json_schema()
    )


def test_output_key_union_schema_matches_exact_tags_and_closed_fields():
    actual = TypeAdapter(client_schemas.StreamDAGOutputKey).json_schema()
    assert actual == TypeAdapter(server_schemas.StreamDAGOutputKey).json_schema()
    assert set(actual["discriminator"]["mapping"]) == {"N", "B", "I", "S", "Y"}
    assert all(
        model["additionalProperties"] is False for model in actual["$defs"].values()
    )


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("name", METHODS)
def test_actual_server_request_dump_validates_in_independent_client(
    name: str, key: dict[str, Any]
):
    arguments = _arguments(name) | {"output_key": key}
    body = _body(name, arguments)
    model = (
        client_schemas.StreamDAGSourceAppendRequest
        if name == METHODS[0]
        else client_schemas.StreamDAGConsumerCompleteRequest
    )
    assert model.model_validate(body).model_dump(mode="json") == body


@pytest.mark.parametrize(
    "name,returned", [(METHODS[0], False), (METHODS[1], False), (METHODS[1], True)]
)
def test_actual_server_outcome_dump_validates_in_independent_client(
    name: str, returned: bool
):
    body = server_schemas.StreamDAGRouteOutcome.model_validate(
        _outcome(name, returned=returned)
    ).model_dump(mode="json")
    assert (
        client_schemas.StreamDAGRouteOutcome.model_validate(body).model_dump(
            mode="json"
        )
        == body
    )


@pytest.mark.parametrize(
    "key",
    [
        {"kind": "N", "value": None},
        {"kind": "B", "value": 1},
        {"kind": "I", "value": "-0"},
        {"kind": "I", "value": "+1"},
        {"kind": "I", "value": "01"},
        {"kind": "I", "value": "9223372036854775808"},
        {"kind": "S", "value": "\ud800"},
        {"kind": "Y", "value": "AB=="},
        {"kind": "Y", "value": "AA"},
        {"kind": "Y", "value": "AA==\n"},
        {"kind": "Y", "value": "__8="},
        {"kind": "S", "value": "x", "extra": 1},
    ],
)
def test_malformed_key_wire_is_refused_without_coercion(key: dict[str, Any]):
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGSourceAppendRequest.model_validate(
            _body(METHODS[0]) | {"output_key": key}
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("subtask_index", True),
        ("subtask_index", 0.0),
        ("subtask_index", 16),
        ("has_keyed_outgoing", 1),
        ("operator_uid", "wrong:uid"),
        ("routing_digest", "bad"),
        ("target_stream_id", str(UUID(int=99))),
    ],
)
def test_context_refuses_malformed_or_client_chosen_targets(field: str, value: Any):
    body = _body(METHODS[0])
    body["context"][field] = value
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGSourceAppendRequest.model_validate(body)


def test_unkeyed_absence_differs_from_explicit_null_key():
    body = _body(METHODS[0])
    body.pop("output_key")
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGSourceAppendRequest.model_validate(body)
    body["context"]["has_keyed_outgoing"] = False
    assert (
        client_schemas.StreamDAGSourceAppendRequest.model_validate(body).output_key
        is None
    )
    assert (
        client_schemas.StreamDAGSourceAppendRequest.model_validate(
            body | {"output_key": None}
        ).output_key
        is None
    )
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGSourceAppendRequest.model_validate(
            body | {"output_key": {"kind": "N"}}
        )


def test_return_is_state_only_and_yield_none_is_a_payload():
    body = _body(METHODS[1]) | {"kind": "RETURN", "value": None, "output_key": None}
    assert (
        client_schemas.StreamDAGConsumerCompleteRequest.model_validate(body).kind
        == "RETURN"
    )
    for changed in ({"value": {}}, {"output_key": {"kind": "N"}}):
        with pytest.raises(ValidationError):
            client_schemas.StreamDAGConsumerCompleteRequest.model_validate(
                body | changed
            )
    yielded = body | {"kind": "YIELD", "output_key": {"kind": "N"}}
    assert (
        client_schemas.StreamDAGConsumerCompleteRequest.model_validate(yielded).value
        is None
    )


@pytest.mark.parametrize(
    "field",
    [
        "flow_run_id",
        "consumer_id",
        "input_stream_id",
        "output_stream_id",
        "owner_id",
        "attempt",
        "owner_epoch",
    ],
)
def test_operator_token_must_match_current_binding(field: str):
    body = _body(METHODS[1])
    body["token"][field] = (
        2 if field in {"attempt", "owner_epoch"} else str(UUID(int=999))
    )
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGConsumerCompleteRequest.model_validate(body)


@pytest.mark.parametrize(
    "field",
    [
        "input_stream_id",
        "input_record_id",
        "input_payload_record_id",
        "input_payload_value_digest",
        "input_sequence",
        "consumer_id",
    ],
)
@pytest.mark.parametrize("name", METHODS)
def test_outcome_has_exact_original_input_identity(name: str, field: str):
    body = _outcome(name)
    body[field] = None if name == METHODS[1] else _outcome(METHODS[1])[field]
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGRouteOutcome.model_validate(body)


@pytest.mark.parametrize(
    "mutation",
    [
        "order",
        "record",
        "partition",
        "stream",
        "payload",
        "ordinal",
        "input_sequence",
        "extra",
        "return",
    ],
)
def test_malformed_route_outcome_fails_closed(mutation: str):
    body = _outcome(METHODS[1])
    if mutation == "order":
        body["references"].reverse()
    elif mutation in {"record", "partition", "stream"}:
        field = {
            "record": "record_id",
            "partition": "edge_partition_id",
            "stream": "stream_id",
        }[mutation]
        body["references"][1][field] = body["references"][0][field]
    elif mutation == "payload":
        body["references"][0]["payload_record_id"] = str(UUID(int=99))
    elif mutation == "ordinal":
        body["output_ordinal"] = False
    elif mutation == "input_sequence":
        body["input_sequence"] = 201
    elif mutation == "extra":
        body["targets"] = []
    else:
        body["kind"] = "RETURN"
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGRouteOutcome.model_validate(body)


def test_route_receipt_reference_bound_is_exactly_128():
    assert (
        len(
            client_schemas.StreamDAGRouteOutcome.model_validate(
                _outcome(METHODS[0], count=128)
            ).references
        )
        == 128
    )
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGRouteOutcome.model_validate(
            _outcome(METHODS[0], count=129)
        )


@pytest.mark.parametrize("name", METHODS)
def test_new_methods_are_paired_and_only_new_routes_are_added(name: str):
    assert inspect.signature(getattr(StreamClient, name)) == inspect.signature(
        getattr(StreamAsyncClient, name)
    )
    assert "/streams/v2/dags/sources/append" in get_args(ServerRoutes)
    assert "/streams/v2/dags/consumers/complete" in get_args(ServerRoutes)


@pytest.mark.parametrize(
    "name,returned",
    [(METHODS[0], False), (METHODS[1], False), (METHODS[1], True)],
)
@pytest.mark.parametrize("synchronous", [True, False])
async def test_paired_methods_send_exact_real_tcp_wire(
    name: str, returned: bool, synchronous: bool
):
    arguments = _arguments(name)
    if returned:
        arguments.update(kind="RETURN", value=None, output_key=None)
    outcome = _outcome(name, returned=returned)
    with (
        _peer(outcome) as peer,
        temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}),
    ):
        if synchronous:
            with SyncPrefectClient(peer.url) as client:
                result = getattr(client, name)(**arguments)
        else:
            async with PrefectClient(peer.url) as client:
                result = await getattr(client, name)(**arguments)
    assert result.model_dump(mode="json") == outcome
    assert len(peer.requests) == 1
    method, path, raw = peer.requests[0]
    assert method == "POST"
    assert path == (
        "/api/streams/v2/dags/sources/append"
        if name == METHODS[0]
        else "/api/streams/v2/dags/consumers/complete"
    )
    assert json.loads(raw) == _body(name, arguments)


@pytest.mark.parametrize("name", METHODS)
@pytest.mark.parametrize("synchronous", [True, False])
async def test_unknown_ack_retries_identical_frozen_body_after_caller_mutation(
    name: str, synchronous: bool
):
    arguments = _arguments(name)
    expected = _body(name)
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        if len(requests) == 1:
            arguments["context"]["routing_digest"] = "f" * 64
            arguments["value"]["refs"].append("object://mutated")
            arguments["output_key"]["value"] = "201"
            if name == METHODS[1]:
                arguments["managed_state"]["counts"].append(999)
            raise httpx.ReadTimeout("Accepted reply was lost", request=request)
        return httpx.Response(200, json=_outcome(name))

    result = await _invoke(name, arguments, synchronous, handler)
    assert len(requests) == 2 and requests[0] == requests[1]
    assert json.loads(requests[0]) == expected
    assert result.receipt_id == UUID(int=21)
    assert result.producer_position == 200


@pytest.mark.parametrize(
    "field",
    ["graph_revision_id", "routing_digest", "producer_stream_id", "producer_position"],
)
@pytest.mark.parametrize("name", METHODS)
@pytest.mark.parametrize("synchronous", [True, False])
async def test_valid_looking_wrong_receipt_is_typed_identity_conflict(
    name: str, field: str, synchronous: bool
):
    body = _outcome(name)
    body[field] = (
        201
        if field == "producer_position"
        else "f" * 64
        if field == "routing_digest"
        else str(UUID(int=999))
    )
    if field == "producer_position" and name == METHODS[1]:
        body["input_sequence"] = 201
    with pytest.raises(StreamRequestError) as caught:
        await _invoke(
            name,
            _arguments(name),
            synchronous,
            lambda request: httpx.Response(200, json=body),
        )
    assert caught.value.code == "IDENTITY_CONFLICT"


@pytest.mark.parametrize("field", ["input_stream_id", "input_record_id", "consumer_id"])
@pytest.mark.parametrize("synchronous", [True, False])
async def test_operator_ack_must_bind_supplied_input(field: str, synchronous: bool):
    body = _outcome(METHODS[1]) | {field: str(UUID(int=999))}
    with pytest.raises(StreamRequestError) as caught:
        await _invoke(
            METHODS[1],
            _arguments(METHODS[1]),
            synchronous,
            lambda request: httpx.Response(200, json=body),
        )
    assert caught.value.code == "IDENTITY_CONFLICT"


@pytest.mark.parametrize("name", METHODS)
@pytest.mark.parametrize("synchronous", [True, False])
async def test_new_route_execution_fence_is_typed(name: str, synchronous: bool):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "detail": {
                    "code": "EXECUTION_PROFILE_NOT_READY",
                    "message": "Route execution is unavailable",
                    "retryable": False,
                }
            },
        )

    with pytest.raises(StreamRequestError) as caught:
        await _invoke(name, _arguments(name), synchronous, handler)
    assert caught.value.code == "EXECUTION_PROFILE_NOT_READY"
    assert caught.value.status_code == 409 and caught.value.retryable is False


@pytest.mark.parametrize("name", METHODS)
@pytest.mark.parametrize("synchronous", [True, False])
async def test_existing_child_scope_does_not_admit_new_routed_paths(
    name: str, synchronous: bool
):
    requests: list[httpx.Request] = []
    context = SubtaskExecutionContext(
        flow_run_id=UUID(int=1),
        graph_revision_id=UUID(int=5),
        assignment_epoch=1,
        subtask_id=UUID(int=6),
        grant_id=UUID(int=12),
        physical_epoch=1,
        holder_id=UUID(int=4),
        scope_digest="a" * 64,
        manifest_digest="b" * 64,
        task_run_id=UUID(int=3),
        input_stream_id=UUID(int=8),
        output_stream_id=UUID(int=2),
        child_process_id=100,
        process_start_ticks=200,
        api_url="http://protocol.test/api",
        capability="c" * 64,
        event_private_key=b"s" * 32,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_outcome(name))

    with (
        stream_subtask_execution_context(context),
        pytest.raises(ValueError, match="outside|scope|route"),
    ):
        await _invoke(name, _arguments(name), synchronous, handler)
    assert requests == []


# Literal byte fixtures deliberately do not derive their expected shape from a DTO.
SOURCE_GOLDEN = (
    b'{"binding":{"flow_run_id":"00000000-0000-0000-0000-000000000001",'
    b'"stream_id":"00000000-0000-0000-0000-000000000002",'
    b'"task_run_id":"00000000-0000-0000-0000-000000000003",'
    b'"owner_id":"00000000-0000-0000-0000-000000000004","attempt":1,"owner_epoch":1},'
    b'"position":200,"value":{"refs":["object://documents/200"]}}'
)
_CONSUMER_BINDING_GOLDEN = (
    b'"flow_run_id":"00000000-0000-0000-0000-000000000001",'
    b'"consumer_id":"00000000-0000-0000-0000-000000000003",'
    b'"input_stream_id":"00000000-0000-0000-0000-000000000008",'
    b'"output_stream_id":"00000000-0000-0000-0000-000000000002",'
    b'"owner_id":"00000000-0000-0000-0000-000000000004","attempt":1,"owner_epoch":1'
)
CONSUMER_GOLDEN = (
    b'{"binding":{'
    + _CONSUMER_BINDING_GOLDEN
    + b'},"token":{'
    + _CONSUMER_BINDING_GOLDEN
    + b',"record_id":"00000000-0000-0000-0000-000000000009","sequence":200,'
    b'"delivery_id":"00000000-0000-0000-0000-00000000000a"},'
    b'"kind":"YIELD","value":{"refs":["object://documents/200"]},"managed_state":{"counts":[200]}}'
)


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize(
    "source,returned", [(True, False), (False, False), (False, True)]
)
async def test_legacy_source_and_completion_request_bytes_remain_exact(
    source: bool, returned: bool, synchronous: bool
):
    arguments = _arguments(METHODS[0] if source else METHODS[1])
    arguments.pop("context")
    arguments.pop("output_key")
    if returned:
        arguments.update(kind="RETURN", value=None)
    expected = SOURCE_GOLDEN if source else CONSUMER_GOLDEN
    if returned:
        expected = expected.replace(
            b'"kind":"YIELD","value":{"refs":["object://documents/200"]}',
            b'"kind":"RETURN","value":null',
        )
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        assert request.url.path == (
            "/api/streams/v1/sources/append"
            if source
            else "/api/streams/v1/consumers/complete"
        )
        body = (
            {
                "id": str(UUID(int=20)),
                "stream_id": str(UUID(int=2)),
                "sequence": 200,
                "producer_position": 200,
                "value": arguments["value"],
                "value_digest": "e" * 64,
                "size_bytes": 39,
            }
            if source
            else {"input_sequence": 200, "output_sequence": None if returned else 170}
        )
        return httpx.Response(200, json=body)

    await _invoke(
        "append_stream_source" if source else "complete_stream_input",
        arguments,
        synchronous,
        handler,
    )
    assert requests == [expected]
