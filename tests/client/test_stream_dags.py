"""Paired DAG metadata schemas and real TCP SDK encoding; no execution claim."""

import copy
import inspect
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, get_args
from uuid import uuid4

import httpx
import pytest
from pydantic import BaseModel, ValidationError

from prefect._internal.stream_execution import (
    StreamExecutionContext,
    SubtaskExecutionContext,
    stream_execution_context,
    stream_subtask_execution_context,
)
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect.client.orchestration._streams.client import (
    StreamAsyncClient,
    StreamClient,
    StreamRequestError,
)
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import stream_dags as client_schemas
from prefect.server.schemas import stream_dags as server_schemas
from prefect.settings import PREFECT_CLIENT_CSRF_SUPPORT_ENABLED, temporary_settings

STAMP = "2026-11-05T12:00:00Z"
MODELS = tuple(
    name
    for name, value in vars(server_schemas).items()
    if isinstance(value, type)
    and issubclass(value, BaseModel)
    and value.__module__ == server_schemas.__name__
    and name != "StreamDAGAdmissionRequest"
)
METHODS = ("seal_stream_dag", "read_stream_dag")


def _metadata(shape: str = "branched") -> tuple[dict[str, Any], dict[str, Any]]:
    flow_id = uuid4()
    topology = {
        "branched": (
            (("A", 1), ("B", 1), ("C", 2)),
            (("A", "B", "forward"), ("A", "C", "broadcast")),
        ),
        "linear_minimal": ((("A", 1), ("B", 1)), (("A", "B", "forward"),)),
        "linear_rebalance": ((("A", 2), ("B", 3)), (("A", "B", "rebalance"),)),
        "chain": (
            (("A", 2), ("B", 2), ("C", 2)),
            (("A", "B", "forward"), ("B", "C", "forward")),
        ),
    }
    vertices, connections = topology[shape]
    operators = []
    for uid, count in vertices:
        operators.append(
            {
                "operator_uid": uid,
                "role": "SOURCE" if uid == "A" else "OPERATOR",
                "parallelism": count,
                "task_name": uid,
                "task_key": f"{uid}-key",
                "task_version": "v1",
                "artifact_digest": "a" * 64,
                "entrypoint": f"flow.py:{uid}",
                "context_key": "ctx",
                "parameters": [
                    {"split": index} if uid == "A" and count > 1 else {}
                    for index in range(count)
                ],
                "input_port": None if uid == "A" else {"uid": "input"},
                "output_port": {"uid": "output"},
                "input_key": None if uid == "A" else "document",
                "task_policy": {"retries": 2},
            }
        )
    edges = [
        {
            "edge_uid": f"{producer}-{uid}",
            "producer_operator_uid": producer,
            "output_port_uid": "output",
            "subscriber_uid": f"{uid}-documents",
            "consumer_operator_uid": uid,
            "input_port_uid": "input",
            "exchange": exchange,
        }
        for producer, uid, exchange in connections
    ]
    request = server_schemas.StreamDAGSealRequest(
        flow_run_id=flow_id,
        operation_id=uuid4(),
        flow_version="v1",
        deployment_id=uuid4(),
        deployment_version="v1",
        working_directory="/work",
        operators=operators,
        edges=edges,
    ).model_dump(mode="json")
    projected = []
    subtasks = []
    for operator in request["operators"]:
        uid = operator["operator_uid"]
        operator_id = uuid4()
        projected.append(
            {
                "id": operator_id,
                "operator_uid": uid,
                "role": operator["role"],
                "parallelism": operator["parallelism"],
                "input_port": None
                if uid == "A"
                else {**operator["input_port"], "id": uuid4(), "direction": "INPUT"},
                "output_port": {
                    **operator["output_port"],
                    "id": uuid4(),
                    "direction": "OUTPUT",
                },
            }
        )
        for index in range(operator["parallelism"]):
            output_id = uuid4()
            subtasks.append(
                {
                    "id": uuid4(),
                    "operator_run_id": operator_id,
                    "operator_uid": uid,
                    "subtask_index": index,
                    "input_stream_id": None if uid == "A" else uuid4(),
                    "output_stream_id": output_id,
                    "manifest": {
                        "flow_run_id": flow_id,
                        "submission_key": f"{uid}/{index}",
                        "manifest_digest": "b" * 64,
                        "task_run_id": uuid4(),
                        "stream_id": output_id,
                        "dynamic_key": f"{uid}/{index}",
                        "role": operator["role"],
                        "protocol": "source-v1" if uid == "A" else "yield-v2",
                        "state": "ACTIVE",
                        "created_at": STAMP,
                    },
                }
            )
    projected_edges = []
    for edge in request["edges"]:
        targets = [
            subtask
            for subtask in subtasks
            if subtask["operator_uid"] == edge["consumer_operator_uid"]
        ]
        projected_edges.append(
            {
                **edge,
                "id": uuid4(),
                "partitions": [
                    {
                        "id": uuid4(),
                        "partition_index": target["subtask_index"],
                        "stream_id": target["input_stream_id"],
                        "subscriber_id": uuid4(),
                        "target_subtask_id": target["id"],
                    }
                    for target in targets
                ],
            }
        )
    plan = server_schemas.StreamDAGPlan(
        id=uuid4(),
        flow_run_id=flow_id,
        operation_id=request["operation_id"],
        graph_digest="d" * 64,
        state="SEALED",
        created_at=STAMP,
        operators=projected,
        edges=projected_edges,
        subtasks=subtasks,
        leaf_operator_uids=sorted(
            {uid for uid, _ in vertices} - {producer for producer, _, _ in connections}
        ),
    ).model_dump(mode="json")
    return request, plan


@contextmanager
def _peer(body: dict[str, Any], status: int = 200) -> Iterator[SimpleNamespace]:
    requests: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def respond(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(length) if length else b""
            requests.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "headers": httpx.Headers(self.headers.items()),
                    "json": json.loads(payload) if payload else None,
                }
            )
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        do_POST = respond
        do_GET = respond

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
def test_dag_native_schemas_have_exact_parity(name: str):
    assert (
        getattr(client_schemas, name).model_json_schema()
        == getattr(server_schemas, name).model_json_schema()
    )


@pytest.mark.parametrize("name", METHODS)
def test_dag_methods_have_paired_signatures(name: str):
    assert inspect.signature(getattr(StreamClient, name)) == inspect.signature(
        getattr(StreamAsyncClient, name)
    )


@pytest.mark.parametrize(
    "shape,physical_count,leaves",
    [
        ("linear_minimal", 2, ["B"]),
        ("linear_rebalance", 5, ["B"]),
        ("chain", 6, ["C"]),
    ],
)
def test_generic_linear_metadata_keeps_exact_server_client_roundtrip(
    shape: str, physical_count: int, leaves: list[str]
):
    request, plan = _metadata(shape)
    assert (
        client_schemas.StreamDAGSealRequest.model_validate(request).model_dump(
            mode="json"
        )
        == request
    )
    result = client_schemas.StreamDAGPlan.model_validate(plan)
    assert result.model_dump(mode="json") == plan
    assert len(result.subtasks) == physical_count
    assert len({row.manifest.task_run_id for row in result.subtasks}) == physical_count
    assert result.leaf_operator_uids == leaves
    assert result.execution_ready is False


def test_source_only_topology_remains_refused():
    request, _ = _metadata("linear_minimal")
    request["operators"] = request["operators"][:1]
    request["edges"] = []
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGSealRequest.model_validate(request)


def test_forward_still_refuses_mismatched_parallelism():
    request, _ = _metadata("linear_rebalance")
    request["edges"][0]["exchange"] = "forward"
    with pytest.raises(ValidationError, match="parallelism"):
        client_schemas.StreamDAGSealRequest.model_validate(request)


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize("name", METHODS)
async def test_dag_methods_send_exact_real_tcp_wire(name: str, synchronous: bool):
    request, plan = _metadata()
    args = request if name == "seal_stream_dag" else {"graph_revision_id": plan["id"]}
    with (
        _peer(plan) as peer,
        temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}),
    ):
        authority = StreamExecutionContext(
            flow_run_id=request["flow_run_id"],
            generation=1,
            holder_id=uuid4(),
            api_url=peer.url,
            capability="c" * 64,
            event_private_key=b"s" * 32,
        )
        with stream_execution_context(authority):
            if synchronous:
                with SyncPrefectClient(peer.url) as client:
                    result = getattr(client, name)(**args)
            else:
                async with PrefectClient(peer.url) as client:
                    result = await getattr(client, name)(**args)
        assert result.model_dump(mode="json") == plan
        assert len(peer.requests) == 1
        actual = peer.requests[0]
        assert actual["headers"]["X-Prefect-Stream-Execution-Capability"] == "c" * 64
        assert "X-Prefect-Subtask-Grant" not in actual["headers"]
        assert actual["json"] == (request if name == "seal_stream_dag" else None)
        assert actual["method"] == ("POST" if name == "seal_stream_dag" else "GET")
        assert actual["path"] == (
            "/api/streams/v2/dags/seal"
            if name == "seal_stream_dag"
            else f"/api/streams/v2/dags/{plan['id']}"
        )


@pytest.mark.parametrize("synchronous", [True, False])
async def test_dag_metadata_refusal_is_typed(synchronous: bool):
    _, plan = _metadata()
    body = {
        "detail": {"code": "EXECUTION_PROFILE_NOT_READY", "message": "Metadata only"}
    }
    with (
        _peer(body, 409) as peer,
        temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}),
    ):
        with pytest.raises(StreamRequestError) as error:
            if synchronous:
                with SyncPrefectClient(peer.url) as client:
                    client.read_stream_dag(graph_revision_id=plan["id"])
            else:
                async with PrefectClient(peer.url) as client:
                    await client.read_stream_dag(graph_revision_id=plan["id"])
        assert error.value.code == "EXECUTION_PROFILE_NOT_READY"
        assert error.value.status_code == 409
        assert error.value.retryable is False


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize("name", METHODS)
async def test_child_cannot_call_parent_dag_metadata(name: str, synchronous: bool):
    request, plan = _metadata()
    args = request if name == "seal_stream_dag" else {"graph_revision_id": plan["id"]}
    with _peer(plan) as peer:
        context = SubtaskExecutionContext(
            flow_run_id=uuid4(),
            graph_revision_id=uuid4(),
            assignment_epoch=1,
            subtask_id=uuid4(),
            grant_id=uuid4(),
            physical_epoch=1,
            holder_id=uuid4(),
            scope_digest="a" * 64,
            manifest_digest="b" * 64,
            task_run_id=uuid4(),
            input_stream_id=None,
            output_stream_id=uuid4(),
            child_process_id=100,
            process_start_ticks=200,
            api_url=peer.url,
            capability="c" * 64,
            event_private_key=b"s" * 32,
        )
        with stream_subtask_execution_context(context):
            with pytest.raises(ValueError, match="outside|scope|route"):
                if synchronous:
                    with SyncPrefectClient(peer.url) as client:
                        getattr(client, name)(**args)
                else:
                    async with PrefectClient(peer.url) as client:
                        await getattr(client, name)(**args)
        assert peer.requests == []


@pytest.mark.parametrize("field,value", [("parallelism", True), ("parallelism", 1.0)])
def test_dag_physical_identity_refuses_coercion(field: str, value: Any):
    request, _ = _metadata()
    request["operators"][0][field] = value
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGSealRequest.model_validate(request)


@pytest.mark.parametrize(
    "mutation", ["subscriber", "fan_in", "schema", "ports", "source", "colon"]
)
def test_invalid_dag_topology_is_refused_locally(mutation: str):
    request, _ = _metadata()
    if mutation == "subscriber":
        request["edges"][1]["subscriber_uid"] = request["edges"][0]["subscriber_uid"]
    elif mutation == "fan_in":
        request["edges"][1]["consumer_operator_uid"] = "B"
    elif mutation == "schema":
        request["operators"][1]["input_port"]["schema_digest"] = "f" * 64
    elif mutation == "ports":
        request["operators"][1]["input_port"]["uid"] = "output"
    elif mutation == "source":
        request["operators"][0]["role"] = "OPERATOR"
    else:
        request["operators"][0]["operator_uid"] = "bad:uid"
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGSealRequest.model_validate(request)


@pytest.mark.parametrize(
    "mutation",
    ["task", "target", "leaves", "slots", "ready", "extra", "role", "protocol"],
)
def test_malformed_projected_plan_fails_closed(mutation: str):
    _, original = _metadata()
    plan = copy.deepcopy(original)
    if mutation == "task":
        plan["subtasks"][1]["manifest"]["task_run_id"] = plan["subtasks"][0][
            "manifest"
        ]["task_run_id"]
    elif mutation == "target":
        plan["edges"][0]["partitions"][0]["target_subtask_id"] = str(uuid4())
    elif mutation == "leaves":
        plan["leaf_operator_uids"] = ["B", "A"]
    elif mutation == "slots":
        plan["subtasks"][0]["physical_slot_released"] = True
    elif mutation == "ready":
        plan["execution_ready"] = True
    elif mutation == "role":
        plan["subtasks"][0]["manifest"]["role"] = "OPERATOR"
    elif mutation == "protocol":
        plan["subtasks"][0]["manifest"]["protocol"] = "yield-v2"
    else:
        plan["unexpected"] = 1
    with pytest.raises(ValidationError):
        client_schemas.StreamDAGPlan.model_validate(plan)


def test_dag_routes_are_public_but_admission_is_not_a_client_method():
    assert {
        "/streams/v2/dags/seal",
        "/streams/v2/dags/{graph_id}",
        "/streams/v2/dags/{graph_id}/start",
        "/streams/v2/dags/{graph_id}/activate",
    } <= set(get_args(ServerRoutes))
    for client in (StreamClient, StreamAsyncClient):
        assert callable(getattr(client, "start_stream_dag"))
        assert callable(getattr(client, "activate_stream_dag"))
        assert not hasattr(client, "admit_stream_dag_region")
    assert not hasattr(client_schemas, "StreamDAGAdmissionRequest")
