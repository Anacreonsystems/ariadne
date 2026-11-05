"""Independent DAG lifecycle schemas and immutable lifecycle HTTP receipts."""

import copy
import inspect
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, get_args
from uuid import uuid4

import httpx
import pytest
from pydantic import BaseModel, ValidationError
from tests.client.test_stream_dags import _metadata

from prefect.client.orchestration._streams.client import (
    StreamAsyncClient,
    StreamClient,
    StreamRequestError,
)
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import stream_dag_executions as c
from prefect.server.schemas import stream_dag_executions as s
from prefect.server.schemas.core import TaskRun as ServerTaskRun
from prefect.server.schemas.states import State, StateType

STAMP = "2026-11-05T12:00:00Z"
MODELS = tuple(
    name
    for name, cls in vars(s).items()
    if isinstance(cls, type)
    and issubclass(cls, BaseModel)
    and cls.__module__ == s.__name__
)
PARENT_METHODS = {
    "start_stream_dag": ("start", "START"),
    "activate_stream_dag": ("activate", "ACTIVATE"),
    "abort_stream_dag_preview": ("abort-preview", "ABORT_PREVIEW"),
    "cancel_stream_dag": ("cancel", "CANCEL"),
    "fail_stream_dag": ("fail", "FAIL"),
}


def native_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Keep ordinary TaskRun wire compatibility; compare all native definitions."""
    definitions = schema.get("$defs", {})
    standard: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if "$ref" in value:
                name = value["$ref"].rsplit("/", 1)[-1]
                if name not in standard:
                    standard.add(name)
                    visit(definitions[name])
            for key, item in value.items():
                if key != "$ref":
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    if "TaskRun" in definitions:
        standard.add("TaskRun")
        visit(definitions["TaskRun"])
    return {
        **schema,
        "$defs": {
            key: item for key, item in definitions.items() if key not in standard
        },
    }


def execution() -> dict[str, Any]:
    return {
        "graph_revision_id": uuid4(),
        "flow_run_id": uuid4(),
        "graph_digest": "1" * 64,
        "routing_digest": "2" * 64,
        "phase": "PREVIEW",
        "terminal_state": None,
        "cleanup_complete": False,
        "counts": {
            "total": 4,
            "registered": 0,
            "bootstrap_ready": 0,
            "terminal_tasks": 0,
            "stopped": 0,
            "physical_slots_remaining": 0,
            "tags_remaining": 0,
        },
        "control": None,
        "activation_operation_id": None,
        "readiness_set_digest": None,
        "terminal_operation_id": None,
        "terminal_outcome_digest": None,
    }


def principal() -> dict[str, Any]:
    return dict(
        work_pool_id=uuid4(),
        host_id=uuid4(),
        boot_id=uuid4(),
        launcher_process_id=17,
        launcher_start_ticks=19,
    )


def bootstrap() -> dict[str, Any]:
    request, plan = _metadata()
    spec = request["operators"][1]
    row = copy.deepcopy(plan["subtasks"][1])
    grant_id, worker_id = uuid4(), uuid4()
    row.update(
        graph_revision_id=plan["id"],
        assignment_epoch=1,
        current_grant_id=grant_id,
        physical_epoch=1,
        assigned_worker_id=worker_id,
        physical_slot_reserved=True,
        physical_slot_released=False,
    )
    p = principal()
    grant = dict(
        id=grant_id,
        subtask_id=row["id"],
        graph_revision_id=plan["id"],
        assignment_epoch=1,
        physical_epoch=1,
        holder_id=uuid4(),
        launcher_binding_kind="ASSIGNED_WORKER",
        worker_id=worker_id,
        work_pool_id=p["work_pool_id"],
        launcher_process_id=p["launcher_process_id"],
        launcher_start_ticks=p["launcher_start_ticks"],
        event_key_id="3" * 64,
        scope_digest="4" * 64,
        state="REGISTERED",
        host_id=p["host_id"],
        boot_id=p["boot_id"],
        child_process_id=101,
        process_start_ticks=103,
        created_at=STAMP,
        last_renewed_at=STAMP,
    )
    task = ServerTaskRun(
        id=row["manifest"]["task_run_id"],
        flow_run_id=plan["flow_run_id"],
        name="B-0",
        task_key=spec["task_key"],
        task_version=spec["task_version"],
        dynamic_key=row["manifest"]["dynamic_key"],
        run_count=2,
        flow_run_run_count=1,
        empirical_policy=spec["task_policy"],
        tags=["etl"],
        labels={"operator": "B", "partition": 0, "native": True},
        state=State(
            type=StateType.RUNNING,
            name="Retrying",
            timestamp=STAMP,
            state_details={
                "task_run_id": row["manifest"]["task_run_id"],
                "flow_run_id": plan["flow_run_id"],
            },
        ),
        state_type=StateType.RUNNING,
        state_name="Retrying",
    ).model_dump(mode="json")
    edge = plan["edges"][0]
    part = edge["partitions"][0]
    return s.StreamDAGSubtaskBootstrap.model_validate(
        dict(
            flow_run_id=plan["flow_run_id"],
            flow_run_run_count=1,
            flow_generation=1,
            flow_holder_id=uuid4(),
            assignment=row,
            grant=grant,
            context=dict(
                graph_revision_id=plan["id"],
                graph_digest=plan["graph_digest"],
                routing_digest="5" * 64,
                producer_subtask_id=row["id"],
                operator_uid="B",
                output_port_uid="output",
                subtask_index=0,
                has_keyed_outgoing=False,
            ),
            operator_parallelism=1,
            input=dict(
                subscriber_id=part["subscriber_id"],
                edge_id=edge["id"],
                edge_partition_id=part["id"],
                input_port_uid="input",
                input_stream_id=row["input_stream_id"],
                schema_id="json-value-v1",
                schema_digest="d93f54187397ae62d4e1b98c56edab2a67a9d10422b028dfdea87ba6616a849c",
            ),
            output=dict(port_uid="output", stream_id=row["output_stream_id"]),
            entrypoint=spec["entrypoint"],
            task_key=spec["task_key"],
            task_version=spec["task_version"],
            artifact_digest=spec["artifact_digest"],
            context_key=spec["context_key"],
            task_policy=spec["task_policy"],
            effective_tags=["etl"],
            parameters={"limit": 3, "nested": [None, True]},
            input_key=spec["input_key"],
            task_run=task,
            reserved_tag_names=["tag:etl"],
            event_public_key="6" * 64,
            launch_attempt_id=uuid4(),
            worker_generation_digest="7" * 64,
            bootstrap_digest="8" * 64,
        )
    ).model_dump(mode="json")


@pytest.mark.parametrize("name", MODELS)
def test_exact_native_schema_parity(name: str):
    client, server = (
        getattr(c, name).model_json_schema(),
        getattr(s, name).model_json_schema(),
    )
    assert (
        (native_schema(client) == native_schema(server))
        if name == "StreamDAGSubtaskBootstrap"
        else client == server
    )


def test_bootstrap_standard_task_run_actual_wire_roundtrip():
    wire = bootstrap()
    parsed = c.StreamDAGSubtaskBootstrap.model_validate(wire)
    dumped = parsed.model_dump(mode="json")
    # The existing client State adds nullable audit fields absent from server
    # State. Preserve the ordinary DTO and prove every server field survives.
    for field in ("created", "updated"):
        assert dumped["task_run"]["state"].pop(field) is None
    assert dumped == wire
    assert parsed.task_run.run_count == 2
    assert parsed.task_run.state.name == "Retrying"
    assert parsed.task_run.labels == {"operator": "B", "partition": 0, "native": True}
    assert parsed.parameters == {"limit": 3, "nested": [None, True]}
    assert parsed.task_run.dynamic_key == wire["assignment"]["manifest"]["dynamic_key"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("flow_run_run_count", 9),
        ("task_key", "other"),
        ("reserved_tag_names", []),
        ("effective_tags", []),
        ("unknown", "secret"),
    ],
)
def test_malformed_native_bootstrap_fails_closed(field: str, value: object):
    wire = bootstrap()
    for schema in (c, s):
        with pytest.raises(ValidationError):
            schema.StreamDAGSubtaskBootstrap.model_validate(wire | {field: value})


@pytest.mark.parametrize("method", PARENT_METHODS)
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_lifecycle_methods_frozen_request_and_ack(
    method: str, asynchronous: bool
):
    path, kind = PARENT_METHODS[method]
    current = execution()
    op = uuid4()
    if kind == "ACTIVATE":
        current.update(
            phase="ACTIVE", activation_operation_id=op, readiness_set_digest="c" * 64
        )
    receipt = dict(
        operation_id=op,
        graph_revision_id=current["graph_revision_id"],
        kind=kind,
        request_digest="a" * 64,
        outcome_digest="b" * 64,
        accepted_at=STAMP,
        effective_control_kind=None,
        control_operation_id=None,
    )
    body = s.StreamDAGOperationResult(receipt=receipt, execution=current).model_dump(
        mode="json"
    )
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert (
            request.url.path
            == f"/streams/v2/dags/{current['graph_revision_id']}/{path}"
        )
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=body)

    kwargs = dict(
        graph_revision_id=current["graph_revision_id"],
        operation_id=op,
        graph_digest=current["graph_digest"],
        routing_digest=current["routing_digest"],
    )
    if method == "cancel_stream_dag":
        kwargs["reason"] = "user requested cancellation"
    if method == "fail_stream_dag":
        kwargs["failure_kind"] = "START_TIMEOUT"
    if asynchronous:
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handler)
        ) as http:
            client = StreamAsyncClient(http)
            for _ in range(2):
                result = await getattr(client, method)(**kwargs)
    else:
        with httpx.Client(
            base_url="http://test", transport=httpx.MockTransport(handler)
        ) as http:
            client = StreamClient(http)
            for _ in range(2):
                result = getattr(client, method)(**kwargs)
    assert (
        seen[0]
        == seen[1]
        == {
            key: str(value) if key == "operation_id" else value
            for key, value in kwargs.items()
            if key != "graph_revision_id"
        }
    )
    assert result.model_dump(mode="json") == body
    assert inspect.signature(getattr(StreamClient, method)) == inspect.signature(
        getattr(StreamAsyncClient, method)
    )
    assert f"/streams/v2/dags/{{graph_id}}/{path}" in get_args(ServerRoutes)


@pytest.mark.parametrize("phase", ["STOPPING", "TERMINAL"])
def test_activate_ack_may_observe_later_progress(phase: str):
    current = execution()
    activation = uuid4()
    current.update(
        phase=phase,
        activation_operation_id=activation,
        readiness_set_digest="c" * 64,
        control=dict(operation_id=uuid4(), kind="CANCEL", state="STOPPING"),
    )
    if phase == "TERMINAL":
        current.update(
            terminal_state="CANCELLED",
            cleanup_complete=True,
            terminal_operation_id=uuid4(),
            terminal_outcome_digest="d" * 64,
        )
        current["counts"].update(terminal_tasks=4, stopped=4)
        current["control"]["state"] = "COMPLETED"
    receipt = dict(
        operation_id=activation,
        graph_revision_id=current["graph_revision_id"],
        kind="ACTIVATE",
        request_digest="a" * 64,
        outcome_digest="b" * 64,
        accepted_at=STAMP,
        effective_control_kind=None,
        control_operation_id=None,
    )
    response = s.StreamDAGOperationResult(
        receipt=receipt, execution=current
    ).model_dump(mode="json")
    with httpx.Client(
        base_url="http://test",
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=response)),
    ) as http:
        result = StreamClient(http).activate_stream_dag(
            graph_revision_id=current["graph_revision_id"],
            operation_id=activation,
            graph_digest=current["graph_digest"],
            routing_digest=current["routing_digest"],
        )
    assert result.receipt.operation_id == activation
    assert result.execution.phase == phase


def test_activate_ack_binds_the_accepted_activation_operation():
    current = execution()
    op = uuid4()
    current.update(
        phase="ACTIVE", activation_operation_id=uuid4(), readiness_set_digest="c" * 64
    )
    body = s.StreamDAGOperationResult(
        receipt=dict(
            operation_id=op,
            graph_revision_id=current["graph_revision_id"],
            kind="ACTIVATE",
            request_digest="a" * 64,
            outcome_digest="b" * 64,
            accepted_at=STAMP,
            effective_control_kind=None,
            control_operation_id=None,
        ),
        execution=current,
    ).model_dump(mode="json")
    with httpx.Client(
        base_url="http://test",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)),
    ) as http:
        with pytest.raises(StreamRequestError, match="does not match"):
            StreamClient(http).activate_stream_dag(
                graph_revision_id=current["graph_revision_id"],
                operation_id=op,
                graph_digest=current["graph_digest"],
                routing_digest=current["routing_digest"],
            )


@pytest.mark.parametrize(
    "status,code",
    [
        (401, "SUBTASK_AUTH_INVALID"),
        (403, "SUBTASK_SCOPE_DENIED"),
        (409, "TERMINAL_CONFLICT"),
        (409, "CONTROL_CONFLICT"),
        (409, "UNSUPPORTED_FLOW_RETRY"),
    ],
)
def test_lifecycle_typed_refusals(status: int, code: str):
    def refuse(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(
            status,
            request=request,
            json={"detail": {"code": code, "message": "refused", "retryable": False}},
        )
        response.raise_for_status()
        return response

    with httpx.Client(
        base_url="http://test",
        transport=httpx.MockTransport(refuse),
    ) as http:
        with pytest.raises(StreamRequestError) as exc:
            StreamClient(http).read_stream_dag_execution(graph_revision_id=uuid4())
    assert exc.value.code == code
    assert exc.value.status_code == status


def test_new_client_surface_cold_import_denies_all_server_modules(tmp_path: Path):
    source = Path(__file__).resolve().parents[2] / "src"
    script = """
import importlib.abc
import sys
class DenyServer(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "prefect.server" or fullname.startswith("prefect.server."):
            raise ImportError("server import forbidden")
sys.meta_path.insert(0, DenyServer())
from prefect.client.schemas.stream_dag_executions import StreamDAGStartRequest, StreamDAGSubtaskBootstrap
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect._internal.stream_execution import DAGSubtaskExecutionContext
from prefect._internal.stream_event_auth import canonical_dag_bootstrap_ready_witness
from prefect.streams import RunningStreamGraph
from prefect.task_engine import SyncTaskRunEngine, AsyncTaskRunEngine
assert callable(PrefectClient.start_stream_dag)
assert callable(SyncPrefectClient.prepare_worker_stream_dag_launch)
assert not any(name == "prefect.server" or name.startswith("prefect.server.") for name in sys.modules)
print("PASS_COLD_CLIENT_IMPORT_WITH_SERVER_DENIED")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTHONPATH": str(source),
            "PREFECT_SERVER_EPHEMERAL_ENABLED": "false",
        },
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "PASS_COLD_CLIENT_IMPORT_WITH_SERVER_DENIED"
