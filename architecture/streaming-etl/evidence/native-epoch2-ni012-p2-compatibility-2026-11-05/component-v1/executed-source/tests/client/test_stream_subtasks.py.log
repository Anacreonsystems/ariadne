"""Independent native wire mirrors, real HTTP encoding and physical auth bounds."""

import contextlib
import dataclasses
import inspect
import json
import pickle
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, get_args
from uuid import UUID, uuid4

import cloudpickle
import httpx
import pytest
from pydantic import BaseModel, ValidationError

from prefect._internal.stream_event_auth import (
    canonical_event_witness,
    canonical_physical_event_witness,
    sign_task_event,
    verify_task_event,
)
from prefect._internal.stream_execution import (
    StreamExecutionContext,
    SubtaskExecutionContext,
    get_stream_execution,
    get_stream_subtask_execution,
    stream_execution_context,
    stream_subtask_execution_context,
    stream_task_event_resource,
)
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect.client.orchestration._streams.client import (
    StreamAsyncClient,
    StreamClient,
    StreamRequestError,
)
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import stream_subtasks as client_schemas
from prefect.client.schemas.objects import TaskRun
from prefect.context import AsyncClientContext, SyncClientContext
from prefect.server.api.stream_subtasks import router as physical_router
from prefect.server.schemas import stream_subtasks as server_schemas
from prefect.server.schemas.core import TaskRun as ServerTaskRun
from prefect.server.schemas.states import State as ServerState
from prefect.server.schemas.states import StateType
from prefect.settings import (
    PREFECT_API_URL,
    PREFECT_CLIENT_CSRF_SUPPORT_ENABLED,
    PREFECT_CLIENT_SERVER_VERSION_CHECK_ENABLED,
    temporary_settings,
)

NATIVE_NAMES = tuple(
    name
    for name, value in vars(server_schemas).items()
    if name.startswith("Stream")
    and isinstance(value, type)
    and issubclass(value, BaseModel)
    and value.__module__ == server_schemas.__name__
)
METHODS = (
    "seal_stream_graph",
    "read_stream_graph",
    "admit_stream_region",
    "activate_stream_graph",
    "abort_stream_graph",
    "claim_stream_subtask",
    "register_stream_subtask_process",
    "renew_stream_subtask",
    "authorize_stream_subtask_replacement",
    "confirm_stream_subtask_replacement",
    "retire_verified_stream_subtask",
    "read_stream_subtask_bootstrap",
    "read_stream_subtask_task_run",
    "emit_stream_subtask_event",
    "renew_stream_subtask_lease",
    "release_stream_subtask_lease",
)
STAMP = "2026-11-04T12:00:00Z"


def _native_schema(schema: dict[str, Any]) -> dict[str, Any]:
    # Standard server/client TaskRun schemas already differ in descriptions and
    # legacy labels/state details. Keep that existing DTO; every native outer
    # field and native $def must still match exactly. Its actual wire is tested
    # through a representative ordinary server TaskRun round trip below.
    definitions = schema.get("$defs", {})
    standard = set()

    def references(value: Any) -> None:
        if isinstance(value, dict):
            if "$ref" in value:
                name = value["$ref"].rsplit("/", 1)[-1]
                if name not in standard:
                    standard.add(name)
                    references(definitions[name])
            for key, item in value.items():
                if key != "$ref":
                    references(item)
        elif isinstance(value, list):
            for item in value:
                references(item)

    if "TaskRun" in definitions:
        standard.add("TaskRun")
        references(definitions["TaskRun"])
    return {
        **schema,
        "$defs": {
            name: value for name, value in definitions.items() if name not in standard
        },
    }


@pytest.mark.parametrize("name", NATIVE_NAMES)
def test_native_subtask_schema_parity(name: str):
    client = getattr(client_schemas, name).model_json_schema()
    server = getattr(server_schemas, name).model_json_schema()
    if name == "StreamSubtaskBootstrap":
        assert _native_schema(client) == _native_schema(server)
    else:
        assert client == server


@pytest.mark.parametrize("name", METHODS)
def test_physical_client_signatures_match(name: str):
    assert inspect.signature(getattr(StreamClient, name)) == inspect.signature(
        getattr(StreamAsyncClient, name)
    )


def _wire() -> dict[str, Any]:
    (
        flow,
        graph,
        operator,
        subtask,
        grant,
        task,
        stream,
        upstream,
        holder,
        host,
        boot,
        op,
    ) = [uuid4() for _ in range(12)]
    manifest = {
        "flow_run_id": flow,
        "submission_key": "B/0",
        "manifest_digest": "1" * 64,
        "task_run_id": task,
        "stream_id": stream,
        "dynamic_key": "B/0",
        "role": "OPERATOR",
        "protocol": "yield-v2",
        "state": "ACTIVE",
        "created_at": STAMP,
    }
    assignment = {
        "id": subtask,
        "graph_revision_id": graph,
        "operator_run_id": operator,
        "operator_uid": "B",
        "subtask_index": 0,
        "assignment_epoch": 1,
        "input_stream_id": upstream,
        "manifest": manifest,
        "current_grant_id": grant,
        "physical_epoch": 1,
    }
    grant_body = {
        "id": grant,
        "subtask_id": subtask,
        "graph_revision_id": graph,
        "assignment_epoch": 1,
        "physical_epoch": 1,
        "holder_id": holder,
        "launcher_binding_kind": "FLOW_AUTHORITY_LOCAL",
        "launcher_process_id": 123,
        "launcher_start_ticks": 456,
        "event_key_id": "2" * 64,
        "scope_digest": "3" * 64,
        "state": "REGISTERED",
        "host_id": host,
        "boot_id": boot,
        "child_process_id": 345,
        "process_start_ticks": 678,
        "created_at": STAMP,
        "last_renewed_at": STAMP,
    }
    task_run = ServerTaskRun(
        id=task,
        flow_run_id=flow,
        name="B-0",
        task_key="partition-key",
        dynamic_key="B/0",
        task_version="v1",
        run_count=2,
        flow_run_run_count=1,
        empirical_policy={"retries": 3, "retry_delay": 0.25},
        tags=["etl"],
        labels={"operator": "B", "partition": 0, "native": True},
        state=ServerState(
            type=StateType.RUNNING,
            name="Retrying",
            timestamp=STAMP,
            state_details={"task_run_id": task, "flow_run_id": flow},
        ),
        state_type=StateType.RUNNING,
        state_name="Retrying",
    ).model_dump(mode="json")
    bootstrap = {
        "grant": grant_body,
        "assignment": assignment,
        "flow_run_id": flow,
        "flow_run_run_count": 1,
        "entrypoint": "flows.py:operator",
        "context_key": "ctx",
        "task_version": "v1",
        "artifact_digest": "5" * 64,
        "timeout_seconds": 30.0,
        "task_policy": task_run["empirical_policy"],
        "effective_tags": task_run["tags"],
        "parameters": {"ref": "object://batch", "split": 0},
        "input_key": "document",
        "task_run": task_run,
        "reserved_tag_names": ["tag:etl"],
    }
    plan = {
        "id": graph,
        "flow_run_id": flow,
        "profile": "partitioned-fixed-v1",
        "assignment_epoch": 1,
        "graph_digest": "4" * 64,
        "state": "ADMITTED",
        "created_at": STAMP,
        "subtasks": [assignment] * 4,
    }
    spec = {
        "operator_uid": "A",
        "role": "SOURCE",
        "task_name": "source",
        "task_key": "source-key",
        "task_version": "v1",
        "artifact_digest": "5" * 64,
        "entrypoint": "flows.py:source",
        "context_key": "ctx",
        "parameters": [{"split": 0}, {"split": 1}],
        "task_policy": {"retries": 2},
        "tags": ["etl"],
    }
    specs = [
        spec,
        {
            **spec,
            "operator_uid": "B",
            "role": "OPERATOR",
            "input_key": "document",
            "upstream_operator_uid": "A",
            "entrypoint": "flows.py:operator",
        },
    ]
    event = {
        "id": str(uuid4()),
        "event": "prefect.task-run.Running",
        "occurred": STAMP,
        "resource": {"prefect.resource.id": f"prefect.task-run.{task}"},
        "related": [],
        "payload": {},
        "follows": None,
    }
    authorization = {
        "operation_id": op,
        "subtask_id": subtask,
        "expected_grant_id": grant,
        "expected_physical_epoch": 1,
        "verification_not_before": STAMP,
        "host_id": host,
        "boot_id": boot,
        "child_process_id": 345,
        "process_start_ticks": 678,
        "state": "AUTHORIZED",
    }
    cases = {
        "seal_stream_graph": (
            {
                "flow_run_id": flow,
                "operation_id": op,
                "flow_version": "v1",
                "operators": specs,
            },
            "POST",
            "/graphs/seal",
            plan,
            server_schemas.StreamGraphSealRequest,
        ),
        "read_stream_graph": (
            {"graph_revision_id": graph},
            "GET",
            f"/graphs/{graph}",
            plan,
            None,
        ),
        "admit_stream_region": (
            {"graph_revision_id": graph, "operation_id": op, "graph_digest": "4" * 64},
            "POST",
            f"/graphs/{graph}/admit",
            plan,
            server_schemas.StreamRegionAdmissionRequest,
        ),
        "claim_stream_subtask": (
            {
                "subtask_id": subtask,
                "operation_id": op,
                "graph_digest": "4" * 64,
                "holder_id": holder,
                "capability": "6" * 64,
                "event_public_key": "7" * 64,
                "launcher_binding_kind": "FLOW_AUTHORITY_LOCAL",
                "host_id": host,
                "boot_id": boot,
                "launcher_process_id": 123,
                "launcher_start_ticks": 456,
            },
            "POST",
            f"/subtasks/{subtask}/claim",
            grant_body,
            server_schemas.StreamSubtaskClaimRequest,
        ),
        "register_stream_subtask_process": (
            {
                "subtask_id": subtask,
                "operation_id": op,
                "grant_id": grant,
                "child_process_id": 345,
                "process_start_ticks": 678,
            },
            "POST",
            f"/subtasks/{subtask}/register",
            grant_body,
            server_schemas.StreamSubtaskRegistrationRequest,
        ),
        "renew_stream_subtask": (
            {"subtask_id": subtask, "grant_id": grant},
            "POST",
            f"/subtasks/{subtask}/renew",
            grant_body,
            server_schemas.StreamSubtaskRenewRequest,
        ),
        "authorize_stream_subtask_replacement": (
            {
                "subtask_id": subtask,
                "operation_id": op,
                "expected_grant_id": grant,
                "expected_physical_epoch": 1,
                "host_id": host,
                "challenge_capability": "8" * 64,
            },
            "POST",
            f"/subtasks/{subtask}/replacements/authorize",
            authorization,
            server_schemas.StreamSubtaskReplacementAuthorizeRequest,
        ),
        "confirm_stream_subtask_replacement": (
            {
                "subtask_id": subtask,
                "operation_id": op,
                "challenge_capability": "8" * 64,
                "death_witness": {
                    "kind": "PIDFD_EXIT",
                    "observed_at": STAMP,
                    "host_id": host,
                    "boot_id": boot,
                    "child_process_id": 345,
                    "process_start_ticks": 678,
                },
                "new_holder_id": uuid4(),
                "new_capability": "9" * 64,
                "new_event_public_key": "a" * 64,
                "host_id": host,
                "boot_id": boot,
                "new_child_process_id": 567,
                "new_process_start_ticks": 890,
            },
            "POST",
            f"/subtasks/{subtask}/replacements/confirm",
            grant_body,
            server_schemas.StreamSubtaskReplacementConfirmRequest,
        ),
        "read_stream_subtask_bootstrap": (
            {"subtask_id": subtask},
            "GET",
            f"/subtasks/{subtask}/bootstrap",
            bootstrap,
            None,
        ),
        "read_stream_subtask_task_run": (
            {"subtask_id": subtask},
            "GET",
            f"/subtasks/{subtask}/task-run",
            task_run,
            None,
        ),
        "emit_stream_subtask_event": (
            {"subtask_id": subtask, "event": event},
            "POST",
            f"/subtasks/{subtask}/events",
            {
                "event_id": event["id"],
                "accepted": True,
                "task_run_id": task,
                "run_count": 2,
                "state_type": "RUNNING",
            },
            None,
        ),
        "renew_stream_subtask_lease": (
            {"subtask_id": subtask},
            "POST",
            f"/subtasks/{subtask}/lease/renew",
            None,
            None,
        ),
        "release_stream_subtask_lease": (
            {"subtask_id": subtask},
            "POST",
            f"/subtasks/{subtask}/lease/release",
            None,
            None,
        ),
    }
    cases["activate_stream_graph"] = (
        {
            "graph_revision_id": graph,
            "operation_id": op,
            "graph_digest": "4" * 64,
            "grant_ids": [uuid4() for _ in range(4)],
        },
        "POST",
        f"/graphs/{graph}/activate",
        plan,
        server_schemas.StreamGraphActivateRequest,
    )
    cases["abort_stream_graph"] = (
        {
            "graph_revision_id": graph,
            "operation_id": op,
            "graph_digest": "4" * 64,
            "error": "Failed before shared child release",
        },
        "POST",
        f"/graphs/{graph}/abort",
        {**plan, "state": "RELEASED"},
        server_schemas.StreamGraphAbortRequest,
    )
    cases["retire_verified_stream_subtask"] = (
        {
            "subtask_id": subtask,
            "operation_id": op,
            "challenge_capability": "8" * 64,
            "death_witness": {
                "kind": "PIDFD_EXIT",
                "observed_at": STAMP,
                "host_id": host,
                "boot_id": boot,
                "child_process_id": 345,
                "process_start_ticks": 678,
            },
            "error": "Verified process death exhausted its retry budget",
        },
        "POST",
        f"/subtasks/{subtask}/replacements/retire",
        {
            "operation_id": op,
            "subtask_id": subtask,
            "task_run_id": task,
            "grant_id": grant,
            "physical_epoch": 2,
            "state": "RETIRED",
            "task_state": "FAILED",
            "allocation_released": True,
            "outcome_kind": "RETRY_EXHAUSTED",
            "outcome_digest": "f" * 64,
        },
        server_schemas.StreamSubtaskVerifiedRetireRequest,
    )
    return {
        "cases": cases,
        "bootstrap": bootstrap,
        "specs": specs,
        "task_run": task_run,
    }


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize("method_name", METHODS)
async def test_physical_methods_send_exact_http_wire(
    method_name: str, synchronous: bool
):
    args, method, route, response_body, schema = _wire()["cases"][method_name]
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            204 if response_body is None else 200,
            request=request,
            json=_json(response_body) if response_body is not None else None,
        )

    settings = {"transport": httpx.MockTransport(handler)}
    with temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}):
        if synchronous:
            with SyncPrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                result = getattr(client, method_name)(**args)
        else:
            async with PrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                result = await getattr(client, method_name)(**args)
    assert len(requests) == 1
    request = requests[0]
    assert request.method == method
    assert request.url.path == "/api/streams/v2" + route
    assert not request.url.params
    if method == "POST":
        payload = {
            key: value
            for key, value in args.items()
            if key not in {"subtask_id", "graph_revision_id"}
        }
        if method_name == "emit_stream_subtask_event":
            expected = args["event"]
        elif schema:
            expected = schema.model_validate(payload).model_dump(mode="json")
        else:
            expected = {}
        assert json.loads(request.content) == _json(expected)
    else:
        assert request.content == b""
    if response_body is None:
        assert result is None
    elif method_name == "read_stream_subtask_task_run":
        assert isinstance(result, TaskRun)
        assert result.id == UUID(response_body["id"])
    else:
        assert isinstance(result, BaseModel)
        expected = (
            getattr(server_schemas, type(result).__name__)
            .model_validate(response_body)
            .model_dump(mode="json")
        )
        actual = result.model_dump(mode="json")
        if method_name == "read_stream_subtask_bootstrap":
            expected.pop("task_run")
            actual.pop("task_run")
        assert actual == expected


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize(
    "code,status",
    [
        ("STALE_SUBTASK_GRANT", 409),
        ("SUBTASK_AUTH_INVALID", 401),
        ("SUBTASK_SCOPE_DENIED", 403),
    ],
)
async def test_physical_wire_refusal_is_typed(
    synchronous: bool, code: str, status: int
):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            request=request,
            json={
                "detail": {
                    "code": code,
                    "message": "Physical attempt fenced",
                    "retryable": False,
                }
            },
        )

    settings = {"transport": httpx.MockTransport(handler)}
    with (
        temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}),
        pytest.raises(StreamRequestError) as caught,
    ):
        if synchronous:
            with SyncPrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                client.read_stream_subtask_bootstrap(subtask_id=uuid4())
        else:
            async with PrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                await client.read_stream_subtask_bootstrap(subtask_id=uuid4())
    assert caught.value.code == code
    assert caught.value.status_code == status
    assert not caught.value.retryable


def test_bootstrap_uses_the_ordinary_task_run_wire():
    fixture = _wire()
    server = server_schemas.StreamSubtaskBootstrap.model_validate(fixture["bootstrap"])
    client = client_schemas.StreamSubtaskBootstrap.model_validate(
        server.model_dump(mode="json")
    )
    for key in (
        "id",
        "flow_run_id",
        "task_key",
        "dynamic_key",
        "task_version",
        "name",
        "run_count",
        "flow_run_run_count",
        "empirical_policy",
        "tags",
        "labels",
        "state",
    ):
        actual = client.task_run.model_dump(mode="json")[key]
        expected = server.task_run.model_dump(mode="json")[key]
        if key == "state":
            # The ordinary client State carries two extra nullable ORM fields.
            assert actual.pop("created") is None
            assert actual.pop("updated") is None
        assert actual == expected
    assert client.parameters == {"ref": "object://batch", "split": 0}


@pytest.mark.parametrize(
    "field,value",
    [
        ("unexpected", True),
        ("grant", {}),
        ("assignment", {}),
        ("parameters", []),
        ("task_run", {}),
        ("reserved_tag_names", {}),
    ],
)
def test_bootstrap_rejects_extra_or_malformed_native_fields(field: str, value: Any):
    payload = _wire()["bootstrap"]
    payload[field] = value
    with pytest.raises(ValidationError):
        client_schemas.StreamSubtaskBootstrap.model_validate(payload)


@pytest.mark.parametrize("tags", [[""], ["x" * 256], ["é" * 128]])
def test_operator_refuses_invalid_tags(tags: list[str]):
    payload = _wire()["specs"][0]
    payload["tags"] = tags
    with pytest.raises(ValidationError):
        client_schemas.StreamOperatorSpec.model_validate(payload)


@pytest.mark.parametrize(
    "kind,worker,pool",
    [
        ("DEPLOYMENT", None, None),
        ("DEPLOYMENT", uuid4(), None),
        ("FLOW_AUTHORITY_LOCAL", uuid4(), uuid4()),
    ],
)
def test_claim_rejects_incomplete_or_mixed_launcher_binding(
    kind: str, worker: UUID | None, pool: UUID | None
):
    args = _wire()["cases"]["claim_stream_subtask"][0]
    args.pop("subtask_id")
    args.update(launcher_binding_kind=kind, worker_id=worker, work_pool_id=pool)
    with pytest.raises(ValidationError):
        client_schemas.StreamSubtaskClaimRequest.model_validate(args)


def _context(payload: dict[str, Any] | None = None) -> SubtaskExecutionContext:
    payload = payload or _wire()["bootstrap"]
    grant = payload["grant"]
    assignment = payload["assignment"]
    return SubtaskExecutionContext(
        flow_run_id=payload["flow_run_id"],
        graph_revision_id=grant["graph_revision_id"],
        assignment_epoch=1,
        subtask_id=grant["subtask_id"],
        grant_id=grant["id"],
        physical_epoch=1,
        holder_id=grant["holder_id"],
        scope_digest=grant["scope_digest"],
        manifest_digest=assignment["manifest"]["manifest_digest"],
        task_run_id=assignment["manifest"]["task_run_id"],
        input_stream_id=assignment["input_stream_id"],
        output_stream_id=assignment["manifest"]["stream_id"],
        child_process_id=grant["child_process_id"],
        process_start_ticks=grant["process_start_ticks"],
        api_url="http://prefect.test/api",
        capability="c" * 64,
        event_private_key=b"s" * 32,
    )


def test_physical_context_is_immutable_nonserializable_and_parent_exclusive():
    context = _context()
    assert context.capability not in repr(context)
    assert context.event_private_key.hex() not in repr(context)
    for serialize in (pickle.dumps, cloudpickle.dumps):
        with pytest.raises(TypeError, match="cannot enter serialized"):
            serialize(context)
    with pytest.raises(dataclasses.FrozenInstanceError):
        context.physical_epoch = 2
    parent = StreamExecutionContext(
        context.flow_run_id, 1, uuid4(), context.api_url, "d" * 64, b"p" * 32
    )
    with (
        stream_execution_context(parent),
        pytest.raises(ValueError, match="cannot inherit"),
    ):
        with stream_subtask_execution_context(context):
            pass
    with stream_subtask_execution_context(context):
        assert get_stream_subtask_execution() is context
        assert get_stream_execution() is None
        with pytest.raises(ValueError, match="cannot enter a parent"):
            with stream_execution_context(parent):
                pass
    assert get_stream_subtask_execution() is None


@pytest.mark.parametrize("synchronous", [True, False])
async def test_physical_http_cannot_send_inherited_auth_or_csrf(synchronous: bool):
    context = _context()
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204, request=request)

    settings = {
        "transport": httpx.MockTransport(handler),
        "auth": httpx.BasicAuth("inherited", "password"),
        "headers": {
            "Authorization": "Bearer parent-key",
            "Cookie": "parent=session",
            "X-Prefect-Stream-Execution-Capability": "d" * 64,
        },
    }
    with (
        stream_subtask_execution_context(context),
        temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: True}),
    ):
        if synchronous:
            with SyncPrefectClient(
                context.api_url,
                auth_string="parent:password",
                api_key="parent-key",
                httpx_settings=settings,
            ) as client:
                client.renew_stream_subtask_lease(subtask_id=context.subtask_id)
        else:
            async with PrefectClient(
                context.api_url,
                auth_string="parent:password",
                api_key="parent-key",
                httpx_settings=settings,
            ) as client:
                await client.renew_stream_subtask_lease(subtask_id=context.subtask_id)
    assert len(requests) == 1
    headers = requests[0].headers
    assert headers["Authorization"] == "PrefectSubtask " + context.capability
    assert headers["X-Prefect-Subtask-Grant"] == str(context.grant_id)
    assert not any(
        key in headers
        for key in (
            "Cookie",
            "Proxy-Authorization",
            "X-Prefect-Stream-Execution-Capability",
            "Prefect-Csrf-Token",
        )
    )
    assert "parent-key" not in str(headers) and "password" not in str(headers)


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize(
    "target",
    [
        "ordinary",
        "other_subtask",
        "other_partition",
        "parent",
        "wrong_method",
        "other_origin",
    ],
)
async def test_physical_http_refuses_offscope_before_transport(
    synchronous: bool, target: str
):
    context = _context()
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json={})

    method = "GET"
    url = {
        "ordinary": "/task_runs/" + str(context.task_run_id),
        "other_subtask": "/streams/v2/subtasks/" + str(uuid4()) + "/bootstrap",
        "other_partition": "/streams/v1/" + str(uuid4()),
        "parent": "/streams/v2/graphs/" + str(context.graph_revision_id),
        "wrong_method": "/streams/v2/subtasks/" + str(context.subtask_id) + "/events",
        "other_origin": "http://other.test/api/streams/v2/subtasks/"
        + str(context.subtask_id)
        + "/bootstrap",
    }[target]
    settings = {"transport": httpx.MockTransport(handler)}
    with (
        stream_subtask_execution_context(context),
        pytest.raises(ValueError, match="Physical credentials"),
    ):
        if synchronous:
            with SyncPrefectClient(context.api_url, httpx_settings=settings) as client:
                client._client.request(method, url)
        else:
            async with PrefectClient(
                context.api_url, httpx_settings=settings
            ) as client:
                await client._client.request(method, url)
    assert requests == []


def _event_arguments(context: SubtaskExecutionContext) -> dict[str, Any]:
    return {
        "event_id": uuid4(),
        "event": "prefect.task-run.Running",
        "occurred": datetime(2026, 11, 4, tzinfo=timezone.utc),
        "flow_run_id": context.flow_run_id,
        "task_run_id": context.task_run_id,
        "generation": context.physical_epoch,
        "holder_id": context.holder_id,
        "manifest_digest": context.manifest_digest,
        "run_count": 2,
        "task_run": {"id": str(context.task_run_id), "run_count": 2},
        "validated_state": {"type": "RUNNING", "name": "Running"},
        "graph_revision_id": context.graph_revision_id,
        "assignment_epoch": 1,
        "subtask_id": context.subtask_id,
        "grant_id": context.grant_id,
        "physical_epoch": 1,
        "scope_digest": context.scope_digest,
    }


def test_physical_event_signature_binds_assignment_and_state_bytes():
    context = _context()
    arguments = _event_arguments(context)
    witness = canonical_physical_event_witness(**arguments)
    signature = sign_task_event(context.event_private_key, witness)
    assert verify_task_event(context.event_public_key, witness, signature)
    decoded = json.loads(witness)
    assert decoded["schema"] == "prefect-stream-physical-task-event-v1"
    for field, value in [
        ("graph_revision_id", uuid4()),
        ("subtask_id", uuid4()),
        ("grant_id", uuid4()),
        ("scope_digest", "0" * 64),
        ("assignment_epoch", 2),
        ("task_run", {"id": str(context.task_run_id), "run_count": 3}),
    ]:
        changed = canonical_physical_event_witness(**{**arguments, field: value})
        assert not verify_task_event(context.event_public_key, changed, signature)
    standard_arguments = {
        key: value
        for key, value in arguments.items()
        if key
        not in {
            "graph_revision_id",
            "assignment_epoch",
            "subtask_id",
            "grant_id",
            "physical_epoch",
            "scope_digest",
        }
    }
    assert not verify_task_event(
        context.event_public_key,
        canonical_event_witness(**standard_arguments),
        signature,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("physical_epoch", True),
        ("physical_epoch", 65),
        ("physical_epoch", 2),
        ("assignment_epoch", True),
        ("scope_digest", "bad"),
    ],
)
def test_physical_event_rejects_noncanonical_authority(field: str, value: Any):
    with pytest.raises(ValueError):
        canonical_physical_event_witness(
            **{**_event_arguments(_context()), field: value}
        )


def test_ordinary_task_event_resource_uses_exact_physical_signature():
    context = _context()
    args = _event_arguments(context)
    task = SimpleNamespace(
        id=context.task_run_id, flow_run_id=context.flow_run_id, run_count=2
    )
    state = SimpleNamespace(
        id=args["event_id"], name="Running", timestamp=args["occurred"]
    )
    with stream_subtask_execution_context(context):
        resource = stream_task_event_resource(
            task,
            state,
            {"task_run": args["task_run"], "validated_state": args["validated_state"]},
            {"prefect.resource.id": "task"},
        )
    assert resource["prefect.stream-subtask-grant"] == str(context.grant_id)
    assert resource["prefect.stream-scope-digest"] == context.scope_digest
    assert verify_task_event(
        context.event_public_key,
        canonical_physical_event_witness(**args),
        resource["prefect.stream-event-signature"],
    )
    assert context.capability not in json.dumps(resource)


def test_physical_routes_are_in_the_public_route_union():
    routes = get_args(ServerRoutes)
    for route in (
        "/streams/v2/graphs/seal",
        "/streams/v2/graphs/{graph_id}",
        "/streams/v2/graphs/{graph_id}/admit",
        "/streams/v2/graphs/{graph_id}/activate",
        "/streams/v2/graphs/{graph_id}/abort",
        "/streams/v2/subtasks/{subtask_id}/claim",
        "/streams/v2/subtasks/{subtask_id}/register",
        "/streams/v2/subtasks/{subtask_id}/renew",
        "/streams/v2/subtasks/{subtask_id}/replacements/authorize",
        "/streams/v2/subtasks/{subtask_id}/replacements/confirm",
        "/streams/v2/subtasks/{subtask_id}/replacements/retire",
        "/streams/v2/subtasks/{subtask_id}/bootstrap",
        "/streams/v2/subtasks/{subtask_id}/task-run",
        "/streams/v2/subtasks/{subtask_id}/events",
        "/streams/v2/subtasks/{subtask_id}/lease/renew",
        "/streams/v2/subtasks/{subtask_id}/lease/release",
    ):
        assert route in routes


@pytest.mark.parametrize("tags", [["z", "a"], ["a", "a"]])
def test_operator_canonicalizes_tags_for_stable_graph_identity(tags: list[str]):
    payload = _wire()["specs"][0]
    payload["tags"] = tags
    assert client_schemas.StreamOperatorSpec.model_validate(payload).tags == sorted(
        set(tags)
    )


@pytest.mark.parametrize("synchronous", [True, False])
async def test_existing_parent_client_drops_auth_under_physical_context(
    synchronous: bool,
):
    context = _context()
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204, request=request)

    settings = {
        "transport": httpx.MockTransport(handler),
        "auth": httpx.BasicAuth("parent", "secret"),
    }
    with temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: True}):
        if synchronous:
            with SyncPrefectClient(
                context.api_url, auth_string="parent:secret", httpx_settings=settings
            ) as client:
                with stream_subtask_execution_context(context):
                    client.renew_stream_subtask_lease(subtask_id=context.subtask_id)
        else:
            async with PrefectClient(
                context.api_url, auth_string="parent:secret", httpx_settings=settings
            ) as client:
                with stream_subtask_execution_context(context):
                    await client.renew_stream_subtask_lease(
                        subtask_id=context.subtask_id
                    )
    assert len(requests) == 1
    assert (
        requests[0].headers["Authorization"] == "PrefectSubtask " + context.capability
    )
    assert "Prefect-Csrf-Token" not in requests[0].headers


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize("destination", ["same_route", "parent_route", "other_origin"])
async def test_physical_redirects_cannot_widen_authority(
    synchronous: bool, destination: str
):
    context = _context()
    route = f"/api/streams/v2/subtasks/{context.subtask_id}/lease/renew"
    target = {
        "same_route": route,
        "parent_route": f"/api/streams/v2/graphs/{context.graph_revision_id}",
        "other_origin": "http://outside.test" + route,
    }[destination]
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(307, request=request, headers={"Location": target})
        return httpx.Response(204, request=request)

    settings = {"transport": httpx.MockTransport(handler), "follow_redirects": True}
    expectation = (
        contextlib.nullcontext()
        if destination == "same_route"
        else pytest.raises(ValueError, match="Physical credentials")
    )
    with stream_subtask_execution_context(context), expectation:
        if synchronous:
            with SyncPrefectClient(context.api_url, httpx_settings=settings) as client:
                client.renew_stream_subtask_lease(subtask_id=context.subtask_id)
        else:
            async with PrefectClient(
                context.api_url, httpx_settings=settings
            ) as client:
                await client.renew_stream_subtask_lease(subtask_id=context.subtask_id)
    assert len(requests) == (2 if destination == "same_route" else 1)
    assert all(
        request.headers["Authorization"] == "PrefectSubtask " + context.capability
        for request in requests
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("physical_epoch", True),
        ("physical_epoch", 65),
        ("assignment_epoch", 2),
        ("capability", "secret"),
        ("scope_digest", "BAD"),
        ("api_url", "http://user:password@prefect.test/api"),
        ("api_url", "http://prefect.test/api?secret=1"),
    ],
)
def test_physical_context_rejects_malformed_authority(field: str, value: Any):
    with pytest.raises(ValueError):
        dataclasses.replace(_context(), **{field: value})


@pytest.mark.parametrize("synchronous", [True, False])
async def test_scoped_bootstrap_has_no_automatic_admin_or_auth_calls(synchronous: bool):
    payload = _wire()["bootstrap"]
    context = _context(payload)
    payload["grant"]["event_key_id"] = context.event_key_id
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert (
            request.url.path
            == f"/api/streams/v2/subtasks/{context.subtask_id}/bootstrap"
        )
        return httpx.Response(200, request=request, json=_json(payload))

    with (
        stream_subtask_execution_context(context),
        temporary_settings(
            {
                PREFECT_API_URL: context.api_url,
                PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: True,
                PREFECT_CLIENT_SERVER_VERSION_CHECK_ENABLED: True,
            }
        ),
    ):
        settings = {"transport": httpx.MockTransport(handler)}
        if synchronous:
            with SyncClientContext(httpx_settings=settings) as client_context:
                result = client_context.client.read_stream_subtask_bootstrap(
                    subtask_id=context.subtask_id
                )
        else:
            async with AsyncClientContext(httpx_settings=settings) as client_context:
                result = await client_context.client.read_stream_subtask_bootstrap(
                    subtask_id=context.subtask_id
                )
    assert len(requests) == 1
    assert result.grant.id == context.grant_id
    assert result.assignment.manifest.stream_id == context.output_stream_id
    assert (
        requests[0].headers["Authorization"] == "PrefectSubtask " + context.capability
    )


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize(
    "changed",
    [
        "grant",
        "holder",
        "scope",
        "task",
        "stream",
        "event_key",
        "input",
        "child_process",
        "start_ticks",
    ],
)
async def test_scoped_bootstrap_rejects_changed_identity(
    synchronous: bool, changed: str
):
    payload = _wire()["bootstrap"]
    context = _context(payload)
    payload["grant"]["event_key_id"] = context.event_key_id
    if changed == "grant":
        payload["grant"]["id"] = uuid4()
    elif changed == "holder":
        payload["grant"]["holder_id"] = uuid4()
    elif changed == "scope":
        payload["grant"]["scope_digest"] = "0" * 64
    elif changed == "task":
        payload["task_run"]["id"] = str(uuid4())
    elif changed == "stream":
        payload["assignment"]["manifest"]["stream_id"] = uuid4()
    elif changed == "event_key":
        payload["grant"]["event_key_id"] = "0" * 64
    elif changed == "child_process":
        payload["grant"]["child_process_id"] += 1
    elif changed == "start_ticks":
        payload["grant"]["process_start_ticks"] += 1
    else:
        payload["assignment"]["input_stream_id"] = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, json=_json(payload))

    settings = {"transport": httpx.MockTransport(handler)}
    with (
        stream_subtask_execution_context(context),
        pytest.raises((StreamRequestError, ValidationError)) as caught,
    ):
        if synchronous:
            with SyncPrefectClient(context.api_url, httpx_settings=settings) as client:
                client.read_stream_subtask_bootstrap(subtask_id=context.subtask_id)
        else:
            async with PrefectClient(
                context.api_url, httpx_settings=settings
            ) as client:
                await client.read_stream_subtask_bootstrap(
                    subtask_id=context.subtask_id
                )
    if isinstance(caught.value, StreamRequestError):
        assert caught.value.code == "IDENTITY_CONFLICT"
    else:
        assert changed in {"grant", "task"}


def test_private_request_validation_does_not_print_capabilities():
    payload = _wire()["cases"]["claim_stream_subtask"][0]
    payload.pop("subtask_id")
    payload["launcher_binding_kind"] = "DEPLOYMENT"
    with pytest.raises(ValidationError) as caught:
        client_schemas.StreamSubtaskClaimRequest.model_validate(payload)
    assert payload["capability"] not in str(caught.value)
    assert "input_value" not in str(caught.value)


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize(
    "route",
    [
        "sources/claim",
        "sources/open",
        "sources/append",
        "sources/finish",
        "sources/close",
        "sources/fail",
        "sources/reject",
        "consumers/open",
        "consumers/read",
        "consumers/complete",
        "consumers/finish",
        "outputs/fail",
        "outputs/reject",
    ],
)
async def test_existing_data_routes_keep_only_the_physical_capability(
    synchronous: bool, route: str
):
    context = _context()
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204, request=request)

    settings = {"transport": httpx.MockTransport(handler)}
    with stream_subtask_execution_context(context):
        if synchronous:
            with SyncPrefectClient(context.api_url, httpx_settings=settings) as client:
                client._client.post("/streams/v1/" + route, json={})
        else:
            async with PrefectClient(
                context.api_url, httpx_settings=settings
            ) as client:
                await client._client.post("/streams/v1/" + route, json={})
    assert len(requests) == 1
    assert (
        requests[0].headers["Authorization"] == "PrefectSubtask " + context.capability
    )
    assert requests[0].headers["X-Prefect-Subtask-Grant"] == str(context.grant_id)


@pytest.mark.parametrize(
    "state,pid,ticks",
    [
        ("REGISTERED", None, None),
        ("REGISTERED", 12, None),
        ("REGISTERED", 0, 12),
        ("CLAIMED", 12, 34),
        ("CLAIMED", None, 34),
        ("RELEASED", 12, None),
    ],
)
def test_grant_rejects_incomplete_or_wrong_phase_process_identity(
    state: str, pid: int | None, ticks: int | None
):
    payload = _wire()["bootstrap"]["grant"]
    payload.update(state=state, child_process_id=pid, process_start_ticks=ticks)
    with pytest.raises(ValidationError):
        client_schemas.StreamSubtaskGrant.model_validate(payload)


def _signed_event(context: SubtaskExecutionContext) -> tuple[dict[str, Any], bytes]:
    arguments = _event_arguments(context)
    task = SimpleNamespace(
        id=context.task_run_id, flow_run_id=context.flow_run_id, run_count=2
    )
    state = SimpleNamespace(
        id=arguments["event_id"], name="Running", timestamp=arguments["occurred"]
    )
    payload = {
        "task_run": arguments["task_run"],
        "validated_state": arguments["validated_state"],
    }
    with stream_subtask_execution_context(context):
        resource = stream_task_event_resource(
            task, state, payload, {"prefect.resource.id": f"prefect.task-run.{task.id}"}
        )
    return {
        "id": str(state.id),
        "occurred": state.timestamp.isoformat(),
        "event": "prefect.task-run.Running",
        "resource": resource,
        "payload": payload,
        "related": [],
        "follows": None,
    }, canonical_physical_event_witness(**arguments)


@pytest.mark.parametrize("synchronous", [True, False])
@pytest.mark.parametrize("mutation", [None, "payload", "grant", "signature", "receipt"])
async def test_signed_physical_event_http_transport(
    synchronous: bool, mutation: str | None
):
    context = _context()
    event, witness = _signed_event(context)
    if mutation == "payload":
        event["payload"]["task_run"]["run_count"] += 1
    elif mutation == "grant":
        event["resource"]["prefect.stream-subtask-grant"] = str(uuid4())
    elif mutation == "signature":
        event["resource"].pop("prefect.stream-event-signature")
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        received = json.loads(request.content)
        assert received["id"] == event["id"]
        assert received["resource"] == event["resource"]
        assert received["payload"] == event["payload"]
        assert verify_task_event(
            context.event_public_key,
            witness,
            received["resource"]["prefect.stream-event-signature"],
        )
        assert (
            request.url.path == f"/api/streams/v2/subtasks/{context.subtask_id}/events"
        )
        assert (
            request.headers["Authorization"] == "PrefectSubtask " + context.capability
        )
        return httpx.Response(
            200,
            request=request,
            json={
                "event_id": str(uuid4()) if mutation == "receipt" else event["id"],
                "accepted": True,
                "task_run_id": str(context.task_run_id),
                "run_count": 2,
                "state_type": "RUNNING",
            },
        )

    settings = {"transport": httpx.MockTransport(handler)}
    expectation = (
        contextlib.nullcontext()
        if mutation is None
        else pytest.raises(StreamRequestError)
    )
    with stream_subtask_execution_context(context), expectation:
        if synchronous:
            with SyncPrefectClient(context.api_url, httpx_settings=settings) as client:
                result = client.emit_stream_subtask_event(
                    subtask_id=context.subtask_id, event=event
                )
        else:
            async with PrefectClient(
                context.api_url, httpx_settings=settings
            ) as client:
                result = await client.emit_stream_subtask_event(
                    subtask_id=context.subtask_id, event=event
                )
        assert result.event_id == UUID(event["id"])
        assert result.accepted and result.run_count == 2
    assert len(requests) == (1 if mutation in (None, "receipt") else 0)


@pytest.mark.parametrize(
    "case,field,value",
    [
        ("seal_stream_graph", "assignment_epoch", True),
        ("seal_stream_graph", "assignment_epoch", 1.0),
        ("seal_stream_graph", "assignment_epoch", "1"),
        ("claim_stream_subtask", "expected_physical_epoch", 0.0),
        ("claim_stream_subtask", "expected_physical_epoch", "0"),
    ],
)
def test_physical_identity_counters_refuse_coercion(case: str, field: str, value: Any):
    arguments = _wire()["cases"][case][0]
    arguments.pop("subtask_id", None)
    arguments[field] = value
    model = (
        client_schemas.StreamGraphSealRequest
        if case == "seal_stream_graph"
        else client_schemas.StreamSubtaskClaimRequest
    )
    with pytest.raises(ValidationError):
        model.model_validate(arguments)


@pytest.mark.parametrize("value", [2.0, "2", True])
def test_physical_parallelism_is_an_exact_integer(value: Any):
    payload = _wire()["specs"][0]
    payload["parallelism"] = value
    with pytest.raises(ValidationError):
        client_schemas.StreamOperatorSpec.model_validate(payload)


def test_source_splits_must_be_canonically_distinct_but_operator_parameters_may_match():
    source, operator = _wire()["specs"]
    source["parameters"] = [{"x": 1, "y": 2}, {"y": 2, "x": 1}]
    with pytest.raises(ValidationError, match="distinct immutable"):
        client_schemas.StreamOperatorSpec.model_validate(source)
    operator["parameters"] = [{}, {}]
    assert client_schemas.StreamOperatorSpec.model_validate(operator).parameters == [
        {},
        {},
    ]


@pytest.mark.parametrize("error", ["", "x" * 4097, "é" * 2049])
def test_abort_error_is_bounded_in_utf8_bytes(error: str):
    with pytest.raises(ValidationError):
        client_schemas.StreamGraphAbortRequest(
            operation_id=uuid4(), graph_digest="a" * 64, error=error
        )


@pytest.mark.parametrize("count", [0, 3, 5])
def test_activation_requires_exactly_four_grants(count: int):
    with pytest.raises(ValidationError):
        client_schemas.StreamGraphActivateRequest(
            operation_id=uuid4(),
            graph_digest="a" * 64,
            grant_ids=[uuid4() for _ in range(count)],
        )


@pytest.mark.parametrize(
    "field", ["task_version", "artifact_digest", "task_policy", "effective_tags"]
)
def test_bootstrap_requires_own_frozen_task_metadata(field: str):
    payload = _wire()["bootstrap"]
    payload.pop(field)
    with pytest.raises(ValidationError):
        client_schemas.StreamSubtaskBootstrap.model_validate(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("task_version", "drift"),
        ("effective_tags", ["other"]),
        ("effective_tags", ["etl", "etl"]),
        ("task_policy", {"retries": 1}),
    ],
)
def test_bootstrap_refuses_task_configuration_drift(field: str, value: Any):
    payload = _wire()["bootstrap"]
    payload[field] = value
    with pytest.raises(ValidationError, match="frozen physical task"):
        client_schemas.StreamSubtaskBootstrap.model_validate(payload)


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True, "2"])
@pytest.mark.parametrize("model", ["StreamOperatorSpec", "StreamSubtaskBootstrap"])
def test_physical_task_timeout_is_a_strict_finite_positive_float_or_none(
    model: str, value: Any
):
    fixture = _wire()
    payload = (
        fixture["specs"][0] if model == "StreamOperatorSpec" else fixture["bootstrap"]
    )
    payload["timeout_seconds"] = value
    with pytest.raises(ValidationError):
        getattr(client_schemas, model).model_validate(payload)


def test_paired_method_routes_match_the_real_server_router():
    cases = _wire()["cases"]
    for name in METHODS:
        arguments, method, path, _, _ = cases[name]
        matches = []
        for route in physical_router.routes:
            route_path = route.path.replace(":uuid}", "}").format(
                graph_id=arguments.get("graph_revision_id"),
                subtask_id=arguments.get("subtask_id"),
            )
            if method in route.methods and route_path == "/streams/v2" + path:
                matches.append(route)
        assert len(matches) == 1, name
        assert matches[0].path.replace(":uuid}", "}") in get_args(ServerRoutes)
        if name in {"renew_stream_subtask_lease", "release_stream_subtask_lease"}:
            assert matches[0].status_code == 204


@pytest.mark.parametrize(
    "field,value",
    [
        ("retries", True),
        ("retries", 1.0),
        ("retries", "1"),
        ("retry_delay", True),
        ("retry_delay", float("inf")),
        ("retry_delay", -1),
        ("retry_jitter_factor", True),
        ("retry_jitter_factor", float("nan")),
    ],
)
def test_physical_task_policy_refuses_coercion_and_nonfinite_values(
    field: str, value: Any
):
    payload = _wire()["specs"][0]
    payload["task_policy"] = {"retries": 2, field: value}
    with pytest.raises(ValidationError):
        client_schemas.StreamOperatorSpec.model_validate(payload)


def test_context_parameter_names_cannot_shadow_user_inputs():
    source, operator = _wire()["specs"]
    source["parameters"][0]["ctx"] = None
    with pytest.raises(ValidationError):
        client_schemas.StreamOperatorSpec.model_validate(source)
    operator = _wire()["specs"][1]
    operator["input_key"] = "ctx"
    with pytest.raises(ValidationError):
        client_schemas.StreamOperatorSpec.model_validate(operator)


@pytest.mark.parametrize(
    "field,value",
    [
        ("flow_run_run_count", 2),
        ("flow_run_run_count", -1),
        ("reserved_tag_names", ["tag:other"]),
        ("reserved_tag_names", ["tag:etl", "tag:etl"]),
    ],
)
def test_bootstrap_counter_and_reserved_lease_metadata_agree(field: str, value: Any):
    payload = _wire()["bootstrap"]
    payload[field] = value
    for model in (
        client_schemas.StreamSubtaskBootstrap,
        server_schemas.StreamSubtaskBootstrap,
    ):
        with pytest.raises(ValidationError):
            model.model_validate(payload)


@pytest.mark.parametrize("parameters", [[{}, {"split": 1}], [{"split": 0}, {}]])
def test_source_splits_are_explicit_nonempty_snapshots(
    parameters: list[dict[str, Any]],
):
    payload = _wire()["specs"][0]
    payload["parameters"] = parameters
    with pytest.raises(ValidationError, match="nonempty immutable"):
        client_schemas.StreamOperatorSpec.model_validate(payload)


def test_epoch64_can_authorize_verified_cleanup_without_increment():
    payload = _wire()["cases"]["authorize_stream_subtask_replacement"][0]
    payload.pop("subtask_id")
    payload["expected_physical_epoch"] = 64
    assert (
        client_schemas.StreamSubtaskReplacementAuthorizeRequest.model_validate(
            payload
        ).expected_physical_epoch
        == 64
    )
    payload["expected_physical_epoch"] = 65
    with pytest.raises(ValidationError):
        client_schemas.StreamSubtaskReplacementAuthorizeRequest.model_validate(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("new_child_process_id", True),
        ("new_child_process_id", 0),
        ("new_process_start_ticks", 0),
        ("new_process_start_ticks", "10"),
    ],
)
def test_replacement_registration_identity_is_strict(field: str, value: Any):
    payload = _wire()["cases"]["confirm_stream_subtask_replacement"][0]
    payload.pop("subtask_id")
    payload[field] = value
    with pytest.raises(ValidationError):
        client_schemas.StreamSubtaskReplacementConfirmRequest.model_validate(payload)


@pytest.mark.parametrize("error", ["", "x" * 4097, "é" * 2049])
def test_verified_retirement_error_is_bounded_in_utf8_bytes(error: str):
    payload = _wire()["cases"]["retire_verified_stream_subtask"][0]
    payload.pop("subtask_id")
    payload["error"] = error
    with pytest.raises(ValidationError):
        client_schemas.StreamSubtaskVerifiedRetireRequest.model_validate(payload)


@pytest.mark.parametrize(
    "task_state,outcome",
    [
        ("COMPLETED", "COMPLETED_RECONCILED"),
        ("FAILED", "FAILED_RECONCILED"),
        ("FAILED", "ATTEMPT_LIMIT"),
    ],
)
def test_verified_retirement_preserves_terminal_outcome(task_state: str, outcome: str):
    payload = _wire()["cases"]["retire_verified_stream_subtask"][3]
    payload.update(task_state=task_state, outcome_kind=outcome)
    result = client_schemas.StreamSubtaskVerifiedRetirement.model_validate(payload)
    assert result.task_state == task_state
    assert result.outcome_kind == outcome
    assert result.allocation_released is True


@pytest.mark.parametrize("synchronous", [True, False])
async def test_verified_retirement_refuses_an_unrelated_receipt(synchronous: bool):
    arguments, _, _, reply, _ = _wire()["cases"]["retire_verified_stream_subtask"]
    reply["operation_id"] = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, json=_json(reply))

    settings = {"transport": httpx.MockTransport(handler)}
    with (
        temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}),
        pytest.raises(StreamRequestError, match="receipt differs"),
    ):
        if synchronous:
            with SyncPrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                client.retire_verified_stream_subtask(**arguments)
        else:
            async with PrefectClient(
                "http://prefect.test/api", httpx_settings=settings
            ) as client:
                await client.retire_verified_stream_subtask(**arguments)
