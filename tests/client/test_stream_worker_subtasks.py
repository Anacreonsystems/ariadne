"""Worker placement contracts keep trusted-worker and child authority distinct."""

import inspect
import json
from typing import Any, get_args
from uuid import uuid4

import httpx
import pytest
from pydantic import BaseModel, ValidationError
from tests.client.test_stream_subtasks import _context, _json, _wire

from prefect._internal.stream_execution import stream_subtask_execution_context
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect.client.orchestration._streams.client import StreamAsyncClient, StreamClient
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import stream_subtasks as client_schemas
from prefect.server.api.stream_subtasks import router as parent_router
from prefect.server.api.stream_workers import router as worker_router
from prefect.server.schemas import stream_subtasks as server_schemas
from prefect.settings import PREFECT_CLIENT_CSRF_SUPPORT_ENABLED, temporary_settings

WORKER_METHODS = (
    "poll_worker_stream_subtasks",
    "claim_worker_stream_subtask",
    "register_worker_stream_subtask_process",
    "renew_worker_stream_subtask",
    "authorize_worker_stream_subtask_replacement",
    "confirm_worker_stream_subtask_replacement",
    "retire_verified_worker_stream_subtask",
    "abort_worker_stream_graph_startup",
    "acknowledge_worker_stream_subtask_startup_stop",
    "acknowledge_stream_subtask_startup_stop",
)


def _worker_cases() -> dict[str, tuple[dict[str, Any], str, Any, type[BaseModel]]]:
    base = _wire()
    bootstrap = base["bootstrap"]
    grant, assignment = bootstrap["grant"], bootstrap["assignment"]
    worker, pool = uuid4(), uuid4()
    subtask, graph = assignment["id"], assignment["graph_revision_id"]
    principal = {
        "work_pool_id": pool,
        "host_id": grant["host_id"],
        "boot_id": grant["boot_id"],
        "launcher_process_id": grant["launcher_process_id"],
        "launcher_start_ticks": grant["launcher_start_ticks"],
    }
    grant.update(
        launcher_binding_kind="ASSIGNED_WORKER", worker_id=worker, work_pool_id=pool
    )
    assignment["assigned_worker_id"] = worker
    plan = base["cases"]["read_stream_graph"][3]
    plan["placement_digest"] = "a" * 64
    configuration = {
        key: bootstrap[key]
        for key in (
            "entrypoint",
            "task_version",
            "artifact_digest",
            "context_key",
            "timeout_seconds",
            "task_policy",
            "effective_tags",
            "input_key",
        )
    }
    configuration.update(
        task_key=bootstrap["task_run"]["task_key"],
        role=assignment["manifest"]["role"],
        parameters=bootstrap["parameters"],
    )
    row = {
        "flow_run_id": bootstrap["flow_run_id"],
        "graph_digest": plan["graph_digest"],
        "placement_digest": plan["placement_digest"],
        "assignment": assignment,
        "phase": "WAITING",
        "abort_operation_id": None,
        "current_grant": grant,
        "working_directory": "/workspace/ariadne",
        "task_configuration": configuration,
    }

    def control(name: str) -> dict[str, Any]:
        return {
            key: value
            for key, value in base["cases"][name][0].items()
            if key != "subtask_id"
        }

    row.update(
        task_state="RUNNING",
        task_run_count=bootstrap["task_run"]["run_count"],
        task_retriable=None,
        output_state="OPEN",
        output_attempt=bootstrap["task_run"]["run_count"],
    )
    claim = dict(base["cases"]["claim_stream_subtask"][0])
    claim.pop("launcher_binding_kind")
    claim.update(worker_id=worker, work_pool_id=pool)
    stop = {
        "operation_id": uuid4(),
        "abort_operation_id": uuid4(),
        "grant_id": grant["id"],
        "expected_physical_epoch": 1,
        "unstarted": True,
    }
    stopped = {
        "operation_id": stop["operation_id"],
        "graph_revision_id": graph,
        "subtask_id": subtask,
        "grant_id": grant["id"],
        "physical_epoch": 1,
        "graph_state": "ABORTING",
    }
    prefix = f"/workers/{worker}/subtasks/{subtask}"
    common = {"worker_id": worker, "subtask_id": subtask, "principal": principal}
    return {
        "poll_worker_stream_subtasks": (
            {
                "worker_id": worker,
                "principal": principal,
                "active_subtask_ids": [subtask],
                "limit": 32,
            },
            f"/workers/{worker}/poll",
            [row],
            server_schemas.StreamWorkerPollRequest,
        ),
        "claim_worker_stream_subtask": (
            claim,
            prefix + "/claim",
            {
                **grant,
                "state": "CLAIMED",
                "child_process_id": None,
                "process_start_ticks": None,
            },
            server_schemas.StreamSubtaskClaimRequest,
        ),
        "register_worker_stream_subtask_process": (
            {**common, "registration": control("register_stream_subtask_process")},
            prefix + "/register",
            grant,
            server_schemas.StreamWorkerRegistrationRequest,
        ),
        "renew_worker_stream_subtask": (
            {**common, "grant_id": grant["id"]},
            prefix + "/renew",
            grant,
            server_schemas.StreamWorkerRenewRequest,
        ),
        "authorize_worker_stream_subtask_replacement": (
            {**common, "replacement": control("authorize_stream_subtask_replacement")},
            prefix + "/replacements/authorize",
            base["cases"]["authorize_stream_subtask_replacement"][3],
            server_schemas.StreamWorkerReplacementAuthorizeRequest,
        ),
        "confirm_worker_stream_subtask_replacement": (
            {**common, "replacement": control("confirm_stream_subtask_replacement")},
            prefix + "/replacements/confirm",
            grant,
            server_schemas.StreamWorkerReplacementConfirmRequest,
        ),
        "retire_verified_worker_stream_subtask": (
            {**common, "retirement": control("retire_verified_stream_subtask")},
            prefix + "/replacements/retire",
            base["cases"]["retire_verified_stream_subtask"][3],
            server_schemas.StreamWorkerVerifiedRetireRequest,
        ),
        "abort_worker_stream_graph_startup": (
            {"worker_id": worker, "graph_revision_id": graph, "principal": principal},
            f"/workers/{worker}/graphs/{graph}/startup-abort",
            {**plan, "state": "ABORTING"},
            server_schemas.StreamWorkerStartupAbortRequest,
        ),
        "acknowledge_worker_stream_subtask_startup_stop": (
            {**common, "stop": stop},
            prefix + "/startup-stop",
            stopped,
            server_schemas.StreamWorkerStartupStopRequest,
        ),
        "acknowledge_stream_subtask_startup_stop": (
            {"subtask_id": subtask, **stop},
            f"/subtasks/{subtask}/startup-stop",
            stopped,
            server_schemas.StreamGraphStartupStopRequest,
        ),
    }


@pytest.mark.parametrize("name", WORKER_METHODS)
def test_worker_methods_have_paired_signatures(name: str):
    assert inspect.signature(getattr(StreamClient, name)) == inspect.signature(
        getattr(StreamAsyncClient, name)
    )


@pytest.mark.parametrize("name", WORKER_METHODS)
@pytest.mark.parametrize("synchronous", [True, False])
async def test_worker_control_uses_exact_typed_wire_and_basic_authority(
    name: str, synchronous: bool
):
    args, route, body, schema = _worker_cases()[name]
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json=_json(body))

    settings = {"transport": httpx.MockTransport(handler)}
    with temporary_settings({PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False}):
        if synchronous:
            with SyncPrefectClient(
                "http://prefect.test/api",
                auth_string="worker:credential",
                httpx_settings=settings,
            ) as client:
                result = getattr(client, name)(**args)
        else:
            async with PrefectClient(
                "http://prefect.test/api",
                auth_string="worker:credential",
                httpx_settings=settings,
            ) as client:
                result = await getattr(client, name)(**args)
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST" and request.url.path == "/api/streams/v2" + route
    assert request.headers["Authorization"].startswith("Basic ")
    assert "X-Prefect-Subtask-Grant" not in request.headers
    assert "X-Prefect-Stream-Execution-Capability" not in request.headers
    payload = {
        key: value
        for key, value in args.items()
        if key not in {"worker_id", "subtask_id", "graph_revision_id"}
    }
    if name == "claim_worker_stream_subtask":
        payload.update(
            worker_id=args["worker_id"],
            launcher_binding_kind="ASSIGNED_WORKER",
            assignment_epoch=1,
            expected_physical_epoch=0,
        )
    assert json.loads(request.content) == schema.model_validate(payload).model_dump(
        mode="json"
    )
    if name == "poll_worker_stream_subtasks":
        assert len(result) == 1
        assert result[0].model_dump(
            mode="json"
        ) == server_schemas.StreamWorkerAssignment.model_validate(body[0]).model_dump(
            mode="json"
        )
        assert result[0].assignment.assigned_worker_id == args["worker_id"]
    else:
        assert result.model_dump(mode="json") == getattr(
            server_schemas, type(result).__name__
        ).model_validate(body).model_dump(mode="json")


@pytest.mark.parametrize("name", WORKER_METHODS)
@pytest.mark.parametrize("synchronous", [True, False])
async def test_child_context_cannot_use_worker_or_stop_controls(
    name: str, synchronous: bool
):
    args, _, _, _ = _worker_cases()[name]
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json={})

    context = _context()
    settings = {"transport": httpx.MockTransport(handler)}
    with (
        stream_subtask_execution_context(context),
        pytest.raises(ValueError, match="Physical credentials"),
    ):
        if synchronous:
            with SyncPrefectClient(
                context.api_url,
                auth_string="worker:credential",
                httpx_settings=settings,
            ) as client:
                getattr(client, name)(**args)
        else:
            async with PrefectClient(
                context.api_url,
                auth_string="worker:credential",
                httpx_settings=settings,
            ) as client:
                await getattr(client, name)(**args)
    assert not requests


@pytest.mark.parametrize(
    "field,value",
    [
        ("limit", 0),
        ("limit", 129),
        ("limit", True),
        ("active_subtask_ids", [uuid4() for _ in range(65)]),
    ],
)
def test_worker_poll_is_bounded_before_network(field: str, value: Any):
    payload = dict(_worker_cases()["poll_worker_stream_subtasks"][0])
    payload.pop("worker_id")
    payload[field] = value
    with pytest.raises(ValidationError):
        client_schemas.StreamWorkerPollRequest.model_validate(payload)


@pytest.mark.parametrize(
    "unstarted,witness",
    [
        (False, None),
        (
            True,
            {
                "kind": "IDENTITY_GONE",
                "host_id": uuid4(),
                "boot_id": uuid4(),
                "child_process_id": 1,
                "process_start_ticks": 1,
                "observed_at": "2026-11-04T12:00:00Z",
            },
        ),
    ],
)
def test_startup_stop_requires_one_explicit_proof(
    unstarted: bool, witness: dict[str, Any] | None
):
    payload = dict(_worker_cases()["acknowledge_stream_subtask_startup_stop"][0])
    payload.pop("subtask_id")
    payload.update(unstarted=unstarted, death_witness=witness)
    with pytest.raises(ValidationError):
        client_schemas.StreamGraphStartupStopRequest.model_validate(payload)


@pytest.mark.parametrize(
    "directory", ["relative", "/root/../workspace", "//shared/work", "/workspace/\0bad"]
)
def test_deployed_workspace_is_canonical(directory: str):
    payload = dict(_wire()["cases"]["seal_stream_graph"][0])
    payload["working_directory"] = directory
    with pytest.raises(ValidationError):
        client_schemas.StreamGraphSealRequest.model_validate(payload)


def test_worker_paths_are_in_the_public_route_union():
    routes = get_args(ServerRoutes)
    assert "/streams/v2/workers/{worker_id}/poll" in routes
    assert "/streams/v2/workers/{worker_id}/graphs/{graph_id}/startup-abort" in routes
    for suffix in (
        "claim",
        "register",
        "renew",
        "replacements/authorize",
        "replacements/confirm",
        "replacements/retire",
        "startup-stop",
    ):
        assert (
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/" + suffix in routes
        )
    assert "/streams/v2/subtasks/{subtask_id}/startup-stop" in routes


@pytest.mark.parametrize("version", [True, 1.0, "1", 2])
def test_worker_protocol_version_is_explicit_and_strict(version: Any):
    payload = dict(_worker_cases()["poll_worker_stream_subtasks"][0])
    payload.pop("worker_id")
    payload["protocol_version"] = version
    with pytest.raises(ValidationError):
        client_schemas.StreamWorkerPollRequest.model_validate(payload)


def test_worker_configuration_contains_only_the_assigned_parameter_snapshot():
    row = _worker_cases()["poll_worker_stream_subtasks"][2][0]
    result = client_schemas.StreamWorkerAssignment.model_validate(row)
    assert result.task_configuration.role == "OPERATOR"
    assert result.task_configuration.task_key == "partition-key"
    assert result.task_configuration.parameters == {"ref": "object://batch", "split": 0}
    assert not hasattr(result.task_configuration, "operators")


@pytest.mark.parametrize(
    "field,value",
    [
        ("task_run_count", -1),
        ("task_run_count", True),
        ("output_attempt", -1),
        ("output_state", "UNKNOWN"),
        ("task_state", "UNKNOWN"),
    ],
)
def test_worker_observation_hints_are_typed_and_bounded(field: str, value: Any):
    payload = _worker_cases()["poll_worker_stream_subtasks"][2][0]
    payload[field] = value
    with pytest.raises(ValidationError):
        client_schemas.StreamWorkerAssignment.model_validate(payload)


def test_worker_method_paths_match_actual_server_routes():
    cases = _worker_cases()
    for name in WORKER_METHODS:
        arguments, route, _, _ = cases[name]
        matches = [
            candidate
            for candidate in [*parent_router.routes, *worker_router.routes]
            if "POST" in candidate.methods
            and candidate.path.replace(":uuid}", "}").format(
                worker_id=arguments.get("worker_id"),
                subtask_id=arguments.get("subtask_id"),
                graph_id=arguments.get("graph_revision_id"),
            )
            == "/streams/v2" + route
        ]
        assert len(matches) == 1, name
        assert matches[0].path.replace(":uuid}", "}") in get_args(ServerRoutes)
