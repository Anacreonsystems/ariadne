"""Exact paired worker/child DAG request bodies and journal acknowledgement IDs."""

import copy
import inspect
import json
from typing import Any, get_args
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import ValidationError
from tests.client.test_stream_dag_executions import (
    STAMP,
    bootstrap,
    execution,
    principal,
)

from prefect._internal.stream_execution import (
    DAGSubtaskExecutionContext,
    stream_subtask_execution_context,
)
from prefect.client.orchestration._streams.client import (
    StreamAsyncClient,
    StreamClient,
    StreamRequestError,
)
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import stream_dag_executions as c
from prefect.client.schemas.streams import StreamInfo
from prefect.server.schemas import stream_dag_executions as s

ROUTE_PATHS = {
    "read_stream_dag_subtask_bootstrap": "/streams/v2/dags/subtasks/{subtask_id}/bootstrap",
    "read_stream_dag_subtask_activation": "/streams/v2/dags/subtasks/{subtask_id}/activation",
    "mark_stream_dag_subtask_bootstrap_ready": "/streams/v2/dags/subtasks/{subtask_id}/bootstrap-ready",
    "read_stream_dag_input_status": "/streams/v2/dags/subtasks/{subtask_id}/input/status",
    "read_stream_dag_output_status": "/streams/v2/dags/subtasks/{subtask_id}/output/status",
    "open_stream_dag_source": "/streams/v2/dags/sources/open",
    "open_stream_dag_consumer": "/streams/v2/dags/consumers/open",
    "read_stream_dag_consumer": "/streams/v2/dags/consumers/read",
    "finish_stream_dag_source": "/streams/v2/dags/sources/finish",
    "finish_stream_dag_consumer": "/streams/v2/dags/consumers/finish",
    "poll_worker_stream_dag_subtasks": "/streams/v2/dags/workers/{worker_id}/poll",
    "abort_worker_stream_dag_startup": "/streams/v2/dags/workers/{worker_id}/graphs/{graph_id}/startup-abort",
    "prepare_worker_stream_dag_launch": "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/prepare",
    "record_worker_stream_dag_launch": "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/record",
    "close_worker_stream_dag_launch": "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/close",
    "stop_worker_stream_dag_subtask": "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/stop",
}
WORKER_METHODS = (
    "poll_worker_stream_dag_subtasks",
    "abort_worker_stream_dag_startup",
    "prepare_worker_stream_dag_launch",
    "record_worker_stream_dag_launch",
    "close_worker_stream_dag_launch",
    "stop_worker_stream_dag_subtask",
)
CHILD_METHODS = (
    "read_stream_dag_subtask_bootstrap",
    "read_stream_dag_subtask_activation",
    "mark_stream_dag_subtask_bootstrap_ready",
    "read_stream_dag_input_status",
    "read_stream_dag_output_status",
    "open_stream_dag_source",
    "open_stream_dag_consumer",
    "read_stream_dag_consumer",
    "finish_stream_dag_source",
    "finish_stream_dag_consumer",
)


def launch(boot: dict[str, Any]) -> dict[str, Any]:
    return dict(
        id=boot["launch_attempt_id"],
        graph_revision_id=boot["assignment"]["graph_revision_id"],
        subtask_id=boot["assignment"]["id"],
        start_operation_id=uuid4(),
        worker_id=boot["grant"]["worker_id"],
        worker_generation_digest=boot["worker_generation_digest"],
        attempt_index=1,
        expected_grant_id=None,
        expected_physical_epoch=0,
        replacement_operation_id=None,
        state="PID_RECORDED",
        child_process_id=boot["grant"]["child_process_id"],
        process_start_ticks=boot["grant"]["process_start_ticks"],
        local_journal_digest="9" * 64,
        grant_id=boot["grant"]["id"],
    )


def assignment(boot: dict[str, Any]) -> dict[str, Any]:
    attempt = launch(boot)
    return s.StreamDAGWorkerAssignment.model_validate(
        dict(
            flow_run_id=boot["flow_run_id"],
            graph_digest=boot["context"]["graph_digest"],
            routing_digest=boot["context"]["routing_digest"],
            placement_digest="a" * 64,
            start_operation_id=attempt["start_operation_id"],
            assignment=boot["assignment"],
            phase="STARTING",
            current_grant=boot["grant"],
            bootstrap_ready=False,
            working_directory="/work",
            task_configuration={
                key: boot[key]
                for key in (
                    "entrypoint",
                    "task_key",
                    "parameters",
                    "task_version",
                    "artifact_digest",
                    "context_key",
                    "timeout_seconds",
                    "task_policy",
                    "effective_tags",
                    "input_key",
                )
            }
            | {"role": "OPERATOR"},
            task_state="RUNNING",
            task_run_count=2,
            task_retriable=None,
            output_state="OPEN",
            output_attempt=2,
            control=None,
            launch_attempts=[attempt],
        )
    ).model_dump(mode="json")


def wire_case(method: str) -> tuple[dict[str, Any], object, str, dict[str, Any] | None]:
    b = bootstrap()
    sid, gid, wid = (
        b["assignment"]["id"],
        b["assignment"]["graph_revision_id"],
        b["grant"]["worker_id"],
    )
    op, start = uuid4(), uuid4()
    p = principal()
    root = f"/streams/v2/dags/workers/{wid}"
    path = f"{root}/subtasks/{sid}"
    kwargs: dict[str, Any] = dict(
        worker_id=UUID(wid), subtask_id=UUID(sid), principal=p, operation_id=op
    )
    request: dict[str, Any] = dict(principal=p, operation_id=op)
    if method == "poll_worker_stream_dag_subtasks":
        return (
            dict(worker_id=UUID(wid), principal=p),
            [assignment(b)],
            root + "/poll",
            s.StreamDAGWorkerPollRequest(principal=p).model_dump(mode="json"),
        )
    if method == "abort_worker_stream_dag_startup":
        e = execution()
        e.update(
            graph_revision_id=gid,
            graph_digest=b["context"]["graph_digest"],
            routing_digest=b["context"]["routing_digest"],
        )
        request = dict(
            principal=p,
            start_operation_id=start,
            graph_digest=e["graph_digest"],
            routing_digest=e["routing_digest"],
        )
        return (
            dict(worker_id=UUID(wid), graph_revision_id=UUID(gid), **request),
            s.StreamDAGWorkerStartupAbortResult(
                control_operation_id=op, execution=e
            ).model_dump(mode="json"),
            f"{root}/graphs/{gid}/startup-abort",
            s.StreamDAGWorkerStartupAbortRequest.model_validate(request).model_dump(
                mode="json"
            ),
        )
    attempt = launch(b)
    if method in {
        "prepare_worker_stream_dag_launch",
        "record_worker_stream_dag_launch",
    }:
        kind = "PREPARE" if method.startswith("prepare") else "RECORD"
        if kind == "PREPARE":
            attempt.update(
                state="PREPARED",
                child_process_id=None,
                process_start_ticks=None,
                local_journal_digest=None,
                grant_id=None,
                start_operation_id=start,
            )
            kwargs["start_operation_id"] = start
            request.update(start_operation_id=start, replacement_operation_id=None)
            route = path + "/launches/prepare"
            request_model = s.StreamDAGLaunchPrepareRequest
        else:
            kwargs.update(
                launch_attempt_id=UUID(attempt["id"]),
                child_process_id=attempt["child_process_id"],
                process_start_ticks=attempt["process_start_ticks"],
                local_journal_digest=attempt["local_journal_digest"],
            )
            request.update(
                child_process_id=attempt["child_process_id"],
                process_start_ticks=attempt["process_start_ticks"],
                local_journal_digest=attempt["local_journal_digest"],
            )
            route = f"{path}/launches/{attempt['id']}/record"
            request_model = s.StreamDAGLaunchRecordRequest
        body = s.StreamDAGLaunchOperationResult(
            receipt=dict(
                operation_id=op,
                launch_attempt_id=attempt["id"],
                kind=kind,
                request_digest="b" * 64,
                outcome_digest="c" * 64,
                accepted_at=STAMP,
            ),
            current=attempt,
            disposition="SPAWN_ALLOWED" if kind == "PREPARE" else "PREFLIGHT_ALLOWED",
        ).model_dump(mode="json")
        return (
            kwargs,
            body,
            route,
            request_model.model_validate(request).model_dump(mode="json"),
        )
    if method == "close_worker_stream_dag_launch":
        proof = dict(
            kind="CLOSED_UNSTARTED",
            local_journal_digest="d" * 64,
            recovery_complete=True,
            spawn_fence_closed=True,
        )
        kwargs.update(launch_attempt_id=UUID(attempt["id"]), proof=proof)
        body = s.StreamDAGLaunchCloseReceipt(
            operation_id=op,
            launch_attempt_id=attempt["id"],
            graph_revision_id=gid,
            subtask_id=sid,
            attempt_index=1,
            proof_kind=proof["kind"],
            closed_at=STAMP,
            request_digest="e" * 64,
            outcome_digest="f" * 64,
        ).model_dump(mode="json")
        return (
            kwargs,
            body,
            f"{path}/launches/{attempt['id']}/close",
            s.StreamDAGLaunchCloseRequest.model_validate(
                request | {"proof": proof}
            ).model_dump(mode="json"),
        )
    if method == "stop_worker_stream_dag_subtask":
        kwargs["control_operation_id"] = start
        body = s.StreamDAGStopReceipt(
            operation_id=op,
            control_operation_id=start,
            graph_revision_id=gid,
            subtask_id=sid,
            task_run_id=b["task_run"]["id"],
            grant_id=None,
            physical_epoch=0,
            task_state="CANCELLED",
            tag_allocation_released=True,
            all_attempts_closed_digest="1" * 64,
            outcome_digest="2" * 64,
        ).model_dump(mode="json")
        return (
            kwargs,
            body,
            path + "/stop",
            s.StreamDAGWorkerStopRequest.model_validate(
                request | {"control_operation_id": start}
            ).model_dump(mode="json"),
        )
    path = f"/streams/v2/dags/subtasks/{sid}"
    kwargs = dict(subtask_id=UUID(sid))
    own = dict(
        graph_revision_id=gid,
        subtask_id=sid,
        grant_id=b["grant"]["id"],
        physical_epoch=1,
    )
    if method == "read_stream_dag_subtask_bootstrap":
        return kwargs, b, path + "/bootstrap", None
    if method == "read_stream_dag_subtask_activation":
        return (
            kwargs,
            s.StreamDAGActivationObservation(
                **own, phase="WAITING", activation_operation_id=None
            ).model_dump(mode="json"),
            path + "/activation",
            None,
        )
    if method == "mark_stream_dag_subtask_bootstrap_ready":
        kwargs.update(
            operation_id=op, bootstrap_digest=b["bootstrap_digest"], signature="a" * 128
        )
        body = s.StreamDAGBootstrapReadyResult(
            **own,
            operation_id=op,
            bootstrap_digest=b["bootstrap_digest"],
            ready_at=STAMP,
            request_digest="b" * 64,
            outcome_digest="c" * 64,
        ).model_dump(mode="json")
        return (
            kwargs,
            body,
            path + "/bootstrap-ready",
            s.StreamDAGBootstrapReadyRequest.model_validate(
                {key: value for key, value in kwargs.items() if key != "subtask_id"}
            ).model_dump(mode="json"),
        )
    if method in {"read_stream_dag_input_status", "read_stream_dag_output_status"}:
        direction = "input" if "input" in method else "output"
        info = dict(
            id=b["assignment"][direction + "_stream_id"],
            flow_run_id=b["flow_run_id"],
            protocol_version=1,
            state="OPEN",
            error=None,
            last_sequence=0,
            tail_sequence=None,
            producer_position=0,
            record_count=0,
            byte_count=0,
            limits=dict(max_records=1000, max_bytes=1000000, max_record_bytes=1000),
            released=False,
        )
        return (
            kwargs,
            StreamInfo.model_validate(info).model_dump(mode="json"),
            path + f"/{direction}/status",
            None,
        )
    context = b["context"]
    binding = dict(
        flow_run_id=b["flow_run_id"],
        consumer_id=b["input"]["subscriber_id"],
        input_stream_id=b["assignment"]["input_stream_id"],
        output_stream_id=b["assignment"]["output_stream_id"],
        owner_id=b["grant"]["holder_id"],
        attempt=2,
        owner_epoch=3,
    )
    source_binding = dict(
        flow_run_id=b["flow_run_id"],
        stream_id=b["output"]["stream_id"],
        task_run_id=b["task_run"]["id"],
        owner_id=b["grant"]["holder_id"],
        attempt=2,
        owner_epoch=3,
    )
    if method == "open_stream_dag_source":
        return (
            dict(context=context),
            dict(binding=source_binding, state="OPEN", accepted_source_position=200),
            "/streams/v2/dags/sources/open",
            dict(context=context),
        )
    if method == "open_stream_dag_consumer":
        return (
            dict(context=context),
            binding,
            "/streams/v2/dags/consumers/open",
            dict(context=context),
        )
    if method == "read_stream_dag_consumer":
        token = binding | dict(
            record_id=str(uuid4()), sequence=201, delivery_id=str(uuid4())
        )
        return (
            dict(context=context, binding=binding),
            dict(
                token=token, value={"document": 201}, managed_state={"processed": 200}
            ),
            "/streams/v2/dags/consumers/read",
            dict(context=context, binding=binding),
        )
    source = method == "finish_stream_dag_source"
    kwargs = dict(
        operation_id=op, context=context, binding=source_binding if source else binding
    )
    if source:
        kwargs["expected_position"] = 200
    body = s.StreamDAGProducerFinishReceipt(
        operation_id=op,
        graph_revision_id=gid,
        producer_subtask_id=sid,
        producer_stream_id=b["output"]["stream_id"],
        kind="SOURCE_EOF" if source else "CONSUMER_EOF",
        tail_sequence=200,
        accepted_source_position=200 if source else None,
        accepted_input_sequence=None if source else 201,
        semantic_digest="e" * 64,
        outcome_digest="f" * 64,
    ).model_dump(mode="json")
    req_model = (
        s.StreamDAGSourceFinishRequest if source else s.StreamDAGConsumerFinishRequest
    )
    return (
        kwargs,
        body,
        f"/streams/v2/dags/{'sources' if source else 'consumers'}/finish",
        req_model.model_validate(kwargs).model_dump(mode="json"),
    )


@pytest.mark.parametrize("method", WORKER_METHODS + CHILD_METHODS)
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_exact_worker_child_methods(method: str, asynchronous: bool):
    kwargs, response, path, body = wire_case(method)
    seen: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == path
        assert request.method == ("GET" if body is None else "POST")
        seen.append(request.content)
        return httpx.Response(200, json=response)

    if asynchronous:
        async with httpx.AsyncClient(
            base_url="http://test", transport=httpx.MockTransport(handler)
        ) as http:
            client = StreamAsyncClient(http)
            result = await getattr(client, method)(**kwargs)
            await getattr(client, method)(**kwargs)
    else:
        with httpx.Client(
            base_url="http://test", transport=httpx.MockTransport(handler)
        ) as http:
            client = StreamClient(http)
            result = getattr(client, method)(**kwargs)
            getattr(client, method)(**kwargs)
    assert seen[0] == seen[1]
    assert (json.loads(seen[0]) if seen[0] else None) == body
    assert inspect.signature(getattr(StreamClient, method)) == inspect.signature(
        getattr(StreamAsyncClient, method)
    )
    if method != "read_stream_dag_subtask_bootstrap":
        assert (
            [row.model_dump(mode="json") for row in result]
            if isinstance(result, list)
            else result.model_dump(mode="json")
        ) == response
    assert ROUTE_PATHS[method] in get_args(ServerRoutes)


@pytest.mark.parametrize(
    "method",
    [
        "prepare_worker_stream_dag_launch",
        "record_worker_stream_dag_launch",
        "close_worker_stream_dag_launch",
        "stop_worker_stream_dag_subtask",
        "mark_stream_dag_subtask_bootstrap_ready",
        "finish_stream_dag_source",
        "finish_stream_dag_consumer",
    ],
)
def test_valid_but_foreign_receipt_fails_closed(method: str):
    kwargs, body, _, _ = wire_case(method)
    body = copy.deepcopy(body)
    if "receipt" in body:
        body["receipt"]["operation_id"] = str(uuid4())
    else:
        body["operation_id"] = str(uuid4())
    with httpx.Client(
        base_url="http://test",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)),
    ) as http:
        with pytest.raises(StreamRequestError) as exc:
            getattr(StreamClient(http), method)(**kwargs)
    assert exc.value.code == "IDENTITY_CONFLICT"


def test_worker_preview_control_is_not_an_assigned_control():
    body = assignment(bootstrap())
    body.update(
        phase="STOPPING",
        control=dict(operation_id=str(uuid4()), kind="PREVIEW_ABORT", state="STOPPING"),
    )
    for schema in (c, s):
        with pytest.raises(ValidationError, match="preview"):
            schema.StreamDAGWorkerAssignment.model_validate(body)


@pytest.mark.parametrize(
    "method", ["prepare_worker_stream_dag_launch", "record_worker_stream_dag_launch"]
)
def test_known_launch_ack_can_report_later_stop_required(method: str):
    kwargs, body, _, _ = wire_case(method)
    original_receipt = copy.deepcopy(body["receipt"])
    body["disposition"] = "STOP_REQUIRED"
    body["current"].update(
        state="CLOSED",
        child_process_id=101,
        process_start_ticks=103,
        local_journal_digest="9" * 64,
    )
    with httpx.Client(
        base_url="http://test",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)),
    ) as http:
        result = getattr(StreamClient(http), method)(**kwargs)
    assert result.receipt.model_dump(mode="json") == original_receipt
    assert result.current.state == "CLOSED"
    assert result.disposition == "STOP_REQUIRED"


@pytest.mark.parametrize(
    "malformed", ["closed_no_digest", "prepared_with_grant", "closed_grant_without_pid"]
)
def test_launch_state_requires_the_complete_recorded_journal(malformed: str):
    attempt = launch(bootstrap())
    if malformed == "closed_no_digest":
        attempt.update(state="CLOSED", local_journal_digest=None)
    elif malformed == "prepared_with_grant":
        attempt.update(
            state="PREPARED",
            child_process_id=None,
            process_start_ticks=None,
            local_journal_digest=None,
        )
    else:
        attempt.update(state="CLOSED", child_process_id=None, process_start_ticks=None)
    for schema in (c, s):
        with pytest.raises(ValidationError):
            schema.StreamDAGLaunchAttempt.model_validate(attempt)


@pytest.mark.parametrize(
    "field", ["attempt_ids", "grant_id", "death_witness", "unstarted"]
)
def test_stop_cannot_nominate_attempt_subset(field: str):
    request = dict(
        principal=principal(), operation_id=uuid4(), control_operation_id=uuid4()
    )
    for schema in (c, s):
        with pytest.raises(ValidationError):
            schema.StreamDAGWorkerStopRequest.model_validate(request | {field: []})


@pytest.mark.parametrize(
    "field", ["expected_grant_id", "expected_physical_epoch", "attempt_index"]
)
def test_prepare_does_not_accept_worker_selected_authority(field: str):
    request = dict(
        principal=principal(),
        operation_id=uuid4(),
        start_operation_id=uuid4(),
        replacement_operation_id=None,
    )
    for schema in (c, s):
        with pytest.raises(ValidationError):
            schema.StreamDAGLaunchPrepareRequest.model_validate(request | {field: 1})


@pytest.mark.parametrize(
    "name", ["StreamDAGSourceOpenRequest", "StreamDAGConsumerOpenRequest"]
)
@pytest.mark.parametrize(
    "field", ["owner_id", "attempt", "owner_epoch", "task_run_id", "consumer_id"]
)
def test_open_cannot_nominate_ownership(name: str, field: str):
    context = bootstrap()["context"]
    for schema in (c, s):
        with pytest.raises(ValidationError):
            getattr(schema, name).model_validate(
                dict(context=context) | {field: str(uuid4())}
            )


@pytest.mark.parametrize("flag", ["recovery_complete", "spawn_fence_closed"])
@pytest.mark.parametrize("value", [False, 1, "true", None])
def test_no_pid_attestation_requires_exact_closed_journal(flag: str, value: object):
    proof = dict(
        kind="CLOSED_UNSTARTED",
        local_journal_digest="a" * 64,
        recovery_complete=True,
        spawn_fence_closed=True,
    )
    for schema in (c, s):
        with pytest.raises(ValidationError):
            schema.StreamDAGLaunchCloseRequest.model_validate(
                dict(
                    principal=principal(),
                    operation_id=uuid4(),
                    proof=proof | {flag: value},
                )
            )


@pytest.mark.parametrize("direction", ["input", "output"])
def test_scoped_status_rejects_other_own_port(direction: str):
    b = bootstrap()
    context = DAGSubtaskExecutionContext(
        flow_run_id=UUID(b["flow_run_id"]),
        graph_revision_id=UUID(b["assignment"]["graph_revision_id"]),
        assignment_epoch=1,
        subtask_id=UUID(b["assignment"]["id"]),
        grant_id=UUID(b["grant"]["id"]),
        physical_epoch=1,
        holder_id=UUID(b["grant"]["holder_id"]),
        scope_digest=b["grant"]["scope_digest"],
        manifest_digest=b["assignment"]["manifest"]["manifest_digest"],
        task_run_id=UUID(b["task_run"]["id"]),
        input_stream_id=UUID(b["assignment"]["input_stream_id"]),
        output_stream_id=UUID(b["output"]["stream_id"]),
        child_process_id=101,
        process_start_ticks=103,
        api_url="http://test",
        capability="a" * 64,
        event_private_key=b"b" * 32,
    )
    wrong = "output" if direction == "input" else "input"
    response = StreamInfo(
        id=b["assignment"][wrong + "_stream_id"],
        flow_run_id=b["flow_run_id"],
        protocol_version=1,
        state="OPEN",
        error=None,
        last_sequence=0,
        tail_sequence=None,
        producer_position=0,
        record_count=0,
        byte_count=0,
        limits={},
        released=False,
    ).model_dump(mode="json")
    with (
        stream_subtask_execution_context(context),
        httpx.Client(
            base_url="http://test",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=response)
            ),
        ) as http,
    ):
        with pytest.raises(StreamRequestError, match="does not match"):
            getattr(StreamClient(http), f"read_stream_dag_{direction}_status")(
                subtask_id=context.subtask_id
            )
