"""Strict lifecycle/launch wire guards independently of executing a DAG."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from prefect.server.schemas import stream_dag_executions as s


def operation() -> dict[str, object]:
    return {
        "operation_id": uuid4(),
        "graph_digest": "11" * 32,
        "routing_digest": "22" * 32,
    }


def principal() -> dict[str, object]:
    return {
        "work_pool_id": uuid4(),
        "host_id": uuid4(),
        "boot_id": uuid4(),
        "launcher_process_id": 7,
        "launcher_start_ticks": 8,
    }


@pytest.mark.parametrize(
    "field", ["grant_ids", "readiness_set_digest", "targets", "expected_phase"]
)
def test_activation_rejects_client_selected_authority(field: str):
    with pytest.raises(ValidationError):
        s.StreamDAGActivateRequest.model_validate(operation() | {field: []})


@pytest.mark.parametrize("value", [True, 1.0, "1", 0, 2])
def test_worker_poll_requires_exact_version_integer(value: object):
    with pytest.raises(ValidationError):
        s.StreamDAGWorkerPollRequest.model_validate(
            {"principal": principal(), "protocol_version": value}
        )


def test_worker_prepare_cannot_nominate_grant_or_epoch():
    request = {
        "principal": principal(),
        "operation_id": uuid4(),
        "start_operation_id": uuid4(),
        "replacement_operation_id": None,
    }
    valid = s.StreamDAGLaunchPrepareRequest.model_validate(request)
    assert (
        s.StreamDAGLaunchPrepareRequest.model_validate_json(valid.model_dump_json())
        == valid
    )
    for field, value in (
        ("expected_grant_id", uuid4()),
        ("expected_physical_epoch", 0),
        ("attempt_index", 1),
    ):
        with pytest.raises(ValidationError):
            s.StreamDAGLaunchPrepareRequest.model_validate(request | {field: value})


@pytest.mark.parametrize("value", [False, 1, "true", None])
def test_closed_local_journal_requires_exact_true(value: object):
    with pytest.raises(ValidationError):
        s.StreamDAGLaunchUnstartedProof(
            kind="CLOSED_UNSTARTED",
            local_journal_digest="11" * 32,
            recovery_complete=value,
            spawn_fence_closed=True,
        )


def test_finalize_stop_rejects_worker_selected_attempt_subset():
    request = {
        "principal": principal(),
        "operation_id": uuid4(),
        "control_operation_id": uuid4(),
    }
    s.StreamDAGWorkerStopRequest.model_validate(request)
    for field in ("attempt_ids", "grant_id", "death_witness", "unstarted"):
        with pytest.raises(ValidationError):
            s.StreamDAGWorkerStopRequest.model_validate(request | {field: []})


@pytest.mark.parametrize("kind", ["SOURCE_EOF", "CONSUMER_EOF"])
def test_finish_receipt_has_exact_original_coordinate(kind: str):
    receipt = {
        "operation_id": uuid4(),
        "graph_revision_id": uuid4(),
        "producer_subtask_id": uuid4(),
        "producer_stream_id": uuid4(),
        "kind": kind,
        "tail_sequence": 0,
        "accepted_source_position": 0 if kind == "SOURCE_EOF" else None,
        "accepted_input_sequence": 0 if kind == "CONSUMER_EOF" else None,
        "semantic_digest": "11" * 32,
        "outcome_digest": "22" * 32,
    }
    s.StreamDAGProducerFinishReceipt.model_validate(receipt)
    for source, consumer in ((None, None), (0, 0)):
        with pytest.raises(ValidationError):
            s.StreamDAGProducerFinishReceipt.model_validate(
                receipt
                | {
                    "accepted_source_position": source,
                    "accepted_input_sequence": consumer,
                }
            )


def test_fail_has_bounded_kind_and_no_arbitrary_error():
    s.StreamDAGFailRequest.model_validate(
        operation() | {"failure_kind": "START_TIMEOUT"}
    )
    with pytest.raises(ValidationError):
        s.StreamDAGFailRequest.model_validate(
            operation()
            | {"failure_kind": "START_TIMEOUT", "error": "user-selected error"}
        )


def test_preview_projection_cannot_claim_occupied_or_completed_work():
    counts = {
        "total": 2,
        "registered": 0,
        "bootstrap_ready": 0,
        "terminal_tasks": 0,
        "stopped": 0,
        "physical_slots_remaining": 0,
        "tags_remaining": 0,
    }
    projection = {
        "graph_revision_id": uuid4(),
        "flow_run_id": uuid4(),
        "graph_digest": "11" * 32,
        "routing_digest": "22" * 32,
        "phase": "PREVIEW",
        "terminal_state": None,
        "cleanup_complete": False,
        "counts": counts,
        "control": None,
        "activation_operation_id": None,
        "readiness_set_digest": None,
        "terminal_operation_id": None,
        "terminal_outcome_digest": None,
    }
    s.StreamDAGExecution.model_validate(projection)
    for field in (
        "registered",
        "terminal_tasks",
        "stopped",
        "physical_slots_remaining",
        "tags_remaining",
    ):
        with pytest.raises(ValidationError):
            s.StreamDAGExecution.model_validate(
                projection | {"counts": counts | {field: 1}}
            )


def worker_assignment() -> dict[str, object]:
    flow, graph, worker, subtask, task, stream = (uuid4() for _ in range(6))
    assignment = {
        "id": subtask,
        "operator_run_id": uuid4(),
        "operator_uid": "A",
        "subtask_index": 0,
        "assignment_epoch": 1,
        "input_stream_id": None,
        "output_stream_id": stream,
        "assigned_worker_id": worker,
        "physical_slot_reserved": True,
        "physical_slot_released": False,
        "graph_revision_id": graph,
        "current_grant_id": None,
        "physical_epoch": 0,
        "manifest": {
            "flow_run_id": flow,
            "submission_key": "dag/A/0",
            "manifest_digest": "33" * 32,
            "task_run_id": task,
            "stream_id": stream,
            "dynamic_key": "dag-a",
            "role": "SOURCE",
            "protocol": "source-v1",
            "state": "ACTIVE",
            "created_at": "2026-11-05T00:00:00Z",
        },
    }
    return {
        "flow_run_id": flow,
        "graph_digest": "11" * 32,
        "routing_digest": "22" * 32,
        "placement_digest": "44" * 32,
        "start_operation_id": uuid4(),
        "assignment": assignment,
        "phase": "STARTING",
        "current_grant": None,
        "bootstrap_ready": False,
        "working_directory": "/workspace",
        "task_configuration": {
            "entrypoint": "fixture.py:source",
            "task_key": "a",
            "role": "SOURCE",
            "parameters": {"split": 0},
            "task_version": "v1",
            "artifact_digest": "55" * 32,
            "context_key": "ctx",
            "timeout_seconds": None,
            "task_policy": {"retries": 1},
            "effective_tags": [],
            "input_key": None,
        },
        "task_state": "PENDING",
        "task_run_count": 0,
        "task_retriable": None,
        "output_state": "OPEN",
        "output_attempt": 0,
        "control": None,
        "launch_attempts": [],
    }


@pytest.mark.parametrize(
    "directory",
    [
        "relative",
        "/workspace/../other",
        "/workspace//other",
        "/workspace/",
        "/bad\x00path",
    ],
)
def test_worker_directory_is_bounded_absolute_normalized(directory: str):
    with pytest.raises(ValidationError):
        s.StreamDAGWorkerAssignment.model_validate(
            worker_assignment() | {"working_directory": directory}
        )


def test_worker_projection_cannot_hide_current_grant_or_fabricate_readiness():
    request = worker_assignment()
    valid = s.StreamDAGWorkerAssignment.model_validate(request)
    assert (
        s.StreamDAGWorkerAssignment.model_validate_json(valid.model_dump_json())
        == valid
    )
    with pytest.raises(ValidationError):
        s.StreamDAGWorkerAssignment.model_validate(request | {"bootstrap_ready": True})
    bad = valid.assignment.model_dump() | {
        "current_grant_id": uuid4(),
        "physical_epoch": 1,
    }
    with pytest.raises(ValidationError):
        s.StreamDAGWorkerAssignment.model_validate(request | {"assignment": bad})
    with pytest.raises(ValidationError):
        s.StreamDAGWorkerAssignment.model_validate(request | {"task_retriable": 1})


@pytest.mark.parametrize("pid", [None, 21])
def test_closed_attempt_retains_recovered_local_journal(pid: int | None):
    request = {
        "id": uuid4(),
        "graph_revision_id": uuid4(),
        "subtask_id": uuid4(),
        "start_operation_id": uuid4(),
        "worker_id": uuid4(),
        "worker_generation_digest": "11" * 32,
        "attempt_index": 1,
        "expected_grant_id": None,
        "expected_physical_epoch": 0,
        "replacement_operation_id": None,
        "state": "CLOSED",
        "child_process_id": pid,
        "process_start_ticks": 22 if pid is not None else None,
        "local_journal_digest": "22" * 32,
        "grant_id": None,
    }
    s.StreamDAGLaunchAttempt.model_validate(request)
    with pytest.raises(ValidationError):
        s.StreamDAGLaunchAttempt.model_validate(
            request | {"local_journal_digest": None}
        )


def test_prepare_cannot_carry_a_grant_before_recording_process():
    request = {
        "id": uuid4(),
        "graph_revision_id": uuid4(),
        "subtask_id": uuid4(),
        "start_operation_id": uuid4(),
        "worker_id": uuid4(),
        "worker_generation_digest": "11" * 32,
        "attempt_index": 1,
        "expected_grant_id": None,
        "expected_physical_epoch": 0,
        "replacement_operation_id": None,
        "state": "PREPARED",
        "child_process_id": None,
        "process_start_ticks": None,
        "local_journal_digest": None,
        "grant_id": None,
    }
    s.StreamDAGLaunchAttempt.model_validate(request)
    with pytest.raises(ValidationError):
        s.StreamDAGLaunchAttempt.model_validate(request | {"grant_id": uuid4()})
