"""Private DAG child gates and stable worker operation identities."""

from __future__ import annotations

import os
import stat
from uuid import UUID, uuid5

DAG_RECORD_ALLOWED = b"P"
DAG_CODE_READY = b"R"
DAG_BOOTSTRAP_READY = b"B"
DAG_GO = b"G"
DAG_STARTUP_ERROR = b"E"
DAG_PREFLIGHT_MAX_BYTES = 256 * 1024
DAG_BOOTSTRAP_MAX_BYTES = 16 * 1024
DAG_STARTUP_TIMEOUT_SECONDS = 30.0
DAG_MAX_LAUNCH_ATTEMPTS = 64


class DAGChildGateError(ValueError):
    """The child has no verified permission to advance its private startup."""


def read_dag_private_bytes(descriptor: int, maximum: int) -> bytes:
    """Read one bounded, EOF-delimited private pipe; never inspect a file."""
    if type(descriptor) is not int or descriptor < 3:
        raise DAGChildGateError("A private DAG pipe descriptor is required")
    if type(maximum) is not int or not 1 <= maximum <= DAG_PREFLIGHT_MAX_BYTES:
        raise DAGChildGateError("A bounded private DAG message is required")
    try:
        if not stat.S_ISFIFO(os.fstat(descriptor).st_mode):
            raise DAGChildGateError("DAG startup accepts only a private pipe")
        with os.fdopen(descriptor, "rb") as pipe:
            value = pipe.read(maximum + 1)
    except OSError as error:
        raise DAGChildGateError("The private DAG pipe is unavailable") from error
    if not value or len(value) > maximum:
        raise DAGChildGateError("The private DAG message is absent or oversized")
    return value


def read_dag_private_gate(descriptor: int, expected: bytes) -> None:
    """Parent/control-channel loss cannot authorize preflight or user code."""
    if type(expected) is not bytes or len(expected) != 1:
        raise DAGChildGateError("A private DAG gate requires one exact token")
    if read_dag_private_bytes(descriptor, 1) != expected:
        raise DAGChildGateError("The private DAG startup gate was refused")


def _identity(value: UUID) -> UUID:
    if type(value) is not UUID:
        raise ValueError("A DAG operation requires an exact UUID")
    return value


def dag_launch_prepare_operation_id(
    subtask_id: UUID,
    start_operation_id: UUID,
    replacement_operation_id: UUID | None = None,
) -> UUID:
    _identity(start_operation_id)
    authorization = (
        _identity(replacement_operation_id)
        if replacement_operation_id is not None
        else start_operation_id
    )
    return uuid5(
        _identity(subtask_id), "prefect-stream-dag-launch-v1/" + str(authorization)
    )


def dag_launch_record_operation_id(launch_attempt_id: UUID) -> UUID:
    return uuid5(_identity(launch_attempt_id), "prefect-stream-dag-record-v1")


def dag_launch_close_operation_id(launch_attempt_id: UUID) -> UUID:
    return uuid5(_identity(launch_attempt_id), "prefect-stream-dag-close-v1")


def dag_subtask_stop_operation_id(control_operation_id: UUID, subtask_id: UUID) -> UUID:
    return uuid5(
        _identity(control_operation_id),
        "prefect-stream-dag-stop-v1/" + str(_identity(subtask_id)),
    )


def dag_bootstrap_ready_operation_id(grant_id: UUID) -> UUID:
    return uuid5(_identity(grant_id), "prefect-stream-dag-bootstrap-ready-v1")


def dag_startup_abort_operation_id(graph_revision_id: UUID) -> UUID:
    return uuid5(_identity(graph_revision_id), "prefect-stream-dag-startup-abort-v1")
