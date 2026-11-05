"""Shared fixed-profile control operation identities; no execution credentials."""

from uuid import UUID, uuid5

STARTUP_ABORT_OPERATION = "prefect-stream-startup-abort-v1"
STARTUP_ABORT_ERROR = "Physical startup did not cross the activation barrier"
PREFLIGHT_MAX_BYTES = 256 * 1024
PREFLIGHT_TIMEOUT_SECONDS = 30.0
PREFLIGHT_READY = b"R"
PREFLIGHT_ERROR = b"E"
BOOTSTRAP_MAX_BYTES = 16 * 1024
METADATA_POLL_INTERVAL_SECONDS = 1.0


def startup_abort_operation_id(graph_revision_id: UUID) -> UUID:
    """Coordinate one exact preactivation abort across Flow and worker actors."""
    return uuid5(graph_revision_id, STARTUP_ABORT_OPERATION)
