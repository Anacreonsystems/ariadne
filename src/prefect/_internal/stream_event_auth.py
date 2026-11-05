"""Canonical, signed TaskRun observations for native execution generations.

Only public signatures and key IDs enter events. Holder capabilities and
private keys belong to the launching process's execution context.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

_HEX_DIGEST = re.compile(r"[0-9a-f]{64}")
_SIGNATURE = re.compile(r"[A-Za-z0-9_-]{86}")


def canonical_event_witness(
    *,
    event_id: UUID,
    event: str,
    occurred: datetime,
    flow_run_id: UUID,
    task_run_id: UUID,
    generation: int,
    holder_id: UUID,
    manifest_digest: str,
    run_count: int,
    task_run: dict[str, Any],
    validated_state: dict[str, Any],
) -> bytes:
    """Bind all state-bearing event bytes to one immutable execution holder."""
    if type(generation) is not int or generation < 1:
        raise ValueError("An event requires a positive execution generation")
    if type(run_count) is not int or run_count < 0:
        raise ValueError("An event requires a nonnegative TaskRun run count")
    if not _HEX_DIGEST.fullmatch(manifest_digest):
        raise ValueError("An event requires a canonical manifest digest")
    if occurred.tzinfo is None or occurred.utcoffset() is None:
        raise ValueError("An event requires an aware timestamp")
    witness = {
        "schema": "prefect-stream-task-event-v1",
        "event_id": str(event_id),
        "event": event,
        "occurred": occurred.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ),
        "flow_run_id": str(flow_run_id),
        "task_run_id": str(task_run_id),
        "generation": generation,
        "holder_id": str(holder_id),
        "manifest_digest": manifest_digest,
        "run_count": run_count,
        "task_run": task_run,
        "validated_state": validated_state,
    }
    return json.dumps(
        witness,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_physical_event_witness(
    *,
    event_id: UUID,
    event: str,
    occurred: datetime,
    flow_run_id: UUID,
    task_run_id: UUID,
    generation: int,
    holder_id: UUID,
    manifest_digest: str,
    run_count: int,
    task_run: dict[str, Any],
    validated_state: dict[str, Any],
    graph_revision_id: UUID,
    assignment_epoch: int,
    subtask_id: UUID,
    grant_id: UUID,
    physical_epoch: int,
    scope_digest: str,
) -> bytes:
    """Bind an ordinary TaskRun event to one sealed physical assignment."""
    if type(assignment_epoch) is not int or assignment_epoch < 1:
        raise ValueError("An event requires a positive assignment epoch")
    if (
        type(physical_epoch) is not int
        or not 1 <= physical_epoch <= 64
        or physical_epoch != generation
    ):
        raise ValueError("An event requires its exact physical generation")
    if not _HEX_DIGEST.fullmatch(scope_digest):
        raise ValueError("An event requires a canonical physical scope digest")
    witness = json.loads(
        canonical_event_witness(
            event_id=event_id,
            event=event,
            occurred=occurred,
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
            generation=generation,
            holder_id=holder_id,
            manifest_digest=manifest_digest,
            run_count=run_count,
            task_run=task_run,
            validated_state=validated_state,
        )
    )
    witness.update(
        schema="prefect-stream-physical-task-event-v1",
        graph_revision_id=str(graph_revision_id),
        assignment_epoch=assignment_epoch,
        subtask_id=str(subtask_id),
        grant_id=str(grant_id),
        physical_epoch=physical_epoch,
        scope_digest=scope_digest,
    )
    return json.dumps(
        witness,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def event_key_id(public_key: bytes) -> str:
    """Return the public identity of one raw Ed25519 key."""
    if len(public_key) != 32:
        raise ValueError("An Ed25519 public key must contain 32 bytes")
    return hashlib.sha256(public_key).hexdigest()


def sign_task_event(private_key: bytes, witness: bytes) -> str:
    """Sign a canonical witness without including the private key in the event."""
    signature = Ed25519PrivateKey.from_private_bytes(private_key).sign(witness)
    return base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")


def verify_task_event(public_key: bytes, witness: bytes, signature: object) -> bool:
    """Fail closed for invalid keys, signatures or noncanonical encodings."""
    if not isinstance(signature, str) or not _SIGNATURE.fullmatch(signature):
        return False
    try:
        decoded = base64.b64decode(signature + "==", altchars=b"-_", validate=True)
        # Reject alternate encodings of the same last byte, including unused bits.
        if base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=") != signature:
            return False
        Ed25519PublicKey.from_public_bytes(public_key).verify(decoded, witness)
    except (InvalidSignature, ValueError, TypeError, binascii.Error):
        return False
    return True
