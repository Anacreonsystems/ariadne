"""Canonical immutable witnesses for native stream receipts."""

import hashlib
import json
from typing import Any
from uuid import UUID


def canonical_json_digest(value: Any) -> tuple[int, str]:
    encoded = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return len(encoded), hashlib.sha256(encoded).hexdigest()


def finite_receipt_outcome_digest(
    *,
    flow_run_id: UUID,
    phase: str,
    submission_key: str,
    manifest_digest: str,
    task_run_id: UUID,
    operation_id: UUID,
    run_count: int,
    result_digest: str,
) -> str:
    _, digest = canonical_json_digest(
        {
            "schema": "native-finite-receipt-outcome-v1",
            "flow_run_id": str(flow_run_id),
            "phase": phase,
            "submission_key": submission_key,
            "manifest_digest": manifest_digest,
            "task_run_id": str(task_run_id),
            "operation_id": str(operation_id),
            "run_count": run_count,
            "result_digest": result_digest,
        }
    )
    return digest
