"""One-shot launcher authority for the ordinary flow-engine entrypoint."""

from __future__ import annotations

import json
import os
import select
import time
from contextlib import contextmanager
from typing import Any, cast
from uuid import UUID

from prefect._internal.stream_execution import (
    StreamExecutionContext,
    stream_execution_context,
)
from prefect.settings import PREFECT_API_URL

_EXECUTION_FD = "PREFECT__STREAM_EXECUTION_FD"
_MAX_BOOTSTRAP_BYTES = 4096


def _unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate bootstrap field")
        result[key] = value
    return result


@contextmanager
def flow_execution_bootstrap(flow_run_id: UUID):
    selector = os.environ.pop(_EXECUTION_FD, None)
    if selector is None:
        yield
        return
    if os.name != "posix":
        raise ValueError("Native execution descriptor bootstrap requires POSIX")
    descriptor = int(selector)
    if descriptor < 3:
        raise ValueError("Native execution requires a dedicated inherited descriptor")
    encoded = bytearray()
    try:
        os.set_inheritable(descriptor, False)
        deadline = time.monotonic() + 5
        while len(encoded) <= _MAX_BOOTSTRAP_BYTES:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([descriptor], [], [], remaining)[0]:
                raise ValueError("Native execution bootstrap was not completed")
            chunk = os.read(descriptor, _MAX_BOOTSTRAP_BYTES + 1 - len(encoded))
            if not chunk:
                break
            encoded.extend(chunk)
        if len(encoded) > _MAX_BOOTSTRAP_BYTES:
            raise ValueError("Native execution bootstrap exceeds its size limit")
    finally:
        os.close(descriptor)
    try:
        value = json.loads(encoded, object_pairs_hook=_unique_fields)
        if type(value) is not dict:
            raise ValueError("Unexpected bootstrap shape")
        value = cast(dict[str, Any], value)
        if set(value) != {
            "schema",
            "flow_run_id",
            "generation",
            "holder_id",
            "api_url",
            "holder_capability",
            "event_private_key",
        }:
            raise ValueError("Unexpected bootstrap shape")
        if (
            value["schema"] != "native-stream-execution-v1"
            or UUID(value["flow_run_id"]) != flow_run_id
        ):
            raise ValueError("Bootstrap identity mismatch")
        configured = PREFECT_API_URL.value()
        if not configured or configured.rstrip("/") != value["api_url"].rstrip("/"):
            raise ValueError("Bootstrap routing mismatch")
        if (
            not isinstance(value["event_private_key"], str)
            or len(value["event_private_key"]) != 64
        ):
            raise ValueError("Invalid signing key")
        execution = StreamExecutionContext(
            flow_run_id,
            value["generation"],
            UUID(value["holder_id"]),
            value["api_url"],
            value["holder_capability"],
            bytes.fromhex(value["event_private_key"]),
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        # Never echo capability/key material from invalid bootstrap JSON.
        raise ValueError("Invalid native execution bootstrap") from None
    with stream_execution_context(execution):
        yield
