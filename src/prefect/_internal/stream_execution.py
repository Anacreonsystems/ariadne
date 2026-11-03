"""Process-local authority supplied by the native flow launcher.

This context is separate from Flow/Task parameters and serialized run context.
Thread-pool tasks inherit it through ordinary ContextVar propagation.
"""

from __future__ import annotations

import re
from collections.abc import Generator, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, SupportsIndex
from urllib.parse import urlsplit
from uuid import UUID

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prefect._internal.stream_event_auth import (
    canonical_event_witness,
    event_key_id,
    sign_task_event,
)

_HEX = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class StreamExecutionContext:
    """Immutable launch credentials; registration adds only public witnesses."""

    flow_run_id: UUID
    generation: int
    holder_id: UUID
    api_url: str
    capability: str = field(repr=False)
    event_private_key: bytes = field(repr=False)
    _manifest_digests: dict[UUID, str] = field(
        default_factory=lambda: dict[UUID, str](), init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if type(self.generation) is not int or not 1 <= self.generation <= 64:
            raise ValueError("A native execution generation must be between 1 and 64")
        if not _HEX.fullmatch(self.capability):
            raise ValueError("An execution capability must contain 32 opaque bytes")
        if len(self.event_private_key) != 32:
            raise ValueError("An event signing key must contain 32 bytes")
        parsed = urlsplit(self.api_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Execution routing requires a credential-free API URL")
        object.__setattr__(self, "api_url", self.api_url.rstrip("/"))

    @property
    def event_public_key(self) -> bytes:
        return (
            Ed25519PrivateKey.from_private_bytes(self.event_private_key)
            .public_key()
            .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        )

    @property
    def event_key_id(self) -> str:
        return event_key_id(self.event_public_key)

    def register_manifest(self, task_run_id: UUID, digest: str) -> None:
        if not _HEX.fullmatch(digest):
            raise ValueError("An execution requires a canonical manifest digest")
        existing = self._manifest_digests.setdefault(task_run_id, digest)
        if existing != digest:
            raise ValueError("A TaskRun cannot change its submission manifest")

    def manifest_digest(self, task_run_id: UUID) -> str | None:
        return self._manifest_digests.get(task_run_id)

    def __reduce_ex__(self, protocol: SupportsIndex) -> Any:
        raise TypeError("Execution credentials cannot enter serialized task context")


_CURRENT: ContextVar[StreamExecutionContext | None] = ContextVar(
    "native_stream_execution", default=None
)


def get_stream_execution() -> StreamExecutionContext | None:
    return _CURRENT.get()


@contextmanager
def stream_execution_context(
    execution: StreamExecutionContext,
) -> Generator[StreamExecutionContext, None, None]:
    """Bind credentials before entering the normal flow engine."""
    token = _CURRENT.set(execution)
    try:
        yield execution
    finally:
        _CURRENT.reset(token)


def stream_execution_headers(
    api_url: str, request_url: str | None = None
) -> dict[str, str]:
    """Attach holder authority only to the exact launching API."""
    execution = get_stream_execution()
    if execution is None:
        return {}
    if api_url.rstrip("/") != execution.api_url:
        return {}
    if request_url is not None and not (
        request_url == execution.api_url
        or request_url.startswith(execution.api_url + "/")
        or request_url.startswith(execution.api_url + "?")
    ):
        return {}
    return {
        "X-Prefect-Stream-Execution-Generation": str(execution.generation),
        "X-Prefect-Stream-Execution-Holder": str(execution.holder_id),
        "X-Prefect-Stream-Execution-Capability": execution.capability,
    }


def apply_stream_execution_headers(
    api_url: str, request_url: str, headers: MutableMapping[str, str]
) -> None:
    """Scope bearer authority on original requests and HTTP redirects."""
    if get_stream_execution() is None:
        return
    for name in (
        "X-Prefect-Stream-Execution-Generation",
        "X-Prefect-Stream-Execution-Holder",
        "X-Prefect-Stream-Execution-Capability",
    ):
        headers.pop(name, None)
    headers.update(stream_execution_headers(api_url, request_url))


def stream_task_event_resource(
    task_run: Any,
    state: Any,
    payload: dict[str, Any],
    resource: dict[str, str],
) -> dict[str, str]:
    """Authenticate a registered native TaskRun without changing its state."""
    execution = get_stream_execution()
    if execution is None:
        return resource
    digest = execution.manifest_digest(task_run.id)
    if digest is None:
        return resource
    if task_run.flow_run_id != execution.flow_run_id:
        raise ValueError("TaskRun does not belong to its execution authority")
    witness = canonical_event_witness(
        event_id=state.id,
        event=f"prefect.task-run.{state.name}",
        occurred=state.timestamp,
        flow_run_id=execution.flow_run_id,
        task_run_id=task_run.id,
        generation=execution.generation,
        holder_id=execution.holder_id,
        manifest_digest=digest,
        run_count=task_run.run_count,
        task_run=payload["task_run"],
        validated_state=payload["validated_state"],
    )
    return resource | {
        "prefect.stream-execution-generation": str(execution.generation),
        "prefect.stream-execution-holder": str(execution.holder_id),
        "prefect.stream-submission-digest": digest,
        "prefect.stream-event-key-id": execution.event_key_id,
        "prefect.stream-event-signature": sign_task_event(
            execution.event_private_key, witness
        ),
    }
