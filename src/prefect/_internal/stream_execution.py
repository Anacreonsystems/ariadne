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
from typing import Any, ClassVar, Literal, SupportsIndex
from urllib.parse import urlsplit
from uuid import UUID

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prefect._internal.stream_event_auth import (
    canonical_event_witness,
    canonical_physical_event_witness,
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


@dataclass(frozen=True)
class SubtaskExecutionContext:
    """One nonserializable physical grant; never a parent execution credential."""

    flow_run_id: UUID
    graph_revision_id: UUID
    assignment_epoch: int
    subtask_id: UUID
    grant_id: UUID
    physical_epoch: int
    holder_id: UUID
    scope_digest: str
    manifest_digest: str
    task_run_id: UUID
    input_stream_id: UUID | None
    output_stream_id: UUID
    child_process_id: int
    process_start_ticks: int
    api_url: str
    capability: str = field(repr=False)
    event_private_key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.assignment_epoch) is not int or self.assignment_epoch != 1:
            raise ValueError("The fixed physical profile requires assignment epoch 1")
        if type(self.physical_epoch) is not int or not 1 <= self.physical_epoch <= 64:
            raise ValueError("A physical epoch must be between 1 and 64")
        if any(
            type(value) is not int or value < 1
            for value in (self.child_process_id, self.process_start_ticks)
        ):
            raise ValueError("Physical process identity requires positive integers")
        for name in (
            "flow_run_id",
            "graph_revision_id",
            "subtask_id",
            "grant_id",
            "holder_id",
            "task_run_id",
            "output_stream_id",
        ):
            if not isinstance(getattr(self, name), UUID):
                raise ValueError("Physical identities require UUIDs")
        if self.input_stream_id is not None and type(self.input_stream_id) is not UUID:
            raise ValueError("Physical input identity requires a UUID")
        if self.input_stream_id == self.output_stream_id:
            raise ValueError("Physical input and output partitions must differ")
        if any(
            type(value) is not str or not _HEX.fullmatch(value)
            for value in (self.capability, self.scope_digest, self.manifest_digest)
        ):
            raise ValueError("Physical authority requires canonical opaque digests")
        if (
            type(self.event_private_key) is not bytes
            or len(self.event_private_key) != 32
        ):
            raise ValueError("A physical event signing key must contain 32 bytes")
        parsed = urlsplit(self.api_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or "%" in parsed.path
            or "//" in parsed.path
        ):
            raise ValueError("Physical routing requires a credential-free API URL")
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

    def __reduce_ex__(self, protocol: SupportsIndex) -> Any:
        raise TypeError("Physical credentials cannot enter serialized task context")


@dataclass(frozen=True)
class DAGSubtaskExecutionContext(SubtaskExecutionContext):
    """One DAG child grant with a separate, closed HTTP capability surface.

    The server independently derives the persisted profile. This local marker
    prevents a DAG process from attempting the legacy data or lease protocol.
    """

    profile: ClassVar[Literal["partitioned-dag-v1"]] = "partitioned-dag-v1"


_CURRENT_SUBTASK: ContextVar[SubtaskExecutionContext | None] = ContextVar(
    "native_stream_subtask_execution", default=None
)


def get_stream_subtask_execution() -> SubtaskExecutionContext | None:
    return _CURRENT_SUBTASK.get()


@contextmanager
def stream_subtask_execution_context(
    execution: SubtaskExecutionContext,
) -> Generator[SubtaskExecutionContext, None, None]:
    """Enter a child grant without inheriting a flow-wide holder."""
    if _CURRENT.get() is not None:
        raise ValueError("A physical child cannot inherit a parent execution holder")
    token = _CURRENT_SUBTASK.set(execution)
    try:
        yield execution
    finally:
        _CURRENT_SUBTASK.reset(token)


def get_stream_execution() -> StreamExecutionContext | None:
    return _CURRENT.get()


@contextmanager
def stream_execution_context(
    execution: StreamExecutionContext,
) -> Generator[StreamExecutionContext, None, None]:
    """Bind credentials before entering the normal flow engine."""
    if _CURRENT_SUBTASK.get() is not None:
        raise ValueError("A physical child cannot enter a parent execution holder")
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
    api_url: str,
    request_url: str,
    headers: MutableMapping[str, str],
    method: str | None = None,
) -> None:
    """Scope bearer authority on original requests and HTTP redirects."""
    subtask = get_stream_subtask_execution()
    if subtask is not None:
        for name in tuple(headers):
            if name.lower() in {
                "authorization",
                "proxy-authorization",
                "cookie",
                "prefect-csrf-token",
                "prefect-csrf-client",
                "x-prefect-subtask-grant",
            } or name.lower().startswith("x-prefect-stream-execution-"):
                del headers[name]
        headers.update(stream_subtask_headers(api_url, request_url, method))
        return
    if get_stream_execution() is None:
        return
    for name in (
        "X-Prefect-Stream-Execution-Generation",
        "X-Prefect-Stream-Execution-Holder",
        "X-Prefect-Stream-Execution-Capability",
    ):
        headers.pop(name, None)
    headers.update(stream_execution_headers(api_url, request_url))


def stream_subtask_headers(
    api_url: str,
    request_url: str,
    method: str | None,
) -> dict[str, str]:
    """Refuse off-scope routes before transporting a physical capability."""
    subtask = get_stream_subtask_execution()
    if subtask is None:
        return {}
    target = urlsplit(request_url)
    api = urlsplit(subtask.api_url)
    if (
        api_url.rstrip("/") != subtask.api_url
        or (target.scheme, target.netloc) != (api.scheme, api.netloc)
        or target.username is not None
        or target.password is not None
        or target.fragment
    ):
        raise ValueError("Physical credentials cannot leave their launching API")
    child = f"{api.path}/streams/v2/subtasks/{subtask.subtask_id}"
    if isinstance(subtask, DAGSubtaskExecutionContext):
        if target.query:
            raise ValueError("DAG child routing does not accept query parameters")
        dag_child = f"{api.path}/streams/v2/dags/subtasks/{subtask.subtask_id}"
        allowed_get = {
            dag_child + "/bootstrap",
            dag_child + "/activation",
            dag_child + "/output/status",
        }
        role = "sources" if subtask.input_stream_id is None else "consumers"
        if subtask.input_stream_id is not None:
            allowed_get.add(dag_child + "/input/status")
        allowed_post = {
            dag_child + "/bootstrap-ready",
            child + "/events",
            *(
                f"{api.path}/streams/v2/dags/{role}/{operation}"
                for operation in (
                    ("open", "append", "finish")
                    if role == "sources"
                    else ("open", "read", "complete", "finish")
                )
            ),
        }
    else:
        allowed_get = {child + "/bootstrap", child + "/task-run"}
        for stream_id in (subtask.input_stream_id, subtask.output_stream_id):
            if stream_id is not None:
                allowed_get.update(
                    {
                        f"{api.path}/streams/v1/{stream_id}",
                        f"{api.path}/streams/v1/{stream_id}/prefix",
                    }
                )
        allowed_post = {
            child + "/events",
            child + "/lease/renew",
            child + "/lease/release",
            *(
                f"{api.path}/streams/v1/{route}"
                for route in (
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
                )
            ),
        }
    if not (
        (method == "GET" and target.path in allowed_get)
        or (method == "POST" and target.path in allowed_post)
    ):
        raise ValueError("Physical credentials cannot access this API route")
    return {
        "Authorization": f"PrefectSubtask {subtask.capability}",
        "X-Prefect-Subtask-Grant": str(subtask.grant_id),
    }


def stream_task_event_resource(
    task_run: Any,
    state: Any,
    payload: dict[str, Any],
    resource: dict[str, str],
) -> dict[str, str]:
    """Authenticate a registered native TaskRun without changing its state."""
    subtask = get_stream_subtask_execution()
    if subtask is not None:
        if (
            task_run.id != subtask.task_run_id
            or task_run.flow_run_id != subtask.flow_run_id
        ):
            raise ValueError("TaskRun does not belong to its physical authority")
        witness = canonical_physical_event_witness(
            event_id=state.id,
            event=f"prefect.task-run.{state.name}",
            occurred=state.timestamp,
            flow_run_id=subtask.flow_run_id,
            task_run_id=task_run.id,
            generation=subtask.physical_epoch,
            holder_id=subtask.holder_id,
            manifest_digest=subtask.manifest_digest,
            run_count=task_run.run_count,
            task_run=payload["task_run"],
            validated_state=payload["validated_state"],
            graph_revision_id=subtask.graph_revision_id,
            assignment_epoch=subtask.assignment_epoch,
            subtask_id=subtask.subtask_id,
            grant_id=subtask.grant_id,
            physical_epoch=subtask.physical_epoch,
            scope_digest=subtask.scope_digest,
        )
        return resource | {
            "prefect.stream-graph-revision": str(subtask.graph_revision_id),
            "prefect.stream-assignment-epoch": str(subtask.assignment_epoch),
            "prefect.stream-subtask": str(subtask.subtask_id),
            "prefect.stream-subtask-grant": str(subtask.grant_id),
            "prefect.stream-physical-epoch": str(subtask.physical_epoch),
            "prefect.stream-scope-digest": subtask.scope_digest,
            "prefect.stream-execution-generation": str(subtask.physical_epoch),
            "prefect.stream-execution-holder": str(subtask.holder_id),
            "prefect.stream-submission-digest": subtask.manifest_digest,
            "prefect.stream-event-key-id": subtask.event_key_id,
            "prefect.stream-event-signature": sign_task_event(
                subtask.event_private_key, witness
            ),
        }
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
