"""Native stream dependency contracts.

Source declarations select the platform HTTP backend. Operator edges carry
their storage handle explicitly; durable publication never stores Python frames.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Protocol,
    TypeVar,
)
from uuid import UUID

from anyio.to_thread import run_sync

from prefect.exceptions import PrefectException

if TYPE_CHECKING:
    from collections.abc import Callable

    from prefect.futures import PrefectFuture

T = TypeVar("T")


def _validate_collection_limit(limit: object) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise StreamProtocolError("Collection requires an explicit positive limit")


class StreamProtocolError(PrefectException):
    """An unsupported stream profile or invalid terminal operation."""


@dataclass(frozen=True)
class StreamTaskConfig:
    """Declare one input port and the terminal-event execution contract.

    The context parameter is injected by the engine and is omitted when calling
    the task. A stream task is still an ordinary Prefect Task and TaskRun.
    """

    input: str
    context: str = "ctx"
    protocol: str = "yield-v2"
    output_cardinality: int = 1
    source: bool = False
    allow_empty: bool = False

    def __post_init__(self) -> None:
        if (
            not self.input
            or not self.context
            or self.input == self.context
            or self.protocol != "yield-v2"
            or self.output_cardinality != 1
            or self.source
            or self.allow_empty
        ):
            raise StreamProtocolError(
                "Only one declared stream input and one terminal yield/return "
                "are supported; source, multi-output and generalized empty-output "
                "profiles require separate protocols."
            )

    def validate_function(self, fn: Callable[..., Any]) -> None:
        parameters = inspect.signature(fn).parameters
        for name in (self.input, self.context):
            if name not in parameters or parameters[name].kind not in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                raise StreamProtocolError(
                    f"Stream parameter {name!r} must be an explicit keyword-capable parameter"
                )
        if "__prefect_stream_run__" in parameters:
            raise StreamProtocolError("Reserved stream execution parameter")

    def bind_parameters(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        signature = inspect.signature(fn)
        exposed = signature.replace(
            parameters=[
                p for name, p in signature.parameters.items() if name != self.context
            ]
        )
        bound = exposed.bind(*args, **kwargs)
        bound.apply_defaults()
        return {**bound.arguments, self.context: None}


@dataclass(frozen=True)
class StreamSourceConfig:
    """Declare a finite, positioned source generator inside an ordinary TaskRun.

    One generator lives for an attempt. A retry creates a fresh generator with
    the durable accepted position; Python frames are never recovery authority.
    """

    context: str = "ctx"
    protocol: str = "source-v1"
    bounded: bool = True

    def __post_init__(self) -> None:
        if not self.context or self.protocol != "source-v1" or self.bounded is not True:
            raise StreamProtocolError("Only a finite source-v1 generator is supported")

    def validate_function(self, fn: Callable[..., Any]) -> None:
        if not (inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn)):
            raise StreamProtocolError("Source tasks require a sync or async generator")
        parameters = inspect.signature(fn).parameters
        if self.context not in parameters or parameters[self.context].kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise StreamProtocolError(
                "A source needs an explicit keyword-capable context"
            )
        if "__prefect_stream_run__" in parameters:
            raise StreamProtocolError("Reserved stream execution parameter")

    def bind_parameters(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        signature = inspect.signature(fn)
        exposed = signature.replace(
            parameters=[
                p for name, p in signature.parameters.items() if name != self.context
            ]
        )
        bound = exposed.bind(*args, **kwargs)
        bound.apply_defaults()
        return {**bound.arguments, self.context: None}


@dataclass(frozen=True)
class SourceRecord(Generic[T]):
    """One stable, contiguous source position and a small JSON value/reference."""

    position: int
    value: T

    def __post_init__(self) -> None:
        if type(self.position) is not int or self.position < 1:
            raise StreamProtocolError("A source position must be a positive integer")


@dataclass
class SourceContext:
    """Injected source metadata; only platform acceptance advances position."""

    task_run_id: UUID
    stream_id: str
    attempt: int
    accepted_source_position: int


@dataclass
class StreamContext:
    """Per-input context; managed state is saved with terminal acceptance."""

    task_run_id: UUID
    consumer_id: str
    input_sequence: int
    attempt: int
    managed_state: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())


@dataclass(frozen=True)
class StreamRecord(Generic[T]):
    """An immutable input coordinate and a storage-owned delivery token."""

    sequence: int
    value: T
    managed_state: dict[str, Any]
    token: Any


@dataclass(frozen=True)
class StreamStatus:
    state: Literal["OPEN", "COMPLETED", "FAILED"]
    error: str | None = None


class StreamStorage(Protocol):
    """Internal boundary for a fenced, atomic stream storage implementation.

    Implementations must retain the same consumer and input/output binding
    across task retries, fence prior owners, and durably freeze accepted values.
    A lost completion acknowledgement must be recoverable from stored progress.
    Prefix reads require a retained, immutable prefix for the requested bound.
    """

    async def create_stream(self, stream_id: str) -> None: ...

    async def open_consumer(
        self,
        input_id: str,
        output_id: str,
        consumer_id: str,
        owner_id: str,
        attempt: int,
    ) -> Any:
        """Open or exactly reconcile a binding across TaskRun retries.

        A completed output can be reconciled only with its existing exact
        binding and fully drained immutable input tail. It must never reopen or
        admit new records. A failed output cannot become a successful one.
        """
        ...

    async def read(self, binding: Any) -> StreamRecord[Any] | None: ...

    async def complete(
        self,
        binding: Any,
        record: StreamRecord[Any],
        kind: Literal["YIELD", "RETURN"],
        value: Any,
        managed_state: dict[str, Any],
    ) -> None: ...

    async def finish(self, stream_id: str, error: str | None = None) -> None: ...

    async def status(self, stream_id: str) -> StreamStatus: ...

    async def read_prefix(self, stream_id: str, limit: int) -> list[Any]: ...


class Stream(Generic[T]):
    """Opaque native stream edge, separate from a finite task future.

    A handle does not promise a final materialized result. Use an explicit
    bounded collection for a finite dependency, or wait for its TaskRun.
    """

    dependency_kind: Literal["stream"] = "stream"

    def __init__(
        self,
        storage: StreamStorage,
        stream_id: str,
        *,
        producer_task_run_id: UUID | None = None,
    ) -> None:
        if not stream_id:
            raise StreamProtocolError("A stream needs an explicit stable identity")
        self._storage = storage
        self._stream_id = stream_id
        self._producer_task_run_id = producer_task_run_id
        self.completion: PrefectFuture[Any] | None = None

    @property
    def storage(self) -> StreamStorage:
        return self._storage

    @property
    def stream_id(self) -> str:
        return self._stream_id

    @property
    def producer_task_run_id(self) -> UUID | None:
        return self._producer_task_run_id

    def _bind_completion(self, future: PrefectFuture[Any]) -> None:
        if self.completion is not None:
            raise StreamProtocolError("Stream producer identity is already bound")
        if self._producer_task_run_id not in (None, future.task_run_id):
            raise StreamProtocolError("Stream future changed its reserved TaskRun id")
        self._producer_task_run_id = future.task_run_id
        self.completion = future

    def collect(self, *, limit: int) -> StreamCollection[T]:
        """Declare a finite prefix bound; a closed stream can contain fewer items."""
        _validate_collection_limit(limit)
        return StreamCollection(self, limit)

    def wait(self, timeout: float | None = None) -> Any:
        """Wait for the operator TaskRun and propagate its final failure."""
        if self.completion is None:
            raise StreamProtocolError("This input handle has no operator completion")
        from prefect._internal.stream_reconstruction import seal_declared_graph

        seal_declared_graph()
        return self.completion.result(timeout=timeout)


class StreamCollection(Generic[T]):
    """An explicitly bounded finite dependency on a stream prefix."""

    dependency_kind: Literal["stream_collection"] = "stream_collection"

    def __init__(self, stream: Stream[T], limit: int) -> None:
        _validate_collection_limit(limit)
        self.stream = stream
        self.limit = limit

    async def resolve(self) -> list[T]:
        from prefect._internal.stream_reconstruction import seal_declared_graph

        await run_sync(seal_declared_graph)
        while True:
            values = await self.stream.storage.read_prefix(
                self.stream.stream_id, self.limit
            )
            if len(values) >= self.limit:
                return values
            status = await self.stream.storage.status(self.stream.stream_id)
            if status.state == "FAILED":
                raise StreamProtocolError(f"Upstream stream failed: {status.error}")
            if status.state == "COMPLETED":
                # Re-read after the terminal marker to include its committed tail.
                return await self.stream.storage.read_prefix(
                    self.stream.stream_id, self.limit
                )
            await asyncio.sleep(0.01)


@dataclass(frozen=True)
class StreamRunResult:
    """Bounded TaskRun result; per-record results live in stream storage."""

    consumer_id: str
    output_stream_id: str


@dataclass(frozen=True)
class SourceRunResult:
    """Bounded source TaskRun result; records remain in durable storage."""

    task_run_id: UUID
    stream_id: str
    accepted_source_position: int
