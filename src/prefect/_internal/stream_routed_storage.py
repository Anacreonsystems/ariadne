"""DAG-only terminal intents; public execution remains separately fenced.

The ordinary source/operator loops already distinguish source positions and
YIELD/RETURN. This adapter freezes their intent before transport and leaves the
legacy storage implementation and its wire bodies unchanged. Routing targets
belong exclusively to the sealed server plan.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Literal, cast
from uuid import UUID

import anyio
import httpx
from pydantic import ValidationError

from prefect._internal.stream_key_wire import encode_tagged_key_wire_v1
from prefect._internal.stream_routing import KeyEncodingError
from prefect.client.schemas.stream_routes import (
    StreamDAGConsumerCompleteRequest,
    StreamDAGRouteContext,
    StreamDAGSourceAppendRequest,
)
from prefect.client.schemas.streams import ConsumerBinding, DeliveryToken, SourceBinding
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.streams import KeyedStreamOutput, StreamProtocolError, StreamRecord

_RouteMethod = Literal["append_stream_dag_source", "complete_stream_dag_consumer"]
_RouteIntent = StreamDAGSourceAppendRequest | StreamDAGConsumerCompleteRequest


def _freeze(intent: _RouteIntent | StreamDAGRouteContext) -> bytes:
    return json.dumps(
        intent.model_dump(mode="json"),
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8", errors="strict")


def _output(value: Any, *, keyed: bool) -> tuple[Any, dict[str, Any] | None]:
    if type(value) is KeyedStreamOutput:
        wrapped = cast(KeyedStreamOutput[Any], value)
        if not keyed:
            raise StreamProtocolError(
                "A keyed output requires a sealed keyed outgoing edge"
            )
        try:
            return wrapped.value, dict(encode_tagged_key_wire_v1(wrapped.key))
        except KeyEncodingError as error:
            raise StreamProtocolError(
                "A DAG output key must satisfy tagged-key-v1"
            ) from error
    if isinstance(value, KeyedStreamOutput):
        raise StreamProtocolError("A DAG requires the exact KeyedStreamOutput wrapper")
    if keyed:
        raise StreamProtocolError(
            "A sealed keyed outgoing edge requires KeyedStreamOutput"
        )
    return value, None


@dataclass(frozen=True, init=False)
class PrefectRoutedStreamStorage(PrefectStreamStorage):
    """Client-safe terminal adapter with a private immutable routing witness.

    This internal handle does not enable DAG admission, task bootstrap or Flow
    lifecycle operations. It adds only the two separately versioned terminal
    requests. No client-provided target list or producer sequence is accepted.
    """

    _context_bytes: bytes = field(repr=False)

    def __init__(
        self,
        flow_run_id: UUID,
        api_url: str | None = None,
        *,
        route_context: StreamDAGRouteContext,
        admission_timeout: float = 20.0,
        poll_interval: float = 0.05,
    ) -> None:
        super().__init__(flow_run_id, api_url, admission_timeout, poll_interval)
        try:
            context = StreamDAGRouteContext.model_validate(route_context)
            object.__setattr__(self, "_context_bytes", _freeze(context))
        except (ValidationError, ValueError, TypeError, RecursionError) as error:
            raise StreamProtocolError("Invalid sealed DAG routing context") from error

    @property
    def route_context(self) -> StreamDAGRouteContext:
        """Return a detached projection, never the owned routing witness."""
        return StreamDAGRouteContext.model_validate_json(self._context_bytes)

    async def _send_frozen(self, method: _RouteMethod, frozen: bytes) -> None:
        deadline = time.monotonic() + self.admission_timeout
        while True:
            # Fresh objects on every attempt also protect the retained intent
            # against a client callback mutating its received arguments.
            if method == "append_stream_dag_source":
                source = StreamDAGSourceAppendRequest.model_validate_json(frozen)
                arguments = {
                    **source.binding.model_dump(),
                    "context": source.context.model_dump(mode="json"),
                    "position": source.position,
                    "value": source.value,
                    "output_key": (
                        source.output_key.model_dump(mode="json")
                        if source.output_key is not None
                        else None
                    ),
                }
            else:
                consumer = StreamDAGConsumerCompleteRequest.model_validate_json(frozen)
                arguments = {
                    **consumer.token.model_dump(),
                    "context": consumer.context.model_dump(mode="json"),
                    "kind": consumer.kind,
                    "value": consumer.value,
                    "managed_state": consumer.managed_state,
                    "output_key": (
                        consumer.output_key.model_dump(mode="json")
                        if consumer.output_key is not None
                        else None
                    ),
                }
            try:
                await self._dispatch(method, **arguments)
                return
            except httpx.TransportError:
                if time.monotonic() >= deadline:
                    raise
            await anyio.sleep(self.poll_interval)

    async def append_source(
        self, binding: SourceBinding, position: int, value: Any
    ) -> None:
        context = self.route_context
        if binding.flow_run_id != self.flow_run_id:
            raise StreamProtocolError("DAG source belongs to another FlowRun")
        payload, key = _output(value, keyed=context.has_keyed_outgoing)
        try:
            intent = StreamDAGSourceAppendRequest.model_validate(
                {
                    "context": context,
                    "binding": binding,
                    "position": position,
                    "value": payload,
                    "output_key": key,
                }
            )
            frozen = _freeze(intent)
        except (ValidationError, ValueError, TypeError, RecursionError) as error:
            raise StreamProtocolError(
                "Invalid finite JSON DAG source intent"
            ) from error
        await self._send_frozen("append_stream_dag_source", frozen)

    async def complete(
        self,
        binding: ConsumerBinding,
        record: StreamRecord[Any],
        kind: Literal["YIELD", "RETURN"],
        value: Any,
        managed_state: dict[str, Any],
    ) -> None:
        context = self.route_context
        if binding.flow_run_id != self.flow_run_id:
            raise StreamProtocolError("DAG consumer belongs to another FlowRun")
        if kind == "RETURN":
            if value is not None:
                raise StreamProtocolError("RETURN has no output value or key")
            payload, key = None, None
        else:
            payload, key = _output(value, keyed=context.has_keyed_outgoing)
        try:
            token = DeliveryToken.model_validate(record.token)
            if type(record.sequence) is not int or record.sequence != token.sequence:
                raise StreamProtocolError("DAG record and delivery token differ")
            intent = StreamDAGConsumerCompleteRequest.model_validate(
                {
                    "context": context,
                    "binding": binding,
                    "token": token,
                    "kind": kind,
                    "value": payload,
                    "managed_state": managed_state,
                    "output_key": key,
                }
            )
            frozen = _freeze(intent)
        except (ValidationError, ValueError, TypeError, RecursionError) as error:
            raise StreamProtocolError(
                "Invalid finite JSON DAG terminal intent"
            ) from error
        await self._send_frozen("complete_stream_dag_consumer", frozen)
