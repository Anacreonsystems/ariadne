"""A client-safe native storage handle; live clients stay in engine contexts."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

import anyio
import httpx
from anyio.to_thread import run_sync
from pydantic import JsonValue

from prefect.client.orchestration import get_client
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.client.schemas.streams import (
    ConsumerBinding,
    DeliveryToken,
    SourceAdmission,
    SourceBinding,
)
from prefect.context import AsyncClientContext, SyncClientContext
from prefect.settings import PREFECT_API_URL
from prefect.streams import StreamProtocolError, StreamRecord, StreamStatus


@dataclass(frozen=True)
class PrefectStreamStorage:
    """HTTP routing/config only, suitable for native Task parameters.

    Task engines own client pools and credentials. Async tasks reuse their
    loop's client; sync tasks reuse their context's thread-safe client through
    a worker thread. No live client, pool, token or credential is pickled here.
    Native source-task submission selects this handle from its FlowRun.
    """

    flow_run_id: UUID
    api_url: str | None = None
    admission_timeout: float = 20.0
    poll_interval: float = 0.05

    def __post_init__(self) -> None:
        url = self.api_url or PREFECT_API_URL.value()
        if not url:
            raise StreamProtocolError("Native streams require a configured Prefect API")
        parsed = urlsplit(url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise StreamProtocolError(
                "Stream routing must be an API URL without credentials"
            )
        if self.admission_timeout <= 0 or self.poll_interval <= 0:
            raise StreamProtocolError("Native stream deadlines must be positive")
        object.__setattr__(self, "api_url", url.rstrip("/"))

    def _matches(self, client: Any) -> bool:
        return str(client.api_url).rstrip("/") == self.api_url

    async def _dispatch(self, method: str, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        asynchronous = AsyncClientContext.get()
        if (
            asynchronous is not None
            and self._matches(asynchronous.client)
            and getattr(asynchronous.client, "_loop", None) in (None, loop)
        ):
            return await getattr(asynchronous.client, method)(**kwargs)
        synchronous = SyncClientContext.get()
        if synchronous is not None and self._matches(synchronous.client):
            call = partial(getattr(synchronous.client, method), **kwargs)
            return await run_sync(call)
        configured = PREFECT_API_URL.value()
        if not configured or configured.rstrip("/") != self.api_url:
            raise StreamProtocolError(
                "Stream API differs from the active Prefect profile"
            )
        async with get_client() as client:
            if not self._matches(client):
                raise StreamProtocolError("Stream API differs from the active client")
            return await getattr(client, method)(**kwargs)

    async def _request(
        self, method: str, *, metadata_lag: bool = False, **kwargs: Any
    ) -> Any:
        deadline = time.monotonic() + self.admission_timeout
        while True:
            try:
                return await self._dispatch(method, **kwargs)
            except StreamRequestError as error:
                if not (
                    metadata_lag
                    and error.code == "METADATA_NOT_READY"
                    and error.retryable
                ):
                    raise
                if time.monotonic() >= deadline:
                    raise
            except httpx.TransportError:
                if time.monotonic() >= deadline:
                    raise
            await anyio.sleep(self.poll_interval)

    async def create_stream(self, stream_id: str) -> None:
        await self._request(
            "create_stream", flow_run_id=self.flow_run_id, stream_id=UUID(stream_id)
        )

    async def reserve_output(
        self, stream_id: str, task_run_id: UUID, *, source: bool = False
    ) -> None:
        await self._request(
            "reserve_stream_source" if source else "reserve_stream_output",
            flow_run_id=self.flow_run_id,
            stream_id=UUID(stream_id),
            task_run_id=task_run_id,
            metadata_lag=True,
        )

    async def register_task_run(self, task: Any, task_run: Any) -> Any:
        try:
            registered = await self._request(
                "create_task_run",
                task=task,
                flow_run_id=self.flow_run_id,
                dynamic_key=task_run.dynamic_key,
                id=task_run.id,
                name=task_run.name,
                extra_tags=task_run.tags,
                state=task_run.state,
                task_inputs=task_run.task_inputs,
            )
        except httpx.TransportError:
            # A lost create response does not justify another logical TaskRun.
            registered = await self._request("read_task_run", task_run_id=task_run.id)
        if (
            registered.id != task_run.id
            or registered.flow_run_id != self.flow_run_id
            or registered.task_key != task_run.task_key
            or registered.dynamic_key != task_run.dynamic_key
        ):
            raise StreamProtocolError("TaskRun registration changed stream identity")
        return registered

    async def reject_output(
        self, stream_id: str, task_run_id: UUID, error: str, *, source: bool = False
    ) -> None:
        await self._request(
            "reject_stream_source" if source else "reject_stream_output",
            flow_run_id=self.flow_run_id,
            stream_id=UUID(stream_id),
            task_run_id=task_run_id,
            error=error,
        )

    async def open_consumer(
        self,
        input_id: str,
        output_id: str,
        consumer_id: str,
        owner_id: str,
        attempt: int,
    ) -> ConsumerBinding:
        binding = await self._request(
            "open_stream_consumer",
            input_stream_id=UUID(input_id),
            output_stream_id=UUID(output_id),
            task_run_id=UUID(consumer_id),
            owner_id=UUID(owner_id),
            attempt=attempt,
            metadata_lag=True,
        )
        if binding.flow_run_id != self.flow_run_id:
            raise StreamProtocolError("Admitted stream belongs to another FlowRun")
        return binding

    async def read(self, binding: ConsumerBinding) -> StreamRecord[Any] | None:
        delivery = await self._request("read_stream_consumer", **binding.model_dump())
        if delivery is None:
            return None
        return StreamRecord(
            delivery.token.sequence,
            delivery.value,
            delivery.managed_state,
            delivery.token,
        )

    async def complete(
        self,
        binding: ConsumerBinding,
        record: StreamRecord[Any],
        kind: Literal["YIELD", "RETURN"],
        value: Any,
        managed_state: dict[str, Any],
    ) -> None:
        # Freeze the terminal intent before the first await or transport retry.
        try:
            snapshot: dict[str, JsonValue] = json.loads(
                json.dumps({"value": value, "state": managed_state}, allow_nan=False)
            )
        except (TypeError, ValueError, RecursionError) as error:
            raise StreamProtocolError("Terminal intent must be finite JSON") from error
        token = DeliveryToken.model_validate(record.token)
        if any(
            getattr(token, name) != getattr(binding, name)
            for name in ConsumerBinding.model_fields
        ):
            raise StreamProtocolError("Delivery token belongs to another binding")
        await self._request(
            "complete_stream_input",
            **binding.model_dump(),
            record_id=token.record_id,
            sequence=token.sequence,
            delivery_id=token.delivery_id,
            kind=kind,
            value=snapshot["value"],
            managed_state=snapshot["state"],
        )

    async def finish_consumer(
        self, binding: ConsumerBinding, error: str | None = None
    ) -> None:
        await self._request(
            "finish_stream_consumer", **binding.model_dump(), error=error
        )

    async def fail_task_output(
        self,
        stream_id: str,
        task_run_id: UUID,
        attempt: int,
        error: str,
        *,
        source: bool = False,
    ) -> None:
        deadline = time.monotonic() + self.admission_timeout
        while True:
            try:
                await self._request(
                    "fail_stream_source" if source else "fail_stream_task_output",
                    flow_run_id=self.flow_run_id,
                    stream_id=UUID(stream_id),
                    task_run_id=task_run_id,
                    attempt=attempt,
                    error=error,
                )
                return
            except StreamRequestError as refused:
                if refused.code != "ATTEMPT_NOT_CURRENT":
                    raise
                task_run = await self._request("read_task_run", task_run_id=task_run_id)
                if (
                    task_run.flow_run_id != self.flow_run_id
                    or task_run.run_count > attempt
                    or time.monotonic() >= deadline
                ):
                    raise
                await anyio.sleep(self.poll_interval)

    async def finish(self, stream_id: str, error: str | None = None) -> None:
        raise StreamProtocolError(
            "Native output finish requires an explicit binding or lifecycle identity"
        )

    async def open_source(
        self, stream_id: str, task_run_id: UUID, owner_id: UUID, attempt: int
    ) -> SourceAdmission:
        admission = await self._request(
            "open_stream_source",
            stream_id=UUID(stream_id),
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            metadata_lag=True,
        )
        if admission.binding.flow_run_id != self.flow_run_id:
            raise StreamProtocolError("Source belongs to another FlowRun")
        return admission

    async def append_source(
        self, binding: SourceBinding, position: int, value: Any
    ) -> None:
        try:
            snapshot = json.loads(json.dumps(value, allow_nan=False))
        except (TypeError, ValueError, RecursionError) as error:
            raise StreamProtocolError("Source values must be finite JSON") from error
        await self._request(
            "append_stream_source",
            **binding.model_dump(),
            position=position,
            value=snapshot,
        )

    async def close_source(
        self, binding: SourceBinding, expected_position: int
    ) -> None:
        await self._request(
            "close_stream_source",
            **binding.model_dump(),
            expected_position=expected_position,
        )

    async def status(self, stream_id: str) -> StreamStatus:
        info = await self._request("read_stream", stream_id=UUID(stream_id))
        return StreamStatus(info.state, info.error)

    async def read_prefix(self, stream_id: str, limit: int) -> list[Any]:
        records = await self._request(
            "read_stream_prefix", stream_id=UUID(stream_id), limit=limit
        )
        return [record.value for record in records]
