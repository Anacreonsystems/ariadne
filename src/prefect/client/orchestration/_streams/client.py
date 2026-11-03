"""Paired native-stream HTTP clients with typed, non-string-based refusals."""

from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import JsonValue, TypeAdapter

from prefect.client.orchestration.base import HTTP_METHODS, BaseAsyncClient, BaseClient
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import streams as schemas
from prefect.exceptions import PrefectException


class StreamRequestError(PrefectException):
    """A wire refusal; admission may retry only explicit metadata lag."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        status_code: int = 409,
    ) -> None:
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        super().__init__(message)


def _refusal(error: httpx.HTTPStatusError) -> StreamRequestError:
    try:
        detail = error.response.json().get("detail")
    except (ValueError, AttributeError):
        detail = None
    if isinstance(detail, dict) and isinstance(detail.get("code"), str):
        return StreamRequestError(
            detail["code"],
            str(detail.get("message", "Stream request refused")),
            retryable=detail.get("retryable") is True,
            status_code=error.response.status_code,
        )
    code = "UNSUPPORTED_BACKEND" if error.response.status_code == 404 else "HTTP_ERROR"
    return StreamRequestError(
        code, "Native stream API request failed", status_code=error.response.status_code
    )


class StreamClient(BaseClient):
    """Native data-plane methods; credentials belong to the owning client."""

    def _stream_request(
        self, method: HTTP_METHODS, path: ServerRoutes, **kwargs: Any
    ) -> httpx.Response:
        try:
            return self.request(method, path, **kwargs)
        except httpx.HTTPStatusError as error:
            raise _refusal(error) from error

    def reserve_stream_submission(
        self,
        *,
        flow_run_id: UUID,
        submission_key: str,
        role: Literal["SOURCE", "OPERATOR"],
        protocol: Literal["source-v1", "yield-v2"],
        task_name: str,
        task_key: str,
        task_version: str,
        flow_version: str,
        parameters: dict[str, JsonValue],
        input_key: str | None = None,
        upstream_stream_id: UUID | None = None,
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        artifact_digest: str | None = None,
        limits: dict[str, int] | None = None,
        task_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        labels: dict[str, bool | int | float | str] | None = None,
    ) -> schemas.StreamSubmissionManifest:
        """Create or exactly resolve a durable call slot and its native resources."""
        payload = schemas.StreamSubmissionReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "submission_key": submission_key,
                "role": role,
                "protocol": protocol,
                "task_name": task_name,
                "task_key": task_key,
                "task_version": task_version,
                "flow_version": flow_version,
                "parameters": parameters,
                "input_key": input_key,
                "upstream_stream_id": upstream_stream_id,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "artifact_digest": artifact_digest,
                "limits": limits,
                "task_policy": task_policy or {},
                "tags": tags or [],
                "labels": labels or {},
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/submissions/reserve", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    def read_stream_submission(
        self, *, flow_run_id: UUID, submission_key: str
    ) -> schemas.StreamSubmissionManifest:
        """Resolve the original resources without process-local identifiers."""
        payload = schemas.StreamSubmissionLookupRequest(
            flow_run_id=flow_run_id, submission_key=submission_key
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/submissions/lookup", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    def create_stream(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamCreateRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = self._stream_request("POST", "/streams/v1/create", json=payload)
        return schemas.StreamInfo.model_validate(response.json())

    def reserve_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/outputs/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def reject_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputRejectRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "error": error,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/outputs/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def fail_stream_task_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputFailRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "attempt": attempt,
                "error": error,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/outputs/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def open_stream_consumer(
        self,
        *,
        input_stream_id: UUID,
        output_stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        terminal_protocol: str = "yield-v2",
    ) -> schemas.ConsumerBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamConsumerOpenRequest.model_validate(
            {
                "input_stream_id": input_stream_id,
                "output_stream_id": output_stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
                "protocol_version": protocol_version,
                "terminal_protocol": terminal_protocol,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/consumers/open", json=payload
        )
        return schemas.ConsumerBinding.model_validate(response.json())

    def read_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
    ) -> schemas.StreamDelivery | None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerReadRequest.model_validate(
            {"binding": binding}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/consumers/read", json=payload
        )
        return (
            schemas.StreamDelivery.model_validate(response.json())
            if response.json() is not None
            else None
        )

    def complete_stream_input(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        record_id: UUID,
        sequence: int,
        delivery_id: UUID,
        kind: Literal["YIELD", "RETURN"],
        value: JsonValue,
        managed_state: dict[str, JsonValue],
    ) -> schemas.StreamCompletionResult:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        token = schemas.DeliveryToken(
            **binding.model_dump(),
            record_id=record_id,
            sequence=sequence,
            delivery_id=delivery_id,
        )
        payload = schemas.StreamConsumerCompleteRequest.model_validate(
            {
                "binding": binding,
                "token": token,
                "kind": kind,
                "value": value,
                "managed_state": managed_state,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/consumers/complete", json=payload
        )
        return schemas.StreamCompletionResult.model_validate(response.json())

    def finish_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        self._stream_request("POST", "/streams/v1/consumers/finish", json=payload)

    def claim_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
    ) -> schemas.SourceBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamSourceClaimRequest.model_validate(
            {
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/claim", json=payload
        )
        return schemas.SourceBinding.model_validate(response.json())

    def append_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        position: int,
        value: JsonValue,
    ) -> schemas.StreamEnvelope:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceAppendRequest.model_validate(
            {"binding": binding, "position": position, "value": value}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/append", json=payload
        )
        return schemas.StreamEnvelope.model_validate(response.json())

    def finish_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        self._stream_request("POST", "/streams/v1/sources/finish", json=payload)

    def reserve_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceReserveRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                limits=limits,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def reject_stream_source(
        self, *, flow_run_id: UUID, stream_id: UUID, task_run_id: UUID, error: str
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceRejectRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                error=error,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def fail_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceFailRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                attempt=attempt,
                error=error,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def open_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.SourceAdmission:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceOpenRequest.model_validate(
            dict(
                stream_id=stream_id,
                task_run_id=task_run_id,
                owner_id=owner_id,
                attempt=attempt,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/open", json=payload
        )
        return schemas.SourceAdmission.model_validate(response.json())

    def close_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        expected_position: int,
    ) -> None:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceCloseRequest.model_validate(
            dict(
                binding=schemas.SourceBinding(
                    flow_run_id=flow_run_id,
                    stream_id=stream_id,
                    task_run_id=task_run_id,
                    owner_id=owner_id,
                    attempt=attempt,
                    owner_epoch=owner_epoch,
                ),
                expected_position=expected_position,
            )
        ).model_dump(mode="json")
        self._stream_request("POST", "/streams/v1/sources/close", json=payload)

    def release_stream_flow(
        self,
        *,
        flow_run_id: UUID,
    ) -> schemas.StreamReleaseResult:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamReleaseRequest.model_validate(
            {"flow_run_id": flow_run_id}
        ).model_dump(mode="json")
        response = self._stream_request("POST", "/streams/v1/release", json=payload)
        return schemas.StreamReleaseResult.model_validate(response.json())

    def read_stream(self, stream_id: UUID) -> schemas.StreamInfo:
        response = self._stream_request(
            "GET", "/streams/v1/{stream_id}", path_params={"stream_id": stream_id}
        )
        return schemas.StreamInfo.model_validate(response.json())

    def read_stream_prefix(
        self, stream_id: UUID, *, limit: int
    ) -> list[schemas.StreamEnvelope]:
        response = self._stream_request(
            "GET",
            "/streams/v1/{stream_id}/prefix",
            path_params={"stream_id": stream_id},
            params={"limit": limit},
        )
        return TypeAdapter(list[schemas.StreamEnvelope]).validate_python(
            response.json()
        )


class StreamAsyncClient(BaseAsyncClient):
    """Native data-plane methods; credentials belong to the owning client."""

    async def _stream_request(
        self, method: HTTP_METHODS, path: ServerRoutes, **kwargs: Any
    ) -> httpx.Response:
        try:
            return await self.request(method, path, **kwargs)
        except httpx.HTTPStatusError as error:
            raise _refusal(error) from error

    async def reserve_stream_submission(
        self,
        *,
        flow_run_id: UUID,
        submission_key: str,
        role: Literal["SOURCE", "OPERATOR"],
        protocol: Literal["source-v1", "yield-v2"],
        task_name: str,
        task_key: str,
        task_version: str,
        flow_version: str,
        parameters: dict[str, JsonValue],
        input_key: str | None = None,
        upstream_stream_id: UUID | None = None,
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        artifact_digest: str | None = None,
        limits: dict[str, int] | None = None,
        task_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        labels: dict[str, bool | int | float | str] | None = None,
    ) -> schemas.StreamSubmissionManifest:
        """Create or exactly resolve a durable call slot and its native resources."""
        payload = schemas.StreamSubmissionReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "submission_key": submission_key,
                "role": role,
                "protocol": protocol,
                "task_name": task_name,
                "task_key": task_key,
                "task_version": task_version,
                "flow_version": flow_version,
                "parameters": parameters,
                "input_key": input_key,
                "upstream_stream_id": upstream_stream_id,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "artifact_digest": artifact_digest,
                "limits": limits,
                "task_policy": task_policy or {},
                "tags": tags or [],
                "labels": labels or {},
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/submissions/reserve", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    async def read_stream_submission(
        self, *, flow_run_id: UUID, submission_key: str
    ) -> schemas.StreamSubmissionManifest:
        """Resolve the original resources without process-local identifiers."""
        payload = schemas.StreamSubmissionLookupRequest(
            flow_run_id=flow_run_id, submission_key=submission_key
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/submissions/lookup", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    async def create_stream(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamCreateRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/create", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def reserve_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/outputs/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def reject_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputRejectRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "error": error,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/outputs/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def fail_stream_task_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputFailRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "attempt": attempt,
                "error": error,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/outputs/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def open_stream_consumer(
        self,
        *,
        input_stream_id: UUID,
        output_stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        terminal_protocol: str = "yield-v2",
    ) -> schemas.ConsumerBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamConsumerOpenRequest.model_validate(
            {
                "input_stream_id": input_stream_id,
                "output_stream_id": output_stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
                "protocol_version": protocol_version,
                "terminal_protocol": terminal_protocol,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/consumers/open", json=payload
        )
        return schemas.ConsumerBinding.model_validate(response.json())

    async def read_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
    ) -> schemas.StreamDelivery | None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerReadRequest.model_validate(
            {"binding": binding}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/consumers/read", json=payload
        )
        return (
            schemas.StreamDelivery.model_validate(response.json())
            if response.json() is not None
            else None
        )

    async def complete_stream_input(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        record_id: UUID,
        sequence: int,
        delivery_id: UUID,
        kind: Literal["YIELD", "RETURN"],
        value: JsonValue,
        managed_state: dict[str, JsonValue],
    ) -> schemas.StreamCompletionResult:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        token = schemas.DeliveryToken(
            **binding.model_dump(),
            record_id=record_id,
            sequence=sequence,
            delivery_id=delivery_id,
        )
        payload = schemas.StreamConsumerCompleteRequest.model_validate(
            {
                "binding": binding,
                "token": token,
                "kind": kind,
                "value": value,
                "managed_state": managed_state,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/consumers/complete", json=payload
        )
        return schemas.StreamCompletionResult.model_validate(response.json())

    async def finish_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        await self._stream_request("POST", "/streams/v1/consumers/finish", json=payload)

    async def claim_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
    ) -> schemas.SourceBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamSourceClaimRequest.model_validate(
            {
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/claim", json=payload
        )
        return schemas.SourceBinding.model_validate(response.json())

    async def append_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        position: int,
        value: JsonValue,
    ) -> schemas.StreamEnvelope:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceAppendRequest.model_validate(
            {"binding": binding, "position": position, "value": value}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/append", json=payload
        )
        return schemas.StreamEnvelope.model_validate(response.json())

    async def finish_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        await self._stream_request("POST", "/streams/v1/sources/finish", json=payload)

    async def reserve_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceReserveRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                limits=limits,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def reject_stream_source(
        self, *, flow_run_id: UUID, stream_id: UUID, task_run_id: UUID, error: str
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceRejectRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                error=error,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def fail_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceFailRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                attempt=attempt,
                error=error,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def open_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.SourceAdmission:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceOpenRequest.model_validate(
            dict(
                stream_id=stream_id,
                task_run_id=task_run_id,
                owner_id=owner_id,
                attempt=attempt,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/open", json=payload
        )
        return schemas.SourceAdmission.model_validate(response.json())

    async def close_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        expected_position: int,
    ) -> None:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceCloseRequest.model_validate(
            dict(
                binding=schemas.SourceBinding(
                    flow_run_id=flow_run_id,
                    stream_id=stream_id,
                    task_run_id=task_run_id,
                    owner_id=owner_id,
                    attempt=attempt,
                    owner_epoch=owner_epoch,
                ),
                expected_position=expected_position,
            )
        ).model_dump(mode="json")
        await self._stream_request("POST", "/streams/v1/sources/close", json=payload)

    async def release_stream_flow(
        self,
        *,
        flow_run_id: UUID,
    ) -> schemas.StreamReleaseResult:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamReleaseRequest.model_validate(
            {"flow_run_id": flow_run_id}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/release", json=payload
        )
        return schemas.StreamReleaseResult.model_validate(response.json())

    async def read_stream(self, stream_id: UUID) -> schemas.StreamInfo:
        response = await self._stream_request(
            "GET", "/streams/v1/{stream_id}", path_params={"stream_id": stream_id}
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def read_stream_prefix(
        self, stream_id: UUID, *, limit: int
    ) -> list[schemas.StreamEnvelope]:
        response = await self._stream_request(
            "GET",
            "/streams/v1/{stream_id}/prefix",
            path_params={"stream_id": stream_id},
            params={"limit": limit},
        )
        return TypeAdapter(list[schemas.StreamEnvelope]).validate_python(
            response.json()
        )
