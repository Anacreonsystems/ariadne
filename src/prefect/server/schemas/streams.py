"""Versioned, wire-safe native stream storage contracts."""

from typing import ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class _StreamRequest(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")


class StreamLimits(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        frozen=True, extra="forbid", strict=True
    )

    max_records: int = Field(default=10_000, ge=1, le=1_000_000)
    max_bytes: int = Field(default=64 * 1024 * 1024, ge=1, le=1024 * 1024 * 1024)
    max_record_bytes: int = Field(default=16_384, ge=1, le=65_536)
    max_state_bytes: int = Field(default=16_384, ge=1, le=65_536)
    max_consumers: int = Field(default=64, ge=1, le=128)
    max_prefix_records: int = Field(default=1000, ge=1, le=10_000)
    max_prefix_bytes: int = Field(default=1024 * 1024, ge=1, le=16 * 1024 * 1024)


class SourceBinding(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    stream_id: UUID
    task_run_id: UUID
    owner_id: UUID
    attempt: int = Field(ge=1, strict=True)
    owner_epoch: int = Field(ge=1, strict=True)


class ConsumerBinding(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    consumer_id: UUID
    input_stream_id: UUID
    output_stream_id: UUID
    owner_id: UUID
    attempt: int = Field(ge=1, strict=True)
    owner_epoch: int = Field(ge=1, strict=True)


class DeliveryToken(ConsumerBinding):
    record_id: UUID
    sequence: int = Field(ge=1, strict=True)
    delivery_id: UUID


class StreamDelivery(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    token: DeliveryToken
    value: JsonValue
    managed_state: dict[str, JsonValue]


class StreamEnvelope(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, from_attributes=True)

    id: UUID
    stream_id: UUID
    sequence: int
    producer_position: int
    value: JsonValue
    value_digest: str
    size_bytes: int


class StreamInfo(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, from_attributes=True)

    id: UUID
    flow_run_id: UUID
    protocol_version: Literal[1]
    state: Literal["OPEN", "COMPLETED", "FAILED"]
    error: str | None
    last_sequence: int
    tail_sequence: int | None
    producer_position: int
    record_count: int
    byte_count: int
    limits: StreamLimits
    released: bool


class StreamCompletionResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    input_sequence: int
    output_sequence: int | None


class StreamCreateRequest(_StreamRequest):
    flow_run_id: UUID
    stream_id: UUID
    protocol_version: Literal[1] = 1
    limits: StreamLimits | None = None

    @field_validator("protocol_version", mode="before")
    @classmethod
    def protocol_version_is_an_integer(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("protocol_version must be the integer 1")
        return value


class StreamOutputReserveRequest(StreamCreateRequest):
    task_run_id: UUID


class StreamOutputRejectRequest(_StreamRequest):
    flow_run_id: UUID
    stream_id: UUID
    task_run_id: UUID
    error: str = Field(min_length=1, max_length=4096)


class StreamOutputFailRequest(StreamOutputRejectRequest):
    attempt: int = Field(ge=1, strict=True)


class StreamConsumerOpenRequest(_StreamRequest):
    input_stream_id: UUID
    output_stream_id: UUID
    task_run_id: UUID
    owner_id: UUID
    attempt: int = Field(ge=1, strict=True)
    protocol_version: Literal[1] = 1
    terminal_protocol: Literal["yield-v2"] = "yield-v2"

    @field_validator("protocol_version", mode="before")
    @classmethod
    def protocol_version_is_an_integer(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("protocol_version must be the integer 1")
        return value


class StreamConsumerReadRequest(_StreamRequest):
    binding: ConsumerBinding


class StreamConsumerCompleteRequest(StreamConsumerReadRequest):
    token: DeliveryToken
    kind: Literal["YIELD", "RETURN"]
    value: JsonValue
    managed_state: dict[str, JsonValue]


class StreamConsumerFinishRequest(StreamConsumerReadRequest):
    error: str | None = Field(default=None, min_length=1, max_length=4096)


class StreamSourceClaimRequest(_StreamRequest):
    stream_id: UUID
    task_run_id: UUID
    owner_id: UUID
    attempt: int = Field(ge=1, strict=True)


class StreamSourceAppendRequest(_StreamRequest):
    binding: SourceBinding
    position: int = Field(ge=1, strict=True)
    value: JsonValue


class StreamSourceFinishRequest(_StreamRequest):
    binding: SourceBinding
    error: str | None = Field(default=None, min_length=1, max_length=4096)


class StreamReleaseRequest(_StreamRequest):
    flow_run_id: UUID


class StreamReleaseResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    released: int = Field(ge=0)
