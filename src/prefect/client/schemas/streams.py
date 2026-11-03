"""Client-only mirror of native stream wire contracts; no server imports."""

from typing import ClassVar, Literal, Union
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    StrictFloat,
    StrictInt,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from prefect.client.schemas.objects import TaskRunPolicy
from prefect.types import DateTime

KeyValueLabels = dict[str, Union[StrictBool, StrictInt, StrictFloat, str]]


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


class SourceAdmission(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    binding: SourceBinding
    state: Literal["OPEN", "COMPLETED"]
    accepted_source_position: int = Field(ge=0, strict=True)


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


class StreamSourceReserveRequest(StreamCreateRequest):
    task_run_id: UUID
    source_protocol: Literal["source-v1"] = "source-v1"


class StreamSourceRejectRequest(_StreamRequest):
    flow_run_id: UUID
    stream_id: UUID
    task_run_id: UUID
    error: str = Field(min_length=1, max_length=4096)


class StreamSourceFailRequest(StreamSourceRejectRequest):
    attempt: int = Field(ge=1, strict=True)


class StreamSourceOpenRequest(StreamSourceClaimRequest):
    protocol_version: Literal[1] = 1
    source_protocol: Literal["source-v1"] = "source-v1"

    @field_validator("protocol_version", mode="before")
    @classmethod
    def protocol_version_is_an_integer(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("protocol_version must be the integer 1")
        return value


class StreamSourceAppendRequest(_StreamRequest):
    binding: SourceBinding
    position: int = Field(ge=1, strict=True)
    value: JsonValue


class StreamSourceFinishRequest(_StreamRequest):
    binding: SourceBinding
    error: str | None = Field(default=None, min_length=1, max_length=4096)


class StreamSourceCloseRequest(_StreamRequest):
    binding: SourceBinding
    expected_position: int = Field(ge=0, strict=True)


class StreamReleaseRequest(_StreamRequest):
    flow_run_id: UUID


class StreamReleaseResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    released: int = Field(ge=0)


class StreamSubmissionReserveRequest(_StreamRequest):
    flow_run_id: UUID
    submission_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    )
    role: Literal["SOURCE", "OPERATOR"]
    protocol: Literal["source-v1", "yield-v2"]
    task_name: str = Field(min_length=1, max_length=255)
    task_key: str = Field(min_length=1, max_length=255)
    task_version: str = Field(min_length=1, max_length=255)
    flow_version: str = Field(min_length=1, max_length=255)
    deployment_id: UUID | None = None
    deployment_version: str | None = Field(default=None, min_length=1, max_length=255)
    artifact_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    parameters: dict[str, JsonValue]
    upstream_stream_id: UUID | None = None
    input_key: str | None = Field(default=None, min_length=1, max_length=255)
    limits: StreamLimits | None = None
    task_policy: TaskRunPolicy = Field(default_factory=TaskRunPolicy)
    tags: list[str] = Field(default_factory=list, max_length=64)
    labels: KeyValueLabels = Field(default_factory=dict)

    @field_validator("tags")
    @classmethod
    def tags_are_bounded(cls, value: list[str]) -> list[str]:
        if any(not tag or len(tag.encode("utf-8")) > 255 for tag in value):
            raise ValueError("tags must be nonempty and at most 255 UTF-8 bytes")
        return value

    @model_validator(mode="after")
    def topology_and_deployment_shape(self) -> Self:
        if self.role == "SOURCE":
            if (
                self.protocol != "source-v1"
                or self.upstream_stream_id is not None
                or self.input_key is not None
            ):
                raise ValueError("SOURCE requires source-v1 and no upstream input")
        elif (
            self.protocol != "yield-v2"
            or self.upstream_stream_id is None
            or self.input_key is None
        ):
            raise ValueError("OPERATOR requires yield-v2 and an upstream stream input")
        if (self.deployment_id is None) != (self.deployment_version is None):
            raise ValueError(
                "deployment_id and deployment_version must be provided together"
            )
        return self


class StreamSubmissionLookupRequest(_StreamRequest):
    flow_run_id: UUID
    submission_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    )


class StreamSubmissionManifest(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    submission_key: str
    manifest_digest: str
    task_run_id: UUID
    stream_id: UUID
    dynamic_key: str
    role: Literal["SOURCE", "OPERATOR"]
    protocol: Literal["source-v1", "yield-v2"]
    state: Literal["ACTIVE", "RELEASED"]
    created_at: DateTime
