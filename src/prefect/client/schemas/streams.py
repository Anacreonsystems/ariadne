"""Client-only mirror of native stream wire contracts; no server imports."""

from typing import ClassVar, Literal, Self, cast
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

from prefect.client.schemas.objects import TaskRunPolicy
from prefect.types import DateTime

KeyValueLabels = dict[str, StrictBool | StrictInt | StrictFloat | str]


def _strict_finite_json(value: object) -> object:
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("finite JSON numbers are required")
        return value
    if type(value) is list:
        return [_strict_finite_json(item) for item in cast(list[object], value)]
    if type(value) is dict:
        mapping = cast(dict[object, object], value)
        if any(type(key) is not str for key in mapping):
            raise ValueError("JSON object keys must be strings")
        return {
            cast(str, key): _strict_finite_json(item) for key, item in mapping.items()
        }
    raise ValueError("strict JSON values are required")


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


class StreamFiniteReserveRequest(_StreamRequest):
    flow_run_id: UUID
    phase: Literal["PREFIX", "TAIL"]
    submission_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    )
    task_name: str = Field(min_length=1, max_length=255)
    task_key: str = Field(min_length=1, max_length=255)
    artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    task_version: str = Field(min_length=1, max_length=255)
    flow_version: str = Field(min_length=1, max_length=255)
    deployment_id: UUID | None = None
    deployment_version: str | None = Field(default=None, min_length=1, max_length=255)
    parameters: dict[str, JsonValue]
    task_policy: TaskRunPolicy = Field(default_factory=TaskRunPolicy)
    tags: list[str] = Field(default_factory=list, max_length=64)
    labels: KeyValueLabels = Field(default_factory=dict)

    @field_validator("parameters", mode="before")
    @classmethod
    def finite_parameters_are_strict_json(cls, value: object) -> object:
        return _strict_finite_json(value)

    @field_validator("tags")
    @classmethod
    def finite_tags_are_bounded(cls, value: list[str]) -> list[str]:
        if any(not tag or len(tag.encode("utf-8")) > 255 for tag in value):
            raise ValueError("tags must be nonempty and at most 255 UTF-8 bytes")
        return value

    @model_validator(mode="after")
    def finite_deployment_shape(self) -> Self:
        if (self.deployment_id is None) != (self.deployment_version is None):
            raise ValueError(
                "deployment_id and deployment_version must be provided together"
            )
        return self


class StreamFiniteLookupRequest(_StreamRequest):
    flow_run_id: UUID
    submission_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    )


class StreamFiniteManifest(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    phase: Literal["PREFIX", "TAIL"]
    submission_key: str
    manifest_digest: str
    task_run_id: UUID
    dynamic_key: str
    state: Literal["ACTIVE", "RELEASED"]
    result_state: Literal["PENDING", "COMPLETED"]
    created_at: DateTime


class StreamFinitePlan(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    manifest_sealed: bool
    items: list[StreamFiniteManifest] = Field(max_length=2)


class StreamFiniteCompleteRequest(_StreamRequest):
    flow_run_id: UUID
    task_run_id: UUID
    operation_id: UUID
    run_count: int = Field(ge=1, strict=True)
    result: JsonValue

    @field_validator("result", mode="before")
    @classmethod
    def finite_result_is_strict_json(cls, value: object) -> object:
        return _strict_finite_json(value)


class StreamFiniteReceipt(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    phase: Literal["PREFIX", "TAIL"]
    submission_key: str
    task_run_id: UUID
    operation_id: UUID
    run_count: int = Field(ge=1, strict=True)
    result: JsonValue
    result_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: DateTime


class StreamExecutionCredentials(BaseModel):
    """Opaque runtime authority extracted from internal HTTP headers."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    generation: int = Field(ge=1, le=64, strict=True)
    holder_id: UUID
    capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )


class StreamExecutionDeathWitness(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    launcher_instance_id: UUID
    child_process_id: int = Field(ge=1, strict=True)
    exit_code: int = Field(ge=-255, le=255, strict=True)
    observed_at: DateTime
    kind: Literal["WAITPID", "OWNED_SIGKILL"]

    @field_validator("observed_at")
    @classmethod
    def observed_at_is_aware(cls, value: DateTime) -> DateTime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must include an explicit timezone")
        return value


class StreamExecutionClaimRequest(_StreamRequest):
    flow_run_id: UUID
    operation_id: UUID
    expected_generation: Literal[0] = 0
    holder_id: UUID
    holder_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    holder_event_public_key: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    supervisor_id: UUID
    supervisor_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    protocol_version: Literal[1] = 1

    @field_validator("expected_generation", mode="before")
    @classmethod
    def expected_generation_is_integer_zero(cls, value: object) -> object:
        if type(value) is not int or value != 0:
            raise ValueError("expected_generation must be the integer 0")
        return value

    @field_validator("protocol_version", mode="before")
    @classmethod
    def protocol_version_is_an_integer(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("protocol_version must be the integer 1")
        return value


class StreamExecutionTakeoverRequest(_StreamRequest):
    flow_run_id: UUID
    operation_id: UUID
    expected_generation: int = Field(ge=1, le=63, strict=True)
    expected_holder_id: UUID
    supervisor_id: UUID
    supervisor_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    new_holder_id: UUID
    new_holder_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    new_holder_event_public_key: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    death_witness: StreamExecutionDeathWitness


class StreamExecutionBinding(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    holder_id: UUID
    operation_id: UUID
    state: Literal["CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL", "RELEASED"]
    flow_run_run_count: int = Field(ge=0, strict=True)
    created_at: DateTime


class StreamManifestWitness(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    submission_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    )
    manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class StreamFiniteManifestWitness(StreamManifestWitness):
    phase: Literal["PREFIX", "TAIL"]


class StreamFiniteReceiptWitness(StreamFiniteManifestWitness):
    task_run_id: UUID
    result_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class StreamManifestSealRequest(_StreamRequest):
    flow_run_id: UUID
    operation_id: UUID
    manifests: list[StreamManifestWitness] = Field(min_length=1, max_length=128)
    finite_prefix: StreamFiniteReceiptWitness | None = None


class StreamManifestSealResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    operation_id: UUID
    manifest_set_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_count: int = Field(ge=1, le=128, strict=True)


class StreamRecoveryPrepareRequest(_StreamRequest):
    flow_run_id: UUID
    operation_id: UUID
    manifests: list[StreamManifestWitness] = Field(
        default_factory=lambda: list[StreamManifestWitness](), max_length=128
    )
    finite_manifests: list[StreamFiniteManifestWitness] | None = Field(
        default=None, min_length=1, max_length=2
    )

    @model_validator(mode="after")
    def recovery_has_an_authority_witness(self) -> Self:
        if not self.manifests and not self.finite_manifests:
            raise ValueError("recovery requires a stream or finite manifest witness")
        return self


class StreamRecoveryItem(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    submission_key: str
    task_run_id: UUID
    stream_id: UUID
    durable_position: int = Field(ge=0, strict=True)
    stream_state: Literal["OPEN", "COMPLETED", "FAILED"]
    task_state: str
    current_run_count: int = Field(ge=0, strict=True)
    next_run_count: int = Field(ge=1, strict=True)


class StreamFiniteRecoveryItem(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    submission_key: str
    phase: Literal["PREFIX", "TAIL"]
    task_run_id: UUID
    task_state: str
    current_run_count: int = Field(ge=0, strict=True)
    next_run_count: int = Field(ge=1, strict=True)
    receipt: StreamFiniteReceipt | None = None


class StreamRecoveryResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    operation_id: UUID
    items: list[StreamRecoveryItem]
    finite_items: list[StreamFiniteRecoveryItem] | None = None
    outcome_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class StreamUnsealedAbortRequest(_StreamRequest):
    flow_run_id: UUID
    operation_id: UUID
    manifests: list[StreamManifestWitness] = Field(min_length=1, max_length=128)
    error: str = Field(min_length=1, max_length=4096)

    @field_validator("error")
    @classmethod
    def error_is_bounded_utf8(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 4096:
            raise ValueError("abort error must be at most 4096 UTF-8 bytes")
        return value


class StreamUnsealedAbortItem(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    submission_key: str
    task_run_id: UUID
    stream_id: UUID


class StreamUnsealedAbortResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    operation_id: UUID
    state: Literal["TERMINAL"]
    items: list[StreamUnsealedAbortItem]
    outcome_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
