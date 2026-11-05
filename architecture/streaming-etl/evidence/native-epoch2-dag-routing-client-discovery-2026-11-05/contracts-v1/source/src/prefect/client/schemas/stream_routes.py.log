"""Client-only DAG routing contracts; legacy storage bodies stay unchanged."""

from typing import Annotated, ClassVar, Literal, Self, TypeAlias
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

from prefect._internal.stream_key_wire import canonical_tagged_key_wire_v1
from prefect.client.schemas.stream_dags import Identifier
from prefect.client.schemas.stream_subtasks import Digest
from prefect.client.schemas.streams import (
    ConsumerBinding,
    DeliveryToken,
    SourceBinding,
)
from prefect.client.schemas.streams import (
    strict_finite_json as validate_strict_finite_json,
)

MAX_ROUTE_REFERENCES = 128


class StreamRouteWire(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


class _StreamDAGOutputKey(StreamRouteWire):
    @model_validator(mode="before")
    @classmethod
    def canonical_key(cls, value: object) -> object:
        if isinstance(value, _StreamDAGOutputKey):
            value = value.model_dump(mode="json")
        canonical_tagged_key_wire_v1(value)
        return value


class StreamNullOutputKey(_StreamDAGOutputKey):
    kind: Literal["N"]


class StreamBooleanOutputKey(_StreamDAGOutputKey):
    kind: Literal["B"]
    value: bool = Field(strict=True)


class StreamIntegerOutputKey(_StreamDAGOutputKey):
    kind: Literal["I"]
    value: str = Field(strict=True, max_length=20, pattern=r"^(0|-?[1-9][0-9]*)$")


class StreamStringOutputKey(_StreamDAGOutputKey):
    kind: Literal["S"]
    value: str = Field(strict=True, max_length=4091)


class StreamBytesOutputKey(_StreamDAGOutputKey):
    kind: Literal["Y"]
    value: str = Field(strict=True, max_length=5456)


StreamDAGOutputKey: TypeAlias = Annotated[
    StreamNullOutputKey
    | StreamBooleanOutputKey
    | StreamIntegerOutputKey
    | StreamStringOutputKey
    | StreamBytesOutputKey,
    Field(discriminator="kind"),
]


class StreamDAGRouteContext(StreamRouteWire):
    graph_revision_id: UUID
    graph_digest: Digest
    routing_digest: Digest
    producer_subtask_id: UUID
    operator_uid: Identifier
    output_port_uid: Identifier
    subtask_index: int = Field(ge=0, le=15, strict=True)
    has_keyed_outgoing: bool = Field(strict=True)


class StreamDAGSourceAppendRequest(StreamRouteWire):
    context: StreamDAGRouteContext
    binding: SourceBinding
    position: int = Field(ge=1, strict=True)
    value: JsonValue
    output_key: StreamDAGOutputKey | None = None

    @field_validator("value", mode="before")
    @classmethod
    def strict_value(cls, value: object) -> object:
        return validate_strict_finite_json(value)

    @field_validator("output_key", mode="before")
    @classmethod
    def canonical_key(cls, value: object) -> object:
        if isinstance(value, _StreamDAGOutputKey):
            return value.model_dump(mode="json")
        if value is not None:
            canonical_tagged_key_wire_v1(value)
        return value

    @model_validator(mode="after")
    def key_matches_sealed_context(self) -> Self:
        if self.context.has_keyed_outgoing != (self.output_key is not None):
            raise ValueError(
                "an output key is required exactly for keyed outgoing edges"
            )
        return self


class StreamDAGConsumerCompleteRequest(StreamRouteWire):
    context: StreamDAGRouteContext
    binding: ConsumerBinding
    token: DeliveryToken
    kind: Literal["YIELD", "RETURN"]
    value: JsonValue
    managed_state: dict[str, JsonValue]
    output_key: StreamDAGOutputKey | None = None

    @field_validator("value", "managed_state", mode="before")
    @classmethod
    def strict_values(cls, value: object) -> object:
        return validate_strict_finite_json(value)

    @field_validator("output_key", mode="before")
    @classmethod
    def canonical_key(cls, value: object) -> object:
        if isinstance(value, _StreamDAGOutputKey):
            return value.model_dump(mode="json")
        if value is not None:
            canonical_tagged_key_wire_v1(value)
        return value

    @model_validator(mode="after")
    def exact_terminal_shape(self) -> Self:
        if any(
            getattr(self.binding, field) != getattr(self.token, field)
            for field in ConsumerBinding.model_fields
        ):
            raise ValueError(
                "delivery token must belong to the supplied current binding"
            )
        if self.kind == "RETURN":
            if self.value is not None or self.output_key is not None:
                raise ValueError("RETURN has no output value or key")
        elif self.context.has_keyed_outgoing != (self.output_key is not None):
            raise ValueError(
                "an output key is required exactly for keyed outgoing edges"
            )
        return self


class StreamDAGRouteReference(StreamRouteWire):
    edge_id: UUID
    edge_partition_id: UUID
    stream_id: UUID
    sequence: int = Field(ge=1, strict=True)
    record_id: UUID
    payload_record_id: UUID


class StreamDAGRouteOutcome(StreamRouteWire):
    receipt_id: UUID
    graph_revision_id: UUID
    routing_digest: Digest
    semantic_digest: Digest
    outcome_digest: Digest
    producer_stream_id: UUID
    producer_position: int = Field(ge=1, strict=True)
    producer_role: Literal["SOURCE", "OPERATOR"]
    kind: Literal["YIELD", "RETURN"]
    input_stream_id: UUID | None
    input_record_id: UUID | None
    input_payload_record_id: UUID | None
    input_payload_value_digest: Digest | None
    input_sequence: int | None = Field(ge=1, strict=True)
    consumer_id: UUID | None
    producer_sequence: int | None = Field(ge=1, strict=True)
    output_ordinal: Literal[0] | None
    payload_record_id: UUID | None
    payload_value_digest: Digest | None
    payload_size_bytes: int | None = Field(ge=1, strict=True)
    references: list[StreamDAGRouteReference] = Field(max_length=MAX_ROUTE_REFERENCES)

    @field_validator("output_ordinal", mode="before")
    @classmethod
    def strict_ordinal(cls, value: object) -> object:
        if value is not None and type(value) is not int:
            raise ValueError("output ordinal is the exact integer zero or absent")
        return value

    @model_validator(mode="after")
    def exact_outcome_shape(self) -> Self:
        inputs = (
            self.input_stream_id,
            self.input_record_id,
            self.input_payload_record_id,
            self.input_payload_value_digest,
            self.input_sequence,
            self.consumer_id,
        )
        if self.producer_role == "SOURCE":
            if self.kind != "YIELD" or any(value is not None for value in inputs):
                raise ValueError(
                    "source outcomes have no consumed input and always yield"
                )
        elif (
            any(value is None for value in inputs)
            or self.producer_position != self.input_sequence
        ):
            raise ValueError(
                "operator outcomes bind their exact original input coordinate"
            )
        payload = (
            self.producer_sequence,
            self.payload_record_id,
            self.payload_value_digest,
            self.payload_size_bytes,
        )
        if self.kind == "RETURN":
            if (
                any(value is not None for value in payload)
                or self.output_ordinal is not None
                or self.references
            ):
                raise ValueError("RETURN has neither a payload nor routed references")
        elif any(value is None for value in payload) or self.output_ordinal != 0:
            raise ValueError("YIELD has exactly one payload at output ordinal zero")
        if (
            len({item.record_id for item in self.references}) != len(self.references)
            or len({item.edge_partition_id for item in self.references})
            != len(self.references)
            or len({item.stream_id for item in self.references}) != len(self.references)
        ):
            raise ValueError("route reference identities and targets must be distinct")
        if self.references != sorted(
            self.references,
            key=lambda item: (
                item.edge_id.bytes,
                item.edge_partition_id.bytes,
                item.record_id.bytes,
            ),
        ):
            raise ValueError("route references require canonical UUID-byte ordering")
        if any(
            item.payload_record_id != self.payload_record_id for item in self.references
        ):
            raise ValueError(
                "every reference must point directly to this turn's payload"
            )
        return self
