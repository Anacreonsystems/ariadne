"""Client-only mirror of versioned DAG metadata; no server imports."""

import hashlib
import json
from typing import Annotated, ClassVar, Literal, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from prefect.client.schemas.objects import TaskRunPolicy
from prefect.client.schemas.stream_subtasks import (
    Digest,
    SizedParameters,
    StreamGraphSealRequest,
    StreamOperatorSpec,
)
from prefect.client.schemas.streams import (
    StreamLimits,
    StreamSubmissionManifest,
)
from prefect.client.schemas.streams import (
    strict_finite_json as validate_strict_finite_json,
)
from prefect.types import DateTime

JSON_VALUE_SCHEMA_DESCRIPTOR: dict[str, str | int] = {
    "id": "json-value-v1",
    "type": "json-value",
    "version": 1,
}
JSON_VALUE_SCHEMA_DIGEST = hashlib.sha256(
    json.dumps(
        JSON_VALUE_SCHEMA_DESCRIPTOR, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
).hexdigest()
MAX_DAG_MANIFEST_BYTES = 1024 * 1024
Identifier = Annotated[
    str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
]


class StreamDAGWire(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def exact_identity_integers(cls, value: object) -> object:
        if isinstance(value, dict):
            mapping = cast(dict[object, object], value)
            for key in (
                "parallelism",
                "assignment_epoch",
                "schema_version",
                "routing_version",
                "key_group_count",
            ):
                if (
                    key in mapping
                    and mapping[key] is not None
                    and type(mapping[key]) is not int
                ):
                    raise ValueError("DAG identity counters require JSON integers")
            return mapping
        return value


class StreamPortSpec(StreamDAGWire):
    uid: Identifier
    schema_id: Literal["json-value-v1"] = "json-value-v1"
    schema_version: Literal[1] = 1
    schema_digest: Digest = JSON_VALUE_SCHEMA_DIGEST

    @model_validator(mode="after")
    def exact_schema_identity(self) -> Self:
        if self.schema_digest != JSON_VALUE_SCHEMA_DIGEST:
            raise ValueError("json-value-v1 requires its exact canonical schema digest")
        return self


class StreamDAGOperatorSpec(StreamDAGWire):
    operator_uid: Identifier
    role: Literal["SOURCE", "OPERATOR"]
    parallelism: int = Field(ge=1, le=16, strict=True)
    task_name: str = Field(min_length=1, max_length=255)
    task_key: str = Field(min_length=1, max_length=255)
    task_version: str = Field(min_length=1, max_length=255)
    artifact_digest: Digest
    entrypoint: str = Field(min_length=1, max_length=1024)
    context_key: str = Field(min_length=1, max_length=255)
    parameters: list[SizedParameters] = Field(min_length=1, max_length=16)
    input_port: StreamPortSpec | None = None
    output_port: StreamPortSpec
    input_key: str | None = Field(default=None, min_length=1, max_length=255)
    task_policy: TaskRunPolicy = Field(default_factory=TaskRunPolicy)
    timeout_seconds: float | None = Field(
        default=None, gt=0, allow_inf_nan=False, strict=True
    )
    tags: list[str] = Field(default_factory=list, max_length=64)
    limits: StreamLimits = Field(default_factory=StreamLimits)

    @field_validator("parameters", mode="before")
    @classmethod
    def strict_parameters(cls, value: object) -> object:
        return validate_strict_finite_json(value)

    @field_validator("task_policy", mode="before")
    @classmethod
    def bounded_policy(cls, value: object) -> object:
        return StreamOperatorSpec.bounded_physical_policy(value)

    @model_validator(mode="after")
    def immutable_task_shape(self) -> Self:
        if len(self.parameters) != self.parallelism:
            raise ValueError("one immutable parameter snapshot is required per subtask")
        if self.role == "SOURCE":
            if self.input_port is not None or self.input_key is not None:
                raise ValueError("SOURCE declares no input port or input key")
            if self.parallelism > 1:
                canonical = [
                    json.dumps(
                        value, sort_keys=True, allow_nan=False, separators=(",", ":")
                    )
                    for value in self.parameters
                ]
                if not all(self.parameters) or len(set(canonical)) != self.parallelism:
                    raise ValueError(
                        "parallel SOURCE splits must be nonempty and distinct"
                    )
        elif self.input_port is None or self.input_key is None:
            raise ValueError("OPERATOR requires one input port and input key")
        if self.input_port is not None and self.input_port.uid == self.output_port.uid:
            raise ValueError(
                "input and output port UIDs must differ within an operator"
            )
        if self.context_key == self.input_key or any(
            self.context_key in value
            or (self.input_key is not None and self.input_key in value)
            for value in self.parameters
        ):
            raise ValueError(
                "context and injected input cannot be supplied as parameters"
            )
        if self.task_policy.retries is None or not 1 <= self.task_policy.retries <= 63:
            raise ValueError("DAG tasks require 1..63 retries")
        if any(not tag or len(tag.encode("utf-8")) > 255 for tag in self.tags):
            raise ValueError("tags require 1..255 UTF-8 bytes")
        self.tags = sorted(set(self.tags))
        return self


class StreamEdgeSpec(StreamDAGWire):
    edge_uid: Identifier
    producer_operator_uid: Identifier
    output_port_uid: Identifier
    subscriber_uid: Identifier
    consumer_operator_uid: Identifier
    input_port_uid: Identifier
    exchange: Literal["forward", "rebalance", "broadcast", "keyed"]
    routing_version: Literal[1] = 1
    key_codec: Literal["tagged-key-v1"] | None = None
    hash_algorithm: Literal["sha256"] | None = None
    key_group_count: int | None = Field(default=None, ge=1, le=2**31 - 1, strict=True)

    @model_validator(mode="after")
    def exchange_identity(self) -> Self:
        if self.exchange == "keyed":
            if (
                self.key_codec != "tagged-key-v1"
                or self.hash_algorithm != "sha256"
                or self.key_group_count is None
            ):
                raise ValueError(
                    "keyed edges require exact codec/hash and key-group count"
                )
        elif (
            self.key_codec is not None
            or self.hash_algorithm is not None
            or self.key_group_count is not None
        ):
            raise ValueError("only keyed edges declare key routing metadata")
        return self


class StreamDAGSealRequest(StreamDAGWire):
    flow_run_id: UUID
    operation_id: UUID
    profile: Literal["partitioned-dag-v1"] = "partitioned-dag-v1"
    assignment_epoch: Literal[1] = 1
    flow_version: str = Field(min_length=1, max_length=255)
    deployment_id: UUID | None = None
    deployment_version: str | None = Field(default=None, min_length=1, max_length=255)
    working_directory: str | None = None
    operators: list[StreamDAGOperatorSpec] = Field(min_length=2, max_length=16)
    edges: list[StreamEdgeSpec] = Field(min_length=1, max_length=32)

    @field_validator("working_directory")
    @classmethod
    def normalized_directory(cls, value: str | None) -> str | None:
        return StreamGraphSealRequest.canonical_working_directory(value)

    @model_validator(mode="after")
    def immutable_branched_topology(self) -> Self:
        if (self.deployment_id is None) != (self.deployment_version is None):
            raise ValueError("deployment identity must be complete")
        operators = {value.operator_uid: value for value in self.operators}
        if len(operators) != len(self.operators):
            raise ValueError("operator UIDs must be unique")
        if sum(value.parallelism for value in self.operators) > 128:
            raise ValueError("DAG exceeds its 128 physical-subtask bound")
        sources = [
            value.operator_uid for value in self.operators if value.role == "SOURCE"
        ]
        if len(sources) != 1:
            raise ValueError("DAG requires exactly one SOURCE root")
        if len({value.edge_uid for value in self.edges}) != len(self.edges):
            raise ValueError("edge UIDs must be unique")
        if len({value.subscriber_uid for value in self.edges}) != len(self.edges):
            raise ValueError("subscriber UIDs must be unique")
        incoming: dict[str, int] = {uid: 0 for uid in operators}
        outgoing: dict[str, list[str]] = {uid: [] for uid in operators}
        for edge in self.edges:
            producer = operators.get(edge.producer_operator_uid)
            consumer = operators.get(edge.consumer_operator_uid)
            if producer is None or consumer is None:
                raise ValueError("edge endpoints must name this graph's operators")
            if (
                producer.output_port.uid != edge.output_port_uid
                or consumer.input_port is None
                or consumer.input_port.uid != edge.input_port_uid
            ):
                raise ValueError("edge endpoints require the exact named ports")
            if producer.output_port.model_dump(
                exclude={"uid"}
            ) != consumer.input_port.model_dump(exclude={"uid"}):
                raise ValueError(
                    "connected ports require identical portable schema identities"
                )
            if (
                edge.exchange == "forward"
                and producer.parallelism != consumer.parallelism
            ):
                raise ValueError("forward requires compatible equal parallelism")
            if (
                edge.key_group_count is not None
                and edge.key_group_count < consumer.parallelism
            ):
                raise ValueError("key-group count must cover every target subtask")
            incoming[consumer.operator_uid] += 1
            outgoing[producer.operator_uid].append(consumer.operator_uid)
        if incoming[sources[0]] != 0 or any(
            count != 1 for uid, count in incoming.items() if uid != sources[0]
        ):
            raise ValueError(
                "each OPERATOR requires exactly one logical input; fan-in is unsupported"
            )
        pending = sorted(uid for uid, count in incoming.items() if count == 0)
        order: list[str] = []
        while pending:
            uid = pending.pop(0)
            order.append(uid)
            for child in sorted(outgoing[uid]):
                incoming[child] -= 1
                if incoming[child] == 0:
                    pending.append(child)
                    pending.sort()
        if len(order) != len(operators):
            raise ValueError("cycles and unreachable operators are unsupported")
        self.operators = [operators[uid] for uid in order]
        self.edges = sorted(self.edges, key=lambda value: value.edge_uid)
        encoded = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > MAX_DAG_MANIFEST_BYTES:
            raise ValueError("DAG manifest exceeds its 1MiB bound")
        return self


class StreamDAGPortProjection(StreamPortSpec):
    id: UUID
    direction: Literal["INPUT", "OUTPUT"]


class StreamDAGOperatorProjection(StreamDAGWire):
    id: UUID
    operator_uid: Identifier
    role: Literal["SOURCE", "OPERATOR"]
    parallelism: int = Field(ge=1, le=16, strict=True)
    input_port: StreamDAGPortProjection | None
    output_port: StreamDAGPortProjection

    @model_validator(mode="after")
    def exact_role_ports(self) -> Self:
        if (self.role == "SOURCE") != (self.input_port is None):
            raise ValueError("projected operator ports must match its role")
        if self.output_port.direction != "OUTPUT" or (
            self.input_port is not None
            and (
                self.input_port.direction != "INPUT"
                or self.input_port.uid == self.output_port.uid
            )
        ):
            raise ValueError("projected port directions and identities must match")
        return self


class StreamDAGEdgePartitionProjection(StreamDAGWire):
    id: UUID
    partition_index: int = Field(ge=0, le=15, strict=True)
    stream_id: UUID
    subscriber_id: UUID
    target_subtask_id: UUID


class StreamDAGEdgeProjection(StreamEdgeSpec):
    id: UUID
    partitions: list[StreamDAGEdgePartitionProjection] = Field(
        min_length=1, max_length=16
    )


class StreamDAGSubtaskProjection(StreamDAGWire):
    id: UUID
    operator_run_id: UUID
    operator_uid: Identifier
    subtask_index: int = Field(ge=0, le=15, strict=True)
    assignment_epoch: Literal[1] = 1
    input_stream_id: UUID | None
    output_stream_id: UUID
    manifest: StreamSubmissionManifest
    assigned_worker_id: UUID | None = None
    physical_slot_reserved: bool = False
    physical_slot_released: bool = False

    @model_validator(mode="after")
    def exact_slot_shape(self) -> Self:
        if self.physical_slot_released and not self.physical_slot_reserved:
            raise ValueError("a released slot requires its durable reservation")
        if (self.assigned_worker_id is not None) != self.physical_slot_reserved:
            raise ValueError("assigned worker and durable reservation must agree")
        if self.manifest.stream_id != self.output_stream_id:
            raise ValueError("subtask output must match its immutable manifest")
        return self


class StreamDAGPlan(StreamDAGWire):
    id: UUID
    flow_run_id: UUID
    profile: Literal["partitioned-dag-v1"] = "partitioned-dag-v1"
    assignment_epoch: Literal[1] = 1
    operation_id: UUID
    graph_digest: Digest
    state: Literal["SEALED", "ADMITTED", "ABORTING", "RELEASED"]
    created_at: DateTime
    placement_digest: Digest | None = None
    execution_ready: Literal[False] = False
    operators: list[StreamDAGOperatorProjection] = Field(min_length=2, max_length=16)
    edges: list[StreamDAGEdgeProjection] = Field(min_length=1, max_length=32)
    subtasks: list[StreamDAGSubtaskProjection] = Field(min_length=2, max_length=128)
    leaf_operator_uids: list[Identifier] = Field(min_length=1, max_length=15)

    @model_validator(mode="after")
    def exact_plan_shape(self) -> Self:
        operators = {value.operator_uid: value for value in self.operators}
        if len(operators) != len(self.operators):
            raise ValueError("plan operator identities must be unique")
        if len({value.id for value in self.operators}) != len(self.operators):
            raise ValueError("operator projection IDs must be unique")
        ports = [
            port
            for value in self.operators
            for port in (value.input_port, value.output_port)
            if port is not None
        ]
        if len({value.id for value in ports}) != len(ports):
            raise ValueError("port projection IDs must be unique")
        if (
            len({value.id for value in self.edges}) != len(self.edges)
            or len({value.edge_uid for value in self.edges}) != len(self.edges)
            or len({value.subscriber_uid for value in self.edges}) != len(self.edges)
        ):
            raise ValueError("edge projection and subscriber identities must be unique")
        if len(self.subtasks) != sum(value.parallelism for value in self.operators):
            raise ValueError("plan must retain every physical subtask")
        indexed = {
            (value.operator_uid, value.subtask_index): value for value in self.subtasks
        }
        if len(indexed) != len(self.subtasks):
            raise ValueError("plan physical subtask coordinates must be unique")
        for operator in self.operators:
            for index in range(operator.parallelism):
                subtask = indexed.get((operator.operator_uid, index))
                if (
                    subtask is None
                    or subtask.operator_run_id != operator.id
                    or subtask.manifest.flow_run_id != self.flow_run_id
                    or subtask.manifest.role != operator.role
                    or subtask.manifest.protocol
                    != ("source-v1" if operator.role == "SOURCE" else "yield-v2")
                ):
                    raise ValueError(
                        "plan subtask must belong to its exact operator and flow"
                    )
        if len({value.id for value in self.subtasks}) != len(self.subtasks) or len(
            {value.manifest.task_run_id for value in self.subtasks}
        ) != len(self.subtasks):
            raise ValueError("physical subtask and TaskRun identities must be unique")
        producer_outputs = {value.output_stream_id for value in self.subtasks}
        if len(producer_outputs) != len(self.subtasks):
            raise ValueError("producer partitions must be distinct")
        input_streams: dict[tuple[str, int], UUID] = {}
        partition_ids: set[UUID] = set()
        edge_stream_ids: set[UUID] = set()
        subscriber_ids: set[UUID] = set()
        target_ids: set[UUID] = set()
        for edge in self.edges:
            consumer = operators.get(edge.consumer_operator_uid)
            producer = operators.get(edge.producer_operator_uid)
            if consumer is None or producer is None or consumer.input_port is None:
                raise ValueError("edge endpoints must belong to this plan")
            if (
                producer.output_port.uid != edge.output_port_uid
                or consumer.input_port.uid != edge.input_port_uid
            ):
                raise ValueError("plan edge ports must match")
            if producer.output_port.model_dump(
                exclude={"id", "uid", "direction"}
            ) != consumer.input_port.model_dump(exclude={"id", "uid", "direction"}):
                raise ValueError(
                    "projected connected ports require identical schema identity"
                )
            if len(edge.partitions) != consumer.parallelism or {
                value.partition_index for value in edge.partitions
            } != set(range(consumer.parallelism)):
                raise ValueError("edge partitions must cover every target subtask")
            for partition in edge.partitions:
                if (
                    partition.id in partition_ids
                    or partition.stream_id in edge_stream_ids
                    or partition.subscriber_id in subscriber_ids
                    or partition.target_subtask_id in target_ids
                ):
                    raise ValueError(
                        "edge partition, stream, subscriber and target identities must be independent"
                    )
                partition_ids.add(partition.id)
                edge_stream_ids.add(partition.stream_id)
                subscriber_ids.add(partition.subscriber_id)
                target_ids.add(partition.target_subtask_id)
                key = (consumer.operator_uid, partition.partition_index)
                if key in input_streams or partition.stream_id in producer_outputs:
                    raise ValueError(
                        "edge partitions must be independent input bindings"
                    )
                if indexed[key].id != partition.target_subtask_id:
                    raise ValueError(
                        "edge target must match the exact physical subtask"
                    )
                input_streams[key] = partition.stream_id
        for key, subtask in indexed.items():
            if subtask.input_stream_id != input_streams.get(key):
                raise ValueError(
                    "subtask input must match its persisted edge partition"
                )
        leaves = sorted(
            set(operators) - {value.producer_operator_uid for value in self.edges}
        )
        if self.leaf_operator_uids != leaves:
            raise ValueError("plan leaf set must be canonical and complete")
        any_reserved = any(value.physical_slot_reserved for value in self.subtasks)
        if any_reserved != (self.placement_digest is not None):
            raise ValueError("placement witness must match reserved physical slots")
        if self.state == "SEALED" and any_reserved:
            raise ValueError("SEALED subtasks must be unassigned and unreserved")
        if self.state == "ADMITTED" and not all(
            value.physical_slot_reserved and not value.physical_slot_released
            for value in self.subtasks
        ):
            raise ValueError("ADMITTED subtasks require every live region reservation")
        if (
            self.state == "ABORTING"
            and any_reserved
            and not all(value.physical_slot_reserved for value in self.subtasks)
        ):
            raise ValueError("an admitted abort retains the complete reserved region")
        if (
            self.state == "RELEASED"
            and any_reserved
            and not all(
                value.physical_slot_reserved and value.physical_slot_released
                for value in self.subtasks
            )
        ):
            raise ValueError("RELEASED graph must release its complete reserved region")
        return self
