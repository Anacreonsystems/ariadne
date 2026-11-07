"""Independent client DAG lifecycle, bootstrap and launch contracts."""

from typing import Annotated, ClassVar, Literal, Self, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from prefect.client.schemas.native_deployments import NativeStreamProcessDeathWitness
from prefect.client.schemas.objects import StateType, TaskRun, TaskRunPolicy
from prefect.client.schemas.stream_dags import (
    JSON_VALUE_SCHEMA_DIGEST,
    Identifier,
    StreamDAGSubtaskProjection,
)
from prefect.client.schemas.stream_routes import StreamDAGRouteContext
from prefect.client.schemas.stream_subtasks import (
    Digest,
    SizedParameters,
    StreamSubtaskGrant,
    StreamWorkerPrincipal,
    StreamWorkerTaskConfiguration,
)
from prefect.client.schemas.streams import (
    ConsumerBinding,
    SourceBinding,
)
from prefect.client.schemas.streams import (
    strict_finite_json as validate_strict_finite_json,
)
from prefect.types import DateTime

Signature = Annotated[str, Field(pattern=r"^[0-9a-f]{128}$", strict=True)]
DAGPhase: TypeAlias = Literal["PREVIEW", "STARTING", "ACTIVE", "STOPPING", "TERMINAL"]
ControlKind: TypeAlias = Literal["PREVIEW_ABORT", "STARTUP_ABORT", "FAIL", "CANCEL"]
TerminalState: TypeAlias = Literal["COMPLETED", "FAILED", "CANCELLED"]


class StreamDAGExecutionWire(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )


class StreamDAGOperationRequest(StreamDAGExecutionWire):
    operation_id: UUID
    graph_digest: Digest
    routing_digest: Digest


class StreamDAGStartRequest(StreamDAGOperationRequest):
    pass


class StreamDAGActivateRequest(StreamDAGOperationRequest):
    pass


class StreamDAGAbortPreviewRequest(StreamDAGOperationRequest):
    pass


class StreamDAGCancelRequest(StreamDAGOperationRequest):
    reason: str = Field(min_length=1, max_length=4096, strict=True)

    @field_validator("reason")
    @classmethod
    def bounded_reason(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 4096:
            raise ValueError("reason exceeds 4096 UTF-8 bytes")
        return value


class StreamDAGFailRequest(StreamDAGOperationRequest):
    failure_kind: Literal[
        "FLOW_SCOPE_EXCEPTION", "START_EXCEPTION", "START_TIMEOUT", "COORDINATOR_ERROR"
    ]


class StreamDAGOperationReceipt(StreamDAGExecutionWire):
    operation_id: UUID
    graph_revision_id: UUID
    kind: Literal["START", "ACTIVATE", "ABORT_PREVIEW", "CANCEL", "FAIL"]
    request_digest: Digest
    outcome_digest: Digest
    accepted_at: DateTime
    effective_control_kind: ControlKind | None
    control_operation_id: UUID | None

    @model_validator(mode="after")
    def complete_control_identity(self) -> Self:
        if (self.effective_control_kind is None) != (self.control_operation_id is None):
            raise ValueError("control kind and operation identity must be complete")
        return self


class StreamDAGExecutionCounts(StreamDAGExecutionWire):
    total: int = Field(ge=2, le=128, strict=True)
    registered: int = Field(ge=0, le=128, strict=True)
    bootstrap_ready: int = Field(ge=0, le=128, strict=True)
    terminal_tasks: int = Field(ge=0, le=128, strict=True)
    stopped: int = Field(ge=0, le=128, strict=True)
    physical_slots_remaining: int = Field(ge=0, le=128, strict=True)
    tags_remaining: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def bounded_region(self) -> Self:
        if (
            any(
                value > self.total
                for value in (
                    self.registered,
                    self.bootstrap_ready,
                    self.terminal_tasks,
                    self.stopped,
                    self.physical_slots_remaining,
                )
            )
            or self.bootstrap_ready > self.registered
        ):
            raise ValueError("execution counters exceed their complete physical region")
        return self


class StreamDAGControlProjection(StreamDAGExecutionWire):
    operation_id: UUID
    kind: ControlKind
    state: Literal["REQUESTED", "STOPPING", "COMPLETED"]


class StreamDAGExecution(StreamDAGExecutionWire):
    graph_revision_id: UUID
    flow_run_id: UUID
    graph_digest: Digest
    routing_digest: Digest
    phase: DAGPhase
    terminal_state: TerminalState | None
    cleanup_complete: bool = Field(strict=True)
    counts: StreamDAGExecutionCounts
    control: StreamDAGControlProjection | None
    activation_operation_id: UUID | None
    readiness_set_digest: Digest | None
    terminal_operation_id: UUID | None
    terminal_outcome_digest: Digest | None

    @model_validator(mode="after")
    def durable_phase(self) -> Self:
        if (self.activation_operation_id is None) != (
            self.readiness_set_digest is None
        ):
            raise ValueError("activation operation and readiness witness must agree")
        if (self.terminal_operation_id is None) != (
            self.terminal_outcome_digest is None
        ):
            raise ValueError("terminal operation and outcome must agree")
        if self.phase == "TERMINAL":
            if (
                self.terminal_state is None
                or self.terminal_operation_id is None
                or not self.cleanup_complete
                or self.counts.physical_slots_remaining != 0
                or self.counts.tags_remaining != 0
                or self.counts.terminal_tasks != self.counts.total
                or (self.control is not None and self.control.state != "COMPLETED")
            ):
                raise ValueError("terminal execution requires durable complete cleanup")
            preview_abort = (
                self.control is not None and self.control.kind == "PREVIEW_ABORT"
            )
            if self.counts.stopped != (0 if preview_abort else self.counts.total):
                raise ValueError(
                    "terminal execution requires every admitted target stopped"
                )
        elif (
            self.terminal_state is not None
            or self.terminal_operation_id is not None
            or self.cleanup_complete
        ):
            raise ValueError("nonterminal execution cannot report terminal cleanup")
        if self.phase == "ACTIVE" and (
            self.activation_operation_id is None or self.control is not None
        ):
            raise ValueError("ACTIVE requires activation and no control fence")
        if self.phase in {"PREVIEW", "STARTING"} and (
            self.activation_operation_id is not None or self.control is not None
        ):
            raise ValueError("unactivated execution cannot expose activation/control")
        if self.phase == "PREVIEW" and any(
            (
                self.counts.registered,
                self.counts.bootstrap_ready,
                self.counts.terminal_tasks,
                self.counts.stopped,
                self.counts.physical_slots_remaining,
                self.counts.tags_remaining,
            )
        ):
            raise ValueError("unassigned preview cannot own running/terminal resources")
        if self.phase == "STOPPING" and (
            self.control is None or self.control.state == "COMPLETED"
        ):
            raise ValueError("STOPPING requires its pending durable control")
        return self


class StreamDAGOperationResult(StreamDAGExecutionWire):
    receipt: StreamDAGOperationReceipt
    execution: StreamDAGExecution

    @model_validator(mode="after")
    def same_graph(self) -> Self:
        if self.receipt.graph_revision_id != self.execution.graph_revision_id:
            raise ValueError("operation receipt and execution must name one graph")
        return self


class StreamDAGInputProjection(StreamDAGExecutionWire):
    subscriber_id: UUID
    edge_id: UUID
    edge_partition_id: UUID
    input_port_uid: Identifier
    input_stream_id: UUID
    schema_id: Literal["json-value-v1"]
    schema_digest: Digest

    @model_validator(mode="after")
    def exact_input_schema(self) -> Self:
        if self.schema_digest != JSON_VALUE_SCHEMA_DIGEST:
            raise ValueError("input schema must have the frozen json-value-v1 digest")
        return self


class StreamDAGOutputProjection(StreamDAGExecutionWire):
    port_uid: Identifier
    stream_id: UUID


class StreamDAGExecutionAssignment(StreamDAGSubtaskProjection):
    """Private execution binding; the published topology projection is unchanged."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", frozen=True, hide_input_in_errors=True
    )
    graph_revision_id: UUID
    current_grant_id: UUID | None
    physical_epoch: int = Field(ge=0, le=64, strict=True)
    physical_slot_reserved: bool = Field(default=False, strict=True)
    physical_slot_released: bool = Field(default=False, strict=True)

    @model_validator(mode="after")
    def current_grant_shape(self) -> Self:
        if (self.current_grant_id is None) != (self.physical_epoch == 0):
            raise ValueError("execution assignment requires a complete current grant")
        return self


class StreamDAGSubtaskBootstrap(StreamDAGExecutionWire):
    flow_run_id: UUID
    flow_run_run_count: int = Field(ge=0, strict=True)
    flow_generation: int = Field(ge=1, le=64, strict=True)
    flow_holder_id: UUID
    assignment: StreamDAGExecutionAssignment
    grant: StreamSubtaskGrant
    context: StreamDAGRouteContext
    operator_parallelism: int = Field(ge=1, le=16, strict=True)
    input: StreamDAGInputProjection | None
    output: StreamDAGOutputProjection
    entrypoint: str = Field(min_length=1, max_length=1024, strict=True)
    task_key: str = Field(min_length=1, max_length=255, strict=True)
    task_version: str = Field(min_length=1, max_length=255, strict=True)
    artifact_digest: Digest
    context_key: str = Field(min_length=1, max_length=255, strict=True)
    task_policy: TaskRunPolicy
    timeout_seconds: float | None = Field(
        default=None, gt=0, allow_inf_nan=False, strict=True
    )
    effective_tags: list[str]
    parameters: SizedParameters
    input_key: str | None
    task_run: TaskRun
    reserved_tag_names: list[str]
    event_public_key: Digest
    launch_attempt_id: UUID
    worker_generation_digest: Digest
    bootstrap_digest: Digest

    @field_validator("parameters", mode="before")
    @classmethod
    def copied_parameters(cls, value: object) -> object:
        return validate_strict_finite_json(value)

    @model_validator(mode="after")
    def exact_own_bootstrap(self) -> Self:
        assignment, grant, context = self.assignment, self.grant, self.context
        if (
            assignment.id != grant.subtask_id
            or assignment.id != context.producer_subtask_id
            or assignment.graph_revision_id != grant.graph_revision_id
            or assignment.graph_revision_id != context.graph_revision_id
            or assignment.current_grant_id != grant.id
            or assignment.physical_epoch != grant.physical_epoch
            or assignment.assignment_epoch != grant.assignment_epoch
            or assignment.assigned_worker_id != grant.worker_id
            or assignment.operator_uid != context.operator_uid
            or assignment.subtask_index != context.subtask_index
            or assignment.subtask_index >= self.operator_parallelism
            or assignment.output_stream_id != self.output.stream_id
            or self.output.port_uid != context.output_port_uid
            or self.task_run.id != assignment.manifest.task_run_id
            or self.task_run.flow_run_id != self.flow_run_id
            or assignment.manifest.flow_run_id != self.flow_run_id
            or self.task_run.flow_run_run_count != self.flow_run_run_count
            or self.task_run.task_key != self.task_key
            or self.task_run.task_version != self.task_version
            or self.task_run.empirical_policy.model_dump(mode="json")
            != self.task_policy.model_dump(mode="json")
            or self.effective_tags != sorted(set(self.effective_tags))
            or sorted(set(self.task_run.tags)) != self.effective_tags
            or self.reserved_tag_names != [f"tag:{tag}" for tag in self.effective_tags]
            or grant.state != "REGISTERED"
            or assignment.manifest.state != "ACTIVE"
            or not assignment.physical_slot_reserved
            or assignment.physical_slot_released
        ):
            raise ValueError(
                "bootstrap differs from its frozen own task/grant/topology"
            )
        if assignment.manifest.role == "SOURCE":
            if self.input is not None or self.input_key is not None:
                raise ValueError("SOURCE bootstrap has no input")
        elif (
            self.input is None
            or self.input_key is None
            or self.input.input_stream_id != assignment.input_stream_id
        ):
            raise ValueError("operator bootstrap requires its exact stable subscriber")
        return self


class StreamDAGBootstrapReadyRequest(StreamDAGExecutionWire):
    operation_id: UUID
    bootstrap_digest: Digest
    signature: Signature


class StreamDAGBootstrapReadyResult(StreamDAGExecutionWire):
    operation_id: UUID
    graph_revision_id: UUID
    subtask_id: UUID
    grant_id: UUID
    physical_epoch: int = Field(ge=1, le=64, strict=True)
    bootstrap_digest: Digest
    ready_at: DateTime
    request_digest: Digest
    outcome_digest: Digest


class StreamDAGActivationObservation(StreamDAGExecutionWire):
    graph_revision_id: UUID
    subtask_id: UUID
    grant_id: UUID
    physical_epoch: int = Field(ge=1, le=64, strict=True)
    phase: Literal["WAITING", "ACTIVE", "STOPPING", "TERMINAL"]
    activation_operation_id: UUID | None

    @model_validator(mode="after")
    def active_requires_activation(self) -> Self:
        if self.phase == "ACTIVE" and self.activation_operation_id is None:
            raise ValueError("ACTIVE requires durable graph activation")
        return self


class StreamDAGSourceOpenRequest(StreamDAGExecutionWire):
    context: StreamDAGRouteContext


class StreamDAGConsumerOpenRequest(StreamDAGExecutionWire):
    context: StreamDAGRouteContext


class StreamDAGConsumerReadRequest(StreamDAGExecutionWire):
    context: StreamDAGRouteContext
    binding: ConsumerBinding


class StreamDAGSourceFinishRequest(StreamDAGExecutionWire):
    operation_id: UUID
    context: StreamDAGRouteContext
    binding: SourceBinding
    expected_position: int = Field(ge=0, strict=True)


class StreamDAGConsumerFinishRequest(StreamDAGExecutionWire):
    operation_id: UUID
    context: StreamDAGRouteContext
    binding: ConsumerBinding


class StreamDAGProducerFinishReceipt(StreamDAGExecutionWire):
    operation_id: UUID
    graph_revision_id: UUID
    producer_subtask_id: UUID
    producer_stream_id: UUID
    kind: Literal["SOURCE_EOF", "CONSUMER_EOF"]
    tail_sequence: int = Field(ge=0, strict=True)
    accepted_source_position: int | None = Field(ge=0, strict=True)
    accepted_input_sequence: int | None = Field(ge=0, strict=True)
    semantic_digest: Digest
    outcome_digest: Digest

    @model_validator(mode="after")
    def exact_eof_coordinate(self) -> Self:
        if self.kind == "SOURCE_EOF":
            if (
                self.accepted_source_position is None
                or self.accepted_input_sequence is not None
            ):
                raise ValueError("SOURCE EOF requires only accepted source position")
        elif (
            self.accepted_input_sequence is None
            or self.accepted_source_position is not None
        ):
            raise ValueError("CONSUMER EOF requires only accepted input sequence")
        return self


class StreamDAGWorkerPollRequest(StreamDAGExecutionWire):
    protocol: Literal["partitioned-dag-v1"] = "partitioned-dag-v1"
    protocol_version: Literal[1] = 1
    principal: StreamWorkerPrincipal
    active_subtask_ids: list[UUID] = Field(default_factory=list[UUID], max_length=64)
    limit: int = Field(default=64, ge=1, le=128, strict=True)

    @field_validator("protocol_version", mode="before")
    @classmethod
    def exact_protocol_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("DAG worker protocol version requires exact integer 1")
        return value

    @model_validator(mode="after")
    def bounded_active_set(self) -> Self:
        if (
            len(set(self.active_subtask_ids)) != len(self.active_subtask_ids)
            or len(self.active_subtask_ids) > self.limit
        ):
            raise ValueError("active subtask identities must be distinct and fit limit")
        return self


class StreamDAGWorkerStartupAbortRequest(StreamDAGExecutionWire):
    principal: StreamWorkerPrincipal
    start_operation_id: UUID
    graph_digest: Digest
    routing_digest: Digest


class StreamDAGWorkerStartupAbortResult(StreamDAGExecutionWire):
    control_operation_id: UUID
    execution: StreamDAGExecution


class StreamDAGLaunchPrepareRequest(StreamDAGExecutionWire):
    principal: StreamWorkerPrincipal
    operation_id: UUID
    start_operation_id: UUID
    replacement_operation_id: UUID | None


class StreamDAGLaunchRecordRequest(StreamDAGExecutionWire):
    principal: StreamWorkerPrincipal
    operation_id: UUID
    child_process_id: int = Field(ge=1, strict=True)
    process_start_ticks: int = Field(ge=1, strict=True)
    local_journal_digest: Digest


class StreamDAGLaunchAttempt(StreamDAGExecutionWire):
    id: UUID
    graph_revision_id: UUID
    subtask_id: UUID
    start_operation_id: UUID
    worker_id: UUID
    worker_generation_digest: Digest
    attempt_index: int = Field(ge=1, le=64, strict=True)
    expected_grant_id: UUID | None
    expected_physical_epoch: int = Field(ge=0, le=64, strict=True)
    replacement_operation_id: UUID | None
    state: Literal["PREPARED", "PID_RECORDED", "CLOSED"]
    child_process_id: int | None = Field(ge=1, strict=True)
    process_start_ticks: int | None = Field(ge=1, strict=True)
    local_journal_digest: Digest | None
    grant_id: UUID | None

    @model_validator(mode="after")
    def complete_process_identity(self) -> Self:
        if (self.child_process_id is None) != (self.process_start_ticks is None):
            raise ValueError("launch process identity must be complete")
        if (self.expected_grant_id is None) != (self.expected_physical_epoch == 0):
            raise ValueError("initial/expected grant identity must be complete")
        if (self.replacement_operation_id is None) != (self.expected_grant_id is None):
            raise ValueError("replacement launch requires its exact authorization")
        if self.state == "PREPARED" and self.child_process_id is not None:
            raise ValueError("PREPARED cannot carry a recorded process")
        if self.state == "PID_RECORDED" and (
            self.child_process_id is None or self.local_journal_digest is None
        ):
            raise ValueError("PID_RECORDED requires process and local journal identity")
        if self.state == "CLOSED" and self.local_journal_digest is None:
            raise ValueError("CLOSED requires its recovered closed local journal")
        if self.grant_id is not None and (
            self.state == "PREPARED"
            or self.child_process_id is None
            or self.local_journal_digest is None
        ):
            raise ValueError("a grant can only bind an already recorded process")
        return self


class StreamDAGLaunchOperationReceipt(StreamDAGExecutionWire):
    operation_id: UUID
    launch_attempt_id: UUID
    kind: Literal["PREPARE", "RECORD"]
    request_digest: Digest
    outcome_digest: Digest
    accepted_at: DateTime


class StreamDAGLaunchOperationResult(StreamDAGExecutionWire):
    receipt: StreamDAGLaunchOperationReceipt
    current: StreamDAGLaunchAttempt
    disposition: Literal["SPAWN_ALLOWED", "PREFLIGHT_ALLOWED", "STOP_REQUIRED"]

    @model_validator(mode="after")
    def own_current_launch(self) -> Self:
        if self.receipt.launch_attempt_id != self.current.id:
            raise ValueError("launch receipt/current identities disagree")
        if self.disposition == "SPAWN_ALLOWED" and self.current.state != "PREPARED":
            raise ValueError("only PREPARED permits spawning")
        if (
            self.disposition == "PREFLIGHT_ALLOWED"
            and self.current.state != "PID_RECORDED"
        ):
            raise ValueError("only PID_RECORDED permits preflight")
        return self


class StreamDAGLaunchDeathProof(StreamDAGExecutionWire):
    kind: Literal["DEATH"]
    death_witness: NativeStreamProcessDeathWitness


class StreamDAGLaunchUnstartedProof(StreamDAGExecutionWire):
    kind: Literal["CLOSED_UNSTARTED"]
    local_journal_digest: Digest
    recovery_complete: Literal[True]
    spawn_fence_closed: Literal[True]

    @field_validator("recovery_complete", "spawn_fence_closed", mode="before")
    @classmethod
    def exact_true(cls, value: object) -> object:
        if value is not True:
            raise ValueError("closed local journal attestation requires exact true")
        return value


class StreamDAGLaunchCloseRequest(StreamDAGExecutionWire):
    principal: StreamWorkerPrincipal
    operation_id: UUID
    proof: Annotated[
        StreamDAGLaunchDeathProof | StreamDAGLaunchUnstartedProof,
        Field(discriminator="kind"),
    ]


class StreamDAGLaunchCloseReceipt(StreamDAGExecutionWire):
    operation_id: UUID
    launch_attempt_id: UUID
    graph_revision_id: UUID
    subtask_id: UUID
    attempt_index: int = Field(ge=1, le=64, strict=True)
    proof_kind: Literal["DEATH", "CLOSED_UNSTARTED"]
    closed_at: DateTime
    request_digest: Digest
    outcome_digest: Digest


class StreamDAGWorkerStopRequest(StreamDAGExecutionWire):
    principal: StreamWorkerPrincipal
    operation_id: UUID
    control_operation_id: UUID


class StreamDAGStopReceipt(StreamDAGExecutionWire):
    operation_id: UUID
    control_operation_id: UUID
    graph_revision_id: UUID
    subtask_id: UUID
    task_run_id: UUID
    grant_id: UUID | None
    physical_epoch: int = Field(ge=0, le=64, strict=True)
    state: Literal["STOPPED"] = "STOPPED"
    task_state: Literal["COMPLETED", "FAILED", "CRASHED", "CANCELLED"]
    tag_allocation_released: bool = Field(strict=True)
    physical_slot_released: Literal[True] = True
    all_attempts_closed_digest: Digest
    outcome_digest: Digest

    @model_validator(mode="after")
    def exact_grant_shape(self) -> Self:
        if (self.grant_id is None) != (self.physical_epoch == 0):
            raise ValueError("stop grant and epoch must be complete")
        return self


class StreamDAGWorkerAssignment(StreamDAGExecutionWire):
    flow_run_id: UUID
    graph_digest: Digest
    routing_digest: Digest
    placement_digest: Digest
    start_operation_id: UUID
    assignment: StreamDAGExecutionAssignment
    phase: Literal["STARTING", "ACTIVE", "STOPPING", "TERMINAL"]
    current_grant: StreamSubtaskGrant | None
    bootstrap_ready: bool = Field(strict=True)
    working_directory: str
    task_configuration: StreamWorkerTaskConfiguration
    task_state: StateType
    task_run_count: int = Field(ge=0, strict=True)
    task_retriable: bool | None = Field(strict=True)
    output_state: Literal["OPEN", "COMPLETED", "FAILED"]
    output_attempt: int = Field(ge=0, strict=True)
    control: StreamDAGControlProjection | None
    launch_attempts: list[StreamDAGLaunchAttempt] = Field(max_length=64)

    @field_validator("working_directory")
    @classmethod
    def bounded_directory(cls, value: str) -> str:
        if (
            not value.startswith("/")
            or "\x00" in value
            or len(value.encode("utf-8")) > 4096
        ):
            raise ValueError("worker directory must be an absolute bounded Linux path")
        parts = value.split("/")
        if value != "/" and (
            "" in parts[1:] or any(part in {".", ".."} for part in parts)
        ):
            raise ValueError("worker directory must be normalized")
        return value

    @model_validator(mode="after")
    def exact_own_attempt_set(self) -> Self:
        if self.control is not None and self.control.kind == "PREVIEW_ABORT":
            raise ValueError("unassigned preview controls cannot be worker assignments")
        if (
            self.assignment.assigned_worker_id is None
            or not self.assignment.physical_slot_reserved
        ):
            raise ValueError(
                "worker assignment requires its persisted physical reservation"
            )
        if len({row.id for row in self.launch_attempts}) != len(self.launch_attempts):
            raise ValueError("launch attempt identities must be unique")
        if [row.attempt_index for row in self.launch_attempts] != list(
            range(1, len(self.launch_attempts) + 1)
        ):
            raise ValueError("complete launch attempts must be contiguous and ordered")
        if len({row.worker_generation_digest for row in self.launch_attempts}) > 1:
            raise ValueError("launch attempts must bind one exact worker generation")
        if any(
            row.subtask_id != self.assignment.id
            or row.graph_revision_id != self.assignment.graph_revision_id
            or row.start_operation_id != self.start_operation_id
            or row.worker_id != self.assignment.assigned_worker_id
            for row in self.launch_attempts
        ):
            raise ValueError("launch attempt is outside the assigned target")
        if self.assignment.manifest.flow_run_id != self.flow_run_id:
            raise ValueError("assignment belongs to another FlowRun")
        if self.bootstrap_ready and self.current_grant is None:
            raise ValueError("bootstrap readiness requires its current grant")
        if (self.current_grant is None) != (self.assignment.current_grant_id is None):
            raise ValueError("assignment and current grant presence must agree")
        if self.current_grant is not None:
            grant = self.current_grant
            matching = [row for row in self.launch_attempts if row.grant_id == grant.id]
            if (
                grant.subtask_id != self.assignment.id
                or grant.id != self.assignment.current_grant_id
                or grant.physical_epoch != self.assignment.physical_epoch
                or grant.graph_revision_id != self.assignment.graph_revision_id
                or grant.assignment_epoch != self.assignment.assignment_epoch
                or grant.worker_id != self.assignment.assigned_worker_id
                or len(matching) != 1
                or grant.physical_epoch != matching[0].expected_physical_epoch + 1
                or (
                    grant.child_process_id is not None
                    and (grant.child_process_id, grant.process_start_ticks)
                    != (matching[0].child_process_id, matching[0].process_start_ticks)
                )
            ):
                raise ValueError("current grant differs from its exact assigned launch")
        if self.phase == "STOPPING" and self.control is None:
            raise ValueError("STOPPING assignment requires its exact control")
        return self
