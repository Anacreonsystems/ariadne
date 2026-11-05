"""Wire contracts for native streaming deployment enrollment."""

from typing import ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from prefect.types import DateTime


class _NativeDeploymentRequest(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")


class NativeStreamDeclaration(BaseModel):
    """Secret-free native capability persisted in deployment job variables."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    protocol_version: Literal[1] = 1
    profile: Literal["bounded-linear-v1", "partitioned-fixed-v1", "partitioned-dag-v1"]
    flow_version: str = Field(min_length=1, max_length=255)
    flow_code_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    resource_profile: Literal["bounded-default-v1"]
    flow_retries: int = Field(ge=1, le=63, strict=True)
    flow_retry_delay_seconds: float | None = Field(
        default=None, ge=0, allow_inf_nan=False
    )

    @field_validator("schema_version", "protocol_version", mode="before")
    @classmethod
    def versions_are_integers(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("native declaration versions must be integers")
        return value


class NativeStreamWorkerEnrollRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    expected_generation: Literal[0] = 0
    declaration_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    worker_id: UUID
    work_pool_id: UUID
    launcher_instance_id: UUID
    launcher_host_id: UUID
    launcher_boot_id: UUID
    launcher_process_id: int = Field(ge=1, strict=True)
    launcher_start_ticks: int = Field(ge=1, strict=True)
    launch_profile: Literal["linux-local-pidfd-v1"]
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

    @field_validator("expected_generation", mode="before")
    @classmethod
    def expected_generation_is_integer_zero(cls, value: object) -> object:
        if type(value) is not int or value != 0:
            raise ValueError("expected_generation must be the integer 0")
        return value


class NativeStreamWorkerEnrollment(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    deployment_id: UUID
    deployment_version: str | None
    work_pool_id: UUID
    worker_id: UUID
    snapshot_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation: int = Field(ge=1, le=64, strict=True)
    holder_id: UUID
    operation_id: UUID
    launcher_instance_id: UUID
    launch_profile: Literal["linux-local-pidfd-v1"]
    state: Literal["ENROLLED", "PROCESS_REGISTERED", "RELEASED"]
    created_at: DateTime


class NativeStreamProcessRegistrationRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    enrollment_operation_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    launcher_instance_id: UUID
    host_id: UUID
    boot_id: UUID
    child_process_id: int = Field(ge=1, strict=True)
    process_start_ticks: int = Field(ge=1, strict=True)
    launch_nonce_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class NativeStreamLaunchProjection(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    deployment_id: UUID
    work_pool_id: UUID
    worker_id: UUID
    snapshot_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation: int = Field(ge=1, le=64, strict=True)
    enrollment_operation_id: UUID
    launcher_instance_id: UUID
    launch_profile: Literal["linux-local-pidfd-v1"]
    state: Literal["ENROLLED", "PROCESS_REGISTERED", "RELEASED"]
    created_at: DateTime
    process_registered_at: DateTime | None
    last_renewed_at: DateTime | None


class NativeStreamDeploymentPlan(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    deployment_id: UUID
    deployment_version: str | None
    work_pool_id: UUID
    declaration: NativeStreamDeclaration
    declaration_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: Literal["ACTIVE", "RELEASED"]
    created_at: DateTime
    current_launch: NativeStreamLaunchProjection | None
    source_admission_closed: bool
    cancel_requested: bool


class NativeStreamTaskProjection(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    task_run_id: UUID
    submission_key: str
    role: Literal["SOURCE", "OPERATOR"]
    protocol: Literal["source-v1", "yield-v2"]
    task_state: str
    run_count: int = Field(ge=0, strict=True)
    input_stream_id: UUID | None
    output_stream_id: UUID
    stream_state: Literal["OPEN", "COMPLETED", "FAILED"]
    record_count: int = Field(ge=0, strict=True)
    byte_count: int = Field(ge=0, strict=True)
    record_limit: int = Field(ge=1, strict=True)
    byte_limit: int = Field(ge=1, strict=True)
    last_sequence: int = Field(ge=0, strict=True)
    tail_sequence: int | None = Field(default=None, ge=0, strict=True)
    completed_through: int | None = Field(default=None, ge=0, strict=True)
    inflight_sequence: int | None = Field(default=None, ge=1, strict=True)
    input_tail_sequence: int | None = Field(default=None, ge=0, strict=True)
    pending_inputs: int | None = Field(default=None, ge=0, strict=True)


class NativeStreamFiniteProjection(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    task_run_id: UUID
    submission_key: str
    phase: Literal["PREFIX", "TAIL"]
    task_state: str
    run_count: int = Field(ge=0, strict=True)
    result_committed: bool


class NativeStreamFlowProjection(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    flow_state: str
    observed_at: DateTime
    generation: int | None = Field(default=None, ge=1, le=64, strict=True)
    authority_state: str | None
    prepared_generation: int | None = Field(default=None, ge=0, le=64, strict=True)
    manifest_sealed: bool
    source_admission_closed: bool
    cancel_requested: bool
    launch_state: Literal["ENROLLED", "PROCESS_REGISTERED", "RELEASED"] | None
    worker_id: UUID | None
    items: list[NativeStreamTaskProjection] = Field(max_length=128)
    finite_items: list[NativeStreamFiniteProjection] = Field(max_length=2)


class NativeStreamRecoveryCandidate(BaseModel):
    """Secret-free process identity available to a trusted same-host worker."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    deployment_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    work_pool_id: UUID
    worker_id: UUID
    launcher_instance_id: UUID
    enrollment_operation_id: UUID
    launch_profile: Literal["linux-local-pidfd-v1"]
    identity_kind: Literal["LAUNCHER", "CHILD"]
    host_id: UUID
    boot_id: UUID
    child_process_id: int = Field(ge=1, strict=True)
    process_start_ticks: int = Field(ge=1, strict=True)
    last_renewed_at: DateTime
    verification_not_before: DateTime


class NativeStreamLaunchHeartbeatRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    enrollment_operation_id: UUID


class NativeStreamLaunchHeartbeat(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    enrollment_operation_id: UUID
    last_renewed_at: DateTime


class NativeStreamReplacementAuthorizeRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    expected_generation: int = Field(ge=1, le=64, strict=True)
    enrollment_operation_id: UUID
    worker_id: UUID
    work_pool_id: UUID
    host_id: UUID
    identity_kind: Literal["LAUNCHER", "CHILD"]
    challenge_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )


class NativeStreamReplacementAuthorization(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    operation_id: UUID
    expected_generation: int = Field(ge=1, le=64, strict=True)
    worker_id: UUID
    work_pool_id: UUID
    host_id: UUID
    identity_kind: Literal["LAUNCHER", "CHILD"]
    state: Literal["AUTHORIZED", "CONFIRMED", "RELEASED"]
    created_at: DateTime


class NativeStreamProcessDeathWitness(_NativeDeploymentRequest):
    kind: Literal["WAITPID", "PIDFD_EXIT", "IDENTITY_GONE", "BOOT_CHANGED"]
    observed_at: DateTime
    host_id: UUID
    boot_id: UUID
    child_process_id: int = Field(ge=1, strict=True)
    process_start_ticks: int = Field(ge=1, strict=True)
    exit_code: int | None = Field(default=None, ge=-255, le=255, strict=True)
    observed_boot_id: UUID | None = None

    @field_validator("observed_at")
    @classmethod
    def observed_at_is_aware(cls, value: DateTime) -> DateTime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must include an explicit timezone")
        return value

    @model_validator(mode="after")
    def witness_shape_is_exact(self) -> "NativeStreamProcessDeathWitness":
        if self.kind == "WAITPID" and self.exit_code is None:
            raise ValueError("WAITPID requires exit_code")
        if self.kind != "WAITPID" and self.exit_code is not None:
            raise ValueError("exit_code is only valid for WAITPID")
        if self.kind == "BOOT_CHANGED":
            if self.observed_boot_id is None or self.observed_boot_id == self.boot_id:
                raise ValueError("BOOT_CHANGED requires a distinct observed_boot_id")
        elif self.observed_boot_id is not None:
            raise ValueError("observed_boot_id is only valid for BOOT_CHANGED")
        return self


class NativeStreamReplacementConfirmRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    challenge_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    launcher_instance_id: UUID
    launcher_host_id: UUID
    launcher_boot_id: UUID
    launcher_process_id: int = Field(ge=1, strict=True)
    launcher_start_ticks: int = Field(ge=1, strict=True)
    new_holder_id: UUID
    new_holder_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    new_holder_event_public_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    new_supervisor_id: UUID
    new_supervisor_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    death_witness: NativeStreamProcessDeathWitness


class NativeStreamControlRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    expected_generation: int = Field(ge=1, le=64, strict=True)
    reason: str = Field(min_length=1, max_length=1024)


class NativeStreamControlResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")

    flow_run_id: UUID
    operation_id: UUID
    generation: int = Field(ge=1, le=64, strict=True)
    kind: Literal[
        "DRAIN",
        "CANCEL",
        "CANCEL_FINALIZED",
        "SPAWN_FAILED",
        "VERIFIED_RETIRED",
    ]
    source_admission_closed: bool
    cancel_requested: bool
    created_at: DateTime


class NativeStreamUnstartedRetireRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    enrollment_operation_id: UUID
    error: str = Field(min_length=1, max_length=1024)


class NativeStreamCancelFinalizeRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    expected_generation: int = Field(ge=1, le=64, strict=True)
    enrollment_operation_id: UUID
    challenge_capability: str | None = Field(
        default=None,
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    death_witness: NativeStreamProcessDeathWitness


class NativeStreamVerifiedRetireRequest(_NativeDeploymentRequest):
    flow_run_id: UUID
    operation_id: UUID
    expected_generation: int = Field(ge=1, le=64, strict=True)
    enrollment_operation_id: UUID
    challenge_capability: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        repr=False,
    )
    death_witness: NativeStreamProcessDeathWitness
    error: str = Field(min_length=1, max_length=1024)
