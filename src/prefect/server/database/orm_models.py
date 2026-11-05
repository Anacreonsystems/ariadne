import datetime
import uuid
from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

import sqlalchemy as sa
from sqlalchemy import FetchedValue
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    declared_attr,
    mapped_column,
    registry,
    relationship,
    synonym,
)
from sqlalchemy.orm.decl_api import registry as RegistryType
from sqlalchemy.sql import roles
from sqlalchemy.sql.functions import coalesce

import prefect.server.schemas as schemas
from prefect.server.events.actions import ServerActionTypes
from prefect.server.events.schemas.automations import (
    AutomationSort,
    Firing,
    ServerTriggerTypes,
)
from prefect.server.events.schemas.events import ReceivedEvent
from prefect.server.schemas.statuses import (
    DeploymentStatus,
    WorkerStatus,
    WorkPoolStatus,
    WorkQueueStatus,
)
from prefect.server.utilities.database import (
    CAMEL_TO_SNAKE,
    JSON,
    UUID,
    GenerateUUID,
    Pydantic,
    Timestamp,
)
from prefect.server.utilities.encryption import decrypt_fernet, encrypt_fernet
from prefect.types._datetime import DateTime, now
from prefect.utilities.names import generate_slug

# for 'plain JSON' columns, use the postgresql variant (which comes with an
# extra operator) and fall back to the generic JSON variant for SQLite
sa_JSON: postgresql.JSON = postgresql.JSON().with_variant(sa.JSON(), "sqlite")

# Stream values preserve protocol-significant JSON scalar semantics. PostgreSQL
# JSONB normalizes signed zero, and Prefect's general JSON type maps Python
# `None` to SQL NULL; both would change a valid value after durable acceptance.
stream_value_JSON: postgresql.JSON = postgresql.JSON(none_as_null=False).with_variant(
    sa.JSON(none_as_null=False), "sqlite"
)


class Base(DeclarativeBase):
    """
    Base SQLAlchemy model that automatically infers the table name
    and provides ID, created, and updated columns
    """

    registry: ClassVar[RegistryType] = registry(
        metadata=sa.schema.MetaData(
            # define naming conventions for our Base class to use
            # sqlalchemy will use the following templated strings
            # to generate the names of indices, constraints, and keys
            #
            # we offset the table name with two underscores (__) to
            # help differentiate, for example, between "flow_run.state_type"
            # and "flow_run_state.type".
            #
            # more information on this templating and available
            # customization can be found here
            # https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData
            #
            # this also allows us to avoid having to specify names explicitly
            # when using sa.ForeignKey.use_alter = True
            # https://docs.sqlalchemy.org/en/14/core/constraints.html
            naming_convention={
                "ix": "ix_%(table_name)s__%(column_0_N_name)s",
                "uq": "uq_%(table_name)s__%(column_0_N_name)s",
                "ck": "ck_%(table_name)s__%(constraint_name)s",
                "fk": "fk_%(table_name)s__%(column_0_N_name)s__%(referred_table_name)s",
                "pk": "pk_%(table_name)s",
            }
        ),
        type_annotation_map={
            uuid.UUID: UUID,
            DateTime: Timestamp,
        },
    )

    # required in order to access columns with server defaults
    # or SQL expression defaults, subsequent to a flush, without
    # triggering an expired load
    #
    # this allows us to load attributes with a server default after
    # an INSERT, for example
    #
    # https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html#preventing-implicit-io-when-using-asyncsession
    __mapper_args__: dict[str, Any] = {"eager_defaults": True}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id})"

    @declared_attr.directive
    def __tablename__(cls) -> str:
        """
        By default, turn the model's camel-case class name
        into a snake-case table name. Override by providing
        an explicit `__tablename__` class property.
        """
        return CAMEL_TO_SNAKE.sub("_", cls.__name__).lower()

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        server_default=GenerateUUID(),
        default=uuid.uuid4,
    )

    created: Mapped[DateTime] = mapped_column(
        server_default=sa.func.now(), default=lambda: now("UTC")
    )

    # onupdate is only called when statements are actually issued
    # against the database. until COMMIT is issued, this column
    # will not be updated
    updated: Mapped[DateTime] = mapped_column(
        index=True,
        server_default=sa.func.now(),
        default=lambda: now("UTC"),
        onupdate=sa.func.now(),
        server_onupdate=FetchedValue(),
    )


class Flow(Base):
    """SQLAlchemy mixin of a flow."""

    name: Mapped[str]
    tags: Mapped[list[str]] = mapped_column(JSON, server_default="[]", default=list)
    labels: Mapped[Optional[schemas.core.KeyValueLabels]] = mapped_column(JSON)

    flow_runs: Mapped[list["FlowRun"]] = relationship(
        back_populates="flow", lazy="raise"
    )
    deployments: Mapped[list["Deployment"]] = relationship(
        back_populates="flow", lazy="raise"
    )

    __table_args__: Any = (
        sa.UniqueConstraint("name"),
        sa.Index("ix_flow__created", "created"),
        sa.Index("trgm_ix_flow_name", "name", postgresql_using="gin").ddl_if(
            dialect="postgresql"
        ),
    )


class FlowRunState(Base):
    """SQLAlchemy mixin of a flow run state."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="cascade")
    )

    type: Mapped[schemas.states.StateType] = mapped_column(
        sa.Enum(schemas.states.StateType, name="state_type"), index=True
    )
    timestamp: Mapped[DateTime] = mapped_column(
        server_default=sa.func.now(), default=lambda: now("UTC")
    )
    name: Mapped[str] = mapped_column(index=True)
    message: Mapped[Optional[str]]
    state_details: Mapped[schemas.states.StateDetails] = mapped_column(
        Pydantic(schemas.states.StateDetails),
        server_default="{}",
        default=schemas.states.StateDetails,
    )
    _data: Mapped[Optional[Any]] = mapped_column(JSON, name="data")

    result_artifact_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("artifact.id", ondelete="SET NULL", use_alter=True),
        index=True,
    )

    _result_artifact: Mapped[Optional["Artifact"]] = relationship(
        lazy="selectin",
        foreign_keys=[result_artifact_id],
        primaryjoin="Artifact.id==FlowRunState.result_artifact_id",
    )

    @hybrid_property
    def data(self) -> Optional[Any]:
        if self._data:
            # ensures backwards compatibility for results stored on state objects
            return self._data
        if not self.result_artifact_id:
            # do not try to load the relationship if there's no artifact id
            return None
        if TYPE_CHECKING:
            assert self._result_artifact is not None
        return self._result_artifact.data

    flow_run: Mapped["FlowRun"] = relationship(lazy="raise", foreign_keys=[flow_run_id])

    def as_state(self) -> schemas.states.State:
        return schemas.states.State.model_validate(self, from_attributes=True)

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> Iterable[sa.Index]:
        return (
            sa.Index(
                "uq_flow_run_state__flow_run_id_timestamp_desc",
                cls.flow_run_id,
                cls.timestamp.desc(),
                unique=True,
            ),
        )


class TaskRunState(Base):
    """SQLAlchemy model of a task run state."""

    # this column isn't explicitly indexed because it is included in
    # the unique compound index on (task_run_id, timestamp)
    task_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("task_run.id", ondelete="cascade")
    )

    type: Mapped[schemas.states.StateType] = mapped_column(
        sa.Enum(schemas.states.StateType, name="state_type"), index=True
    )
    timestamp: Mapped[DateTime] = mapped_column(
        server_default=sa.func.now(), default=lambda: now("UTC")
    )
    name: Mapped[str] = mapped_column(index=True)
    message: Mapped[Optional[str]]
    state_details: Mapped[schemas.states.StateDetails] = mapped_column(
        Pydantic(schemas.states.StateDetails),
        server_default="{}",
        default=schemas.states.StateDetails,
    )
    _data: Mapped[Optional[Any]] = mapped_column(JSON, name="data")

    result_artifact_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("artifact.id", ondelete="SET NULL", use_alter=True), index=True
    )

    _result_artifact: Mapped[Optional["Artifact"]] = relationship(
        lazy="selectin",
        foreign_keys=[result_artifact_id],
        primaryjoin="Artifact.id==TaskRunState.result_artifact_id",
    )

    @hybrid_property
    def data(self) -> Optional[Any]:
        if self._data:
            # ensures backwards compatibility for results stored on state objects
            return self._data
        if not self.result_artifact_id:
            # do not try to load the relationship if there's no artifact id
            return None
        if TYPE_CHECKING:
            assert self._result_artifact is not None
        return self._result_artifact.data

    task_run: Mapped["TaskRun"] = relationship(lazy="raise", foreign_keys=[task_run_id])

    def as_state(self) -> schemas.states.State:
        return schemas.states.State.model_validate(self, from_attributes=True)

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> Iterable[sa.Index]:
        return (
            sa.Index(
                "uq_task_run_state__task_run_id_timestamp_desc",
                cls.task_run_id,
                cls.timestamp.desc(),
                unique=True,
            ),
        )


class Artifact(Base):
    """
    SQLAlchemy model of artifacts.
    """

    key: Mapped[Optional[str]] = mapped_column(index=True)

    task_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(index=True)

    flow_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(index=True)

    type: Mapped[Optional[str]]
    data: Mapped[Optional[Any]] = mapped_column(sa_JSON)
    description: Mapped[Optional[str]]

    # Suffixed with underscore as attribute name 'metadata' is reserved for the MetaData instance when using a declarative base class.
    metadata_: Mapped[Optional[dict[str, str]]] = mapped_column(sa_JSON)

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> Iterable[sa.Index]:
        return (
            sa.Index(
                "ix_artifact__key",
                cls.key,
            ),
            sa.Index(
                "ix_artifact__key_created_desc",
                cls.key,
                cls.created.desc(),
                postgresql_include=[
                    "id",
                    "updated",
                    "type",
                    "task_run_id",
                    "flow_run_id",
                ],
            ),
        )


class ArtifactCollection(Base):
    key: Mapped[str]

    latest_id: Mapped[uuid.UUID]

    task_run_id: Mapped[Optional[uuid.UUID]]

    flow_run_id: Mapped[Optional[uuid.UUID]]

    type: Mapped[Optional[str]]
    data: Mapped[Optional[Any]] = mapped_column(sa_JSON)
    description: Mapped[Optional[str]]
    metadata_: Mapped[Optional[dict[str, str]]] = mapped_column(sa_JSON)

    __table_args__: Any = (
        sa.UniqueConstraint("key"),
        sa.Index(
            "ix_artifact_collection__key_latest_id",
            "key",
            "latest_id",
        ),
    )


class TaskRunStateCache(Base):
    """
    SQLAlchemy model of a task run state cache.
    """

    cache_key: Mapped[str] = mapped_column()
    cache_expiration: Mapped[Optional[DateTime]]
    task_run_state_id: Mapped[uuid.UUID]

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> Iterable[sa.Index]:
        return (
            sa.Index(
                "ix_task_run_state_cache__cache_key_created_desc",
                cls.cache_key,
                cls.created.desc(),
            ),
        )


class Run(Base):
    """
    Common columns and logic for FlowRun and TaskRun models
    """

    __abstract__ = True

    name: Mapped[str] = mapped_column(default=lambda: generate_slug(2), index=True)
    state_type: Mapped[Optional[schemas.states.StateType]] = mapped_column(
        sa.Enum(schemas.states.StateType, name="state_type")
    )
    state_name: Mapped[Optional[str]]
    state_timestamp: Mapped[Optional[DateTime]]
    run_count: Mapped[int] = mapped_column(server_default="0", default=0)
    expected_start_time: Mapped[Optional[DateTime]]
    next_scheduled_start_time: Mapped[Optional[DateTime]]
    start_time: Mapped[Optional[DateTime]]
    end_time: Mapped[Optional[DateTime]]
    total_run_time: Mapped[datetime.timedelta] = mapped_column(
        server_default="0", default=datetime.timedelta(0)
    )

    @hybrid_property
    def estimated_run_time(self) -> datetime.timedelta:
        """Total run time is incremented in the database whenever a RUNNING
        state is exited. To give up-to-date estimates, we estimate incremental
        run time for any runs currently in a RUNNING state."""
        if self.state_type and self.state_type == schemas.states.StateType.RUNNING:
            if TYPE_CHECKING:
                assert self.state_timestamp is not None
            return self.total_run_time + (now("UTC") - self.state_timestamp)
        else:
            return self.total_run_time

    @estimated_run_time.inplace.expression
    @classmethod
    def _estimated_run_time_expression(cls) -> sa.Label[datetime.timedelta]:
        return (
            sa.select(
                sa.case(
                    (
                        cls.state_type == schemas.states.StateType.RUNNING,
                        sa.func.interval_add(
                            cls.total_run_time,
                            sa.func.date_diff(sa.func.now(), cls.state_timestamp),
                        ),
                    ),
                    else_=cls.total_run_time,
                )
            )
            # add a correlate statement so this can reuse the `FROM` clause
            # of any parent query
            .correlate(cls)
            .label("estimated_run_time")
        )

    @hybrid_property
    def estimated_start_time_delta(self) -> datetime.timedelta:
        """The delta to the expected start time (or "lateness") is computed as
        the difference between the actual start time and expected start time. To
        give up-to-date estimates, we estimate lateness for any runs that don't
        have a start time and are not in a final state and were expected to
        start already."""
        if (
            self.start_time
            and self.expected_start_time is not None
            and self.start_time > (self.expected_start_time)
        ):
            return self.start_time - self.expected_start_time
        elif (
            self.start_time is None
            and self.expected_start_time
            and self.expected_start_time < now("UTC")
            and self.state_type not in schemas.states.TERMINAL_STATES
        ):
            return now("UTC") - self.expected_start_time
        else:
            return datetime.timedelta(0)

    @estimated_start_time_delta.inplace.expression
    @classmethod
    def _estimated_start_time_delta_expression(
        cls,
    ) -> sa.SQLColumnExpression[datetime.timedelta]:
        return sa.case(
            (
                cls.start_time > cls.expected_start_time,
                sa.func.date_diff(cls.start_time, cls.expected_start_time),
            ),
            (
                sa.and_(
                    cls.start_time.is_(None),
                    cls.state_type.not_in(schemas.states.TERMINAL_STATES),
                    cls.expected_start_time < sa.func.now(),
                ),
                sa.func.date_diff(sa.func.now(), cls.expected_start_time),
            ),
            else_=datetime.timedelta(0),
        )


class FlowRun(Run):
    """SQLAlchemy model of a flow run."""

    flow_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow.id", ondelete="cascade"), index=True
    )

    deployment_id: Mapped[Optional[uuid.UUID]] = mapped_column(index=True)
    work_queue_name: Mapped[Optional[str]] = mapped_column(index=True)
    flow_version: Mapped[Optional[str]] = mapped_column(index=True)
    deployment_version: Mapped[Optional[str]] = mapped_column(index=True)
    parameters: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default=dict
    )
    idempotency_key: Mapped[Optional[str]] = mapped_column()
    context: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default=dict
    )
    empirical_policy: Mapped[schemas.core.FlowRunPolicy] = mapped_column(
        Pydantic(schemas.core.FlowRunPolicy),
        server_default="{}",
        default=schemas.core.FlowRunPolicy,
    )
    tags: Mapped[list[str]] = mapped_column(JSON, server_default="[]", default=list)
    labels: Mapped[Optional[schemas.core.KeyValueLabels]] = mapped_column(JSON)

    created_by: Mapped[Optional[schemas.core.CreatedBy]] = mapped_column(
        Pydantic(schemas.core.CreatedBy)
    )

    infrastructure_pid: Mapped[Optional[str]]
    job_variables: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, server_default="{}", default=dict
    )

    infrastructure_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("block_document.id", ondelete="CASCADE"), index=True
    )

    parent_task_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("task_run.id", ondelete="SET NULL", use_alter=True), index=True
    )

    auto_scheduled: Mapped[bool] = mapped_column(server_default="0", default=False)

    # TODO remove this foreign key for significant delete performance gains
    state_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("flow_run_state.id", ondelete="SET NULL", use_alter=True),
        index=True,
    )

    work_queue_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("work_queue.id", ondelete="SET NULL"), index=True
    )

    # -------------------------- relationships

    # current states are eagerly loaded unless otherwise specified
    _state: Mapped[Optional["FlowRunState"]] = relationship(
        lazy="selectin",
        foreign_keys=[state_id],
        primaryjoin="FlowRunState.id==FlowRun.state_id",
    )

    @hybrid_property
    def state(self) -> Optional[FlowRunState]:
        return self._state

    @state.inplace.setter
    def _set_state(self, value: Optional[FlowRunState]) -> None:
        # because this is a slightly non-standard SQLAlchemy relationship, we
        # prefer an explicit setter method to a setter property, because
        # user expectations about SQLAlchemy attribute assignment might not be
        # met, namely that an unrelated (from SQLAlchemy's perspective) field of
        # the provided state is also modified. However, property assignment
        # still works because the ORM model's __init__ depends on it.
        return self.set_state(value)

    def set_state(self, state: Optional[FlowRunState]) -> None:
        """
        If a state is assigned to this run, populate its run id.

        This would normally be handled by the back-populated SQLAlchemy
        relationship, but because this is a one-to-one pointer to a
        one-to-many relationship, SQLAlchemy can't figure it out.
        """
        if state is not None:
            state.flow_run_id = self.id
        self._state = state

    flow: Mapped["Flow"] = relationship(back_populates="flow_runs", lazy="raise")

    task_runs: Mapped[list["TaskRun"]] = relationship(
        back_populates="flow_run",
        lazy="raise",
        # foreign_keys=lambda: [flow_run_id],
        primaryjoin="TaskRun.flow_run_id==FlowRun.id",
    )

    parent_task_run: Mapped[Optional["TaskRun"]] = relationship(
        back_populates="subflow_run",
        lazy="raise",
        foreign_keys=[parent_task_run_id],
    )

    work_queue: Mapped[Optional["WorkQueue"]] = relationship(
        lazy="selectin", foreign_keys=[work_queue_id]
    )

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> Iterable[sa.Index]:
        return (
            sa.Index(
                "uq_flow_run__flow_id_idempotency_key",
                cls.flow_id,
                cls.idempotency_key,
                unique=True,
            ),
            sa.Index(
                "ix_flow_run__coalesce_start_time_expected_start_time_desc",
                coalesce(cls.start_time, cls.expected_start_time).desc(),
            ),
            sa.Index(
                "ix_flow_run__coalesce_start_time_expected_start_time_asc",
                coalesce(cls.start_time, cls.expected_start_time).asc(),
            ),
            sa.Index(
                "ix_flow_run__state_type_coalesce_start_time",
                cls.state_type,
                coalesce(cls.start_time, cls.expected_start_time),
            ),
            sa.Index(
                "ix_flow_run__expected_start_time_desc",
                cls.expected_start_time.desc(),
            ),
            sa.Index(
                "ix_flow_run__next_scheduled_start_time_asc",
                cls.next_scheduled_start_time.asc(),
            ),
            sa.Index(
                "ix_flow_run__end_time_desc",
                cls.end_time.desc(),
            ),
            sa.Index(
                "ix_flow_run__start_time",
                cls.start_time,
            ),
            sa.Index(
                "ix_flow_run__state_type",
                cls.state_type,
            ),
            sa.Index(
                "ix_flow_run__state_name",
                cls.state_name,
            ),
            sa.Index(
                "ix_flow_run__state_timestamp",
                cls.state_timestamp,
            ),
            sa.Index("trgm_ix_flow_run_name", cls.name, postgresql_using="gin").ddl_if(
                dialect="postgresql"
            ),
            sa.Index(
                # index names are at most 63 characters long.
                "ix_flow_run__scheduler_deployment_id_auto_scheduled_next_schedu",
                cls.deployment_id,
                cls.auto_scheduled,
                cls.next_scheduled_start_time,
                postgresql_where=cls.state_type == schemas.states.StateType.SCHEDULED,
                sqlite_where=cls.state_type == schemas.states.StateType.SCHEDULED,
            ),
        )


class StreamPartition(Base):
    """A durable stream owned by a flow run."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    protocol_version: Mapped[int] = mapped_column(server_default="1", default=1)
    state: Mapped[str] = mapped_column(server_default="OPEN", default="OPEN")
    error: Mapped[Optional[str]] = mapped_column(sa.Text)
    tail_sequence: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    last_sequence: Mapped[int] = mapped_column(
        sa.BigInteger, server_default="0", default=0
    )
    producer_position: Mapped[int] = mapped_column(
        sa.BigInteger, server_default="0", default=0
    )
    producer_kind: Mapped[Optional[str]]
    producer_task_run_id: Mapped[Optional[uuid.UUID]]
    owner_id: Mapped[Optional[uuid.UUID]]
    attempt: Mapped[int] = mapped_column(server_default="0", default=0)
    owner_epoch: Mapped[int] = mapped_column(server_default="0", default=0)
    limits: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default=dict
    )
    record_count: Mapped[int] = mapped_column(
        sa.BigInteger, server_default="0", default=0
    )
    byte_count: Mapped[int] = mapped_column(
        sa.BigInteger, server_default="0", default=0
    )
    released: Mapped[bool] = mapped_column(server_default="0", default=False)

    __table_args__: Any = (
        sa.UniqueConstraint("id", "flow_run_id"),
        sa.CheckConstraint("protocol_version = 1", name="protocol_version_supported"),
        sa.CheckConstraint(
            "state IN ('OPEN', 'COMPLETED', 'FAILED')", name="state_valid"
        ),
        sa.CheckConstraint(
            "producer_kind IS NULL OR producer_kind IN ('SOURCE', 'CONSUMER')",
            name="producer_kind_valid",
        ),
        sa.CheckConstraint(
            "last_sequence >= 0 AND producer_position >= last_sequence",
            name="sequence_progress_valid",
        ),
        sa.CheckConstraint(
            "record_count >= 0 AND byte_count >= 0", name="usage_nonnegative"
        ),
        sa.CheckConstraint(
            "attempt >= 0 AND owner_epoch >= 0", name="authority_nonnegative"
        ),
        sa.CheckConstraint(
            "(owner_id IS NULL AND attempt = 0 AND owner_epoch = 0) OR "
            "(owner_id IS NOT NULL AND attempt >= 1 AND owner_epoch >= 1)",
            name="authority_shape_valid",
        ),
        sa.CheckConstraint(
            "(state = 'OPEN' AND tail_sequence IS NULL AND error IS NULL) OR "
            "(state = 'COMPLETED' AND tail_sequence IS NOT NULL "
            "AND tail_sequence = last_sequence "
            "AND error IS NULL) OR "
            "(state = 'FAILED' AND tail_sequence IS NOT NULL "
            "AND tail_sequence = last_sequence "
            "AND error IS NOT NULL)",
            name="terminal_shape_valid",
        ),
        sa.CheckConstraint(
            "released = false OR state IN ('COMPLETED', 'FAILED')",
            name="release_requires_terminal",
        ),
        sa.Index("ix_stream_partition__flow_run_id_state", "flow_run_id", "state"),
    )


class StreamRecord(Base):
    """An immutable record in a durable stream."""

    stream_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_partition.id", ondelete="CASCADE")
    )
    record_kind: Mapped[str] = mapped_column(
        default="PAYLOAD", server_default="PAYLOAD"
    )
    graph_revision_id: Mapped[Optional[uuid.UUID]]
    flow_run_id: Mapped[Optional[uuid.UUID]]
    payload_record_id: Mapped[Optional[uuid.UUID]]
    payload_record_kind: Mapped[Optional[str]]
    payload_value_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    route_receipt_id: Mapped[Optional[uuid.UUID]]
    edge_partition_id: Mapped[Optional[uuid.UUID]]
    sequence: Mapped[int] = mapped_column(sa.BigInteger)
    producer_position: Mapped[int] = mapped_column(sa.BigInteger)
    value: Mapped[Any] = mapped_column(stream_value_JSON)
    value_digest: Mapped[str] = mapped_column(sa.String(64))
    size_bytes: Mapped[int]

    __table_args__: Any = (
        sa.UniqueConstraint(
            "id",
            "graph_revision_id",
            "record_kind",
            "value_digest",
            name="uq_stream_record__id_graph_kind_value",
        ),
        sa.UniqueConstraint(
            "id",
            "graph_revision_id",
            "record_kind",
            "payload_record_id",
            name="uq_stream_record__id_graph_kind_payload",
        ),
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "flow_run_id"],
            ["stream_graph_revision.id", "stream_graph_revision.flow_run_id"],
            name="fk_stream_record__graph_flow",
        ),
        sa.ForeignKeyConstraint(
            ["stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
            name="fk_stream_record__partition_flow",
        ),
        sa.ForeignKeyConstraint(
            [
                "payload_record_id",
                "graph_revision_id",
                "payload_record_kind",
                "payload_value_digest",
            ],
            [
                "stream_record.id",
                "stream_record.graph_revision_id",
                "stream_record.record_kind",
                "stream_record.value_digest",
            ],
            ondelete="RESTRICT",
            name="fk_stream_record__direct_payload",
        ),
        sa.ForeignKeyConstraint(
            ["edge_partition_id", "stream_id", "graph_revision_id"],
            [
                "stream_graph_edge_partition.id",
                "stream_graph_edge_partition.stream_id",
                "stream_graph_edge_partition.graph_revision_id",
            ],
            ondelete="RESTRICT",
            name="fk_stream_record__edge_partition",
        ),
        sa.ForeignKeyConstraint(
            ["route_receipt_id", "graph_revision_id", "payload_record_id"],
            [
                "stream_route_receipt.id",
                "stream_route_receipt.graph_revision_id",
                "stream_route_receipt.payload_record_id",
            ],
            ondelete="RESTRICT",
            use_alter=True,
            name="fk_stream_record__route_receipt_payload",
        ),
        sa.CheckConstraint(
            "(graph_revision_id IS NULL AND flow_run_id IS NULL) OR (graph_revision_id IS NOT NULL AND flow_run_id IS NOT NULL)",
            name="graph_flow_shape_valid",
        ),
        sa.CheckConstraint(
            "(record_kind = 'PAYLOAD' AND payload_record_id IS NULL AND payload_record_kind IS NULL AND payload_value_digest IS NULL AND route_receipt_id IS NULL AND edge_partition_id IS NULL) OR (record_kind = 'REFERENCE' AND graph_revision_id IS NOT NULL AND payload_record_id IS NOT NULL AND payload_record_kind = 'PAYLOAD' AND payload_value_digest IS NOT NULL AND length(payload_value_digest) = 64 AND route_receipt_id IS NOT NULL AND edge_partition_id IS NOT NULL)",
            name="record_reference_shape_valid",
        ),
        sa.UniqueConstraint("stream_id", "sequence"),
        sa.UniqueConstraint("stream_id", "producer_position"),
        sa.CheckConstraint("sequence > 0", name="sequence_positive"),
        sa.CheckConstraint("producer_position > 0", name="producer_position_positive"),
        sa.CheckConstraint("length(value_digest) = 64", name="digest_length_valid"),
        sa.CheckConstraint("size_bytes > 0", name="size_positive"),
    )


class StreamConsumer(Base):
    """A logical TaskRun consumer and its durable cursor."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    consumer_kind: Mapped[str] = mapped_column(
        default="LEGACY", server_default="LEGACY"
    )
    graph_revision_id: Mapped[Optional[uuid.UUID]]
    edge_partition_id: Mapped[Optional[uuid.UUID]]
    edge_id: Mapped[Optional[uuid.UUID]]
    input_port_id: Mapped[Optional[uuid.UUID]]
    subtask_id: Mapped[Optional[uuid.UUID]]
    task_run_id: Mapped[Optional[uuid.UUID]]
    subscriber_uid: Mapped[Optional[str]] = mapped_column(sa.String(64))
    input_port_uid: Mapped[Optional[str]] = mapped_column(sa.String(64))
    input_stream_id: Mapped[uuid.UUID]
    output_stream_id: Mapped[uuid.UUID]
    owner_id: Mapped[uuid.UUID]
    attempt: Mapped[int]
    owner_epoch: Mapped[int]
    completed_through: Mapped[int] = mapped_column(
        sa.BigInteger, server_default="0", default=0
    )
    managed_state: Mapped[dict[str, Any]] = mapped_column(
        stream_value_JSON, server_default="{}", default=dict
    )
    inflight_sequence: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    delivery_id: Mapped[Optional[uuid.UUID]]

    __table_args__: Any = (
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "flow_run_id"],
            ["stream_graph_revision.id", "stream_graph_revision.flow_run_id"],
            name="fk_stream_consumer__graph_flow",
        ),
        sa.ForeignKeyConstraint(
            [
                "edge_partition_id",
                "graph_revision_id",
                "edge_id",
                "subtask_id",
                "input_stream_id",
                "id",
            ],
            [
                "stream_graph_edge_partition.id",
                "stream_graph_edge_partition.graph_revision_id",
                "stream_graph_edge_partition.edge_id",
                "stream_graph_edge_partition.target_subtask_id",
                "stream_graph_edge_partition.stream_id",
                "stream_graph_edge_partition.subscriber_id",
            ],
            name="fk_stream_consumer__stable_subscriber",
        ),
        sa.ForeignKeyConstraint(
            ["subtask_id", "graph_revision_id", "task_run_id", "output_stream_id"],
            [
                "stream_physical_subtask.id",
                "stream_physical_subtask.graph_revision_id",
                "stream_physical_subtask.task_run_id",
                "stream_physical_subtask.output_stream_id",
            ],
            name="fk_stream_consumer__current_dag_task",
        ),
        sa.ForeignKeyConstraint(
            ["edge_id", "graph_revision_id", "subscriber_uid", "input_port_id"],
            [
                "stream_graph_edge.id",
                "stream_graph_edge.graph_revision_id",
                "stream_graph_edge.subscriber_uid",
                "stream_graph_edge.consumer_port_id",
            ],
            name="fk_stream_consumer__edge_subscriber_port",
        ),
        sa.ForeignKeyConstraint(
            ["input_port_id", "graph_revision_id", "input_port_uid"],
            [
                "stream_graph_port.id",
                "stream_graph_port.graph_revision_id",
                "stream_graph_port.port_uid",
            ],
            name="fk_stream_consumer__input_port_uid",
        ),
        sa.UniqueConstraint("id", "consumer_kind"),
        sa.UniqueConstraint(
            "id",
            "graph_revision_id",
            "subtask_id",
            "task_run_id",
            "input_stream_id",
            "output_stream_id",
            name="uq_stream_consumer__dag_owner",
        ),
        sa.UniqueConstraint("edge_partition_id"),
        sa.CheckConstraint(
            "(consumer_kind = 'LEGACY' AND graph_revision_id IS NULL AND edge_partition_id IS NULL AND edge_id IS NULL AND input_port_id IS NULL AND subtask_id IS NULL AND task_run_id IS NULL AND subscriber_uid IS NULL AND input_port_uid IS NULL) OR (consumer_kind = 'DAG' AND graph_revision_id IS NOT NULL AND edge_partition_id IS NOT NULL AND edge_id IS NOT NULL AND input_port_id IS NOT NULL AND subtask_id IS NOT NULL AND task_run_id IS NOT NULL AND subscriber_uid IS NOT NULL AND length(subscriber_uid) BETWEEN 1 AND 64 AND input_port_uid IS NOT NULL AND length(input_port_uid) BETWEEN 1 AND 64)",
            name="dag_subscriber_shape_valid",
        ),
        sa.ForeignKeyConstraint(
            ["input_stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["output_stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("output_stream_id"),
        sa.UniqueConstraint("delivery_id"),
        sa.CheckConstraint(
            "input_stream_id != output_stream_id", name="distinct_streams"
        ),
        sa.CheckConstraint("attempt >= 1", name="attempt_positive"),
        sa.CheckConstraint("owner_epoch >= 1", name="owner_epoch_positive"),
        sa.CheckConstraint(
            "completed_through >= 0", name="completed_through_nonnegative"
        ),
        sa.CheckConstraint(
            "(inflight_sequence IS NULL AND delivery_id IS NULL) OR "
            "(inflight_sequence IS NOT NULL "
            "AND inflight_sequence = completed_through + 1 "
            "AND delivery_id IS NOT NULL)",
            name="delivery_shape_valid",
        ),
        sa.Index("ix_stream_consumer__input_stream_id", "input_stream_id"),
    )


class StreamCompletion(Base):
    """The immutable terminal witness for one consumed input record."""

    consumer_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_consumer.id", ondelete="CASCADE")
    )
    consumer_kind: Mapped[str] = mapped_column(
        default="LEGACY", server_default="LEGACY"
    )
    route_receipt_id: Mapped[Optional[uuid.UUID]] = mapped_column(unique=True)
    input_sequence: Mapped[int] = mapped_column(sa.BigInteger)
    delivery_id: Mapped[uuid.UUID]
    owner_epoch: Mapped[int]
    kind: Mapped[str]
    intent_digest: Mapped[str] = mapped_column(sa.String(64))
    output_sequence: Mapped[Optional[int]] = mapped_column(sa.BigInteger)

    __table_args__: Any = (
        sa.ForeignKeyConstraint(
            ["consumer_id", "consumer_kind"],
            ["stream_consumer.id", "stream_consumer.consumer_kind"],
            name="fk_stream_completion__consumer_kind",
        ),
        sa.ForeignKeyConstraint(
            [
                "route_receipt_id",
                "consumer_id",
                "input_sequence",
                "kind",
                "intent_digest",
            ],
            [
                "stream_route_receipt.id",
                "stream_route_receipt.consumer_id",
                "stream_route_receipt.input_sequence",
                "stream_route_receipt.kind",
                "stream_route_receipt.semantic_digest",
            ],
            ondelete="RESTRICT",
            name="fk_stream_completion__route_receipt",
        ),
        sa.CheckConstraint(
            "(consumer_kind = 'LEGACY' AND route_receipt_id IS NULL) OR (consumer_kind = 'DAG' AND route_receipt_id IS NOT NULL)",
            name="route_receipt_shape_valid",
        ),
        sa.UniqueConstraint("consumer_id", "input_sequence"),
        sa.UniqueConstraint("consumer_id", "delivery_id"),
        sa.UniqueConstraint("consumer_id", "output_sequence"),
        sa.CheckConstraint("input_sequence > 0", name="input_sequence_positive"),
        sa.CheckConstraint("owner_epoch >= 1", name="owner_epoch_positive"),
        sa.CheckConstraint("kind IN ('YIELD', 'RETURN')", name="kind_valid"),
        sa.CheckConstraint(
            "length(intent_digest) = 64", name="intent_digest_length_valid"
        ),
        sa.CheckConstraint(
            "(kind = 'YIELD' AND output_sequence IS NOT NULL "
            "AND output_sequence > 0) OR "
            "(kind = 'RETURN' AND output_sequence IS NULL)",
            name="output_shape_valid",
        ),
    )


class StreamRouteReceipt(Base):
    """One immutable DAG terminal outcome; no raw business key is persisted."""

    graph_revision_id: Mapped[uuid.UUID]
    flow_run_id: Mapped[uuid.UUID]
    producer_subtask_id: Mapped[uuid.UUID]
    producer_task_run_id: Mapped[uuid.UUID]
    producer_stream_id: Mapped[uuid.UUID]
    producer_position: Mapped[int] = mapped_column(sa.BigInteger)
    producer_sequence: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    producer_role: Mapped[str]
    kind: Mapped[str]
    routing_digest: Mapped[str] = mapped_column(sa.String(64))
    semantic_digest: Mapped[str] = mapped_column(sa.String(64))
    outcome_digest: Mapped[str] = mapped_column(sa.String(64))
    value_digest: Mapped[str] = mapped_column(sa.String(64))
    managed_state_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    key_present: Mapped[bool] = mapped_column(default=False, server_default=sa.false())
    output_key_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    output_ordinal: Mapped[Optional[int]]
    payload_record_id: Mapped[Optional[uuid.UUID]]
    payload_record_kind: Mapped[Optional[str]]
    input_stream_id: Mapped[Optional[uuid.UUID]]
    input_reference_record_id: Mapped[Optional[uuid.UUID]]
    input_reference_record_kind: Mapped[Optional[str]]
    input_payload_record_id: Mapped[Optional[uuid.UUID]]
    input_payload_record_kind: Mapped[Optional[str]]
    input_payload_value_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    input_sequence: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    consumer_id: Mapped[Optional[uuid.UUID]]
    outcome: Mapped[dict[str, Any]] = mapped_column(JSON)
    outcome_size: Mapped[int]

    __table_args__: Any = (
        sa.UniqueConstraint("producer_stream_id", "producer_position"),
        sa.UniqueConstraint("payload_record_id"),
        sa.UniqueConstraint(
            "id",
            "consumer_id",
            "input_sequence",
            "kind",
            "semantic_digest",
            name="uq_stream_route_receipt__completion_witness",
        ),
        sa.ForeignKeyConstraint(
            [
                "consumer_id",
                "graph_revision_id",
                "producer_subtask_id",
                "producer_task_run_id",
                "input_stream_id",
                "producer_stream_id",
            ],
            [
                "stream_consumer.id",
                "stream_consumer.graph_revision_id",
                "stream_consumer.subtask_id",
                "stream_consumer.task_run_id",
                "stream_consumer.input_stream_id",
                "stream_consumer.output_stream_id",
            ],
            name="fk_stream_route_receipt__stable_consumer",
        ),
        sa.UniqueConstraint(
            "id",
            "graph_revision_id",
            "payload_record_id",
            name="uq_stream_route_receipt__reference_payload",
        ),
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "flow_run_id"],
            ["stream_graph_revision.id", "stream_graph_revision.flow_run_id"],
            name="fk_stream_route_receipt__graph_flow",
        ),
        sa.ForeignKeyConstraint(
            [
                "producer_subtask_id",
                "graph_revision_id",
                "producer_task_run_id",
                "producer_stream_id",
            ],
            [
                "stream_physical_subtask.id",
                "stream_physical_subtask.graph_revision_id",
                "stream_physical_subtask.task_run_id",
                "stream_physical_subtask.output_stream_id",
            ],
            name="fk_stream_route_receipt__producer",
        ),
        sa.ForeignKeyConstraint(
            ["producer_stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
            name="fk_stream_route_receipt__producer_flow",
        ),
        sa.ForeignKeyConstraint(
            ["input_stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
            name="fk_stream_route_receipt__input_flow",
        ),
        sa.ForeignKeyConstraint(
            [
                "payload_record_id",
                "graph_revision_id",
                "payload_record_kind",
                "value_digest",
            ],
            [
                "stream_record.id",
                "stream_record.graph_revision_id",
                "stream_record.record_kind",
                "stream_record.value_digest",
            ],
            ondelete="RESTRICT",
            name="fk_stream_route_receipt__output_payload",
        ),
        # Only this edge is deferred: whole-Flow locked cleanup deletes all
        # REFERENCEs, completions, receipts, consumers, then PAYLOADs in one txn.
        # Committed dangling input references remain forbidden in both dialects.
        sa.ForeignKeyConstraint(
            [
                "input_reference_record_id",
                "graph_revision_id",
                "input_reference_record_kind",
                "input_payload_record_id",
            ],
            [
                "stream_record.id",
                "stream_record.graph_revision_id",
                "stream_record.record_kind",
                "stream_record.payload_record_id",
            ],
            deferrable=True,
            initially="DEFERRED",
            name="fk_stream_route_receipt__input_reference",
        ),
        sa.ForeignKeyConstraint(
            [
                "input_payload_record_id",
                "graph_revision_id",
                "input_payload_record_kind",
                "input_payload_value_digest",
            ],
            [
                "stream_record.id",
                "stream_record.graph_revision_id",
                "stream_record.record_kind",
                "stream_record.value_digest",
            ],
            ondelete="RESTRICT",
            name="fk_stream_route_receipt__input_payload",
        ),
        sa.CheckConstraint(
            "producer_position > 0 AND (producer_sequence IS NULL OR producer_sequence > 0)",
            name="position_valid",
        ),
        sa.CheckConstraint(
            "length(routing_digest) = 64 AND length(semantic_digest) = 64 AND length(outcome_digest) = 64 AND length(value_digest) = 64 AND (managed_state_digest IS NULL OR length(managed_state_digest) = 64)",
            name="digests_valid",
        ),
        sa.CheckConstraint(
            "(key_present = false AND output_key_digest IS NULL) OR (key_present = true AND output_key_digest IS NOT NULL AND length(output_key_digest) = 64)",
            name="key_shape_valid",
        ),
        sa.CheckConstraint(
            "(kind = 'YIELD' AND producer_sequence IS NOT NULL AND output_ordinal = 0 AND payload_record_id IS NOT NULL AND payload_record_kind = 'PAYLOAD') OR (kind = 'RETURN' AND producer_sequence IS NULL AND output_ordinal IS NULL AND payload_record_id IS NULL AND payload_record_kind IS NULL AND key_present = false)",
            name="terminal_shape_valid",
        ),
        sa.CheckConstraint(
            "(producer_role = 'SOURCE' AND kind = 'YIELD' AND input_stream_id IS NULL AND input_reference_record_id IS NULL AND input_reference_record_kind IS NULL AND input_payload_record_id IS NULL AND input_payload_record_kind IS NULL AND input_payload_value_digest IS NULL AND input_sequence IS NULL AND consumer_id IS NULL AND managed_state_digest IS NULL) OR (producer_role = 'OPERATOR' AND input_stream_id IS NOT NULL AND input_reference_record_id IS NOT NULL AND input_reference_record_kind = 'REFERENCE' AND input_payload_record_id IS NOT NULL AND input_payload_record_kind = 'PAYLOAD' AND input_payload_value_digest IS NOT NULL AND length(input_payload_value_digest) = 64 AND input_sequence = producer_position AND consumer_id IS NOT NULL AND managed_state_digest IS NOT NULL)",
            name="input_shape_valid",
        ),
        sa.CheckConstraint(
            "outcome_size BETWEEN 1 AND 65536", name="outcome_size_valid"
        ),
    )


class StreamSubmission(Base):
    """An immutable semantic submission slot within a FlowRun."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    graph_revision_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("stream_graph_revision.id")
    )
    submission_key: Mapped[str] = mapped_column(sa.String(128))
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    manifest_digest: Mapped[str] = mapped_column(sa.String(64))
    canonical_size: Mapped[int]
    parameters: Mapped[dict[str, Any]] = mapped_column(stream_value_JSON)
    parameters_digest: Mapped[str] = mapped_column(sa.String(64))
    role: Mapped[str]
    protocol: Mapped[str]
    input_key: Mapped[Optional[str]] = mapped_column(sa.String(255))
    upstream_stream_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("stream_partition.id")
    )
    task_run_id: Mapped[uuid.UUID] = mapped_column(sa.ForeignKey("task_run.id"))
    stream_id: Mapped[uuid.UUID] = mapped_column(sa.ForeignKey("stream_partition.id"))
    task_name: Mapped[str]
    task_key: Mapped[str]
    task_version: Mapped[str]
    flow_version: Mapped[str]
    deployment_id: Mapped[Optional[uuid.UUID]]
    deployment_version: Mapped[Optional[str]]
    artifact_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    dynamic_key: Mapped[str]
    limits: Mapped[dict[str, Any]] = mapped_column(JSON)
    task_policy: Mapped[dict[str, Any]] = mapped_column(JSON)
    tags: Mapped[list[str]] = mapped_column(JSON)
    labels: Mapped[dict[str, Any]] = mapped_column(JSON)
    state: Mapped[str] = mapped_column(server_default="ACTIVE", default="ACTIVE")
    released_at: Mapped[Optional[DateTime]]

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id", "submission_key"),
        sa.UniqueConstraint("task_run_id"),
        sa.UniqueConstraint("stream_id"),
        # The first recovery profile is a linear graph without fan-out.
        sa.UniqueConstraint("upstream_stream_id"),
        sa.CheckConstraint(
            "length(submission_key) BETWEEN 1 AND 128",
            name="submission_key_length_valid",
        ),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND "
            "length(manifest_digest) = 64 AND "
            "length(parameters_digest) = 64",
            name="digest_lengths_valid",
        ),
        sa.CheckConstraint("canonical_size > 0", name="canonical_size_positive"),
        sa.CheckConstraint("role IN ('SOURCE', 'OPERATOR')", name="role_valid"),
        sa.CheckConstraint(
            "(role = 'SOURCE' AND protocol = 'source-v1' "
            "AND input_key IS NULL AND upstream_stream_id IS NULL) OR "
            "(role = 'OPERATOR' AND protocol = 'yield-v2' "
            "AND input_key IS NOT NULL AND upstream_stream_id IS NOT NULL)",
            name="topology_shape_valid",
        ),
        sa.CheckConstraint("state IN ('ACTIVE', 'RELEASED')", name="state_valid"),
        sa.CheckConstraint(
            "(state = 'ACTIVE' AND released_at IS NULL) OR "
            "(state = 'RELEASED' AND released_at IS NOT NULL)",
            name="release_shape_valid",
        ),
        sa.Index(
            "uq_stream_submission__one_source_per_flow",
            "flow_run_id",
            unique=True,
            postgresql_where=sa.text("role = 'SOURCE' AND graph_revision_id IS NULL"),
            sqlite_where=sa.text("role = 'SOURCE' AND graph_revision_id IS NULL"),
        ),
        sa.Index("ix_stream_submission__flow_run_id_state", "flow_run_id", "state"),
    )


class StreamGraphRevision(Base):
    """Immutable fixed-P2 topology; records remain in StreamPartition."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), unique=True
    )
    operation_id: Mapped[uuid.UUID]
    profile: Mapped[str]
    assignment_epoch: Mapped[int]
    graph_digest: Mapped[str] = mapped_column(sa.String(64))
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    declaration: Mapped[dict[str, Any]] = mapped_column(JSON)
    state: Mapped[str]
    admission_operation_id: Mapped[Optional[uuid.UUID]]
    admission_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    startup_completed_at: Mapped[Optional[DateTime]]
    activation_operation_id: Mapped[Optional[uuid.UUID]]
    activation_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    abort_operation_id: Mapped[Optional[uuid.UUID]]
    abort_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    abort_error: Mapped[Optional[str]] = mapped_column(sa.Text)
    placement_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))

    __table_args__: Any = (
        sa.UniqueConstraint("id", "profile"),
        sa.UniqueConstraint("id", "flow_run_id"),
        sa.UniqueConstraint("id", "admission_operation_id", "profile"),
        sa.CheckConstraint(
            "profile IN ('partitioned-fixed-v1', 'partitioned-dag-v1') AND assignment_epoch = 1",
            name="profile_valid",
        ),
        sa.CheckConstraint(
            "length(graph_digest) = 64 AND length(request_digest) = 64",
            name="digests_valid",
        ),
        sa.CheckConstraint(
            "state IN ('SEALED', 'ADMITTED', 'ABORTING', 'RELEASED')",
            name="state_valid",
        ),
        sa.CheckConstraint(
            "(admission_operation_id IS NULL AND admission_request_digest IS NULL) OR (admission_operation_id IS NOT NULL AND admission_request_digest IS NOT NULL AND length(admission_request_digest) = 64)",
            name="admission_shape_valid",
        ),
        sa.CheckConstraint(
            "(startup_completed_at IS NULL AND activation_operation_id IS NULL AND activation_request_digest IS NULL) OR (startup_completed_at IS NOT NULL AND activation_operation_id IS NOT NULL AND activation_request_digest IS NOT NULL AND length(activation_request_digest) = 64)",
            name="activation_shape_valid",
        ),
        sa.CheckConstraint(
            "(abort_operation_id IS NULL AND abort_request_digest IS NULL AND abort_error IS NULL) OR (abort_operation_id IS NOT NULL AND abort_request_digest IS NOT NULL AND length(abort_request_digest) = 64 AND abort_error IS NOT NULL AND length(abort_error) BETWEEN 1 AND 4096 AND startup_completed_at IS NULL AND state IN ('ABORTING', 'RELEASED'))",
            name="abort_shape_valid",
        ),
        sa.CheckConstraint(
            "(state = 'SEALED' AND admission_operation_id IS NULL AND startup_completed_at IS NULL AND abort_operation_id IS NULL) OR (state = 'ADMITTED' AND admission_operation_id IS NOT NULL AND abort_operation_id IS NULL) OR (state IN ('ABORTING', 'RELEASED') AND abort_operation_id IS NOT NULL AND startup_completed_at IS NULL) OR (state = 'RELEASED' AND abort_operation_id IS NULL AND startup_completed_at IS NOT NULL AND admission_operation_id IS NOT NULL)",
            name="lifecycle_shape_valid",
        ),
        sa.CheckConstraint(
            "placement_digest IS NULL OR length(placement_digest) = 64",
            name="placement_digest_valid",
        ),
    )


class StreamOperatorRun(Base):
    """Logical operator grouping, independent from physical TaskRun invocations."""

    graph_revision_id: Mapped[uuid.UUID]
    profile: Mapped[str] = mapped_column(
        default="partitioned-fixed-v1", server_default="partitioned-fixed-v1"
    )
    operator_uid: Mapped[str] = mapped_column(sa.String(64))
    role: Mapped[str]
    parallelism: Mapped[int]
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)

    __table_args__: Any = (
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "profile"],
            ["stream_graph_revision.id", "stream_graph_revision.profile"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "graph_revision_id", "profile"),
        sa.UniqueConstraint("graph_revision_id", "operator_uid"),
        sa.CheckConstraint(
            "role IN ('SOURCE', 'OPERATOR') AND ((profile = 'partitioned-fixed-v1' AND parallelism = 2) OR (profile = 'partitioned-dag-v1' AND parallelism BETWEEN 1 AND 16))",
            name="fixed_shape_valid",
        ),
    )


class StreamPhysicalSubtask(Base):
    """Stable physical invocation; replacement never creates another TaskRun."""

    graph_revision_id: Mapped[uuid.UUID] = mapped_column(index=True)
    profile: Mapped[str] = mapped_column(
        default="partitioned-fixed-v1", server_default="partitioned-fixed-v1"
    )
    operator_run_id: Mapped[uuid.UUID]
    subtask_index: Mapped[int]
    assignment_epoch: Mapped[int]
    task_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("task_run.id"), unique=True
    )
    submission_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_submission.id"), unique=True
    )
    input_stream_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("stream_partition.id")
    )
    output_stream_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_partition.id"), unique=True
    )
    current_grant_id: Mapped[Optional[uuid.UUID]]
    physical_epoch: Mapped[int] = mapped_column(default=0, server_default="0")
    assigned_worker_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("worker.id"), index=True
    )
    physical_slot_released: Mapped[bool] = mapped_column(
        default=False, server_default=sa.false()
    )
    startup_stop_operation_id: Mapped[Optional[uuid.UUID]]
    startup_stop_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    startup_stop_witness: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    startup_stop_unstarted: Mapped[bool] = mapped_column(
        default=False, server_default=sa.false()
    )

    __table_args__: Any = (
        sa.UniqueConstraint(
            "id",
            "graph_revision_id",
            "task_run_id",
            "output_stream_id",
            name="uq_stream_physical_subtask__route_producer",
        ),
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "profile"],
            ["stream_graph_revision.id", "stream_graph_revision.profile"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["operator_run_id", "graph_revision_id", "profile"],
            [
                "stream_operator_run.id",
                "stream_operator_run.graph_revision_id",
                "stream_operator_run.profile",
            ],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "graph_revision_id", "profile", "subtask_index"),
        sa.UniqueConstraint("id", "graph_revision_id", "assigned_worker_id"),
        sa.UniqueConstraint("operator_run_id", "subtask_index"),
        sa.CheckConstraint(
            "assignment_epoch = 1 AND ((profile = 'partitioned-fixed-v1' AND subtask_index BETWEEN 0 AND 1) OR (profile = 'partitioned-dag-v1' AND subtask_index BETWEEN 0 AND 15))",
            name="assignment_valid",
        ),
        sa.CheckConstraint(
            "(physical_epoch = 0 AND current_grant_id IS NULL) OR (physical_epoch BETWEEN 1 AND 64 AND current_grant_id IS NOT NULL)",
            name="grant_shape_valid",
        ),
        sa.CheckConstraint(
            "(startup_stop_operation_id IS NULL AND startup_stop_request_digest IS NULL AND startup_stop_witness IS NULL AND startup_stop_unstarted = false) OR (startup_stop_operation_id IS NOT NULL AND startup_stop_request_digest IS NOT NULL AND length(startup_stop_request_digest) = 64 AND ((startup_stop_witness IS NOT NULL AND startup_stop_unstarted = false) OR (startup_stop_witness IS NULL AND startup_stop_unstarted = true)))",
            name="startup_stop_shape_valid",
        ),
        sa.CheckConstraint(
            "assigned_worker_id IS NOT NULL OR physical_slot_released = false",
            name="slot_assignment_valid",
        ),
    )


class StreamSubtaskGrant(Base):
    """One physical process authority; only digests of capabilities are stored."""

    subtask_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_physical_subtask.id", ondelete="CASCADE")
    )
    operation_id: Mapped[uuid.UUID]
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    physical_epoch: Mapped[int]
    holder_id: Mapped[uuid.UUID]
    capability_digest: Mapped[str] = mapped_column(sa.String(64))
    event_public_key: Mapped[str] = mapped_column(sa.String(64))
    event_key_id: Mapped[str] = mapped_column(sa.String(64))
    scope_digest: Mapped[str] = mapped_column(sa.String(64))
    launcher_binding_kind: Mapped[str]
    flow_generation: Mapped[int]
    flow_holder_id: Mapped[uuid.UUID]
    worker_id: Mapped[Optional[uuid.UUID]]
    work_pool_id: Mapped[Optional[uuid.UUID]]
    host_id: Mapped[uuid.UUID]
    boot_id: Mapped[uuid.UUID]
    launcher_process_id: Mapped[int]
    launcher_start_ticks: Mapped[int] = mapped_column(sa.BigInteger)
    child_process_id: Mapped[Optional[int]]
    process_start_ticks: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    registration_operation_id: Mapped[Optional[uuid.UUID]]
    state: Mapped[str]
    last_renewed_at: Mapped[DateTime]

    __table_args__: Any = (
        sa.UniqueConstraint("subtask_id", "physical_epoch"),
        sa.UniqueConstraint("subtask_id", "operation_id"),
        sa.CheckConstraint("physical_epoch BETWEEN 1 AND 64", name="epoch_bounded"),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(capability_digest) = 64 AND length(event_public_key) = 64 AND length(event_key_id) = 64 AND length(scope_digest) = 64",
            name="digests_valid",
        ),
        sa.CheckConstraint(
            "state IN ('CLAIMED', 'REGISTERED', 'RETIRED', 'RELEASED')",
            name="state_valid",
        ),
        sa.CheckConstraint(
            "(child_process_id IS NULL AND process_start_ticks IS NULL AND registration_operation_id IS NULL) OR (child_process_id IS NOT NULL AND process_start_ticks IS NOT NULL AND child_process_id > 0 AND process_start_ticks > 0 AND registration_operation_id IS NOT NULL)",
            name="process_shape_valid",
        ),
        sa.CheckConstraint(
            "state != 'REGISTERED' OR child_process_id IS NOT NULL",
            name="registration_required",
        ),
        sa.CheckConstraint(
            "state != 'CLAIMED' OR child_process_id IS NULL", name="unregistered_claim"
        ),
        sa.CheckConstraint(
            "launcher_process_id > 0 AND launcher_start_ticks > 0 AND flow_generation BETWEEN 1 AND 64",
            name="launcher_identity_valid",
        ),
        sa.CheckConstraint(
            "(launcher_binding_kind IN ('DEPLOYMENT', 'ASSIGNED_WORKER') AND worker_id IS NOT NULL AND work_pool_id IS NOT NULL) OR (launcher_binding_kind = 'FLOW_AUTHORITY_LOCAL' AND worker_id IS NULL AND work_pool_id IS NULL)",
            name="launcher_binding_valid",
        ),
    )


class StreamSubtaskReplacement(Base):
    """Bounded exact-replay local death proof and compare-and-swap journal."""

    subtask_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_physical_subtask.id", ondelete="CASCADE")
    )
    operation_id: Mapped[uuid.UUID]
    expected_grant_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_subtask_grant.id")
    )
    expected_physical_epoch: Mapped[int]
    challenge_digest: Mapped[str] = mapped_column(sa.String(64))
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    verification_not_before: Mapped[DateTime]
    state: Mapped[str]
    confirm_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    new_grant_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("stream_subtask_grant.id")
    )
    death_witness: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    retirement_outcome: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)

    __table_args__: Any = (
        sa.UniqueConstraint("subtask_id", "operation_id"),
        sa.UniqueConstraint("expected_grant_id"),
        sa.CheckConstraint(
            "expected_physical_epoch BETWEEN 1 AND 64", name="epoch_bounded"
        ),
        sa.CheckConstraint(
            "length(challenge_digest) = 64 AND length(request_digest) = 64",
            name="digests_valid",
        ),
        sa.CheckConstraint(
            "(state = 'AUTHORIZED' AND confirm_digest IS NULL AND new_grant_id IS NULL AND death_witness IS NULL AND retirement_outcome IS NULL) OR (state = 'CONFIRMED' AND confirm_digest IS NOT NULL AND length(confirm_digest) = 64 AND new_grant_id IS NOT NULL AND death_witness IS NOT NULL AND retirement_outcome IS NULL) OR (state = 'RETIRED' AND confirm_digest IS NOT NULL AND length(confirm_digest) = 64 AND new_grant_id IS NULL AND death_witness IS NOT NULL AND retirement_outcome IS NOT NULL)",
            name="confirmation_shape_valid",
        ),
    )


class StreamWorkerCapability(Base):
    """Bounded live protocol advertisement, never a process-death witness."""

    worker_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("worker.id", ondelete="CASCADE")
    )
    work_pool_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("work_pool.id", ondelete="CASCADE")
    )
    protocol: Mapped[str]
    protocol_version: Mapped[int]
    host_id: Mapped[uuid.UUID]
    boot_id: Mapped[uuid.UUID]
    launcher_process_id: Mapped[int] = mapped_column(sa.BigInteger)
    launcher_start_ticks: Mapped[int] = mapped_column(sa.BigInteger)
    last_renewed_at: Mapped[DateTime]

    __table_args__: Any = (
        sa.UniqueConstraint("worker_id", "protocol", "protocol_version"),
        sa.CheckConstraint(
            "protocol IN ('partitioned-fixed-v1', 'partitioned-dag-v1') AND protocol_version = 1",
            name="protocol_valid",
        ),
        sa.CheckConstraint(
            "launcher_process_id > 0 AND launcher_start_ticks > 0",
            name="process_identity_valid",
        ),
    )


class StreamSubtaskAllocation(Base):
    """DB-authoritative reservation against ordinary V2 concurrency counters."""

    subtask_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_physical_subtask.id", ondelete="CASCADE")
    )
    concurrency_limit_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("concurrency_limit_v2.id", ondelete="CASCADE")
    )
    released: Mapped[bool] = mapped_column(default=False, server_default=sa.false())

    __table_args__: Any = (sa.UniqueConstraint("subtask_id", "concurrency_limit_id"),)


class StreamGraphPort(Base):
    """Immutable qualified operator port; no records or execution authority."""

    graph_revision_id: Mapped[uuid.UUID]
    operator_run_id: Mapped[uuid.UUID]
    profile: Mapped[str] = mapped_column(
        default="partitioned-dag-v1", server_default="partitioned-dag-v1"
    )
    port_uid: Mapped[str] = mapped_column(sa.String(64))
    direction: Mapped[str]
    schema_id: Mapped[str]
    schema_version: Mapped[int]
    schema_digest: Mapped[str] = mapped_column(sa.String(64))

    __table_args__: Any = (
        sa.UniqueConstraint("id", "graph_revision_id", "port_uid"),
        sa.ForeignKeyConstraint(
            ["operator_run_id", "graph_revision_id", "profile"],
            [
                "stream_operator_run.id",
                "stream_operator_run.graph_revision_id",
                "stream_operator_run.profile",
            ],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("operator_run_id", "direction"),
        sa.UniqueConstraint("operator_run_id", "port_uid"),
        sa.UniqueConstraint("id", "graph_revision_id", "direction"),
        sa.CheckConstraint(
            "profile = 'partitioned-dag-v1' AND direction IN ('INPUT', 'OUTPUT') AND length(port_uid) BETWEEN 1 AND 64",
            name="port_shape_valid",
        ),
        sa.CheckConstraint(
            "schema_id = 'json-value-v1' AND schema_version = 1 AND schema_digest = 'd93f54187397ae62d4e1b98c56edab2a67a9d10422b028dfdea87ba6616a849c'",
            name="schema_identity_valid",
        ),
    )


class StreamGraphEdge(Base):
    """Immutable logical subscriber/exchange metadata, never a second log."""

    graph_revision_id: Mapped[uuid.UUID]
    profile: Mapped[str] = mapped_column(
        default="partitioned-dag-v1", server_default="partitioned-dag-v1"
    )
    edge_uid: Mapped[str] = mapped_column(sa.String(64))
    subscriber_uid: Mapped[str] = mapped_column(sa.String(64))
    producer_port_id: Mapped[uuid.UUID]
    producer_direction: Mapped[str] = mapped_column(
        default="OUTPUT", server_default="OUTPUT"
    )
    consumer_port_id: Mapped[uuid.UUID]
    consumer_direction: Mapped[str] = mapped_column(
        default="INPUT", server_default="INPUT"
    )
    producer_parallelism: Mapped[int]
    target_parallelism: Mapped[int]
    exchange: Mapped[str]
    routing_version: Mapped[int]
    key_codec: Mapped[Optional[str]]
    hash_algorithm: Mapped[Optional[str]]
    key_group_count: Mapped[Optional[int]]
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)

    __table_args__: Any = (
        sa.UniqueConstraint(
            "id",
            "graph_revision_id",
            "subscriber_uid",
            "consumer_port_id",
            name="uq_stream_graph_edge__subscriber_port",
        ),
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "profile"],
            ["stream_graph_revision.id", "stream_graph_revision.profile"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["producer_port_id", "graph_revision_id", "producer_direction"],
            [
                "stream_graph_port.id",
                "stream_graph_port.graph_revision_id",
                "stream_graph_port.direction",
            ],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["consumer_port_id", "graph_revision_id", "consumer_direction"],
            [
                "stream_graph_port.id",
                "stream_graph_port.graph_revision_id",
                "stream_graph_port.direction",
            ],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("graph_revision_id", "edge_uid"),
        sa.UniqueConstraint("graph_revision_id", "subscriber_uid"),
        sa.UniqueConstraint("consumer_port_id"),
        sa.UniqueConstraint("id", "graph_revision_id"),
        sa.CheckConstraint(
            "profile = 'partitioned-dag-v1' AND producer_direction = 'OUTPUT' AND consumer_direction = 'INPUT' AND producer_port_id != consumer_port_id",
            name="edge_shape_valid",
        ),
        sa.CheckConstraint(
            "length(edge_uid) BETWEEN 1 AND 64 AND length(subscriber_uid) BETWEEN 1 AND 64",
            name="identifiers_valid",
        ),
        sa.CheckConstraint(
            "producer_parallelism BETWEEN 1 AND 16 AND target_parallelism BETWEEN 1 AND 16 AND exchange IN ('forward', 'rebalance', 'broadcast', 'keyed') AND routing_version = 1",
            name="exchange_valid",
        ),
        sa.CheckConstraint(
            "exchange != 'forward' OR producer_parallelism = target_parallelism",
            name="forward_shape_valid",
        ),
        sa.CheckConstraint(
            "(exchange = 'keyed' AND key_codec = 'tagged-key-v1' AND hash_algorithm = 'sha256' AND key_group_count IS NOT NULL AND key_group_count BETWEEN target_parallelism AND 2147483647) OR (exchange != 'keyed' AND key_codec IS NULL AND hash_algorithm IS NULL AND key_group_count IS NULL)",
            name="key_shape_valid",
        ),
    )


class StreamGraphEdgePartition(Base):
    """Stable subscriber partition binding to the existing stream data plane."""

    graph_revision_id: Mapped[uuid.UUID]
    flow_run_id: Mapped[uuid.UUID]
    profile: Mapped[str] = mapped_column(
        default="partitioned-dag-v1", server_default="partitioned-dag-v1"
    )
    edge_id: Mapped[uuid.UUID]
    partition_index: Mapped[int]
    stream_id: Mapped[uuid.UUID]
    subscriber_id: Mapped[uuid.UUID]
    target_subtask_id: Mapped[uuid.UUID]

    __table_args__: Any = (
        sa.UniqueConstraint(
            "id",
            "stream_id",
            "graph_revision_id",
            name="uq_stream_graph_edge_partition__route_stream",
        ),
        sa.UniqueConstraint(
            "id",
            "graph_revision_id",
            "edge_id",
            "target_subtask_id",
            "stream_id",
            "subscriber_id",
            name="uq_stream_graph_edge_partition__subscriber_target",
        ),
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "flow_run_id"],
            ["stream_graph_revision.id", "stream_graph_revision.flow_run_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["edge_id", "graph_revision_id"],
            ["stream_graph_edge.id", "stream_graph_edge.graph_revision_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
        ),
        sa.ForeignKeyConstraint(
            ["target_subtask_id", "graph_revision_id", "profile", "partition_index"],
            [
                "stream_physical_subtask.id",
                "stream_physical_subtask.graph_revision_id",
                "stream_physical_subtask.profile",
                "stream_physical_subtask.subtask_index",
            ],
        ),
        sa.UniqueConstraint("edge_id", "partition_index"),
        sa.UniqueConstraint("stream_id"),
        sa.UniqueConstraint("subscriber_id"),
        sa.UniqueConstraint("target_subtask_id"),
        sa.CheckConstraint(
            "profile = 'partitioned-dag-v1' AND partition_index BETWEEN 0 AND 15",
            name="partition_shape_valid",
        ),
    )


class StreamPhysicalSlotAllocation(Base):
    """Durable physical reservation; tag allocations may legitimately be empty."""

    graph_revision_id: Mapped[uuid.UUID]
    subtask_id: Mapped[uuid.UUID]
    worker_id: Mapped[uuid.UUID] = mapped_column(sa.ForeignKey("worker.id"), index=True)
    profile: Mapped[str] = mapped_column(
        default="partitioned-dag-v1", server_default="partitioned-dag-v1"
    )
    admission_operation_id: Mapped[uuid.UUID]
    released: Mapped[bool] = mapped_column(default=False, server_default=sa.false())
    release_operation_id: Mapped[Optional[uuid.UUID]]
    released_at: Mapped[Optional[DateTime]]

    __table_args__: Any = (
        sa.ForeignKeyConstraint(
            ["graph_revision_id", "admission_operation_id", "profile"],
            [
                "stream_graph_revision.id",
                "stream_graph_revision.admission_operation_id",
                "stream_graph_revision.profile",
            ],
        ),
        sa.ForeignKeyConstraint(
            ["subtask_id", "graph_revision_id", "worker_id"],
            [
                "stream_physical_subtask.id",
                "stream_physical_subtask.graph_revision_id",
                "stream_physical_subtask.assigned_worker_id",
            ],
        ),
        sa.UniqueConstraint("subtask_id"),
        sa.CheckConstraint("profile = 'partitioned-dag-v1'", name="profile_valid"),
        sa.CheckConstraint(
            "(released = false AND release_operation_id IS NULL AND released_at IS NULL) OR (released = true AND release_operation_id IS NOT NULL AND released_at IS NOT NULL)",
            name="release_shape_valid",
        ),
    )


class StreamFiniteSubmission(Base):
    """One immutable finite Task slot in a mixed native execution."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    phase: Mapped[str]
    submission_key: Mapped[str] = mapped_column(sa.String(128))
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    manifest_digest: Mapped[str] = mapped_column(sa.String(64))
    canonical_size: Mapped[int]
    parameters: Mapped[dict[str, Any]] = mapped_column(stream_value_JSON)
    parameters_digest: Mapped[str] = mapped_column(sa.String(64))
    task_run_id: Mapped[uuid.UUID] = mapped_column(sa.ForeignKey("task_run.id"))
    task_name: Mapped[str]
    task_key: Mapped[str]
    task_version: Mapped[str]
    flow_version: Mapped[str]
    deployment_id: Mapped[Optional[uuid.UUID]]
    deployment_version: Mapped[Optional[str]]
    artifact_digest: Mapped[str] = mapped_column(sa.String(64))
    dynamic_key: Mapped[str]
    task_policy: Mapped[dict[str, Any]] = mapped_column(JSON)
    tags: Mapped[list[str]] = mapped_column(JSON)
    labels: Mapped[dict[str, Any]] = mapped_column(JSON)
    state: Mapped[str] = mapped_column(server_default="ACTIVE", default="ACTIVE")
    released_at: Mapped[Optional[DateTime]]

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id", "phase"),
        sa.UniqueConstraint("flow_run_id", "submission_key"),
        sa.UniqueConstraint("task_run_id"),
        sa.CheckConstraint("phase IN ('PREFIX', 'TAIL')", name="phase_valid"),
        sa.CheckConstraint(
            "length(submission_key) BETWEEN 1 AND 128",
            name="submission_key_length_valid",
        ),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND "
            "length(manifest_digest) = 64 AND "
            "length(parameters_digest) = 64 AND "
            "length(artifact_digest) = 64",
            name="digest_lengths_valid",
        ),
        sa.CheckConstraint("canonical_size > 0", name="canonical_size_positive"),
        sa.CheckConstraint("state IN ('ACTIVE', 'RELEASED')", name="state_valid"),
        sa.CheckConstraint(
            "(state = 'ACTIVE' AND released_at IS NULL) OR "
            "(state = 'RELEASED' AND released_at IS NOT NULL)",
            name="release_shape_valid",
        ),
        sa.Index(
            "ix_stream_finite_submission__flow_run_id_state",
            "flow_run_id",
            "state",
        ),
    )


class StreamFiniteReceipt(Base):
    """The immutable bounded result accepted for one finite TaskRun."""

    finite_submission_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("stream_finite_submission.id", ondelete="CASCADE")
    )
    task_run_id: Mapped[uuid.UUID] = mapped_column(sa.ForeignKey("task_run.id"))
    operation_id: Mapped[uuid.UUID]
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    outcome_digest: Mapped[str] = mapped_column(sa.String(64))
    run_count: Mapped[int]
    result: Mapped[Any] = mapped_column(stream_value_JSON)
    result_digest: Mapped[str] = mapped_column(sa.String(64))
    result_size: Mapped[int]

    __table_args__: Any = (
        sa.UniqueConstraint("finite_submission_id"),
        sa.UniqueConstraint("task_run_id"),
        sa.UniqueConstraint("operation_id"),
        sa.CheckConstraint("run_count >= 1", name="run_count_positive"),
        sa.CheckConstraint("result_size > 0", name="result_size_positive"),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND "
            "length(outcome_digest) = 64 AND "
            "length(result_digest) = 64",
            name="digest_lengths_valid",
        ),
    )


class StreamExecutionAuthority(Base):
    """The current process-generation fence for one enrolled FlowRun."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    generation: Mapped[int] = mapped_column(sa.BigInteger)
    prepared_generation: Mapped[int] = mapped_column(sa.BigInteger)
    holder_id: Mapped[uuid.UUID]
    holder_capability_digest: Mapped[str] = mapped_column(sa.String(64))
    holder_event_public_key: Mapped[str] = mapped_column(sa.String(64))
    event_key_id: Mapped[str] = mapped_column(sa.String(64))
    supervisor_id: Mapped[uuid.UUID]
    supervisor_capability_digest: Mapped[str] = mapped_column(sa.String(64))
    state: Mapped[str]
    flow_run_run_count: Mapped[int] = mapped_column(sa.BigInteger)
    manifest_set_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    manifest_count: Mapped[Optional[int]]
    manifest_sealed_at: Mapped[Optional[DateTime]]
    manifest_seal_operation_id: Mapped[Optional[uuid.UUID]]
    manifest_seal_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    finite_prefix_manifest_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    finite_prefix_task_run_id: Mapped[Optional[uuid.UUID]]
    finite_prefix_sealed_at: Mapped[Optional[DateTime]]
    source_admission_closed: Mapped[bool] = mapped_column(
        server_default=sa.false(), default=False
    )
    cancel_requested: Mapped[bool] = mapped_column(
        server_default=sa.false(), default=False
    )

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id"),
        sa.CheckConstraint("generation BETWEEN 1 AND 64", name="generation_bounded"),
        sa.CheckConstraint(
            "prepared_generation BETWEEN 0 AND generation",
            name="prepared_generation_bounded",
        ),
        sa.CheckConstraint(
            "flow_run_run_count >= 0", name="flow_run_run_count_nonnegative"
        ),
        sa.CheckConstraint(
            "length(holder_capability_digest) = 64 "
            "AND length(supervisor_capability_digest) = 64 "
            "AND length(holder_event_public_key) = 64 "
            "AND length(event_key_id) = 64",
            name="authority_identity_lengths_valid",
        ),
        sa.CheckConstraint(
            "state IN ('CLAIMED', 'ACTIVE', 'RECOVERING', 'TERMINAL', 'RELEASED')",
            name="state_valid",
        ),
        sa.CheckConstraint(
            "(manifest_set_digest IS NULL AND manifest_count IS NULL "
            "AND manifest_sealed_at IS NULL AND manifest_seal_operation_id IS NULL "
            "AND manifest_seal_request_digest IS NULL) OR "
            "(manifest_set_digest IS NOT NULL "
            "AND length(manifest_set_digest) = 64 "
            "AND manifest_count IS NOT NULL "
            "AND manifest_count BETWEEN 1 AND 128 "
            "AND manifest_sealed_at IS NOT NULL "
            "AND manifest_seal_operation_id IS NOT NULL "
            "AND manifest_seal_request_digest IS NOT NULL "
            "AND length(manifest_seal_request_digest) = 64)",
            name="manifest_seal_shape_valid",
        ),
        sa.CheckConstraint(
            "(finite_prefix_manifest_digest IS NULL "
            "AND finite_prefix_task_run_id IS NULL "
            "AND finite_prefix_sealed_at IS NULL) OR "
            "(finite_prefix_manifest_digest IS NOT NULL "
            "AND length(finite_prefix_manifest_digest) = 64 "
            "AND finite_prefix_task_run_id IS NOT NULL "
            "AND finite_prefix_sealed_at IS NOT NULL)",
            name="finite_prefix_anchor_shape_valid",
        ),
        sa.CheckConstraint(
            "cancel_requested = false OR source_admission_closed = true",
            name="cancel_closes_source",
        ),
    )


class StreamExecutionAttempt(Base):
    """A bounded immutable claim or takeover witness for one generation."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    generation: Mapped[int] = mapped_column(sa.BigInteger)
    operation_id: Mapped[uuid.UUID]
    kind: Mapped[str]
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    outcome_digest: Mapped[str] = mapped_column(sa.String(64))
    holder_id: Mapped[uuid.UUID]
    holder_event_public_key: Mapped[str] = mapped_column(sa.String(64))
    event_key_id: Mapped[str] = mapped_column(sa.String(64))
    previous_generation: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    previous_holder_id: Mapped[Optional[uuid.UUID]]
    death_witness: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    state: Mapped[str]
    flow_run_run_count: Mapped[int] = mapped_column(sa.BigInteger)
    recovery_operation_id: Mapped[Optional[uuid.UUID]]
    recovery_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    recovery_outcome: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    recovery_outcome_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    abort_operation_id: Mapped[Optional[uuid.UUID]]
    abort_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    abort_outcome: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    abort_outcome_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id", "generation"),
        sa.UniqueConstraint("flow_run_id", "operation_id"),
        sa.UniqueConstraint("flow_run_id", "recovery_operation_id"),
        sa.UniqueConstraint("flow_run_id", "abort_operation_id"),
        sa.CheckConstraint("generation BETWEEN 1 AND 64", name="generation_bounded"),
        sa.CheckConstraint("kind IN ('CLAIM', 'TAKEOVER')", name="kind_valid"),
        sa.CheckConstraint(
            "state IN ('CLAIMED', 'ACTIVE', 'RECOVERING', 'TERMINAL', 'RELEASED')",
            name="state_valid",
        ),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(outcome_digest) = 64 "
            "AND length(holder_event_public_key) = 64 "
            "AND length(event_key_id) = 64",
            name="attempt_identity_lengths_valid",
        ),
        sa.CheckConstraint(
            "flow_run_run_count >= 0", name="flow_run_run_count_nonnegative"
        ),
        sa.CheckConstraint(
            "(kind = 'CLAIM' AND previous_generation IS NULL "
            "AND previous_holder_id IS NULL AND death_witness IS NULL) OR "
            "(kind = 'TAKEOVER' AND previous_generation IS NOT NULL "
            "AND previous_generation >= 1 AND previous_holder_id IS NOT NULL "
            "AND death_witness IS NOT NULL)",
            name="takeover_shape_valid",
        ),
        sa.CheckConstraint(
            "(recovery_operation_id IS NULL AND recovery_request_digest IS NULL "
            "AND recovery_outcome IS NULL AND recovery_outcome_digest IS NULL) OR "
            "(recovery_operation_id IS NOT NULL "
            "AND recovery_request_digest IS NOT NULL "
            "AND length(recovery_request_digest) = 64 "
            "AND recovery_outcome IS NOT NULL "
            "AND recovery_outcome_digest IS NOT NULL "
            "AND length(recovery_outcome_digest) = 64)",
            name="recovery_shape_valid",
        ),
        sa.CheckConstraint(
            "(abort_operation_id IS NULL AND abort_request_digest IS NULL "
            "AND abort_outcome IS NULL AND abort_outcome_digest IS NULL) OR "
            "(abort_operation_id IS NOT NULL "
            "AND abort_request_digest IS NOT NULL "
            "AND length(abort_request_digest) = 64 "
            "AND abort_outcome IS NOT NULL "
            "AND abort_outcome_digest IS NOT NULL "
            "AND length(abort_outcome_digest) = 64)",
            name="abort_shape_valid",
        ),
        sa.Index(
            "ix_stream_execution_attempt__flow_run_id_generation",
            "flow_run_id",
            "generation",
        ),
    )


class NativeStreamDeploymentSnapshot(Base):
    """Immutable native capability copied from a deployment to one FlowRun."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    deployment_id: Mapped[uuid.UUID]
    deployment_version: Mapped[Optional[str]]
    work_pool_id: Mapped[uuid.UUID]
    declaration: Mapped[dict[str, Any]] = mapped_column(JSON)
    declaration_digest: Mapped[str] = mapped_column(sa.String(64))
    protocol_version: Mapped[int]
    profile: Mapped[str]
    flow_version: Mapped[str] = mapped_column(sa.String(255))
    flow_code_digest: Mapped[str] = mapped_column(sa.String(64))
    resource_profile: Mapped[str]
    state: Mapped[str] = mapped_column(server_default="ACTIVE", default="ACTIVE")

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id"),
        sa.CheckConstraint("protocol_version = 1", name="protocol_version_valid"),
        sa.CheckConstraint(
            "profile IN ('bounded-linear-v1', 'partitioned-fixed-v1', 'partitioned-dag-v1')",
            name="profile_valid",
        ),
        sa.CheckConstraint(
            "resource_profile = 'bounded-default-v1'",
            name="resource_profile_valid",
        ),
        sa.CheckConstraint(
            "length(flow_version) BETWEEN 1 AND 255", name="flow_version_valid"
        ),
        sa.CheckConstraint(
            "length(declaration_digest) = 64 AND length(flow_code_digest) = 64",
            name="digest_lengths_valid",
        ),
        sa.CheckConstraint("state IN ('ACTIVE', 'RELEASED')", name="state_valid"),
    )


class NativeStreamLaunchIdentity(Base):
    """Bounded worker and Linux child identity for one execution generation."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("native_stream_deployment_snapshot.id", ondelete="CASCADE")
    )
    generation: Mapped[int]
    enrollment_operation_id: Mapped[uuid.UUID]
    enrollment_request_digest: Mapped[str] = mapped_column(sa.String(64))
    enrollment_outcome_digest: Mapped[str] = mapped_column(sa.String(64))
    work_pool_id: Mapped[uuid.UUID]
    worker_id: Mapped[uuid.UUID]
    launcher_instance_id: Mapped[uuid.UUID]
    launcher_host_id: Mapped[uuid.UUID]
    launcher_boot_id: Mapped[uuid.UUID]
    launcher_process_id: Mapped[int] = mapped_column(sa.BigInteger)
    launcher_start_ticks: Mapped[int] = mapped_column(sa.BigInteger)
    launch_profile: Mapped[str]
    holder_id: Mapped[uuid.UUID]
    holder_capability_digest: Mapped[str] = mapped_column(sa.String(64))
    holder_event_public_key: Mapped[str] = mapped_column(sa.String(64))
    supervisor_id: Mapped[uuid.UUID]
    supervisor_capability_digest: Mapped[str] = mapped_column(sa.String(64))
    state: Mapped[str] = mapped_column(server_default="ENROLLED", default="ENROLLED")
    process_registration_operation_id: Mapped[Optional[uuid.UUID]]
    process_registration_request_digest: Mapped[Optional[str]] = mapped_column(
        sa.String(64)
    )
    process_registration_outcome_digest: Mapped[Optional[str]] = mapped_column(
        sa.String(64)
    )
    host_id: Mapped[Optional[uuid.UUID]]
    boot_id: Mapped[Optional[uuid.UUID]]
    child_process_id: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    process_start_ticks: Mapped[Optional[int]] = mapped_column(sa.BigInteger)
    launch_nonce_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    process_registered_at: Mapped[Optional[DateTime]]
    last_renewed_at: Mapped[Optional[DateTime]]

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id", "generation"),
        sa.UniqueConstraint("flow_run_id", "enrollment_operation_id"),
        sa.UniqueConstraint("flow_run_id", "process_registration_operation_id"),
        sa.CheckConstraint("generation BETWEEN 1 AND 64", name="generation_bounded"),
        sa.CheckConstraint(
            "launch_profile = 'linux-local-pidfd-v1'",
            name="launch_profile_valid",
        ),
        sa.CheckConstraint(
            "state IN ('ENROLLED', 'PROCESS_REGISTERED', 'RELEASED')",
            name="state_valid",
        ),
        sa.CheckConstraint(
            "length(enrollment_request_digest) = 64 "
            "AND length(enrollment_outcome_digest) = 64 "
            "AND length(holder_capability_digest) = 64 "
            "AND length(holder_event_public_key) = 64 "
            "AND length(supervisor_capability_digest) = 64",
            name="identity_lengths_valid",
        ),
        sa.CheckConstraint(
            "launcher_process_id > 0 AND launcher_start_ticks > 0",
            name="launcher_identity_valid",
        ),
        sa.CheckConstraint(
            "(state IN ('ENROLLED', 'RELEASED') "
            "AND process_registration_operation_id IS NULL "
            "AND process_registration_request_digest IS NULL "
            "AND process_registration_outcome_digest IS NULL "
            "AND host_id IS NULL AND boot_id IS NULL "
            "AND child_process_id IS NULL AND process_start_ticks IS NULL "
            "AND launch_nonce_digest IS NULL AND process_registered_at IS NULL) OR "
            "(state IN ('PROCESS_REGISTERED', 'RELEASED') "
            "AND process_registration_operation_id IS NOT NULL "
            "AND process_registration_request_digest IS NOT NULL "
            "AND length(process_registration_request_digest) = 64 "
            "AND process_registration_outcome_digest IS NOT NULL "
            "AND length(process_registration_outcome_digest) = 64 "
            "AND host_id IS NOT NULL AND boot_id IS NOT NULL "
            "AND child_process_id IS NOT NULL AND child_process_id > 0 "
            "AND process_start_ticks IS NOT NULL AND process_start_ticks > 0 "
            "AND launch_nonce_digest IS NOT NULL "
            "AND length(launch_nonce_digest) = 64 "
            "AND process_registered_at IS NOT NULL)",
            name="process_shape_valid",
        ),
        sa.CheckConstraint(
            "(state IN ('ENROLLED', 'PROCESS_REGISTERED') "
            "AND last_renewed_at IS NOT NULL) OR "
            "state = 'RELEASED'",
            name="renewal_shape_valid",
        ),
    )


class NativeStreamReplacementOperation(Base):
    """One server-authorized same-host replacement challenge and outcome."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    operation_id: Mapped[uuid.UUID]
    expected_generation: Mapped[int]
    worker_id: Mapped[uuid.UUID]
    work_pool_id: Mapped[uuid.UUID]
    host_id: Mapped[uuid.UUID]
    identity_kind: Mapped[str]
    challenge_capability_digest: Mapped[str] = mapped_column(sa.String(64))
    authorization_request_digest: Mapped[str] = mapped_column(sa.String(64))
    state: Mapped[str] = mapped_column(
        server_default="AUTHORIZED", default="AUTHORIZED"
    )
    confirm_request_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    confirm_outcome_digest: Mapped[Optional[str]] = mapped_column(sa.String(64))
    death_witness: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    new_generation: Mapped[Optional[int]]

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id", "operation_id"),
        sa.CheckConstraint(
            "expected_generation BETWEEN 1 AND 64",
            name="generation_valid",
        ),
        sa.CheckConstraint(
            "identity_kind IN ('LAUNCHER', 'CHILD')",
            name="identity_kind_valid",
        ),
        sa.CheckConstraint(
            "length(challenge_capability_digest) = 64 "
            "AND length(authorization_request_digest) = 64",
            name="authorization_valid",
        ),
        sa.CheckConstraint(
            "state IN ('AUTHORIZED', 'CONFIRMED', 'RELEASED')",
            name="state_valid",
        ),
        sa.CheckConstraint(
            "(state IN ('AUTHORIZED', 'RELEASED') "
            "AND confirm_request_digest IS NULL "
            "AND confirm_outcome_digest IS NULL "
            "AND new_generation IS NULL) OR "
            "(state IN ('CONFIRMED', 'RELEASED') "
            "AND confirm_request_digest IS NOT NULL "
            "AND length(confirm_request_digest) = 64 "
            "AND confirm_outcome_digest IS NOT NULL "
            "AND length(confirm_outcome_digest) = 64 "
            "AND death_witness IS NOT NULL "
            "AND new_generation = expected_generation + 1)",
            name="confirmation_valid",
        ),
    )


class NativeStreamControlOperation(Base):
    """Idempotent drain or cancel request for one execution generation."""

    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="CASCADE"), index=True
    )
    operation_id: Mapped[uuid.UUID]
    generation: Mapped[int]
    kind: Mapped[str]
    reason: Mapped[str] = mapped_column(sa.String(1024))
    request_digest: Mapped[str] = mapped_column(sa.String(64))
    outcome: Mapped[dict[str, Any]] = mapped_column(JSON)
    outcome_digest: Mapped[str] = mapped_column(sa.String(64))

    __table_args__: Any = (
        sa.UniqueConstraint("flow_run_id", "operation_id"),
        sa.CheckConstraint("generation BETWEEN 1 AND 64", name="generation_bounded"),
        sa.CheckConstraint(
            "kind IN ('DRAIN', 'CANCEL', 'CANCEL_FINALIZED', 'SPAWN_FAILED', "
            "'VERIFIED_RETIRED')",
            name="kind_valid",
        ),
        sa.CheckConstraint("length(reason) BETWEEN 1 AND 1024", name="reason_valid"),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(outcome_digest) = 64",
            name="digest_valid",
        ),
    )


_TaskInput = Union[
    schemas.core.TaskRunResult,
    schemas.core.FlowRunResult,
    schemas.core.Parameter,
    schemas.core.Constant,
]
_TaskInputs = dict[str, list[_TaskInput]]


class TaskRun(Run):
    """SQLAlchemy model of a task run."""

    flow_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="cascade"), index=True
    )

    task_key: Mapped[str] = mapped_column()
    dynamic_key: Mapped[str] = mapped_column()
    cache_key: Mapped[Optional[str]]
    cache_expiration: Mapped[Optional[DateTime]]
    task_version: Mapped[Optional[str]]
    flow_run_run_count: Mapped[int] = mapped_column(server_default="0", default=0)
    empirical_policy: Mapped[schemas.core.TaskRunPolicy] = mapped_column(
        Pydantic(schemas.core.TaskRunPolicy),
        server_default="{}",
        default=schemas.core.TaskRunPolicy,
    )
    task_inputs: Mapped[_TaskInputs] = mapped_column(
        Pydantic(_TaskInputs), server_default="{}", default=dict
    )
    tags: Mapped[list[str]] = mapped_column(JSON, server_default="[]", default=list)
    labels: Mapped[Optional[schemas.core.KeyValueLabels]] = mapped_column(JSON)

    state_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID,
        index=True,
    )

    # -------------------------- relationships

    # current states are eagerly loaded unless otherwise specified
    _state: Mapped[Optional[TaskRunState]] = relationship(
        lazy="selectin",
        foreign_keys=[state_id],
        primaryjoin="TaskRunState.id==TaskRun.state_id",
    )

    @hybrid_property
    def state(self) -> Optional[TaskRunState]:
        return self._state

    @state.inplace.setter
    def _set_state(self, value: Optional[TaskRunState]) -> None:
        # because this is a slightly non-standard SQLAlchemy relationship, we
        # prefer an explicit setter method to a setter property, because
        # user expectations about SQLAlchemy attribute assignment might not be
        # met, namely that an unrelated (from SQLAlchemy's perspective) field of
        # the provided state is also modified. However, property assignment
        # still works because the ORM model's __init__ depends on it.
        return self.set_state(value)

    def set_state(self, state: Optional[TaskRunState]) -> None:
        """
        If a state is assigned to this run, populate its run id.

        This would normally be handled by the back-populated SQLAlchemy
        relationship, but because this is a one-to-one pointer to a
        one-to-many relationship, SQLAlchemy can't figure it out.
        """
        if state is not None:
            state.task_run_id = self.id
        self._state = state

    flow_run: Mapped[Optional["FlowRun"]] = relationship(
        back_populates="task_runs",
        lazy="raise",
        foreign_keys=[flow_run_id],
    )

    subflow_run: Mapped["FlowRun"] = relationship(
        back_populates="parent_task_run",
        lazy="raise",
        # foreign_keys=["FlowRun.parent_task_run_id"],
        primaryjoin="FlowRun.parent_task_run_id==TaskRun.id",
        uselist=False,
    )

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> Iterable[sa.Index]:
        return (
            sa.Index(
                "uq_task_run__flow_run_id_task_key_dynamic_key",
                cls.flow_run_id,
                cls.task_key,
                cls.dynamic_key,
                unique=True,
            ),
            sa.Index(
                "ix_task_run__expected_start_time_desc",
                cls.expected_start_time.desc(),
            ),
            sa.Index(
                "ix_task_run__next_scheduled_start_time_asc",
                cls.next_scheduled_start_time.asc(),
            ),
            sa.Index(
                "ix_task_run__end_time_desc",
                cls.end_time.desc(),
            ),
            sa.Index(
                "ix_task_run__start_time",
                cls.start_time,
            ),
            sa.Index(
                "ix_task_run__state_type",
                cls.state_type,
            ),
            sa.Index(
                "ix_task_run__state_type_start_time",
                cls.state_type,
                cls.start_time,
            ),
            sa.Index(
                "ix_task_run__state_name",
                cls.state_name,
            ),
            sa.Index(
                "ix_task_run__state_timestamp",
                cls.state_timestamp,
            ),
            sa.Index("trgm_ix_task_run_name", cls.name, postgresql_using="gin").ddl_if(
                dialect="postgresql"
            ),
        )


class DeploymentSchedule(Base):
    deployment_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("deployment.id", ondelete="CASCADE"), index=True
    )

    schedule: Mapped[schemas.schedules.SCHEDULE_TYPES] = mapped_column(
        Pydantic(schemas.schedules.SCHEDULE_TYPES)
    )
    active: Mapped[bool] = mapped_column(default=True)
    max_scheduled_runs: Mapped[Optional[int]]
    parameters: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default=dict, nullable=False
    )
    slug: Mapped[Optional[str]] = mapped_column(sa.String, nullable=True)

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> Iterable[sa.Index]:
        return (
            sa.Index(
                "ix_deployment_schedule__deployment_id__slug",
                cls.deployment_id,
                cls.slug,
                unique=True,
            ),
            sa.Index(
                "ix_deployment_schedule__slug",
                cls.slug,
                unique=False,
            ),
        )


class Deployment(Base):
    """SQLAlchemy model of a deployment."""

    name: Mapped[str]
    version: Mapped[Optional[str]]
    description: Mapped[Optional[str]] = mapped_column(sa.Text())
    work_queue_name: Mapped[Optional[str]] = mapped_column(index=True)
    infra_overrides: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default=dict
    )
    path: Mapped[Optional[str]]
    entrypoint: Mapped[Optional[str]]

    last_polled: Mapped[Optional[DateTime]]
    status: Mapped[DeploymentStatus] = mapped_column(
        sa.Enum(DeploymentStatus, name="deployment_status"),
        default=DeploymentStatus.NOT_READY,
        server_default="NOT_READY",
    )

    @declared_attr
    def job_variables(self) -> Mapped[dict[str, Any]]:
        return synonym("infra_overrides")

    flow_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow.id", ondelete="CASCADE"), index=True
    )

    work_queue_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("work_queue.id", ondelete="SET NULL"), index=True
    )
    paused: Mapped[bool] = mapped_column(server_default="0", default=False, index=True)

    schedules: Mapped[list["DeploymentSchedule"]] = relationship(
        lazy="selectin", order_by=lambda: DeploymentSchedule.updated.desc()
    )

    # deprecated in favor of `concurrency_limit_id` FK
    _concurrency_limit: Mapped[Optional[int]] = mapped_column(name="concurrency_limit")
    concurrency_limit_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("concurrency_limit_v2.id", ondelete="SET NULL"),
    )
    global_concurrency_limit: Mapped[Optional["ConcurrencyLimitV2"]] = relationship(
        lazy="selectin",
    )
    concurrency_options: Mapped[Optional[schemas.core.ConcurrencyOptions]] = (
        mapped_column(
            Pydantic(schemas.core.ConcurrencyOptions),
            server_default=None,
            nullable=True,
            default=None,
        )
    )

    tags: Mapped[list[str]] = mapped_column(JSON, server_default="[]", default=list)
    labels: Mapped[Optional[schemas.core.KeyValueLabels]] = mapped_column(JSON)
    parameters: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default=dict
    )
    pull_steps: Mapped[Optional[list[dict[str, Any]]]] = mapped_column(
        JSON, default=list
    )
    parameter_openapi_schema: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, default=dict
    )
    enforce_parameter_schema: Mapped[bool] = mapped_column(
        default=True, server_default="0"
    )
    created_by: Mapped[Optional[schemas.core.CreatedBy]] = mapped_column(
        Pydantic(schemas.core.CreatedBy)
    )
    updated_by: Mapped[Optional[schemas.core.UpdatedBy]] = mapped_column(
        Pydantic(schemas.core.UpdatedBy)
    )

    infrastructure_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("block_document.id", ondelete="CASCADE"), index=False
    )

    storage_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.ForeignKey("block_document.id", ondelete="CASCADE"),
        index=False,
    )

    flow: Mapped["Flow"] = relationship(
        "Flow", back_populates="deployments", lazy="raise"
    )

    work_queue: Mapped[Optional["WorkQueue"]] = relationship(
        lazy="selectin", foreign_keys=[work_queue_id]
    )

    __table_args__: Any = (
        sa.Index(
            "uq_deployment__flow_id_name",
            "flow_id",
            "name",
            unique=True,
        ),
        sa.Index(
            "ix_deployment__created",
            "created",
        ),
        sa.Index("trgm_ix_deployment_name", "name", postgresql_using="gin").ddl_if(
            dialect="postgresql"
        ),
    )


class Log(Base):
    """
    SQLAlchemy model of a logging statement.
    """

    name: Mapped[str]
    level: Mapped[int] = mapped_column(sa.SmallInteger, index=True)
    flow_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(index=True)
    task_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(index=True)
    message: Mapped[str] = mapped_column(sa.Text)

    # The client-side timestamp of this logged statement.
    timestamp: Mapped[DateTime] = mapped_column(index=True)

    __table_args__: Any = (
        sa.Index(
            "ix_log__flow_run_id_timestamp",
            "flow_run_id",
            "timestamp",
        ),
    )


class ConcurrencyLimit(Base):
    tag: Mapped[str]
    concurrency_limit: Mapped[int]
    active_slots: Mapped[list[str]] = mapped_column(
        JSON, server_default="[]", default=list
    )

    __table_args__: Any = (sa.Index("uq_concurrency_limit__tag", "tag", unique=True),)


class ConcurrencyLimitV2(Base):
    active: Mapped[bool] = mapped_column(default=True)
    name: Mapped[str]
    limit: Mapped[int]
    active_slots: Mapped[int] = mapped_column(default=0)
    denied_slots: Mapped[int] = mapped_column(default=0)

    slot_decay_per_second: Mapped[float] = mapped_column(default=0.0)
    avg_slot_occupancy_seconds: Mapped[float] = mapped_column(default=2.0)

    __table_args__: Any = (sa.UniqueConstraint("name"),)


class BlockType(Base):
    name: Mapped[str]
    slug: Mapped[str]
    logo_url: Mapped[Optional[str]]
    documentation_url: Mapped[Optional[str]]
    description: Mapped[Optional[str]]
    code_example: Mapped[Optional[str]]
    is_protected: Mapped[bool] = mapped_column(server_default="0", default=False)

    __table_args__: Any = (
        sa.Index(
            "uq_block_type__slug",
            "slug",
            unique=True,
        ),
        sa.Index("trgm_ix_block_type_name", "name", postgresql_using="gin").ddl_if(
            dialect="postgresql"
        ),
    )


class BlockSchema(Base):
    checksum: Mapped[str]
    fields: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default=dict
    )
    capabilities: Mapped[list[str]] = mapped_column(
        JSON, server_default="[]", default=list
    )
    version: Mapped[str] = mapped_column(
        server_default=schemas.core.DEFAULT_BLOCK_SCHEMA_VERSION,
    )

    block_type_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("block_type.id", ondelete="cascade"), index=True
    )

    block_type: Mapped["BlockType"] = relationship(lazy="selectin")

    __table_args__: Any = (
        sa.Index(
            "uq_block_schema__checksum_version",
            "checksum",
            "version",
            unique=True,
        ),
        sa.Index("ix_block_schema__created", "created"),
        sa.Index(
            "ix_block_schema__capabilities", "capabilities", postgresql_using="gin"
        ).ddl_if(dialect="postgresql"),
    )


class BlockSchemaReference(Base):
    name: Mapped[str]

    parent_block_schema_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("block_schema.id", ondelete="cascade")
    )

    reference_block_schema_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("block_schema.id", ondelete="cascade")
    )


class BlockDocument(Base):
    name: Mapped[str] = mapped_column(index=True)
    data: Mapped[Any] = mapped_column(JSON, server_default="{}", default=dict)
    is_anonymous: Mapped[bool] = mapped_column(server_default="0", index=True)

    block_type_name: Mapped[Optional[str]]

    block_type_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("block_type.id", ondelete="cascade")
    )

    block_type: Mapped["BlockType"] = relationship(lazy="selectin")

    block_schema_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("block_schema.id", ondelete="cascade")
    )

    block_schema: Mapped["BlockSchema"] = relationship(lazy="selectin")

    __table_args__: Any = (
        sa.Index(
            "uq_block__type_id_name",
            "block_type_id",
            "name",
            unique=True,
        ),
        sa.Index("ix_block_document__block_type_name__name", "block_type_name", "name"),
        sa.Index("trgm_ix_block_document_name", "name", postgresql_using="gin").ddl_if(
            dialect="postgresql"
        ),
    )

    async def encrypt_data(self, session: AsyncSession, data: dict[str, Any]) -> None:
        """
        Store encrypted data on the ORM model

        Note: will only succeed if the caller has sufficient permission.
        """
        self.data = await encrypt_fernet(session, data)

    async def decrypt_data(self, session: AsyncSession) -> dict[str, Any]:
        """
        Retrieve decrypted data from the ORM model.

        Note: will only succeed if the caller has sufficient permission.
        """
        return await decrypt_fernet(session, self.data)


class BlockDocumentReference(Base):
    name: Mapped[str]

    parent_block_document_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("block_document.id", ondelete="cascade"),
    )

    reference_block_document_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("block_document.id", ondelete="cascade"),
    )

    __table_args__: Any = (
        sa.CheckConstraint(
            "parent_block_document_id != reference_block_document_id",
            name="ck_block_document_reference__no_self_reference",
        ),
    )


class Configuration(Base):
    key: Mapped[str] = mapped_column(index=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSON)

    __table_args__: Any = (sa.UniqueConstraint("key"),)


class SavedSearch(Base):
    """SQLAlchemy model of a saved search."""

    name: Mapped[str]
    filters: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, server_default="[]", default=list
    )

    __table_args__: Any = (sa.UniqueConstraint("name"),)


class WorkQueue(Base):
    """SQLAlchemy model of a work queue"""

    name: Mapped[str]

    filter: Mapped[Optional[schemas.core.QueueFilter]] = mapped_column(
        Pydantic(schemas.core.QueueFilter)
    )
    description: Mapped[str] = mapped_column(default="", server_default="")
    is_paused: Mapped[bool] = mapped_column(server_default="0", default=False)
    concurrency_limit: Mapped[Optional[int]]
    priority: Mapped[int]

    last_polled: Mapped[Optional[DateTime]]
    status: Mapped[WorkQueueStatus] = mapped_column(
        sa.Enum(WorkQueueStatus, name="work_queue_status"),
        default=WorkQueueStatus.NOT_READY,
        server_default=WorkQueueStatus.NOT_READY,
    )

    work_pool_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("work_pool.id", ondelete="cascade"), index=True
    )

    work_pool: Mapped["WorkPool"] = relationship(
        lazy="selectin", foreign_keys=[work_pool_id]
    )

    __table_args__: ClassVar[Any] = (
        sa.UniqueConstraint("work_pool_id", "name"),
        sa.Index("ix_work_queue__work_pool_id_priority", "work_pool_id", "priority"),
        sa.Index("trgm_ix_work_queue_name", "name", postgresql_using="gin").ddl_if(
            dialect="postgresql"
        ),
    )


class WorkPool(Base):
    """SQLAlchemy model of an worker"""

    name: Mapped[str]
    description: Mapped[Optional[str]]
    type: Mapped[str] = mapped_column(index=True)
    base_job_template: Mapped[dict[str, Any]] = mapped_column(
        JSON, server_default="{}", default={}
    )
    is_paused: Mapped[bool] = mapped_column(server_default="0", default=False)
    default_queue_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID,
        sa.ForeignKey("work_queue.id", ondelete="RESTRICT", use_alter=True),
        nullable=True,
    )
    concurrency_limit: Mapped[Optional[int]]

    status: Mapped[WorkPoolStatus] = mapped_column(
        sa.Enum(WorkPoolStatus, name="work_pool_status"),
        default=WorkPoolStatus.NOT_READY,
        server_default=WorkPoolStatus.NOT_READY,
    )
    last_transitioned_status_at: Mapped[Optional[DateTime]]
    last_status_event_id: Mapped[Optional[uuid.UUID]]

    storage_configuration: Mapped[schemas.core.WorkPoolStorageConfiguration] = (
        mapped_column(
            Pydantic(schemas.core.WorkPoolStorageConfiguration),
            server_default="{}",
            default=schemas.core.WorkPoolStorageConfiguration,
            nullable=False,
        )
    )

    __table_args__: Any = (sa.UniqueConstraint("name"),)


class Worker(Base):
    """SQLAlchemy model of an worker"""

    work_pool_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("work_pool.id", ondelete="cascade"), index=True
    )

    name: Mapped[str]
    last_heartbeat_time: Mapped[DateTime] = mapped_column(
        server_default=sa.func.now(), default=lambda: now("UTC")
    )
    heartbeat_interval_seconds: Mapped[Optional[int]]

    status: Mapped[WorkerStatus] = mapped_column(
        sa.Enum(WorkerStatus, name="worker_status"),
        default=WorkerStatus.OFFLINE,
        server_default=WorkerStatus.OFFLINE,
    )

    __table_args__: Any = (
        sa.UniqueConstraint("work_pool_id", "name"),
        sa.Index(
            "ix_worker__work_pool_id_last_heartbeat_time",
            "work_pool_id",
            "last_heartbeat_time",
        ),
    )


class Agent(Base):
    """SQLAlchemy model of an agent"""

    name: Mapped[str]

    work_queue_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("work_queue.id"), index=True
    )

    last_activity_time: Mapped[DateTime] = mapped_column(
        server_default=sa.func.now(), default=lambda: now("UTC")
    )

    __table_args__: Any = (sa.UniqueConstraint("name"),)


class Variable(Base):
    name: Mapped[str]
    value: Mapped[Optional[Any]] = mapped_column(JSON)
    tags: Mapped[list[str]] = mapped_column(JSON, server_default="[]", default=list)

    __table_args__: Any = (sa.UniqueConstraint("name"),)


class FlowRunInput(Base):
    flow_run_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("flow_run.id", ondelete="cascade")
    )

    key: Mapped[str]
    value: Mapped[str] = mapped_column(sa.Text())
    sender: Mapped[Optional[str]]

    __table_args__: Any = (sa.UniqueConstraint("flow_run_id", "key"),)


class CsrfToken(Base):
    token: Mapped[str]
    client: Mapped[str] = mapped_column(unique=True)
    expiration: Mapped[DateTime]


class Automation(Base):
    name: Mapped[str]
    description: Mapped[str] = mapped_column(default="")

    enabled: Mapped[bool] = mapped_column(server_default="1", default=True)
    tags: Mapped[list[str]] = mapped_column(JSON, server_default="[]", default=list)

    trigger: Mapped[ServerTriggerTypes] = mapped_column(Pydantic(ServerTriggerTypes))

    actions: Mapped[ServerActionTypes] = mapped_column(
        Pydantic(list[ServerActionTypes])
    )
    actions_on_trigger: Mapped[list[ServerActionTypes]] = mapped_column(
        Pydantic(list[ServerActionTypes]), server_default="[]", default=list
    )
    actions_on_resolve: Mapped[list[ServerActionTypes]] = mapped_column(
        Pydantic(list[ServerActionTypes]), server_default="[]", default=list
    )

    related_resources: Mapped[list["AutomationRelatedResource"]] = relationship(
        "AutomationRelatedResource", back_populates="automation", lazy="raise"
    )

    @classmethod
    def sort_expression(cls, value: AutomationSort) -> sa.ColumnExpressionArgument[Any]:
        """Return an expression used to sort Automations"""
        sort_mapping: dict[AutomationSort, sa.ColumnExpressionArgument[Any]] = {
            AutomationSort.CREATED_DESC: cls.created.desc(),
            AutomationSort.UPDATED_DESC: cls.updated.desc(),
            AutomationSort.NAME_ASC: cls.name.asc(),
            AutomationSort.NAME_DESC: cls.name.desc(),
        }
        return sort_mapping[value]


class AutomationBucket(Base):
    __table_args__: Any = (
        sa.Index(
            "uq_automation_bucket__automation_id__trigger_id__bucketing_key",
            "automation_id",
            "trigger_id",
            "bucketing_key",
            unique=True,
        ),
        sa.Index(
            "ix_automation_bucket__automation_id__end",
            "automation_id",
            "end",
        ),
    )

    automation_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("automation.id", ondelete="CASCADE")
    )

    trigger_id: Mapped[uuid.UUID]

    bucketing_key: Mapped[list[str]] = mapped_column(
        JSON, server_default="[]", default=list
    )

    last_event: Mapped[Optional[ReceivedEvent]] = mapped_column(Pydantic(ReceivedEvent))

    start: Mapped[DateTime]
    end: Mapped[DateTime]

    count: Mapped[int]

    last_operation: Mapped[Optional[str]]

    triggered_at: Mapped[Optional[DateTime]]


class AutomationRelatedResource(Base):
    __table_args__: Any = (
        sa.Index(
            "uq_automation_related_resource__automation_id__resource_id",
            "automation_id",
            "resource_id",
            unique=True,
        ),
    )

    automation_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("automation.id", ondelete="CASCADE")
    )

    resource_id: Mapped[Optional[str]] = mapped_column(index=True)
    automation_owned_by_resource: Mapped[bool] = mapped_column(
        default=False, server_default="0"
    )

    automation: Mapped["Automation"] = relationship(
        "Automation", back_populates="related_resources", lazy="raise"
    )


class CompositeTriggerChildFiring(Base):
    __table_args__: Any = (
        sa.Index(
            "uq_composite_trigger_child_firing__a_id__pt_id__ct__id",
            "automation_id",
            "parent_trigger_id",
            "child_trigger_id",
            unique=True,
        ),
    )

    automation_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey("automation.id", ondelete="CASCADE")
    )

    parent_trigger_id: Mapped[uuid.UUID]

    child_trigger_id: Mapped[uuid.UUID]
    child_firing_id: Mapped[uuid.UUID]
    child_fired_at: Mapped[Optional[DateTime]]
    child_firing: Mapped[Firing] = mapped_column(Pydantic(Firing))


class AutomationEventFollower(Base):
    __table_args__: Any = (
        sa.Index(
            "uq_follower_for_scope",
            "scope",
            "follower_event_id",
            unique=True,
        ),
        # allows lookup on (scope, leader_event_id) to use an index-only instead of full table scan
        sa.Index(
            "ix_ae_follower_scope_leader",
            "scope",
            "leader_event_id",
        ),
    )
    scope: Mapped[str] = mapped_column(default="", index=True)
    leader_event_id: Mapped[uuid.UUID] = mapped_column(index=True)
    follower_event_id: Mapped[uuid.UUID]
    received: Mapped[DateTime] = mapped_column(index=True)
    follower: Mapped[ReceivedEvent] = mapped_column(Pydantic(ReceivedEvent))


class Event(Base):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return "events"

    __table_args__: Any = (
        sa.Index(
            "ix_events__related_resource_ids_gin",
            "related_resource_ids",
            postgresql_using="gin",
        ),
        sa.Index("ix_events__occurred", "occurred"),
        sa.Index("ix_events__event__id", "event", "id"),
        sa.Index(
            "ix_events__event_resource_id_occurred",
            "event",
            "resource_id",
            "occurred",
        ),
        sa.Index("ix_events__occurred_id", "occurred", "id"),
        sa.Index("ix_events__event_occurred_id", "event", "occurred", "id"),
        sa.Index(
            "ix_events__related_gin",
            "related",
            postgresql_using="gin",
        ),
        sa.Index(
            "ix_events__event_occurred",
            "event",
            "occurred",
        ),
    )

    occurred: Mapped[DateTime]
    event: Mapped[str] = mapped_column(sa.Text())
    resource_id: Mapped[str] = mapped_column(sa.Text())
    resource: Mapped[dict[str, Any]] = mapped_column(JSON())
    related_resource_ids: Mapped[list[str]] = mapped_column(
        JSON(), server_default="[]", default=list
    )
    related: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON(), server_default="[]", default=list
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSON())
    received: Mapped[DateTime]
    recorded: Mapped[DateTime]
    follows: Mapped[Optional[uuid.UUID]]


class EventResource(Base):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return "event_resources"

    __table_args__: Any = (
        sa.Index(
            "ix_event_resources__resource_id__occurred",
            "resource_id",
            "occurred",
        ),
        sa.Index(
            "ix_event_resources__occurred",
            "occurred",
        ),
        sa.Index(
            "ix_event_resources__event_id",
            "event_id",
        ),
    )

    occurred: Mapped[DateTime]
    resource_id: Mapped[str] = mapped_column(sa.Text())
    resource_role: Mapped[str] = mapped_column(sa.Text())
    resource: Mapped[dict[str, Any]] = mapped_column(sa_JSON)
    event_id: Mapped[uuid.UUID]


# These are temporary until we've migrated all the references to the new,
# non-ORM names

ORMFlow = Flow
ORMFlowRunState = FlowRunState
ORMTaskRunState = TaskRunState
ORMArtifact = Artifact
ORMArtifactCollection = ArtifactCollection
ORMTaskRunStateCache = TaskRunStateCache
ORMRun = Run
ORMFlowRun = FlowRun
ORMTaskRun = TaskRun
ORMDeploymentSchedule = DeploymentSchedule
ORMDeployment = Deployment
ORMLog = Log
ORMConcurrencyLimit = ConcurrencyLimit
ORMConcurrencyLimitV2 = ConcurrencyLimitV2
ORMBlockType = BlockType
ORMBlockSchema = BlockSchema
ORMBlockSchemaReference = BlockSchemaReference
ORMBlockDocument = BlockDocument
ORMBlockDocumentReference = BlockDocumentReference
ORMConfiguration = Configuration
ORMSavedSearch = SavedSearch
ORMWorkQueue = WorkQueue
ORMWorkPool = WorkPool
ORMWorker = Worker
ORMAgent = Agent
ORMVariable = Variable
ORMFlowRunInput = FlowRunInput
ORMCsrfToken = CsrfToken
ORMAutomation = Automation
ORMAutomationBucket = AutomationBucket
ORMAutomationRelatedResource = AutomationRelatedResource
ORMCompositeTriggerChildFiring = CompositeTriggerChildFiring
ORMAutomationEventFollower = AutomationEventFollower
ORMEvent = Event
ORMEventResource = EventResource


_UpsertColumns = Iterable[Union[str, "sa.Column[Any]", roles.DDLConstraintColumnRole]]


class BaseORMConfiguration(ABC):
    """
    Abstract base class used to inject database-specific ORM configuration into Prefect.

    Modifications to core Prefect REST API data structures can have unintended consequences.
    Use with caution.
    """

    def unique_key(self) -> tuple[Hashable, ...]:
        """
        Returns a key used to determine whether to instantiate a new DB interface.
        """
        return (self.__class__, Base.metadata)

    @property
    @abstractmethod
    def versions_dir(self) -> Path:
        """Directory containing migrations"""
        ...

    @property
    def deployment_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a Deployment"""
        return [Deployment.flow_id, Deployment.name]

    @property
    def concurrency_limit_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a ConcurrencyLimit"""
        return [ConcurrencyLimit.tag]

    @property
    def flow_run_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a FlowRun"""
        return [FlowRun.flow_id, FlowRun.idempotency_key]

    @property
    def block_type_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a BlockType"""
        return [BlockType.slug]

    @property
    def artifact_collection_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting an ArtifactCollection"""
        return [ArtifactCollection.key]

    @property
    def block_schema_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a BlockSchema"""
        return [BlockSchema.checksum, BlockSchema.version]

    @property
    def flow_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a Flow"""
        return [Flow.name]

    @property
    def saved_search_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a SavedSearch"""
        return [SavedSearch.name]

    @property
    def task_run_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a TaskRun"""
        return [
            TaskRun.flow_run_id,
            TaskRun.task_key,
            TaskRun.dynamic_key,
        ]

    @property
    def block_document_unique_upsert_columns(self) -> _UpsertColumns:
        """Unique columns for upserting a BlockDocument"""
        return [BlockDocument.block_type_id, BlockDocument.name]


class AsyncPostgresORMConfiguration(BaseORMConfiguration):
    """Postgres specific orm configuration"""

    @property
    def versions_dir(self) -> Path:
        """Directory containing migrations"""
        import prefect.server.database

        return (
            Path(prefect.server.database.__file__).parent
            / "_migrations"
            / "versions"
            / "postgresql"
        )


class AioSqliteORMConfiguration(BaseORMConfiguration):
    """SQLite specific orm configuration"""

    @property
    def versions_dir(self) -> Path:
        """Directory containing migrations"""
        import prefect.server.database

        return (
            Path(prefect.server.database.__file__).parent
            / "_migrations"
            / "versions"
            / "sqlite"
        )
