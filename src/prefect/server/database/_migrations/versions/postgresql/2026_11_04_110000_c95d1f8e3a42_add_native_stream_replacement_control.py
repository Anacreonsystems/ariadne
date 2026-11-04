"""Add native worker replacement and stream control journals.

Revision ID: c95d1f8e3a42
Revises: f73b9d6a1c20
Create Date: 2026-11-04 11:00:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import prefect

revision = "c95d1f8e3a42"
down_revision = "f73b9d6a1c20"
branch_labels = None
depends_on = None

general_JSON = postgresql.JSONB(none_as_null=True).with_variant(
    sa.JSON(none_as_null=True), "sqlite"
)


def _base_columns() -> list[sa.Column]:
    return [
        sa.Column(
            "id",
            prefect.server.utilities.database.UUID(),
            server_default=sa.text("(GEN_RANDOM_UUID())"),
            nullable=False,
        ),
        sa.Column(
            "created",
            prefect.server.utilities.database.Timestamp(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated",
            prefect.server.utilities.database.Timestamp(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    ]


def upgrade():
    op.add_column(
        "native_stream_launch_identity",
        sa.Column(
            "last_renewed_at",
            prefect.server.utilities.database.Timestamp(timezone=True),
            nullable=True,
        ),
    )
    op.execute(
        "UPDATE native_stream_launch_identity "
        "SET last_renewed_at = COALESCE(process_registered_at, created) "
        "WHERE state IN ('ENROLLED', 'PROCESS_REGISTERED')"
    )
    op.create_check_constraint(
        op.f("ck_native_stream_launch_identity__renewal_shape_valid"),
        "native_stream_launch_identity",
        "(state IN ('ENROLLED', 'PROCESS_REGISTERED') "
        "AND last_renewed_at IS NOT NULL) OR "
        "state = 'RELEASED'",
    )
    op.add_column(
        "stream_execution_authority",
        sa.Column(
            "source_admission_closed",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "stream_execution_authority",
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        op.f("ck_stream_execution_authority__cancel_closes_source"),
        "stream_execution_authority",
        "cancel_requested = false OR source_admission_closed = true",
    )
    op.create_table(
        "native_stream_replacement_operation",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "operation_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("expected_generation", sa.Integer(), nullable=False),
        sa.Column(
            "worker_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "work_pool_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("host_id", prefect.server.utilities.database.UUID(), nullable=False),
        sa.Column("identity_kind", sa.String(), nullable=False),
        sa.Column("challenge_capability_digest", sa.String(64), nullable=False),
        sa.Column("authorization_request_digest", sa.String(64), nullable=False),
        sa.Column("state", sa.String(), server_default="AUTHORIZED", nullable=False),
        sa.Column("confirm_request_digest", sa.String(64), nullable=True),
        sa.Column("confirm_outcome_digest", sa.String(64), nullable=True),
        sa.Column("death_witness", general_JSON, nullable=True),
        sa.Column("new_generation", sa.Integer(), nullable=True),
        *_base_columns(),
        sa.CheckConstraint(
            "expected_generation BETWEEN 1 AND 64",
            name=op.f("ck_native_stream_replacement_operation__generation_valid"),
        ),
        sa.CheckConstraint(
            "identity_kind IN ('LAUNCHER', 'CHILD')",
            name=op.f("ck_native_stream_replacement_operation__identity_kind_valid"),
        ),
        sa.CheckConstraint(
            "length(challenge_capability_digest) = 64 "
            "AND length(authorization_request_digest) = 64",
            name=op.f("ck_native_stream_replacement_operation__authorization_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('AUTHORIZED', 'CONFIRMED', 'RELEASED')",
            name=op.f("ck_native_stream_replacement_operation__state_valid"),
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
            name=op.f("ck_native_stream_replacement_operation__confirmation_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_native_stream_replacement_operation__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "id", name=op.f("pk_native_stream_replacement_operation")
        ),
        sa.UniqueConstraint(
            "flow_run_id",
            "operation_id",
            name=op.f(
                "uq_native_stream_replacement_operation__flow_run_id_operation_id"
            ),
        ),
    )
    op.create_index(
        op.f("ix_native_stream_replacement_operation__flow_run_id"),
        "native_stream_replacement_operation",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_native_stream_replacement_operation__updated"),
        "native_stream_replacement_operation",
        ["updated"],
        unique=False,
    )
    op.create_table(
        "native_stream_control_operation",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "operation_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("reason", sa.String(1024), nullable=False),
        sa.Column("request_digest", sa.String(64), nullable=False),
        sa.Column("outcome", general_JSON, nullable=False),
        sa.Column("outcome_digest", sa.String(64), nullable=False),
        *_base_columns(),
        sa.CheckConstraint(
            "generation BETWEEN 1 AND 64",
            name=op.f("ck_native_stream_control_operation__generation_bounded"),
        ),
        sa.CheckConstraint(
            "kind IN ('DRAIN', 'CANCEL', 'CANCEL_FINALIZED', 'SPAWN_FAILED', "
            "'VERIFIED_RETIRED')",
            name=op.f("ck_native_stream_control_operation__kind_valid"),
        ),
        sa.CheckConstraint(
            "length(reason) BETWEEN 1 AND 1024",
            name=op.f("ck_native_stream_control_operation__reason_valid"),
        ),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(outcome_digest) = 64",
            name=op.f("ck_native_stream_control_operation__digest_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_native_stream_control_operation__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_native_stream_control_operation")),
        sa.UniqueConstraint(
            "flow_run_id",
            "operation_id",
            name=op.f("uq_native_stream_control_operation__flow_run_id_operation_id"),
        ),
    )
    op.create_index(
        op.f("ix_native_stream_control_operation__flow_run_id"),
        "native_stream_control_operation",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_native_stream_control_operation__updated"),
        "native_stream_control_operation",
        ["updated"],
        unique=False,
    )


def downgrade():
    op.drop_index(
        op.f("ix_native_stream_control_operation__updated"),
        table_name="native_stream_control_operation",
    )
    op.drop_index(
        op.f("ix_native_stream_control_operation__flow_run_id"),
        table_name="native_stream_control_operation",
    )
    op.drop_table("native_stream_control_operation")
    op.drop_index(
        op.f("ix_native_stream_replacement_operation__updated"),
        table_name="native_stream_replacement_operation",
    )
    op.drop_index(
        op.f("ix_native_stream_replacement_operation__flow_run_id"),
        table_name="native_stream_replacement_operation",
    )
    op.drop_table("native_stream_replacement_operation")
    op.drop_constraint(
        op.f("ck_stream_execution_authority__cancel_closes_source"),
        "stream_execution_authority",
        type_="check",
    )
    op.drop_column("stream_execution_authority", "cancel_requested")
    op.drop_column("stream_execution_authority", "source_admission_closed")
    op.drop_constraint(
        op.f("ck_native_stream_launch_identity__renewal_shape_valid"),
        "native_stream_launch_identity",
        type_="check",
    )
    op.drop_column("native_stream_launch_identity", "last_renewed_at")
