"""Add native stream execution-generation authority.

Revision ID: f64a9d5b8c32
Revises: e42d7b9c5f10
Create Date: 2026-11-03 01:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import prefect

revision = "f64a9d5b8c32"
down_revision = "e42d7b9c5f10"
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
    op.create_table(
        "stream_execution_authority",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("prepared_generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "holder_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("holder_capability_digest", sa.String(length=64), nullable=False),
        sa.Column("holder_event_public_key", sa.String(length=64), nullable=False),
        sa.Column("event_key_id", sa.String(length=64), nullable=False),
        sa.Column(
            "supervisor_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("supervisor_capability_digest", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("flow_run_run_count", sa.BigInteger(), nullable=False),
        sa.Column("manifest_set_digest", sa.String(length=64), nullable=True),
        sa.Column("manifest_count", sa.Integer(), nullable=True),
        sa.Column(
            "manifest_sealed_at",
            prefect.server.utilities.database.Timestamp(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "manifest_seal_operation_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
        sa.Column("manifest_seal_request_digest", sa.String(length=64), nullable=True),
        *_base_columns(),
        sa.CheckConstraint(
            "length(holder_capability_digest) = 64 "
            "AND length(supervisor_capability_digest) = 64 "
            "AND length(holder_event_public_key) = 64 "
            "AND length(event_key_id) = 64",
            name=op.f(
                "ck_stream_execution_authority__authority_identity_lengths_valid"
            ),
        ),
        sa.CheckConstraint(
            "flow_run_run_count >= 0",
            name=op.f("ck_stream_execution_authority__flow_run_run_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "generation BETWEEN 1 AND 64",
            name=op.f("ck_stream_execution_authority__generation_bounded"),
        ),
        sa.CheckConstraint(
            "prepared_generation BETWEEN 0 AND generation",
            name=op.f("ck_stream_execution_authority__prepared_generation_bounded"),
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
            name=op.f("ck_stream_execution_authority__manifest_seal_shape_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('CLAIMED', 'ACTIVE', 'RECOVERING', 'TERMINAL', 'RELEASED')",
            name=op.f("ck_stream_execution_authority__state_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_stream_execution_authority__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_execution_authority")),
        sa.UniqueConstraint(
            "flow_run_id", name=op.f("uq_stream_execution_authority__flow_run_id")
        ),
    )
    op.create_index(
        op.f("ix_stream_execution_authority__flow_run_id"),
        "stream_execution_authority",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_stream_execution_authority__updated"),
        "stream_execution_authority",
        ["updated"],
        unique=False,
    )

    op.create_table(
        "stream_execution_attempt",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "operation_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("outcome_digest", sa.String(length=64), nullable=False),
        sa.Column(
            "holder_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("holder_event_public_key", sa.String(length=64), nullable=False),
        sa.Column("event_key_id", sa.String(length=64), nullable=False),
        sa.Column("previous_generation", sa.BigInteger(), nullable=True),
        sa.Column(
            "previous_holder_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
        sa.Column("death_witness", general_JSON, nullable=True),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("flow_run_run_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "recovery_operation_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
        sa.Column("recovery_request_digest", sa.String(length=64), nullable=True),
        sa.Column("recovery_outcome", general_JSON, nullable=True),
        sa.Column("recovery_outcome_digest", sa.String(length=64), nullable=True),
        *_base_columns(),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(outcome_digest) = 64 "
            "AND length(holder_event_public_key) = 64 "
            "AND length(event_key_id) = 64",
            name=op.f("ck_stream_execution_attempt__attempt_identity_lengths_valid"),
        ),
        sa.CheckConstraint(
            "flow_run_run_count >= 0",
            name=op.f("ck_stream_execution_attempt__flow_run_run_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "generation BETWEEN 1 AND 64",
            name=op.f("ck_stream_execution_attempt__generation_bounded"),
        ),
        sa.CheckConstraint(
            "kind IN ('CLAIM', 'TAKEOVER')",
            name=op.f("ck_stream_execution_attempt__kind_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('CLAIMED', 'ACTIVE', 'RECOVERING', 'TERMINAL', 'RELEASED')",
            name=op.f("ck_stream_execution_attempt__state_valid"),
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
            name=op.f("ck_stream_execution_attempt__recovery_shape_valid"),
        ),
        sa.CheckConstraint(
            "(kind = 'CLAIM' AND previous_generation IS NULL "
            "AND previous_holder_id IS NULL AND death_witness IS NULL) OR "
            "(kind = 'TAKEOVER' AND previous_generation IS NOT NULL "
            "AND previous_generation >= 1 AND previous_holder_id IS NOT NULL "
            "AND death_witness IS NOT NULL)",
            name=op.f("ck_stream_execution_attempt__takeover_shape_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_stream_execution_attempt__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_execution_attempt")),
        sa.UniqueConstraint(
            "flow_run_id",
            "generation",
            name=op.f("uq_stream_execution_attempt__flow_run_id_generation"),
        ),
        sa.UniqueConstraint(
            "flow_run_id",
            "operation_id",
            name=op.f("uq_stream_execution_attempt__flow_run_id_operation_id"),
        ),
        sa.UniqueConstraint(
            "flow_run_id",
            "recovery_operation_id",
            name=op.f("uq_stream_execution_attempt__flow_run_id_recovery_operation_id"),
        ),
    )
    op.create_index(
        op.f("ix_stream_execution_attempt__flow_run_id"),
        "stream_execution_attempt",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_execution_attempt__flow_run_id_generation",
        "stream_execution_attempt",
        ["flow_run_id", "generation"],
        unique=False,
    )
    op.create_index(
        op.f("ix_stream_execution_attempt__updated"),
        "stream_execution_attempt",
        ["updated"],
        unique=False,
    )


def downgrade():
    op.drop_table("stream_execution_attempt")
    op.drop_table("stream_execution_authority")
