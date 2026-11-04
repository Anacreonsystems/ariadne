"""Add native deployment snapshots and worker launch identities.

Revision ID: b84c0e7d2f31
Revises: a12d8e4f6c30
Create Date: 2026-11-04 10:00:00.000000
"""

# The DDL is deliberately identical to the PostgreSQL revision. Runtime native
# deployment mutations remain fail-closed on SQLite.

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import prefect

revision = "b84c0e7d2f31"
down_revision = "a12d8e4f6c30"
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
        "native_stream_deployment_snapshot",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "deployment_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("deployment_version", sa.String(), nullable=True),
        sa.Column(
            "work_pool_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("declaration", general_JSON, nullable=False),
        sa.Column("declaration_digest", sa.String(length=64), nullable=False),
        sa.Column("protocol_version", sa.Integer(), nullable=False),
        sa.Column("profile", sa.String(), nullable=False),
        sa.Column("flow_version", sa.String(length=255), nullable=False),
        sa.Column("flow_code_digest", sa.String(length=64), nullable=False),
        sa.Column("resource_profile", sa.String(), nullable=False),
        sa.Column("state", sa.String(), server_default="ACTIVE", nullable=False),
        *_base_columns(),
        sa.CheckConstraint(
            "protocol_version = 1",
            name=op.f("ck_native_stream_deployment_snapshot__protocol_version_valid"),
        ),
        sa.CheckConstraint(
            "profile = 'bounded-linear-v1'",
            name=op.f("ck_native_stream_deployment_snapshot__profile_valid"),
        ),
        sa.CheckConstraint(
            "resource_profile = 'bounded-default-v1'",
            name=op.f("ck_native_stream_deployment_snapshot__resource_profile_valid"),
        ),
        sa.CheckConstraint(
            "length(flow_version) BETWEEN 1 AND 255",
            name=op.f("ck_native_stream_deployment_snapshot__flow_version_valid"),
        ),
        sa.CheckConstraint(
            "length(declaration_digest) = 64 AND length(flow_code_digest) = 64",
            name=op.f("ck_native_stream_deployment_snapshot__digest_lengths_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('ACTIVE', 'RELEASED')",
            name=op.f("ck_native_stream_deployment_snapshot__state_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_native_stream_deployment_snapshot__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "id", name=op.f("pk_native_stream_deployment_snapshot")
        ),
        sa.UniqueConstraint(
            "flow_run_id",
            name=op.f("uq_native_stream_deployment_snapshot__flow_run_id"),
        ),
    )
    op.create_index(
        op.f("ix_native_stream_deployment_snapshot__flow_run_id"),
        "native_stream_deployment_snapshot",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_native_stream_deployment_snapshot__updated"),
        "native_stream_deployment_snapshot",
        ["updated"],
        unique=False,
    )

    op.create_table(
        "native_stream_launch_identity",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "snapshot_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column(
            "enrollment_operation_id",
            prefect.server.utilities.database.UUID(),
            nullable=False,
        ),
        sa.Column("enrollment_request_digest", sa.String(length=64), nullable=False),
        sa.Column("enrollment_outcome_digest", sa.String(length=64), nullable=False),
        sa.Column(
            "work_pool_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "worker_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "launcher_instance_id",
            prefect.server.utilities.database.UUID(),
            nullable=False,
        ),
        sa.Column(
            "launcher_host_id",
            prefect.server.utilities.database.UUID(),
            nullable=False,
        ),
        sa.Column(
            "launcher_boot_id",
            prefect.server.utilities.database.UUID(),
            nullable=False,
        ),
        sa.Column("launcher_process_id", sa.BigInteger(), nullable=False),
        sa.Column("launcher_start_ticks", sa.BigInteger(), nullable=False),
        sa.Column("launch_profile", sa.String(), nullable=False),
        sa.Column(
            "holder_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("holder_capability_digest", sa.String(length=64), nullable=False),
        sa.Column("holder_event_public_key", sa.String(length=64), nullable=False),
        sa.Column(
            "supervisor_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("supervisor_capability_digest", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(), server_default="ENROLLED", nullable=False),
        sa.Column(
            "process_registration_operation_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
        sa.Column(
            "process_registration_request_digest", sa.String(length=64), nullable=True
        ),
        sa.Column(
            "process_registration_outcome_digest", sa.String(length=64), nullable=True
        ),
        sa.Column("host_id", prefect.server.utilities.database.UUID(), nullable=True),
        sa.Column("boot_id", prefect.server.utilities.database.UUID(), nullable=True),
        sa.Column("child_process_id", sa.BigInteger(), nullable=True),
        sa.Column("process_start_ticks", sa.BigInteger(), nullable=True),
        sa.Column("launch_nonce_digest", sa.String(length=64), nullable=True),
        sa.Column(
            "process_registered_at",
            prefect.server.utilities.database.Timestamp(timezone=True),
            nullable=True,
        ),
        *_base_columns(),
        sa.CheckConstraint(
            "generation BETWEEN 1 AND 64",
            name=op.f("ck_native_stream_launch_identity__generation_bounded"),
        ),
        sa.CheckConstraint(
            "launch_profile = 'linux-local-pidfd-v1'",
            name=op.f("ck_native_stream_launch_identity__launch_profile_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('ENROLLED', 'PROCESS_REGISTERED', 'RELEASED')",
            name=op.f("ck_native_stream_launch_identity__state_valid"),
        ),
        sa.CheckConstraint(
            "length(enrollment_request_digest) = 64 "
            "AND length(enrollment_outcome_digest) = 64 "
            "AND length(holder_capability_digest) = 64 "
            "AND length(holder_event_public_key) = 64 "
            "AND length(supervisor_capability_digest) = 64",
            name=op.f("ck_native_stream_launch_identity__identity_lengths_valid"),
        ),
        sa.CheckConstraint(
            "launcher_process_id > 0 AND launcher_start_ticks > 0",
            name=op.f("ck_native_stream_launch_identity__launcher_identity_valid"),
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
            name=op.f("ck_native_stream_launch_identity__process_shape_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_native_stream_launch_identity__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["native_stream_deployment_snapshot.id"],
            name=op.f(
                "fk_native_stream_launch_identity__snapshot_id__native_stream_deployment_snapshot"
            ),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_native_stream_launch_identity")),
        sa.UniqueConstraint(
            "flow_run_id",
            "generation",
            name=op.f("uq_native_stream_launch_identity__flow_run_id_generation"),
        ),
        sa.UniqueConstraint(
            "flow_run_id",
            "enrollment_operation_id",
            name=op.f(
                "uq_native_stream_launch_identity__flow_run_id_enrollment_operation_id"
            ),
        ),
        sa.UniqueConstraint(
            "flow_run_id",
            "process_registration_operation_id",
            name=op.f(
                "uq_native_stream_launch_identity__flow_run_id_process_registration_operation_id"
            ),
        ),
    )
    op.create_index(
        op.f("ix_native_stream_launch_identity__flow_run_id"),
        "native_stream_launch_identity",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_native_stream_launch_identity__updated"),
        "native_stream_launch_identity",
        ["updated"],
        unique=False,
    )


def downgrade():
    op.drop_index(
        op.f("ix_native_stream_launch_identity__updated"),
        table_name="native_stream_launch_identity",
    )
    op.drop_index(
        op.f("ix_native_stream_launch_identity__flow_run_id"),
        table_name="native_stream_launch_identity",
    )
    op.drop_table("native_stream_launch_identity")
    op.drop_index(
        op.f("ix_native_stream_deployment_snapshot__updated"),
        table_name="native_stream_deployment_snapshot",
    )
    op.drop_index(
        op.f("ix_native_stream_deployment_snapshot__flow_run_id"),
        table_name="native_stream_deployment_snapshot",
    )
    op.drop_table("native_stream_deployment_snapshot")
