"""Add immutable native stream submission manifests.

Revision ID: c31f8a2d6e40
Revises: 7f3c2a9e4b11
Create Date: 2026-11-03 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import prefect

revision = "c31f8a2d6e40"
down_revision = "7f3c2a9e4b11"
branch_labels = None
depends_on = None

stream_value_JSON = postgresql.JSON(none_as_null=False).with_variant(
    sa.JSON(none_as_null=False), "sqlite"
)
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
        "stream_submission",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("submission_key", sa.String(length=128), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("manifest_digest", sa.String(length=64), nullable=False),
        sa.Column("canonical_size", sa.Integer(), nullable=False),
        sa.Column("parameters", stream_value_JSON, nullable=False),
        sa.Column("parameters_digest", sa.String(length=64), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("protocol", sa.String(), nullable=False),
        sa.Column("input_key", sa.String(length=255), nullable=True),
        sa.Column(
            "upstream_stream_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
        sa.Column(
            "task_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "stream_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("task_name", sa.String(), nullable=False),
        sa.Column("task_key", sa.String(), nullable=False),
        sa.Column("task_version", sa.String(), nullable=False),
        sa.Column("flow_version", sa.String(), nullable=False),
        sa.Column(
            "deployment_id", prefect.server.utilities.database.UUID(), nullable=True
        ),
        sa.Column("deployment_version", sa.String(), nullable=True),
        sa.Column("artifact_digest", sa.String(length=64), nullable=True),
        sa.Column("dynamic_key", sa.String(), nullable=False),
        sa.Column("limits", general_JSON, nullable=False),
        sa.Column("task_policy", general_JSON, nullable=False),
        sa.Column("tags", general_JSON, nullable=False),
        sa.Column("labels", general_JSON, nullable=False),
        sa.Column("state", sa.String(), server_default="ACTIVE", nullable=False),
        sa.Column(
            "released_at",
            prefect.server.utilities.database.Timestamp(timezone=True),
            nullable=True,
        ),
        *_base_columns(),
        sa.CheckConstraint(
            "canonical_size > 0",
            name=op.f("ck_stream_submission__canonical_size_positive"),
        ),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(manifest_digest) = 64 "
            "AND length(parameters_digest) = 64",
            name=op.f("ck_stream_submission__digest_lengths_valid"),
        ),
        sa.CheckConstraint(
            "(state = 'ACTIVE' AND released_at IS NULL) OR "
            "(state = 'RELEASED' AND released_at IS NOT NULL)",
            name=op.f("ck_stream_submission__release_shape_valid"),
        ),
        sa.CheckConstraint(
            "role IN ('SOURCE', 'OPERATOR')",
            name=op.f("ck_stream_submission__role_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('ACTIVE', 'RELEASED')",
            name=op.f("ck_stream_submission__state_valid"),
        ),
        sa.CheckConstraint(
            "length(submission_key) BETWEEN 1 AND 128",
            name=op.f("ck_stream_submission__submission_key_length_valid"),
        ),
        sa.CheckConstraint(
            "(role = 'SOURCE' AND protocol = 'source-v1' "
            "AND input_key IS NULL AND upstream_stream_id IS NULL) OR "
            "(role = 'OPERATOR' AND protocol = 'yield-v2' "
            "AND input_key IS NOT NULL AND upstream_stream_id IS NOT NULL)",
            name=op.f("ck_stream_submission__topology_shape_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_stream_submission__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["stream_id"],
            ["stream_partition.id"],
            name=op.f("fk_stream_submission__stream_id__stream_partition"),
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id"],
            ["task_run.id"],
            name=op.f("fk_stream_submission__task_run_id__task_run"),
        ),
        sa.ForeignKeyConstraint(
            ["upstream_stream_id"],
            ["stream_partition.id"],
            name=op.f("fk_stream_submission__upstream_stream_id__stream_partition"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_submission")),
        sa.UniqueConstraint(
            "flow_run_id",
            "submission_key",
            name=op.f("uq_stream_submission__flow_run_id_submission_key"),
        ),
        sa.UniqueConstraint("stream_id", name=op.f("uq_stream_submission__stream_id")),
        sa.UniqueConstraint(
            "task_run_id", name=op.f("uq_stream_submission__task_run_id")
        ),
        sa.UniqueConstraint(
            "upstream_stream_id",
            name=op.f("uq_stream_submission__upstream_stream_id"),
        ),
    )
    op.create_index(
        op.f("ix_stream_submission__flow_run_id"),
        "stream_submission",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_submission__flow_run_id_state",
        "stream_submission",
        ["flow_run_id", "state"],
        unique=False,
    )
    op.create_index(
        op.f("ix_stream_submission__updated"),
        "stream_submission",
        ["updated"],
        unique=False,
    )
    op.create_index(
        "uq_stream_submission__one_source_per_flow",
        "stream_submission",
        ["flow_run_id"],
        unique=True,
        postgresql_where=sa.text("role = 'SOURCE'"),
    )


def downgrade():
    op.drop_table("stream_submission")
