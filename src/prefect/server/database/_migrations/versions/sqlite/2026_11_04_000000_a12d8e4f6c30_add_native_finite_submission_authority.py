"""Add bounded finite Task authority for mixed native executions.

Revision ID: a12d8e4f6c30
Revises: b86f2d7e0c54
Create Date: 2026-11-04 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import prefect

revision = "a12d8e4f6c30"
down_revision = "b86f2d7e0c54"
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
        "stream_finite_submission",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("phase", sa.String(), nullable=False),
        sa.Column("submission_key", sa.String(length=128), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("manifest_digest", sa.String(length=64), nullable=False),
        sa.Column("canonical_size", sa.Integer(), nullable=False),
        sa.Column("parameters", stream_value_JSON, nullable=False),
        sa.Column("parameters_digest", sa.String(length=64), nullable=False),
        sa.Column(
            "task_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("task_name", sa.String(), nullable=False),
        sa.Column("task_key", sa.String(), nullable=False),
        sa.Column("task_version", sa.String(), nullable=False),
        sa.Column("flow_version", sa.String(), nullable=False),
        sa.Column(
            "deployment_id", prefect.server.utilities.database.UUID(), nullable=True
        ),
        sa.Column("deployment_version", sa.String(), nullable=True),
        sa.Column("artifact_digest", sa.String(length=64), nullable=False),
        sa.Column("dynamic_key", sa.String(), nullable=False),
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
            "phase IN ('PREFIX', 'TAIL')",
            name=op.f("ck_stream_finite_submission__phase_valid"),
        ),
        sa.CheckConstraint(
            "length(submission_key) BETWEEN 1 AND 128",
            name=op.f("ck_stream_finite_submission__submission_key_length_valid"),
        ),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(manifest_digest) = 64 "
            "AND length(parameters_digest) = 64 AND length(artifact_digest) = 64",
            name=op.f("ck_stream_finite_submission__digest_lengths_valid"),
        ),
        sa.CheckConstraint(
            "canonical_size > 0",
            name=op.f("ck_stream_finite_submission__canonical_size_positive"),
        ),
        sa.CheckConstraint(
            "state IN ('ACTIVE', 'RELEASED')",
            name=op.f("ck_stream_finite_submission__state_valid"),
        ),
        sa.CheckConstraint(
            "(state = 'ACTIVE' AND released_at IS NULL) OR "
            "(state = 'RELEASED' AND released_at IS NOT NULL)",
            name=op.f("ck_stream_finite_submission__release_shape_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_stream_finite_submission__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id"],
            ["task_run.id"],
            name=op.f("fk_stream_finite_submission__task_run_id__task_run"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_finite_submission")),
        sa.UniqueConstraint(
            "flow_run_id",
            "phase",
            name=op.f("uq_stream_finite_submission__flow_run_id_phase"),
        ),
        sa.UniqueConstraint(
            "flow_run_id",
            "submission_key",
            name=op.f("uq_stream_finite_submission__flow_run_id_submission_key"),
        ),
        sa.UniqueConstraint(
            "task_run_id", name=op.f("uq_stream_finite_submission__task_run_id")
        ),
    )
    op.create_index(
        op.f("ix_stream_finite_submission__flow_run_id"),
        "stream_finite_submission",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_finite_submission__flow_run_id_state",
        "stream_finite_submission",
        ["flow_run_id", "state"],
        unique=False,
    )
    op.create_index(
        op.f("ix_stream_finite_submission__updated"),
        "stream_finite_submission",
        ["updated"],
        unique=False,
    )

    op.create_table(
        "stream_finite_receipt",
        sa.Column(
            "finite_submission_id",
            prefect.server.utilities.database.UUID(),
            nullable=False,
        ),
        sa.Column(
            "task_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "operation_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("outcome_digest", sa.String(length=64), nullable=False),
        sa.Column("run_count", sa.Integer(), nullable=False),
        sa.Column("result", stream_value_JSON, nullable=False),
        sa.Column("result_digest", sa.String(length=64), nullable=False),
        sa.Column("result_size", sa.Integer(), nullable=False),
        *_base_columns(),
        sa.CheckConstraint(
            "run_count >= 1",
            name=op.f("ck_stream_finite_receipt__run_count_positive"),
        ),
        sa.CheckConstraint(
            "result_size > 0",
            name=op.f("ck_stream_finite_receipt__result_size_positive"),
        ),
        sa.CheckConstraint(
            "length(request_digest) = 64 AND length(outcome_digest) = 64 "
            "AND length(result_digest) = 64",
            name=op.f("ck_stream_finite_receipt__digest_lengths_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["finite_submission_id"],
            ["stream_finite_submission.id"],
            name=op.f(
                "fk_stream_finite_receipt__finite_submission_id__stream_finite_submission"
            ),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id"],
            ["task_run.id"],
            name=op.f("fk_stream_finite_receipt__task_run_id__task_run"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_finite_receipt")),
        sa.UniqueConstraint(
            "finite_submission_id",
            name=op.f("uq_stream_finite_receipt__finite_submission_id"),
        ),
        sa.UniqueConstraint(
            "task_run_id", name=op.f("uq_stream_finite_receipt__task_run_id")
        ),
        sa.UniqueConstraint(
            "operation_id", name=op.f("uq_stream_finite_receipt__operation_id")
        ),
    )
    op.create_index(
        op.f("ix_stream_finite_receipt__updated"),
        "stream_finite_receipt",
        ["updated"],
        unique=False,
    )

    with op.batch_alter_table("stream_execution_authority", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "finite_prefix_manifest_digest", sa.String(length=64), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "finite_prefix_task_run_id",
                prefect.server.utilities.database.UUID(),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "finite_prefix_sealed_at",
                prefect.server.utilities.database.Timestamp(timezone=True),
                nullable=True,
            )
        )
        batch_op.create_check_constraint(
            batch_op.f(
                "ck_stream_execution_authority__finite_prefix_anchor_shape_valid"
            ),
            "(finite_prefix_manifest_digest IS NULL "
            "AND finite_prefix_task_run_id IS NULL "
            "AND finite_prefix_sealed_at IS NULL) OR "
            "(finite_prefix_manifest_digest IS NOT NULL "
            "AND length(finite_prefix_manifest_digest) = 64 "
            "AND finite_prefix_task_run_id IS NOT NULL "
            "AND finite_prefix_sealed_at IS NOT NULL)",
        )


def downgrade():
    with op.batch_alter_table("stream_execution_authority", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f(
                "ck_stream_execution_authority__finite_prefix_anchor_shape_valid"
            ),
            type_="check",
        )
        batch_op.drop_column("finite_prefix_sealed_at")
        batch_op.drop_column("finite_prefix_task_run_id")
        batch_op.drop_column("finite_prefix_manifest_digest")
    op.drop_table("stream_finite_receipt")
    op.drop_table("stream_finite_submission")
