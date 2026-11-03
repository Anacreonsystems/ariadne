"""Add native stream unsealed-abort receipts.

Revision ID: b86f2d7e0c54
Revises: f64a9d5b8c32
Create Date: 2026-11-03 02:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

import prefect

revision = "b86f2d7e0c54"
down_revision = "f64a9d5b8c32"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("stream_execution_attempt", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "abort_operation_id",
                prefect.server.utilities.database.UUID(),
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column("abort_request_digest", sa.String(length=64), nullable=True)
        )
        batch_op.add_column(
            sa.Column("abort_outcome", sa.JSON(none_as_null=True), nullable=True)
        )
        batch_op.add_column(
            sa.Column("abort_outcome_digest", sa.String(length=64), nullable=True)
        )
        batch_op.create_unique_constraint(
            batch_op.f("uq_stream_execution_attempt__flow_run_id_abort_operation_id"),
            ["flow_run_id", "abort_operation_id"],
        )
        batch_op.create_check_constraint(
            batch_op.f("ck_stream_execution_attempt__abort_shape_valid"),
            "(abort_operation_id IS NULL AND abort_request_digest IS NULL "
            "AND abort_outcome IS NULL AND abort_outcome_digest IS NULL) OR "
            "(abort_operation_id IS NOT NULL "
            "AND abort_request_digest IS NOT NULL "
            "AND length(abort_request_digest) = 64 "
            "AND abort_outcome IS NOT NULL "
            "AND abort_outcome_digest IS NOT NULL "
            "AND length(abort_outcome_digest) = 64)",
        )


def downgrade():
    with op.batch_alter_table("stream_execution_attempt", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f("ck_stream_execution_attempt__abort_shape_valid"),
            type_="check",
        )
        batch_op.drop_constraint(
            batch_op.f("uq_stream_execution_attempt__flow_run_id_abort_operation_id"),
            type_="unique",
        )
        batch_op.drop_column("abort_outcome_digest")
        batch_op.drop_column("abort_outcome")
        batch_op.drop_column("abort_request_digest")
        batch_op.drop_column("abort_operation_id")
