"""Add native stream unsealed-abort receipts.

Revision ID: a75e1c6d9b43
Revises: d53e8c4f7a21
Create Date: 2026-11-03 02:00:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import prefect

revision = "a75e1c6d9b43"
down_revision = "d53e8c4f7a21"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "stream_execution_attempt",
        sa.Column(
            "abort_operation_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
    )
    op.add_column(
        "stream_execution_attempt",
        sa.Column("abort_request_digest", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "stream_execution_attempt",
        sa.Column("abort_outcome", postgresql.JSONB(none_as_null=True), nullable=True),
    )
    op.add_column(
        "stream_execution_attempt",
        sa.Column("abort_outcome_digest", sa.String(length=64), nullable=True),
    )
    op.create_unique_constraint(
        op.f("uq_stream_execution_attempt__flow_run_id_abort_operation_id"),
        "stream_execution_attempt",
        ["flow_run_id", "abort_operation_id"],
    )
    op.create_check_constraint(
        op.f("ck_stream_execution_attempt__abort_shape_valid"),
        "stream_execution_attempt",
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
    op.drop_constraint(
        op.f("ck_stream_execution_attempt__abort_shape_valid"),
        "stream_execution_attempt",
        type_="check",
    )
    op.drop_constraint(
        op.f("uq_stream_execution_attempt__flow_run_id_abort_operation_id"),
        "stream_execution_attempt",
        type_="unique",
    )
    op.drop_column("stream_execution_attempt", "abort_outcome_digest")
    op.drop_column("stream_execution_attempt", "abort_outcome")
    op.drop_column("stream_execution_attempt", "abort_request_digest")
    op.drop_column("stream_execution_attempt", "abort_operation_id")
