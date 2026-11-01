"""Add native stream storage tables.

Revision ID: 9b6d4e1c8a20
Revises: a3b9c7d2e5f1
Create Date: 2026-11-01 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

import prefect

# revision identifiers, used by Alembic.
revision = "9b6d4e1c8a20"
down_revision = "a3b9c7d2e5f1"
branch_labels = None
depends_on = None

stream_value_JSON = postgresql.JSON(none_as_null=False).with_variant(
    sa.JSON(none_as_null=False), "sqlite"
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
        "stream_partition",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("protocol_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("state", sa.String(), server_default="OPEN", nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("tail_sequence", sa.BigInteger(), nullable=True),
        sa.Column("last_sequence", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column(
            "producer_position", sa.BigInteger(), server_default="0", nullable=False
        ),
        sa.Column("producer_kind", sa.String(), nullable=True),
        sa.Column(
            "producer_task_run_id",
            prefect.server.utilities.database.UUID(),
            nullable=True,
        ),
        sa.Column("owner_id", prefect.server.utilities.database.UUID(), nullable=True),
        sa.Column("attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("owner_epoch", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "limits",
            prefect.server.utilities.database.JSON(astext_type=sa.Text()),
            server_default="{}",
            nullable=False,
        ),
        sa.Column("record_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("byte_count", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("released", sa.Boolean(), server_default="0", nullable=False),
        *_base_columns(),
        sa.CheckConstraint(
            "attempt >= 0 AND owner_epoch >= 0",
            name=op.f("ck_stream_partition__authority_nonnegative"),
        ),
        sa.CheckConstraint(
            "(owner_id IS NULL AND attempt = 0 AND owner_epoch = 0) OR "
            "(owner_id IS NOT NULL AND attempt >= 1 AND owner_epoch >= 1)",
            name=op.f("ck_stream_partition__authority_shape_valid"),
        ),
        sa.CheckConstraint(
            "producer_kind IS NULL OR producer_kind IN ('SOURCE', 'CONSUMER')",
            name=op.f("ck_stream_partition__producer_kind_valid"),
        ),
        sa.CheckConstraint(
            "protocol_version = 1",
            name=op.f("ck_stream_partition__protocol_version_supported"),
        ),
        sa.CheckConstraint(
            "released = false OR state IN ('COMPLETED', 'FAILED')",
            name=op.f("ck_stream_partition__release_requires_terminal"),
        ),
        sa.CheckConstraint(
            "last_sequence >= 0 AND producer_position >= last_sequence",
            name=op.f("ck_stream_partition__sequence_progress_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('OPEN', 'COMPLETED', 'FAILED')",
            name=op.f("ck_stream_partition__state_valid"),
        ),
        sa.CheckConstraint(
            "(state = 'OPEN' AND tail_sequence IS NULL AND error IS NULL) OR "
            "(state = 'COMPLETED' AND tail_sequence = last_sequence "
            "AND error IS NULL) OR "
            "(state = 'FAILED' AND tail_sequence = last_sequence "
            "AND error IS NOT NULL)",
            name=op.f("ck_stream_partition__terminal_shape_valid"),
        ),
        sa.CheckConstraint(
            "record_count >= 0 AND byte_count >= 0",
            name=op.f("ck_stream_partition__usage_nonnegative"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_stream_partition__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_partition")),
        sa.UniqueConstraint(
            "id",
            "flow_run_id",
            name=op.f("uq_stream_partition__id_flow_run_id"),
        ),
    )
    op.create_index(
        op.f("ix_stream_partition__flow_run_id"),
        "stream_partition",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_partition__flow_run_id_state",
        "stream_partition",
        ["flow_run_id", "state"],
        unique=False,
    )
    op.create_index(
        op.f("ix_stream_partition__updated"),
        "stream_partition",
        ["updated"],
        unique=False,
    )

    op.create_table(
        "stream_record",
        sa.Column(
            "stream_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("producer_position", sa.BigInteger(), nullable=False),
        sa.Column(
            "value",
            stream_value_JSON,
            nullable=False,
        ),
        sa.Column("value_digest", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        *_base_columns(),
        sa.CheckConstraint(
            "length(value_digest) = 64",
            name=op.f("ck_stream_record__digest_length_valid"),
        ),
        sa.CheckConstraint(
            "producer_position > 0",
            name=op.f("ck_stream_record__producer_position_positive"),
        ),
        sa.CheckConstraint(
            "sequence > 0", name=op.f("ck_stream_record__sequence_positive")
        ),
        sa.CheckConstraint(
            "size_bytes > 0", name=op.f("ck_stream_record__size_positive")
        ),
        sa.ForeignKeyConstraint(
            ["stream_id"],
            ["stream_partition.id"],
            name=op.f("fk_stream_record__stream_id__stream_partition"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_record")),
        sa.UniqueConstraint(
            "stream_id",
            "producer_position",
            name=op.f("uq_stream_record__stream_id_producer_position"),
        ),
        sa.UniqueConstraint(
            "stream_id",
            "sequence",
            name=op.f("uq_stream_record__stream_id_sequence"),
        ),
    )
    op.create_index(
        op.f("ix_stream_record__updated"),
        "stream_record",
        ["updated"],
        unique=False,
    )

    op.create_table(
        "stream_consumer",
        sa.Column(
            "flow_run_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "input_stream_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column(
            "output_stream_id",
            prefect.server.utilities.database.UUID(),
            nullable=False,
        ),
        sa.Column("owner_id", prefect.server.utilities.database.UUID(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("owner_epoch", sa.Integer(), nullable=False),
        sa.Column(
            "completed_through", sa.BigInteger(), server_default="0", nullable=False
        ),
        sa.Column(
            "managed_state",
            stream_value_JSON,
            server_default="{}",
            nullable=False,
        ),
        sa.Column("inflight_sequence", sa.BigInteger(), nullable=True),
        sa.Column(
            "delivery_id", prefect.server.utilities.database.UUID(), nullable=True
        ),
        *_base_columns(),
        sa.CheckConstraint(
            "attempt >= 1", name=op.f("ck_stream_consumer__attempt_positive")
        ),
        sa.CheckConstraint(
            "completed_through >= 0",
            name=op.f("ck_stream_consumer__completed_through_nonnegative"),
        ),
        sa.CheckConstraint(
            "(inflight_sequence IS NULL AND delivery_id IS NULL) OR "
            "(inflight_sequence = completed_through + 1 AND delivery_id IS NOT NULL)",
            name=op.f("ck_stream_consumer__delivery_shape_valid"),
        ),
        sa.CheckConstraint(
            "input_stream_id != output_stream_id",
            name=op.f("ck_stream_consumer__distinct_streams"),
        ),
        sa.CheckConstraint(
            "owner_epoch >= 1",
            name=op.f("ck_stream_consumer__owner_epoch_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["flow_run_id"],
            ["flow_run.id"],
            name=op.f("fk_stream_consumer__flow_run_id__flow_run"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["input_stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
            name=op.f(
                "fk_stream_consumer__input_stream_id_flow_run_id__stream_partition"
            ),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["output_stream_id", "flow_run_id"],
            ["stream_partition.id", "stream_partition.flow_run_id"],
            name=op.f(
                "fk_stream_consumer__output_stream_id_flow_run_id__stream_partition"
            ),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_consumer")),
        sa.UniqueConstraint(
            "delivery_id", name=op.f("uq_stream_consumer__delivery_id")
        ),
        sa.UniqueConstraint(
            "output_stream_id", name=op.f("uq_stream_consumer__output_stream_id")
        ),
    )
    op.create_index(
        op.f("ix_stream_consumer__flow_run_id"),
        "stream_consumer",
        ["flow_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_stream_consumer__input_stream_id",
        "stream_consumer",
        ["input_stream_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_stream_consumer__updated"),
        "stream_consumer",
        ["updated"],
        unique=False,
    )

    op.create_table(
        "stream_completion",
        sa.Column(
            "consumer_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("input_sequence", sa.BigInteger(), nullable=False),
        sa.Column(
            "delivery_id", prefect.server.utilities.database.UUID(), nullable=False
        ),
        sa.Column("owner_epoch", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("intent_digest", sa.String(length=64), nullable=False),
        sa.Column("output_sequence", sa.BigInteger(), nullable=True),
        *_base_columns(),
        sa.CheckConstraint(
            "input_sequence > 0",
            name=op.f("ck_stream_completion__input_sequence_positive"),
        ),
        sa.CheckConstraint(
            "length(intent_digest) = 64",
            name=op.f("ck_stream_completion__intent_digest_length_valid"),
        ),
        sa.CheckConstraint(
            "kind IN ('YIELD', 'RETURN')",
            name=op.f("ck_stream_completion__kind_valid"),
        ),
        sa.CheckConstraint(
            "(kind = 'YIELD' AND output_sequence > 0) OR "
            "(kind = 'RETURN' AND output_sequence IS NULL)",
            name=op.f("ck_stream_completion__output_shape_valid"),
        ),
        sa.CheckConstraint(
            "owner_epoch >= 1",
            name=op.f("ck_stream_completion__owner_epoch_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["consumer_id"],
            ["stream_consumer.id"],
            name=op.f("fk_stream_completion__consumer_id__stream_consumer"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_completion")),
        sa.UniqueConstraint(
            "consumer_id",
            "delivery_id",
            name=op.f("uq_stream_completion__consumer_id_delivery_id"),
        ),
        sa.UniqueConstraint(
            "consumer_id",
            "input_sequence",
            name=op.f("uq_stream_completion__consumer_id_input_sequence"),
        ),
        sa.UniqueConstraint(
            "consumer_id",
            "output_sequence",
            name=op.f("uq_stream_completion__consumer_id_output_sequence"),
        ),
    )
    op.create_index(
        op.f("ix_stream_completion__updated"),
        "stream_completion",
        ["updated"],
        unique=False,
    )


def downgrade():
    op.drop_table("stream_completion")
    op.drop_table("stream_consumer")
    op.drop_table("stream_record")
    op.drop_table("stream_partition")
