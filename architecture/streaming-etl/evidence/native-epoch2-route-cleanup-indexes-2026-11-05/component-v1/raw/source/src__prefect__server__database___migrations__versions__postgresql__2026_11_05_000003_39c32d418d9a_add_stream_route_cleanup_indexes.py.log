"""Index child foreign keys used by atomic routed-history cleanup.

Revision ID: 39c32d418d9a
Revises: 14e7dbaf6034

Only lookup indexes change; record pins, deferred checks and cleanup ordering
retain their existing semantics.
"""

from alembic import op

revision = "39c32d418d9a"
down_revision = "14e7dbaf6034"
branch_labels = None
depends_on = None

INDEXES = (
    ("ix_stream_record__payload_record_id", "stream_record", "payload_record_id"),
    ("ix_stream_record__route_receipt_id", "stream_record", "route_receipt_id"),
    (
        "ix_stream_route_receipt__input_payload_record_id",
        "stream_route_receipt",
        "input_payload_record_id",
    ),
    (
        "ix_stream_route_receipt__input_reference_record_id",
        "stream_route_receipt",
        "input_reference_record_id",
    ),
)


def upgrade():
    for name, table, column in INDEXES:
        op.create_index(name, table, [column])


def downgrade():
    for name, table, _ in reversed(INDEXES):
        op.drop_index(name, table_name=table)
