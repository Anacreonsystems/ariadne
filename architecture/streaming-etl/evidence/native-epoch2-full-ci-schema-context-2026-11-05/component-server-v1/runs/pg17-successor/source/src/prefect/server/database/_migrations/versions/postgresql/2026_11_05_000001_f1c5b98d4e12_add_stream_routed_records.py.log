"""Add direct DAG references, stable subscribers and atomic route receipts.

Revision ID: f1c5b98d4e12
Revises: d8a3f76b2c90

Legacy records remain PAYLOADs with no graph binding. Only the receipt input-
REFERENCE FK is deferred: whole-Flow locked cleanup removes references,
completions, receipts, consumers and payloads before the transaction commits.
All output-payload and reference pins remain immediate RESTRICT.
"""

import sqlalchemy as sa
from alembic import op

from prefect.server.utilities.database import UUID

revision = "f1c5b98d4e12"
down_revision = "d8a3f76b2c90"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("stream_physical_subtask") as batch:
        batch.create_unique_constraint(
            op.f("uq_stream_physical_subtask__route_producer"),
            ["id", "graph_revision_id", "task_run_id", "output_stream_id"],
        )
    with op.batch_alter_table("stream_graph_port") as batch:
        batch.create_unique_constraint(
            op.f("uq_stream_graph_port__id_graph_revision_id_port_uid"),
            ["id", "graph_revision_id", "port_uid"],
        )
    with op.batch_alter_table("stream_graph_edge") as batch:
        batch.create_unique_constraint(
            op.f("uq_stream_graph_edge__subscriber_port"),
            ["id", "graph_revision_id", "subscriber_uid", "consumer_port_id"],
        )
    with op.batch_alter_table("stream_graph_edge_partition") as batch:
        batch.create_unique_constraint(
            op.f("uq_stream_graph_edge_partition__route_stream"),
            ["id", "stream_id", "graph_revision_id"],
        )
        batch.create_unique_constraint(
            op.f("uq_stream_graph_edge_partition__subscriber_target"),
            [
                "id",
                "graph_revision_id",
                "edge_id",
                "target_subtask_id",
                "stream_id",
                "subscriber_id",
            ],
        )
    with op.batch_alter_table("stream_record") as batch:
        batch.add_column(
            sa.Column(
                "record_kind", sa.String(), nullable=False, server_default="PAYLOAD"
            )
        )
        batch.add_column(sa.Column("graph_revision_id", UUID(), nullable=True))
        batch.add_column(sa.Column("flow_run_id", UUID(), nullable=True))
        batch.add_column(sa.Column("payload_record_id", UUID(), nullable=True))
        batch.add_column(sa.Column("payload_record_kind", sa.String(), nullable=True))
        batch.add_column(
            sa.Column("payload_value_digest", sa.String(64), nullable=True)
        )
        batch.add_column(sa.Column("route_receipt_id", UUID(), nullable=True))
        batch.add_column(sa.Column("edge_partition_id", UUID(), nullable=True))
        batch.create_unique_constraint(
            op.f("uq_stream_record__id_graph_kind_value"),
            ["id", "graph_revision_id", "record_kind", "value_digest"],
        )
        batch.create_unique_constraint(
            op.f("uq_stream_record__id_graph_kind_payload"),
            ["id", "graph_revision_id", "record_kind", "payload_record_id"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_record__graph_flow"),
            "stream_graph_revision",
            ["graph_revision_id", "flow_run_id"],
            ["id", "flow_run_id"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_record__partition_flow"),
            "stream_partition",
            ["stream_id", "flow_run_id"],
            ["id", "flow_run_id"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_record__direct_payload"),
            "stream_record",
            [
                "payload_record_id",
                "graph_revision_id",
                "payload_record_kind",
                "payload_value_digest",
            ],
            ["id", "graph_revision_id", "record_kind", "value_digest"],
            ondelete="RESTRICT",
        )
        batch.create_foreign_key(
            op.f("fk_stream_record__edge_partition"),
            "stream_graph_edge_partition",
            ["edge_partition_id", "stream_id", "graph_revision_id"],
            ["id", "stream_id", "graph_revision_id"],
            ondelete="RESTRICT",
        )
        batch.create_check_constraint(
            op.f("ck_stream_record__graph_flow_shape_valid"),
            "(graph_revision_id IS NULL AND flow_run_id IS NULL) OR (graph_revision_id IS NOT NULL AND flow_run_id IS NOT NULL)",
        )
        batch.create_check_constraint(
            op.f("ck_stream_record__record_reference_shape_valid"),
            "(record_kind = 'PAYLOAD' AND payload_record_id IS NULL AND payload_record_kind IS NULL AND payload_value_digest IS NULL AND route_receipt_id IS NULL AND edge_partition_id IS NULL) OR (record_kind = 'REFERENCE' AND graph_revision_id IS NOT NULL AND payload_record_id IS NOT NULL AND payload_record_kind = 'PAYLOAD' AND payload_value_digest IS NOT NULL AND length(payload_value_digest) = 64 AND route_receipt_id IS NOT NULL AND edge_partition_id IS NOT NULL)",
        )
    with op.batch_alter_table("stream_consumer") as batch:
        batch.add_column(
            sa.Column(
                "consumer_kind", sa.String(), nullable=False, server_default="LEGACY"
            )
        )
        batch.add_column(sa.Column("graph_revision_id", UUID(), nullable=True))
        batch.add_column(sa.Column("edge_partition_id", UUID(), nullable=True))
        batch.add_column(sa.Column("edge_id", UUID(), nullable=True))
        batch.add_column(sa.Column("input_port_id", UUID(), nullable=True))
        batch.add_column(sa.Column("subtask_id", UUID(), nullable=True))
        batch.add_column(sa.Column("task_run_id", UUID(), nullable=True))
        batch.add_column(sa.Column("subscriber_uid", sa.String(64), nullable=True))
        batch.add_column(sa.Column("input_port_uid", sa.String(64), nullable=True))
        batch.create_unique_constraint(
            op.f("uq_stream_consumer__edge_partition_id"), ["edge_partition_id"]
        )
        batch.create_unique_constraint(
            op.f("uq_stream_consumer__id_consumer_kind"), ["id", "consumer_kind"]
        )
        batch.create_unique_constraint(
            op.f("uq_stream_consumer__dag_owner"),
            [
                "id",
                "graph_revision_id",
                "subtask_id",
                "task_run_id",
                "input_stream_id",
                "output_stream_id",
            ],
        )
        batch.create_foreign_key(
            op.f("fk_stream_consumer__graph_flow"),
            "stream_graph_revision",
            ["graph_revision_id", "flow_run_id"],
            ["id", "flow_run_id"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_consumer__stable_subscriber"),
            "stream_graph_edge_partition",
            [
                "edge_partition_id",
                "graph_revision_id",
                "edge_id",
                "subtask_id",
                "input_stream_id",
                "id",
            ],
            [
                "id",
                "graph_revision_id",
                "edge_id",
                "target_subtask_id",
                "stream_id",
                "subscriber_id",
            ],
        )
        batch.create_foreign_key(
            op.f("fk_stream_consumer__current_dag_task"),
            "stream_physical_subtask",
            ["subtask_id", "graph_revision_id", "task_run_id", "output_stream_id"],
            ["id", "graph_revision_id", "task_run_id", "output_stream_id"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_consumer__edge_subscriber_port"),
            "stream_graph_edge",
            ["edge_id", "graph_revision_id", "subscriber_uid", "input_port_id"],
            ["id", "graph_revision_id", "subscriber_uid", "consumer_port_id"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_consumer__input_port_uid"),
            "stream_graph_port",
            ["input_port_id", "graph_revision_id", "input_port_uid"],
            ["id", "graph_revision_id", "port_uid"],
        )
        batch.create_check_constraint(
            op.f("ck_stream_consumer__dag_subscriber_shape_valid"),
            "(consumer_kind = 'LEGACY' AND graph_revision_id IS NULL AND edge_partition_id IS NULL AND edge_id IS NULL AND input_port_id IS NULL AND subtask_id IS NULL AND task_run_id IS NULL AND subscriber_uid IS NULL AND input_port_uid IS NULL) OR (consumer_kind = 'DAG' AND graph_revision_id IS NOT NULL AND edge_partition_id IS NOT NULL AND edge_id IS NOT NULL AND input_port_id IS NOT NULL AND subtask_id IS NOT NULL AND task_run_id IS NOT NULL AND subscriber_uid IS NOT NULL AND length(subscriber_uid) BETWEEN 1 AND 64 AND input_port_uid IS NOT NULL AND length(input_port_uid) BETWEEN 1 AND 64)",
        )
    op.execute(
        "CREATE TABLE stream_route_receipt (\n\tgraph_revision_id UUID NOT NULL, \n\tflow_run_id UUID NOT NULL, \n\tproducer_subtask_id UUID NOT NULL, \n\tproducer_task_run_id UUID NOT NULL, \n\tproducer_stream_id UUID NOT NULL, \n\tproducer_position BIGINT NOT NULL, \n\tproducer_sequence BIGINT, \n\tproducer_role VARCHAR NOT NULL, \n\tkind VARCHAR NOT NULL, \n\trouting_digest VARCHAR(64) NOT NULL, \n\tsemantic_digest VARCHAR(64) NOT NULL, \n\toutcome_digest VARCHAR(64) NOT NULL, \n\tvalue_digest VARCHAR(64) NOT NULL, \n\tmanaged_state_digest VARCHAR(64), \n\tkey_present BOOLEAN DEFAULT false NOT NULL, \n\toutput_key_digest VARCHAR(64), \n\toutput_ordinal INTEGER, \n\tpayload_record_id UUID, \n\tpayload_record_kind VARCHAR, \n\tinput_stream_id UUID, \n\tinput_reference_record_id UUID, \n\tinput_reference_record_kind VARCHAR, \n\tinput_payload_record_id UUID, \n\tinput_payload_record_kind VARCHAR, \n\tinput_payload_value_digest VARCHAR(64), \n\tinput_sequence BIGINT, \n\tconsumer_id UUID, \n\toutcome JSONB NOT NULL, \n\toutcome_size INTEGER NOT NULL, \n\tid UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, \n\tcreated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tCONSTRAINT pk_stream_route_receipt PRIMARY KEY (id), \n\tCONSTRAINT uq_stream_route_receipt__producer_stream_id_producer_position UNIQUE (producer_stream_id, producer_position), \n\tCONSTRAINT uq_stream_route_receipt__payload_record_id UNIQUE (payload_record_id), \n\tCONSTRAINT uq_stream_route_receipt__completion_witness UNIQUE (id, consumer_id, input_sequence, kind, semantic_digest), \n\tCONSTRAINT fk_stream_route_receipt__stable_consumer FOREIGN KEY(consumer_id, graph_revision_id, producer_subtask_id, producer_task_run_id, input_stream_id, producer_stream_id) REFERENCES stream_consumer (id, graph_revision_id, subtask_id, task_run_id, input_stream_id, output_stream_id), \n\tCONSTRAINT uq_stream_route_receipt__reference_payload UNIQUE (id, graph_revision_id, payload_record_id), \n\tCONSTRAINT fk_stream_route_receipt__graph_flow FOREIGN KEY(graph_revision_id, flow_run_id) REFERENCES stream_graph_revision (id, flow_run_id), \n\tCONSTRAINT fk_stream_route_receipt__producer FOREIGN KEY(producer_subtask_id, graph_revision_id, producer_task_run_id, producer_stream_id) REFERENCES stream_physical_subtask (id, graph_revision_id, task_run_id, output_stream_id), \n\tCONSTRAINT fk_stream_route_receipt__producer_flow FOREIGN KEY(producer_stream_id, flow_run_id) REFERENCES stream_partition (id, flow_run_id), \n\tCONSTRAINT fk_stream_route_receipt__input_flow FOREIGN KEY(input_stream_id, flow_run_id) REFERENCES stream_partition (id, flow_run_id), \n\tCONSTRAINT fk_stream_route_receipt__output_payload FOREIGN KEY(payload_record_id, graph_revision_id, payload_record_kind, value_digest) REFERENCES stream_record (id, graph_revision_id, record_kind, value_digest) ON DELETE RESTRICT, \n\tCONSTRAINT fk_stream_route_receipt__input_reference FOREIGN KEY(input_reference_record_id, graph_revision_id, input_reference_record_kind, input_payload_record_id) REFERENCES stream_record (id, graph_revision_id, record_kind, payload_record_id) DEFERRABLE INITIALLY DEFERRED, \n\tCONSTRAINT fk_stream_route_receipt__input_payload FOREIGN KEY(input_payload_record_id, graph_revision_id, input_payload_record_kind, input_payload_value_digest) REFERENCES stream_record (id, graph_revision_id, record_kind, value_digest) ON DELETE RESTRICT, \n\tCONSTRAINT ck_stream_route_receipt__position_valid CHECK (producer_position > 0 AND (producer_sequence IS NULL OR producer_sequence > 0)), \n\tCONSTRAINT ck_stream_route_receipt__digests_valid CHECK (length(routing_digest) = 64 AND length(semantic_digest) = 64 AND length(outcome_digest) = 64 AND length(value_digest) = 64 AND (managed_state_digest IS NULL OR length(managed_state_digest) = 64)), \n\tCONSTRAINT ck_stream_route_receipt__key_shape_valid CHECK ((key_present = false AND output_key_digest IS NULL) OR (key_present = true AND output_key_digest IS NOT NULL AND length(output_key_digest) = 64)), \n\tCONSTRAINT ck_stream_route_receipt__terminal_shape_valid CHECK ((kind = 'YIELD' AND producer_sequence IS NOT NULL AND output_ordinal = 0 AND payload_record_id IS NOT NULL AND payload_record_kind = 'PAYLOAD') OR (kind = 'RETURN' AND producer_sequence IS NULL AND output_ordinal IS NULL AND payload_record_id IS NULL AND payload_record_kind IS NULL AND key_present = false)), \n\tCONSTRAINT ck_stream_route_receipt__input_shape_valid CHECK ((producer_role = 'SOURCE' AND kind = 'YIELD' AND input_stream_id IS NULL AND input_reference_record_id IS NULL AND input_reference_record_kind IS NULL AND input_payload_record_id IS NULL AND input_payload_record_kind IS NULL AND input_payload_value_digest IS NULL AND input_sequence IS NULL AND consumer_id IS NULL AND managed_state_digest IS NULL) OR (producer_role = 'OPERATOR' AND input_stream_id IS NOT NULL AND input_reference_record_id IS NOT NULL AND input_reference_record_kind = 'REFERENCE' AND input_payload_record_id IS NOT NULL AND input_payload_record_kind = 'PAYLOAD' AND input_payload_value_digest IS NOT NULL AND length(input_payload_value_digest) = 64 AND input_sequence = producer_position AND consumer_id IS NOT NULL AND managed_state_digest IS NOT NULL)), \n\tCONSTRAINT ck_stream_route_receipt__outcome_size_valid CHECK (outcome_size BETWEEN 1 AND 65536)\n)"
    )
    op.execute(
        "CREATE INDEX ix_stream_route_receipt__updated ON stream_route_receipt (updated)"
    )
    with op.batch_alter_table("stream_record") as batch:
        batch.create_foreign_key(
            op.f("fk_stream_record__route_receipt_payload"),
            "stream_route_receipt",
            ["route_receipt_id", "graph_revision_id", "payload_record_id"],
            ["id", "graph_revision_id", "payload_record_id"],
            ondelete="RESTRICT",
        )
    with op.batch_alter_table("stream_completion") as batch:
        batch.add_column(
            sa.Column(
                "consumer_kind", sa.String(), nullable=False, server_default="LEGACY"
            )
        )
        batch.add_column(sa.Column("route_receipt_id", UUID(), nullable=True))
        batch.create_foreign_key(
            op.f("fk_stream_completion__consumer_kind"),
            "stream_consumer",
            ["consumer_id", "consumer_kind"],
            ["id", "consumer_kind"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_completion__route_receipt"),
            "stream_route_receipt",
            [
                "route_receipt_id",
                "consumer_id",
                "input_sequence",
                "kind",
                "intent_digest",
            ],
            ["id", "consumer_id", "input_sequence", "kind", "semantic_digest"],
            ondelete="RESTRICT",
        )
        batch.create_unique_constraint(
            op.f("uq_stream_completion__route_receipt_id"), ["route_receipt_id"]
        )
        batch.create_check_constraint(
            op.f("ck_stream_completion__route_receipt_shape_valid"),
            "(consumer_kind = 'LEGACY' AND route_receipt_id IS NULL) OR (consumer_kind = 'DAG' AND route_receipt_id IS NOT NULL)",
        )


def downgrade():
    if any(
        op.get_bind().execute(sa.text(query)).scalar()
        for query in [
            "SELECT COUNT(*) FROM stream_route_receipt",
            "SELECT COUNT(*) FROM stream_record WHERE record_kind != 'PAYLOAD' OR graph_revision_id IS NOT NULL",
            "SELECT COUNT(*) FROM stream_consumer WHERE graph_revision_id IS NOT NULL",
            "SELECT COUNT(*) FROM stream_completion WHERE consumer_kind != 'LEGACY' OR route_receipt_id IS NOT NULL",
        ]
    ):
        raise RuntimeError(
            "Cannot downgrade durable DAG routed record, receipt or subscriber history"
        )
    with op.batch_alter_table("stream_completion") as batch:
        batch.drop_constraint(
            op.f("ck_stream_completion__route_receipt_shape_valid"), type_="check"
        )
        batch.drop_constraint(
            op.f("uq_stream_completion__route_receipt_id"), type_="unique"
        )
        batch.drop_constraint(
            op.f("fk_stream_completion__route_receipt"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("fk_stream_completion__consumer_kind"), type_="foreignkey"
        )
        batch.drop_column("route_receipt_id")
        batch.drop_column("consumer_kind")
    with op.batch_alter_table("stream_record") as batch:
        batch.drop_constraint(
            op.f("fk_stream_record__route_receipt_payload"), type_="foreignkey"
        )
    op.drop_table("stream_route_receipt")
    with op.batch_alter_table("stream_consumer") as batch:
        batch.drop_constraint(
            op.f("ck_stream_consumer__dag_subscriber_shape_valid"), type_="check"
        )
        batch.drop_constraint(
            op.f("fk_stream_consumer__graph_flow"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("fk_stream_consumer__stable_subscriber"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("fk_stream_consumer__current_dag_task"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("fk_stream_consumer__edge_subscriber_port"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("fk_stream_consumer__input_port_uid"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("uq_stream_consumer__edge_partition_id"), type_="unique"
        )
        batch.drop_constraint(
            op.f("uq_stream_consumer__id_consumer_kind"), type_="unique"
        )
        batch.drop_constraint(op.f("uq_stream_consumer__dag_owner"), type_="unique")
        batch.drop_column("input_port_uid")
        batch.drop_column("subscriber_uid")
        batch.drop_column("task_run_id")
        batch.drop_column("subtask_id")
        batch.drop_column("input_port_id")
        batch.drop_column("edge_id")
        batch.drop_column("edge_partition_id")
        batch.drop_column("graph_revision_id")
        batch.drop_column("consumer_kind")
    with op.batch_alter_table("stream_record") as batch:
        batch.drop_constraint(
            op.f("ck_stream_record__graph_flow_shape_valid"), type_="check"
        )
        batch.drop_constraint(
            op.f("ck_stream_record__record_reference_shape_valid"), type_="check"
        )
        batch.drop_constraint(op.f("fk_stream_record__graph_flow"), type_="foreignkey")
        batch.drop_constraint(
            op.f("fk_stream_record__partition_flow"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("fk_stream_record__direct_payload"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("fk_stream_record__edge_partition"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f("uq_stream_record__id_graph_kind_value"), type_="unique"
        )
        batch.drop_constraint(
            op.f("uq_stream_record__id_graph_kind_payload"), type_="unique"
        )
        batch.drop_column("edge_partition_id")
        batch.drop_column("route_receipt_id")
        batch.drop_column("payload_value_digest")
        batch.drop_column("payload_record_kind")
        batch.drop_column("payload_record_id")
        batch.drop_column("flow_run_id")
        batch.drop_column("graph_revision_id")
        batch.drop_column("record_kind")
    with op.batch_alter_table("stream_graph_edge_partition") as batch:
        batch.drop_constraint(
            op.f("uq_stream_graph_edge_partition__subscriber_target"), type_="unique"
        )
        batch.drop_constraint(
            op.f("uq_stream_graph_edge_partition__route_stream"), type_="unique"
        )
    with op.batch_alter_table("stream_graph_edge") as batch:
        batch.drop_constraint(
            op.f("uq_stream_graph_edge__subscriber_port"), type_="unique"
        )
    with op.batch_alter_table("stream_graph_port") as batch:
        batch.drop_constraint(
            op.f("uq_stream_graph_port__id_graph_revision_id_port_uid"), type_="unique"
        )
    with op.batch_alter_table("stream_physical_subtask") as batch:
        batch.drop_constraint(
            op.f("uq_stream_physical_subtask__route_producer"), type_="unique"
        )
