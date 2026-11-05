"""Add immutable DAG topology and physical region reservation metadata.

Revision ID: e9b4a87c3d01
Revises: e59a3f7b8d42

Existing fixed-P2 metadata and all stream data remain in place. No record
routing or execution support is enabled by this migration.
"""

import sqlalchemy as sa
from alembic import op

revision = "e9b4a87c3d01"
down_revision = "e59a3f7b8d42"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("stream_graph_revision") as batch:
        batch.drop_constraint(
            op.f("ck_stream_graph_revision__profile_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_graph_revision__profile_valid"),
            "profile IN ('partitioned-fixed-v1', 'partitioned-dag-v1') AND assignment_epoch = 1",
        )
        batch.create_unique_constraint(
            op.f("uq_stream_graph_revision__id_profile"), ["id", "profile"]
        )
        batch.create_unique_constraint(
            op.f("uq_stream_graph_revision__id_flow_run_id"), ["id", "flow_run_id"]
        )
        batch.create_unique_constraint(
            op.f("uq_stream_graph_revision__id_admission_operation_id_profile"),
            ["id", "admission_operation_id", "profile"],
        )
    with op.batch_alter_table("stream_operator_run") as batch:
        batch.add_column(
            sa.Column(
                "profile",
                sa.String(),
                nullable=False,
                server_default="partitioned-fixed-v1",
            )
        )
        batch.drop_constraint(
            op.f("fk_stream_operator_run__graph_revision_id__stream_graph_revision"),
            type_="foreignkey",
        )
        batch.create_foreign_key(
            op.f(
                "fk_stream_operator_run__graph_revision_id_profile__stream_graph_revision"
            ),
            "stream_graph_revision",
            ["graph_revision_id", "profile"],
            ["id", "profile"],
            ondelete="CASCADE",
        )
        batch.create_unique_constraint(
            op.f("uq_stream_operator_run__id_graph_revision_id_profile"),
            ["id", "graph_revision_id", "profile"],
        )
        batch.drop_constraint(
            op.f("ck_stream_operator_run__fixed_shape_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_operator_run__fixed_shape_valid"),
            "role IN ('SOURCE', 'OPERATOR') AND ((profile = 'partitioned-fixed-v1' AND parallelism = 2) OR (profile = 'partitioned-dag-v1' AND parallelism BETWEEN 1 AND 16))",
        )
    with op.batch_alter_table("stream_physical_subtask") as batch:
        batch.add_column(
            sa.Column(
                "profile",
                sa.String(),
                nullable=False,
                server_default="partitioned-fixed-v1",
            )
        )
        batch.drop_constraint(
            op.f(
                "fk_stream_physical_subtask__graph_revision_id__stream_graph_revision"
            ),
            type_="foreignkey",
        )
        batch.drop_constraint(
            op.f("fk_stream_physical_subtask__operator_run_id__stream_operator_run"),
            type_="foreignkey",
        )
        batch.create_foreign_key(
            op.f(
                "fk_stream_physical_subtask__graph_revision_id_profile__stream_graph_revision"
            ),
            "stream_graph_revision",
            ["graph_revision_id", "profile"],
            ["id", "profile"],
            ondelete="CASCADE",
        )
        batch.create_foreign_key(
            op.f(
                "fk_stream_physical_subtask__operator_run_id_graph_revision_id_profile__stream_operator_run"
            ),
            "stream_operator_run",
            ["operator_run_id", "graph_revision_id", "profile"],
            ["id", "graph_revision_id", "profile"],
            ondelete="CASCADE",
        )
        batch.create_unique_constraint(
            op.f(
                "uq_stream_physical_subtask__id_graph_revision_id_profile_subtask_index"
            ),
            ["id", "graph_revision_id", "profile", "subtask_index"],
        )
        batch.create_unique_constraint(
            op.f("uq_stream_physical_subtask__id_graph_revision_id_assigned_worker_id"),
            ["id", "graph_revision_id", "assigned_worker_id"],
        )
        batch.drop_constraint(
            op.f("ck_stream_physical_subtask__assignment_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_physical_subtask__assignment_valid"),
            "assignment_epoch = 1 AND ((profile = 'partitioned-fixed-v1' AND subtask_index BETWEEN 0 AND 1) OR (profile = 'partitioned-dag-v1' AND subtask_index BETWEEN 0 AND 15))",
        )
    with op.batch_alter_table("stream_worker_capability") as batch:
        batch.drop_constraint(
            op.f("uq_stream_worker_capability__worker_id"), type_="unique"
        )
        batch.create_unique_constraint(
            op.f("uq_stream_worker_capability__worker_id_protocol_protocol_version"),
            ["worker_id", "protocol", "protocol_version"],
        )
        batch.drop_constraint(
            op.f("ck_stream_worker_capability__protocol_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_worker_capability__protocol_valid"),
            "protocol IN ('partitioned-fixed-v1', 'partitioned-dag-v1') AND protocol_version = 1",
        )
    with op.batch_alter_table("native_stream_deployment_snapshot") as batch:
        batch.drop_constraint(
            op.f("ck_native_stream_deployment_snapshot__profile_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_native_stream_deployment_snapshot__profile_valid"),
            "profile IN ('bounded-linear-v1', 'partitioned-fixed-v1', 'partitioned-dag-v1')",
        )
    op.execute(
        "CREATE TABLE stream_graph_port (\n\tgraph_revision_id CHAR(36) NOT NULL, \n\toperator_run_id CHAR(36) NOT NULL, \n\tprofile VARCHAR DEFAULT 'partitioned-dag-v1' NOT NULL, \n\tport_uid VARCHAR(64) NOT NULL, \n\tdirection VARCHAR NOT NULL, \n\tschema_id VARCHAR NOT NULL, \n\tschema_version INTEGER NOT NULL, \n\tschema_digest VARCHAR(64) NOT NULL, \n\tid CHAR(36) DEFAULT \n    (\n        lower(hex(randomblob(4)))\n        || '-'\n        || lower(hex(randomblob(2)))\n        || '-4'\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || substr('89ab',abs(random()) % 4 + 1, 1)\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || lower(hex(randomblob(6)))\n    )\n     NOT NULL, \n\tcreated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tupdated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tCONSTRAINT pk_stream_graph_port PRIMARY KEY (id), \n\tCONSTRAINT fk_stream_graph_port__operator_run_id_graph_revision_id_profile__stream_operator_run FOREIGN KEY(operator_run_id, graph_revision_id, profile) REFERENCES stream_operator_run (id, graph_revision_id, profile) ON DELETE CASCADE, \n\tCONSTRAINT uq_stream_graph_port__operator_run_id_direction UNIQUE (operator_run_id, direction), \n\tCONSTRAINT uq_stream_graph_port__operator_run_id_port_uid UNIQUE (operator_run_id, port_uid), \n\tCONSTRAINT uq_stream_graph_port__id_graph_revision_id_direction UNIQUE (id, graph_revision_id, direction), \n\tCONSTRAINT ck_stream_graph_port__port_shape_valid CHECK (profile = 'partitioned-dag-v1' AND direction IN ('INPUT', 'OUTPUT') AND length(port_uid) BETWEEN 1 AND 64), \n\tCONSTRAINT ck_stream_graph_port__schema_identity_valid CHECK (schema_id = 'json-value-v1' AND schema_version = 1 AND schema_digest = 'd93f54187397ae62d4e1b98c56edab2a67a9d10422b028dfdea87ba6616a849c')\n)"
    )
    op.execute(
        "CREATE INDEX ix_stream_graph_port__updated ON stream_graph_port (updated)"
    )
    op.execute(
        "CREATE TABLE stream_graph_edge (\n\tgraph_revision_id CHAR(36) NOT NULL, \n\tprofile VARCHAR DEFAULT 'partitioned-dag-v1' NOT NULL, \n\tedge_uid VARCHAR(64) NOT NULL, \n\tsubscriber_uid VARCHAR(64) NOT NULL, \n\tproducer_port_id CHAR(36) NOT NULL, \n\tproducer_direction VARCHAR DEFAULT 'OUTPUT' NOT NULL, \n\tconsumer_port_id CHAR(36) NOT NULL, \n\tconsumer_direction VARCHAR DEFAULT 'INPUT' NOT NULL, \n\tproducer_parallelism INTEGER NOT NULL, \n\ttarget_parallelism INTEGER NOT NULL, \n\texchange VARCHAR NOT NULL, \n\trouting_version INTEGER NOT NULL, \n\tkey_codec VARCHAR, \n\thash_algorithm VARCHAR, \n\tkey_group_count INTEGER, \n\tspecification JSON NOT NULL, \n\tid CHAR(36) DEFAULT \n    (\n        lower(hex(randomblob(4)))\n        || '-'\n        || lower(hex(randomblob(2)))\n        || '-4'\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || substr('89ab',abs(random()) % 4 + 1, 1)\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || lower(hex(randomblob(6)))\n    )\n     NOT NULL, \n\tcreated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tupdated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tCONSTRAINT pk_stream_graph_edge PRIMARY KEY (id), \n\tCONSTRAINT fk_stream_graph_edge__graph_revision_id_profile__stream_graph_revision FOREIGN KEY(graph_revision_id, profile) REFERENCES stream_graph_revision (id, profile) ON DELETE CASCADE, \n\tCONSTRAINT fk_stream_graph_edge__producer_port_id_graph_revision_id_producer_direction__stream_graph_port FOREIGN KEY(producer_port_id, graph_revision_id, producer_direction) REFERENCES stream_graph_port (id, graph_revision_id, direction) ON DELETE CASCADE, \n\tCONSTRAINT fk_stream_graph_edge__consumer_port_id_graph_revision_id_consumer_direction__stream_graph_port FOREIGN KEY(consumer_port_id, graph_revision_id, consumer_direction) REFERENCES stream_graph_port (id, graph_revision_id, direction) ON DELETE CASCADE, \n\tCONSTRAINT uq_stream_graph_edge__graph_revision_id_edge_uid UNIQUE (graph_revision_id, edge_uid), \n\tCONSTRAINT uq_stream_graph_edge__graph_revision_id_subscriber_uid UNIQUE (graph_revision_id, subscriber_uid), \n\tCONSTRAINT uq_stream_graph_edge__consumer_port_id UNIQUE (consumer_port_id), \n\tCONSTRAINT uq_stream_graph_edge__id_graph_revision_id UNIQUE (id, graph_revision_id), \n\tCONSTRAINT ck_stream_graph_edge__edge_shape_valid CHECK (profile = 'partitioned-dag-v1' AND producer_direction = 'OUTPUT' AND consumer_direction = 'INPUT' AND producer_port_id != consumer_port_id), \n\tCONSTRAINT ck_stream_graph_edge__identifiers_valid CHECK (length(edge_uid) BETWEEN 1 AND 64 AND length(subscriber_uid) BETWEEN 1 AND 64), \n\tCONSTRAINT ck_stream_graph_edge__exchange_valid CHECK (producer_parallelism BETWEEN 1 AND 16 AND target_parallelism BETWEEN 1 AND 16 AND exchange IN ('forward', 'rebalance', 'broadcast', 'keyed') AND routing_version = 1), \n\tCONSTRAINT ck_stream_graph_edge__forward_shape_valid CHECK (exchange != 'forward' OR producer_parallelism = target_parallelism), \n\tCONSTRAINT ck_stream_graph_edge__key_shape_valid CHECK ((exchange = 'keyed' AND key_codec = 'tagged-key-v1' AND hash_algorithm = 'sha256' AND key_group_count IS NOT NULL AND key_group_count BETWEEN target_parallelism AND 2147483647) OR (exchange != 'keyed' AND key_codec IS NULL AND hash_algorithm IS NULL AND key_group_count IS NULL))\n)"
    )
    op.execute(
        "CREATE INDEX ix_stream_graph_edge__updated ON stream_graph_edge (updated)"
    )
    op.execute(
        "CREATE TABLE stream_graph_edge_partition (\n\tgraph_revision_id CHAR(36) NOT NULL, \n\tflow_run_id CHAR(36) NOT NULL, \n\tprofile VARCHAR DEFAULT 'partitioned-dag-v1' NOT NULL, \n\tedge_id CHAR(36) NOT NULL, \n\tpartition_index INTEGER NOT NULL, \n\tstream_id CHAR(36) NOT NULL, \n\tsubscriber_id CHAR(36) NOT NULL, \n\ttarget_subtask_id CHAR(36) NOT NULL, \n\tid CHAR(36) DEFAULT \n    (\n        lower(hex(randomblob(4)))\n        || '-'\n        || lower(hex(randomblob(2)))\n        || '-4'\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || substr('89ab',abs(random()) % 4 + 1, 1)\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || lower(hex(randomblob(6)))\n    )\n     NOT NULL, \n\tcreated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tupdated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tCONSTRAINT pk_stream_graph_edge_partition PRIMARY KEY (id), \n\tCONSTRAINT fk_stream_graph_edge_partition__graph_revision_id_flow_run_id__stream_graph_revision FOREIGN KEY(graph_revision_id, flow_run_id) REFERENCES stream_graph_revision (id, flow_run_id) ON DELETE CASCADE, \n\tCONSTRAINT fk_stream_graph_edge_partition__edge_id_graph_revision_id__stream_graph_edge FOREIGN KEY(edge_id, graph_revision_id) REFERENCES stream_graph_edge (id, graph_revision_id) ON DELETE CASCADE, \n\tCONSTRAINT fk_stream_graph_edge_partition__stream_id_flow_run_id__stream_partition FOREIGN KEY(stream_id, flow_run_id) REFERENCES stream_partition (id, flow_run_id), \n\tCONSTRAINT fk_stream_graph_edge_partition__target_subtask_id_graph_revision_id_profile_partition_index__stream_physical_subtask FOREIGN KEY(target_subtask_id, graph_revision_id, profile, partition_index) REFERENCES stream_physical_subtask (id, graph_revision_id, profile, subtask_index), \n\tCONSTRAINT uq_stream_graph_edge_partition__edge_id_partition_index UNIQUE (edge_id, partition_index), \n\tCONSTRAINT uq_stream_graph_edge_partition__stream_id UNIQUE (stream_id), \n\tCONSTRAINT uq_stream_graph_edge_partition__subscriber_id UNIQUE (subscriber_id), \n\tCONSTRAINT uq_stream_graph_edge_partition__target_subtask_id UNIQUE (target_subtask_id), \n\tCONSTRAINT ck_stream_graph_edge_partition__partition_shape_valid CHECK (profile = 'partitioned-dag-v1' AND partition_index BETWEEN 0 AND 15)\n)"
    )
    op.execute(
        "CREATE INDEX ix_stream_graph_edge_partition__updated ON stream_graph_edge_partition (updated)"
    )
    op.execute(
        "CREATE TABLE stream_physical_slot_allocation (\n\tgraph_revision_id CHAR(36) NOT NULL, \n\tsubtask_id CHAR(36) NOT NULL, \n\tworker_id CHAR(36) NOT NULL, \n\tprofile VARCHAR DEFAULT 'partitioned-dag-v1' NOT NULL, \n\tadmission_operation_id CHAR(36) NOT NULL, \n\treleased BOOLEAN DEFAULT 0 NOT NULL, \n\trelease_operation_id CHAR(36), \n\treleased_at DATETIME, \n\tid CHAR(36) DEFAULT \n    (\n        lower(hex(randomblob(4)))\n        || '-'\n        || lower(hex(randomblob(2)))\n        || '-4'\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || substr('89ab',abs(random()) % 4 + 1, 1)\n        || substr(lower(hex(randomblob(2))),2)\n        || '-'\n        || lower(hex(randomblob(6)))\n    )\n     NOT NULL, \n\tcreated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tupdated DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f000', 'now')) NOT NULL, \n\tCONSTRAINT pk_stream_physical_slot_allocation PRIMARY KEY (id), \n\tCONSTRAINT fk_stream_physical_slot_allocation__graph_revision_id_admission_operation_id_profile__stream_graph_revision FOREIGN KEY(graph_revision_id, admission_operation_id, profile) REFERENCES stream_graph_revision (id, admission_operation_id, profile), \n\tCONSTRAINT fk_stream_physical_slot_allocation__subtask_id_graph_revision_id_worker_id__stream_physical_subtask FOREIGN KEY(subtask_id, graph_revision_id, worker_id) REFERENCES stream_physical_subtask (id, graph_revision_id, assigned_worker_id), \n\tCONSTRAINT uq_stream_physical_slot_allocation__subtask_id UNIQUE (subtask_id), \n\tCONSTRAINT ck_stream_physical_slot_allocation__profile_valid CHECK (profile = 'partitioned-dag-v1'), \n\tCONSTRAINT ck_stream_physical_slot_allocation__release_shape_valid CHECK ((released = false AND release_operation_id IS NULL AND released_at IS NULL) OR (released = true AND release_operation_id IS NOT NULL AND released_at IS NOT NULL)), \n\tCONSTRAINT fk_stream_physical_slot_allocation__worker_id__worker FOREIGN KEY(worker_id) REFERENCES worker (id)\n)"
    )
    op.execute(
        "CREATE INDEX ix_stream_physical_slot_allocation__updated ON stream_physical_slot_allocation (updated)"
    )
    op.execute(
        "CREATE INDEX ix_stream_physical_slot_allocation__worker_id ON stream_physical_slot_allocation (worker_id)"
    )


def downgrade():
    if any(
        op.get_bind().execute(sa.text(query)).scalar()
        for query in [
            "SELECT COUNT(*) FROM stream_graph_revision WHERE profile = 'partitioned-dag-v1'",
            "SELECT COUNT(*) FROM native_stream_deployment_snapshot WHERE profile = 'partitioned-dag-v1'",
            "SELECT COUNT(*) FROM stream_graph_port",
            "SELECT COUNT(*) FROM stream_graph_edge",
            "SELECT COUNT(*) FROM stream_graph_edge_partition",
            "SELECT COUNT(*) FROM stream_physical_slot_allocation",
        ]
    ):
        raise RuntimeError(
            "Cannot downgrade durable DAG topology history or deployment snapshots"
        )
    op.drop_table("stream_physical_slot_allocation")
    op.drop_table("stream_graph_edge_partition")
    op.drop_table("stream_graph_edge")
    op.drop_table("stream_graph_port")
    op.execute(
        "DELETE FROM stream_worker_capability WHERE protocol = 'partitioned-dag-v1'"
    )
    with op.batch_alter_table("native_stream_deployment_snapshot") as batch:
        batch.drop_constraint(
            op.f("ck_native_stream_deployment_snapshot__profile_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_native_stream_deployment_snapshot__profile_valid"),
            "profile IN ('bounded-linear-v1', 'partitioned-fixed-v1')",
        )
    with op.batch_alter_table("stream_worker_capability") as batch:
        batch.drop_constraint(
            op.f("uq_stream_worker_capability__worker_id_protocol_protocol_version"),
            type_="unique",
        )
        batch.create_unique_constraint(
            op.f("uq_stream_worker_capability__worker_id"), ["worker_id"]
        )
        batch.drop_constraint(
            op.f("ck_stream_worker_capability__protocol_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_worker_capability__protocol_valid"),
            "protocol = 'partitioned-fixed-v1' AND protocol_version = 1",
        )
    with op.batch_alter_table("stream_physical_subtask") as batch:
        batch.drop_constraint(
            op.f(
                "fk_stream_physical_subtask__graph_revision_id_profile__stream_graph_revision"
            ),
            type_="foreignkey",
        )
        batch.drop_constraint(
            op.f(
                "fk_stream_physical_subtask__operator_run_id_graph_revision_id_profile__stream_operator_run"
            ),
            type_="foreignkey",
        )
        batch.drop_constraint(
            op.f(
                "uq_stream_physical_subtask__id_graph_revision_id_profile_subtask_index"
            ),
            type_="unique",
        )
        batch.drop_constraint(
            op.f("uq_stream_physical_subtask__id_graph_revision_id_assigned_worker_id"),
            type_="unique",
        )
        batch.drop_constraint(
            op.f("ck_stream_physical_subtask__assignment_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_physical_subtask__assignment_valid"),
            "subtask_index BETWEEN 0 AND 1 AND assignment_epoch = 1",
        )
        batch.create_foreign_key(
            op.f(
                "fk_stream_physical_subtask__graph_revision_id__stream_graph_revision"
            ),
            "stream_graph_revision",
            ["graph_revision_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch.create_foreign_key(
            op.f("fk_stream_physical_subtask__operator_run_id__stream_operator_run"),
            "stream_operator_run",
            ["operator_run_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch.drop_column("profile")
    with op.batch_alter_table("stream_operator_run") as batch:
        batch.drop_constraint(
            op.f(
                "fk_stream_operator_run__graph_revision_id_profile__stream_graph_revision"
            ),
            type_="foreignkey",
        )
        batch.drop_constraint(
            op.f("uq_stream_operator_run__id_graph_revision_id_profile"), type_="unique"
        )
        batch.drop_constraint(
            op.f("ck_stream_operator_run__fixed_shape_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_operator_run__fixed_shape_valid"),
            "role IN ('SOURCE', 'OPERATOR') AND parallelism = 2",
        )
        batch.create_foreign_key(
            op.f("fk_stream_operator_run__graph_revision_id__stream_graph_revision"),
            "stream_graph_revision",
            ["graph_revision_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch.drop_column("profile")
    with op.batch_alter_table("stream_graph_revision") as batch:
        batch.drop_constraint(
            op.f("uq_stream_graph_revision__id_profile"), type_="unique"
        )
        batch.drop_constraint(
            op.f("uq_stream_graph_revision__id_flow_run_id"), type_="unique"
        )
        batch.drop_constraint(
            op.f("uq_stream_graph_revision__id_admission_operation_id_profile"),
            type_="unique",
        )
        batch.drop_constraint(
            op.f("ck_stream_graph_revision__profile_valid"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_stream_graph_revision__profile_valid"),
            "profile = 'partitioned-fixed-v1' AND assignment_epoch = 1",
        )
