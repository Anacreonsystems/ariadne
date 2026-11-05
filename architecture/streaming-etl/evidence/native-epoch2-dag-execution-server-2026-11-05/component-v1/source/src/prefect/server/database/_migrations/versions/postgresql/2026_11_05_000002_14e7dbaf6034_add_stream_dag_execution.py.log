"""Add finite DAG lifecycle receipts and durable process launch journals.

Revision ID: 14e7dbaf6034
Revises: f1c5b98d4e12

A launch is prepared before Popen and records its PID before cap-free import.
Launch-to-grant composite keys retain process identity without a reverse cycle.
No legacy record bodies or fixed-P2 grant/bootstrap values are rewritten.
"""

import sqlalchemy as sa
from alembic import op

from prefect.server.utilities.database import JSON, UUID, Timestamp

revision = "14e7dbaf6034"
down_revision = "f1c5b98d4e12"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("stream_physical_subtask") as batch:
        batch.create_unique_constraint(
            op.f("uq_stream_subtask__id_graph"), ["id", "graph_revision_id"]
        )
        batch.create_unique_constraint(
            op.f("uq_stream_subtask__id_graph_task"),
            ["id", "graph_revision_id", "task_run_id"],
        )
    with op.batch_alter_table("stream_subtask_grant") as batch:
        batch.add_column(sa.Column("bootstrap_digest", sa.String(64), nullable=True))
        batch.add_column(sa.Column("bootstrap_witness", JSON, nullable=True))
        batch.add_column(
            sa.Column("bootstrap_ready_operation_id", UUID(), nullable=True)
        )
        batch.add_column(
            sa.Column("bootstrap_ready_request_digest", sa.String(64), nullable=True)
        )
        batch.add_column(
            sa.Column("bootstrap_ready_signature", sa.String(128), nullable=True)
        )
        batch.add_column(sa.Column("bootstrap_ready_at", Timestamp(), nullable=True))
        batch.add_column(sa.Column("bootstrap_ready_receipt", JSON, nullable=True))
        batch.create_unique_constraint(
            op.f("uq_stream_grant__id_subtask_epoch"),
            ["id", "subtask_id", "physical_epoch"],
        )
        batch.create_unique_constraint(
            op.f("uq_stream_grant__recorded_process"),
            [
                "id",
                "subtask_id",
                "physical_epoch",
                "child_process_id",
                "process_start_ticks",
            ],
        )
        batch.create_check_constraint(
            op.f("ck_stream_subtask_grant__bootstrap_shape_valid"),
            "(bootstrap_digest IS NULL AND bootstrap_witness IS NULL) OR (bootstrap_digest IS NOT NULL AND length(bootstrap_digest) = 64 AND bootstrap_witness IS NOT NULL)",
        )
        batch.create_check_constraint(
            op.f("ck_stream_subtask_grant__bootstrap_ready_shape_valid"),
            "(bootstrap_ready_operation_id IS NULL AND bootstrap_ready_request_digest IS NULL AND bootstrap_ready_signature IS NULL AND bootstrap_ready_at IS NULL AND bootstrap_ready_receipt IS NULL) OR (bootstrap_ready_operation_id IS NOT NULL AND bootstrap_ready_request_digest IS NOT NULL AND length(bootstrap_ready_request_digest) = 64 AND bootstrap_ready_signature IS NOT NULL AND length(bootstrap_ready_signature) = 128 AND bootstrap_ready_at IS NOT NULL AND bootstrap_ready_receipt IS NOT NULL AND bootstrap_digest IS NOT NULL AND child_process_id IS NOT NULL)",
        )
    op.execute(
        "CREATE TABLE stream_dag_lifecycle_operation (\n\tgraph_revision_id UUID NOT NULL, \n\tprofile VARCHAR DEFAULT 'partitioned-dag-v1' NOT NULL, \n\tflow_run_id UUID NOT NULL, \n\toperation_id UUID NOT NULL, \n\tkind VARCHAR NOT NULL, \n\tflow_generation INTEGER NOT NULL, \n\tflow_holder_id UUID NOT NULL, \n\tflow_run_run_count INTEGER NOT NULL, \n\trequest_digest VARCHAR(64) NOT NULL, \n\taccepted_receipt JSONB NOT NULL, \n\toutcome_digest VARCHAR(64) NOT NULL, \n\tstate VARCHAR NOT NULL, \n\teffective_control_kind VARCHAR, \n\tcontrol_operation_id UUID, \n\tfirst_cause JSONB, \n\tsubject_subtask_id UUID, \n\tsource_event_id UUID, \n\tsource_task_state_id UUID, \n\ttarget_set_digest VARCHAR(64), \n\tterminal_state VARCHAR, \n\tterminal_outcome JSONB, \n\tcompleted_at TIMESTAMP WITH TIME ZONE, \n\tid UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, \n\tcreated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tCONSTRAINT pk_stream_dag_lifecycle_operation PRIMARY KEY (id), \n\tCONSTRAINT uq_dag_operation__id_graph UNIQUE (id, graph_revision_id), \n\tCONSTRAINT uq_dag_operation__graph_operation UNIQUE (graph_revision_id, operation_id), \n\tCONSTRAINT fk_dag_operation__graph_profile FOREIGN KEY(graph_revision_id, profile) REFERENCES stream_graph_revision (id, profile) ON DELETE CASCADE, \n\tCONSTRAINT fk_dag_operation__graph_flow FOREIGN KEY(graph_revision_id, flow_run_id) REFERENCES stream_graph_revision (id, flow_run_id), \n\tCONSTRAINT fk_dag_operation__control_scope FOREIGN KEY(control_operation_id, graph_revision_id) REFERENCES stream_dag_lifecycle_operation (id, graph_revision_id), \n\tCONSTRAINT fk_dag_operation__subject_scope FOREIGN KEY(subject_subtask_id, graph_revision_id) REFERENCES stream_physical_subtask (id, graph_revision_id), \n\tCONSTRAINT ck_stream_dag_lifecycle_operation__identity_valid CHECK (id = operation_id AND profile = 'partitioned-dag-v1'), \n\tCONSTRAINT ck_stream_dag_lifecycle_operation__kind_valid CHECK (kind IN ('START', 'ACTIVATE', 'ABORT_PREVIEW', 'CANCEL', 'FAIL', 'CONTROL', 'TASK_FAILURE', 'FINALIZE', 'SOURCE_FINISH', 'CONSUMER_FINISH')), \n\tCONSTRAINT ck_stream_dag_lifecycle_operation__attempt_valid CHECK (flow_generation BETWEEN 1 AND 64 AND flow_run_run_count >= 1), \n\tCONSTRAINT ck_stream_dag_lifecycle_operation__digests_valid CHECK (length(request_digest) = 64 AND length(outcome_digest) = 64 AND (target_set_digest IS NULL OR length(target_set_digest) = 64)), \n\tCONSTRAINT ck_stream_dag_lifecycle_operation__state_valid CHECK (state IN ('ACCEPTED', 'STOPPING', 'COMPLETED') AND ((state = 'COMPLETED' AND completed_at IS NOT NULL) OR (state != 'COMPLETED' AND completed_at IS NULL))), \n\tCONSTRAINT ck_stream_dag_lifecycle_operation__control_shape_valid CHECK ((kind = 'CONTROL' AND effective_control_kind IS NOT NULL AND effective_control_kind IN ('PREVIEW_ABORT', 'STARTUP_ABORT', 'FAIL', 'CANCEL') AND first_cause IS NOT NULL AND control_operation_id IS NULL AND target_set_digest IS NOT NULL) OR (kind != 'CONTROL' AND first_cause IS NULL)), \n\tCONSTRAINT ck_stream_dag_lifecycle_operation__terminal_shape_valid CHECK ((kind = 'FINALIZE' AND terminal_state IS NOT NULL AND terminal_state IN ('COMPLETED', 'FAILED', 'CANCELLED') AND terminal_outcome IS NOT NULL AND state = 'COMPLETED') OR (kind != 'FINALIZE' AND terminal_state IS NULL AND terminal_outcome IS NULL)), \n\tCONSTRAINT uq_stream_dag_lifecycle_operation__source_event_id UNIQUE (source_event_id), \n\tCONSTRAINT fk_dag_operation__source_state FOREIGN KEY(source_task_state_id) REFERENCES task_run_state (id)\n)"
    )
    op.execute(
        "CREATE INDEX ix_stream_dag_lifecycle_operation__updated ON stream_dag_lifecycle_operation (updated)"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_dag_operation__one_control ON stream_dag_lifecycle_operation (graph_revision_id) WHERE kind = 'CONTROL'"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_dag_operation__one_finalize ON stream_dag_lifecycle_operation (graph_revision_id) WHERE kind = 'FINALIZE'"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_dag_operation__producer_finish ON stream_dag_lifecycle_operation (subject_subtask_id) WHERE kind IN ('SOURCE_FINISH', 'CONSUMER_FINISH')"
    )
    op.execute(
        "CREATE TABLE stream_dag_launch_attempt (\n\tgraph_revision_id UUID NOT NULL, \n\tsubtask_id UUID NOT NULL, \n\ttask_run_id UUID NOT NULL, \n\tstart_operation_id UUID NOT NULL, \n\tworker_id UUID NOT NULL, \n\twork_pool_id UUID NOT NULL, \n\thost_id UUID NOT NULL, \n\tboot_id UUID NOT NULL, \n\tlauncher_process_id BIGINT NOT NULL, \n\tlauncher_start_ticks BIGINT NOT NULL, \n\tworker_generation_digest VARCHAR(64) NOT NULL, \n\tflow_generation INTEGER NOT NULL, \n\tflow_holder_id UUID NOT NULL, \n\tflow_run_run_count INTEGER NOT NULL, \n\tattempt_index INTEGER NOT NULL, \n\tplanned_physical_epoch INTEGER NOT NULL, \n\texpected_grant_id UUID, \n\texpected_physical_epoch INTEGER NOT NULL, \n\treplacement_operation_id UUID, \n\tgrant_id UUID, \n\tregistered_grant_id UUID, \n\tstate VARCHAR NOT NULL, \n\tprepare_operation_id UUID NOT NULL, \n\tprepare_request_digest VARCHAR(64) NOT NULL, \n\tprepare_receipt JSONB NOT NULL, \n\trecord_operation_id UUID, \n\trecord_request_digest VARCHAR(64), \n\trecord_receipt JSONB, \n\tchild_process_id INTEGER, \n\tprocess_start_ticks BIGINT, \n\tlocal_journal_digest VARCHAR(64), \n\tclose_operation_id UUID, \n\tclose_request_digest VARCHAR(64), \n\tclose_proof_kind VARCHAR, \n\tclose_proof JSONB, \n\tclose_receipt JSONB, \n\tclosed_at TIMESTAMP WITH TIME ZONE, \n\tid UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, \n\tcreated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tCONSTRAINT pk_stream_dag_launch_attempt PRIMARY KEY (id), \n\tCONSTRAINT uq_dag_launch__subtask_index UNIQUE (subtask_id, attempt_index), \n\tCONSTRAINT uq_dag_launch__prepare_operation UNIQUE (subtask_id, prepare_operation_id), \n\tCONSTRAINT uq_dag_launch__planned_epoch UNIQUE (subtask_id, planned_physical_epoch), \n\tCONSTRAINT fk_dag_launch__assignment FOREIGN KEY(subtask_id, graph_revision_id, worker_id) REFERENCES stream_physical_subtask (id, graph_revision_id, assigned_worker_id), \n\tCONSTRAINT fk_dag_launch__task_scope FOREIGN KEY(subtask_id, graph_revision_id, task_run_id) REFERENCES stream_physical_subtask (id, graph_revision_id, task_run_id), \n\tCONSTRAINT fk_dag_launch__start_scope FOREIGN KEY(start_operation_id, graph_revision_id) REFERENCES stream_dag_lifecycle_operation (id, graph_revision_id), \n\tCONSTRAINT fk_dag_launch__expected_grant FOREIGN KEY(expected_grant_id, subtask_id, expected_physical_epoch) REFERENCES stream_subtask_grant (id, subtask_id, physical_epoch), \n\tCONSTRAINT fk_dag_launch__replacement_scope FOREIGN KEY(subtask_id, replacement_operation_id) REFERENCES stream_subtask_replacement (subtask_id, operation_id), \n\tCONSTRAINT fk_dag_launch__grant_scope FOREIGN KEY(grant_id, subtask_id, planned_physical_epoch) REFERENCES stream_subtask_grant (id, subtask_id, physical_epoch), \n\tCONSTRAINT fk_dag_launch__registered_process FOREIGN KEY(registered_grant_id, subtask_id, planned_physical_epoch, child_process_id, process_start_ticks) REFERENCES stream_subtask_grant (id, subtask_id, physical_epoch, child_process_id, process_start_ticks), \n\tCONSTRAINT ck_stream_dag_launch_attempt__attempt_valid CHECK (attempt_index BETWEEN 1 AND 64 AND planned_physical_epoch BETWEEN 1 AND 64 AND expected_physical_epoch BETWEEN 0 AND 63 AND planned_physical_epoch = expected_physical_epoch + 1 AND flow_generation BETWEEN 1 AND 64 AND flow_run_run_count >= 1), \n\tCONSTRAINT ck_stream_dag_launch_attempt__launcher_valid CHECK (launcher_process_id > 0 AND launcher_start_ticks > 0 AND length(worker_generation_digest) = 64 AND length(prepare_request_digest) = 64), \n\tCONSTRAINT ck_stream_dag_launch_attempt__replacement_shape_valid CHECK ((expected_grant_id IS NULL AND expected_physical_epoch = 0 AND replacement_operation_id IS NULL) OR (expected_grant_id IS NOT NULL AND expected_physical_epoch > 0 AND replacement_operation_id IS NOT NULL)), \n\tCONSTRAINT ck_stream_dag_launch_attempt__record_shape_valid CHECK ((child_process_id IS NULL AND process_start_ticks IS NULL AND record_operation_id IS NULL AND record_request_digest IS NULL AND record_receipt IS NULL) OR (child_process_id IS NOT NULL AND process_start_ticks IS NOT NULL AND child_process_id > 0 AND process_start_ticks > 0 AND record_operation_id IS NOT NULL AND record_request_digest IS NOT NULL AND length(record_request_digest) = 64 AND record_receipt IS NOT NULL AND local_journal_digest IS NOT NULL AND length(local_journal_digest) = 64)), \n\tCONSTRAINT ck_stream_dag_launch_attempt__state_valid CHECK ((state = 'PREPARED' AND child_process_id IS NULL AND close_operation_id IS NULL) OR (state = 'PID_RECORDED' AND child_process_id IS NOT NULL AND close_operation_id IS NULL) OR (state = 'CLOSED' AND close_operation_id IS NOT NULL)), \n\tCONSTRAINT ck_stream_dag_launch_attempt__close_shape_valid CHECK ((close_operation_id IS NULL AND close_request_digest IS NULL AND close_proof_kind IS NULL AND close_proof IS NULL AND close_receipt IS NULL AND closed_at IS NULL) OR (close_operation_id IS NOT NULL AND close_request_digest IS NOT NULL AND length(close_request_digest) = 64 AND close_proof_kind IS NOT NULL AND close_proof IS NOT NULL AND close_receipt IS NOT NULL AND closed_at IS NOT NULL AND ((close_proof_kind = 'DEATH' AND child_process_id IS NOT NULL) OR (close_proof_kind = 'CLOSED_UNSTARTED' AND child_process_id IS NULL AND local_journal_digest IS NOT NULL AND length(local_journal_digest) = 64)))), \n\tCONSTRAINT ck_stream_dag_launch_attempt__registration_shape_valid CHECK (registered_grant_id IS NULL OR (registered_grant_id = grant_id AND grant_id IS NOT NULL AND child_process_id IS NOT NULL AND record_operation_id IS NOT NULL AND state != 'PREPARED')), \n\tCONSTRAINT ck_stream_dag_launch_attempt__grant_link_shape_valid CHECK (grant_id IS NULL OR (child_process_id IS NOT NULL AND process_start_ticks IS NOT NULL AND local_journal_digest IS NOT NULL AND record_operation_id IS NOT NULL AND state != 'PREPARED')), \n\tCONSTRAINT fk_stream_dag_launch_attempt__work_pool_id__work_pool FOREIGN KEY(work_pool_id) REFERENCES work_pool (id), \n\tCONSTRAINT uq_stream_dag_launch_attempt__grant_id UNIQUE (grant_id), \n\tCONSTRAINT uq_stream_dag_launch_attempt__registered_grant_id UNIQUE (registered_grant_id)\n)"
    )
    op.execute(
        "CREATE INDEX ix_dag_launch__target_state ON stream_dag_launch_attempt (subtask_id, state)"
    )
    op.execute(
        "CREATE INDEX ix_stream_dag_launch_attempt__updated ON stream_dag_launch_attempt (updated)"
    )
    op.execute(
        "CREATE TABLE stream_dag_control_target (\n\tgraph_revision_id UUID NOT NULL, \n\tcontrol_operation_id UUID NOT NULL, \n\tstart_operation_id UUID NOT NULL, \n\tsubtask_id UUID NOT NULL, \n\ttask_run_id UUID NOT NULL, \n\tworker_id UUID NOT NULL, \n\twork_pool_id UUID NOT NULL, \n\thost_id UUID NOT NULL, \n\tboot_id UUID NOT NULL, \n\tlauncher_process_id BIGINT NOT NULL, \n\tlauncher_start_ticks BIGINT NOT NULL, \n\tworker_generation_digest VARCHAR(64) NOT NULL, \n\tflow_generation INTEGER NOT NULL, \n\tflow_holder_id UUID NOT NULL, \n\tflow_run_run_count INTEGER NOT NULL, \n\tgrant_id UUID, \n\tphysical_epoch INTEGER NOT NULL, \n\tstate VARCHAR NOT NULL, \n\tstop_operation_id UUID, \n\tstop_request_digest VARCHAR(64), \n\tall_attempts_closed_digest VARCHAR(64), \n\tstop_receipt JSONB, \n\tstopped_at TIMESTAMP WITH TIME ZONE, \n\tid UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, \n\tcreated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tupdated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n\tCONSTRAINT pk_stream_dag_control_target PRIMARY KEY (id), \n\tCONSTRAINT uq_dag_target__control_subtask UNIQUE (control_operation_id, subtask_id), \n\tCONSTRAINT fk_dag_target__control_scope FOREIGN KEY(control_operation_id, graph_revision_id) REFERENCES stream_dag_lifecycle_operation (id, graph_revision_id), \n\tCONSTRAINT fk_dag_target__start_scope FOREIGN KEY(start_operation_id, graph_revision_id) REFERENCES stream_dag_lifecycle_operation (id, graph_revision_id), \n\tCONSTRAINT fk_dag_target__assignment FOREIGN KEY(subtask_id, graph_revision_id, worker_id) REFERENCES stream_physical_subtask (id, graph_revision_id, assigned_worker_id), \n\tCONSTRAINT fk_dag_target__task_scope FOREIGN KEY(subtask_id, graph_revision_id, task_run_id) REFERENCES stream_physical_subtask (id, graph_revision_id, task_run_id), \n\tCONSTRAINT fk_dag_target__grant_scope FOREIGN KEY(grant_id, subtask_id, physical_epoch) REFERENCES stream_subtask_grant (id, subtask_id, physical_epoch), \n\tCONSTRAINT ck_stream_dag_control_target__worker_valid CHECK (flow_generation BETWEEN 1 AND 64 AND flow_run_run_count >= 1 AND launcher_process_id > 0 AND launcher_start_ticks > 0 AND length(worker_generation_digest) = 64), \n\tCONSTRAINT ck_stream_dag_control_target__grant_shape_valid CHECK ((grant_id IS NULL AND physical_epoch = 0) OR (grant_id IS NOT NULL AND physical_epoch BETWEEN 1 AND 64)), \n\tCONSTRAINT ck_stream_dag_control_target__stop_shape_valid CHECK ((state = 'PENDING' AND stop_operation_id IS NULL AND stop_request_digest IS NULL AND all_attempts_closed_digest IS NULL AND stop_receipt IS NULL AND stopped_at IS NULL) OR (state = 'STOPPED' AND stop_operation_id IS NOT NULL AND stop_request_digest IS NOT NULL AND length(stop_request_digest) = 64 AND all_attempts_closed_digest IS NOT NULL AND length(all_attempts_closed_digest) = 64 AND stop_receipt IS NOT NULL AND stopped_at IS NOT NULL)), \n\tCONSTRAINT fk_stream_dag_control_target__work_pool_id__work_pool FOREIGN KEY(work_pool_id) REFERENCES work_pool (id)\n)"
    )
    op.execute(
        "CREATE INDEX ix_stream_dag_control_target__updated ON stream_dag_control_target (updated)"
    )
    with op.batch_alter_table("stream_graph_revision") as batch:
        batch.add_column(
            sa.Column("current_control_operation_id", UUID(), nullable=True)
        )
        batch.add_column(sa.Column("terminal_operation_id", UUID(), nullable=True))
        batch.create_foreign_key(
            op.f("fk_stream_graph__dag_control"),
            "stream_dag_lifecycle_operation",
            ["current_control_operation_id", "id"],
            ["id", "graph_revision_id"],
        )
        batch.create_foreign_key(
            op.f("fk_stream_graph__dag_terminal"),
            "stream_dag_lifecycle_operation",
            ["terminal_operation_id", "id"],
            ["id", "graph_revision_id"],
        )
        batch.create_check_constraint(
            op.f("ck_stream_graph_revision__dag_operations_profile_valid"),
            "profile = 'partitioned-dag-v1' OR (current_control_operation_id IS NULL AND terminal_operation_id IS NULL)",
        )


def downgrade():
    queries = [
        "SELECT COUNT(*) FROM stream_dag_lifecycle_operation",
        "SELECT COUNT(*) FROM stream_dag_launch_attempt",
        "SELECT COUNT(*) FROM stream_dag_control_target",
        "SELECT COUNT(*) FROM stream_subtask_grant WHERE bootstrap_digest IS NOT NULL OR bootstrap_ready_operation_id IS NOT NULL",
        "SELECT COUNT(*) FROM stream_graph_revision WHERE current_control_operation_id IS NOT NULL OR terminal_operation_id IS NOT NULL",
    ]
    if any(op.get_bind().execute(sa.text(query)).scalar() for query in queries):
        raise RuntimeError(
            "Cannot downgrade retained DAG execution or process-launch history"
        )
    with op.batch_alter_table("stream_graph_revision") as batch:
        batch.drop_constraint(
            op.f("ck_stream_graph_revision__dag_operations_profile_valid"),
            type_="check",
        )
        batch.drop_constraint(op.f("fk_stream_graph__dag_terminal"), type_="foreignkey")
        batch.drop_constraint(op.f("fk_stream_graph__dag_control"), type_="foreignkey")
        batch.drop_column("terminal_operation_id")
        batch.drop_column("current_control_operation_id")
    op.drop_table("stream_dag_control_target")
    op.drop_table("stream_dag_launch_attempt")
    op.drop_table("stream_dag_lifecycle_operation")
    with op.batch_alter_table("stream_subtask_grant") as batch:
        batch.drop_constraint(
            op.f("ck_stream_subtask_grant__bootstrap_ready_shape_valid"), type_="check"
        )
        batch.drop_constraint(
            op.f("ck_stream_subtask_grant__bootstrap_shape_valid"), type_="check"
        )
        batch.drop_constraint(op.f("uq_stream_grant__recorded_process"), type_="unique")
        batch.drop_constraint(op.f("uq_stream_grant__id_subtask_epoch"), type_="unique")
        batch.drop_column("bootstrap_ready_receipt")
        batch.drop_column("bootstrap_ready_at")
        batch.drop_column("bootstrap_ready_signature")
        batch.drop_column("bootstrap_ready_request_digest")
        batch.drop_column("bootstrap_ready_operation_id")
        batch.drop_column("bootstrap_witness")
        batch.drop_column("bootstrap_digest")
    with op.batch_alter_table("stream_physical_subtask") as batch:
        batch.drop_constraint(op.f("uq_stream_subtask__id_graph_task"), type_="unique")
        batch.drop_constraint(op.f("uq_stream_subtask__id_graph"), type_="unique")
