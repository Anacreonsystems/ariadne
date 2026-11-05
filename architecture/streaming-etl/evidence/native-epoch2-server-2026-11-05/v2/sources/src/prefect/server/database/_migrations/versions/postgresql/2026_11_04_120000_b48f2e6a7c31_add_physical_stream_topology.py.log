"""Add fixed-partition graph and physical admission metadata.

Revision ID: b48f2e6a7c31
Revises: c95d1f8e3a42

No data-plane tables are introduced. SQLite has schema parity; execution is PG17
only. Downgrade refuses graph-bound history and physical-profile snapshots.
"""

import sqlalchemy as sa
from alembic import op

revision = "b48f2e6a7c31"
down_revision = "c95d1f8e3a42"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE stream_graph_revision (
	flow_run_id UUID NOT NULL, 
	operation_id UUID NOT NULL, 
	profile VARCHAR NOT NULL, 
	assignment_epoch INTEGER NOT NULL, 
	graph_digest VARCHAR(64) NOT NULL, 
	request_digest VARCHAR(64) NOT NULL, 
	declaration JSONB NOT NULL, 
	state VARCHAR NOT NULL, 
	admission_operation_id UUID, 
	admission_request_digest VARCHAR(64), 
	startup_completed_at TIMESTAMP WITH TIME ZONE, 
	activation_operation_id UUID, 
	activation_request_digest VARCHAR(64), 
	abort_operation_id UUID, 
	abort_request_digest VARCHAR(64), 
	abort_error TEXT, 
	placement_digest VARCHAR(64), 
	id UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	CONSTRAINT pk_stream_graph_revision PRIMARY KEY (id), 
	CONSTRAINT ck_stream_graph_revision__profile_valid CHECK (profile = 'partitioned-fixed-v1' AND assignment_epoch = 1), 
	CONSTRAINT ck_stream_graph_revision__digests_valid CHECK (length(graph_digest) = 64 AND length(request_digest) = 64), 
	CONSTRAINT ck_stream_graph_revision__state_valid CHECK (state IN ('SEALED', 'ADMITTED', 'ABORTING', 'RELEASED')), 
	CONSTRAINT ck_stream_graph_revision__admission_shape_valid CHECK ((admission_operation_id IS NULL AND admission_request_digest IS NULL) OR (admission_operation_id IS NOT NULL AND admission_request_digest IS NOT NULL AND length(admission_request_digest) = 64)), 
	CONSTRAINT ck_stream_graph_revision__activation_shape_valid CHECK ((startup_completed_at IS NULL AND activation_operation_id IS NULL AND activation_request_digest IS NULL) OR (startup_completed_at IS NOT NULL AND activation_operation_id IS NOT NULL AND activation_request_digest IS NOT NULL AND length(activation_request_digest) = 64)), 
	CONSTRAINT ck_stream_graph_revision__abort_shape_valid CHECK ((abort_operation_id IS NULL AND abort_request_digest IS NULL AND abort_error IS NULL) OR (abort_operation_id IS NOT NULL AND abort_request_digest IS NOT NULL AND length(abort_request_digest) = 64 AND abort_error IS NOT NULL AND length(abort_error) BETWEEN 1 AND 4096 AND startup_completed_at IS NULL AND state IN ('ABORTING', 'RELEASED'))), 
	CONSTRAINT ck_stream_graph_revision__lifecycle_shape_valid CHECK ((state = 'SEALED' AND admission_operation_id IS NULL AND startup_completed_at IS NULL AND abort_operation_id IS NULL) OR (state = 'ADMITTED' AND admission_operation_id IS NOT NULL AND abort_operation_id IS NULL) OR (state IN ('ABORTING', 'RELEASED') AND abort_operation_id IS NOT NULL AND startup_completed_at IS NULL) OR (state = 'RELEASED' AND abort_operation_id IS NULL AND startup_completed_at IS NOT NULL AND admission_operation_id IS NOT NULL)), 
	CONSTRAINT ck_stream_graph_revision__placement_digest_valid CHECK (placement_digest IS NULL OR length(placement_digest) = 64), 
	CONSTRAINT uq_stream_graph_revision__flow_run_id UNIQUE (flow_run_id), 
	CONSTRAINT fk_stream_graph_revision__flow_run_id__flow_run FOREIGN KEY(flow_run_id) REFERENCES flow_run (id) ON DELETE CASCADE
)""")
    op.execute(
        "ALTER TABLE stream_submission ADD COLUMN graph_revision_id UUID CONSTRAINT fk_stream_submission__graph_revision_id__stream_graph_revision REFERENCES stream_graph_revision(id)"
    )
    op.drop_index(
        "uq_stream_submission__one_source_per_flow", table_name="stream_submission"
    )
    op.create_index(
        "uq_stream_submission__one_source_per_flow",
        "stream_submission",
        ["flow_run_id"],
        unique=True,
        postgresql_where=sa.text("role = 'SOURCE' AND graph_revision_id IS NULL"),
    )
    op.execute(
        "CREATE INDEX ix_stream_graph_revision__updated ON stream_graph_revision (updated)"
    )
    op.execute("""CREATE TABLE stream_operator_run (
	graph_revision_id UUID NOT NULL, 
	operator_uid VARCHAR(64) NOT NULL, 
	role VARCHAR NOT NULL, 
	parallelism INTEGER NOT NULL, 
	specification JSONB NOT NULL, 
	id UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	CONSTRAINT pk_stream_operator_run PRIMARY KEY (id), 
	CONSTRAINT uq_stream_operator_run__graph_revision_id_operator_uid UNIQUE (graph_revision_id, operator_uid), 
	CONSTRAINT ck_stream_operator_run__fixed_shape_valid CHECK (role IN ('SOURCE', 'OPERATOR') AND parallelism = 2), 
	CONSTRAINT fk_stream_operator_run__graph_revision_id__stream_graph_2909 FOREIGN KEY(graph_revision_id) REFERENCES stream_graph_revision (id) ON DELETE CASCADE
)""")
    op.execute(
        "CREATE INDEX ix_stream_operator_run__updated ON stream_operator_run (updated)"
    )
    op.execute("""CREATE TABLE stream_physical_subtask (
	graph_revision_id UUID NOT NULL, 
	operator_run_id UUID NOT NULL, 
	subtask_index INTEGER NOT NULL, 
	assignment_epoch INTEGER NOT NULL, 
	task_run_id UUID NOT NULL, 
	submission_id UUID NOT NULL, 
	input_stream_id UUID, 
	output_stream_id UUID NOT NULL, 
	current_grant_id UUID, 
	physical_epoch INTEGER DEFAULT '0' NOT NULL, 
	assigned_worker_id UUID, 
	physical_slot_released BOOLEAN DEFAULT false NOT NULL, 
	startup_stop_operation_id UUID, 
	startup_stop_request_digest VARCHAR(64), 
	startup_stop_witness JSONB, 
	startup_stop_unstarted BOOLEAN DEFAULT false NOT NULL, 
	id UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	CONSTRAINT pk_stream_physical_subtask PRIMARY KEY (id), 
	CONSTRAINT uq_stream_physical_subtask__operator_run_id_subtask_index UNIQUE (operator_run_id, subtask_index), 
	CONSTRAINT ck_stream_physical_subtask__assignment_valid CHECK (subtask_index BETWEEN 0 AND 1 AND assignment_epoch = 1), 
	CONSTRAINT ck_stream_physical_subtask__grant_shape_valid CHECK ((physical_epoch = 0 AND current_grant_id IS NULL) OR (physical_epoch BETWEEN 1 AND 64 AND current_grant_id IS NOT NULL)), 
	CONSTRAINT ck_stream_physical_subtask__startup_stop_shape_valid CHECK ((startup_stop_operation_id IS NULL AND startup_stop_request_digest IS NULL AND startup_stop_witness IS NULL AND startup_stop_unstarted = false) OR (startup_stop_operation_id IS NOT NULL AND startup_stop_request_digest IS NOT NULL AND length(startup_stop_request_digest) = 64 AND ((startup_stop_witness IS NOT NULL AND startup_stop_unstarted = false) OR (startup_stop_witness IS NULL AND startup_stop_unstarted = true)))), 
	CONSTRAINT ck_stream_physical_subtask__slot_assignment_valid CHECK (assigned_worker_id IS NOT NULL OR physical_slot_released = false), 
	CONSTRAINT fk_stream_physical_subtask__graph_revision_id__stream_g_316f FOREIGN KEY(graph_revision_id) REFERENCES stream_graph_revision (id) ON DELETE CASCADE, 
	CONSTRAINT fk_stream_physical_subtask__operator_run_id__stream_ope_62d7 FOREIGN KEY(operator_run_id) REFERENCES stream_operator_run (id) ON DELETE CASCADE, 
	CONSTRAINT uq_stream_physical_subtask__task_run_id UNIQUE (task_run_id), 
	CONSTRAINT fk_stream_physical_subtask__task_run_id__task_run FOREIGN KEY(task_run_id) REFERENCES task_run (id), 
	CONSTRAINT uq_stream_physical_subtask__submission_id UNIQUE (submission_id), 
	CONSTRAINT fk_stream_physical_subtask__submission_id__stream_submission FOREIGN KEY(submission_id) REFERENCES stream_submission (id), 
	CONSTRAINT fk_stream_physical_subtask__input_stream_id__stream_partition FOREIGN KEY(input_stream_id) REFERENCES stream_partition (id), 
	CONSTRAINT uq_stream_physical_subtask__output_stream_id UNIQUE (output_stream_id), 
	CONSTRAINT fk_stream_physical_subtask__output_stream_id__stream_partition FOREIGN KEY(output_stream_id) REFERENCES stream_partition (id), 
	CONSTRAINT fk_stream_physical_subtask__assigned_worker_id__worker FOREIGN KEY(assigned_worker_id) REFERENCES worker (id)
)""")
    op.execute(
        "CREATE INDEX ix_stream_physical_subtask__assigned_worker_id ON stream_physical_subtask (assigned_worker_id)"
    )
    op.execute(
        "CREATE INDEX ix_stream_physical_subtask__graph_revision_id ON stream_physical_subtask (graph_revision_id)"
    )
    op.execute(
        "CREATE INDEX ix_stream_physical_subtask__updated ON stream_physical_subtask (updated)"
    )
    op.execute("""CREATE TABLE stream_subtask_grant (
	subtask_id UUID NOT NULL, 
	operation_id UUID NOT NULL, 
	request_digest VARCHAR(64) NOT NULL, 
	physical_epoch INTEGER NOT NULL, 
	holder_id UUID NOT NULL, 
	capability_digest VARCHAR(64) NOT NULL, 
	event_public_key VARCHAR(64) NOT NULL, 
	event_key_id VARCHAR(64) NOT NULL, 
	scope_digest VARCHAR(64) NOT NULL, 
	launcher_binding_kind VARCHAR NOT NULL, 
	flow_generation INTEGER NOT NULL, 
	flow_holder_id UUID NOT NULL, 
	worker_id UUID, 
	work_pool_id UUID, 
	host_id UUID NOT NULL, 
	boot_id UUID NOT NULL, 
	launcher_process_id INTEGER NOT NULL, 
	launcher_start_ticks BIGINT NOT NULL, 
	child_process_id INTEGER, 
	process_start_ticks BIGINT, 
	registration_operation_id UUID, 
	state VARCHAR NOT NULL, 
	last_renewed_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	id UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	CONSTRAINT pk_stream_subtask_grant PRIMARY KEY (id), 
	CONSTRAINT uq_stream_subtask_grant__subtask_id_physical_epoch UNIQUE (subtask_id, physical_epoch), 
	CONSTRAINT uq_stream_subtask_grant__subtask_id_operation_id UNIQUE (subtask_id, operation_id), 
	CONSTRAINT ck_stream_subtask_grant__epoch_bounded CHECK (physical_epoch BETWEEN 1 AND 64), 
	CONSTRAINT ck_stream_subtask_grant__digests_valid CHECK (length(request_digest) = 64 AND length(capability_digest) = 64 AND length(event_public_key) = 64 AND length(event_key_id) = 64 AND length(scope_digest) = 64), 
	CONSTRAINT ck_stream_subtask_grant__state_valid CHECK (state IN ('CLAIMED', 'REGISTERED', 'RETIRED', 'RELEASED')), 
	CONSTRAINT ck_stream_subtask_grant__process_shape_valid CHECK ((child_process_id IS NULL AND process_start_ticks IS NULL AND registration_operation_id IS NULL) OR (child_process_id IS NOT NULL AND process_start_ticks IS NOT NULL AND child_process_id > 0 AND process_start_ticks > 0 AND registration_operation_id IS NOT NULL)), 
	CONSTRAINT ck_stream_subtask_grant__registration_required CHECK (state != 'REGISTERED' OR child_process_id IS NOT NULL), 
	CONSTRAINT ck_stream_subtask_grant__unregistered_claim CHECK (state != 'CLAIMED' OR child_process_id IS NULL), 
	CONSTRAINT ck_stream_subtask_grant__launcher_identity_valid CHECK (launcher_process_id > 0 AND launcher_start_ticks > 0 AND flow_generation BETWEEN 1 AND 64), 
	CONSTRAINT ck_stream_subtask_grant__launcher_binding_valid CHECK ((launcher_binding_kind IN ('DEPLOYMENT', 'ASSIGNED_WORKER') AND worker_id IS NOT NULL AND work_pool_id IS NOT NULL) OR (launcher_binding_kind = 'FLOW_AUTHORITY_LOCAL' AND worker_id IS NULL AND work_pool_id IS NULL)), 
	CONSTRAINT fk_stream_subtask_grant__subtask_id__stream_physical_subtask FOREIGN KEY(subtask_id) REFERENCES stream_physical_subtask (id) ON DELETE CASCADE
)""")
    op.execute(
        "CREATE INDEX ix_stream_subtask_grant__updated ON stream_subtask_grant (updated)"
    )
    op.execute("""CREATE TABLE stream_subtask_replacement (
	subtask_id UUID NOT NULL, 
	operation_id UUID NOT NULL, 
	expected_grant_id UUID NOT NULL, 
	expected_physical_epoch INTEGER NOT NULL, 
	challenge_digest VARCHAR(64) NOT NULL, 
	request_digest VARCHAR(64) NOT NULL, 
	verification_not_before TIMESTAMP WITH TIME ZONE NOT NULL, 
	state VARCHAR NOT NULL, 
	confirm_digest VARCHAR(64), 
	new_grant_id UUID, 
	death_witness JSONB, 
	retirement_outcome JSONB, 
	id UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	CONSTRAINT pk_stream_subtask_replacement PRIMARY KEY (id), 
	CONSTRAINT uq_stream_subtask_replacement__subtask_id_operation_id UNIQUE (subtask_id, operation_id), 
	CONSTRAINT uq_stream_subtask_replacement__expected_grant_id UNIQUE (expected_grant_id), 
	CONSTRAINT ck_stream_subtask_replacement__epoch_bounded CHECK (expected_physical_epoch BETWEEN 1 AND 64), 
	CONSTRAINT ck_stream_subtask_replacement__digests_valid CHECK (length(challenge_digest) = 64 AND length(request_digest) = 64), 
	CONSTRAINT ck_stream_subtask_replacement__confirmation_shape_valid CHECK ((state = 'AUTHORIZED' AND confirm_digest IS NULL AND new_grant_id IS NULL AND death_witness IS NULL AND retirement_outcome IS NULL) OR (state = 'CONFIRMED' AND confirm_digest IS NOT NULL AND length(confirm_digest) = 64 AND new_grant_id IS NOT NULL AND death_witness IS NOT NULL AND retirement_outcome IS NULL) OR (state = 'RETIRED' AND confirm_digest IS NOT NULL AND length(confirm_digest) = 64 AND new_grant_id IS NULL AND death_witness IS NOT NULL AND retirement_outcome IS NOT NULL)), 
	CONSTRAINT fk_stream_subtask_replacement__subtask_id__stream_physi_161f FOREIGN KEY(subtask_id) REFERENCES stream_physical_subtask (id) ON DELETE CASCADE, 
	CONSTRAINT fk_stream_subtask_replacement__expected_grant_id__strea_ada4 FOREIGN KEY(expected_grant_id) REFERENCES stream_subtask_grant (id), 
	CONSTRAINT fk_stream_subtask_replacement__new_grant_id__stream_sub_cee8 FOREIGN KEY(new_grant_id) REFERENCES stream_subtask_grant (id)
)""")
    op.execute(
        "CREATE INDEX ix_stream_subtask_replacement__updated ON stream_subtask_replacement (updated)"
    )
    op.execute("""CREATE TABLE stream_worker_capability (
	worker_id UUID NOT NULL, 
	work_pool_id UUID NOT NULL, 
	protocol VARCHAR NOT NULL, 
	protocol_version INTEGER NOT NULL, 
	host_id UUID NOT NULL, 
	boot_id UUID NOT NULL, 
	launcher_process_id BIGINT NOT NULL, 
	launcher_start_ticks BIGINT NOT NULL, 
	last_renewed_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	id UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	CONSTRAINT pk_stream_worker_capability PRIMARY KEY (id), 
	CONSTRAINT ck_stream_worker_capability__protocol_valid CHECK (protocol = 'partitioned-fixed-v1' AND protocol_version = 1), 
	CONSTRAINT ck_stream_worker_capability__process_identity_valid CHECK (launcher_process_id > 0 AND launcher_start_ticks > 0), 
	CONSTRAINT uq_stream_worker_capability__worker_id UNIQUE (worker_id), 
	CONSTRAINT fk_stream_worker_capability__worker_id__worker FOREIGN KEY(worker_id) REFERENCES worker (id) ON DELETE CASCADE, 
	CONSTRAINT fk_stream_worker_capability__work_pool_id__work_pool FOREIGN KEY(work_pool_id) REFERENCES work_pool (id) ON DELETE CASCADE
)""")
    op.execute(
        "CREATE INDEX ix_stream_worker_capability__updated ON stream_worker_capability (updated)"
    )
    op.execute("""CREATE TABLE stream_subtask_allocation (
	subtask_id UUID NOT NULL, 
	concurrency_limit_id UUID NOT NULL, 
	released BOOLEAN DEFAULT false NOT NULL, 
	id UUID DEFAULT (GEN_RANDOM_UUID()) NOT NULL, 
	created TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	updated TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	CONSTRAINT pk_stream_subtask_allocation PRIMARY KEY (id), 
	CONSTRAINT uq_stream_subtask_allocation__subtask_id_concurrency_limit_id UNIQUE (subtask_id, concurrency_limit_id), 
	CONSTRAINT fk_stream_subtask_allocation__subtask_id__stream_physic_d1d3 FOREIGN KEY(subtask_id) REFERENCES stream_physical_subtask (id) ON DELETE CASCADE, 
	CONSTRAINT fk_stream_subtask_allocation__concurrency_limit_id__con_7c98 FOREIGN KEY(concurrency_limit_id) REFERENCES concurrency_limit_v2 (id) ON DELETE CASCADE
)""")
    op.execute(
        "CREATE INDEX ix_stream_subtask_allocation__updated ON stream_subtask_allocation (updated)"
    )
    op.drop_constraint(
        op.f("ck_native_stream_deployment_snapshot__profile_valid"),
        "native_stream_deployment_snapshot",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_native_stream_deployment_snapshot__profile_valid"),
        "native_stream_deployment_snapshot",
        "profile IN ('bounded-linear-v1', 'partitioned-fixed-v1')",
    )


def downgrade():
    if (
        op.get_bind()
        .execute(sa.text("SELECT COUNT(*) FROM stream_graph_revision"))
        .scalar()
        or op.get_bind()
        .execute(
            sa.text(
                "SELECT COUNT(*) FROM native_stream_deployment_snapshot WHERE profile = 'partitioned-fixed-v1'"
            )
        )
        .scalar()
    ):
        raise RuntimeError(
            "Cannot downgrade graph-bound stream history or physical deployment snapshots"
        )
    op.drop_constraint(
        op.f("ck_native_stream_deployment_snapshot__profile_valid"),
        "native_stream_deployment_snapshot",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_native_stream_deployment_snapshot__profile_valid"),
        "native_stream_deployment_snapshot",
        "profile = 'bounded-linear-v1'",
    )
    op.drop_index(
        "uq_stream_submission__one_source_per_flow", table_name="stream_submission"
    )
    op.create_index(
        "uq_stream_submission__one_source_per_flow",
        "stream_submission",
        ["flow_run_id"],
        unique=True,
        postgresql_where=sa.text("role = 'SOURCE'"),
    )
    op.execute("ALTER TABLE stream_submission DROP COLUMN graph_revision_id")
    op.drop_table("stream_subtask_allocation")
    op.drop_table("stream_worker_capability")
    op.drop_table("stream_subtask_replacement")
    op.drop_table("stream_subtask_grant")
    op.drop_table("stream_physical_subtask")
    op.drop_table("stream_operator_run")
    op.drop_table("stream_graph_revision")
