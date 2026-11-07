from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, AsyncGenerator, NamedTuple, NoReturn, Optional
from uuid import UUID

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import prefect.types._datetime
from prefect._internal.stream_event_auth import (
    canonical_event_witness,
    canonical_physical_event_witness,
    verify_task_event,
)
from prefect.logging import get_logger
from prefect.server.database import (
    PrefectDBInterface,
    db_injector,
    provide_database_interface,
)
from prefect.server.events.schemas.events import Event, ReceivedEvent
from prefect.server.models import stream_dag_executions as dag_lifecycle
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models._stream_receipts import (
    canonical_json_digest,
    finite_receipt_outcome_digest,
)
from prefect.server.models.stream_subtask_authority import (
    release_terminal_allocation,
    require_subtask,
)
from prefect.server.schemas.core import TaskRun, TaskRunPolicy
from prefect.server.schemas.states import TERMINAL_STATES, State
from prefect.server.schemas.stream_subtasks import (
    StreamSubtaskCredentials,
    StreamSubtaskEventResult,
)
from prefect.server.services.base import RunInEphemeralServers, Service
from prefect.server.utilities.database import get_max_query_parameters
from prefect.server.utilities.messaging import (
    Consumer,
    Message,
    MessageHandler,
    create_consumer,
)
from prefect.server.utilities.messaging._consumer_names import (
    generate_unique_consumer_name,
)
from prefect.server.utilities.messaging.memory import log_metrics_periodically
from prefect.settings.context import get_current_settings
from prefect.settings.models.server.services import ServicesBaseSetting

if TYPE_CHECKING:
    import logging

    TaskRunUpsertKey = tuple[str, UUID] | tuple[str, UUID, str, str]
    TaskRunTargetValue = UUID | tuple[UUID, str, str]

logger: "logging.Logger" = get_logger(__name__)

DEFAULT_PERSIST_MAX_RETRIES = 5

_NATIVE_RESERVED_TASK_PAYLOAD_KEYS = frozenset(
    {
        "id",
        "created",
        "updated",
        "flow_run_id",
        "state_id",
        "state_type",
        "state_name",
        "state",
    }
)
_NATIVE_RESERVED_STATE_PAYLOAD_KEYS = frozenset({"id", "timestamp"})


def _is_canonical_dag_running_event(event: Event) -> bool:
    """Select a lock mode only; the signed event still needs full authorization."""
    state = event.payload.get("validated_state")
    task = event.payload.get("task_run")
    if (
        event.event != "prefect.task-run.Running"
        or not isinstance(state, dict)
        or state.get("type") != "RUNNING"
        or state.get("name") != "Running"
        or not isinstance(task, dict)
        or _NATIVE_RESERVED_STATE_PAYLOAD_KEYS.intersection(state)
        or _NATIVE_RESERVED_TASK_PAYLOAD_KEYS.intersection(task)
        or type(task.get("run_count")) is not int
        or task["run_count"] < 1
    ):
        return False
    try:
        # Reject malformed model payloads before selecting the shared prefix.
        task_run_from_event(event.receive())
    except (KeyError, TypeError, ValueError):
        return False
    return True


async def dispatch_stream_subtask_event(
    session: AsyncSession,
    db: PrefectDBInterface,
    subtask_id: UUID,
    event: Event,
    credentials: StreamSubtaskCredentials,
) -> StreamSubtaskEventResult:
    """Share the DAG prefix for one canonical RUNNING event; fence other states."""
    running_child: dag_lifecycle.DAGChildScope | None = None
    profile = await session.scalar(
        sa.select(db.StreamGraphRevision.profile)
        .join(
            db.StreamPhysicalSubtask,
            db.StreamPhysicalSubtask.graph_revision_id == db.StreamGraphRevision.id,
        )
        .where(db.StreamPhysicalSubtask.id == subtask_id)
    )
    if profile == "partitioned-dag-v1":
        running = _is_canonical_dag_running_event(event)
        child = await dag_lifecycle.lock_dag_child(
            session,
            db,
            credentials,
            subtask_id=subtask_id,
            graph_update=not running,
            observe=True,
            ready=True,
            active=True,
        )
        if child.grant.state != "REGISTERED" or child.launch.state != "PID_RECORDED":
            raise NativeStreamError(
                "SUBTASK_FENCED", "A retired DAG child cannot submit events"
            )
        subtask = child.subtask
        if running:
            running_child = child
    else:
        subtask, _ = await require_subtask(
            session, db, credentials, subtask_id=subtask_id
        )
    if event.resource.prefect_object_id("prefect.task-run") != subtask.task_run_id:
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "Event TaskRun is outside the grant"
        )
    authorized, native_ids = await _authorized_stream_events(
        session, db, [event.receive()], dag_running_child=running_child
    )
    if authorized:
        await record_bulk_task_run_events_in_session(
            authorized, db, session, native_ids, commit=False
        )
    task = await session.get(db.TaskRun, subtask.task_run_id, populate_existing=True)
    if task is None or task.state_type is None:
        raise NativeStreamError("IDENTITY_CONFLICT", "Physical TaskRun disappeared")
    return StreamSubtaskEventResult(
        event_id=event.id,
        accepted=bool(authorized),
        task_run_id=task.id,
        run_count=task.run_count,
        state_type=task.state_type,
    )


async def _authorized_stream_events(
    session: AsyncSession,
    db: PrefectDBInterface,
    events: list[ReceivedEvent],
    *,
    dag_running_child: dag_lifecycle.DAGChildScope | None = None,
) -> tuple[list[ReceivedEvent], set[UUID]]:
    """Fence enrolled task events under the same locks as their upsert."""
    shared_running = dag_running_child is not None
    if dag_running_child is not None:
        if len(events) != 1 or not _is_canonical_dag_running_event(events[0]):
            return [], set()
        dag_running_child.graph.validate(session)
        flow_resource = events[0].resource_in_role.get("flow-run")
        if (
            dag_running_child.graph.update
            or events[0].resource.prefect_object_id("prefect.task-run")
            != dag_running_child.subtask.task_run_id
            or flow_resource is None
            or flow_resource.prefect_object_id("prefect.flow-run")
            != dag_running_child.graph.flow.id
        ):
            return [], set()

    event_scope: list[tuple[ReceivedEvent, UUID | None, UUID]] = []
    flow_ids: set[UUID] = set()
    task_ids: set[UUID] = set()
    for event in events:
        task_id = event.resource.prefect_object_id("prefect.task-run")
        flow_id = None
        if flow_resource := event.resource_in_role.get("flow-run"):
            flow_id = flow_resource.prefect_object_id("prefect.flow-run")
            flow_ids.add(flow_id)
        event_scope.append((event, flow_id, task_id))
        task_ids.add(task_id)

    manifests = {
        row.task_run_id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamSubmission).where(
                    db.StreamSubmission.task_run_id.in_(task_ids),
                    db.StreamSubmission.state == "ACTIVE",
                )
            )
        ).all()
    }
    finite_manifests = {
        row.task_run_id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamFiniteSubmission).where(
                    db.StreamFiniteSubmission.task_run_id.in_(task_ids),
                    db.StreamFiniteSubmission.state == "ACTIVE",
                )
            )
        ).all()
    }
    task_flow_ids = dict(
        (
            await session.execute(
                sa.select(db.TaskRun.id, db.TaskRun.flow_run_id).where(
                    db.TaskRun.id.in_(task_ids)
                )
            )
        ).all()
    )
    flow_ids.update(row.flow_run_id for row in manifests.values())
    flow_ids.update(row.flow_run_id for row in finite_manifests.values())
    flow_ids.update(
        flow_run_id for flow_run_id in task_flow_ids.values() if flow_run_id is not None
    )
    if not flow_ids:
        return events, set()
    if dag_running_child is not None and flow_ids != {dag_running_child.graph.flow.id}:
        return [], set()

    locator_query = (
        sa.select(
            db.StreamGraphRevision.id,
            db.StreamGraphRevision.flow_run_id,
            db.StreamGraphRevision.profile,
        )
        .where(db.StreamGraphRevision.flow_run_id.in_(flow_ids))
        .order_by(db.StreamGraphRevision.id)
    )
    if dag_running_child is not None:
        locator_query = locator_query.where(
            db.StreamGraphRevision.id == dag_running_child.graph.graph.id
        )
    graph_locators = (await session.execute(locator_query)).all()
    if shared_running and (
        len(graph_locators) != 1 or graph_locators[0].profile != "partitioned-dag-v1"
    ):
        return [], set()
    dag_flow_ids = {
        row.flow_run_id for row in graph_locators if row.profile == "partitioned-dag-v1"
    }
    locked_flows = {}
    authorities = {}
    for flow_id in sorted(flow_ids):
        flow = await session.scalar(
            sa.select(db.FlowRun)
            .where(db.FlowRun.id == flow_id)
            .with_for_update(read=shared_running or flow_id not in dag_flow_ids)
            .execution_options(populate_existing=True)
        )
        if flow is not None:
            locked_flows[flow_id] = flow
    for flow_id in sorted(flow_ids):
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority)
            .where(db.StreamExecutionAuthority.flow_run_id == flow_id)
            .with_for_update(read=shared_running or flow_id not in dag_flow_ids)
            .execution_options(populate_existing=True)
        )
        if authority is not None:
            authorities[flow_id] = authority
    # Locator reads acquire no ownership. Lock and refresh the complete prefix
    # before using a cached graph/subtask/grant for an event decision.
    physical_graphs = {}
    for locator in graph_locators:
        graph = await session.scalar(
            sa.select(db.StreamGraphRevision)
            .where(db.StreamGraphRevision.id == locator.id)
            .with_for_update(
                read=shared_running or locator.profile != "partitioned-dag-v1"
            )
            .execution_options(populate_existing=True)
        )
        if graph is not None:
            physical_graphs[graph.id] = graph
    graphs_by_flow = {row.flow_run_id: row for row in physical_graphs.values()}
    dag_graph_ids = [
        row.id
        for row in physical_graphs.values()
        if row.profile == "partitioned-dag-v1"
    ]
    # The private RUNNING proof already holds its exact Worker identity lock. Taking
    # another, earlier Worker here would reverse admission's sorted lock order.
    if dag_graph_ids and not shared_running:
        assigned_workers = sa.select(db.StreamPhysicalSubtask.assigned_worker_id).where(
            db.StreamPhysicalSubtask.graph_revision_id.in_(dag_graph_ids),
            db.StreamPhysicalSubtask.assigned_worker_id.is_not(None),
        )
        await session.execute(
            sa.select(db.Worker.id)
            .where(db.Worker.id.in_(assigned_workers))
            .order_by(db.Worker.id)
            .with_for_update(read=True)
        )
    physical_subtasks = {
        row.task_run_id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamPhysicalSubtask)
                .where(db.StreamPhysicalSubtask.task_run_id.in_(task_ids))
                .order_by(db.StreamPhysicalSubtask.id)
                .with_for_update(read=True)
                .execution_options(populate_existing=True)
            )
        ).all()
    }
    physical_grants = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamSubtaskGrant)
                .where(
                    db.StreamSubtaskGrant.id.in_(
                        [
                            subtask.current_grant_id
                            for subtask in physical_subtasks.values()
                            if subtask.current_grant_id is not None
                        ]
                    )
                )
                .order_by(db.StreamSubtaskGrant.id)
                .with_for_update(read=True)
                .execution_options(populate_existing=True)
            )
        ).all()
    }
    dag_launches = (
        list(
            (
                await session.scalars(
                    sa.select(db.StreamDAGLaunchAttempt)
                    .where(
                        db.StreamDAGLaunchAttempt.registered_grant_id.in_(
                            physical_grants
                        )
                    )
                    .order_by(db.StreamDAGLaunchAttempt.id)
                    .with_for_update(read=True)
                )
            ).all()
        )
        if dag_graph_ids
        else []
    )
    dag_slots = (
        {
            row.subtask_id: row
            for row in (
                await session.scalars(
                    sa.select(db.StreamPhysicalSlotAllocation).where(
                        db.StreamPhysicalSlotAllocation.graph_revision_id.in_(
                            dag_graph_ids
                        )
                    )
                )
            ).all()
        }
        if dag_graph_ids
        else {}
    )
    for manifest in [*manifests.values(), *finite_manifests.values()]:
        await session.refresh(manifest)
    locked_tasks = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.TaskRun)
                .where(db.TaskRun.id.in_(task_ids))
                .order_by(db.TaskRun.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).all()
    }
    outputs = {
        row.id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamPartition)
                .where(
                    db.StreamPartition.id.in_(
                        [manifest.stream_id for manifest in manifests.values()]
                    )
                )
                .order_by(db.StreamPartition.id)
                .with_for_update(read=True)
            )
        ).all()
    }
    finite_receipts = {
        row.finite_submission_id: row
        for row in (
            await session.scalars(
                sa.select(db.StreamFiniteReceipt)
                .where(
                    db.StreamFiniteReceipt.finite_submission_id.in_(
                        [manifest.id for manifest in finite_manifests.values()]
                    )
                )
                .order_by(db.StreamFiniteReceipt.finite_submission_id)
                .with_for_update(read=True)
            )
        ).all()
    }

    decisions: dict[int, bool] = {}
    candidates: dict[UUID, list[tuple[ReceivedEvent, int, str]]] = {}
    native_task_ids: set[UUID] = set()
    for event, flow_id, task_id in event_scope:
        manifest = manifests.get(task_id)
        finite_manifest = finite_manifests.get(task_id)
        active_manifest = manifest or finite_manifest
        authoritative_flow_id = (
            active_manifest.flow_run_id
            if active_manifest is not None
            else task_flow_ids.get(task_id, flow_id)
        )
        graph = graphs_by_flow.get(authoritative_flow_id)
        is_dag = graph is not None and graph.profile == "partitioned-dag-v1"
        if is_dag:
            physical = physical_subtasks.get(task_id)
            grant = (
                physical_grants.get(physical.current_grant_id)
                if physical is not None
                else None
            )
            authority = authorities.get(authoritative_flow_id)
            flow = locked_flows.get(authoritative_flow_id)
            linked = [
                row
                for row in dag_launches
                if grant is not None and row.registered_grant_id == grant.id
            ]
            slot = dag_slots.get(physical.id) if physical is not None else None
            if (
                physical is None
                or grant is None
                or authority is None
                or flow is None
                or graph is None
                or slot is None
                or slot.released
                or physical.physical_slot_released
                or slot.worker_id != physical.assigned_worker_id
                or len(linked) != 1
                or linked[0].state != "PID_RECORDED"
                or linked[0].grant_id != grant.id
                or linked[0].subtask_id != physical.id
                or (linked[0].child_process_id, linked[0].process_start_ticks)
                != (grant.child_process_id, grant.process_start_ticks)
            ):
                decisions[id(event)] = False
                continue
            try:
                dag_lifecycle.validate_current_bootstrap_ready(
                    dag_lifecycle.DAGChildScope(
                        dag_lifecycle.DAGGraphScope(
                            session,
                            session.get_transaction(),
                            flow,
                            authority,
                            graph,
                            not shared_running,
                        ),
                        physical,
                        grant,
                        linked[0],
                    )
                )
            except (NativeStreamError, ValueError):
                decisions[id(event)] = False
                continue
        authority = authorities.get(authoritative_flow_id)
        if authority is None:
            decisions[id(event)] = True
            continue
        if not is_dag:
            native_task_ids.add(task_id)
        try:
            if flow_id != authoritative_flow_id:
                raise ValueError("task event FlowRun identity drift")
            generation = int(event.resource["prefect.stream-execution-generation"])
            holder_id = UUID(event.resource["prefect.stream-execution-holder"])
            manifest_digest = event.resource["prefect.stream-submission-digest"]
            key_id = event.resource["prefect.stream-event-key-id"]
            signature = event.resource["prefect.stream-event-signature"]
            physical = physical_subtasks.get(task_id)
            physical_grant = (
                physical_grants.get(physical.current_grant_id)
                if physical is not None
                else None
            )
            current_generation = authority.generation
            current_holder = authority.holder_id
            current_key_id = authority.event_key_id
            current_public_key = authority.holder_event_public_key
            if physical is not None:
                graph = physical_graphs.get(physical.graph_revision_id)
                if (
                    physical_grant is None
                    or graph is None
                    or graph.state != "ADMITTED"
                    or graph.startup_completed_at is None
                    or db.dialect.name != "postgresql"
                    or physical_grant.state != "REGISTERED"
                    or physical_grant.physical_epoch != physical.physical_epoch
                    or physical.assignment_epoch != graph.assignment_epoch
                    or physical_grant.flow_generation != authority.generation
                    or physical_grant.flow_holder_id != authority.holder_id
                ):
                    raise ValueError("physical subtask is no longer current")
                if (
                    event.resource["prefect.stream-graph-revision"] != str(graph.id)
                    or int(event.resource["prefect.stream-assignment-epoch"])
                    != graph.assignment_epoch
                    or event.resource["prefect.stream-subtask"] != str(physical.id)
                    or event.resource["prefect.stream-subtask-grant"]
                    != str(physical_grant.id)
                    or int(event.resource["prefect.stream-physical-epoch"])
                    != physical_grant.physical_epoch
                    or event.resource["prefect.stream-scope-digest"]
                    != physical_grant.scope_digest
                ):
                    raise ValueError("physical event scope drift")
                current_generation = physical_grant.physical_epoch
                current_holder = physical_grant.holder_id
                current_key_id = physical_grant.event_key_id
                current_public_key = physical_grant.event_public_key
            run_count = int(event.resource["prefect.run-count"])
            task_run_payload = event.payload["task_run"]
            validated_state = event.payload["validated_state"]
            if not isinstance(task_run_payload, dict) or not isinstance(
                validated_state, dict
            ):
                raise ValueError("native event payload is not an object")
            if _NATIVE_RESERVED_TASK_PAYLOAD_KEYS.intersection(task_run_payload):
                raise ValueError("native event carries reserved TaskRun fields")
            if _NATIVE_RESERVED_STATE_PAYLOAD_KEYS.intersection(validated_state):
                raise ValueError("native event carries reserved State fields")
            state_type = validated_state.get("type")
            if (
                is_dag
                and graph is not None
                and graph.current_control_operation_id is not None
                and state_type not in {"FAILED", "CRASHED", "CANCELLED", "COMPLETED"}
            ):
                raise ValueError("DAG control fences new nonterminal Task events")
            payload_run_count = task_run_payload.get("run_count")
            details = validated_state.get("state_details")
            non_retriable = (
                isinstance(details, dict) and details.get("retriable") is False
            )
            if physical is not None and state_type in {"FAILED", "CRASHED"}:
                physical_task = locked_tasks.get(task_id)
                output = (
                    outputs.get(manifest.stream_id) if manifest is not None else None
                )
                if (
                    physical_task is None
                    or type(payload_run_count) is not int
                    or payload_run_count
                    < (physical_task.empirical_policy.retries or 0) + 1
                    and not non_retriable
                    or output is None
                    or output.state not in ("OPEN", "FAILED")
                ):
                    raise ValueError(
                        "physical terminal failure requires exhausted policy and durable failed output"
                    )
            flow_run_run_count = task_run_payload.get("flow_run_run_count")
            payload_tags = task_run_payload.get("tags")
            if not isinstance(payload_tags, list) or not all(
                isinstance(tag, str) for tag in payload_tags
            ):
                raise ValueError("native event tags are not a string list")
            normalized_tags = sorted(set(payload_tags))
            event_policy = TaskRunPolicy.model_validate(
                task_run_payload.get("empirical_policy")
            ).model_dump(mode="json")
            manifest_policy = TaskRunPolicy.model_validate(
                active_manifest.task_policy if active_manifest is not None else None
            ).model_dump(mode="json")
            manifest_ready = False
            if manifest is not None:
                manifest_ready = authority.manifest_set_digest is not None
            elif finite_manifest is not None:
                manifest_ready = (
                    finite_manifest.phase == "PREFIX"
                    and authority.finite_prefix_manifest_digest
                    == finite_manifest.manifest_digest
                    and authority.finite_prefix_task_run_id
                    == finite_manifest.task_run_id
                ) or (
                    finite_manifest.phase == "TAIL"
                    and authority.manifest_set_digest is not None
                )
            if (
                (
                    authority.state != "ACTIVE"
                    and not (
                        authority.state == "TERMINAL" and state_type == "COMPLETED"
                    )
                )
                or generation != current_generation
                or holder_id != current_holder
                or key_id != current_key_id
                or authority.prepared_generation != authority.generation
                or not manifest_ready
                or active_manifest is None
                or active_manifest.flow_run_id != authoritative_flow_id
                or active_manifest.manifest_digest != manifest_digest
                or task_run_payload.get("task_key") != active_manifest.task_key
                or task_run_payload.get("dynamic_key") != active_manifest.dynamic_key
                or task_run_payload.get("task_version") != active_manifest.task_version
                or normalized_tags != active_manifest.tags
                or event_policy != manifest_policy
                or type(payload_run_count) is not int
                or payload_run_count != run_count
                or type(flow_run_run_count) is not int
                or flow_run_run_count != authority.flow_run_run_count
                or (authority.cancel_requested and state_type != "CANCELLED")
            ):
                raise ValueError("stale execution event")
            finite_receipt = (
                finite_receipts.get(finite_manifest.id)
                if finite_manifest is not None
                else None
            )
            finite_result_shape = (
                canonical_json_digest(finite_receipt.result)
                if finite_receipt is not None
                else None
            )
            if finite_receipt is not None and (
                finite_receipt.run_count != run_count
                or state_type not in {"RUNNING", "COMPLETED"}
                or finite_manifest is None
                or finite_result_shape
                != (finite_receipt.result_size, finite_receipt.result_digest)
                or finite_receipt.outcome_digest
                != finite_receipt_outcome_digest(
                    flow_run_id=finite_manifest.flow_run_id,
                    phase=finite_manifest.phase,
                    submission_key=finite_manifest.submission_key,
                    manifest_digest=finite_manifest.manifest_digest,
                    task_run_id=finite_manifest.task_run_id,
                    operation_id=finite_receipt.operation_id,
                    run_count=finite_receipt.run_count,
                    result_digest=finite_receipt.result_digest,
                )
            ):
                raise ValueError("finite result event would advance a durable receipt")
            if state_type == "COMPLETED":
                if manifest is not None:
                    output = outputs.get(manifest.stream_id)
                    if (
                        output is None
                        or output.state != "COMPLETED"
                        or output.producer_task_run_id != task_id
                        or output.attempt != run_count
                    ):
                        raise ValueError("TaskRun completed before durable stream EOF")
                else:
                    assert finite_manifest is not None
                    if finite_receipt is None:
                        raise ValueError(
                            "TaskRun completed before durable finite result receipt"
                        )
            witness_arguments = dict(
                event_id=event.id,
                event=event.event,
                occurred=event.occurred,
                flow_run_id=authoritative_flow_id,
                task_run_id=task_id,
                generation=generation,
                holder_id=holder_id,
                manifest_digest=manifest_digest,
                run_count=run_count,
                task_run=task_run_payload,
                validated_state=validated_state,
            )
            if physical is not None:
                assert physical_grant is not None
                witness = canonical_physical_event_witness(
                    **witness_arguments,
                    graph_revision_id=physical.graph_revision_id,
                    assignment_epoch=physical.assignment_epoch,
                    subtask_id=physical.id,
                    grant_id=physical_grant.id,
                    physical_epoch=physical_grant.physical_epoch,
                    scope_digest=physical_grant.scope_digest,
                )
            else:
                witness = canonical_event_witness(**witness_arguments)
            if not verify_task_event(
                bytes.fromhex(current_public_key), witness, signature
            ):
                raise ValueError("invalid execution event signature")
            candidates.setdefault(task_id, []).append((event, run_count, state_type))
        except (KeyError, TypeError, ValueError):
            decisions[id(event)] = False
            logger.warning(
                "Discarding fenced native TaskRun event %s for flow %s",
                event.id,
                authoritative_flow_id,
            )

    for task_id, task_events in candidates.items():
        task = locked_tasks.get(task_id)
        if task is None:
            for event, _, _ in task_events:
                decisions[id(event)] = False
            continue
        current_run_count = task.run_count
        retry_limit = task.empirical_policy.retries or 0
        maximum_run_count = retry_limit + 1
        terminal_event_id = (
            task.state_id if task.state_type in TERMINAL_STATES else None
        )
        terminal_state_type = (
            task.state_type.value if task.state_type in TERMINAL_STATES else None
        )
        for event, run_count, state_type in sorted(
            task_events, key=lambda item: (item[1], item[0].occurred, item[0].id)
        ):
            if task_id in physical_subtasks and terminal_event_id is not None:
                decisions[id(event)] = (
                    event.id == terminal_event_id
                    and state_type == terminal_state_type
                    and run_count == current_run_count
                )
                if decisions[id(event)] and task.state_type in TERMINAL_STATES:
                    decisions[id(event)] = _exact_terminal_replay(task, event)
            elif run_count > maximum_run_count or run_count < current_run_count:
                decisions[id(event)] = False
            elif run_count == current_run_count:
                decisions[id(event)] = True
            elif run_count == current_run_count + 1 and state_type == "RUNNING":
                current_run_count = run_count
                decisions[id(event)] = True
            else:
                decisions[id(event)] = False
            if (
                task_id in physical_subtasks
                and decisions[id(event)]
                and state_type in {state.value for state in TERMINAL_STATES}
            ):
                terminal_event_id = event.id
                terminal_state_type = state_type
            if not decisions[id(event)]:
                logger.warning(
                    "Discarding stale native TaskRun attempt event %s for task %s",
                    event.id,
                    task_id,
                )

    accepted = [event for event in events if decisions.get(id(event), False)]
    for event in accepted:
        task_id = event.resource.prefect_object_id("prefect.task-run")
        if task_id in physical_subtasks:
            graph = physical_graphs.get(physical_subtasks[task_id].graph_revision_id)
            if graph is not None and graph.profile == "partitioned-dag-v1":
                native_task_ids.add(task_id)
    return accepted, native_task_ids


def _task_run_upsert_key(task_run: TaskRun) -> TaskRunUpsertKey:
    if task_run.flow_run_id is None:
        return ("id", task_run.id)
    return (
        "natural-key",
        task_run.flow_run_id,
        task_run.task_key,
        task_run.dynamic_key,
    )


def _task_run_conflict_keys(task_run: TaskRun) -> list[TaskRunUpsertKey]:
    keys: list[TaskRunUpsertKey] = [("id", task_run.id)]
    if task_run.flow_run_id is not None:
        keys.append(
            (
                "natural-key",
                task_run.flow_run_id,
                task_run.task_key,
                task_run.dynamic_key,
            )
        )
    return keys


def _task_run_target_value(
    task_run: TaskRun, conflict_target: str
) -> TaskRunTargetValue:
    """The value `ON CONFLICT` matches this row on."""
    if conflict_target == "natural-key":
        return (task_run.flow_run_id, task_run.task_key, task_run.dynamic_key)
    return task_run.id


@dataclass
class _TaskRunRecord:
    """One event's task run, and the upsert bookkeeping resolved for it.

    The fields the constructor does not take are filled in stages, each by the
    method named below it. They have no defaults, so reading one before its
    stage has run raises `AttributeError` instead of silently reading a
    placeholder.
    """

    task_run: TaskRun
    # the columns to insert, keyed by column name; a key it does not carry is
    # left to its default, and a `None` value asks to write a real NULL
    task_run_dict: dict[str, Any]

    # `pin_upsert_key`
    conflict_group: TaskRunUpsertKey = field(init=False)
    upsert_key: TaskRunUpsertKey = field(init=False)
    # `resolve_conflict_target`
    conflict_target: str = field(init=False)
    target_value: TaskRunTargetValue = field(init=False)

    def pin_upsert_key(self, conflict_group: TaskRunUpsertKey) -> None:
        """Record the coalesced group, and the upsert key as it stands now.

        `rewrite_id` can change `task_run.id`, and so the upsert key, so it has
        to be pinned before that happens and used for every later lookup.
        """
        self.conflict_group = conflict_group
        self.upsert_key = _task_run_upsert_key(self.task_run)

    def rewrite_id(self, canonical_task_run_id: UUID) -> None:
        """Point the row at the id an existing row already holds.

        Both the model and the payload are rewritten, so the statement inserts
        the canonical id and callers observe it too.
        """
        self.task_run.id = canonical_task_run_id
        self.task_run_dict["id"] = canonical_task_run_id

    def resolve_conflict_target(self, conflict_target: str) -> None:
        """Fix the `ON CONFLICT` target, and the value it matches this row on."""
        self.conflict_target = conflict_target
        self.target_value = _task_run_target_value(self.task_run, conflict_target)


class _UpsertSegment(NamedTuple):
    """A contiguous run of task runs that can share one upsert statement."""

    conflict_target: str
    columns: frozenset[str]
    null_filled: frozenset[str]
    rows: list[_TaskRunRecord]


def _fillable_columns(db: PrefectDBInterface) -> frozenset[str]:
    """Columns where passing `NULL` does the same thing as leaving the key out.

    An explicit `None` overrides a column default, so any column with a default
    is excluded.
    """
    return (
        frozenset(
            column.name
            for column in db.TaskRun.__table__.columns
            if column.nullable
            and column.default is None
            and column.server_default is None
        )
        # the ON CONFLICT WHERE clause compares against this column, and
        # `x < NULL` is NULL, so a NULL-filled row would silently skip its update
        - {"state_timestamp"}
        # ON CONFLICT matches rows on these columns, and NULL matches nothing, so
        # a filled row would insert a duplicate instead of updating. Excluding them
        # also keeps `flow_run_id` out of the coalesce, so an event with no flow
        # run still clears it.
        - {column.key for column in db.orm.task_run_unique_upsert_columns}
    )


def _segment_task_runs_for_upsert(
    task_runs: list[_TaskRunRecord],
    fillable: frozenset[str],
    max_rows: int,
) -> list[_UpsertSegment]:
    """Group already-sorted task runs into the longest batches that can share a statement.

    Each record must have had `resolve_conflict_target` called on it.

    Segments are contiguous blocks of the input, emitted in order, so
    concatenating them gives back the input unchanged. The caller has already
    sorted the rows by conflict key, which is what makes concurrent recorders
    take row locks in the same order and so not deadlock.
    """
    segments: list[_UpsertSegment] = []

    conflict_target = ""
    # Segments below hold on to these, and `|=` on a frozenset rebinds rather
    # than mutating in place, so an emitted segment's sets can never grow. Plain
    # sets would leave every segment sharing one object that ends up describing
    # the whole flush.
    union: frozenset[str] = frozenset()
    null_filled: frozenset[str] = frozenset()
    explicit_nulls: frozenset[str] = frozenset()
    seen: set[TaskRunTargetValue] = set()
    rows: list[_TaskRunRecord] = []

    for tr in task_runs:
        columns = frozenset(tr.task_run_dict)
        row_nulls = frozenset(
            column for column, value in tr.task_run_dict.items() if value is None
        )
        if rows:
            # A statement has a single column list, so any row missing one of its
            # columns is given NULL there. Merging adds a fill for every column the
            # batch and this row do not both have.
            merged_fills = null_filled | (union ^ columns)
            merged_nulls = explicit_nulls | row_nulls
            if (
                tr.conflict_target == conflict_target
                # a fill is only safe on a fillable column, and only where no row
                # writes a real NULL: the two are the same value, so one SET
                # clause cannot mean both
                and merged_fills <= fillable - merged_nulls
                # a target value already in the batch makes PostgreSQL raise
                # CardinalityViolationError, which is not an IntegrityError and so is
                # never retried, and makes SQLite silently keep only the last write
                and tr.target_value not in seen
                and len(rows) < max_rows
            ):
                union |= columns
                null_filled, explicit_nulls = merged_fills, merged_nulls
                seen.add(tr.target_value)
                rows.append(tr)
                continue
            segments.append(_UpsertSegment(conflict_target, union, null_filled, rows))

        conflict_target = tr.conflict_target
        union = columns
        null_filled = frozenset()
        explicit_nulls = row_nulls
        seen = {tr.target_value}
        rows = [tr]

    if rows:
        segments.append(_UpsertSegment(conflict_target, union, null_filled, rows))

    return segments


@db_injector
async def _insert_task_run_states(
    db: PrefectDBInterface, session: AsyncSession, task_runs: list[TaskRun]
):
    if TYPE_CHECKING:
        for task_run in task_runs:
            assert task_run.state is not None

    now = prefect.types._datetime.now("UTC")

    await session.execute(
        db.queries.insert(db.TaskRunState)
        .values(
            [
                {
                    "created": now,
                    "task_run_id": task_run.id,
                    **task_run.state.model_dump(),
                }
                for task_run in task_runs
            ]
        )
        .on_conflict_do_nothing(
            index_elements=[
                "id",
            ]
        )
    )

    logger.debug(f"Recorded {len(task_runs)} task run state change(s)")


def task_run_from_event(event: ReceivedEvent) -> TaskRun:
    task_run_id = event.resource.prefect_object_id("prefect.task-run")

    flow_run_id: Optional[UUID] = None
    if flow_run_resource := event.resource_in_role.get("flow-run"):
        flow_run_id = flow_run_resource.prefect_object_id("prefect.flow-run")

    state: State = State.model_validate(
        {
            "id": event.id,
            "timestamp": event.occurred,
            **event.payload["validated_state"],
        }
    )
    state.state_details.task_run_id = task_run_id
    state.state_details.flow_run_id = flow_run_id

    return TaskRun.model_validate(
        {
            "id": task_run_id,
            "flow_run_id": flow_run_id,
            "state_id": state.id,
            "state": state,
            **event.payload["task_run"],
        }
    )


def _exact_terminal_replay(task: Any, event: ReceivedEvent) -> bool:
    """A retained event ID cannot authorize different signed terminal bytes."""
    try:
        proposed, attributes = db_recordable_task_run_from_event(event)
        if (
            task.state is None
            or proposed.state is None
            or proposed.state.model_dump(mode="json")
            != task.state.as_state().model_dump(mode="json")
        ):
            return False
        for key, value in attributes.items():
            current = getattr(task, key)
            if hasattr(value, "model_dump"):
                value = value.model_dump(mode="json")
            if hasattr(current, "model_dump"):
                current = current.model_dump(mode="json")
            if current != value:
                return False
        return True
    except (KeyError, ValueError, TypeError):
        return False


def db_recordable_task_run_from_event(
    event: ReceivedEvent,
) -> tuple[TaskRun, dict[str, Any]]:
    task_run: TaskRun = task_run_from_event(event)

    task_run_attributes = task_run.model_dump_for_orm(
        exclude={
            "state_id",
            "state",
            "created",
            "estimated_run_time",
            "estimated_start_time_delta",
        },
        exclude_unset=True,
    )

    assert task_run.state is not None

    denormalized_state_attributes = {
        "state_id": task_run.state.id,
        "state_type": task_run.state.type,
        "state_name": task_run.state.name,
        "state_timestamp": task_run.state.timestamp,
    }

    return task_run, {
        **task_run_attributes,
        **denormalized_state_attributes,
    }


async def record_task_run_event(event: ReceivedEvent, depth: int = 0) -> None:
    """Record a single task run event in the database.

    Delegates to `record_bulk_task_run_events`, which already retries once on
    `IntegrityError` to recover from TOCTOU races against concurrent recorders.
    Any `IntegrityError` that survives the retry is treated as an unrecoverable
    duplicate and the event is discarded.
    """
    try:
        await record_bulk_task_run_events([event])
    except IntegrityError:
        logger.warning(
            "Duplicate task_run, discarding event %s",
            event.id,
            exc_info=True,
        )


async def record_bulk_task_run_events(events: list[ReceivedEvent]) -> None:
    """Record multiple task run events in the database, taking advantage of bulk inserts.

    Retries once on `IntegrityError` to handle TOCTOU races between concurrent
    recorder instances: when two batches reference the same `task_run.id` with
    different natural keys, one batch's existence-check SELECT may run before
    the other batch's INSERT commits. The retry re-runs the SELECT in a fresh
    session so the conflict target is chosen against the now-visible row.
    """

    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        try:
            await _record_bulk_task_run_events(events)
            return
        except IntegrityError:
            if attempt < max_attempts:
                logger.info(
                    "Retrying bulk task_run upsert after IntegrityError"
                    " (attempt %s/%s)",
                    attempt,
                    max_attempts,
                )
                continue
            raise


async def _record_bulk_task_run_events(events: list[ReceivedEvent]) -> None:
    if not events:
        return
    db = provide_database_interface()
    async with db.session_context() as session:
        authorized, native_task_ids = await _authorized_stream_events(
            session, db, events
        )
        if not authorized:
            return
        await _record_bulk_task_run_events_in_session(
            authorized, db, session, native_task_ids
        )


async def _record_bulk_task_run_events_in_session(
    events: list[ReceivedEvent],
    db: PrefectDBInterface,
    session: AsyncSession,
    native_task_ids: set[UUID],
    *,
    commit: bool = True,
) -> None:
    now = prefect.types._datetime.now("UTC")

    all_task_runs = [
        _TaskRunRecord(task_run=task_run, task_run_dict=task_run_dict)
        for event in events
        for task_run, task_run_dict in [db_recordable_task_run_from_event(event)]
    ]

    # Drop duplicate task run rows, keep the one with the latest state_timestamp.
    # A single bulk flush can contain events that collide on either id or natural
    # key, so coalesce connected conflicts before choosing the ON CONFLICT target.
    all_task_runs.sort(
        key=lambda tr: (
            tr.task_run.run_count if tr.task_run.id in native_task_ids else 0,
            tr.task_run.state.timestamp,
        )
    )
    parent: dict[TaskRunUpsertKey, TaskRunUpsertKey] = {}

    def find(key: TaskRunUpsertKey) -> TaskRunUpsertKey:
        parent.setdefault(key, key)
        if parent[key] != key:
            parent[key] = find(parent[key])
        return parent[key]

    def union(left: TaskRunUpsertKey, right: TaskRunUpsertKey) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for tr in all_task_runs:
        conflict_keys = _task_run_conflict_keys(tr.task_run)
        for conflict_key in conflict_keys[1:]:
            union(conflict_keys[0], conflict_key)

    unique_task_runs_by_group: dict[TaskRunUpsertKey, _TaskRunRecord] = {}
    conflict_keys_by_group: dict[TaskRunUpsertKey, set[TaskRunUpsertKey]] = {}
    upsert_key_aliases: dict[TaskRunUpsertKey, TaskRunUpsertKey] = {}
    for tr in all_task_runs:
        conflict_keys = _task_run_conflict_keys(tr.task_run)
        conflict_group = find(conflict_keys[0])
        tr.pin_upsert_key(conflict_group)
        unique_task_runs_by_group[conflict_group] = tr
        conflict_keys_by_group.setdefault(conflict_group, set()).update(conflict_keys)

    for tr in all_task_runs:
        upsert_key_aliases[tr.upsert_key] = unique_task_runs_by_group[
            tr.conflict_group
        ].upsert_key

    unique_task_runs = sorted(
        unique_task_runs_by_group.values(),
        key=lambda tr: tr.upsert_key,
    )

    task_run_ids: list[UUID] = []
    natural_keys: list[tuple[UUID, str, str]] = []
    for conflict_keys in conflict_keys_by_group.values():
        for conflict_key in conflict_keys:
            if len(conflict_key) == 2:
                task_run_ids.append(conflict_key[1])
            else:
                natural_keys.append((conflict_key[1], conflict_key[2], conflict_key[3]))

    existing_task_run_ids: set[UUID] = set()
    existing_natural_keys: set[tuple[UUID, str, str]] = set()
    existing_task_run_ids_by_key: dict[TaskRunUpsertKey, UUID] = {}
    if task_run_ids or natural_keys:
        conditions = []
        if task_run_ids:
            conditions.append(db.TaskRun.id.in_(task_run_ids))
        if natural_keys:
            conditions.append(
                sa.tuple_(
                    db.TaskRun.flow_run_id,
                    db.TaskRun.task_key,
                    db.TaskRun.dynamic_key,
                ).in_(natural_keys)
            )
        result = await session.execute(
            sa.select(
                db.TaskRun.id,
                db.TaskRun.flow_run_id,
                db.TaskRun.task_key,
                db.TaskRun.dynamic_key,
            ).where(sa.or_(*conditions))
        )
        for task_run_id, flow_run_id, task_key, dynamic_key in result.all():
            existing_task_run_ids.add(task_run_id)
            existing_task_run_ids_by_key[("id", task_run_id)] = task_run_id
            if flow_run_id is not None:
                existing_natural_keys.add((flow_run_id, task_key, dynamic_key))
                existing_task_run_ids_by_key[
                    ("natural-key", flow_run_id, task_key, dynamic_key)
                ] = task_run_id

    def conflict_target(tr: _TaskRunRecord) -> str:
        task_run = tr.task_run
        natural_key = (
            task_run.flow_run_id,
            task_run.task_key,
            task_run.dynamic_key,
        )
        if task_run.flow_run_id is not None and natural_key in existing_natural_keys:
            return "natural-key"
        if task_run.id in existing_task_run_ids:
            return "id"
        for conflict_key in sorted(conflict_keys_by_group[tr.conflict_group], key=str):
            if conflict_key in existing_task_run_ids_by_key:
                tr.rewrite_id(existing_task_run_ids_by_key[conflict_key])
                return "id"
        if task_run.flow_run_id is not None:
            return "natural-key"
        return "id"

    for tr in unique_task_runs:
        tr.resolve_conflict_target(conflict_target(tr))

    fillable = _fillable_columns(db)
    # Sized on how many columns the table has, not how many keys the rows
    # carry: SQLAlchemy renders a bind parameter for every column with a
    # Python-side default, whether the row supplies it or not.
    max_rows = get_max_query_parameters() // len(db.TaskRun.__table__.columns)
    segments = _segment_task_runs_for_upsert(unique_task_runs, fillable, max_rows)

    logger.debug(
        "Partitioned %s task runs into %s upsert statement(s)",
        len(unique_task_runs),
        len(segments),
    )

    canonical_task_run_ids: dict[TaskRunUpsertKey, UUID] = {}
    supports_insertmanyvalues = db.dialect.name == "postgresql"

    for segment in segments:
        update_cols = (segment.columns | {"updated"}) - {"id", "created"}
        # sorted so the column order does not vary with the hash seed, which
        # keeps the generated SQL text stable
        insert_cols = sorted(segment.columns)

        # Execution-time parameter sets must share a column list, so give every
        # row the same keys.
        to_insert = [
            {column: tr.task_run_dict.get(column) for column in insert_cols}
            | {"created": now, "updated": now}
            for tr in segment.rows
        ]
        use_insertmanyvalues = supports_insertmanyvalues and len(to_insert) > 1

        insert_statement = db.queries.insert(db.TaskRun)
        if not use_insertmanyvalues:
            insert_statement = insert_statement.values(to_insert)
        index_elements = (
            db.orm.task_run_unique_upsert_columns
            if segment.conflict_target == "natural-key"
            else ["id"]
        )
        # See https://www.postgresql.org/docs/current/sql-insert.html for details on excluded.
        # Idea is excluded.x references the proposed insertion value for column x.
        # `coalesce` undoes the NULL fills, and applies to exactly the columns
        # that were filled. A column every row supplied keeps a plain
        # `excluded.col`, so a row asking to write a real NULL still writes one.
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=index_elements,
            set_={
                col.name: (
                    sa.func.coalesce(
                        getattr(insert_statement.excluded, col.name),
                        getattr(db.TaskRun, col.name),
                    )
                    if col.name in segment.null_filled
                    else getattr(insert_statement.excluded, col.name)
                )
                for col in insert_statement.excluded
                if col.name in update_cols
            },
            where=db.TaskRun.state_timestamp
            < insert_statement.excluded.state_timestamp,
        )
        if use_insertmanyvalues:
            # asyncpg uses insertmanyvalues for execution-time parameters only
            # when RETURNING is present. Return the ids and discard them. Core
            # DML keeps the explicit NULL values used by the upsert semantics.
            upsert_statement = upsert_statement.returning(
                db.TaskRun.id
            ).execution_options(dml_strategy="raw")
            result = await session.execute(upsert_statement, to_insert)
            result.close()
        else:
            # SQLite versions before 3.35 do not support RETURNING. Keep the
            # existing multi-row statement for supported older versions.
            await session.execute(upsert_statement)

        if segment.conflict_target == "natural-key":
            segment_natural_keys = [
                (
                    tr.task_run.flow_run_id,
                    tr.task_run.task_key,
                    tr.task_run.dynamic_key,
                )
                for tr in segment.rows
            ]
            result = await session.execute(
                sa.select(
                    db.TaskRun.flow_run_id,
                    db.TaskRun.task_key,
                    db.TaskRun.dynamic_key,
                    db.TaskRun.id,
                ).where(
                    sa.tuple_(
                        db.TaskRun.flow_run_id,
                        db.TaskRun.task_key,
                        db.TaskRun.dynamic_key,
                    ).in_(segment_natural_keys)
                )
            )
            for flow_run_id, task_key, dynamic_key, task_run_id in result.all():
                canonical_task_run_ids[
                    ("natural-key", flow_run_id, task_key, dynamic_key)
                ] = task_run_id
        else:
            for tr in segment.rows:
                canonical_task_run_ids[tr.upsert_key] = tr.task_run.id

    for alias_key, canonical_key in upsert_key_aliases.items():
        canonical_task_run_ids[alias_key] = canonical_task_run_ids[canonical_key]

    for tr in all_task_runs:
        task_run = tr.task_run
        canonical_task_run_id = canonical_task_run_ids[tr.upsert_key]
        task_run.id = canonical_task_run_id
        if task_run.state is not None:
            task_run.state.state_details.task_run_id = canonical_task_run_id

    # Insert all task run states - we only coalesce task run updates, not states
    await _insert_task_run_states(session, [tr.task_run for tr in all_task_runs])
    terminal_physical = (
        await session.scalars(
            sa.select(db.StreamPhysicalSubtask)
            .join(db.TaskRun, db.TaskRun.id == db.StreamPhysicalSubtask.task_run_id)
            .where(
                db.TaskRun.id.in_(native_task_ids),
                db.TaskRun.state_type.in_(TERMINAL_STATES),
            )
        )
    ).all()
    for subtask in terminal_physical:
        graph = await session.get(db.StreamGraphRevision, subtask.graph_revision_id)
        if graph is not None and graph.profile == "partitioned-dag-v1":
            task = await session.get(
                db.TaskRun, subtask.task_run_id, populate_existing=True
            )
            if (
                task is not None
                and task.state_type is not None
                and task.state_type.value in {"FAILED", "CRASHED"}
                and task.state_id is not None
            ):
                await dag_lifecycle.record_dag_failed_task_event(
                    session, db, graph, subtask, task.state_id
                )
        await release_terminal_allocation(session, db, subtask)
    if commit:
        await session.commit()


class RetryableEvent(BaseModel):
    event: ReceivedEvent
    persist_attempts: int = 0


@asynccontextmanager
async def consumer(
    write_batch_size: int,
    flush_every: int,
    max_persist_retries: int = DEFAULT_PERSIST_MAX_RETRIES,
) -> AsyncGenerator[MessageHandler, None]:
    logger.info(
        f"Creating TaskRunRecorder consumer with batch size {write_batch_size} and flush every {flush_every} seconds"
    )

    queue: asyncio.Queue[RetryableEvent] = asyncio.Queue()

    async def flush() -> None:
        logger.debug(f"Persisting {queue.qsize()} events...")

        batch: list[RetryableEvent] = []

        while queue.qsize() > 0 and len(batch) < write_batch_size:
            batch.append(await queue.get())

        try:
            await record_bulk_task_run_events([item.event for item in batch])
        except (Exception, asyncio.CancelledError) as exc:
            # Database drivers can surface connection failures as
            # CancelledError, so apply the same retry accounting to it
            dropped = 0
            to_retry = 0
            for item in batch:
                item.persist_attempts += 1
                if item.persist_attempts <= max_persist_retries:
                    to_retry += 1
                    await queue.put(item)
                else:
                    dropped += 1
                    logger.error(
                        f"Dropping event {item.event.id} after {item.persist_attempts} failed attempts"
                    )
            logger.error(
                f"Error flushing {len(batch)} events ({to_retry} to retry, {dropped} dropped)",
                exc_info=True,
            )

            if dropped > 0 or isinstance(exc, asyncio.CancelledError):
                raise

    stopping = False

    async def flush_periodically():
        while True:
            try:
                await asyncio.sleep(flush_every)
                if queue.qsize():
                    await flush()
            except asyncio.CancelledError:
                if stopping:
                    return
                # Not a shutdown request (e.g. a CancelledError raised by the
                # database driver during an outage); exiting here would stop
                # periodic flushing for the life of the service
                logger.warning(
                    "Periodic flush was unexpectedly cancelled; continuing",
                    exc_info=True,
                )
            except Exception:
                # flush() re-raises when events are dropped; this task is never
                # awaited, so letting that propagate would kill periodic
                # flushing silently and strand queued events (issue #21057)
                logger.exception("Error during periodic flush; continuing")

    async def message_handler(message: Message):
        event: ReceivedEvent = ReceivedEvent.model_validate_json(message.data)

        if not event.event.startswith("prefect.task-run"):
            return

        if not event.resource.get("prefect.orchestration") == "client":
            return

        logger.debug(
            "Received event: %s with id: %s for resource: %s",
            event.event,
            event.id,
            event.resource.get("prefect.resource.id"),
        )

        await queue.put(RetryableEvent(event=event))

        if queue.qsize() >= write_batch_size:
            await flush()

    periodic_flush = asyncio.create_task(flush_periodically())

    try:
        yield message_handler
    finally:
        stopping = True
        periodic_flush.cancel()
        # let an in-flight periodic flush restore its batch before the final flush
        await asyncio.wait([periodic_flush])

        if queue.qsize():
            await flush()


class TaskRunRecorder(RunInEphemeralServers, Service):
    """Constructs task runs and states from client-emitted events"""

    consumer_task: asyncio.Task[None] | None = None
    metrics_task: asyncio.Task[None] | None = None

    @classmethod
    def service_settings(cls) -> ServicesBaseSetting:
        return get_current_settings().server.services.task_run_recorder

    def __init__(self):
        super().__init__()
        self._started_event: Optional[asyncio.Event] = None

    @property
    def started_event(self) -> asyncio.Event:
        if self._started_event is None:
            self._started_event = asyncio.Event()
        return self._started_event

    @started_event.setter
    def started_event(self, value: asyncio.Event) -> None:
        self._started_event = value

    async def start(
        self, max_persist_retries: int = DEFAULT_PERSIST_MAX_RETRIES
    ) -> NoReturn:
        assert self.consumer_task is None, "TaskRunRecorder already started"
        self.consumer: Consumer = create_consumer(
            "events",
            group="task-run-recorder",
            name=generate_unique_consumer_name("task-run-recorder"),
            read_batch_size=self.service_settings().read_batch_size,
        )

        async with consumer(
            write_batch_size=self.service_settings().batch_size,
            flush_every=int(self.service_settings().flush_interval),
            max_persist_retries=max_persist_retries,
        ) as handler:
            self.consumer_task = asyncio.create_task(self.consumer.run(handler))
            self.metrics_task = asyncio.create_task(log_metrics_periodically())

            logger.debug("TaskRunRecorder started")
            self.started_event.set()

            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass

    async def stop(self) -> None:
        assert self.consumer_task is not None, "Logger not started"
        self.consumer_task.cancel()
        if self.metrics_task:
            self.metrics_task.cancel()
        try:
            await self.consumer_task
            if self.metrics_task:
                await self.metrics_task
        except asyncio.CancelledError:
            pass
        finally:
            self.consumer_task = None
            self.metrics_task = None
        logger.debug("TaskRunRecorder stopped")


# Shared transaction-aware recorder boundary for physical HTTP task events.
authorized_stream_events = _authorized_stream_events
record_bulk_task_run_events_in_session = _record_bulk_task_run_events_in_session
