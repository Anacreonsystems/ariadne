"""Paired native-stream HTTP clients with typed, non-string-based refusals."""

from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import JsonValue, TypeAdapter

from prefect._internal.stream_event_auth import (
    canonical_physical_event_witness,
    verify_task_event,
)
from prefect._internal.stream_execution import (
    DAGSubtaskExecutionContext,
    get_stream_subtask_execution,
)
from prefect.client.orchestration.base import HTTP_METHODS, BaseAsyncClient, BaseClient
from prefect.client.orchestration.routes import ServerRoutes
from prefect.client.schemas import native_deployments as native_schemas
from prefect.client.schemas import stream_dag_executions as dag_execution_schemas
from prefect.client.schemas import stream_dags as dag_schemas
from prefect.client.schemas import stream_routes as route_schemas
from prefect.client.schemas import stream_subtasks as subtask_schemas
from prefect.client.schemas import streams as schemas
from prefect.client.schemas.objects import TaskRun
from prefect.events.schemas.events import Event
from prefect.exceptions import PrefectException


class StreamRequestError(PrefectException):
    """A wire refusal; admission may retry only explicit metadata lag."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        status_code: int = 409,
    ) -> None:
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        super().__init__(message)


def _refusal(error: httpx.HTTPStatusError) -> StreamRequestError:
    try:
        detail = error.response.json().get("detail")
    except (ValueError, AttributeError):
        detail = None
    if isinstance(detail, dict) and isinstance(detail.get("code"), str):
        return StreamRequestError(
            detail["code"],
            str(detail.get("message", "Stream request refused")),
            retryable=detail.get("retryable") is True,
            status_code=error.response.status_code,
        )
    code = "UNSUPPORTED_BACKEND" if error.response.status_code == 404 else "HTTP_ERROR"
    return StreamRequestError(
        code, "Native stream API request failed", status_code=error.response.status_code
    )


def _validate_dag_route_ack(
    outcome: route_schemas.StreamDAGRouteOutcome,
    request: route_schemas.StreamDAGSourceAppendRequest
    | route_schemas.StreamDAGConsumerCompleteRequest,
) -> route_schemas.StreamDAGRouteOutcome:
    """Reject a valid-looking receipt for another graph, producer or input."""
    matches = [
        outcome.graph_revision_id == request.context.graph_revision_id,
        outcome.routing_digest == request.context.routing_digest,
    ]
    if isinstance(request, route_schemas.StreamDAGSourceAppendRequest):
        matches.extend(
            [
                outcome.producer_role == "SOURCE",
                outcome.kind == "YIELD",
                outcome.producer_stream_id == request.binding.stream_id,
                outcome.producer_position == request.position,
            ]
        )
    else:
        matches.extend(
            [
                outcome.producer_role == "OPERATOR",
                outcome.kind == request.kind,
                outcome.producer_stream_id == request.binding.output_stream_id,
                outcome.producer_position == request.token.sequence,
                outcome.input_stream_id == request.binding.input_stream_id,
                outcome.input_record_id == request.token.record_id,
                outcome.input_sequence == request.token.sequence,
                outcome.consumer_id == request.binding.consumer_id,
            ]
        )
    if not all(matches):
        raise StreamRequestError(
            "IDENTITY_CONFLICT", "Routed receipt does not match its frozen request"
        )
    return outcome


def _validate_subtask_bootstrap(
    bootstrap: subtask_schemas.StreamSubtaskBootstrap, subtask_id: UUID
) -> subtask_schemas.StreamSubtaskBootstrap:
    """Check the reply against its sealed assignment and process-local grant."""
    grant, assignment, task = bootstrap.grant, bootstrap.assignment, bootstrap.task_run
    manifest = assignment.manifest
    expected = (
        grant.subtask_id == assignment.id == subtask_id,
        grant.graph_revision_id == assignment.graph_revision_id,
        grant.assignment_epoch == assignment.assignment_epoch,
        grant.id == assignment.current_grant_id,
        grant.physical_epoch == assignment.physical_epoch,
        grant.state == "REGISTERED",
        bootstrap.flow_run_id == manifest.flow_run_id == task.flow_run_id,
        task.id == manifest.task_run_id,
        task.dynamic_key == manifest.dynamic_key,
        assignment.input_stream_id != manifest.stream_id,
        (manifest.role == "SOURCE") == (assignment.input_stream_id is None),
        (manifest.role == "SOURCE") == (bootstrap.input_key is None),
    )
    execution = get_stream_subtask_execution()
    if execution is not None:
        expected += (
            bootstrap.flow_run_id == execution.flow_run_id,
            grant.graph_revision_id == execution.graph_revision_id,
            grant.assignment_epoch == execution.assignment_epoch,
            grant.subtask_id == execution.subtask_id,
            grant.id == execution.grant_id,
            grant.physical_epoch == execution.physical_epoch,
            grant.holder_id == execution.holder_id,
            grant.event_key_id == execution.event_key_id,
            grant.child_process_id == execution.child_process_id,
            grant.process_start_ticks == execution.process_start_ticks,
            grant.scope_digest == execution.scope_digest,
            manifest.manifest_digest == execution.manifest_digest,
            task.id == execution.task_run_id,
            assignment.input_stream_id == execution.input_stream_id,
            manifest.stream_id == execution.output_stream_id,
        )
    if not all(expected):
        raise StreamRequestError(
            "IDENTITY_CONFLICT", "Physical bootstrap does not match its sealed grant"
        )
    return bootstrap


def _subtask_event_payload(event: dict[str, Any], subtask_id: UUID) -> dict[str, Any]:
    """Transport only an exact own-TaskRun signature under a physical context."""
    parsed = Event.model_validate(event)
    execution = get_stream_subtask_execution()
    if execution is not None:
        expected = {
            "prefect.resource.id": f"prefect.task-run.{execution.task_run_id}",
            "prefect.stream-graph-revision": str(execution.graph_revision_id),
            "prefect.stream-assignment-epoch": str(execution.assignment_epoch),
            "prefect.stream-subtask": str(execution.subtask_id),
            "prefect.stream-subtask-grant": str(execution.grant_id),
            "prefect.stream-physical-epoch": str(execution.physical_epoch),
            "prefect.stream-scope-digest": execution.scope_digest,
            "prefect.stream-execution-generation": str(execution.physical_epoch),
            "prefect.stream-execution-holder": str(execution.holder_id),
            "prefect.stream-submission-digest": execution.manifest_digest,
            "prefect.stream-event-key-id": execution.event_key_id,
        }
        if subtask_id != execution.subtask_id or any(
            parsed.resource.get(name) != value for name, value in expected.items()
        ):
            raise StreamRequestError(
                "IDENTITY_CONFLICT", "Physical event identity differs"
            )
        try:
            task_run = parsed.payload["task_run"]
            witness = canonical_physical_event_witness(
                event_id=parsed.id,
                event=parsed.event,
                occurred=parsed.occurred,
                flow_run_id=execution.flow_run_id,
                task_run_id=execution.task_run_id,
                generation=execution.physical_epoch,
                holder_id=execution.holder_id,
                manifest_digest=execution.manifest_digest,
                run_count=task_run["run_count"],
                task_run=task_run,
                validated_state=parsed.payload["validated_state"],
                graph_revision_id=execution.graph_revision_id,
                assignment_epoch=execution.assignment_epoch,
                subtask_id=execution.subtask_id,
                grant_id=execution.grant_id,
                physical_epoch=execution.physical_epoch,
                scope_digest=execution.scope_digest,
            )
            valid = verify_task_event(
                execution.event_public_key,
                witness,
                parsed.resource.get("prefect.stream-event-signature"),
            )
        except (KeyError, TypeError, ValueError):
            valid = False
        if not valid:
            raise StreamRequestError(
                "PROTOCOL_ERROR", "Physical event signature is invalid"
            )
    return parsed.model_dump(mode="json")


def _subtask_event_result(
    response: httpx.Response, payload: dict[str, Any]
) -> subtask_schemas.StreamSubtaskEventResult:
    result = subtask_schemas.StreamSubtaskEventResult.model_validate(response.json())
    execution = get_stream_subtask_execution()
    if str(result.event_id) != payload["id"] or (
        execution is not None and result.task_run_id != execution.task_run_id
    ):
        raise StreamRequestError("IDENTITY_CONFLICT", "Physical event receipt differs")
    return result


def _dag_lifecycle_ack(result: object, expected: dict[str, object]) -> None:
    """Check immutable identities without confusing current progress with the ACK."""
    for path, value in expected.items():
        actual = result
        for field in path.split("."):
            actual = getattr(actual, field)
        if actual != value:
            raise StreamRequestError(
                "IDENTITY_CONFLICT", "DAG response does not match its request"
            )


def _dag_child_ack(
    result: object,
    *,
    subtask_id: UUID | None = None,
    intent: object = None,
    stream_role: Literal["input", "output"] | None = None,
) -> None:
    """Bind scoped responses to the current grant; Task attempts are not epochs."""
    execution = get_stream_subtask_execution()
    if not isinstance(execution, DAGSubtaskExecutionContext):
        return
    expected: dict[str, object] = {}
    if isinstance(result, dag_execution_schemas.StreamDAGSubtaskBootstrap):
        expected = {
            "flow_run_id": execution.flow_run_id,
            "assignment.id": execution.subtask_id,
            "assignment.graph_revision_id": execution.graph_revision_id,
            "assignment.current_grant_id": execution.grant_id,
            "assignment.physical_epoch": execution.physical_epoch,
            "assignment.assignment_epoch": execution.assignment_epoch,
            "assignment.input_stream_id": execution.input_stream_id,
            "assignment.manifest.stream_id": execution.output_stream_id,
            "assignment.manifest.manifest_digest": execution.manifest_digest,
            "grant.id": execution.grant_id,
            "grant.holder_id": execution.holder_id,
            "grant.scope_digest": execution.scope_digest,
            "grant.event_key_id": execution.event_key_id,
            "grant.child_process_id": execution.child_process_id,
            "grant.process_start_ticks": execution.process_start_ticks,
            "task_run.id": execution.task_run_id,
        }
    elif isinstance(
        result,
        (
            dag_execution_schemas.StreamDAGBootstrapReadyResult,
            dag_execution_schemas.StreamDAGActivationObservation,
        ),
    ):
        expected = {
            "graph_revision_id": execution.graph_revision_id,
            "subtask_id": execution.subtask_id,
            "grant_id": execution.grant_id,
            "physical_epoch": execution.physical_epoch,
        }
    elif isinstance(result, schemas.SourceAdmission):
        expected = {
            "binding.flow_run_id": execution.flow_run_id,
            "binding.stream_id": execution.output_stream_id,
            "binding.task_run_id": execution.task_run_id,
            "binding.owner_id": execution.holder_id,
        }
    elif isinstance(result, schemas.ConsumerBinding):
        expected = {
            "flow_run_id": execution.flow_run_id,
            "input_stream_id": execution.input_stream_id,
            "output_stream_id": execution.output_stream_id,
            "owner_id": execution.holder_id,
        }
    elif isinstance(result, schemas.StreamInfo):
        expected = {"flow_run_id": execution.flow_run_id}
        if stream_role is not None:
            expected["id"] = (
                execution.input_stream_id
                if stream_role == "input"
                else execution.output_stream_id
            )
        if result.id not in {execution.input_stream_id, execution.output_stream_id}:
            raise StreamRequestError(
                "IDENTITY_CONFLICT", "DAG stream is outside its assignment"
            )
    elif isinstance(result, schemas.StreamDelivery) and isinstance(
        intent, dag_execution_schemas.StreamDAGConsumerReadRequest
    ):
        expected = {
            "token.flow_run_id": intent.binding.flow_run_id,
            "token.input_stream_id": intent.binding.input_stream_id,
            "token.output_stream_id": intent.binding.output_stream_id,
            "token.consumer_id": intent.binding.consumer_id,
            "token.owner_id": intent.binding.owner_id,
            "token.owner_epoch": intent.binding.owner_epoch,
            "token.attempt": intent.binding.attempt,
        }
    if subtask_id is not None and subtask_id != execution.subtask_id:
        raise StreamRequestError(
            "IDENTITY_CONFLICT", "DAG response targets another subtask"
        )
    _dag_lifecycle_ack(result, expected)


class StreamClient(BaseClient):
    """Native data-plane methods; credentials belong to the owning client."""

    def start_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGStartRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/start",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "START",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    def activate_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGActivateRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/activate",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "ACTIVATE",
                "execution.activation_operation_id": intent.operation_id,
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    def abort_stream_dag_preview(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGAbortPreviewRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/abort-preview",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "ABORT_PREVIEW",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    def cancel_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
        reason: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGCancelRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
                "reason": reason,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/cancel",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "CANCEL",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    def fail_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
        failure_kind: Literal[
            "FLOW_SCOPE_EXCEPTION",
            "START_EXCEPTION",
            "START_TIMEOUT",
            "COORDINATOR_ERROR",
        ],
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGFailRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
                "failure_kind": failure_kind,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/fail",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "FAIL",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    def read_stream_dag_execution(
        self,
        *,
        graph_revision_id: UUID,
    ) -> dag_execution_schemas.StreamDAGExecution:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = self._stream_request(
            "GET",
            "/streams/v2/dags/{graph_id}/execution",
            path_params={"graph_id": graph_revision_id},
        )
        result = dag_execution_schemas.StreamDAGExecution.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(result, {"graph_revision_id": graph_revision_id})
        return result

    def read_stream_dag_subtask_bootstrap(
        self,
        *,
        subtask_id: UUID,
    ) -> dag_execution_schemas.StreamDAGSubtaskBootstrap:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/bootstrap",
            path_params={"subtask_id": subtask_id},
        )
        result = dag_execution_schemas.StreamDAGSubtaskBootstrap.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(result, {"assignment.id": subtask_id})
        _dag_child_ack(result, subtask_id=subtask_id)
        return result

    def read_stream_dag_subtask_activation(
        self,
        *,
        subtask_id: UUID,
    ) -> dag_execution_schemas.StreamDAGActivationObservation:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/activation",
            path_params={"subtask_id": subtask_id},
        )
        result = dag_execution_schemas.StreamDAGActivationObservation.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(result, {"subtask_id": subtask_id})
        _dag_child_ack(result, subtask_id=subtask_id)
        return result

    def mark_stream_dag_subtask_bootstrap_ready(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        bootstrap_digest: str,
        signature: str,
    ) -> dag_execution_schemas.StreamDAGBootstrapReadyResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGBootstrapReadyRequest.model_validate(
            {
                "operation_id": operation_id,
                "bootstrap_digest": bootstrap_digest,
                "signature": signature,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/subtasks/{subtask_id}/bootstrap-ready",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGBootstrapReadyResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "bootstrap_digest": intent.bootstrap_digest,
                "subtask_id": subtask_id,
            },
        )
        _dag_child_ack(result, subtask_id=subtask_id, intent=intent)
        return result

    def read_stream_dag_input_status(
        self,
        *,
        subtask_id: UUID,
    ) -> schemas.StreamInfo:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/input/status",
            path_params={"subtask_id": subtask_id},
        )
        result = schemas.StreamInfo.model_validate(response.json())
        _dag_child_ack(result, subtask_id=subtask_id, stream_role="input")
        return result

    def read_stream_dag_output_status(
        self,
        *,
        subtask_id: UUID,
    ) -> schemas.StreamInfo:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/output/status",
            path_params={"subtask_id": subtask_id},
        )
        result = schemas.StreamInfo.model_validate(response.json())
        _dag_child_ack(result, subtask_id=subtask_id, stream_role="output")
        return result

    def open_stream_dag_source(
        self,
        *,
        context: dict[str, Any],
    ) -> schemas.SourceAdmission:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGSourceOpenRequest.model_validate(
            {"context": context}
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/sources/open",
            json=payload,
        )
        result = schemas.SourceAdmission.model_validate(response.json())
        _dag_child_ack(result, intent=intent)
        return result

    def open_stream_dag_consumer(
        self,
        *,
        context: dict[str, Any],
    ) -> schemas.ConsumerBinding:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGConsumerOpenRequest.model_validate(
            {"context": context}
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/open",
            json=payload,
        )
        result = schemas.ConsumerBinding.model_validate(response.json())
        _dag_child_ack(result, intent=intent)
        return result

    def read_stream_dag_consumer(
        self,
        *,
        context: dict[str, Any],
        binding: dict[str, Any],
    ) -> schemas.StreamDelivery | None:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGConsumerReadRequest.model_validate(
            {"context": context, "binding": binding}
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/read",
            json=payload,
        )
        if response.json() is None:
            return None
        result = schemas.StreamDelivery.model_validate(response.json())
        _dag_child_ack(result, intent=intent)
        return result

    def finish_stream_dag_source(
        self,
        *,
        operation_id: UUID,
        context: dict[str, Any],
        binding: dict[str, Any],
        expected_position: int,
    ) -> dag_execution_schemas.StreamDAGProducerFinishReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGSourceFinishRequest.model_validate(
            {
                "operation_id": operation_id,
                "context": context,
                "binding": binding,
                "expected_position": expected_position,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/sources/finish",
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGProducerFinishReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "graph_revision_id": intent.context.graph_revision_id,
                "producer_subtask_id": intent.context.producer_subtask_id,
                "producer_stream_id": intent.binding.stream_id,
                "kind": "SOURCE_EOF",
                "accepted_source_position": intent.expected_position,
            },
        )
        _dag_child_ack(result, intent=intent)
        return result

    def finish_stream_dag_consumer(
        self,
        *,
        operation_id: UUID,
        context: dict[str, Any],
        binding: dict[str, Any],
    ) -> dag_execution_schemas.StreamDAGProducerFinishReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGConsumerFinishRequest.model_validate(
            {"operation_id": operation_id, "context": context, "binding": binding}
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/finish",
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGProducerFinishReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "graph_revision_id": intent.context.graph_revision_id,
                "producer_subtask_id": intent.context.producer_subtask_id,
                "producer_stream_id": intent.binding.output_stream_id,
                "kind": "CONSUMER_EOF",
            },
        )
        _dag_child_ack(result, intent=intent)
        return result

    def poll_worker_stream_dag_subtasks(
        self,
        *,
        worker_id: UUID,
        principal: dict[str, Any],
        active_subtask_ids: list[UUID] | None = None,
        limit: int = 64,
    ) -> list[dag_execution_schemas.StreamDAGWorkerAssignment]:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGWorkerPollRequest.model_validate(
            {
                "principal": principal,
                "active_subtask_ids": []
                if active_subtask_ids is None
                else active_subtask_ids,
                "limit": limit,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/poll",
            path_params={"worker_id": worker_id},
            json=payload,
        )
        result = TypeAdapter(
            list[dag_execution_schemas.StreamDAGWorkerAssignment]
        ).validate_python(response.json())
        if len({row.assignment.id for row in result}) != len(result) or any(
            row.assignment.assigned_worker_id != worker_id for row in result
        ):
            raise StreamRequestError(
                "IDENTITY_CONFLICT",
                "DAG worker poll contains a foreign or repeated assignment",
            )
        return result

    def abort_worker_stream_dag_startup(
        self,
        *,
        worker_id: UUID,
        graph_revision_id: UUID,
        principal: dict[str, Any],
        start_operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGWorkerStartupAbortResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = (
            dag_execution_schemas.StreamDAGWorkerStartupAbortRequest.model_validate(
                {
                    "principal": principal,
                    "start_operation_id": start_operation_id,
                    "graph_digest": graph_digest,
                    "routing_digest": routing_digest,
                }
            )
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/graphs/{graph_id}/startup-abort",
            path_params={"worker_id": worker_id, "graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGWorkerStartupAbortResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    def prepare_worker_stream_dag_launch(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        start_operation_id: UUID,
        replacement_operation_id: UUID | None = None,
    ) -> dag_execution_schemas.StreamDAGLaunchOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGLaunchPrepareRequest.model_validate(
            {
                "principal": principal,
                "operation_id": operation_id,
                "start_operation_id": start_operation_id,
                "replacement_operation_id": replacement_operation_id,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/prepare",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGLaunchOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.kind": "PREPARE",
                "current.subtask_id": subtask_id,
                "current.worker_id": worker_id,
                "current.start_operation_id": intent.start_operation_id,
                "current.replacement_operation_id": intent.replacement_operation_id,
            },
        )
        return result

    def record_worker_stream_dag_launch(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        launch_attempt_id: UUID,
        child_process_id: int,
        process_start_ticks: int,
        local_journal_digest: str,
    ) -> dag_execution_schemas.StreamDAGLaunchOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGLaunchRecordRequest.model_validate(
            {
                "principal": principal,
                "operation_id": operation_id,
                "child_process_id": child_process_id,
                "process_start_ticks": process_start_ticks,
                "local_journal_digest": local_journal_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/record",
            path_params={
                "worker_id": worker_id,
                "subtask_id": subtask_id,
                "launch_attempt_id": launch_attempt_id,
            },
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGLaunchOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.kind": "RECORD",
                "current.id": launch_attempt_id,
                "current.subtask_id": subtask_id,
                "current.worker_id": worker_id,
                "current.child_process_id": intent.child_process_id,
                "current.process_start_ticks": intent.process_start_ticks,
                "current.local_journal_digest": intent.local_journal_digest,
            },
        )
        return result

    def close_worker_stream_dag_launch(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        launch_attempt_id: UUID,
        proof: dict[str, Any],
    ) -> dag_execution_schemas.StreamDAGLaunchCloseReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGLaunchCloseRequest.model_validate(
            {"principal": principal, "operation_id": operation_id, "proof": proof}
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/close",
            path_params={
                "worker_id": worker_id,
                "subtask_id": subtask_id,
                "launch_attempt_id": launch_attempt_id,
            },
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGLaunchCloseReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "launch_attempt_id": launch_attempt_id,
                "subtask_id": subtask_id,
                "proof_kind": intent.proof.kind,
            },
        )
        return result

    def stop_worker_stream_dag_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        control_operation_id: UUID,
    ) -> dag_execution_schemas.StreamDAGStopReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGWorkerStopRequest.model_validate(
            {
                "principal": principal,
                "operation_id": operation_id,
                "control_operation_id": control_operation_id,
            }
        )
        payload = intent.model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/stop",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGStopReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "control_operation_id": intent.control_operation_id,
                "subtask_id": subtask_id,
            },
        )
        return result

    def _stream_request(
        self, method: HTTP_METHODS, path: ServerRoutes, **kwargs: Any
    ) -> httpx.Response:
        try:
            return self.request(method, path, **kwargs)
        except httpx.HTTPStatusError as error:
            raise _refusal(error) from error

    def poll_worker_stream_subtasks(
        self,
        *,
        worker_id: UUID,
        principal: dict[str, Any],
        active_subtask_ids: list[UUID] | None = None,
        limit: int = 64,
    ) -> list[subtask_schemas.StreamWorkerAssignment]:
        """Poll persisted placements through the trusted worker's Basic authority."""
        payload = subtask_schemas.StreamWorkerPollRequest.model_validate(
            {
                "principal": principal,
                "active_subtask_ids": active_subtask_ids
                if active_subtask_ids is not None
                else [],
                "limit": limit,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/poll",
            path_params={"worker_id": worker_id},
            json=payload,
        )
        return TypeAdapter(
            list[subtask_schemas.StreamWorkerAssignment]
        ).validate_python(response.json())

    def claim_worker_stream_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        holder_id: UUID,
        capability: str,
        event_public_key: str,
        work_pool_id: UUID,
        host_id: UUID,
        boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        assignment_epoch: Literal[1] = 1,
        expected_physical_epoch: Literal[0] = 0,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Claim exactly this worker's assigned subtask, without a Flow capability."""
        payload = subtask_schemas.StreamSubtaskClaimRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "assignment_epoch": assignment_epoch,
                "expected_physical_epoch": expected_physical_epoch,
                "holder_id": holder_id,
                "capability": capability,
                "event_public_key": event_public_key,
                "launcher_binding_kind": "ASSIGNED_WORKER",
                "worker_id": worker_id,
                "work_pool_id": work_pool_id,
                "host_id": host_id,
                "boot_id": boot_id,
                "launcher_process_id": launcher_process_id,
                "launcher_start_ticks": launcher_start_ticks,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/claim",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def register_worker_stream_subtask_process(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        registration: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerRegistrationRequest.model_validate(
            {"principal": principal, "registration": registration}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/register",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def renew_worker_stream_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        grant_id: UUID,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerRenewRequest.model_validate(
            {"principal": principal, "grant_id": grant_id}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/renew",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def authorize_worker_stream_subtask_replacement(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        replacement: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskReplacementAuthorization:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = (
            subtask_schemas.StreamWorkerReplacementAuthorizeRequest.model_validate(
                {"principal": principal, "replacement": replacement}
            ).model_dump(mode="json")
        )
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/replacements/authorize",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskReplacementAuthorization.model_validate(
            response.json()
        )

    def confirm_worker_stream_subtask_replacement(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        replacement: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerReplacementConfirmRequest.model_validate(
            {"principal": principal, "replacement": replacement}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/replacements/confirm",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def retire_verified_worker_stream_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        retirement: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskVerifiedRetirement:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerVerifiedRetireRequest.model_validate(
            {"principal": principal, "retirement": retirement}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/replacements/retire",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskVerifiedRetirement.model_validate(
            response.json()
        )

    def acknowledge_worker_stream_subtask_startup_stop(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        stop: dict[str, Any],
    ) -> subtask_schemas.StreamGraphStartupStopResult:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerStartupStopRequest.model_validate(
            {"principal": principal, "stop": stop}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/startup-stop",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphStartupStopResult.model_validate(
            response.json()
        )

    def abort_worker_stream_graph_startup(
        self,
        *,
        worker_id: UUID,
        graph_revision_id: UUID,
        principal: dict[str, Any],
    ) -> subtask_schemas.StreamGraphPlan:
        """Request the versioned startup abort; worker stop receipts release slots."""
        payload = subtask_schemas.StreamWorkerStartupAbortRequest.model_validate(
            {"principal": principal}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/graphs/{graph_id}/startup-abort",
            path_params={"worker_id": worker_id, "graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    def acknowledge_stream_subtask_startup_stop(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        abort_operation_id: UUID,
        grant_id: UUID,
        expected_physical_epoch: int,
        death_witness: dict[str, Any] | None = None,
        unstarted: bool = False,
    ) -> subtask_schemas.StreamGraphStartupStopResult:
        """Record one exact preactivation stop through the parent Flow authority."""
        payload = subtask_schemas.StreamGraphStartupStopRequest.model_validate(
            {
                "operation_id": operation_id,
                "abort_operation_id": abort_operation_id,
                "grant_id": grant_id,
                "expected_physical_epoch": expected_physical_epoch,
                "death_witness": death_witness,
                "unstarted": unstarted,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/startup-stop",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphStartupStopResult.model_validate(
            response.json()
        )

    def append_stream_dag_source(
        self,
        *,
        context: dict[str, Any],
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        position: int,
        value: JsonValue,
        output_key: dict[str, Any] | None = None,
    ) -> route_schemas.StreamDAGRouteOutcome:
        """Send a separately versioned DAG source intent; execution stays fenced."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        intent = route_schemas.StreamDAGSourceAppendRequest.model_validate(
            {
                "context": context,
                "binding": binding,
                "position": position,
                "value": value,
                "output_key": output_key,
            }
        )
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/sources/append",
            json=intent.model_dump(mode="json"),
        )
        return _validate_dag_route_ack(
            route_schemas.StreamDAGRouteOutcome.model_validate(response.json()), intent
        )

    def complete_stream_dag_consumer(
        self,
        *,
        context: dict[str, Any],
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        record_id: UUID,
        sequence: int,
        delivery_id: UUID,
        kind: Literal["YIELD", "RETURN"],
        value: JsonValue,
        managed_state: dict[str, JsonValue],
        output_key: dict[str, Any] | None = None,
    ) -> route_schemas.StreamDAGRouteOutcome:
        """Send one frozen DAG terminal intent without changing legacy bodies."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        token = schemas.DeliveryToken(
            **binding.model_dump(),
            record_id=record_id,
            sequence=sequence,
            delivery_id=delivery_id,
        )
        intent = route_schemas.StreamDAGConsumerCompleteRequest.model_validate(
            {
                "context": context,
                "binding": binding,
                "token": token,
                "kind": kind,
                "value": value,
                "managed_state": managed_state,
                "output_key": output_key,
            }
        )
        response = self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/complete",
            json=intent.model_dump(mode="json"),
        )
        return _validate_dag_route_ack(
            route_schemas.StreamDAGRouteOutcome.model_validate(response.json()), intent
        )

    def seal_stream_dag(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        flow_version: str,
        operators: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        working_directory: str | None = None,
        profile: Literal["partitioned-dag-v1"] = "partitioned-dag-v1",
        assignment_epoch: Literal[1] = 1,
    ) -> dag_schemas.StreamDAGPlan:
        """Seal one complete DAG metadata declaration without starting tasks."""
        payload = dag_schemas.StreamDAGSealRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "flow_version": flow_version,
                "operators": operators,
                "edges": edges,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "working_directory": working_directory,
                "profile": profile,
                "assignment_epoch": assignment_epoch,
            }
        ).model_dump(mode="json")
        response = self._stream_request("POST", "/streams/v2/dags/seal", json=payload)
        return dag_schemas.StreamDAGPlan.model_validate(response.json())

    def read_stream_dag(self, *, graph_revision_id: UUID) -> dag_schemas.StreamDAGPlan:
        """Read sealed DAG metadata through its parent Flow execution authority."""
        response = self._stream_request(
            "GET",
            "/streams/v2/dags/{graph_id}",
            path_params={"graph_id": graph_revision_id},
        )
        return dag_schemas.StreamDAGPlan.model_validate(response.json())

    def seal_stream_graph(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        flow_version: str,
        operators: list[dict[str, Any]],
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        working_directory: str | None = None,
        profile: Literal["partitioned-fixed-v1"] = "partitioned-fixed-v1",
        assignment_epoch: Literal[1] = 1,
    ) -> subtask_schemas.StreamGraphPlan:
        """Seal the immutable finite P2 graph before any physical task starts."""
        payload = subtask_schemas.StreamGraphSealRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "flow_version": flow_version,
                "operators": operators,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "working_directory": working_directory,
                "profile": profile,
                "assignment_epoch": assignment_epoch,
            }
        ).model_dump(mode="json")
        response = self._stream_request("POST", "/streams/v2/graphs/seal", json=payload)
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    def read_stream_graph(
        self, *, graph_revision_id: UUID
    ) -> subtask_schemas.StreamGraphPlan:
        """Read a graph through its parent execution authority."""
        response = self._stream_request(
            "GET",
            "/streams/v2/graphs/{graph_id}",
            path_params={"graph_id": graph_revision_id},
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    def admit_stream_region(
        self, *, graph_revision_id: UUID, operation_id: UUID, graph_digest: str
    ) -> subtask_schemas.StreamGraphPlan:
        """Reserve the complete runnable region before source admission."""
        payload = subtask_schemas.StreamRegionAdmissionRequest(
            operation_id=operation_id, graph_digest=graph_digest
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/graphs/{graph_id}/admit",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    def activate_stream_graph(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        grant_ids: list[UUID],
    ) -> subtask_schemas.StreamGraphPlan:
        """Activate the exact four registered grants before releasing child user code."""
        payload = subtask_schemas.StreamGraphActivateRequest(
            operation_id=operation_id, graph_digest=graph_digest, grant_ids=grant_ids
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/graphs/{graph_id}/activate",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    def abort_stream_graph(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        error: str,
    ) -> subtask_schemas.StreamGraphPlan:
        """Compensate a preactivation failure through its retained graph identity."""
        payload = subtask_schemas.StreamGraphAbortRequest(
            operation_id=operation_id, graph_digest=graph_digest, error=error
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/graphs/{graph_id}/abort",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    def claim_stream_subtask(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        holder_id: UUID,
        capability: str,
        event_public_key: str,
        launcher_binding_kind: Literal[
            "DEPLOYMENT", "FLOW_AUTHORITY_LOCAL", "ASSIGNED_WORKER"
        ],
        host_id: UUID,
        boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        worker_id: UUID | None = None,
        work_pool_id: UUID | None = None,
        assignment_epoch: Literal[1] = 1,
        expected_physical_epoch: Literal[0] = 0,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Bind caller-generated child credentials without returning the secret."""
        payload = subtask_schemas.StreamSubtaskClaimRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "assignment_epoch": assignment_epoch,
                "expected_physical_epoch": expected_physical_epoch,
                "holder_id": holder_id,
                "capability": capability,
                "event_public_key": event_public_key,
                "launcher_binding_kind": launcher_binding_kind,
                "worker_id": worker_id,
                "work_pool_id": work_pool_id,
                "host_id": host_id,
                "boot_id": boot_id,
                "launcher_process_id": launcher_process_id,
                "launcher_start_ticks": launcher_start_ticks,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/claim",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def register_stream_subtask_process(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        grant_id: UUID,
        child_process_id: int,
        process_start_ticks: int,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Register an exact child before delivering its capability bootstrap."""
        payload = subtask_schemas.StreamSubtaskRegistrationRequest.model_validate(
            {
                "operation_id": operation_id,
                "grant_id": grant_id,
                "child_process_id": child_process_id,
                "process_start_ticks": process_start_ticks,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/register",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def renew_stream_subtask(
        self, *, subtask_id: UUID, grant_id: UUID
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Renew parent supervision without asserting kernel process liveness."""
        payload = subtask_schemas.StreamSubtaskRenewRequest(
            grant_id=grant_id
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/renew",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def authorize_stream_subtask_replacement(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        expected_grant_id: UUID,
        expected_physical_epoch: int,
        host_id: UUID,
        challenge_capability: str,
    ) -> subtask_schemas.StreamSubtaskReplacementAuthorization:
        """Authorize verification for one exact physical attempt."""
        payload = (
            subtask_schemas.StreamSubtaskReplacementAuthorizeRequest.model_validate(
                {
                    "operation_id": operation_id,
                    "expected_grant_id": expected_grant_id,
                    "expected_physical_epoch": expected_physical_epoch,
                    "host_id": host_id,
                    "challenge_capability": challenge_capability,
                }
            ).model_dump(mode="json")
        )
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/replacements/authorize",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskReplacementAuthorization.model_validate(
            response.json()
        )

    def confirm_stream_subtask_replacement(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        challenge_capability: str,
        death_witness: dict[str, Any],
        new_holder_id: UUID,
        new_capability: str,
        new_event_public_key: str,
        host_id: UUID,
        boot_id: UUID,
        new_child_process_id: int,
        new_process_start_ticks: int,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Fence one dead physical attempt while retaining its TaskRun identity."""
        payload = subtask_schemas.StreamSubtaskReplacementConfirmRequest.model_validate(
            {
                "operation_id": operation_id,
                "challenge_capability": challenge_capability,
                "death_witness": death_witness,
                "new_holder_id": new_holder_id,
                "new_capability": new_capability,
                "new_event_public_key": new_event_public_key,
                "host_id": host_id,
                "boot_id": boot_id,
                "new_child_process_id": new_child_process_id,
                "new_process_start_ticks": new_process_start_ticks,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/replacements/confirm",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    def retire_verified_stream_subtask(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        challenge_capability: str,
        death_witness: dict[str, Any],
        error: str,
        reason: Literal[
            "TERMINAL_RECONCILIATION", "REPLACEMENT_PREFLIGHT_FAILED"
        ] = "TERMINAL_RECONCILIATION",
    ) -> subtask_schemas.StreamSubtaskVerifiedRetirement:
        """Reconcile a verified terminal attempt without rerunning accepted work."""
        payload = subtask_schemas.StreamSubtaskVerifiedRetireRequest.model_validate(
            {
                "operation_id": operation_id,
                "challenge_capability": challenge_capability,
                "death_witness": death_witness,
                "error": error,
                "reason": reason,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/replacements/retire",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        result = subtask_schemas.StreamSubtaskVerifiedRetirement.model_validate(
            response.json()
        )
        if result.operation_id != operation_id or result.subtask_id != subtask_id:
            raise StreamRequestError(
                "IDENTITY_CONFLICT", "Physical retirement receipt differs"
            )
        return result

    def read_stream_subtask_bootstrap(
        self, *, subtask_id: UUID
    ) -> subtask_schemas.StreamSubtaskBootstrap:
        """Read only this grant's immutable assignment and ordinary TaskRun."""
        response = self._stream_request(
            "GET",
            "/streams/v2/subtasks/{subtask_id}/bootstrap",
            path_params={"subtask_id": subtask_id},
        )
        return _validate_subtask_bootstrap(
            subtask_schemas.StreamSubtaskBootstrap.model_validate(response.json()),
            subtask_id,
        )

    def read_stream_subtask_task_run(self, *, subtask_id: UUID) -> TaskRun:
        """Read this physical TaskRun without access to ordinary run APIs."""
        response = self._stream_request(
            "GET",
            "/streams/v2/subtasks/{subtask_id}/task-run",
            path_params={"subtask_id": subtask_id},
        )
        return TaskRun.model_validate(response.json())

    def emit_stream_subtask_event(
        self, *, subtask_id: UUID, event: dict[str, Any]
    ) -> subtask_schemas.StreamSubtaskEventResult:
        """Deliver an ordinary signed TaskRun event through scoped HTTP."""
        payload = _subtask_event_payload(event, subtask_id)
        response = self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/events",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return _subtask_event_result(response, payload)

    def renew_stream_subtask_lease(self, *, subtask_id: UUID) -> None:
        """Renew this subtask's pre-admitted task/tag lease."""
        self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/lease/renew",
            path_params={"subtask_id": subtask_id},
            json={},
        )

    def release_stream_subtask_lease(self, *, subtask_id: UUID) -> None:
        """Release this subtask's lease after its accepted terminal state."""
        self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/lease/release",
            path_params={"subtask_id": subtask_id},
            json={},
        )

    def reserve_stream_finite_submission(
        self,
        *,
        flow_run_id: UUID,
        phase: Literal["PREFIX", "TAIL"],
        submission_key: str,
        task_name: str,
        task_key: str,
        artifact_digest: str,
        task_version: str,
        flow_version: str,
        parameters: dict[str, JsonValue],
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        task_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        labels: dict[str, bool | int | float | str] | None = None,
    ) -> schemas.StreamFiniteManifest:
        """Reserve an ordinary TaskRun in the fenced finite prefix/tail plan."""
        payload = schemas.StreamFiniteReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "phase": phase,
                "submission_key": submission_key,
                "task_name": task_name,
                "task_key": task_key,
                "artifact_digest": artifact_digest,
                "task_version": task_version,
                "flow_version": flow_version,
                "parameters": parameters,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "task_policy": task_policy or {},
                "tags": tags or [],
                "labels": labels or {},
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/finite/reserve", json=payload
        )
        return schemas.StreamFiniteManifest.model_validate(response.json())

    def read_stream_finite_submission(
        self, *, flow_run_id: UUID, submission_key: str
    ) -> schemas.StreamFiniteManifest:
        """Read a finite binding without admitting or changing its task."""
        payload = schemas.StreamFiniteLookupRequest(
            flow_run_id=flow_run_id, submission_key=submission_key
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/finite/lookup", json=payload
        )
        return schemas.StreamFiniteManifest.model_validate(response.json())

    def complete_stream_finite_task(
        self,
        *,
        flow_run_id: UUID,
        task_run_id: UUID,
        operation_id: UUID,
        run_count: int,
        result: JsonValue,
    ) -> schemas.StreamFiniteReceipt:
        """Persist the bounded finite result before normal TaskRun success."""
        payload = schemas.StreamFiniteCompleteRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "task_run_id": task_run_id,
                "operation_id": operation_id,
                "run_count": run_count,
                "result": result,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/finite/complete", json=payload
        )
        return schemas.StreamFiniteReceipt.model_validate(response.json())

    def enroll_native_stream_deployment(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        declaration_digest: str,
        worker_id: UUID,
        work_pool_id: UUID,
        launcher_instance_id: UUID,
        launcher_host_id: UUID,
        launcher_boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        holder_id: UUID,
        holder_capability: str,
        holder_event_public_key: str,
        supervisor_id: UUID,
        supervisor_capability: str,
        launch_profile: Literal["linux-local-pidfd-v1"] = "linux-local-pidfd-v1",
        expected_generation: Literal[0] = 0,
    ) -> native_schemas.NativeStreamWorkerEnrollment:
        """Enroll a trusted worker before launching the declared Flow."""
        payload = native_schemas.NativeStreamWorkerEnrollRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            declaration_digest=declaration_digest,
            worker_id=worker_id,
            work_pool_id=work_pool_id,
            launcher_instance_id=launcher_instance_id,
            launcher_host_id=launcher_host_id,
            launcher_boot_id=launcher_boot_id,
            launcher_process_id=launcher_process_id,
            launcher_start_ticks=launcher_start_ticks,
            holder_id=holder_id,
            holder_capability=holder_capability,
            holder_event_public_key=holder_event_public_key,
            supervisor_id=supervisor_id,
            supervisor_capability=supervisor_capability,
            launch_profile=launch_profile,
            expected_generation=expected_generation,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/enroll", json=payload
        )
        return native_schemas.NativeStreamWorkerEnrollment.model_validate(
            response.json()
        )

    def register_native_stream_process(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        enrollment_operation_id: UUID,
        launcher_instance_id: UUID,
        host_id: UUID,
        boot_id: UUID,
        child_process_id: int,
        process_start_ticks: int,
        launch_nonce_digest: str,
        generation: int = 1,
    ) -> native_schemas.NativeStreamLaunchProjection:
        """Bind the blocked child identity before delivering its credentials."""
        payload = native_schemas.NativeStreamProcessRegistrationRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            enrollment_operation_id=enrollment_operation_id,
            launcher_instance_id=launcher_instance_id,
            host_id=host_id,
            boot_id=boot_id,
            child_process_id=child_process_id,
            process_start_ticks=process_start_ticks,
            launch_nonce_digest=launch_nonce_digest,
            generation=generation,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/launches/register-process", json=payload
        )
        return native_schemas.NativeStreamLaunchProjection.model_validate(
            response.json()
        )

    def renew_native_stream_launch(
        self,
        *,
        flow_run_id: UUID,
        generation: int,
        enrollment_operation_id: UUID,
    ) -> native_schemas.NativeStreamLaunchHeartbeat:
        """Renew launch eligibility without treating a deadline as process death."""
        payload = native_schemas.NativeStreamLaunchHeartbeatRequest(
            flow_run_id=flow_run_id,
            generation=generation,
            enrollment_operation_id=enrollment_operation_id,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/launches/heartbeat", json=payload
        )
        return native_schemas.NativeStreamLaunchHeartbeat.model_validate(
            response.json()
        )

    def retire_unstarted_native_stream_launch(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        enrollment_operation_id: UUID,
        error: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Release an enrollment that never admitted a child process."""
        payload = native_schemas.NativeStreamUnstartedRetireRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            enrollment_operation_id=enrollment_operation_id,
            error=error,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/launches/retire-unstarted", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    def read_native_stream_recovery_candidates(
        self,
        *,
        worker_id: UUID,
        work_pool_id: UUID,
        host_id: UUID,
    ) -> list[native_schemas.NativeStreamRecoveryCandidate]:
        """Read stale launch identities eligible for trusted host verification."""
        response = self._stream_request(
            "GET",
            "/streams/v1/deployments/replacements/candidates",
            params={
                "worker_id": str(worker_id),
                "work_pool_id": str(work_pool_id),
                "host_id": str(host_id),
            },
        )
        return TypeAdapter(
            list[native_schemas.NativeStreamRecoveryCandidate]
        ).validate_python(response.json())

    def authorize_native_stream_replacement(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        enrollment_operation_id: UUID,
        worker_id: UUID,
        work_pool_id: UUID,
        host_id: UUID,
        identity_kind: Literal["LAUNCHER", "CHILD"],
        challenge_capability: str,
    ) -> native_schemas.NativeStreamReplacementAuthorization:
        """Reserve exact replacement intent before inspecting process death."""
        payload = native_schemas.NativeStreamReplacementAuthorizeRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            enrollment_operation_id=enrollment_operation_id,
            worker_id=worker_id,
            work_pool_id=work_pool_id,
            host_id=host_id,
            identity_kind=identity_kind,
            challenge_capability=challenge_capability,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/replacements/authorize", json=payload
        )
        return native_schemas.NativeStreamReplacementAuthorization.model_validate(
            response.json()
        )

    def confirm_native_stream_replacement(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        challenge_capability: str,
        launcher_instance_id: UUID,
        launcher_host_id: UUID,
        launcher_boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        new_holder_id: UUID,
        new_holder_capability: str,
        new_holder_event_public_key: str,
        new_supervisor_id: UUID,
        new_supervisor_capability: str,
        death_witness: dict[str, Any],
    ) -> native_schemas.NativeStreamWorkerEnrollment:
        """Confirm verified death and rotate authority with an exact replay key."""
        payload = native_schemas.NativeStreamReplacementConfirmRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            challenge_capability=challenge_capability,
            launcher_instance_id=launcher_instance_id,
            launcher_host_id=launcher_host_id,
            launcher_boot_id=launcher_boot_id,
            launcher_process_id=launcher_process_id,
            launcher_start_ticks=launcher_start_ticks,
            new_holder_id=new_holder_id,
            new_holder_capability=new_holder_capability,
            new_holder_event_public_key=new_holder_event_public_key,
            new_supervisor_id=new_supervisor_id,
            new_supervisor_capability=new_supervisor_capability,
            death_witness=native_schemas.NativeStreamProcessDeathWitness.model_validate(
                death_witness
            ),
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/replacements/confirm-death", json=payload
        )
        return native_schemas.NativeStreamWorkerEnrollment.model_validate(
            response.json()
        )

    def drain_native_stream(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        reason: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Close new source admission while allowing accepted records to finish."""
        payload = native_schemas.NativeStreamControlRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            reason=reason,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/control/drain", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    def cancel_native_stream(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        reason: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Fence stream writes for an operator cancellation request."""
        payload = native_schemas.NativeStreamControlRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            reason=reason,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/control/cancel", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    def finalize_native_stream_cancellation(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        enrollment_operation_id: UUID,
        death_witness: dict[str, Any],
        challenge_capability: str | None = None,
    ) -> native_schemas.NativeStreamControlResult:
        """Release cancellation only after verified registered process death."""
        payload = native_schemas.NativeStreamCancelFinalizeRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            enrollment_operation_id=enrollment_operation_id,
            challenge_capability=challenge_capability,
            death_witness=native_schemas.NativeStreamProcessDeathWitness.model_validate(
                death_witness
            ),
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/control/finalize-cancel", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    def read_native_stream_flow_projection(
        self,
        *,
        flow_run_id: UUID,
    ) -> native_schemas.NativeStreamFlowProjection:
        """Read bounded native progress without record payloads or private authority."""
        response = self._stream_request(
            "GET",
            "/streams/v1/deployments/{flow_run_id}/projection",
            path_params={"flow_run_id": flow_run_id},
        )
        return native_schemas.NativeStreamFlowProjection.model_validate(response.json())

    def retire_verified_native_stream_launch(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        enrollment_operation_id: UUID,
        challenge_capability: str,
        death_witness: dict[str, Any],
        error: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Retire a failed launch only with authorized exact process death."""
        payload = native_schemas.NativeStreamVerifiedRetireRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            enrollment_operation_id=enrollment_operation_id,
            challenge_capability=challenge_capability,
            death_witness=native_schemas.NativeStreamProcessDeathWitness.model_validate(
                death_witness
            ),
            error=error,
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/deployments/replacements/retire-verified", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    def read_native_stream_deployment_plan(
        self,
        *,
        flow_run_id: UUID,
    ) -> native_schemas.NativeStreamDeploymentPlan:
        """Read public immutable deployment and launch witnesses."""
        response = self._stream_request(
            "GET",
            "/streams/v1/deployments/{flow_run_id}/plan",
            path_params={"flow_run_id": flow_run_id},
        )
        return native_schemas.NativeStreamDeploymentPlan.model_validate(response.json())

    def read_stream_finite_plan(self, *, flow_run_id: UUID) -> schemas.StreamFinitePlan:
        """Read bounded finite witnesses and the durable stream-seal state."""
        response = self._stream_request(
            "GET", "/streams/v1/finite/plan", params={"flow_run_id": str(flow_run_id)}
        )
        return schemas.StreamFinitePlan.model_validate(response.json())

    def read_stream_finite_receipt(
        self, *, task_run_id: UUID
    ) -> schemas.StreamFiniteReceipt:
        """Read a durable finite completion result for reconstruction."""
        response = self._stream_request(
            "GET",
            "/streams/v1/finite/{task_run_id}/receipt",
            path_params={"task_run_id": task_run_id},
        )
        return schemas.StreamFiniteReceipt.model_validate(response.json())

    def abort_unsealed_stream_execution(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        manifests: list[dict[str, str]],
        error: str,
    ) -> schemas.StreamUnsealedAbortResult:
        """Close an exact unstarted graph after normal terminal flow failure."""
        payload = schemas.StreamUnsealedAbortRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "manifests": manifests,
                "error": error,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/executions/abort-unsealed", json=payload
        )
        return schemas.StreamUnsealedAbortResult.model_validate(response.json())

    def claim_stream_execution(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        holder_id: UUID,
        holder_capability: str,
        holder_event_public_key: str,
        supervisor_id: UUID,
        supervisor_capability: str,
        expected_generation: int = 0,
        protocol_version: int = 1,
    ) -> schemas.StreamExecutionBinding:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamExecutionClaimRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "holder_id": holder_id,
                "holder_capability": holder_capability,
                "holder_event_public_key": holder_event_public_key,
                "supervisor_id": supervisor_id,
                "supervisor_capability": supervisor_capability,
                "expected_generation": expected_generation,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/executions/claim", json=payload
        )
        return schemas.StreamExecutionBinding.model_validate(response.json())

    def takeover_stream_execution(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        expected_holder_id: UUID,
        supervisor_id: UUID,
        supervisor_capability: str,
        new_holder_id: UUID,
        new_holder_capability: str,
        new_holder_event_public_key: str,
        death_witness: dict[str, Any],
    ) -> schemas.StreamExecutionBinding:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamExecutionTakeoverRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "expected_generation": expected_generation,
                "expected_holder_id": expected_holder_id,
                "supervisor_id": supervisor_id,
                "supervisor_capability": supervisor_capability,
                "new_holder_id": new_holder_id,
                "new_holder_capability": new_holder_capability,
                "new_holder_event_public_key": new_holder_event_public_key,
                "death_witness": death_witness,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/executions/takeover", json=payload
        )
        return schemas.StreamExecutionBinding.model_validate(response.json())

    def seal_stream_manifest(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        manifests: list[dict[str, str]],
        finite_prefix: dict[str, Any] | None = None,
    ) -> schemas.StreamManifestSealResult:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamManifestSealRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "manifests": manifests,
                "finite_prefix": finite_prefix,
            }
        ).model_dump(
            mode="json", exclude={"finite_prefix"} if finite_prefix is None else set()
        )
        response = self._stream_request(
            "POST", "/streams/v1/executions/seal-manifest", json=payload
        )
        return schemas.StreamManifestSealResult.model_validate(response.json())

    def prepare_stream_recovery(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        manifests: list[dict[str, str]],
        finite_manifests: list[dict[str, Any]] | None = None,
    ) -> schemas.StreamRecoveryResult:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamRecoveryPrepareRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "manifests": manifests,
                "finite_manifests": finite_manifests,
            }
        ).model_dump(
            mode="json",
            exclude={"finite_manifests"} if finite_manifests is None else set(),
        )
        response = self._stream_request(
            "POST", "/streams/v1/executions/prepare-recovery", json=payload
        )
        return schemas.StreamRecoveryResult.model_validate(response.json())

    def reserve_stream_submission(
        self,
        *,
        flow_run_id: UUID,
        submission_key: str,
        role: Literal["SOURCE", "OPERATOR"],
        protocol: Literal["source-v1", "yield-v2"],
        task_name: str,
        task_key: str,
        task_version: str,
        flow_version: str,
        parameters: dict[str, JsonValue],
        input_key: str | None = None,
        upstream_stream_id: UUID | None = None,
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        artifact_digest: str | None = None,
        limits: dict[str, int] | None = None,
        task_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        labels: dict[str, bool | int | float | str] | None = None,
    ) -> schemas.StreamSubmissionManifest:
        """Create or exactly resolve a durable call slot and its native resources."""
        payload = schemas.StreamSubmissionReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "submission_key": submission_key,
                "role": role,
                "protocol": protocol,
                "task_name": task_name,
                "task_key": task_key,
                "task_version": task_version,
                "flow_version": flow_version,
                "parameters": parameters,
                "input_key": input_key,
                "upstream_stream_id": upstream_stream_id,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "artifact_digest": artifact_digest,
                "limits": limits,
                "task_policy": task_policy or {},
                "tags": tags or [],
                "labels": labels or {},
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/submissions/reserve", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    def read_stream_submission(
        self, *, flow_run_id: UUID, submission_key: str
    ) -> schemas.StreamSubmissionManifest:
        """Resolve the original resources without process-local identifiers."""
        payload = schemas.StreamSubmissionLookupRequest(
            flow_run_id=flow_run_id, submission_key=submission_key
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/submissions/lookup", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    def create_stream(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamCreateRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = self._stream_request("POST", "/streams/v1/create", json=payload)
        return schemas.StreamInfo.model_validate(response.json())

    def reserve_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/outputs/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def reject_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputRejectRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "error": error,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/outputs/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def fail_stream_task_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputFailRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "attempt": attempt,
                "error": error,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/outputs/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def open_stream_consumer(
        self,
        *,
        input_stream_id: UUID,
        output_stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        terminal_protocol: str = "yield-v2",
    ) -> schemas.ConsumerBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamConsumerOpenRequest.model_validate(
            {
                "input_stream_id": input_stream_id,
                "output_stream_id": output_stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
                "protocol_version": protocol_version,
                "terminal_protocol": terminal_protocol,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/consumers/open", json=payload
        )
        return schemas.ConsumerBinding.model_validate(response.json())

    def read_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
    ) -> schemas.StreamDelivery | None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerReadRequest.model_validate(
            {"binding": binding}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/consumers/read", json=payload
        )
        return (
            schemas.StreamDelivery.model_validate(response.json())
            if response.json() is not None
            else None
        )

    def complete_stream_input(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        record_id: UUID,
        sequence: int,
        delivery_id: UUID,
        kind: Literal["YIELD", "RETURN"],
        value: JsonValue,
        managed_state: dict[str, JsonValue],
    ) -> schemas.StreamCompletionResult:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        token = schemas.DeliveryToken(
            **binding.model_dump(),
            record_id=record_id,
            sequence=sequence,
            delivery_id=delivery_id,
        )
        payload = schemas.StreamConsumerCompleteRequest.model_validate(
            {
                "binding": binding,
                "token": token,
                "kind": kind,
                "value": value,
                "managed_state": managed_state,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/consumers/complete", json=payload
        )
        return schemas.StreamCompletionResult.model_validate(response.json())

    def finish_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        self._stream_request("POST", "/streams/v1/consumers/finish", json=payload)

    def claim_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
    ) -> schemas.SourceBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamSourceClaimRequest.model_validate(
            {
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
            }
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/claim", json=payload
        )
        return schemas.SourceBinding.model_validate(response.json())

    def append_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        position: int,
        value: JsonValue,
    ) -> schemas.StreamEnvelope:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceAppendRequest.model_validate(
            {"binding": binding, "position": position, "value": value}
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/append", json=payload
        )
        return schemas.StreamEnvelope.model_validate(response.json())

    def finish_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        self._stream_request("POST", "/streams/v1/sources/finish", json=payload)

    def reserve_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceReserveRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                limits=limits,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def reject_stream_source(
        self, *, flow_run_id: UUID, stream_id: UUID, task_run_id: UUID, error: str
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceRejectRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                error=error,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def fail_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceFailRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                attempt=attempt,
                error=error,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    def open_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.SourceAdmission:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceOpenRequest.model_validate(
            dict(
                stream_id=stream_id,
                task_run_id=task_run_id,
                owner_id=owner_id,
                attempt=attempt,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = self._stream_request(
            "POST", "/streams/v1/sources/open", json=payload
        )
        return schemas.SourceAdmission.model_validate(response.json())

    def close_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        expected_position: int,
    ) -> None:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceCloseRequest.model_validate(
            dict(
                binding=schemas.SourceBinding(
                    flow_run_id=flow_run_id,
                    stream_id=stream_id,
                    task_run_id=task_run_id,
                    owner_id=owner_id,
                    attempt=attempt,
                    owner_epoch=owner_epoch,
                ),
                expected_position=expected_position,
            )
        ).model_dump(mode="json")
        self._stream_request("POST", "/streams/v1/sources/close", json=payload)

    def release_stream_flow(
        self,
        *,
        flow_run_id: UUID,
    ) -> schemas.StreamReleaseResult:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamReleaseRequest.model_validate(
            {"flow_run_id": flow_run_id}
        ).model_dump(mode="json")
        response = self._stream_request("POST", "/streams/v1/release", json=payload)
        return schemas.StreamReleaseResult.model_validate(response.json())

    def read_stream(self, stream_id: UUID) -> schemas.StreamInfo:
        response = self._stream_request(
            "GET", "/streams/v1/{stream_id}", path_params={"stream_id": stream_id}
        )
        return schemas.StreamInfo.model_validate(response.json())

    def read_stream_prefix(
        self, stream_id: UUID, *, limit: int
    ) -> list[schemas.StreamEnvelope]:
        response = self._stream_request(
            "GET",
            "/streams/v1/{stream_id}/prefix",
            path_params={"stream_id": stream_id},
            params={"limit": limit},
        )
        return TypeAdapter(list[schemas.StreamEnvelope]).validate_python(
            response.json()
        )


class StreamAsyncClient(BaseAsyncClient):
    """Native data-plane methods; credentials belong to the owning client."""

    async def start_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGStartRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/start",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "START",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    async def activate_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGActivateRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/activate",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "ACTIVATE",
                "execution.activation_operation_id": intent.operation_id,
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    async def abort_stream_dag_preview(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGAbortPreviewRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/abort-preview",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "ABORT_PREVIEW",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    async def cancel_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
        reason: str,
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGCancelRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
                "reason": reason,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/cancel",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "CANCEL",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    async def fail_stream_dag(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
        failure_kind: Literal[
            "FLOW_SCOPE_EXCEPTION",
            "START_EXCEPTION",
            "START_TIMEOUT",
            "COORDINATOR_ERROR",
        ],
    ) -> dag_execution_schemas.StreamDAGOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGFailRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "routing_digest": routing_digest,
                "failure_kind": failure_kind,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/{graph_id}/fail",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.graph_revision_id": graph_revision_id,
                "receipt.kind": "FAIL",
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    async def read_stream_dag_execution(
        self,
        *,
        graph_revision_id: UUID,
    ) -> dag_execution_schemas.StreamDAGExecution:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/dags/{graph_id}/execution",
            path_params={"graph_id": graph_revision_id},
        )
        result = dag_execution_schemas.StreamDAGExecution.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(result, {"graph_revision_id": graph_revision_id})
        return result

    async def read_stream_dag_subtask_bootstrap(
        self,
        *,
        subtask_id: UUID,
    ) -> dag_execution_schemas.StreamDAGSubtaskBootstrap:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/bootstrap",
            path_params={"subtask_id": subtask_id},
        )
        result = dag_execution_schemas.StreamDAGSubtaskBootstrap.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(result, {"assignment.id": subtask_id})
        _dag_child_ack(result, subtask_id=subtask_id)
        return result

    async def read_stream_dag_subtask_activation(
        self,
        *,
        subtask_id: UUID,
    ) -> dag_execution_schemas.StreamDAGActivationObservation:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/activation",
            path_params={"subtask_id": subtask_id},
        )
        result = dag_execution_schemas.StreamDAGActivationObservation.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(result, {"subtask_id": subtask_id})
        _dag_child_ack(result, subtask_id=subtask_id)
        return result

    async def mark_stream_dag_subtask_bootstrap_ready(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        bootstrap_digest: str,
        signature: str,
    ) -> dag_execution_schemas.StreamDAGBootstrapReadyResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGBootstrapReadyRequest.model_validate(
            {
                "operation_id": operation_id,
                "bootstrap_digest": bootstrap_digest,
                "signature": signature,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/subtasks/{subtask_id}/bootstrap-ready",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGBootstrapReadyResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "bootstrap_digest": intent.bootstrap_digest,
                "subtask_id": subtask_id,
            },
        )
        _dag_child_ack(result, subtask_id=subtask_id, intent=intent)
        return result

    async def read_stream_dag_input_status(
        self,
        *,
        subtask_id: UUID,
    ) -> schemas.StreamInfo:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/input/status",
            path_params={"subtask_id": subtask_id},
        )
        result = schemas.StreamInfo.model_validate(response.json())
        _dag_child_ack(result, subtask_id=subtask_id, stream_role="input")
        return result

    async def read_stream_dag_output_status(
        self,
        *,
        subtask_id: UUID,
    ) -> schemas.StreamInfo:
        """Execute the dedicated DAG protocol with a frozen request body."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/dags/subtasks/{subtask_id}/output/status",
            path_params={"subtask_id": subtask_id},
        )
        result = schemas.StreamInfo.model_validate(response.json())
        _dag_child_ack(result, subtask_id=subtask_id, stream_role="output")
        return result

    async def open_stream_dag_source(
        self,
        *,
        context: dict[str, Any],
    ) -> schemas.SourceAdmission:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGSourceOpenRequest.model_validate(
            {"context": context}
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/sources/open",
            json=payload,
        )
        result = schemas.SourceAdmission.model_validate(response.json())
        _dag_child_ack(result, intent=intent)
        return result

    async def open_stream_dag_consumer(
        self,
        *,
        context: dict[str, Any],
    ) -> schemas.ConsumerBinding:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGConsumerOpenRequest.model_validate(
            {"context": context}
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/open",
            json=payload,
        )
        result = schemas.ConsumerBinding.model_validate(response.json())
        _dag_child_ack(result, intent=intent)
        return result

    async def read_stream_dag_consumer(
        self,
        *,
        context: dict[str, Any],
        binding: dict[str, Any],
    ) -> schemas.StreamDelivery | None:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGConsumerReadRequest.model_validate(
            {"context": context, "binding": binding}
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/read",
            json=payload,
        )
        if response.json() is None:
            return None
        result = schemas.StreamDelivery.model_validate(response.json())
        _dag_child_ack(result, intent=intent)
        return result

    async def finish_stream_dag_source(
        self,
        *,
        operation_id: UUID,
        context: dict[str, Any],
        binding: dict[str, Any],
        expected_position: int,
    ) -> dag_execution_schemas.StreamDAGProducerFinishReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGSourceFinishRequest.model_validate(
            {
                "operation_id": operation_id,
                "context": context,
                "binding": binding,
                "expected_position": expected_position,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/sources/finish",
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGProducerFinishReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "graph_revision_id": intent.context.graph_revision_id,
                "producer_subtask_id": intent.context.producer_subtask_id,
                "producer_stream_id": intent.binding.stream_id,
                "kind": "SOURCE_EOF",
                "accepted_source_position": intent.expected_position,
            },
        )
        _dag_child_ack(result, intent=intent)
        return result

    async def finish_stream_dag_consumer(
        self,
        *,
        operation_id: UUID,
        context: dict[str, Any],
        binding: dict[str, Any],
    ) -> dag_execution_schemas.StreamDAGProducerFinishReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGConsumerFinishRequest.model_validate(
            {"operation_id": operation_id, "context": context, "binding": binding}
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/finish",
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGProducerFinishReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "graph_revision_id": intent.context.graph_revision_id,
                "producer_subtask_id": intent.context.producer_subtask_id,
                "producer_stream_id": intent.binding.output_stream_id,
                "kind": "CONSUMER_EOF",
            },
        )
        _dag_child_ack(result, intent=intent)
        return result

    async def poll_worker_stream_dag_subtasks(
        self,
        *,
        worker_id: UUID,
        principal: dict[str, Any],
        active_subtask_ids: list[UUID] | None = None,
        limit: int = 64,
    ) -> list[dag_execution_schemas.StreamDAGWorkerAssignment]:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGWorkerPollRequest.model_validate(
            {
                "principal": principal,
                "active_subtask_ids": []
                if active_subtask_ids is None
                else active_subtask_ids,
                "limit": limit,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/poll",
            path_params={"worker_id": worker_id},
            json=payload,
        )
        result = TypeAdapter(
            list[dag_execution_schemas.StreamDAGWorkerAssignment]
        ).validate_python(response.json())
        if len({row.assignment.id for row in result}) != len(result) or any(
            row.assignment.assigned_worker_id != worker_id for row in result
        ):
            raise StreamRequestError(
                "IDENTITY_CONFLICT",
                "DAG worker poll contains a foreign or repeated assignment",
            )
        return result

    async def abort_worker_stream_dag_startup(
        self,
        *,
        worker_id: UUID,
        graph_revision_id: UUID,
        principal: dict[str, Any],
        start_operation_id: UUID,
        graph_digest: str,
        routing_digest: str,
    ) -> dag_execution_schemas.StreamDAGWorkerStartupAbortResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = (
            dag_execution_schemas.StreamDAGWorkerStartupAbortRequest.model_validate(
                {
                    "principal": principal,
                    "start_operation_id": start_operation_id,
                    "graph_digest": graph_digest,
                    "routing_digest": routing_digest,
                }
            )
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/graphs/{graph_id}/startup-abort",
            path_params={"worker_id": worker_id, "graph_id": graph_revision_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGWorkerStartupAbortResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "execution.graph_revision_id": graph_revision_id,
                "execution.graph_digest": intent.graph_digest,
                "execution.routing_digest": intent.routing_digest,
            },
        )
        return result

    async def prepare_worker_stream_dag_launch(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        start_operation_id: UUID,
        replacement_operation_id: UUID | None = None,
    ) -> dag_execution_schemas.StreamDAGLaunchOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGLaunchPrepareRequest.model_validate(
            {
                "principal": principal,
                "operation_id": operation_id,
                "start_operation_id": start_operation_id,
                "replacement_operation_id": replacement_operation_id,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/prepare",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGLaunchOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.kind": "PREPARE",
                "current.subtask_id": subtask_id,
                "current.worker_id": worker_id,
                "current.start_operation_id": intent.start_operation_id,
                "current.replacement_operation_id": intent.replacement_operation_id,
            },
        )
        return result

    async def record_worker_stream_dag_launch(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        launch_attempt_id: UUID,
        child_process_id: int,
        process_start_ticks: int,
        local_journal_digest: str,
    ) -> dag_execution_schemas.StreamDAGLaunchOperationResult:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGLaunchRecordRequest.model_validate(
            {
                "principal": principal,
                "operation_id": operation_id,
                "child_process_id": child_process_id,
                "process_start_ticks": process_start_ticks,
                "local_journal_digest": local_journal_digest,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/record",
            path_params={
                "worker_id": worker_id,
                "subtask_id": subtask_id,
                "launch_attempt_id": launch_attempt_id,
            },
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGLaunchOperationResult.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "receipt.operation_id": intent.operation_id,
                "receipt.kind": "RECORD",
                "current.id": launch_attempt_id,
                "current.subtask_id": subtask_id,
                "current.worker_id": worker_id,
                "current.child_process_id": intent.child_process_id,
                "current.process_start_ticks": intent.process_start_ticks,
                "current.local_journal_digest": intent.local_journal_digest,
            },
        )
        return result

    async def close_worker_stream_dag_launch(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        launch_attempt_id: UUID,
        proof: dict[str, Any],
    ) -> dag_execution_schemas.StreamDAGLaunchCloseReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGLaunchCloseRequest.model_validate(
            {"principal": principal, "operation_id": operation_id, "proof": proof}
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/launches/{launch_attempt_id}/close",
            path_params={
                "worker_id": worker_id,
                "subtask_id": subtask_id,
                "launch_attempt_id": launch_attempt_id,
            },
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGLaunchCloseReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "launch_attempt_id": launch_attempt_id,
                "subtask_id": subtask_id,
                "proof_kind": intent.proof.kind,
            },
        )
        return result

    async def stop_worker_stream_dag_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        operation_id: UUID,
        control_operation_id: UUID,
    ) -> dag_execution_schemas.StreamDAGStopReceipt:
        """Execute the dedicated DAG protocol with a frozen request body."""
        intent = dag_execution_schemas.StreamDAGWorkerStopRequest.model_validate(
            {
                "principal": principal,
                "operation_id": operation_id,
                "control_operation_id": control_operation_id,
            }
        )
        payload = intent.model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/workers/{worker_id}/subtasks/{subtask_id}/stop",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        result = dag_execution_schemas.StreamDAGStopReceipt.model_validate(
            response.json()
        )
        _dag_lifecycle_ack(
            result,
            {
                "operation_id": intent.operation_id,
                "control_operation_id": intent.control_operation_id,
                "subtask_id": subtask_id,
            },
        )
        return result

    async def _stream_request(
        self, method: HTTP_METHODS, path: ServerRoutes, **kwargs: Any
    ) -> httpx.Response:
        try:
            return await self.request(method, path, **kwargs)
        except httpx.HTTPStatusError as error:
            raise _refusal(error) from error

    async def poll_worker_stream_subtasks(
        self,
        *,
        worker_id: UUID,
        principal: dict[str, Any],
        active_subtask_ids: list[UUID] | None = None,
        limit: int = 64,
    ) -> list[subtask_schemas.StreamWorkerAssignment]:
        """Poll persisted placements through the trusted worker's Basic authority."""
        payload = subtask_schemas.StreamWorkerPollRequest.model_validate(
            {
                "principal": principal,
                "active_subtask_ids": active_subtask_ids
                if active_subtask_ids is not None
                else [],
                "limit": limit,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/poll",
            path_params={"worker_id": worker_id},
            json=payload,
        )
        return TypeAdapter(
            list[subtask_schemas.StreamWorkerAssignment]
        ).validate_python(response.json())

    async def claim_worker_stream_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        holder_id: UUID,
        capability: str,
        event_public_key: str,
        work_pool_id: UUID,
        host_id: UUID,
        boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        assignment_epoch: Literal[1] = 1,
        expected_physical_epoch: Literal[0] = 0,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Claim exactly this worker's assigned subtask, without a Flow capability."""
        payload = subtask_schemas.StreamSubtaskClaimRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "assignment_epoch": assignment_epoch,
                "expected_physical_epoch": expected_physical_epoch,
                "holder_id": holder_id,
                "capability": capability,
                "event_public_key": event_public_key,
                "launcher_binding_kind": "ASSIGNED_WORKER",
                "worker_id": worker_id,
                "work_pool_id": work_pool_id,
                "host_id": host_id,
                "boot_id": boot_id,
                "launcher_process_id": launcher_process_id,
                "launcher_start_ticks": launcher_start_ticks,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/claim",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def register_worker_stream_subtask_process(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        registration: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerRegistrationRequest.model_validate(
            {"principal": principal, "registration": registration}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/register",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def renew_worker_stream_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        grant_id: UUID,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerRenewRequest.model_validate(
            {"principal": principal, "grant_id": grant_id}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/renew",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def authorize_worker_stream_subtask_replacement(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        replacement: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskReplacementAuthorization:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = (
            subtask_schemas.StreamWorkerReplacementAuthorizeRequest.model_validate(
                {"principal": principal, "replacement": replacement}
            ).model_dump(mode="json")
        )
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/replacements/authorize",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskReplacementAuthorization.model_validate(
            response.json()
        )

    async def confirm_worker_stream_subtask_replacement(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        replacement: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerReplacementConfirmRequest.model_validate(
            {"principal": principal, "replacement": replacement}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/replacements/confirm",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def retire_verified_worker_stream_subtask(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        retirement: dict[str, Any],
    ) -> subtask_schemas.StreamSubtaskVerifiedRetirement:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerVerifiedRetireRequest.model_validate(
            {"principal": principal, "retirement": retirement}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/replacements/retire",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskVerifiedRetirement.model_validate(
            response.json()
        )

    async def acknowledge_worker_stream_subtask_startup_stop(
        self,
        *,
        worker_id: UUID,
        subtask_id: UUID,
        principal: dict[str, Any],
        stop: dict[str, Any],
    ) -> subtask_schemas.StreamGraphStartupStopResult:
        """Send the typed own-assignment control request as a trusted worker."""
        payload = subtask_schemas.StreamWorkerStartupStopRequest.model_validate(
            {"principal": principal, "stop": stop}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/subtasks/{subtask_id}/startup-stop",
            path_params={"worker_id": worker_id, "subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphStartupStopResult.model_validate(
            response.json()
        )

    async def abort_worker_stream_graph_startup(
        self,
        *,
        worker_id: UUID,
        graph_revision_id: UUID,
        principal: dict[str, Any],
    ) -> subtask_schemas.StreamGraphPlan:
        """Request the versioned startup abort; worker stop receipts release slots."""
        payload = subtask_schemas.StreamWorkerStartupAbortRequest.model_validate(
            {"principal": principal}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/workers/{worker_id}/graphs/{graph_id}/startup-abort",
            path_params={"worker_id": worker_id, "graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    async def acknowledge_stream_subtask_startup_stop(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        abort_operation_id: UUID,
        grant_id: UUID,
        expected_physical_epoch: int,
        death_witness: dict[str, Any] | None = None,
        unstarted: bool = False,
    ) -> subtask_schemas.StreamGraphStartupStopResult:
        """Record one exact preactivation stop through the parent Flow authority."""
        payload = subtask_schemas.StreamGraphStartupStopRequest.model_validate(
            {
                "operation_id": operation_id,
                "abort_operation_id": abort_operation_id,
                "grant_id": grant_id,
                "expected_physical_epoch": expected_physical_epoch,
                "death_witness": death_witness,
                "unstarted": unstarted,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/startup-stop",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphStartupStopResult.model_validate(
            response.json()
        )

    async def append_stream_dag_source(
        self,
        *,
        context: dict[str, Any],
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        position: int,
        value: JsonValue,
        output_key: dict[str, Any] | None = None,
    ) -> route_schemas.StreamDAGRouteOutcome:
        """Send a separately versioned DAG source intent; execution stays fenced."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        intent = route_schemas.StreamDAGSourceAppendRequest.model_validate(
            {
                "context": context,
                "binding": binding,
                "position": position,
                "value": value,
                "output_key": output_key,
            }
        )
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/sources/append",
            json=intent.model_dump(mode="json"),
        )
        return _validate_dag_route_ack(
            route_schemas.StreamDAGRouteOutcome.model_validate(response.json()), intent
        )

    async def complete_stream_dag_consumer(
        self,
        *,
        context: dict[str, Any],
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        record_id: UUID,
        sequence: int,
        delivery_id: UUID,
        kind: Literal["YIELD", "RETURN"],
        value: JsonValue,
        managed_state: dict[str, JsonValue],
        output_key: dict[str, Any] | None = None,
    ) -> route_schemas.StreamDAGRouteOutcome:
        """Send one frozen DAG terminal intent without changing legacy bodies."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        token = schemas.DeliveryToken(
            **binding.model_dump(),
            record_id=record_id,
            sequence=sequence,
            delivery_id=delivery_id,
        )
        intent = route_schemas.StreamDAGConsumerCompleteRequest.model_validate(
            {
                "context": context,
                "binding": binding,
                "token": token,
                "kind": kind,
                "value": value,
                "managed_state": managed_state,
                "output_key": output_key,
            }
        )
        response = await self._stream_request(
            "POST",
            "/streams/v2/dags/consumers/complete",
            json=intent.model_dump(mode="json"),
        )
        return _validate_dag_route_ack(
            route_schemas.StreamDAGRouteOutcome.model_validate(response.json()), intent
        )

    async def seal_stream_dag(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        flow_version: str,
        operators: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        working_directory: str | None = None,
        profile: Literal["partitioned-dag-v1"] = "partitioned-dag-v1",
        assignment_epoch: Literal[1] = 1,
    ) -> dag_schemas.StreamDAGPlan:
        """Seal one complete DAG metadata declaration without starting tasks."""
        payload = dag_schemas.StreamDAGSealRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "flow_version": flow_version,
                "operators": operators,
                "edges": edges,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "working_directory": working_directory,
                "profile": profile,
                "assignment_epoch": assignment_epoch,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v2/dags/seal", json=payload
        )
        return dag_schemas.StreamDAGPlan.model_validate(response.json())

    async def read_stream_dag(
        self, *, graph_revision_id: UUID
    ) -> dag_schemas.StreamDAGPlan:
        """Read sealed DAG metadata through its parent Flow execution authority."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/dags/{graph_id}",
            path_params={"graph_id": graph_revision_id},
        )
        return dag_schemas.StreamDAGPlan.model_validate(response.json())

    async def seal_stream_graph(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        flow_version: str,
        operators: list[dict[str, Any]],
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        working_directory: str | None = None,
        profile: Literal["partitioned-fixed-v1"] = "partitioned-fixed-v1",
        assignment_epoch: Literal[1] = 1,
    ) -> subtask_schemas.StreamGraphPlan:
        """Seal the immutable finite P2 graph before any physical task starts."""
        payload = subtask_schemas.StreamGraphSealRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "flow_version": flow_version,
                "operators": operators,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "working_directory": working_directory,
                "profile": profile,
                "assignment_epoch": assignment_epoch,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v2/graphs/seal", json=payload
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    async def read_stream_graph(
        self, *, graph_revision_id: UUID
    ) -> subtask_schemas.StreamGraphPlan:
        """Read a graph through its parent execution authority."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/graphs/{graph_id}",
            path_params={"graph_id": graph_revision_id},
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    async def admit_stream_region(
        self, *, graph_revision_id: UUID, operation_id: UUID, graph_digest: str
    ) -> subtask_schemas.StreamGraphPlan:
        """Reserve the complete runnable region before source admission."""
        payload = subtask_schemas.StreamRegionAdmissionRequest(
            operation_id=operation_id, graph_digest=graph_digest
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/graphs/{graph_id}/admit",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    async def activate_stream_graph(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        grant_ids: list[UUID],
    ) -> subtask_schemas.StreamGraphPlan:
        """Activate the exact four registered grants before releasing child user code."""
        payload = subtask_schemas.StreamGraphActivateRequest(
            operation_id=operation_id, graph_digest=graph_digest, grant_ids=grant_ids
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/graphs/{graph_id}/activate",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    async def abort_stream_graph(
        self,
        *,
        graph_revision_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        error: str,
    ) -> subtask_schemas.StreamGraphPlan:
        """Compensate a preactivation failure through its retained graph identity."""
        payload = subtask_schemas.StreamGraphAbortRequest(
            operation_id=operation_id, graph_digest=graph_digest, error=error
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/graphs/{graph_id}/abort",
            path_params={"graph_id": graph_revision_id},
            json=payload,
        )
        return subtask_schemas.StreamGraphPlan.model_validate(response.json())

    async def claim_stream_subtask(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        graph_digest: str,
        holder_id: UUID,
        capability: str,
        event_public_key: str,
        launcher_binding_kind: Literal[
            "DEPLOYMENT", "FLOW_AUTHORITY_LOCAL", "ASSIGNED_WORKER"
        ],
        host_id: UUID,
        boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        worker_id: UUID | None = None,
        work_pool_id: UUID | None = None,
        assignment_epoch: Literal[1] = 1,
        expected_physical_epoch: Literal[0] = 0,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Bind caller-generated child credentials without returning the secret."""
        payload = subtask_schemas.StreamSubtaskClaimRequest.model_validate(
            {
                "operation_id": operation_id,
                "graph_digest": graph_digest,
                "assignment_epoch": assignment_epoch,
                "expected_physical_epoch": expected_physical_epoch,
                "holder_id": holder_id,
                "capability": capability,
                "event_public_key": event_public_key,
                "launcher_binding_kind": launcher_binding_kind,
                "worker_id": worker_id,
                "work_pool_id": work_pool_id,
                "host_id": host_id,
                "boot_id": boot_id,
                "launcher_process_id": launcher_process_id,
                "launcher_start_ticks": launcher_start_ticks,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/claim",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def register_stream_subtask_process(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        grant_id: UUID,
        child_process_id: int,
        process_start_ticks: int,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Register an exact child before delivering its capability bootstrap."""
        payload = subtask_schemas.StreamSubtaskRegistrationRequest.model_validate(
            {
                "operation_id": operation_id,
                "grant_id": grant_id,
                "child_process_id": child_process_id,
                "process_start_ticks": process_start_ticks,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/register",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def renew_stream_subtask(
        self, *, subtask_id: UUID, grant_id: UUID
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Renew parent supervision without asserting kernel process liveness."""
        payload = subtask_schemas.StreamSubtaskRenewRequest(
            grant_id=grant_id
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/renew",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def authorize_stream_subtask_replacement(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        expected_grant_id: UUID,
        expected_physical_epoch: int,
        host_id: UUID,
        challenge_capability: str,
    ) -> subtask_schemas.StreamSubtaskReplacementAuthorization:
        """Authorize verification for one exact physical attempt."""
        payload = (
            subtask_schemas.StreamSubtaskReplacementAuthorizeRequest.model_validate(
                {
                    "operation_id": operation_id,
                    "expected_grant_id": expected_grant_id,
                    "expected_physical_epoch": expected_physical_epoch,
                    "host_id": host_id,
                    "challenge_capability": challenge_capability,
                }
            ).model_dump(mode="json")
        )
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/replacements/authorize",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskReplacementAuthorization.model_validate(
            response.json()
        )

    async def confirm_stream_subtask_replacement(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        challenge_capability: str,
        death_witness: dict[str, Any],
        new_holder_id: UUID,
        new_capability: str,
        new_event_public_key: str,
        host_id: UUID,
        boot_id: UUID,
        new_child_process_id: int,
        new_process_start_ticks: int,
    ) -> subtask_schemas.StreamSubtaskGrant:
        """Fence one dead physical attempt while retaining its TaskRun identity."""
        payload = subtask_schemas.StreamSubtaskReplacementConfirmRequest.model_validate(
            {
                "operation_id": operation_id,
                "challenge_capability": challenge_capability,
                "death_witness": death_witness,
                "new_holder_id": new_holder_id,
                "new_capability": new_capability,
                "new_event_public_key": new_event_public_key,
                "host_id": host_id,
                "boot_id": boot_id,
                "new_child_process_id": new_child_process_id,
                "new_process_start_ticks": new_process_start_ticks,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/replacements/confirm",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return subtask_schemas.StreamSubtaskGrant.model_validate(response.json())

    async def retire_verified_stream_subtask(
        self,
        *,
        subtask_id: UUID,
        operation_id: UUID,
        challenge_capability: str,
        death_witness: dict[str, Any],
        error: str,
        reason: Literal[
            "TERMINAL_RECONCILIATION", "REPLACEMENT_PREFLIGHT_FAILED"
        ] = "TERMINAL_RECONCILIATION",
    ) -> subtask_schemas.StreamSubtaskVerifiedRetirement:
        """Reconcile a verified terminal attempt without rerunning accepted work."""
        payload = subtask_schemas.StreamSubtaskVerifiedRetireRequest.model_validate(
            {
                "operation_id": operation_id,
                "challenge_capability": challenge_capability,
                "death_witness": death_witness,
                "error": error,
                "reason": reason,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/replacements/retire",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        result = subtask_schemas.StreamSubtaskVerifiedRetirement.model_validate(
            response.json()
        )
        if result.operation_id != operation_id or result.subtask_id != subtask_id:
            raise StreamRequestError(
                "IDENTITY_CONFLICT", "Physical retirement receipt differs"
            )
        return result

    async def read_stream_subtask_bootstrap(
        self, *, subtask_id: UUID
    ) -> subtask_schemas.StreamSubtaskBootstrap:
        """Read only this grant's immutable assignment and ordinary TaskRun."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/subtasks/{subtask_id}/bootstrap",
            path_params={"subtask_id": subtask_id},
        )
        return _validate_subtask_bootstrap(
            subtask_schemas.StreamSubtaskBootstrap.model_validate(response.json()),
            subtask_id,
        )

    async def read_stream_subtask_task_run(self, *, subtask_id: UUID) -> TaskRun:
        """Read this physical TaskRun without access to ordinary run APIs."""
        response = await self._stream_request(
            "GET",
            "/streams/v2/subtasks/{subtask_id}/task-run",
            path_params={"subtask_id": subtask_id},
        )
        return TaskRun.model_validate(response.json())

    async def emit_stream_subtask_event(
        self, *, subtask_id: UUID, event: dict[str, Any]
    ) -> subtask_schemas.StreamSubtaskEventResult:
        """Deliver an ordinary signed TaskRun event through scoped HTTP."""
        payload = _subtask_event_payload(event, subtask_id)
        response = await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/events",
            path_params={"subtask_id": subtask_id},
            json=payload,
        )
        return _subtask_event_result(response, payload)

    async def renew_stream_subtask_lease(self, *, subtask_id: UUID) -> None:
        """Renew this subtask's pre-admitted task/tag lease."""
        await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/lease/renew",
            path_params={"subtask_id": subtask_id},
            json={},
        )

    async def release_stream_subtask_lease(self, *, subtask_id: UUID) -> None:
        """Release this subtask's lease after its accepted terminal state."""
        await self._stream_request(
            "POST",
            "/streams/v2/subtasks/{subtask_id}/lease/release",
            path_params={"subtask_id": subtask_id},
            json={},
        )

    async def reserve_stream_finite_submission(
        self,
        *,
        flow_run_id: UUID,
        phase: Literal["PREFIX", "TAIL"],
        submission_key: str,
        task_name: str,
        task_key: str,
        artifact_digest: str,
        task_version: str,
        flow_version: str,
        parameters: dict[str, JsonValue],
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        task_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        labels: dict[str, bool | int | float | str] | None = None,
    ) -> schemas.StreamFiniteManifest:
        """Reserve an ordinary TaskRun in the fenced finite prefix/tail plan."""
        payload = schemas.StreamFiniteReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "phase": phase,
                "submission_key": submission_key,
                "task_name": task_name,
                "task_key": task_key,
                "artifact_digest": artifact_digest,
                "task_version": task_version,
                "flow_version": flow_version,
                "parameters": parameters,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "task_policy": task_policy or {},
                "tags": tags or [],
                "labels": labels or {},
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/finite/reserve", json=payload
        )
        return schemas.StreamFiniteManifest.model_validate(response.json())

    async def read_stream_finite_submission(
        self, *, flow_run_id: UUID, submission_key: str
    ) -> schemas.StreamFiniteManifest:
        """Read a finite binding without admitting or changing its task."""
        payload = schemas.StreamFiniteLookupRequest(
            flow_run_id=flow_run_id, submission_key=submission_key
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/finite/lookup", json=payload
        )
        return schemas.StreamFiniteManifest.model_validate(response.json())

    async def complete_stream_finite_task(
        self,
        *,
        flow_run_id: UUID,
        task_run_id: UUID,
        operation_id: UUID,
        run_count: int,
        result: JsonValue,
    ) -> schemas.StreamFiniteReceipt:
        """Persist the bounded finite result before normal TaskRun success."""
        payload = schemas.StreamFiniteCompleteRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "task_run_id": task_run_id,
                "operation_id": operation_id,
                "run_count": run_count,
                "result": result,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/finite/complete", json=payload
        )
        return schemas.StreamFiniteReceipt.model_validate(response.json())

    async def enroll_native_stream_deployment(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        declaration_digest: str,
        worker_id: UUID,
        work_pool_id: UUID,
        launcher_instance_id: UUID,
        launcher_host_id: UUID,
        launcher_boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        holder_id: UUID,
        holder_capability: str,
        holder_event_public_key: str,
        supervisor_id: UUID,
        supervisor_capability: str,
        launch_profile: Literal["linux-local-pidfd-v1"] = "linux-local-pidfd-v1",
        expected_generation: Literal[0] = 0,
    ) -> native_schemas.NativeStreamWorkerEnrollment:
        """Enroll a trusted worker before launching the declared Flow."""
        payload = native_schemas.NativeStreamWorkerEnrollRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            declaration_digest=declaration_digest,
            worker_id=worker_id,
            work_pool_id=work_pool_id,
            launcher_instance_id=launcher_instance_id,
            launcher_host_id=launcher_host_id,
            launcher_boot_id=launcher_boot_id,
            launcher_process_id=launcher_process_id,
            launcher_start_ticks=launcher_start_ticks,
            holder_id=holder_id,
            holder_capability=holder_capability,
            holder_event_public_key=holder_event_public_key,
            supervisor_id=supervisor_id,
            supervisor_capability=supervisor_capability,
            launch_profile=launch_profile,
            expected_generation=expected_generation,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/enroll", json=payload
        )
        return native_schemas.NativeStreamWorkerEnrollment.model_validate(
            response.json()
        )

    async def register_native_stream_process(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        enrollment_operation_id: UUID,
        launcher_instance_id: UUID,
        host_id: UUID,
        boot_id: UUID,
        child_process_id: int,
        process_start_ticks: int,
        launch_nonce_digest: str,
        generation: int = 1,
    ) -> native_schemas.NativeStreamLaunchProjection:
        """Bind the blocked child identity before delivering its credentials."""
        payload = native_schemas.NativeStreamProcessRegistrationRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            enrollment_operation_id=enrollment_operation_id,
            launcher_instance_id=launcher_instance_id,
            host_id=host_id,
            boot_id=boot_id,
            child_process_id=child_process_id,
            process_start_ticks=process_start_ticks,
            launch_nonce_digest=launch_nonce_digest,
            generation=generation,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/launches/register-process", json=payload
        )
        return native_schemas.NativeStreamLaunchProjection.model_validate(
            response.json()
        )

    async def renew_native_stream_launch(
        self,
        *,
        flow_run_id: UUID,
        generation: int,
        enrollment_operation_id: UUID,
    ) -> native_schemas.NativeStreamLaunchHeartbeat:
        """Renew launch eligibility without treating a deadline as process death."""
        payload = native_schemas.NativeStreamLaunchHeartbeatRequest(
            flow_run_id=flow_run_id,
            generation=generation,
            enrollment_operation_id=enrollment_operation_id,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/launches/heartbeat", json=payload
        )
        return native_schemas.NativeStreamLaunchHeartbeat.model_validate(
            response.json()
        )

    async def retire_unstarted_native_stream_launch(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        enrollment_operation_id: UUID,
        error: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Release an enrollment that never admitted a child process."""
        payload = native_schemas.NativeStreamUnstartedRetireRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            enrollment_operation_id=enrollment_operation_id,
            error=error,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/launches/retire-unstarted", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    async def read_native_stream_recovery_candidates(
        self,
        *,
        worker_id: UUID,
        work_pool_id: UUID,
        host_id: UUID,
    ) -> list[native_schemas.NativeStreamRecoveryCandidate]:
        """Read stale launch identities eligible for trusted host verification."""
        response = await self._stream_request(
            "GET",
            "/streams/v1/deployments/replacements/candidates",
            params={
                "worker_id": str(worker_id),
                "work_pool_id": str(work_pool_id),
                "host_id": str(host_id),
            },
        )
        return TypeAdapter(
            list[native_schemas.NativeStreamRecoveryCandidate]
        ).validate_python(response.json())

    async def authorize_native_stream_replacement(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        enrollment_operation_id: UUID,
        worker_id: UUID,
        work_pool_id: UUID,
        host_id: UUID,
        identity_kind: Literal["LAUNCHER", "CHILD"],
        challenge_capability: str,
    ) -> native_schemas.NativeStreamReplacementAuthorization:
        """Reserve exact replacement intent before inspecting process death."""
        payload = native_schemas.NativeStreamReplacementAuthorizeRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            enrollment_operation_id=enrollment_operation_id,
            worker_id=worker_id,
            work_pool_id=work_pool_id,
            host_id=host_id,
            identity_kind=identity_kind,
            challenge_capability=challenge_capability,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/replacements/authorize", json=payload
        )
        return native_schemas.NativeStreamReplacementAuthorization.model_validate(
            response.json()
        )

    async def confirm_native_stream_replacement(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        challenge_capability: str,
        launcher_instance_id: UUID,
        launcher_host_id: UUID,
        launcher_boot_id: UUID,
        launcher_process_id: int,
        launcher_start_ticks: int,
        new_holder_id: UUID,
        new_holder_capability: str,
        new_holder_event_public_key: str,
        new_supervisor_id: UUID,
        new_supervisor_capability: str,
        death_witness: dict[str, Any],
    ) -> native_schemas.NativeStreamWorkerEnrollment:
        """Confirm verified death and rotate authority with an exact replay key."""
        payload = native_schemas.NativeStreamReplacementConfirmRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            challenge_capability=challenge_capability,
            launcher_instance_id=launcher_instance_id,
            launcher_host_id=launcher_host_id,
            launcher_boot_id=launcher_boot_id,
            launcher_process_id=launcher_process_id,
            launcher_start_ticks=launcher_start_ticks,
            new_holder_id=new_holder_id,
            new_holder_capability=new_holder_capability,
            new_holder_event_public_key=new_holder_event_public_key,
            new_supervisor_id=new_supervisor_id,
            new_supervisor_capability=new_supervisor_capability,
            death_witness=native_schemas.NativeStreamProcessDeathWitness.model_validate(
                death_witness
            ),
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/replacements/confirm-death", json=payload
        )
        return native_schemas.NativeStreamWorkerEnrollment.model_validate(
            response.json()
        )

    async def drain_native_stream(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        reason: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Close new source admission while allowing accepted records to finish."""
        payload = native_schemas.NativeStreamControlRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            reason=reason,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/control/drain", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    async def cancel_native_stream(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        reason: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Fence stream writes for an operator cancellation request."""
        payload = native_schemas.NativeStreamControlRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            reason=reason,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/control/cancel", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    async def finalize_native_stream_cancellation(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        enrollment_operation_id: UUID,
        death_witness: dict[str, Any],
        challenge_capability: str | None = None,
    ) -> native_schemas.NativeStreamControlResult:
        """Release cancellation only after verified registered process death."""
        payload = native_schemas.NativeStreamCancelFinalizeRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            enrollment_operation_id=enrollment_operation_id,
            challenge_capability=challenge_capability,
            death_witness=native_schemas.NativeStreamProcessDeathWitness.model_validate(
                death_witness
            ),
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/control/finalize-cancel", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    async def read_native_stream_flow_projection(
        self,
        *,
        flow_run_id: UUID,
    ) -> native_schemas.NativeStreamFlowProjection:
        """Read bounded native progress without record payloads or private authority."""
        response = await self._stream_request(
            "GET",
            "/streams/v1/deployments/{flow_run_id}/projection",
            path_params={"flow_run_id": flow_run_id},
        )
        return native_schemas.NativeStreamFlowProjection.model_validate(response.json())

    async def retire_verified_native_stream_launch(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        enrollment_operation_id: UUID,
        challenge_capability: str,
        death_witness: dict[str, Any],
        error: str,
    ) -> native_schemas.NativeStreamControlResult:
        """Retire a failed launch only with authorized exact process death."""
        payload = native_schemas.NativeStreamVerifiedRetireRequest(
            flow_run_id=flow_run_id,
            operation_id=operation_id,
            expected_generation=expected_generation,
            enrollment_operation_id=enrollment_operation_id,
            challenge_capability=challenge_capability,
            death_witness=native_schemas.NativeStreamProcessDeathWitness.model_validate(
                death_witness
            ),
            error=error,
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/deployments/replacements/retire-verified", json=payload
        )
        return native_schemas.NativeStreamControlResult.model_validate(response.json())

    async def read_native_stream_deployment_plan(
        self,
        *,
        flow_run_id: UUID,
    ) -> native_schemas.NativeStreamDeploymentPlan:
        """Read public immutable deployment and launch witnesses."""
        response = await self._stream_request(
            "GET",
            "/streams/v1/deployments/{flow_run_id}/plan",
            path_params={"flow_run_id": flow_run_id},
        )
        return native_schemas.NativeStreamDeploymentPlan.model_validate(response.json())

    async def read_stream_finite_plan(
        self, *, flow_run_id: UUID
    ) -> schemas.StreamFinitePlan:
        """Read bounded finite witnesses and the durable stream-seal state."""
        response = await self._stream_request(
            "GET", "/streams/v1/finite/plan", params={"flow_run_id": str(flow_run_id)}
        )
        return schemas.StreamFinitePlan.model_validate(response.json())

    async def read_stream_finite_receipt(
        self, *, task_run_id: UUID
    ) -> schemas.StreamFiniteReceipt:
        """Read a durable finite completion result for reconstruction."""
        response = await self._stream_request(
            "GET",
            "/streams/v1/finite/{task_run_id}/receipt",
            path_params={"task_run_id": task_run_id},
        )
        return schemas.StreamFiniteReceipt.model_validate(response.json())

    async def abort_unsealed_stream_execution(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        manifests: list[dict[str, str]],
        error: str,
    ) -> schemas.StreamUnsealedAbortResult:
        """Close an exact unstarted graph after normal terminal flow failure."""
        payload = schemas.StreamUnsealedAbortRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "manifests": manifests,
                "error": error,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/executions/abort-unsealed", json=payload
        )
        return schemas.StreamUnsealedAbortResult.model_validate(response.json())

    async def claim_stream_execution(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        holder_id: UUID,
        holder_capability: str,
        holder_event_public_key: str,
        supervisor_id: UUID,
        supervisor_capability: str,
        expected_generation: int = 0,
        protocol_version: int = 1,
    ) -> schemas.StreamExecutionBinding:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamExecutionClaimRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "holder_id": holder_id,
                "holder_capability": holder_capability,
                "holder_event_public_key": holder_event_public_key,
                "supervisor_id": supervisor_id,
                "supervisor_capability": supervisor_capability,
                "expected_generation": expected_generation,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/executions/claim", json=payload
        )
        return schemas.StreamExecutionBinding.model_validate(response.json())

    async def takeover_stream_execution(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        expected_generation: int,
        expected_holder_id: UUID,
        supervisor_id: UUID,
        supervisor_capability: str,
        new_holder_id: UUID,
        new_holder_capability: str,
        new_holder_event_public_key: str,
        death_witness: dict[str, Any],
    ) -> schemas.StreamExecutionBinding:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamExecutionTakeoverRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "expected_generation": expected_generation,
                "expected_holder_id": expected_holder_id,
                "supervisor_id": supervisor_id,
                "supervisor_capability": supervisor_capability,
                "new_holder_id": new_holder_id,
                "new_holder_capability": new_holder_capability,
                "new_holder_event_public_key": new_holder_event_public_key,
                "death_witness": death_witness,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/executions/takeover", json=payload
        )
        return schemas.StreamExecutionBinding.model_validate(response.json())

    async def seal_stream_manifest(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        manifests: list[dict[str, str]],
        finite_prefix: dict[str, Any] | None = None,
    ) -> schemas.StreamManifestSealResult:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamManifestSealRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "manifests": manifests,
                "finite_prefix": finite_prefix,
            }
        ).model_dump(
            mode="json", exclude={"finite_prefix"} if finite_prefix is None else set()
        )
        response = await self._stream_request(
            "POST", "/streams/v1/executions/seal-manifest", json=payload
        )
        return schemas.StreamManifestSealResult.model_validate(response.json())

    async def prepare_stream_recovery(
        self,
        *,
        flow_run_id: UUID,
        operation_id: UUID,
        manifests: list[dict[str, str]],
        finite_manifests: list[dict[str, Any]] | None = None,
    ) -> schemas.StreamRecoveryResult:
        """Execute an idempotent native execution-authority operation."""
        payload = schemas.StreamRecoveryPrepareRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "operation_id": operation_id,
                "manifests": manifests,
                "finite_manifests": finite_manifests,
            }
        ).model_dump(
            mode="json",
            exclude={"finite_manifests"} if finite_manifests is None else set(),
        )
        response = await self._stream_request(
            "POST", "/streams/v1/executions/prepare-recovery", json=payload
        )
        return schemas.StreamRecoveryResult.model_validate(response.json())

    async def reserve_stream_submission(
        self,
        *,
        flow_run_id: UUID,
        submission_key: str,
        role: Literal["SOURCE", "OPERATOR"],
        protocol: Literal["source-v1", "yield-v2"],
        task_name: str,
        task_key: str,
        task_version: str,
        flow_version: str,
        parameters: dict[str, JsonValue],
        input_key: str | None = None,
        upstream_stream_id: UUID | None = None,
        deployment_id: UUID | None = None,
        deployment_version: str | None = None,
        artifact_digest: str | None = None,
        limits: dict[str, int] | None = None,
        task_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        labels: dict[str, bool | int | float | str] | None = None,
    ) -> schemas.StreamSubmissionManifest:
        """Create or exactly resolve a durable call slot and its native resources."""
        payload = schemas.StreamSubmissionReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "submission_key": submission_key,
                "role": role,
                "protocol": protocol,
                "task_name": task_name,
                "task_key": task_key,
                "task_version": task_version,
                "flow_version": flow_version,
                "parameters": parameters,
                "input_key": input_key,
                "upstream_stream_id": upstream_stream_id,
                "deployment_id": deployment_id,
                "deployment_version": deployment_version,
                "artifact_digest": artifact_digest,
                "limits": limits,
                "task_policy": task_policy or {},
                "tags": tags or [],
                "labels": labels or {},
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/submissions/reserve", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    async def read_stream_submission(
        self, *, flow_run_id: UUID, submission_key: str
    ) -> schemas.StreamSubmissionManifest:
        """Resolve the original resources without process-local identifiers."""
        payload = schemas.StreamSubmissionLookupRequest(
            flow_run_id=flow_run_id, submission_key=submission_key
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/submissions/lookup", json=payload
        )
        return schemas.StreamSubmissionManifest.model_validate(response.json())

    async def create_stream(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamCreateRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/create", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def reserve_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputReserveRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "limits": limits,
                "protocol_version": protocol_version,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/outputs/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def reject_stream_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputRejectRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "error": error,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/outputs/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def fail_stream_task_output(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamOutputFailRequest.model_validate(
            {
                "flow_run_id": flow_run_id,
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "attempt": attempt,
                "error": error,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/outputs/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def open_stream_consumer(
        self,
        *,
        input_stream_id: UUID,
        output_stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        terminal_protocol: str = "yield-v2",
    ) -> schemas.ConsumerBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamConsumerOpenRequest.model_validate(
            {
                "input_stream_id": input_stream_id,
                "output_stream_id": output_stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
                "protocol_version": protocol_version,
                "terminal_protocol": terminal_protocol,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/consumers/open", json=payload
        )
        return schemas.ConsumerBinding.model_validate(response.json())

    async def read_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
    ) -> schemas.StreamDelivery | None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerReadRequest.model_validate(
            {"binding": binding}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/consumers/read", json=payload
        )
        return (
            schemas.StreamDelivery.model_validate(response.json())
            if response.json() is not None
            else None
        )

    async def complete_stream_input(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        record_id: UUID,
        sequence: int,
        delivery_id: UUID,
        kind: Literal["YIELD", "RETURN"],
        value: JsonValue,
        managed_state: dict[str, JsonValue],
    ) -> schemas.StreamCompletionResult:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        token = schemas.DeliveryToken(
            **binding.model_dump(),
            record_id=record_id,
            sequence=sequence,
            delivery_id=delivery_id,
        )
        payload = schemas.StreamConsumerCompleteRequest.model_validate(
            {
                "binding": binding,
                "token": token,
                "kind": kind,
                "value": value,
                "managed_state": managed_state,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/consumers/complete", json=payload
        )
        return schemas.StreamCompletionResult.model_validate(response.json())

    async def finish_stream_consumer(
        self,
        *,
        flow_run_id: UUID,
        consumer_id: UUID,
        input_stream_id: UUID,
        output_stream_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.ConsumerBinding(
            flow_run_id=flow_run_id,
            consumer_id=consumer_id,
            input_stream_id=input_stream_id,
            output_stream_id=output_stream_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamConsumerFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        await self._stream_request("POST", "/streams/v1/consumers/finish", json=payload)

    async def claim_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
    ) -> schemas.SourceBinding:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamSourceClaimRequest.model_validate(
            {
                "stream_id": stream_id,
                "task_run_id": task_run_id,
                "owner_id": owner_id,
                "attempt": attempt,
            }
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/claim", json=payload
        )
        return schemas.SourceBinding.model_validate(response.json())

    async def append_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        position: int,
        value: JsonValue,
    ) -> schemas.StreamEnvelope:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceAppendRequest.model_validate(
            {"binding": binding, "position": position, "value": value}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/append", json=payload
        )
        return schemas.StreamEnvelope.model_validate(response.json())

    async def finish_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        error: str | None = None,
    ) -> None:
        """Execute one idempotent native stream operation."""
        binding = schemas.SourceBinding(
            flow_run_id=flow_run_id,
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=owner_id,
            attempt=attempt,
            owner_epoch=owner_epoch,
        )
        payload = schemas.StreamSourceFinishRequest.model_validate(
            {"binding": binding, "error": error}
        ).model_dump(mode="json")
        await self._stream_request("POST", "/streams/v1/sources/finish", json=payload)

    async def reserve_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        limits: dict[str, int] | None = None,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceReserveRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                limits=limits,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/reserve", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def reject_stream_source(
        self, *, flow_run_id: UUID, stream_id: UUID, task_run_id: UUID, error: str
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceRejectRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                error=error,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/reject", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def fail_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        attempt: int,
        error: str,
    ) -> schemas.StreamInfo:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceFailRequest.model_validate(
            dict(
                flow_run_id=flow_run_id,
                stream_id=stream_id,
                task_run_id=task_run_id,
                attempt=attempt,
                error=error,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/fail", json=payload
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def open_stream_source(
        self,
        *,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        protocol_version: int = 1,
        source_protocol: str = "source-v1",
    ) -> schemas.SourceAdmission:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceOpenRequest.model_validate(
            dict(
                stream_id=stream_id,
                task_run_id=task_run_id,
                owner_id=owner_id,
                attempt=attempt,
                protocol_version=protocol_version,
                source_protocol=source_protocol,
            )
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/sources/open", json=payload
        )
        return schemas.SourceAdmission.model_validate(response.json())

    async def close_stream_source(
        self,
        *,
        flow_run_id: UUID,
        stream_id: UUID,
        task_run_id: UUID,
        owner_id: UUID,
        attempt: int,
        owner_epoch: int,
        expected_position: int,
    ) -> None:
        """Execute a native source-task lifecycle operation."""
        payload = schemas.StreamSourceCloseRequest.model_validate(
            dict(
                binding=schemas.SourceBinding(
                    flow_run_id=flow_run_id,
                    stream_id=stream_id,
                    task_run_id=task_run_id,
                    owner_id=owner_id,
                    attempt=attempt,
                    owner_epoch=owner_epoch,
                ),
                expected_position=expected_position,
            )
        ).model_dump(mode="json")
        await self._stream_request("POST", "/streams/v1/sources/close", json=payload)

    async def release_stream_flow(
        self,
        *,
        flow_run_id: UUID,
    ) -> schemas.StreamReleaseResult:
        """Execute one idempotent native stream operation."""
        payload = schemas.StreamReleaseRequest.model_validate(
            {"flow_run_id": flow_run_id}
        ).model_dump(mode="json")
        response = await self._stream_request(
            "POST", "/streams/v1/release", json=payload
        )
        return schemas.StreamReleaseResult.model_validate(response.json())

    async def read_stream(self, stream_id: UUID) -> schemas.StreamInfo:
        response = await self._stream_request(
            "GET", "/streams/v1/{stream_id}", path_params={"stream_id": stream_id}
        )
        return schemas.StreamInfo.model_validate(response.json())

    async def read_stream_prefix(
        self, stream_id: UUID, *, limit: int
    ) -> list[schemas.StreamEnvelope]:
        response = await self._stream_request(
            "GET",
            "/streams/v1/{stream_id}/prefix",
            path_params={"stream_id": stream_id},
            params={"limit": limit},
        )
        return TypeAdapter(list[schemas.StreamEnvelope]).validate_python(
            response.json()
        )
