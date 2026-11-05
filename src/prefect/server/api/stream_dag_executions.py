"""Flow-authorized lifecycle and scoped child operations for streaming DAGs."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends

from prefect.server.api.native_deployments import SecretRedactingRoute
from prefect.server.api.stream_execution_dependencies import ExecutionCredentials
from prefect.server.api.stream_subtask_dependencies import SubtaskCredentials
from prefect.server.api.streams import stream_http_error
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.models import stream_dag_executions as models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import stream_dag_executions as s
from prefect.server.schemas import streams as legacy
from prefect.server.utilities.server import PrefectRouter

Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]
router: PrefectRouter = PrefectRouter(
    prefix="/streams/v2/dags", tags=["Streams"], route_class=SecretRedactingRoute
)


@router.post("/{graph_id:uuid}/start")
async def start_stream_dag(
    graph_id: UUID,
    request: s.StreamDAGStartRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamDAGOperationResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.start_stream_dag(
                session, db, graph_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/{graph_id:uuid}/execution")
async def read_stream_dag_execution(
    graph_id: UUID, db: Database, execution: ExecutionCredentials
) -> s.StreamDAGExecution:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_stream_dag_execution(
                session, db, graph_id, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{graph_id:uuid}/abort-preview")
async def abort_stream_dag_preview(
    graph_id: UUID,
    request: s.StreamDAGAbortPreviewRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamDAGOperationResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.abort_stream_dag_preview(
                session, db, graph_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{graph_id:uuid}/cancel")
async def cancel_stream_dag(
    graph_id: UUID,
    request: s.StreamDAGCancelRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamDAGOperationResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.cancel_stream_dag(
                session, db, graph_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{graph_id:uuid}/activate")
async def activate_stream_dag(
    graph_id: UUID,
    request: s.StreamDAGActivateRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamDAGOperationResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.activate_stream_dag(
                session, db, graph_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{graph_id:uuid}/fail")
async def fail_stream_dag(
    graph_id: UUID,
    request: s.StreamDAGFailRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamDAGOperationResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.fail_stream_dag(
                session, db, graph_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/subtasks/{subtask_id:uuid}/bootstrap")
async def read_stream_dag_subtask_bootstrap(
    subtask_id: UUID, db: Database, execution: SubtaskCredentials
) -> s.StreamDAGSubtaskBootstrap:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_stream_dag_subtask_bootstrap(
                session, db, subtask_id, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/bootstrap-ready")
async def mark_stream_dag_subtask_bootstrap_ready(
    subtask_id: UUID,
    request: s.StreamDAGBootstrapReadyRequest,
    db: Database,
    execution: SubtaskCredentials,
) -> s.StreamDAGBootstrapReadyResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.mark_stream_dag_subtask_bootstrap_ready(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/subtasks/{subtask_id:uuid}/activation")
async def read_stream_dag_subtask_activation(
    subtask_id: UUID, db: Database, execution: SubtaskCredentials
) -> s.StreamDAGActivationObservation:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_stream_dag_subtask_activation(
                session, db, subtask_id, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/sources/open")
async def open_stream_dag_source(
    request: s.StreamDAGSourceOpenRequest, db: Database, execution: SubtaskCredentials
) -> legacy.SourceAdmission:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.open_stream_dag_source(session, db, request, execution)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/consumers/open")
async def open_stream_dag_consumer(
    request: s.StreamDAGConsumerOpenRequest, db: Database, execution: SubtaskCredentials
) -> legacy.ConsumerBinding:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.open_stream_dag_consumer(
                session, db, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/consumers/read")
async def read_stream_dag_consumer(
    request: s.StreamDAGConsumerReadRequest, db: Database, execution: SubtaskCredentials
) -> legacy.StreamDelivery | None:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_stream_dag_consumer(
                session, db, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/subtasks/{subtask_id:uuid}/input/status")
async def read_stream_dag_input_status(
    subtask_id: UUID, db: Database, execution: SubtaskCredentials
) -> legacy.StreamInfo:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_stream_dag_input_status(
                session, db, subtask_id, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/subtasks/{subtask_id:uuid}/output/status")
async def read_stream_dag_output_status(
    subtask_id: UUID, db: Database, execution: SubtaskCredentials
) -> legacy.StreamInfo:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_stream_dag_output_status(
                session, db, subtask_id, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/sources/finish")
async def finish_stream_dag_source(
    request: s.StreamDAGSourceFinishRequest, db: Database, execution: SubtaskCredentials
) -> s.StreamDAGProducerFinishReceipt:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.finish_stream_dag_source(
                session, db, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/consumers/finish")
async def finish_stream_dag_consumer(
    request: s.StreamDAGConsumerFinishRequest,
    db: Database,
    execution: SubtaskCredentials,
) -> s.StreamDAGProducerFinishReceipt:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.finish_stream_dag_consumer(
                session, db, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error
