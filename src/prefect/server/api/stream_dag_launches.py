"""Trusted worker launch journal and exact physical stop controls for DAGs."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends

from prefect.server.api.native_deployments import SecretRedactingRoute
from prefect.server.api.stream_workers import trusted_worker
from prefect.server.api.streams import stream_http_error
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.models import stream_dag_launches as models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import stream_dag_executions as s
from prefect.server.utilities.server import PrefectRouter

Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]
router: PrefectRouter = PrefectRouter(
    prefix="/streams/v2/dags/workers",
    tags=["Streams"],
    route_class=SecretRedactingRoute,
    dependencies=[Depends(trusted_worker)],
)


@router.post("/{worker_id:uuid}/poll")
async def poll_worker_stream_dag_subtasks(
    worker_id: UUID, request: s.StreamDAGWorkerPollRequest, db: Database
) -> list[s.StreamDAGWorkerAssignment]:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.poll_worker_stream_dag_subtasks(
                session, db, worker_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/graphs/{graph_id:uuid}/startup-abort")
async def abort_worker_stream_dag_startup(
    worker_id: UUID,
    graph_id: UUID,
    request: s.StreamDAGWorkerStartupAbortRequest,
    db: Database,
) -> s.StreamDAGWorkerStartupAbortResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.abort_worker_stream_dag_startup(
                session, db, worker_id, graph_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/launches/prepare")
async def prepare_worker_stream_dag_launch(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamDAGLaunchPrepareRequest,
    db: Database,
) -> s.StreamDAGLaunchOperationResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.prepare_worker_stream_dag_launch(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post(
    "/{worker_id:uuid}/subtasks/{subtask_id:uuid}/launches/{launch_attempt_id:uuid}/record"
)
async def record_worker_stream_dag_launch(
    worker_id: UUID,
    subtask_id: UUID,
    launch_attempt_id: UUID,
    request: s.StreamDAGLaunchRecordRequest,
    db: Database,
) -> s.StreamDAGLaunchOperationResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.record_worker_stream_dag_launch(
                session, db, worker_id, subtask_id, launch_attempt_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post(
    "/{worker_id:uuid}/subtasks/{subtask_id:uuid}/launches/{launch_attempt_id:uuid}/close"
)
async def close_worker_stream_dag_launch(
    worker_id: UUID,
    subtask_id: UUID,
    launch_attempt_id: UUID,
    request: s.StreamDAGLaunchCloseRequest,
    db: Database,
) -> s.StreamDAGLaunchCloseReceipt:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.close_worker_stream_dag_launch(
                session, db, worker_id, subtask_id, launch_attempt_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/stop")
async def stop_worker_stream_dag_subtask(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamDAGWorkerStopRequest,
    db: Database,
) -> s.StreamDAGStopReceipt:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.stop_worker_stream_dag_subtask(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error
