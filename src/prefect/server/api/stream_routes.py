"""Authenticated routed terminal endpoints, fenced until DAG execution exists."""

from typing import Annotated

from fastapi import Depends

from prefect.server.api.native_deployments import SecretRedactingRoute
from prefect.server.api.stream_subtask_dependencies import DataExecutionCredentials
from prefect.server.api.streams import stream_http_error
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.models import stream_routes as models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import stream_routes as s
from prefect.server.utilities.server import PrefectRouter

Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]
router: PrefectRouter = PrefectRouter(
    prefix="/streams/v2/dags", tags=["Streams"], route_class=SecretRedactingRoute
)


@router.post("/sources/append")
async def append_source(
    request: s.StreamDAGSourceAppendRequest,
    db: Database,
    execution: DataExecutionCredentials,
) -> s.StreamDAGRouteOutcome:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.require_dag_route_execution(
                session, db, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/consumers/complete")
async def complete_consumer(
    request: s.StreamDAGConsumerCompleteRequest,
    db: Database,
    execution: DataExecutionCredentials,
) -> s.StreamDAGRouteOutcome:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.require_dag_route_execution(
                session, db, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error
