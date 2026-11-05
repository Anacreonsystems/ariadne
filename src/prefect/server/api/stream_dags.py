"""Publish immutable DAG topology without enabling DAG execution."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends

from prefect.server.api.native_deployments import SecretRedactingRoute
from prefect.server.api.stream_execution_dependencies import ExecutionCredentials
from prefect.server.api.streams import stream_http_error
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.models import stream_dags as models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import stream_dags as s
from prefect.server.utilities.server import PrefectRouter

Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]
router: PrefectRouter = PrefectRouter(
    prefix="/streams/v2/dags", tags=["Streams"], route_class=SecretRedactingRoute
)


@router.post("/seal")
async def seal_dag(
    request: s.StreamDAGSealRequest, db: Database, execution: ExecutionCredentials
) -> s.StreamDAGPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.seal_dag(session, db, request, execution)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/{graph_id:uuid}")
async def read_dag(
    graph_id: UUID, db: Database, execution: ExecutionCredentials
) -> s.StreamDAGPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_dag_plan(session, db, graph_id, execution)
    except NativeStreamError as error:
        raise stream_http_error(error) from error
