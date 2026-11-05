"""Exact route allowlist for physical-process credentials, never Basic fallback."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Request
from pydantic import ValidationError

from prefect.server.api.stream_execution_dependencies import execution_credentials
from prefect.server.schemas.stream_subtasks import StreamSubtaskCredentials
from prefect.server.schemas.streams import StreamExecutionCredentials


def parse_subtask_credentials(request: Request) -> StreamSubtaskCredentials:
    authorization = request.headers.get("authorization", "")
    if not authorization.startswith("PrefectSubtask ") or any(
        name.lower().startswith("x-prefect-stream-execution-")
        for name in request.headers
    ):
        raise HTTPException(401, detail="Invalid subtask authentication")
    try:
        return StreamSubtaskCredentials(
            grant_id=UUID(request.headers.get("X-Prefect-Subtask-Grant", "")),
            capability=authorization.removeprefix("PrefectSubtask "),
        )
    except (ValueError, ValidationError) as error:
        raise HTTPException(401, detail="Invalid subtask authentication") from error


async def subtask_credentials(request: Request) -> StreamSubtaskCredentials:
    return parse_subtask_credentials(request)


async def data_credentials(
    request: Request,
    execution: Annotated[
        StreamExecutionCredentials | None, Depends(execution_credentials)
    ],
) -> StreamExecutionCredentials | StreamSubtaskCredentials | None:
    if request.headers.get("authorization", "").startswith("PrefectSubtask"):
        return parse_subtask_credentials(request)
    return execution


SubtaskCredentials = Annotated[StreamSubtaskCredentials, Depends(subtask_credentials)]
DataExecutionCredentials = Annotated[
    StreamExecutionCredentials | StreamSubtaskCredentials | None,
    Depends(data_credentials),
]
