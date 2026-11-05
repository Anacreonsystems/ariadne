"""Immutable public deployment capability, separate from execution secrets."""

from __future__ import annotations

# This coordinator composes internal storage/executor boundaries.
# pyright: reportPrivateUsage=false
from typing import Any

from prefect._internal.stream_code import native_task_code_identity
from prefect._internal.stream_execution import get_stream_execution
from prefect.streams import StreamProtocolError

NATIVE_DECLARATION_KEY = "prefect_native_streaming"


def native_deployment_declaration(flow: Any) -> dict[str, Any] | None:
    config = getattr(flow, "streaming", None)
    if config is None:
        return None
    if not flow.version:
        raise StreamProtocolError(
            "A native deployment requires an explicit Flow version"
        )
    if type(flow.retries) is not int or not 1 <= flow.retries <= 63:
        raise StreamProtocolError(
            "Native deployment recovery requires 1..63 configured Flow retries"
        )
    identity = native_task_code_identity(flow.fn)
    return {
        "schema_version": 1,
        "protocol_version": config.protocol_version,
        "profile": config.profile,
        "resource_profile": config.resource_profile,
        "flow_version": flow.version,
        "flow_retries": flow.retries,
        "flow_retry_delay_seconds": flow.retry_delay_seconds,
        "flow_code_digest": identity.artifact_digest,
    }


async def validate_native_flow_launch(flow: Any, flow_run: Any) -> None:
    declaration = native_deployment_declaration(flow)
    if declaration is None:
        execution = get_stream_execution()
        if (
            execution is not None
            and flow_run is not None
            and flow_run.deployment_id is not None
        ):
            from prefect.client.orchestration._streams.client import StreamRequestError
            from prefect.client.stream_storage import PrefectStreamStorage

            try:
                await PrefectStreamStorage(flow_run.id)._request(
                    "read_native_stream_deployment_plan", flow_run_id=flow_run.id
                )
            except StreamRequestError as exc:
                if exc.code != "NOT_FOUND":
                    raise
            else:
                raise StreamProtocolError(
                    "Native deployment requires its declared Flow capability"
                )
        return
    execution = get_stream_execution()
    if (
        execution is None
        or flow_run is None
        or execution.flow_run_id != flow_run.id
        or flow_run.deployment_id is None
    ):
        raise StreamProtocolError(
            "A declared native Flow requires its enrolled worker deployment"
        )
    from prefect.client.stream_storage import PrefectStreamStorage

    plan = await PrefectStreamStorage(flow_run.id)._request(
        "read_native_stream_deployment_plan", flow_run_id=flow_run.id
    )
    if (
        plan.declaration.model_dump(mode="json") != declaration
        or plan.current_launch is None
        or plan.current_launch.generation != execution.generation
        or plan.current_launch.state != "PROCESS_REGISTERED"
    ):
        raise StreamProtocolError(
            "Native deployment code, capability or launch identity differs"
        )
