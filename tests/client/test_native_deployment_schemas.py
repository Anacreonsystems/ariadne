"""The worker declaration protocol remains independent in client/server wheels."""

import pytest

from prefect.client.schemas import native_deployments as client_schemas
from prefect.server.schemas import native_deployments as server_schemas


@pytest.mark.parametrize(
    "name",
    [
        "NativeStreamDeclaration",
        "NativeStreamWorkerEnrollRequest",
        "NativeStreamWorkerEnrollment",
        "NativeStreamProcessRegistrationRequest",
        "NativeStreamLaunchProjection",
        "NativeStreamDeploymentPlan",
        "NativeStreamTaskProjection",
        "NativeStreamFiniteProjection",
        "NativeStreamFlowProjection",
        "NativeStreamRecoveryCandidate",
        "NativeStreamLaunchHeartbeatRequest",
        "NativeStreamLaunchHeartbeat",
        "NativeStreamReplacementAuthorizeRequest",
        "NativeStreamReplacementAuthorization",
        "NativeStreamProcessDeathWitness",
        "NativeStreamReplacementConfirmRequest",
        "NativeStreamControlRequest",
        "NativeStreamControlResult",
        "NativeStreamUnstartedRetireRequest",
        "NativeStreamCancelFinalizeRequest",
        "NativeStreamVerifiedRetireRequest",
    ],
)
def test_native_deployment_wire_models_match(name: str):
    assert (
        getattr(client_schemas, name).model_json_schema()
        == getattr(server_schemas, name).model_json_schema()
    )
