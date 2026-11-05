"""The worker declaration protocol remains independent in client/server wheels."""

import pytest
from pydantic import ValidationError

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


@pytest.mark.parametrize("profile", ["bounded-linear-v1", "partitioned-fixed-v1"])
def test_declared_profiles_keep_the_same_secret_free_wire(profile: str):
    payload = {
        "profile": profile,
        "flow_version": "v1",
        "flow_code_digest": "a" * 64,
        "resource_profile": "bounded-default-v1",
        "flow_retries": 2,
    }
    client = client_schemas.NativeStreamDeclaration.model_validate(payload)
    server = server_schemas.NativeStreamDeclaration.model_validate(payload)
    assert client.model_dump(mode="json") == server.model_dump(mode="json")
    assert set(client.model_dump()) == {
        "schema_version",
        "protocol_version",
        "profile",
        "flow_version",
        "flow_code_digest",
        "resource_profile",
        "flow_retries",
        "flow_retry_delay_seconds",
    }


def test_undeclared_native_profiles_remain_refused():
    with pytest.raises(ValidationError):
        client_schemas.NativeStreamDeclaration.model_validate(
            {
                "profile": "unbounded-future-profile",
                "flow_version": "v1",
                "flow_code_digest": "a" * 64,
                "resource_profile": "bounded-default-v1",
                "flow_retries": 2,
            }
        )
