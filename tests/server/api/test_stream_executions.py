"""HTTP authority checks for enrolled native stream executions."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
import sqlalchemy as sa

from prefect.server import schemas
from prefect.server.database import PrefectDBInterface
from prefect.server.models import flow_runs as flow_run_models
from prefect.server.orchestration.core_policy import CoreFlowPolicy

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def _postgres_17(db: PrefectDBInterface) -> bool:
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    return db.dialect.name == "postgresql" and bool(version) and version[0] == 17


async def _recoverable_flow(db: PrefectDBInterface, flow_run) -> None:
    async with db.session_context(begin_transaction=True) as session:
        row = await session.get(db.FlowRun, flow_run.id)
        row.flow_version = "flow-v1"
        row.empirical_policy = schemas.core.FlowRunPolicy(retries=2)
        await flow_run_models.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Pending(),
            flow_policy=CoreFlowPolicy,
        )


def _claim(flow_run_id):
    return {
        "flow_run_id": str(flow_run_id),
        "operation_id": str(uuid4()),
        "expected_generation": 0,
        "holder_id": str(uuid4()),
        "holder_capability": "11" * 32,
        "holder_event_public_key": "22" * 32,
        "supervisor_id": str(uuid4()),
        "supervisor_capability": "33" * 32,
        "protocol_version": 1,
    }


def _headers(claim: dict[str, object]) -> dict[str, str]:
    return {
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": str(claim["holder_id"]),
        "X-Prefect-Stream-Execution-Capability": str(claim["holder_capability"]),
    }


def _source(flow_run_id):
    return {
        "flow_run_id": str(flow_run_id),
        "submission_key": "source.documents",
        "role": "SOURCE",
        "protocol": "source-v1",
        "task_name": "documents",
        "task_key": "documents-key",
        "task_version": "task-v1",
        "flow_version": "flow-v1",
        "parameters": {"connector": "documents"},
        "task_policy": {"retries": 2},
    }


async def test_execution_claim_fails_closed_on_sqlite(client, db, flow_run):
    if await _postgres_17(db):
        pytest.skip("SQLite capability refusal is covered by its own profile")
    response = await client.post(
        "/streams/v1/executions/claim", json=_claim(flow_run.id)
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNSUPPORTED_BACKEND"
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.StreamExecutionAuthority.id))
            )
            == 0
        )


async def test_invalid_complete_execution_headers_are_typed_and_secret_free(
    client, flow_run
):
    response = await client.post(
        "/streams/v1/create",
        json={"flow_run_id": str(flow_run.id), "stream_id": str(uuid4())},
        headers={
            "X-Prefect-Stream-Execution-Generation": "65",
            "X-Prefect-Stream-Execution-Holder": str(uuid4()),
            "X-Prefect-Stream-Execution-Capability": "secret-that-must-not-echo",
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"] == {
        "code": "PROTOCOL_ERROR",
        "message": "Execution binding headers are invalid",
        "retryable": False,
    }
    assert "secret-that-must-not-echo" not in response.text


async def test_execution_claim_rejects_boolean_generation_before_storage(
    client, db, flow_run
):
    claim = _claim(flow_run.id)
    claim["expected_generation"] = False
    response = await client.post("/streams/v1/executions/claim", json=claim)
    assert response.status_code == 422
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count(db.StreamExecutionAuthority.id))
            )
            == 0
        )


async def test_unenrolled_task_create_and_metadata_update_are_unchanged(
    client, flow_run
):
    created = await client.post(
        "/task_runs/",
        json={
            "flow_run_id": str(flow_run.id),
            "task_key": "ordinary-task",
            "dynamic_key": "ordinary-slot",
        },
    )
    assert created.status_code == 201
    task_run_id = created.json()["id"]
    renamed = await client.patch(
        f"/task_runs/{task_run_id}", json={"name": "ordinary-renamed"}
    )
    assert renamed.status_code == 204
    observed = await client.get(f"/task_runs/{task_run_id}")
    assert observed.status_code == 200
    assert observed.json()["name"] == "ordinary-renamed"


async def test_http_seal_fences_lifecycle_and_unkeyed_mutations(client, db, flow_run):
    if not await _postgres_17(db):
        pytest.skip("Native stream execution authority requires PostgreSQL 17")
    await _recoverable_flow(db, flow_run)
    claim = _claim(flow_run.id)
    claimed = await client.post("/streams/v1/executions/claim", json=claim)
    assert claimed.status_code == 200
    headers = _headers(claim)

    source = await client.post(
        "/streams/v1/submissions/reserve", json=_source(flow_run.id), headers=headers
    )
    source.raise_for_status()
    witness = {
        "submission_key": source.json()["submission_key"],
        "manifest_digest": source.json()["manifest_digest"],
    }
    running = {"state": {"type": "RUNNING", "name": "Running"}}
    missing_binding = await client.post(
        f"/flow_runs/{flow_run.id}/set_state", json=running
    )
    assert missing_binding.status_code == 409
    assert missing_binding.json()["detail"]["code"] == "EXECUTION_BINDING_REQUIRED"
    constructing = await client.post(
        f"/flow_runs/{flow_run.id}/set_state", json=running, headers=headers
    )
    assert constructing.status_code == 201

    seal = {
        "flow_run_id": str(flow_run.id),
        "operation_id": str(uuid4()),
        "manifests": [witness],
    }
    first = await client.post(
        "/streams/v1/executions/seal-manifest", json=seal, headers=headers
    )
    second = await client.post(
        "/streams/v1/executions/seal-manifest", json=seal, headers=headers
    )
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()

    task_run_id = source.json()["task_run_id"]
    task_running = await client.post(
        f"/task_runs/{task_run_id}/set_state",
        json={"state": {"type": "RUNNING", "name": "Running"}},
        headers=headers,
    )
    assert task_running.status_code == 201
    premature_task = await client.post(
        f"/task_runs/{task_run_id}/set_state",
        json={"state": {"type": "COMPLETED", "name": "Completed"}},
        headers=headers,
    )
    assert premature_task.status_code == 409
    assert premature_task.json()["detail"]["code"] == "TERMINAL_CONFLICT"

    unkeyed = await client.post(
        "/streams/v1/create",
        json={"flow_run_id": str(flow_run.id), "stream_id": str(uuid4())},
        headers=headers,
    )
    assert unkeyed.status_code == 409
    assert unkeyed.json()["detail"]["code"] == "IDENTITY_CONFLICT"

    premature_completion = await client.post(
        f"/flow_runs/{flow_run.id}/set_state",
        json={"state": {"type": "COMPLETED", "name": "Completed"}},
        headers=headers,
    )
    assert premature_completion.status_code == 409
    assert premature_completion.json()["detail"]["code"] == "TERMINAL_CONFLICT"

    forced = await client.post(
        f"/flow_runs/{flow_run.id}/set_state",
        json={"state": {"type": "FAILED", "name": "Failed"}, "force": True},
        headers=headers,
    )
    assert forced.status_code == 422
    assert forced.json()["detail"]["code"] == "PROTOCOL_ERROR"

    renamed = await client.patch(
        f"/task_runs/{task_run_id}",
        json={"name": "current-holder-name"},
        headers=headers,
    )
    assert renamed.status_code == 204
    refused_creation = await client.post(
        "/task_runs/",
        json={
            "flow_run_id": str(flow_run.id),
            "task_key": "unmanifested-task",
            "dynamic_key": "unmanifested-slot",
        },
        headers=headers,
    )
    assert refused_creation.status_code == 409
    assert refused_creation.json()["detail"]["code"] == "UNSUPPORTED_TOPOLOGY"

    takeover = await client.post(
        "/streams/v1/executions/takeover",
        json={
            "flow_run_id": str(flow_run.id),
            "operation_id": str(uuid4()),
            "expected_generation": 1,
            "expected_holder_id": claim["holder_id"],
            "supervisor_id": claim["supervisor_id"],
            "supervisor_capability": claim["supervisor_capability"],
            "new_holder_id": str(uuid4()),
            "new_holder_capability": "44" * 32,
            "new_holder_event_public_key": "55" * 32,
            "death_witness": {
                "launcher_instance_id": str(uuid4()),
                "child_process_id": 1234,
                "exit_code": 77,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "kind": "WAITPID",
            },
        },
    )
    assert takeover.status_code == 200
    stale_rename = await client.patch(
        f"/task_runs/{task_run_id}",
        json={"name": "stale-holder-name"},
        headers=headers,
    )
    assert stale_rename.status_code == 409
    assert stale_rename.json()["detail"]["code"] == "STALE_EXECUTION"
    observed = await client.get(f"/task_runs/{task_run_id}")
    assert observed.status_code == 200
    assert observed.json()["name"] == "current-holder-name"
