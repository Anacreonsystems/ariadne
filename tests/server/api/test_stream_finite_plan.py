"""Read-only finite-plan projection for native execution recovery."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from prefect.server import schemas
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import flow_runs as flow_run_models
from prefect.server.orchestration.core_policy import CoreFlowPolicy

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def _postgres_17(db: PrefectDBInterface) -> bool:
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    return db.dialect.name == "postgresql" and bool(version) and version[0] == 17


async def _recoverable_flow(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
) -> None:
    async with db.session_context(begin_transaction=True) as session:
        row = await session.get(db.FlowRun, flow_run.id)
        assert row is not None
        row.flow_version = "flow-v1"
        row.empirical_policy = schemas.core.FlowRunPolicy(retries=2)
        await flow_run_models.set_flow_run_state(
            session=session,
            flow_run_id=flow_run.id,
            state=schemas.states.Pending(),
            flow_policy=CoreFlowPolicy,
        )


def _claim(flow_run_id: UUID) -> dict[str, object]:
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


def _headers(
    claim: dict[str, object],
    *,
    generation: int = 1,
    holder_id: object | None = None,
    capability: object | None = None,
) -> dict[str, str]:
    return {
        "X-Prefect-Stream-Execution-Generation": str(generation),
        "X-Prefect-Stream-Execution-Holder": str(
            holder_id if holder_id is not None else claim["holder_id"]
        ),
        "X-Prefect-Stream-Execution-Capability": str(
            capability if capability is not None else claim["holder_capability"]
        ),
    }


def _finite_request(flow_run_id: UUID, phase: str) -> dict[str, object]:
    name = "prepare" if phase == "PREFIX" else "summary"
    return {
        "flow_run_id": str(flow_run_id),
        "phase": phase,
        "submission_key": f"documents.{name}",
        "task_name": name,
        "task_key": f"{name}-v1",
        "artifact_digest": ("a" if phase == "PREFIX" else "c") * 64,
        "task_version": "task-v1",
        "flow_version": "flow-v1",
        "parameters": {"ref": f"object://documents/{name}"},
        "task_policy": {"retries": 2},
    }


def _source_request(flow_run_id: UUID) -> dict[str, object]:
    return {
        "flow_run_id": str(flow_run_id),
        "submission_key": "source.documents",
        "role": "SOURCE",
        "protocol": "source-v1",
        "task_name": "documents",
        "task_key": "documents-v1",
        "artifact_digest": "b" * 64,
        "task_version": "task-v1",
        "flow_version": "flow-v1",
        "parameters": {"ref": "object://documents"},
        "task_policy": {"retries": 2},
    }


async def _enroll(
    client: httpx.AsyncClient,
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
) -> tuple[dict[str, object], dict[str, str]]:
    await _recoverable_flow(db, flow_run)
    claim = _claim(flow_run.id)
    response = await client.post("/streams/v1/executions/claim", json=claim)
    response.raise_for_status()
    headers = _headers(claim)
    running = await client.post(
        f"/flow_runs/{flow_run.id}/set_state",
        json={"state": {"type": "RUNNING", "name": "Running"}},
        headers=headers,
    )
    assert running.status_code == 201
    return claim, headers


async def _set_task_state(
    client: httpx.AsyncClient,
    task_run_id: str,
    state_type: str,
    headers: dict[str, str],
) -> None:
    response = await client.post(
        f"/task_runs/{task_run_id}/set_state",
        json={"state": {"type": state_type, "name": state_type.title()}},
        headers=headers,
    )
    assert response.status_code == 201, response.text


async def _prefix_with_receipt(
    client: httpx.AsyncClient,
    flow_run_id: UUID,
    headers: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    response = await client.post(
        "/streams/v1/finite/reserve",
        json=_finite_request(flow_run_id, "PREFIX"),
        headers=headers,
    )
    response.raise_for_status()
    prefix = response.json()
    await _set_task_state(client, prefix["task_run_id"], "RUNNING", headers)
    completed = await client.post(
        "/streams/v1/finite/complete",
        json={
            "flow_run_id": str(flow_run_id),
            "task_run_id": prefix["task_run_id"],
            "operation_id": str(uuid4()),
            "run_count": 1,
            "result": {"prepared": True},
        },
        headers=headers,
    )
    completed.raise_for_status()
    await _set_task_state(client, prefix["task_run_id"], "COMPLETED", headers)
    return prefix, completed.json()


async def _sealed_source_and_tail(
    client: httpx.AsyncClient,
    flow_run_id: UUID,
    headers: dict[str, str],
    prefix: dict[str, Any],
    receipt: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    source_response = await client.post(
        "/streams/v1/submissions/reserve",
        json=_source_request(flow_run_id),
        headers=headers,
    )
    source_response.raise_for_status()
    source = source_response.json()
    seal = await client.post(
        "/streams/v1/executions/seal-manifest",
        json={
            "flow_run_id": str(flow_run_id),
            "operation_id": str(uuid4()),
            "manifests": [
                {
                    "submission_key": source["submission_key"],
                    "manifest_digest": source["manifest_digest"],
                }
            ],
            "finite_prefix": {
                "submission_key": prefix["submission_key"],
                "manifest_digest": prefix["manifest_digest"],
                "phase": "PREFIX",
                "task_run_id": prefix["task_run_id"],
                "result_digest": receipt["result_digest"],
            },
        },
        headers=headers,
    )
    seal.raise_for_status()
    await _set_task_state(client, source["task_run_id"], "RUNNING", headers)
    opened = await client.post(
        "/streams/v1/sources/open",
        json={
            "stream_id": source["stream_id"],
            "task_run_id": source["task_run_id"],
            "owner_id": str(uuid4()),
            "attempt": 1,
            "protocol_version": 1,
            "source_protocol": "source-v1",
        },
        headers=headers,
    )
    opened.raise_for_status()
    binding = opened.json()["binding"]
    appended = await client.post(
        "/streams/v1/sources/append",
        json={"binding": binding, "position": 1, "value": {"document": 200}},
        headers=headers,
    )
    appended.raise_for_status()
    closed = await client.post(
        "/streams/v1/sources/close",
        json={"binding": binding, "expected_position": 1},
        headers=headers,
    )
    assert closed.status_code == 204
    await _set_task_state(client, source["task_run_id"], "COMPLETED", headers)
    tail_response = await client.post(
        "/streams/v1/finite/reserve",
        json=_finite_request(flow_run_id, "TAIL"),
        headers=headers,
    )
    tail_response.raise_for_status()
    return source, tail_response.json()


async def test_finite_plan_fails_closed_on_sqlite(
    client: httpx.AsyncClient,
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
):
    if await _postgres_17(db):
        pytest.skip("SQLite capability refusal is covered by its own profile")
    response = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=_headers(_claim(flow_run.id)),
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNSUPPORTED_BACKEND"


async def test_finite_plan_requires_holder_and_projects_receipt_state(
    client: httpx.AsyncClient,
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
):
    if not await _postgres_17(db):
        pytest.skip("Finite plan authority requires PostgreSQL 17")
    _, headers = await _enroll(client, db, flow_run)
    missing = await client.get(
        "/streams/v1/finite/plan", params={"flow_run_id": str(flow_run.id)}
    )
    assert missing.status_code == 409
    assert missing.json()["detail"]["code"] == "EXECUTION_BINDING_REQUIRED"

    empty = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    empty.raise_for_status()
    assert empty.json() == {
        "flow_run_id": str(flow_run.id),
        "generation": 1,
        "manifest_sealed": False,
        "items": [],
    }

    prefix_response = await client.post(
        "/streams/v1/finite/reserve",
        json=_finite_request(flow_run.id, "PREFIX"),
        headers=headers,
    )
    prefix_response.raise_for_status()
    prefix = prefix_response.json()
    pending = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    pending.raise_for_status()
    assert pending.json()["items"] == [prefix]

    _, receipt = await _prefix_with_receipt(client, flow_run.id, headers)
    completed = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    completed.raise_for_status()
    item = completed.json()["items"][0]
    assert item["task_run_id"] == prefix["task_run_id"]
    assert item["result_state"] == "COMPLETED"
    assert set(item) == {
        "flow_run_id",
        "phase",
        "submission_key",
        "manifest_digest",
        "task_run_id",
        "dynamic_key",
        "state",
        "result_state",
        "created_at",
    }
    assert receipt["result"] == {"prepared": True}


async def test_finite_plan_reports_sealed_tail_to_new_recovering_holder(
    client: httpx.AsyncClient,
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
):
    if not await _postgres_17(db):
        pytest.skip("Finite plan authority requires PostgreSQL 17")
    claim, headers = await _enroll(client, db, flow_run)
    prefix, receipt = await _prefix_with_receipt(client, flow_run.id, headers)
    _, tail = await _sealed_source_and_tail(
        client, flow_run.id, headers, prefix, receipt
    )
    current = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    current.raise_for_status()
    assert current.json()["manifest_sealed"] is True
    assert [item["phase"] for item in current.json()["items"]] == ["PREFIX", "TAIL"]
    assert current.json()["items"][1] == tail

    takeover = {
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
    }
    taken = await client.post("/streams/v1/executions/takeover", json=takeover)
    taken.raise_for_status()
    stale = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "STALE_EXECUTION"

    recovered_headers = _headers(
        claim,
        generation=2,
        holder_id=takeover["new_holder_id"],
        capability=takeover["new_holder_capability"],
    )
    recovered = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=recovered_headers,
    )
    recovered.raise_for_status()
    assert recovered.json()["generation"] == 2
    assert recovered.json()["manifest_sealed"] is True
    assert recovered.json()["items"] == current.json()["items"]


async def test_finite_plan_refuses_released_horizon(
    client: httpx.AsyncClient,
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
):
    if not await _postgres_17(db):
        pytest.skip("Finite plan authority requires PostgreSQL 17")
    _, headers = await _enroll(client, db, flow_run)
    prefix, receipt = await _prefix_with_receipt(client, flow_run.id, headers)
    _, tail = await _sealed_source_and_tail(
        client, flow_run.id, headers, prefix, receipt
    )
    await _set_task_state(client, tail["task_run_id"], "RUNNING", headers)
    completed = await client.post(
        "/streams/v1/finite/complete",
        json={
            "flow_run_id": str(flow_run.id),
            "task_run_id": tail["task_run_id"],
            "operation_id": str(uuid4()),
            "run_count": 1,
            "result": {"processed": 1},
        },
        headers=headers,
    )
    completed.raise_for_status()
    await _set_task_state(client, tail["task_run_id"], "COMPLETED", headers)
    flow_completed = await client.post(
        f"/flow_runs/{flow_run.id}/set_state",
        json={"state": {"type": "COMPLETED", "name": "Completed"}},
        headers=headers,
    )
    assert flow_completed.status_code == 201
    released = await client.post(
        "/streams/v1/release",
        json={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    released.raise_for_status()
    expired = await client.get(
        "/streams/v1/finite/plan",
        params={"flow_run_id": str(flow_run.id)},
        headers=headers,
    )
    assert expired.status_code == 410
    assert expired.json()["detail"]["code"] == "RECOVERY_HORIZON_EXPIRED"
