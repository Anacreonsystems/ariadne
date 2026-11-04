"""Real API regressions for worker identity and durable native pool discovery."""

from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

import anyio
import pytest
import sqlalchemy as sa
from httpx import AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.client.base import ServerType
from prefect.client.orchestration._work_pools.client import WorkPoolAsyncClient
from prefect.server import models, schemas
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models.native_deployments import NATIVE_WORK_POOL_MARKER
from tests.server.api.test_native_deployments import _declaration, _native_flow_run


def _replacement_template() -> dict[str, Any]:
    return {
        "job_configuration": {"command": "echo updated"},
        "variables": {"properties": {}},
    }


async def test_ordinary_worker_heartbeat_retains_empty_204_response(
    client: AsyncClient,
    db: PrefectDBInterface,
    work_pool: orm_models.WorkPool,
):
    name = f"ordinary-worker-{uuid4()}"
    response = await client.post(
        f"/work_pools/{work_pool.name}/workers/heartbeat",
        json={"name": name, "heartbeat_interval_seconds": 30},
    )
    assert response.status_code == 204
    assert response.content == b""
    async with db.session_context() as check:
        worker = await check.scalar(
            sa.select(db.Worker).where(
                db.Worker.work_pool_id == work_pool.id,
                db.Worker.name == name,
            )
        )
        assert worker is not None
        assert worker.last_heartbeat_time is not None
        assert worker.heartbeat_interval_seconds == 30


async def test_requested_worker_identity_matches_durable_worker_row(
    client: AsyncClient,
    db: PrefectDBInterface,
    work_pool: orm_models.WorkPool,
):
    name = f"native-worker-{uuid4()}"
    payload = {"name": name, "heartbeat_interval_seconds": 20, "return_id": True}
    response = await client.post(
        f"/work_pools/{work_pool.name}/workers/heartbeat", json=payload
    )
    replay = await client.post(
        f"/work_pools/{work_pool.name}/workers/heartbeat", json=payload
    )
    assert response.status_code == replay.status_code == 200
    assert response.text == replay.text
    worker_id = UUID(response.text)
    async with db.session_context() as check:
        worker = await check.get(db.Worker, worker_id)
        assert worker is not None
        assert worker.work_pool_id == work_pool.id
        assert worker.name == name
        assert worker.heartbeat_interval_seconds == 20


async def test_self_hosted_client_obtains_actual_worker_identity(
    client: AsyncClient,
    db: PrefectDBInterface,
    work_pool: orm_models.WorkPool,
):
    # Exercise the domain client over the stripped-down server test transport;
    # the full prefect_client fixture owns a different server lifecycle.
    domain = WorkPoolAsyncClient(client)
    domain.server_type = ServerType.SERVER
    name = f"self-hosted-worker-{uuid4()}"
    assert (
        await domain.send_worker_heartbeat(work_pool.name, name, get_worker_id=False)
        is None
    )
    worker_id = await domain.send_worker_heartbeat(
        work_pool.name, name, get_worker_id=True
    )
    assert isinstance(worker_id, UUID)
    async with db.session_context() as check:
        worker = await check.get(db.Worker, worker_id)
        assert worker is not None
        assert worker.name == name
        assert worker.work_pool_id == work_pool.id


async def test_active_native_snapshot_restores_marker_on_template_update(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
):
    flow_run, _, pool_id = await _native_flow_run(session, db, deployment)
    async with db.session_context() as check:
        pool = await check.get(db.WorkPool, pool_id)
        assert pool is not None
        pool_name = pool.name
        assert (
            pool.base_job_template["prefect_native_streaming"]
            == NATIVE_WORK_POOL_MARKER
        )
    replacement = _replacement_template()
    response = await client.patch(
        f"/work_pools/{pool_name}", json={"base_job_template": replacement}
    )
    assert response.status_code == 204, response.text
    current = await client.get(f"/work_pools/{pool_name}")
    assert current.status_code == 200
    assert current.json()["base_job_template"] == replacement | {
        "prefect_native_streaming": NATIVE_WORK_POOL_MARKER
    }
    async with db.session_context() as check:
        snapshot = await check.scalar(
            sa.select(db.NativeStreamDeploymentSnapshot).where(
                db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run.id
            )
        )
        assert snapshot is not None
        assert snapshot.state == "ACTIVE"


@pytest.mark.parametrize("invalid", ["conflicting-marker", "empty-template"])
async def test_active_native_snapshot_rejects_discovery_template_drift(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    invalid: str,
):
    await _native_flow_run(session, db, deployment)
    assert deployment.work_queue_id is not None
    async with db.session_context() as check:
        queue = await check.get(db.WorkQueue, deployment.work_queue_id)
        assert queue is not None
        pool = await check.get(db.WorkPool, queue.work_pool_id)
        assert pool is not None
        pool_name, before = pool.name, deepcopy(pool.base_job_template)
    replacement = _replacement_template() if invalid == "conflicting-marker" else {}
    if invalid == "conflicting-marker":
        replacement["prefect_native_streaming"] = {
            "schema_version": 1,
            "profile": "unrecognized-profile",
        }
    response = await client.patch(
        f"/work_pools/{pool_name}", json={"base_job_template": replacement}
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "IDENTITY_CONFLICT"
    current = await client.get(f"/work_pools/{pool_name}")
    assert current.status_code == 200
    assert current.json()["base_job_template"] == before


@pytest.mark.parametrize("replacement", [_replacement_template(), {}])
async def test_ordinary_pool_template_update_does_not_inject_native_marker(
    client: AsyncClient,
    work_pool: orm_models.WorkPool,
    replacement: dict[str, Any],
):
    response = await client.patch(
        f"/work_pools/{work_pool.name}", json={"base_job_template": replacement}
    )
    assert response.status_code == 204, response.text
    current = await client.get(f"/work_pools/{work_pool.name}")
    assert current.status_code == 200
    assert current.json()["base_job_template"] == replacement
    assert "prefect_native_streaming" not in current.json()["base_job_template"]


@pytest.mark.parametrize("retry_delay", [0.05, 1.75, 5])
async def test_ordinary_flow_retry_policy_preserves_fractional_and_integer_delay(
    client: AsyncClient,
    flow: orm_models.Flow,
    retry_delay: float,
):
    created = await client.post(
        "/flow_runs/",
        json={
            "flow_id": str(flow.id),
            "name": f"ordinary-fractional-retry-{uuid4()}",
            "empirical_policy": {"retries": 2, "retry_delay": retry_delay},
        },
    )
    assert created.status_code == 201, created.text
    run_id = UUID(created.json()["id"])
    current = await client.get(f"/flow_runs/{run_id}")
    assert current.status_code == 200
    assert current.json()["empirical_policy"]["retry_delay"] == retry_delay
    assert current.json()["empirical_policy"]["retries"] == 2


@pytest.mark.parametrize("first_writer", ["bind", "template-update"])
async def test_concurrent_native_bind_preserves_pool_discovery_marker(
    client: AsyncClient,
    session: AsyncSession,
    db: PrefectDBInterface,
    deployment: orm_models.Deployment,
    first_writer: str,
):
    if db.dialect.name != "postgresql":
        pytest.skip("The pool row-lock interleaving requires PostgreSQL")
    deployment.version = "deployment-v1"
    deployment.job_variables = {
        "prefect_native_streaming": _declaration().model_dump(mode="json")
    }
    await session.commit()
    assert deployment.work_queue_id is not None
    async with db.session_context() as check:
        queue = await check.get(db.WorkQueue, deployment.work_queue_id)
        assert queue is not None
        pool = await check.get(db.WorkPool, queue.work_pool_id)
        assert pool is not None
        pool_id, pool_name = pool.id, pool.name
    replacement = _replacement_template()
    responses: list[Response] = []

    async def write_second_transaction() -> None:
        if first_writer == "bind":
            response = await client.patch(
                f"/work_pools/{pool_name}", json={"base_job_template": replacement}
            )
        else:
            response = await client.post(
                f"/deployments/{deployment.id}/create_flow_run", json={}
            )
        responses.append(response)

    async with db.session_context(begin_transaction=True) as first:
        blocker_pid = await first.scalar(sa.text("SELECT pg_backend_pid()"))
        assert isinstance(blocker_pid, int)
        if first_writer == "bind":
            await models.flow_runs.create_flow_run(
                session=first,
                flow_run=schemas.core.FlowRun(
                    flow_id=deployment.flow_id,
                    deployment_id=deployment.id,
                    deployment_version=deployment.version,
                    work_queue_id=deployment.work_queue_id,
                    state=schemas.states.Scheduled(),
                ),
            )
        else:
            updated = await models.workers.update_work_pool(
                session=first,
                work_pool_id=pool_id,
                work_pool=schemas.actions.WorkPoolUpdate(base_job_template=replacement),
            )
            assert updated
        async with anyio.create_task_group() as group:
            group.start_soon(write_second_transaction)
            # Wait for a real PostgreSQL lock wait, then release the first
            # writer. No clock manipulation or implementation stub supplies
            # either the ACTIVE snapshot or the replacement template.
            with anyio.fail_after(20):
                while True:
                    async with db.session_context() as observer:
                        blocked = await observer.scalar(
                            sa.text(
                                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                                "WHERE datname=current_database() "
                                "AND :blocker = ANY(pg_blocking_pids(pid)))"
                            ),
                            {"blocker": blocker_pid},
                        )
                    if blocked:
                        break
                    await anyio.sleep(0.01)
            await first.commit()
    assert len(responses) == 1
    expected_status = 204 if first_writer == "bind" else 201
    assert responses[0].status_code == expected_status, responses[0].text
    current = await client.get(f"/work_pools/{pool_name}")
    assert current.status_code == 200
    assert current.json()["base_job_template"] == replacement | {
        "prefect_native_streaming": NATIVE_WORK_POOL_MARKER
    }
    async with db.session_context() as check:
        assert (
            await check.scalar(
                sa.select(sa.func.count())
                .select_from(db.NativeStreamDeploymentSnapshot)
                .where(
                    db.NativeStreamDeploymentSnapshot.work_pool_id == pool_id,
                    db.NativeStreamDeploymentSnapshot.state == "ACTIVE",
                )
            )
            == 1
        )
