"""Physical credentials never become general API authority."""

import base64
from collections.abc import AsyncGenerator, Iterator
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from pydantic import ValidationError

from prefect.server.api.server import create_api_app, create_app
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.events.schemas.events import Event
from prefect.server.models import stream_subtasks as m
from prefect.server.models.stream_executions import claim_execution
from prefect.server.models.streams import NativeStreamError
from prefect.server.schemas import stream_subtasks as s
from prefect.settings import (
    PREFECT_SERVER_API_AUTH_STRING,
    PREFECT_SERVER_CSRF_PROTECTION_ENABLED,
    temporary_settings,
)
from prefect.types._datetime import now
from tests.server.models.test_stream_executions import (
    _claim,
    _recoverable_flow,
    _require_postgres_17,
)
from tests.server.models.test_stream_subtasks import (
    admit,
    claim_registered,
    graph_request,
    prepare,
    task_event,
)

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


@pytest.fixture(autouse=True)
def protected_api() -> Iterator[None]:
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-physical:test"}):
        yield


@pytest.fixture
async def physical_api(protected_api: None) -> AsyncGenerator[httpx.AsyncClient, None]:
    app = create_api_app(ignore_cache=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://native-api"
    ) as client:
        yield client


def headers(scoped: s.StreamSubtaskCredentials) -> dict[str, str]:
    return {
        "Authorization": f"PrefectSubtask {scoped.capability}",
        "X-Prefect-Subtask-Grant": str(scoped.grant_id),
    }


async def test_child_http_scope_events_and_no_basic_or_anonymous_escape(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    await _require_postgres_17(db)
    _, plan, parent_credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, parent_credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, parent_credentials)
    sibling = next(
        item
        for item in plan.subtasks
        if item.operator_uid == "A" and item.subtask_index == 1
    )
    child_headers = headers(scoped)
    response = await physical_api.get(
        f"/streams/v2/subtasks/{subtask.id}/bootstrap", headers=child_headers
    )
    assert response.status_code == 200, response.text
    assert response.json()["task_run"]["id"] == str(subtask.manifest.task_run_id)
    assert scoped.capability not in response.text
    assert parent_credentials.capability not in response.text
    for path in [
        "/admin/version",
        "/flow_runs/filter",
        "/streams/v1/finite/plan",
        "/streams/v2/graphs/seal",
        f"/streams/v2/subtasks/{subtask.id}/claim",
        "/streams/v1/executions/claim",
    ]:
        method = (
            "GET" if path in ("/admin/version", "/streams/v1/finite/plan") else "POST"
        )
        denied = await physical_api.request(
            method, path, headers=child_headers, json={} if method == "POST" else None
        )
        assert denied.status_code == 401, (path, denied.text)
    for path in [
        f"/streams/v1/{sibling.manifest.stream_id}",
        f"/streams/v1/{sibling.manifest.stream_id}/prefix?limit=1",
        f"/streams/v2/subtasks/{sibling.id}/bootstrap",
        f"/streams/v2/subtasks/{sibling.id}/task-run",
    ]:
        denied = await physical_api.get(path, headers=child_headers)
        assert denied.status_code == 403, (path, denied.text)
        assert denied.json()["detail"]["code"] == "SUBTASK_SCOPE_DENIED"
    anonymous = await physical_api.get(f"/streams/v1/{subtask.manifest.stream_id}")
    assert anonymous.status_code == 401
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    parent = await physical_api.get(
        f"/streams/v1/{subtask.manifest.stream_id}", headers={"Authorization": basic}
    )
    assert parent.status_code == 200
    event = task_event(plan, subtask, grant)
    recorded = await physical_api.post(
        f"/streams/v2/subtasks/{subtask.id}/events",
        json=event.model_dump(mode="json"),
        headers=child_headers,
    )
    assert recorded.status_code == 200, recorded.text
    assert recorded.json()["accepted"] is True
    assert recorded.json()["run_count"] == 1
    assert recorded.json()["state_type"] == "RUNNING"
    current = await physical_api.get(
        f"/streams/v2/subtasks/{subtask.id}/task-run", headers=child_headers
    )
    assert current.json()["run_count"] == 1
    forged = task_event(plan, subtask, grant)
    forged.resource["prefect.stream-subtask"] = str(sibling.id)
    refused = await physical_api.post(
        f"/streams/v2/subtasks/{subtask.id}/events",
        json=forged.model_dump(mode="json"),
        headers=child_headers,
    )
    assert refused.status_code == 200 and refused.json()["accepted"] is False
    renewed = await physical_api.post(
        f"/streams/v2/subtasks/{subtask.id}/lease/renew", json={}, headers=child_headers
    )
    assert renewed.status_code == 204
    premature = await physical_api.post(
        f"/streams/v2/subtasks/{subtask.id}/lease/release",
        json={},
        headers=child_headers,
    )
    assert (
        premature.status_code == 409
        and premature.json()["detail"]["code"] == "TERMINAL_CONFLICT"
    )


async def test_capability_is_not_echoed_from_validation_or_off_allowlist(
    db: PrefectDBInterface, physical_api: httpx.AsyncClient
):
    secret = "ab" * 32
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    body = {"operation_id": str(uuid4()), "capability": secret, "unexpected": secret}
    response = await physical_api.post(
        f"/streams/v2/subtasks/{uuid4()}/claim",
        json=body,
        headers={"Authorization": basic},
    )
    assert response.status_code == 422
    assert secret not in response.text
    scoped = s.StreamSubtaskCredentials(grant_id=uuid4(), capability=secret)
    wrong_resource = await physical_api.get(
        f"/streams/v1/{uuid4()}", headers=headers(scoped)
    )
    assert wrong_resource.status_code in (
        (401, 404) if db.dialect.name == "postgresql" else (422,)
    )
    assert secret not in wrong_resource.text
    redirected = await physical_api.get(
        "/streams/v1/finite/plan?flow_run_id=" + str(uuid4()), headers=headers(scoped)
    )
    assert redirected.status_code == 401


async def test_profile_admission_fails_closed_without_protected_api(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    await _require_postgres_17(db)

    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: None}):
        async with db.session_context(begin_transaction=True) as session:
            with pytest.raises(NativeStreamError) as error:
                await m.seal_graph(session, db, graph_request(flow_run.id), None)
            assert error.value.code == "UNPROTECTED_API"


async def test_sqlite_physical_admission_refuses_before_mutation(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    if db.dialect.name == "postgresql":
        pytest.skip("SQLite fail-closed profile")
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    response = await physical_api.post(
        "/streams/v2/graphs/seal",
        json=graph_request(flow_run.id).model_dump(mode="json"),
        headers={"Authorization": basic},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNSUPPORTED_BACKEND"
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamGraphRevision)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubmission)
            )
            == 0
        )


def test_physical_identity_counters_are_strict_and_capability_validation_redacts():

    body = graph_request(uuid4()).model_dump(mode="json")
    body["assignment_epoch"] = True
    with pytest.raises(ValidationError):
        s.StreamGraphSealRequest.model_validate(body)
    secret = "ab" * 32
    with pytest.raises(ValidationError) as error:
        s.StreamSubtaskClaimRequest.model_validate(
            {"capability": secret, "launcher_binding_kind": "DEPLOYMENT"}
        )
    assert secret not in str(error.value)


async def test_equal_source_splits_are_rejected_before_graph_mutation(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    body = graph_request(flow_run.id).model_dump(mode="json")
    body["operators"][0]["parameters"] = [{"partition": 0}, {"partition": 0}]
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    response = await physical_api.post(
        "/streams/v2/graphs/seal", json=body, headers={"Authorization": basic}
    )
    assert response.status_code == 422
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamGraphRevision)
                .where(db.StreamGraphRevision.flow_run_id == flow_run.id)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.TaskRun)
                .where(db.TaskRun.flow_run_id == flow_run.id)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamPartition)
                .where(db.StreamPartition.flow_run_id == flow_run.id)
            )
            == 0
        )


async def test_csrf_enabled_scoped_transport_is_exactly_exempt(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    await _require_postgres_17(db)
    _, plan, credentials = await prepare(db, flow_run)
    _, plan = await admit(db, plan, credentials)
    subtask, _, grant, scoped = await claim_registered(db, plan, credentials)
    with temporary_settings({PREFECT_SERVER_CSRF_PROTECTION_ENABLED: True}):
        app = create_app(ephemeral=True, ignore_cache=True)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://native-api/api"
        ) as client:
            event = task_event(plan, subtask, grant).model_dump(mode="json")
            response = await client.post(
                f"/streams/v2/subtasks/{subtask.id}/events",
                json=event,
                headers=headers(scoped),
            )
            assert response.status_code == 200, response.text
            assert response.json()["accepted"]
            wrong = headers(scoped) | {"Authorization": "PrefectSubtask " + "00" * 32}
            denied = await client.post(
                f"/streams/v2/subtasks/{subtask.id}/lease/renew", json={}, headers=wrong
            )
            assert denied.status_code == 401, denied.text
            forbidden = await client.post(
                "/flow_runs/filter", json={}, headers=headers(scoped)
            )
            assert forbidden.status_code in (401, 403)
            basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
            for authentication in ({}, {"Authorization": basic}):
                normal = await client.post(
                    "/flow_runs/filter", json={}, headers=authentication
                )
                assert normal.status_code == 403
                assert normal.json()["detail"] == "Missing CSRF token."


@pytest.mark.parametrize("value", [True, 1.2, -1, 64])
def test_retry_identity_policy_rejects_noncanonical_count(value: object):
    body = graph_request(uuid4()).model_dump(mode="json")
    body["operators"][0]["task_policy"]["retries"] = value
    with pytest.raises(ValueError):
        s.StreamGraphSealRequest.model_validate(body)


@pytest.mark.parametrize(
    "field,value",
    [
        ("retry_delay", -1),
        ("retry_delay", True),
        ("retry_delay", [1, float("inf")]),
        ("retry_delay", float("nan")),
        ("retry_jitter_factor", float("nan")),
        ("retry_jitter_factor", True),
    ],
)
def test_retry_timing_policy_rejects_invalid_numbers(field: str, value: object):
    body = graph_request(uuid4()).model_dump(mode="json")
    body["operators"][0]["task_policy"][field] = value
    with pytest.raises(ValueError):
        s.StreamGraphSealRequest.model_validate(body)


@pytest.mark.parametrize("value", [True, 0, -1, float("nan"), float("inf")])
def test_timeout_requires_finite_positive_number(value: object):
    body = graph_request(uuid4()).model_dump(mode="json")
    body["operators"][0]["timeout_seconds"] = value
    with pytest.raises(ValueError):
        s.StreamGraphSealRequest.model_validate(body)


async def test_operator_input_and_context_collisions_are_prewrite_errors(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    for operator_index, key in [(1, "document"), (0, "ctx"), (1, "ctx")]:
        body = graph_request(flow_run.id).model_dump(mode="json")
        body["operators"][operator_index]["parameters"][0][key] = (
            "must not overwrite physical input/context"
        )
        response = await physical_api.post(
            "/streams/v2/graphs/seal", json=body, headers={"Authorization": basic}
        )
        assert response.status_code == 422
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamGraphRevision)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count())
                .select_from(db.StreamPartition)
                .where(db.StreamPartition.flow_run_id == flow_run.id)
            )
            == 0
        )


async def test_sqlite_all_physical_mutations_refuse_before_locator_or_write(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    if db.dialect.name == "postgresql":
        pytest.skip("SQLite negative profile")
    subtask_id, grant_id = uuid4(), uuid4()
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    parent_headers = {"Authorization": basic}
    claim = s.StreamSubtaskClaimRequest(
        operation_id=uuid4(),
        graph_digest="aa" * 32,
        holder_id=uuid4(),
        capability="bb" * 32,
        event_public_key="cc" * 32,
        launcher_binding_kind="FLOW_AUTHORITY_LOCAL",
        host_id=uuid4(),
        boot_id=uuid4(),
        launcher_process_id=1,
        launcher_start_ticks=1,
    )
    requests = [
        ("claim", claim.model_dump(mode="json")),
        (
            "register",
            {
                "operation_id": str(uuid4()),
                "grant_id": str(grant_id),
                "child_process_id": 1,
                "process_start_ticks": 1,
            },
        ),
        ("renew", {"grant_id": str(grant_id)}),
        (
            "replacements/authorize",
            {
                "operation_id": str(uuid4()),
                "expected_grant_id": str(grant_id),
                "expected_physical_epoch": 1,
                "host_id": str(claim.host_id),
                "challenge_capability": "dd" * 32,
            },
        ),
        (
            "replacements/confirm",
            {
                "operation_id": str(uuid4()),
                "challenge_capability": "dd" * 32,
                "death_witness": {
                    "kind": "WAITPID",
                    "observed_at": now("UTC").isoformat(),
                    "host_id": str(claim.host_id),
                    "boot_id": str(claim.boot_id),
                    "child_process_id": 1,
                    "process_start_ticks": 1,
                    "exit_code": -9,
                },
                "new_holder_id": str(uuid4()),
                "new_capability": "ee" * 32,
                "new_event_public_key": "ff" * 32,
                "new_child_process_id": 2,
                "new_process_start_ticks": 2,
                "host_id": str(claim.host_id),
                "boot_id": str(claim.boot_id),
            },
        ),
    ]
    for suffix, body in requests:
        response = await physical_api.post(
            f"/streams/v2/subtasks/{subtask_id}/{suffix}",
            json=body,
            headers=parent_headers,
        )
        assert response.status_code == 422, (suffix, response.text)
        assert response.json()["detail"]["code"] == "UNSUPPORTED_BACKEND"
    child_headers = headers(
        s.StreamSubtaskCredentials(grant_id=grant_id, capability="bb" * 32)
    )
    event = Event(
        id=uuid4(),
        occurred=now("UTC"),
        event="prefect.task-run.Running",
        resource={"prefect.resource.id": f"prefect.task-run.{uuid4()}"},
        payload={},
    )
    for suffix, body in [
        ("events", event.model_dump(mode="json")),
        ("lease/renew", {}),
        ("lease/release", {}),
    ]:
        response = await physical_api.post(
            f"/streams/v2/subtasks/{subtask_id}/{suffix}",
            json=body,
            headers=child_headers,
        )
        assert response.status_code == 422, (suffix, response.text)
        assert response.json()["detail"]["code"] == "UNSUPPORTED_BACKEND"
    async with db.session_context() as session:
        for table in (
            db.StreamSubtaskGrant,
            db.StreamSubtaskReplacement,
            db.StreamSubtaskAllocation,
            db.StreamPhysicalSubtask,
        ):
            assert (
                await session.scalar(sa.select(sa.func.count()).select_from(table)) == 0
            )


async def test_empty_source_split_is_rejected_without_any_native_rows(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    body = graph_request(flow_run.id).model_dump(mode="json")
    body["operators"][0]["parameters"][0] = {}
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    response = await physical_api.post(
        "/streams/v2/graphs/seal", json=body, headers={"Authorization": basic}
    )
    assert response.status_code == 422
    async with db.session_context() as session:
        for table in (db.StreamGraphRevision, db.TaskRun, db.StreamPartition):
            assert (
                await session.scalar(
                    sa.select(sa.func.count())
                    .select_from(table)
                    .where(table.flow_run_id == flow_run.id)
                )
                == 0
            )


async def test_public_partitioned_seal_requires_deployment_before_any_topology_rows(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    await _require_postgres_17(db)
    await _recoverable_flow(db, flow_run)
    request = _claim(flow_run.id)
    async with db.session_context(begin_transaction=True) as session:
        await claim_execution(session, db, request)
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    auth = {
        "Authorization": basic,
        "X-Prefect-Stream-Execution-Generation": "1",
        "X-Prefect-Stream-Execution-Holder": str(request.holder_id),
        "X-Prefect-Stream-Execution-Capability": request.holder_capability,
    }
    response = await physical_api.post(
        "/streams/v2/graphs/seal",
        json=graph_request(flow_run.id).model_dump(mode="json"),
        headers=auth,
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "UNSUPPORTED_PROFILE"
    async with db.session_context() as session:
        for table in (
            db.StreamGraphRevision,
            db.TaskRun,
            db.StreamPartition,
            db.StreamSubmission,
        ):
            assert (
                await session.scalar(
                    sa.select(sa.func.count())
                    .select_from(table)
                    .where(table.flow_run_id == flow_run.id)
                )
                == 0
            )


async def test_public_admission_cannot_promote_internal_local_discovery_graph(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    physical_api: httpx.AsyncClient,
):
    await _require_postgres_17(db)
    _, plan, credentials = await prepare(db, flow_run)
    basic = "Basic " + base64.b64encode(b"native-physical:test").decode()
    auth = {
        "Authorization": basic,
        "X-Prefect-Stream-Execution-Generation": str(credentials.generation),
        "X-Prefect-Stream-Execution-Holder": str(credentials.holder_id),
        "X-Prefect-Stream-Execution-Capability": credentials.capability,
    }
    response = await physical_api.post(
        f"/streams/v2/graphs/{plan.id}/admit",
        json={"operation_id": str(uuid4()), "graph_digest": plan.graph_digest},
        headers=auth,
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "UNSUPPORTED_PROFILE"
    async with db.session_context() as session:
        assert (await session.get(db.StreamGraphRevision, plan.id)).state == "SEALED"
        for table in (
            db.StreamSubtaskGrant,
            db.StreamSubtaskAllocation,
            db.StreamRecord,
        ):
            assert (
                await session.scalar(sa.select(sa.func.count()).select_from(table)) == 0
            )
