"""Native stream HTTP contract tests against the real server database."""

from uuid import uuid4

import pytest

from prefect.server import models, schemas


async def _postgres_17(db) -> bool:
    engine = await db.engine()
    version = engine.dialect.server_version_info or ()
    return db.dialect.name == "postgresql" and bool(version) and version[0] == 17


async def _running_task(session, flow_run_id, key: str):
    return await models.task_runs.create_task_run(
        session=session,
        task_run=schemas.core.TaskRun(
            flow_run_id=flow_run_id,
            task_key=f"{key}-{uuid4()}",
            dynamic_key="0",
            state=schemas.states.Running(),
        ),
    )


async def test_backend_capability_error_has_stable_wire_shape(client, db, flow_run):
    if await _postgres_17(db):
        pytest.skip("SQLite capability response is covered by the SQLite profile")
    response = await client.post(
        "/streams/v1/create",
        json={"flow_run_id": str(flow_run.id), "stream_id": str(uuid4())},
    )
    assert response.status_code == 422
    assert response.json()["detail"] == {
        "code": "UNSUPPORTED_BACKEND",
        "message": "Native streams require PostgreSQL 17",
        "retryable": False,
    }


async def test_boolean_protocol_version_is_rejected_before_mutation(
    client, db, flow_run
):
    stream_id = uuid4()
    response = await client.post(
        "/streams/v1/create",
        json={
            "flow_run_id": str(flow_run.id),
            "stream_id": str(stream_id),
            "protocol_version": True,
        },
    )
    assert response.status_code == 422
    assert response.json()["exception_detail"][0]["loc"] == [
        "body",
        "protocol_version",
    ]
    async with db.session_context() as session:
        assert await session.get(db.StreamPartition, stream_id) is None


async def test_http_roundtrip_and_exact_terminal_retry(
    client, db, session, flow_run, task_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    downstream = await _running_task(session, flow_run.id, "http-transform")
    await session.commit()
    input_id, output_id = uuid4(), uuid4()

    response = await client.post(
        "/streams/v1/create",
        json={"flow_run_id": str(flow_run.id), "stream_id": str(input_id)},
    )
    assert response.status_code == 200
    reservation = {
        "flow_run_id": str(flow_run.id),
        "stream_id": str(output_id),
        "task_run_id": str(downstream.id),
    }
    first_reservation = await client.post(
        "/streams/v1/outputs/reserve", json=reservation
    )
    second_reservation = await client.post(
        "/streams/v1/outputs/reserve", json=reservation
    )
    assert first_reservation.status_code == second_reservation.status_code == 200
    assert first_reservation.json() == second_reservation.json()

    claim = await client.post(
        "/streams/v1/sources/claim",
        json={
            "stream_id": str(input_id),
            "task_run_id": str(task_run.id),
            "owner_id": str(uuid4()),
            "attempt": 1,
        },
    )
    claim.raise_for_status()
    append = await client.post(
        "/streams/v1/sources/append",
        json={"binding": claim.json(), "position": 1, "value": {"document": 200}},
    )
    assert append.status_code == 200
    finish_source = await client.post(
        "/streams/v1/sources/finish",
        json={"binding": claim.json(), "error": None},
    )
    assert finish_source.status_code == 204

    opened = await client.post(
        "/streams/v1/consumers/open",
        json={
            "input_stream_id": str(input_id),
            "output_stream_id": str(output_id),
            "task_run_id": str(downstream.id),
            "owner_id": str(uuid4()),
            "attempt": 1,
            "protocol_version": 1,
            "terminal_protocol": "yield-v2",
        },
    )
    opened.raise_for_status()
    delivery = await client.post(
        "/streams/v1/consumers/read", json={"binding": opened.json()}
    )
    delivery.raise_for_status()
    completion = {
        "binding": opened.json(),
        "token": delivery.json()["token"],
        "kind": "YIELD",
        "value": {"signed": 200},
        "managed_state": {"count": 1},
    }
    first_complete = await client.post(
        "/streams/v1/consumers/complete", json=completion
    )
    second_complete = await client.post(
        "/streams/v1/consumers/complete", json=completion
    )
    assert first_complete.status_code == second_complete.status_code == 200
    assert (
        first_complete.json()
        == second_complete.json()
        == {
            "input_sequence": 1,
            "output_sequence": 1,
        }
    )
    finished = await client.post(
        "/streams/v1/consumers/finish",
        json={"binding": opened.json(), "error": None},
    )
    assert finished.status_code == 204
    assert (await client.get(f"/streams/v1/{output_id}")).json()["state"] == "COMPLETED"
    prefix = await client.get(f"/streams/v1/{output_id}/prefix", params={"limit": 1})
    assert prefix.status_code == 200
    assert [item["value"] for item in prefix.json()] == [{"signed": 200}]


async def test_http_metadata_lag_and_permanent_refusals_are_read_only(
    client, db, session, flow, flow_run, task_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    input_id, output_id = uuid4(), uuid4()
    for body in (
        {"flow_run_id": str(flow_run.id), "stream_id": str(input_id)},
        {
            "flow_run_id": str(flow_run.id),
            "stream_id": str(output_id),
            "task_run_id": str(task_run.id),
        },
    ):
        route = "create" if "task_run_id" not in body else "outputs/reserve"
        response = await client.post(f"/streams/v1/{route}", json=body)
        response.raise_for_status()
    owner_id = uuid4()
    open_body = {
        "input_stream_id": str(input_id),
        "output_stream_id": str(output_id),
        "task_run_id": str(task_run.id),
        "owner_id": str(owner_id),
        "attempt": 1,
        "protocol_version": 1,
        "terminal_protocol": "yield-v2",
    }
    lag = await client.post("/streams/v1/consumers/open", json=open_body)
    assert lag.status_code == 409
    assert lag.json()["detail"]["code"] == "METADATA_NOT_READY"
    assert lag.json()["detail"]["retryable"] is True

    other_flow = await models.flow_runs.create_flow_run(
        session=session,
        flow_run=schemas.core.FlowRun(flow_id=flow.id, flow_version="other"),
    )
    other_task = await _running_task(session, other_flow.id, "cross-flow")
    await session.commit()
    missing_reservation_id = uuid4()
    missing_reservation = await client.post(
        "/streams/v1/outputs/reserve",
        json={
            "flow_run_id": str(flow_run.id),
            "stream_id": str(missing_reservation_id),
            "task_run_id": str(uuid4()),
        },
    )
    assert missing_reservation.status_code == 409
    assert missing_reservation.json()["detail"] == {
        "code": "METADATA_NOT_READY",
        "message": "TaskRun metadata is not recorded yet",
        "retryable": True,
    }
    cross_reservation_id = uuid4()
    cross_reservation = await client.post(
        "/streams/v1/outputs/reserve",
        json={
            "flow_run_id": str(flow_run.id),
            "stream_id": str(cross_reservation_id),
            "task_run_id": str(other_task.id),
        },
    )
    assert cross_reservation.status_code == 409
    assert cross_reservation.json()["detail"]["code"] == "IDENTITY_CONFLICT"
    cross_flow = await client.post(
        "/streams/v1/consumers/open",
        json={**open_body, "task_run_id": str(other_task.id)},
    )
    assert cross_flow.status_code == 409
    assert cross_flow.json()["detail"]["code"] == "IDENTITY_CONFLICT"
    assert cross_flow.json()["detail"]["retryable"] is False

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Cancelled(),
        force=True,
    )
    await session.commit()
    final = await client.post("/streams/v1/consumers/open", json=open_body)
    assert final.status_code == 409
    assert final.json()["detail"]["code"] == "ATTEMPT_NOT_CURRENT"
    assert final.json()["detail"]["retryable"] is False
    final_reservation_id = uuid4()
    final_reservation = await client.post(
        "/streams/v1/outputs/reserve",
        json={
            "flow_run_id": str(flow_run.id),
            "stream_id": str(final_reservation_id),
            "task_run_id": str(task_run.id),
        },
    )
    assert final_reservation.status_code == 409
    assert final_reservation.json()["detail"]["code"] == "ATTEMPT_NOT_CURRENT"

    async with db.session_context() as check:
        output = await check.get(db.StreamPartition, output_id)
        assert output.owner_id is None
        assert await check.get(db.StreamConsumer, task_run.id) is None
        assert await check.get(db.StreamPartition, missing_reservation_id) is None
        assert await check.get(db.StreamPartition, cross_reservation_id) is None
        assert await check.get(db.StreamPartition, final_reservation_id) is None


async def test_reject_and_terminal_failure_routes_preserve_fencing(
    client, db, session, flow_run, task_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    rejected_id = uuid4()
    reserve = {
        "flow_run_id": str(flow_run.id),
        "stream_id": str(rejected_id),
        "task_run_id": str(task_run.id),
    }
    (await client.post("/streams/v1/outputs/reserve", json=reserve)).raise_for_status()
    rejection = {**reserve, "error": "submission failed"}
    first = await client.post("/streams/v1/outputs/reject", json=rejection)
    second = await client.post("/streams/v1/outputs/reject", json=rejection)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["state"] == "FAILED"

    failed_id = uuid4()
    failed_reservation = {**reserve, "stream_id": str(failed_id)}
    (
        await client.post("/streams/v1/outputs/reserve", json=failed_reservation)
    ).raise_for_status()
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Failed(message="startup failed"),
        force=True,
    )
    await session.commit()
    terminal = await client.post(
        "/streams/v1/outputs/fail",
        json={**failed_reservation, "attempt": 1, "error": "startup failed"},
    )
    terminal.raise_for_status()
    assert terminal.json()["state"] == "FAILED"
    stale = await client.post(
        "/streams/v1/outputs/fail",
        json={**failed_reservation, "attempt": 2, "error": "future attempt"},
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["retryable"] is False


async def test_http_status_mapping_for_not_found_payload_and_released_horizon(
    client, db, session, flow_run, task_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    unknown = await client.get(f"/streams/v1/{uuid4()}")
    assert unknown.status_code == 404
    assert unknown.json()["detail"]["code"] == "NOT_FOUND"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    stream_id = uuid4()
    created = await client.post(
        "/streams/v1/create",
        json={
            "flow_run_id": str(flow_run.id),
            "stream_id": str(stream_id),
            "limits": {"max_record_bytes": 1},
        },
    )
    created.raise_for_status()
    claim = await client.post(
        "/streams/v1/sources/claim",
        json={
            "stream_id": str(stream_id),
            "task_run_id": str(task_run.id),
            "owner_id": str(uuid4()),
            "attempt": 1,
        },
    )
    claim.raise_for_status()
    too_large = await client.post(
        "/streams/v1/sources/append",
        json={"binding": claim.json(), "position": 1, "value": "document"},
    )
    assert too_large.status_code == 413
    assert too_large.json()["detail"]["code"] == "PAYLOAD_TOO_LARGE"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Failed(message="stop"),
        force=True,
    )
    await models.flow_runs.set_flow_run_state(
        session=session,
        flow_run_id=flow_run.id,
        state=schemas.states.Failed(message="stop"),
        force=True,
    )
    await session.commit()
    released = await client.post(
        "/streams/v1/release", json={"flow_run_id": str(flow_run.id)}
    )
    assert released.status_code == 200
    assert released.json() == {"released": 1}
    expired = await client.get(f"/streams/v1/{stream_id}/prefix", params={"limit": 1})
    assert expired.status_code == 410
    assert expired.json()["detail"]["code"] == "REPLAY_HORIZON_EXPIRED"


async def test_http_source_task_roundtrip_checked_eof_and_completed_retry(
    client, db, session, flow_run, task_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    stream_id = uuid4()
    reservation = {
        "flow_run_id": str(flow_run.id),
        "stream_id": str(stream_id),
        "task_run_id": str(task_run.id),
        "protocol_version": 1,
        "source_protocol": "source-v1",
    }
    first = await client.post("/streams/v1/sources/reserve", json=reservation)
    second = await client.post("/streams/v1/sources/reserve", json=reservation)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()

    owner = uuid4()
    opened_body = {
        "stream_id": str(stream_id),
        "task_run_id": str(task_run.id),
        "owner_id": str(owner),
        "attempt": 1,
        "protocol_version": 1,
        "source_protocol": "source-v1",
    }
    lag = await client.post("/streams/v1/sources/open", json=opened_body)
    assert lag.status_code == 409
    assert lag.json()["detail"]["code"] == "METADATA_NOT_READY"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    opened = await client.post("/streams/v1/sources/open", json=opened_body)
    opened.raise_for_status()
    assert opened.json()["state"] == "OPEN"
    assert opened.json()["accepted_source_position"] == 0
    binding = opened.json()["binding"]
    append = await client.post(
        "/streams/v1/sources/append",
        json={"binding": binding, "position": 1, "value": {"ref": "object:1"}},
    )
    append.raise_for_status()

    drift = await client.post(
        "/streams/v1/sources/close",
        json={"binding": binding, "expected_position": 0},
    )
    assert drift.status_code == 409
    assert drift.json()["detail"]["code"] == "IDENTITY_CONFLICT"
    exact = {"binding": binding, "expected_position": 1}
    assert (
        await client.post("/streams/v1/sources/close", json=exact)
    ).status_code == 204
    assert (
        await client.post("/streams/v1/sources/close", json=exact)
    ).status_code == 204

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    recovered = await client.post(
        "/streams/v1/sources/open",
        json={**opened_body, "owner_id": str(uuid4()), "attempt": 2},
    )
    recovered.raise_for_status()
    assert recovered.json()["state"] == "COMPLETED"
    assert recovered.json()["accepted_source_position"] == 1
    assert recovered.json()["binding"]["owner_epoch"] == 2


async def test_http_source_reject_and_final_failure_preserve_authority(
    client, db, session, flow_run, task_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    rejected_id = uuid4()
    rejected_reservation = {
        "flow_run_id": str(flow_run.id),
        "stream_id": str(rejected_id),
        "task_run_id": str(task_run.id),
        "source_protocol": "source-v1",
    }
    (
        await client.post("/streams/v1/sources/reserve", json=rejected_reservation)
    ).raise_for_status()
    rejection = {
        "flow_run_id": str(flow_run.id),
        "stream_id": str(rejected_id),
        "task_run_id": str(task_run.id),
        "error": "submission failed",
    }
    first = await client.post("/streams/v1/sources/reject", json=rejection)
    second = await client.post("/streams/v1/sources/reject", json=rejection)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["state"] == "FAILED"

    source_id = uuid4()
    reservation = {**rejected_reservation, "stream_id": str(source_id)}
    (
        await client.post("/streams/v1/sources/reserve", json=reservation)
    ).raise_for_status()
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    opened = await client.post(
        "/streams/v1/sources/open",
        json={
            "stream_id": str(source_id),
            "task_run_id": str(task_run.id),
            "owner_id": str(uuid4()),
            "attempt": 1,
            "source_protocol": "source-v1",
        },
    )
    opened.raise_for_status()
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Failed(message="retry exhausted"),
        force=True,
    )
    await session.commit()
    failure = {
        "flow_run_id": str(flow_run.id),
        "stream_id": str(source_id),
        "task_run_id": str(task_run.id),
        "attempt": 2,
        "error": "retry exhausted",
    }
    stale = await client.post(
        "/streams/v1/sources/fail", json={**failure, "attempt": 1}
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "ATTEMPT_NOT_CURRENT"
    terminal = await client.post("/streams/v1/sources/fail", json=failure)
    terminal.raise_for_status()
    assert terminal.json()["state"] == "FAILED"
    assert (
        await client.post("/streams/v1/sources/fail", json=failure)
    ).json() == terminal.json()


async def test_http_source_protocol_position_and_identity_refusals_are_read_only(
    client, db, session, flow, flow_run, task_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    stream_id = uuid4()
    base = {
        "flow_run_id": str(flow_run.id),
        "stream_id": str(stream_id),
        "task_run_id": str(task_run.id),
    }
    invalid = await client.post(
        "/streams/v1/sources/reserve", json={**base, "protocol_version": True}
    )
    assert invalid.status_code == 422
    async with db.session_context() as check:
        assert await check.get(db.StreamPartition, stream_id) is None

    (
        await client.post(
            "/streams/v1/sources/reserve",
            json={**base, "source_protocol": "source-v1"},
        )
    ).raise_for_status()
    other_flow = await models.flow_runs.create_flow_run(
        session=session,
        flow_run=schemas.core.FlowRun(
            flow_id=flow.id, flow_version="source-http-other"
        ),
    )
    other_task = await _running_task(session, other_flow.id, "source-http-cross")
    await session.commit()
    cross = await client.post(
        "/streams/v1/sources/open",
        json={
            "stream_id": str(stream_id),
            "task_run_id": str(other_task.id),
            "owner_id": str(uuid4()),
            "attempt": 1,
            "source_protocol": "source-v1",
        },
    )
    assert cross.status_code == 409
    assert cross.json()["detail"]["code"] == "IDENTITY_CONFLICT"
    future = await client.post(
        "/streams/v1/sources/open",
        json={
            "stream_id": str(stream_id),
            "task_run_id": str(task_run.id),
            "owner_id": str(uuid4()),
            "attempt": 2,
            "source_protocol": "source-v1",
        },
    )
    assert future.status_code == 409
    assert future.json()["detail"]["code"] == "METADATA_NOT_READY"

    await models.task_runs.set_task_run_state(
        session=session,
        task_run_id=task_run.id,
        state=schemas.states.Running(),
        force=True,
    )
    await session.commit()
    opened = await client.post(
        "/streams/v1/sources/open",
        json={
            "stream_id": str(stream_id),
            "task_run_id": str(task_run.id),
            "owner_id": str(uuid4()),
            "attempt": 1,
            "source_protocol": "source-v1",
        },
    )
    opened.raise_for_status()
    bad_position = await client.post(
        "/streams/v1/sources/close",
        json={"binding": opened.json()["binding"], "expected_position": True},
    )
    assert bad_position.status_code == 422
    async with db.session_context() as check:
        stream = await check.get(db.StreamPartition, stream_id)
        assert stream.state == "OPEN"
        assert stream.producer_position == 0


async def test_http_submission_manifest_exact_replay_and_drift(
    client, db, session, flow_run
):
    if not await _postgres_17(db):
        pytest.skip("Native stream data plane requires PostgreSQL 17")
    row = await session.get(db.FlowRun, flow_run.id)
    row.flow_version = "flow-v1"
    row.empirical_policy = schemas.core.FlowRunPolicy(retries=2)
    await session.commit()
    request = {
        "flow_run_id": str(flow_run.id),
        "submission_key": "source.http",
        "role": "SOURCE",
        "protocol": "source-v1",
        "task_name": "http-source",
        "task_key": "http-source-key",
        "task_version": "task-v1",
        "flow_version": "flow-v1",
        "parameters": {"connector": {"block_document_id": "http-v1"}},
        "task_policy": {"retries": 2},
    }
    first = await client.post("/streams/v1/submissions/reserve", json=request)
    second = await client.post("/streams/v1/submissions/reserve", json=request)
    lookup = await client.post(
        "/streams/v1/submissions/lookup",
        json={
            "flow_run_id": str(flow_run.id),
            "submission_key": "source.http",
        },
    )
    assert first.status_code == second.status_code == lookup.status_code == 200
    assert first.json() == second.json() == lookup.json()
    assert first.json()["state"] == "ACTIVE"
    drift = await client.post(
        "/streams/v1/submissions/reserve",
        json={
            **request,
            "parameters": {"connector": {"block_document_id": "http-v2"}},
        },
    )
    assert drift.status_code == 409
    assert drift.json()["detail"]["code"] == "IDENTITY_CONFLICT"
