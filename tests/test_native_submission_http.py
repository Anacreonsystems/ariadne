"""Durable call identity across actual HTTP client/API process loss.

This is the N1.5a identity slice. It deliberately does not claim execution
generation fencing or reconstruction of a running flow process.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import subprocess
import sys
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest

from prefect import flow
from prefect.client.orchestration import PrefectClient
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.states import Running
from tests.test_native_source_recovery_http import _wait_for_advisory_waiter
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI
from tests.test_native_stream_recovery_http import restartable_api as restartable_api


def _source_request(server: RestartableAPI) -> dict[str, Any]:
    @flow(version="orders-v1", retries=2)
    def definition():
        pass

    with server.client() as client:
        run = client.create_flow_run(definition, state=Running())
    return {
        "flow_run_id": run.id,
        "submission_key": "source.orders",
        "role": "SOURCE",
        "protocol": "source-v1",
        "task_name": "orders",
        "task_key": "orders-source",
        "task_version": "orders-v1",
        "flow_version": "orders-v1",
        "parameters": {"tenant": "example", "refs": ["object://orders"]},
        "task_policy": {"retries": 2, "retry_delay": 0},
    }


async def _owned_counts(dsn: str, flow_run_id: UUID) -> tuple[int, int, int]:
    connection = await asyncpg.connect(
        dsn.replace("postgresql+asyncpg:", "postgresql:", 1)
    )
    try:
        return tuple(
            [
                await connection.fetchval(
                    f"SELECT count(*) FROM {table} WHERE flow_run_id = $1",
                    flow_run_id,
                )
                for table in ("stream_submission", "task_run", "stream_partition")
            ]
        )
    finally:
        await connection.close()


async def test_submission_survives_client_and_api_process_loss(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
):
    server = restartable_api
    request = _source_request(server)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, os, sys
from uuid import UUID
from prefect.client.orchestration import SyncPrefectClient
payload = json.load(sys.stdin)
payload['request']['flow_run_id'] = UUID(payload['request']['flow_run_id'])
with SyncPrefectClient(payload['url'], auth_string=payload['auth']) as client:
    client.reserve_stream_submission(**payload['request'])
    os._exit(77)
""",
        ],
        input=json.dumps(
            {"url": server.url, "auth": server.auth, "request": request}, default=str
        ),
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert child.returncode == 77, child.stderr
    server.restart()
    with server.client() as client:
        found = client.read_stream_submission(
            flow_run_id=request["flow_run_id"], submission_key=request["submission_key"]
        )
        replayed = client.reserve_stream_submission(**request)
        assert replayed == found
        registered = client.read_task_run(found.task_run_id)
        assert registered.state.is_pending()
        assert registered.dynamic_key == found.dynamic_key
        assert client.read_stream(found.stream_id).state == "OPEN"
    async with PrefectClient(server.url, auth_string=server.auth) as client:
        assert await client.reserve_stream_submission(**request) == found
        assert (
            await client.read_stream_submission(
                flow_run_id=request["flow_run_id"],
                submission_key=request["submission_key"],
            )
            == found
        )
    assert await _owned_counts(
        test_database_connection_url, request["flow_run_id"]
    ) == (1, 1, 1)


async def test_concurrent_exact_submission_has_one_task_and_channel(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
):
    server = restartable_api
    request = _source_request(server)

    def reserve(_: int) -> Any:
        with server.client() as client:
            return client.reserve_stream_submission(**request)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(16)))
    assert all(result == results[0] for result in results)
    assert await _owned_counts(
        test_database_connection_url, request["flow_run_id"]
    ) == (1, 1, 1)


async def test_running_submission_refuses_unsafe_reconstruction(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
):
    request = _source_request(restartable_api)
    with restartable_api.client() as client:
        manifest = client.reserve_stream_submission(**request)
        admitted = client.set_task_run_state(manifest.task_run_id, Running())
        assert admitted.state.is_running()
        with pytest.raises(StreamRequestError) as refused:
            client.reserve_stream_submission(**request)
        assert refused.value.code == "RECOVERY_NOT_READY"
        assert (
            client.read_stream_submission(
                flow_run_id=request["flow_run_id"],
                submission_key=request["submission_key"],
            )
            == manifest
        )
        assert client.read_task_run(manifest.task_run_id).run_count == 1
    assert await _owned_counts(
        test_database_connection_url, request["flow_run_id"]
    ) == (1, 1, 1)


@pytest.mark.parametrize("budget", ["count", "bytes"])
async def test_submission_hard_budgets_refuse_before_partial_creation(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
    budget: str,
):
    request = _source_request(restartable_api)
    if budget == "bytes":
        request["parameters"] = {"ref": "r" * 30_000}
    with restartable_api.client() as client:
        manifest = client.reserve_stream_submission(**request)
        accepted = 1
        while accepted <= 128:
            next_request = request | {
                "submission_key": f"operator.step-{accepted}",
                "role": "OPERATOR",
                "protocol": "yield-v2",
                "input_key": "document",
                "upstream_stream_id": manifest.stream_id,
            }
            try:
                manifest = client.reserve_stream_submission(**next_request)
            except StreamRequestError as refused:
                assert refused.code == "CAPACITY"
                break
            accepted += 1
        else:
            pytest.fail("submission budget did not reject further creation")
        assert accepted == 128 if budget == "count" else 1 < accepted < 128
        assert await _owned_counts(
            test_database_connection_url, request["flow_run_id"]
        ) == (accepted, accepted, accepted)
        with pytest.raises(StreamRequestError) as missing:
            client.read_stream_submission(
                flow_run_id=request["flow_run_id"],
                submission_key=next_request["submission_key"],
            )
        assert missing.value.code == "NOT_FOUND"


async def test_oversized_parameters_are_refused_without_resources(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
):
    request = _source_request(restartable_api)
    request["parameters"] = {"ref": "r" * 33_000}
    with restartable_api.client() as client:
        with pytest.raises(StreamRequestError) as refused:
            client.reserve_stream_submission(**request)
        assert refused.value.code == "PAYLOAD_TOO_LARGE"
    assert await _owned_counts(
        test_database_connection_url, request["flow_run_id"]
    ) == (0, 0, 0)


@pytest.mark.parametrize(
    "changed",
    [
        {"parameters": {"tenant": "changed"}},
        {"task_key": "other-task"},
        {"task_version": "orders-v2"},
        {"task_name": "renamed-task"},
        {"flow_version": "orders-v2"},
        {"task_policy": {"retries": 3}},
        {"tags": ["new-tag"]},
        {"labels": {"tenant": "changed"}},
        {"limits": {"max_records": 200}},
        {"artifact_digest": "a" * 64},
    ],
)
async def test_submission_drift_does_not_create_partial_resources(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
    changed: dict[str, Any],
):
    request = _source_request(restartable_api)
    with restartable_api.client() as client:
        manifest = client.reserve_stream_submission(**request)
        with pytest.raises(StreamRequestError) as refused:
            client.reserve_stream_submission(**(request | changed))
        assert refused.value.code == "IDENTITY_CONFLICT"
        assert (
            client.read_stream_submission(
                flow_run_id=request["flow_run_id"],
                submission_key=request["submission_key"],
            )
            == manifest
        )
    assert await _owned_counts(
        test_database_connection_url, request["flow_run_id"]
    ) == (1, 1, 1)


async def test_api_death_before_manifest_commit_rolls_back_all_resources(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
):
    server = restartable_api
    request = _source_request(server)
    dsn = test_database_connection_url.replace("postgresql+asyncpg:", "postgresql:", 1)
    holder = await asyncpg.connect(dsn)
    observer = await asyncpg.connect(dsn)
    suffix = uuid4().hex
    function_name = f"native_manifest_block_{suffix}"
    trigger_name = f"native_manifest_trigger_{suffix}"
    key_a, key_b = (uuid4().int & 0x3FFFFFFF for _ in range(2))
    trigger_installed = False
    pending: asyncio.Task[Any] | None = None
    try:
        await holder.execute(
            f"""
            CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $body$
            BEGIN
                IF NEW.flow_run_id = '{request["flow_run_id"]}'::uuid THEN
                    PERFORM pg_advisory_xact_lock({key_a}, {key_b});
                END IF;
                RETURN NEW;
            END
            $body$
            """
        )
        await holder.execute(
            f"CREATE TRIGGER {trigger_name} BEFORE INSERT ON stream_submission "
            f"FOR EACH ROW EXECUTE FUNCTION {function_name}()"
        )
        trigger_installed = True
        await holder.execute("SELECT pg_advisory_lock($1, $2)", key_a, key_b)

        def reserve() -> None:
            with httpx.Client(auth=tuple(server.auth.split(":")), timeout=30) as client:
                response = client.post(
                    server.url + "/streams/v1/submissions/reserve",
                    json=json.loads(json.dumps(request, default=str)),
                )
                response.raise_for_status()

        pending = asyncio.create_task(asyncio.to_thread(reserve))
        await _wait_for_advisory_waiter(observer, key_a, key_b)
        server.stop(crash=True)
        result = (await asyncio.gather(pending, return_exceptions=True))[0]
        assert isinstance(result, BaseException)
        await holder.execute("SELECT pg_advisory_unlock($1, $2)", key_a, key_b)
        server.start()
        assert await _owned_counts(
            test_database_connection_url, request["flow_run_id"]
        ) == (0, 0, 0)
        with server.client() as client:
            with pytest.raises(StreamRequestError) as missing:
                client.read_stream_submission(
                    flow_run_id=request["flow_run_id"],
                    submission_key=request["submission_key"],
                )
            assert missing.value.code == "NOT_FOUND"
            manifest = client.reserve_stream_submission(**request)
            assert client.read_stream(manifest.stream_id).state == "OPEN"
        assert await _owned_counts(
            test_database_connection_url, request["flow_run_id"]
        ) == (1, 1, 1)
    finally:
        await holder.execute("SELECT pg_advisory_unlock($1, $2)", key_a, key_b)
        if pending is not None and not pending.done():
            await asyncio.gather(pending, return_exceptions=True)
        if trigger_installed:
            await observer.execute(
                f"DROP TRIGGER IF EXISTS {trigger_name} ON stream_submission"
            )
        await observer.execute(f"DROP FUNCTION IF EXISTS {function_name}()")
        await holder.close()
        await observer.close()
