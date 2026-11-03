"""Recover retained source intents across real API and client process loss.

These tests retain the exact TaskRun, stream, owner and position identities. They
exercise TaskRun retry and low-level intent replay, not whole-worker submission
reconstruction.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest

from prefect import flow, task
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.states import Failed, Running
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import (
    RestartableAPI,
)
from tests.test_native_stream_recovery_http import (
    restartable_api as restartable_api,
)


def _source_channel(
    server: RestartableAPI,
) -> tuple[UUID, UUID, UUID, Any]:
    @flow
    def definition():
        pass

    @task(retries=1)
    def source_task():
        pass

    stream_id = uuid4()
    owner_id = uuid4()
    with server.client() as client:
        flow_run = client.create_flow_run(definition, state=Running())
        task_run = client.create_task_run(
            source_task,
            flow_run.id,
            uuid4().hex,
            state=Running(),
        )
        client.reserve_stream_source(
            flow_run_id=flow_run.id,
            stream_id=stream_id,
            task_run_id=task_run.id,
        )
        admission = client.open_stream_source(
            stream_id=stream_id,
            task_run_id=task_run.id,
            owner_id=owner_id,
            attempt=1,
        )
    assert admission.state == "OPEN"
    assert admission.accepted_source_position == 0
    return flow_run.id, stream_id, task_run.id, admission.binding


def _asyncpg_url(sqlalchemy_url: str) -> str:
    return sqlalchemy_url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _wait_for_advisory_waiter(
    connection: asyncpg.Connection, key_a: int, key_b: int
) -> None:
    deadline = time.monotonic() + 15
    while True:
        waiting = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND classid = $1
                  AND objid = $2
                  AND granted = false
            )
            """,
            key_a,
            key_b,
        )
        if waiting:
            return
        assert time.monotonic() < deadline, "append never reached the commit barrier"
        await asyncio.sleep(0.02)


async def test_api_crash_before_append_commit_keeps_position(
    restartable_api: RestartableAPI,
    test_database_connection_url: str,
):
    server = restartable_api
    _, stream_id, task_run_id, binding = _source_channel(server)
    dsn = _asyncpg_url(test_database_connection_url)
    suffix = uuid4().hex
    function_name = f"native_source_block_{suffix}"
    trigger_name = f"native_source_trigger_{suffix}"
    key_a = uuid4().int & 0x3FFFFFFF
    key_b = uuid4().int & 0x3FFFFFFF
    holder = await asyncpg.connect(dsn)
    observer = await asyncpg.connect(dsn)
    trigger_installed = False

    try:
        await holder.execute(
            f"""
            CREATE FUNCTION {function_name}() RETURNS trigger
            LANGUAGE plpgsql AS $function$
            BEGIN
                IF NEW.stream_id = '{stream_id}'::uuid THEN
                    PERFORM pg_advisory_xact_lock({key_a}, {key_b});
                END IF;
                RETURN NEW;
            END
            $function$
            """
        )
        await holder.execute(
            f"""
            CREATE TRIGGER {trigger_name}
            BEFORE INSERT ON stream_record
            FOR EACH ROW EXECUTE FUNCTION {function_name}()
            """
        )
        trigger_installed = True
        await holder.execute("SELECT pg_advisory_lock($1, $2)", key_a, key_b)

        def append() -> None:
            username, password = server.auth.split(":", 1)
            with httpx.Client(auth=(username, password), timeout=30) as client:
                response = client.post(
                    server.url + "/streams/v1/sources/append",
                    json={
                        "binding": binding.model_dump(mode="json"),
                        "position": 1,
                        "value": {"ref": "object://before-commit"},
                    },
                )
                response.raise_for_status()

        pending_append = asyncio.create_task(asyncio.to_thread(append))
        await _wait_for_advisory_waiter(observer, key_a, key_b)
        server.stop(crash=True)
        result = (await asyncio.gather(pending_append, return_exceptions=True))[0]
        assert isinstance(result, BaseException)
        await holder.execute("SELECT pg_advisory_unlock($1, $2)", key_a, key_b)
        server.start()

        with server.client() as replacement:
            recovered = replacement.open_stream_source(
                stream_id=stream_id,
                task_run_id=task_run_id,
                owner_id=binding.owner_id,
                attempt=1,
            )
            assert recovered.state == "OPEN"
            assert recovered.accepted_source_position == 0
            assert replacement.read_stream(stream_id).record_count == 0
            assert replacement.read_stream_prefix(stream_id, limit=10) == []
    finally:
        await holder.execute("SELECT pg_advisory_unlock($1, $2)", key_a, key_b)
        if trigger_installed:
            await observer.execute(
                f"DROP TRIGGER IF EXISTS {trigger_name} ON stream_record"
            )
            await observer.execute(f"DROP FUNCTION IF EXISTS {function_name}()")
        await holder.close()
        await observer.close()


def test_client_death_after_append_resumes_same_task_run_retry(
    restartable_api: RestartableAPI,
):
    server = restartable_api
    _, stream_id, task_run_id, first_binding = _source_channel(server)
    worker = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, os, sys
from prefect.client.orchestration import SyncPrefectClient
payload = json.load(sys.stdin)
with SyncPrefectClient(payload['url'], auth_string=payload['auth']) as client:
    client.append_stream_source(**payload['binding'], position=1, value=payload['value'])
    os._exit(77)
""",
        ],
        input=json.dumps(
            {
                "url": server.url,
                "auth": server.auth,
                "binding": first_binding.model_dump(mode="json"),
                "value": {"ref": "object://accepted-200"},
            }
        ),
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert worker.returncode == 77, worker.stderr
    server.restart()

    with server.client() as replacement:
        assert replacement.read_stream(stream_id).producer_position == 1
        failed = replacement.set_task_run_state(
            task_run_id, Failed(message="retry source after client loss")
        )
        assert failed.state is not None and failed.state.name == "AwaitingRetry"
        running = replacement.set_task_run_state(task_run_id, Running())
        assert running.state is not None and running.state.is_running()
        assert replacement.read_task_run(task_run_id).run_count == 2
        second = replacement.open_stream_source(
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=uuid4(),
            attempt=2,
        )
        assert second.state == "OPEN"
        assert second.accepted_source_position == 1
        with pytest.raises(StreamRequestError) as stale:
            replacement.append_stream_source(
                **first_binding.model_dump(), position=2, value="stale"
            )
        assert stale.value.code == "ATTEMPT_NOT_CURRENT"
        assert replacement.read_stream(stream_id).producer_position == 1
        accepted = replacement.append_stream_source(
            **second.binding.model_dump(),
            position=2,
            value={"ref": "object://resumed-201"},
        )
        assert accepted.producer_position == 2
        assert [
            record.value
            for record in replacement.read_stream_prefix(stream_id, limit=10)
        ] == [
            {"ref": "object://accepted-200"},
            {"ref": "object://resumed-201"},
        ]


def test_api_restart_after_eof_reconciles_completed_source(
    restartable_api: RestartableAPI,
):
    server = restartable_api
    _, stream_id, task_run_id, binding = _source_channel(server)
    with server.client() as client:
        client.append_stream_source(
            **binding.model_dump(), position=1, value={"ref": "object://tail"}
        )
        client.close_stream_source(**binding.model_dump(), expected_position=1)

    server.restart()
    with server.client() as replacement:
        recovered = replacement.open_stream_source(
            stream_id=stream_id,
            task_run_id=task_run_id,
            owner_id=binding.owner_id,
            attempt=1,
        )
        assert recovered.state == "COMPLETED"
        assert recovered.accepted_source_position == 1
        assert recovered.binding == binding
        replacement.close_stream_source(
            **recovered.binding.model_dump(), expected_position=1
        )
        info = replacement.read_stream(stream_id)
        assert info.state == "COMPLETED" and info.tail_sequence == 1
