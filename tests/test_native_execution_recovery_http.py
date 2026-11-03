"""Actual stale-client and late-event boundaries of the native authority."""

from __future__ import annotations

import asyncio
import json
import os
import select
import subprocess
import sys
import time
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from prefect._internal.stream_execution import (
    StreamExecutionContext,
    stream_execution_context,
    stream_task_event_resource,
)
from prefect.events import Event
from prefect.states import Completed
from tests.test_native_execution_http import _start, _takeover_request
from tests.test_native_execution_http import execution_api as execution_api
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI


def _signed_event(
    task_run: Any, state: Any, execution: StreamExecutionContext
) -> Event:
    payload = {
        "task_run": task_run.model_dump(
            mode="json",
            exclude_none=True,
            exclude={
                "id",
                "created",
                "updated",
                "flow_run_id",
                "state_id",
                "state_type",
                "state_name",
                "state",
                "estimated_start_time_delta",
                "estimated_run_time",
            },
        ),
        "validated_state": state.model_dump(
            mode="json",
            include={
                "type",
                "name",
                "message",
                "state_details",
                "data",
            },
        ),
    }
    resource = {
        "prefect.resource.id": f"prefect.task-run.{task_run.id}",
        "prefect.run-count": str(task_run.run_count),
        "prefect.orchestration": "client",
    }
    with stream_execution_context(execution):
        resource = stream_task_event_resource(task_run, state, payload, resource)
    return Event(
        id=state.id,
        occurred=state.timestamp,
        event=f"prefect.task-run.{state.name}",
        resource=resource,
        payload=payload,
        related=[
            {
                "prefect.resource.id": f"prefect.flow-run.{task_run.flow_run_id}",
                "prefect.resource.role": "flow-run",
            }
        ],
    )


def _publish(server: RestartableAPI, event: Event) -> None:
    with httpx.Client(auth=tuple(server.auth.split(":")), timeout=10) as client:
        response = client.post(
            server.url + "/events", json=[event.model_dump(mode="json")]
        )
        assert response.status_code == 204


def test_current_generation_task_completion_is_recorded_after_flow_completion(
    execution_api: RestartableAPI,
):
    server = execution_api
    launch = _start(server)
    with stream_execution_context(launch.context), server.client() as client:
        client.close_stream_source(
            **launch.source_binding.model_dump(), expected_position=1
        )
        assert client.set_flow_run_state(
            launch.flow_run_id, Completed()
        ).state.is_completed()
        still_running = client.read_task_run(launch.manifest.task_run_id)
        assert still_running.state.is_running()
        event = _signed_event(still_running, Completed(), launch.context)
    _publish(server, event)
    deadline = time.monotonic() + 15
    with server.client() as client:
        while True:
            recorded = client.read_task_run(launch.manifest.task_run_id)
            if recorded.state.is_completed():
                break
            assert time.monotonic() < deadline, (
                "late current-generation completion was lost"
            )
            time.sleep(0.05)
        assert recorded.state.id == event.id
        assert recorded.run_count == 1
        assert client.read_flow_run(launch.flow_run_id).state.is_completed()


async def _state_exists(dsn: str, state_id: UUID) -> bool:
    connection = await asyncpg.connect(
        dsn.replace("postgresql+asyncpg:", "postgresql:", 1)
    )
    try:
        return bool(
            await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM task_run_state WHERE id = $1)", state_id
            )
        )
    finally:
        await connection.close()


def test_live_former_client_cannot_write_or_publish_late_failed_state(
    execution_api: RestartableAPI,
    test_database_connection_url: str,
):
    server = execution_api
    launch = _start(server)
    child_environment = dict(os.environ)
    child_environment.pop("PREFECT_SERVER_DATABASE_CONNECTION_URL", None)
    child_environment.pop("PREFECT_API_DATABASE_CONNECTION_URL", None)
    actor = subprocess.Popen(
        [
            sys.executable,
            "-c",
            """
import json,sys
from datetime import timedelta
from uuid import UUID
import httpx
from prefect._internal.stream_execution import StreamExecutionContext,stream_execution_context
from prefect.client.orchestration import SyncPrefectClient
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.states import Failed,Completed
from prefect.types._datetime import now
from tests.test_native_execution_recovery_http import _signed_event
p=json.loads(sys.stdin.readline())
execution=StreamExecutionContext(UUID(p['flow_id']),1,UUID(p['holder_id']),p['url'],p['capability'],bytes.fromhex(p['private_key']))
execution.register_manifest(UUID(p['task_id']),p['digest'])
with stream_execution_context(execution),SyncPrefectClient(p['url'],auth_string=p['auth']) as client:
    observed=client.read_task_run(UUID(p['task_id']))
    print('READY',flush=True)
    assert sys.stdin.readline().strip()=='PROCEED'
    result={}
    try: client.append_stream_source(**p['binding'],position=2,value={'document':201})
    except StreamRequestError as error: result['append']=error.code
    try: client.close_stream_source(**p['binding'],expected_position=1)
    except StreamRequestError as error: result['eof']=error.code
    try: client.set_flow_run_state(UUID(p['flow_id']),Completed())
    except httpx.HTTPStatusError as error: result['flow']=error.response.json()['detail']['code']
    failed=Failed(message='late old-holder event',timestamp=now('UTC')+timedelta(minutes=1))
    event=_signed_event(observed,failed,execution)
    with httpx.Client(auth=tuple(p['auth'].split(':'))) as transport:
        response=transport.post(p['url']+'/events',json=[event.model_dump(mode='json')])
        assert response.status_code==204
    result['event_id']=str(event.id)
    print(json.dumps(result),flush=True)
    sys.stdin.readline()
""",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=child_environment,
    )
    try:
        actor.stdin.write(
            json.dumps(
                {
                    "url": server.url,
                    "auth": server.auth,
                    "flow_id": str(launch.flow_run_id),
                    "task_id": str(launch.manifest.task_run_id),
                    "holder_id": str(launch.context.holder_id),
                    "capability": launch.context.capability,
                    "private_key": launch.context.event_private_key.hex(),
                    "digest": launch.manifest.manifest_digest,
                    "binding": launch.source_binding.model_dump(mode="json"),
                }
            )
            + "\n"
        )
        actor.stdin.flush()
        assert select.select([actor.stdout], [], [], 15)[0], (
            "former client did not start"
        )
        assert actor.stdout.readline().strip() == "READY"
        request, _ = _takeover_request(launch)
        with server.client() as client:
            assert client.takeover_stream_execution(**request).generation == 2
        actor.stdin.write("PROCEED\n")
        actor.stdin.flush()
        assert select.select([actor.stdout], [], [], 15)[0], (
            "former client did not respond"
        )
        result = json.loads(actor.stdout.readline())
        assert result["append"] == result["eof"] == result["flow"] == "STALE_EXECUTION"
        assert (
            actor.poll() is None
        )  # The former capability is exercised by a live process.
        deadline = time.monotonic() + 15
        while True:
            lines = "\n".join(
                p.read_text(errors="replace")
                for p in server.directory.glob("api-*.log")
            )
            if any(
                "Discarding fenced native TaskRun event" in line
                and result["event_id"] in line
                for line in lines.splitlines()
            ):
                break
            assert time.monotonic() < deadline, (
                "recorder did not observe the stale event"
            )
            time.sleep(0.05)
        assert not asyncio.run(
            _state_exists(test_database_connection_url, UUID(result["event_id"]))
        )
        with server.client() as client:
            assert client.read_task_run(launch.manifest.task_run_id).state.is_running()
            assert client.read_task_run(launch.manifest.task_run_id).run_count == 1
            assert (
                client.read_flow_run(launch.flow_run_id).state.name == "AwaitingRetry"
            )
            assert client.read_stream(launch.manifest.stream_id).producer_position == 1
        assert actor.poll() is None
    finally:
        if actor.poll() is None:
            actor.kill()
        actor.wait(timeout=10)
        for pipe in (actor.stdin, actor.stdout, actor.stderr):
            pipe.close()
