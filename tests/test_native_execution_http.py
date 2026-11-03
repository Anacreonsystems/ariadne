"""Execution fencing through an owned HTTP API and real PostgreSQL.

The direct protocol controls below cover N1.5b. Whole-flow SDK reconstruction
is a separate gate; an idle child is used only as an actual death witness.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import secrets
import subprocess
import sys
import threading
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prefect import Task, flow, task
from prefect._internal.stream_execution import (
    StreamExecutionContext,
    stream_execution_context,
)
from prefect.client.base import PrefectHttpxAsyncClient, PrefectHttpxSyncClient
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.states import Completed, Pending, Running
from prefect.streams import SourceContext, SourceRecord, StreamSourceConfig
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI
from tests.test_native_stream_recovery_http import restartable_api as restartable_api


@dataclass
class Launch:
    flow_run_id: UUID
    claim: dict[str, Any] = field(repr=False)
    context: StreamExecutionContext = field(repr=False)
    manifest: Any = None
    source_binding: Any = None


def _key_pair() -> tuple[bytes, str]:
    key = Ed25519PrivateKey.generate()
    return (
        key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        ),
        key.public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        .hex(),
    )


def _claim(server: RestartableAPI) -> Launch:
    @flow(version="generation-v1", retries=2)
    def definition():
        pass

    with server.client() as client:
        run = client.create_flow_run(definition, state=Pending())
    private, public = _key_pair()
    request = {
        "flow_run_id": run.id,
        "operation_id": uuid4(),
        "holder_id": uuid4(),
        "holder_capability": secrets.token_hex(32),
        "holder_event_public_key": public,
        "supervisor_id": uuid4(),
        "supervisor_capability": secrets.token_hex(32),
    }
    with server.client() as client:
        binding = client.claim_stream_execution(**request)
    assert binding.generation == 1
    assert binding.state == "CLAIMED"
    assert "capability" not in type(binding).model_fields
    context = StreamExecutionContext(
        run.id,
        binding.generation,
        binding.holder_id,
        server.url,
        request["holder_capability"],
        private,
    )
    return Launch(run.id, request, context)


def _start(
    server: RestartableAPI,
    source_task: Task[Any, Any] | None = None,
    *,
    admit: bool = True,
    parameters: dict[str, Any] | None = None,
) -> Launch:
    launch = _claim(server)
    with stream_execution_context(launch.context), server.client() as client:
        # The ordinary flow engine enters Running before it calls the flow body.
        started = client.set_flow_run_state(launch.flow_run_id, Running())
        assert started.state.is_running()
        launch.manifest = client.reserve_stream_submission(
            flow_run_id=launch.flow_run_id,
            submission_key="source.orders",
            role="SOURCE",
            protocol="source-v1",
            task_name=source_task.name if source_task else "orders",
            task_key=source_task.task_key if source_task else "orders",
            task_version="generation-v1",
            flow_version="generation-v1",
            parameters=parameters
            if parameters is not None
            else {"ref": "object://orders"},
            task_policy={
                "retries": 2,
                "retry_delay": source_task.retry_delay_seconds if source_task else None,
            },
        )
        launch.context.register_manifest(
            launch.manifest.task_run_id, launch.manifest.manifest_digest
        )
        sealed = client.seal_stream_manifest(
            flow_run_id=launch.flow_run_id,
            operation_id=uuid4(),
            manifests=[
                {
                    "submission_key": launch.manifest.submission_key,
                    "manifest_digest": launch.manifest.manifest_digest,
                }
            ],
        )
        assert sealed.manifest_count == 1
        if not admit:
            return launch
        running = client.set_task_run_state(launch.manifest.task_run_id, Running())
        assert running.state.is_running()
        launch.source_binding = client.open_stream_source(
            stream_id=launch.manifest.stream_id,
            task_run_id=launch.manifest.task_run_id,
            owner_id=uuid4(),
            attempt=1,
        ).binding
        client.append_stream_source(
            **launch.source_binding.model_dump(),
            position=1,
            value={"document": 200},
        )
    return launch


def _observe_child_death() -> dict[str, Any]:
    # This child is intentionally not described as a reconstructed workflow.
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys; print('READY', flush=True); sys.stdin.read()",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "READY"
        child.kill()
        code = child.wait(timeout=10)
        assert code < 0
        return {
            "launcher_instance_id": uuid4(),
            "child_process_id": child.pid,
            "exit_code": code,
            "observed_at": datetime.now(timezone.utc),
            "kind": "OWNED_SIGKILL",
        }
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        for pipe in (child.stdin, child.stdout, child.stderr):
            pipe.close()


def _takeover_request(
    launch: Launch, death_witness: dict[str, Any] | None = None
) -> tuple[dict[str, Any], bytes]:
    private, public = _key_pair()
    return {
        "flow_run_id": launch.flow_run_id,
        "operation_id": uuid4(),
        "expected_generation": launch.context.generation,
        "expected_holder_id": launch.context.holder_id,
        "supervisor_id": launch.claim["supervisor_id"],
        "supervisor_capability": launch.claim["supervisor_capability"],
        "new_holder_id": uuid4(),
        "new_holder_capability": secrets.token_hex(32),
        "new_holder_event_public_key": public,
        "death_witness": death_witness
        if death_witness is not None
        else _observe_child_death(),
    }, private


def test_claim_exact_response_replay_after_api_restart(
    restartable_api: RestartableAPI,
):
    launch = _claim(restartable_api)
    restartable_api.restart()
    with restartable_api.client() as client:
        replay = client.claim_stream_execution(**launch.claim)
        assert replay.generation == 1
        assert replay.holder_id == launch.context.holder_id
        with pytest.raises(StreamRequestError) as refused:
            client.claim_stream_execution(**(launch.claim | {"holder_id": uuid4()}))
        assert refused.value.code == "IDENTITY_CONFLICT"


def test_takeover_fences_before_source_owner_rotation_and_prepares_normal_retry(
    restartable_api: RestartableAPI,
):
    server = restartable_api
    launch = _start(server)
    request, private = _takeover_request(launch)
    with server.client() as client:
        replacement = client.takeover_stream_execution(**request)
        assert replacement.generation == 2
        assert client.read_flow_run(launch.flow_run_id).state.name == "AwaitingRetry"
        assert client.read_task_run(launch.manifest.task_run_id).run_count == 1
        assert client.takeover_stream_execution(**request) == replacement

    with stream_execution_context(launch.context), server.client() as client:
        with pytest.raises(StreamRequestError) as stale:
            client.append_stream_source(
                **launch.source_binding.model_dump(),
                position=2,
                value={"document": 201},
            )
        assert stale.value.code == "STALE_EXECUTION"
        with pytest.raises(httpx.HTTPStatusError) as stale_flow:
            client.set_flow_run_state(launch.flow_run_id, Completed())
        assert stale_flow.value.response.json()["detail"]["code"] == "STALE_EXECUTION"

    context = StreamExecutionContext(
        launch.flow_run_id,
        replacement.generation,
        replacement.holder_id,
        server.url,
        request["new_holder_capability"],
        private,
    )
    witnesses = [
        {
            "submission_key": launch.manifest.submission_key,
            "manifest_digest": launch.manifest.manifest_digest,
        }
    ]
    with stream_execution_context(context), server.client() as client:
        assert client.set_flow_run_state(
            launch.flow_run_id, Running()
        ).state.is_running()
        with pytest.raises(StreamRequestError) as unprepared:
            client.append_stream_source(
                **launch.source_binding.model_dump(),
                position=2,
                value={"document": 201},
            )
        assert unprepared.value.code == "RECOVERY_NOT_READY"
        with pytest.raises(httpx.HTTPStatusError) as unprepared_task:
            client.set_task_run_state(launch.manifest.task_run_id, Running())
        assert (
            unprepared_task.value.response.json()["detail"]["code"]
            == "RECOVERY_NOT_READY"
        )
        prepare = dict(
            flow_run_id=launch.flow_run_id, operation_id=uuid4(), manifests=witnesses
        )
        prepared = client.prepare_stream_recovery(**prepare)
        task_run = client.read_task_run(launch.manifest.task_run_id)
        assert task_run.state.name == "AwaitingRetry"
        assert task_run.run_count == 1
        assert prepared.items[0].current_run_count == 1
        assert prepared.items[0].next_run_count == 2
        assert client.prepare_stream_recovery(**prepare) == prepared
        assert client.set_task_run_state(task_run.id, Running()).state.is_running()
        assert client.read_task_run(task_run.id).run_count == 2
        admitted = client.open_stream_source(
            stream_id=launch.manifest.stream_id,
            task_run_id=task_run.id,
            owner_id=uuid4(),
            attempt=2,
        )
        assert admitted.accepted_source_position == 1
        client.append_stream_source(
            **admitted.binding.model_dump(), position=2, value={"document": 201}
        )
        client.close_stream_source(**admitted.binding.model_dump(), expected_position=2)
        assert [
            row.value
            for row in client.read_stream_prefix(launch.manifest.stream_id, limit=10)
        ] == [{"document": 200}, {"document": 201}]


def test_only_one_concurrent_takeover_wins(restartable_api: RestartableAPI):
    launch = _start(restartable_api)
    first, _ = _takeover_request(launch)
    second, _ = _takeover_request(launch)

    def takeover(request: dict[str, Any]) -> Any:
        with restartable_api.client() as client:
            try:
                return client.takeover_stream_execution(**request)
            except StreamRequestError as error:
                return error

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(takeover, [first, second]))
    winners = [item for item in results if not isinstance(item, StreamRequestError)]
    refusals = [item for item in results if isinstance(item, StreamRequestError)]
    assert len(winners) == len(refusals) == 1
    assert winners[0].generation == 2
    assert refusals[0].code == "STALE_EXECUTION"


@pytest.mark.parametrize(
    "headers",
    [
        {"X-Prefect-Stream-Execution-Generation": "1"},
        {
            "X-Prefect-Stream-Execution-Generation": "65",
            "X-Prefect-Stream-Execution-Holder": str(uuid4()),
            "X-Prefect-Stream-Execution-Capability": "a" * 64,
        },
        {
            "X-Prefect-Stream-Execution-Generation": "1",
            "X-Prefect-Stream-Execution-Holder": str(uuid4()),
            "X-Prefect-Stream-Execution-Capability": "invalid",
        },
    ],
)
def test_malformed_execution_headers_return_protocol_refusal(
    restartable_api: RestartableAPI, headers: dict[str, str]
):
    launch = _claim(restartable_api)
    with httpx.Client(auth=tuple(restartable_api.auth.split(":"))) as client:
        response = client.post(
            restartable_api.url + f"/flow_runs/{launch.flow_run_id}/set_state",
            headers=headers,
            json={"state": {"type": "RUNNING"}},
        )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "PROTOCOL_ERROR"


@task(version="generation-v1", retries=2, stream=StreamSourceConfig())
def _sync_positioned_source(*, ctx: SourceContext, fail_after_resume: bool):
    if ctx.attempt > 1 and fail_after_resume:
        raise ValueError("declared client failure after infrastructure retry")
    for position in range(ctx.accepted_source_position + 1, 3):
        yield SourceRecord(position, {"document": 199 + position})
        if ctx.attempt == 1:
            os._exit(77)


@task(version="generation-v1", retries=2, stream=StreamSourceConfig())
async def _async_positioned_source(*, ctx: SourceContext, fail_after_resume: bool):
    if ctx.attempt > 1 and fail_after_resume:
        raise ValueError("declared client failure after infrastructure retry")
    for position in range(ctx.accepted_source_position + 1, 3):
        yield SourceRecord(position, {"document": 199 + position})
        if ctx.attempt == 1:
            os._exit(77)


@pytest.fixture
def execution_api(
    native_backend: None, tmp_path: Path
) -> Generator[RestartableAPI, None, None]:
    server = RestartableAPI(tmp_path)
    server.environment.update(
        {
            "PREFECT_SERVER_SERVICES_TASK_RUN_RECORDER_ENABLED": "true",
            "PREFECT_API_SERVICES_TASK_RUN_RECORDER_ENABLED": "true",
        }
    )
    try:
        server.start()
        yield server
    finally:
        server.stop()
        server.socket.close()


def _source_process(
    server: RestartableAPI,
    launch: Launch,
    context: StreamExecutionContext,
    mode: str,
    fail_after_resume: bool,
    retry_delay: float | None = None,
) -> tuple[int, int]:
    child_environment = dict(os.environ)
    child_environment.pop("PREFECT_SERVER_DATABASE_CONNECTION_URL", None)
    child_environment.pop("PREFECT_API_DATABASE_CONNECTION_URL", None)
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            """
import asyncio,json,sys,time
from uuid import UUID
from prefect import flow
from prefect._internal.stream_execution import StreamExecutionContext, stream_execution_context
from prefect._internal.stream_engine import _StreamRun
from prefect.client.orchestration import get_client
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.context import FlowRunContext
from prefect.events.worker import EventsWorker
from prefect.results import ResultStore
from prefect.settings import PREFECT_API_URL, PREFECT_API_AUTH_STRING, temporary_settings
from prefect.streams import Stream
from prefect.task_engine import run_task_sync,run_task_async
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.utilities.asyncutils import run_coro_as_sync
from tests.test_native_execution_http import _sync_positioned_source,_async_positioned_source
p=json.load(sys.stdin)
task_run_id,stream_id,flow_id=(UUID(p[k]) for k in ('task_run_id','stream_id','flow_id'))
execution=StreamExecutionContext(flow_id,p['generation'],UUID(p['holder_id']),p['url'],p['capability'],bytes.fromhex(p['private_key']))
execution.register_manifest(task_run_id,p['manifest_digest'])
@flow(version='generation-v1', retries=2)
def definition(): pass
with temporary_settings({PREFECT_API_URL:p['url'],PREFECT_API_AUTH_STRING:p['auth']}), stream_execution_context(execution):
    with get_client(sync_client=True) as client:
        task_run=client.read_task_run(task_run_id)
        flow_run=client.read_flow_run(flow_id)
        output=Stream(PrefectStreamStorage(flow_id),str(stream_id),producer_task_run_id=task_run_id)
        run=_StreamRun(output,task_run,source=True)
        run.registration_verified=True
        fn=_sync_positioned_source if p['mode']=='sync' else _async_positioned_source
        if p['retry_delay'] is not None: fn=fn.with_options(retry_delay_seconds=p['retry_delay'])
        arguments=dict(task=fn,task_run=task_run,parameters={'fail_after_resume':p['fail_after_resume'],'__prefect_stream_run__':run})
        expected_failure=False
        with FlowRunContext(flow=definition,flow_run=flow_run,client=client,task_runner=ThreadPoolTaskRunner(max_workers=2),result_store=ResultStore()):
            try:
                if p['mode']=='sync':
                    result=run_task_sync(**arguments)
                else:
                    result=asyncio.run(run_task_async(**arguments))
                assert result.accepted_source_position==2
            except ValueError:
                assert p['fail_after_resume']
                expected_failure=True
        EventsWorker.drain_all()
        deadline=time.monotonic()+15
        while True:
            observed=client.read_task_run(task_run_id)
            if observed.state.is_final(): break
            assert time.monotonic()<deadline,'signed terminal event was not recorded'
            time.sleep(.05)
        if expected_failure:
            assert observed.state.is_failed() and observed.run_count==3
        else:
            assert observed.state.is_completed() and observed.run_count==2
        print('NATIVE_SIGNED_SOURCE_COMPLETE',flush=True)
        sys.exit(78 if expected_failure else 0)
""",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=child_environment,
    )
    payload = {
        "url": server.url,
        "auth": server.auth,
        "flow_id": str(launch.flow_run_id),
        "task_run_id": str(launch.manifest.task_run_id),
        "stream_id": str(launch.manifest.stream_id),
        "manifest_digest": launch.manifest.manifest_digest,
        "generation": context.generation,
        "holder_id": str(context.holder_id),
        "capability": context.capability,
        "private_key": context.event_private_key.hex(),
        "mode": mode,
        "fail_after_resume": fail_after_resume,
        "retry_delay": retry_delay,
    }
    try:
        stdout, stderr = child.communicate(json.dumps(payload), timeout=60)
        assert child.returncode in (0, 77, 78), stdout + stderr
        return child.pid, child.returncode
    except BaseException:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        raise


@pytest.mark.parametrize(
    "mode,fail_after_resume,retry_delay",
    [
        ("sync", False, None),
        ("async", False, None),
        ("sync", True, None),
        ("async", True, None),
        ("sync", False, 5.0),
    ],
)
def test_signed_native_source_resumes_after_actual_process_death(
    execution_api: RestartableAPI,
    mode: str,
    fail_after_resume: bool,
    retry_delay: float | None,
):
    server = execution_api
    source_task = (
        _sync_positioned_source if mode == "sync" else _async_positioned_source
    )
    if retry_delay is not None:
        source_task = source_task.with_options(retry_delay_seconds=retry_delay)
    parameters = {"fail_after_resume": fail_after_resume}
    launch = _start(server, source_task, admit=False, parameters=parameters)
    child_pid, code = _source_process(
        server, launch, launch.context, mode, fail_after_resume, retry_delay
    )
    assert code == 77
    with server.client() as client:
        assert client.read_task_run(launch.manifest.task_run_id).run_count == 1
        assert client.read_stream(launch.manifest.stream_id).producer_position == 1
    request, private = _takeover_request(
        launch,
        {
            "launcher_instance_id": uuid4(),
            "child_process_id": child_pid,
            "exit_code": code,
            "observed_at": datetime.now(timezone.utc),
            "kind": "WAITPID",
        },
    )
    with server.client() as client:
        replacement = client.takeover_stream_execution(**request)
    context = StreamExecutionContext(
        launch.flow_run_id,
        replacement.generation,
        replacement.holder_id,
        server.url,
        request["new_holder_capability"],
        private,
    )
    with stream_execution_context(context), server.client() as client:
        assert client.set_flow_run_state(
            launch.flow_run_id, Running()
        ).state.is_running()
        prepared = client.prepare_stream_recovery(
            flow_run_id=launch.flow_run_id,
            operation_id=uuid4(),
            manifests=[
                {
                    "submission_key": launch.manifest.submission_key,
                    "manifest_digest": launch.manifest.manifest_digest,
                }
            ],
        )
        assert prepared.items[0].current_run_count == 1
        scheduled = client.read_task_run(launch.manifest.task_run_id)
        assert scheduled.run_count == 1
        scheduled_time = scheduled.state.state_details.scheduled_time
    _, replacement_code = _source_process(
        server, launch, context, mode, fail_after_resume, retry_delay
    )
    assert replacement_code == (78 if fail_after_resume else 0)
    with server.client() as client:
        task_run = client.read_task_run(launch.manifest.task_run_id)
        assert task_run.run_count == (3 if fail_after_resume else 2)
        if retry_delay is not None:
            assert task_run.start_time >= scheduled_time
        assert [
            row.value
            for row in client.read_stream_prefix(launch.manifest.stream_id, limit=10)
        ] == (
            [{"document": 200}]
            if fail_after_resume
            else [{"document": 200}, {"document": 201}]
        )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_execution_capability_is_removed_from_foreign_http_redirect(mode: str):
    observed: list[bool] = []

    class Target(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            present = any(
                key.lower().startswith("x-prefect-stream-execution-")
                for key in self.headers
            )
            body = json.dumps({"execution_header_present": present}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    target = ThreadingHTTPServer(("127.0.0.1", 0), Target)

    class Redirect(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            observed.append(
                bool(self.headers.get("X-Prefect-Stream-Execution-Capability"))
            )
            self.send_response(307)
            self.send_header(
                "Location", f"http://127.0.0.1:{target.server_port}/capture"
            )
            self.send_header("Content-Length", "0")
            self.end_headers()

    origin = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
    threads = [
        threading.Thread(target=server.serve_forever) for server in (origin, target)
    ]
    for thread in threads:
        thread.start()
    private, _ = _key_pair()
    api_url = f"http://127.0.0.1:{origin.server_port}/api"
    context = StreamExecutionContext(
        uuid4(), 1, uuid4(), api_url, secrets.token_hex(32), private
    )
    try:
        with stream_execution_context(context):
            if mode == "sync":
                with PrefectHttpxSyncClient(
                    base_url=api_url, follow_redirects=True
                ) as client:
                    response = client.get("/scope")
            else:

                async def request() -> httpx.Response:
                    async with PrefectHttpxAsyncClient(
                        base_url=api_url, follow_redirects=True
                    ) as client:
                        return await client.get("/scope")

                response = asyncio.run(request())
        assert response.json() == {"execution_header_present": False}
        assert observed == [True]
    finally:
        for server in (origin, target):
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=5)
