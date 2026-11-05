"""Actual process controls for worker-owned physical Task startup."""

import os
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from anyio import Event, create_task_group, fail_after
from anyio.to_thread import run_sync

from prefect._internal.stream_code import native_task_code_identity
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.objects import TaskRunPolicy, WorkPool
from prefect.client.schemas.stream_subtasks import StreamWorkerTaskConfiguration
from prefect.logging import get_logger
from prefect.runner._starter_native import create_native_stream_credentials
from prefect.streams import StreamProtocolError
from prefect.utilities.importtools import import_object
from prefect.workers import _stream_subtasks as worker_subtasks
from prefect.workers._stream_subtasks import (
    NativeStreamSubtaskSupervisor,
    _SubtaskCleanupRequired,
    _SubtaskProcess,
)
from prefect.workers._worker_channel import WorkPoolWorkerChannel
from prefect.workers.process import ProcessWorker

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="Native subtask process profile requires Linux pidfd",
)


@pytest.fixture
def bootstrap_endpoint(
    request: pytest.FixtureRequest,
) -> Iterator[tuple[str, queue.Queue[tuple[str, str]]]]:
    requests: queue.Queue[tuple[str, str]] = queue.Queue()
    refusal_status = getattr(request, "param", 404)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            authorization = self.headers.get("Authorization", "")
            csrf_discovery = self.path.startswith("/api/csrf-token?")
            if not csrf_discovery:
                requests.put((self.path, authorization.partition(" ")[0]))
            health = self.path == "/api/health"
            body = (
                b"true"
                if health
                else b'{"detail":"No bootstrap in this transport control"}'
            )
            self.send_response(
                200 if health else 404 if csrf_discovery else refusal_status
            )
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            self.do_GET()

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/api", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def blocked_child(
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
) -> Iterator[_SubtaskProcess]:
    child = _SubtaskProcess.start(
        api_url=bootstrap_endpoint[0],
        cwd=Path(__file__).resolve().parents[2],
        stream_output=False,
    )
    try:
        yield child
    finally:
        child.close()


def bootstrap_payload(child: _SubtaskProcess, api_url: str) -> dict[str, Any]:
    holder, capability, private, _ = create_native_stream_credentials()
    return {
        "flow_run_id": str(uuid4()),
        "graph_revision_id": str(uuid4()),
        "assignment_epoch": 1,
        "subtask_id": str(uuid4()),
        "grant_id": str(uuid4()),
        "physical_epoch": 1,
        "holder_id": str(holder),
        "scope_digest": "11" * 32,
        "manifest_digest": "22" * 32,
        "task_run_id": str(uuid4()),
        "input_stream_id": None,
        "output_stream_id": str(uuid4()),
        "child_process_id": child.process.pid,
        "process_start_ticks": child.process_start_ticks,
        "api_url": api_url,
        "capability": capability,
        "event_private_key": private.hex(),
    }


def wait_for_consumed_bootstrap(child: _SubtaskProcess) -> None:
    descriptor = str(child.process.args[-2])
    path = Path(f"/proc/{child.process.pid}/fd/{descriptor}")
    deadline = time.monotonic() + 20
    while path.exists():
        assert child.process.poll() is None
        assert time.monotonic() < deadline, (
            "Child did not consume its private bootstrap"
        )
        time.sleep(0.01)
    assert child.process.poll() is None


def test_actual_worker_child_waits_for_private_go_and_is_reaped_before_cleanup(
    blocked_child: _SubtaskProcess,
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
):
    child = blocked_child
    child.write_bootstrap(bootstrap_payload(child, bootstrap_endpoint[0]))
    wait_for_consumed_bootstrap(child)
    assert bootstrap_endpoint[1].empty()
    assert child.scope.is_alive(child.process.pid)
    assert child.home.exists()
    child.close()
    assert child.process.returncode is not None
    assert not Path(f"/proc/{child.process.pid}").exists()
    assert not child.home.exists()
    assert bootstrap_endpoint[1].empty()


def test_actual_go_release_uses_only_subtask_http_authority(
    blocked_child: _SubtaskProcess,
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
):
    child = blocked_child
    payload = bootstrap_payload(child, bootstrap_endpoint[0])
    child.write_bootstrap(payload)
    wait_for_consumed_bootstrap(child)
    assert bootstrap_endpoint[1].empty()
    child.release()
    child.release()
    path, scheme = bootstrap_endpoint[1].get(timeout=20)
    assert path == f"/api/streams/v2/subtasks/{payload['subtask_id']}/bootstrap"
    assert scheme == "PrefectSubtask"
    # The transport fixture intentionally refuses business bootstrap. The child
    # must terminate instead of trying another authentication scheme or route.
    assert child.process.wait(timeout=20) != 0
    assert bootstrap_endpoint[1].empty()


def test_worker_child_never_inherits_platform_credentials_or_profiles(
    monkeypatch: pytest.MonkeyPatch,
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
):
    forbidden = {
        "PREFECT_API_AUTH_STRING",
        "PREFECT_API_KEY",
        "PREFECT_SERVER_API_AUTH_STRING",
        "PREFECT__FLOW_RUN_ID",
        "PREFECT__WORKER_ID",
        "PREFECT_TEST_FLOW_CAPABILITY",
    }
    for key in forbidden:
        monkeypatch.setenv(key, "owned-transport-control")
    child = _SubtaskProcess.start(
        api_url=bootstrap_endpoint[0],
        cwd=Path(__file__).resolve().parents[2],
        stream_output=False,
    )
    try:
        names = {
            item.partition(b"=")[0].decode()
            for item in Path(f"/proc/{child.process.pid}/environ")
            .read_bytes()
            .split(b"\0")
            if item
        }
        assert not names.intersection(forbidden)
        assert {"PREFECT_API_URL", "PREFECT_HOME", "PREFECT_PROFILES_PATH"} <= names
        assert child.home.stat().st_mode & 0o777 == 0o700
        assert not (child.home / "profiles.toml").exists()
        assert not any(
            "owned-transport-control" in str(arg) for arg in child.process.args
        )
    finally:
        child.close()


@pytest.mark.parametrize("invalid", ["identity", "oversized"])
def test_worker_refuses_bad_private_bootstrap_before_releasing_child(
    blocked_child: _SubtaskProcess,
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
    invalid: str,
):
    payload = bootstrap_payload(blocked_child, bootstrap_endpoint[0])
    if invalid == "identity":
        payload["process_start_ticks"] += 1
    else:
        payload["untrusted_oversized_field"] = "x" * (16 * 1024)
    with pytest.raises(StreamProtocolError):
        blocked_child.write_bootstrap(payload)
    assert not blocked_child.released
    assert blocked_child.bootstrap_fd >= 0
    assert bootstrap_endpoint[1].empty()


def test_worker_cannot_release_before_bootstrap(blocked_child: _SubtaskProcess):
    with pytest.raises(StreamProtocolError, match="no delivered bootstrap"):
        blocked_child.release()
    assert not blocked_child.released
    assert blocked_child.go_fd >= 0


def test_actual_descriptor_exhaustion_does_not_leak_startup_fds_or_home(tmp_path: Path):
    script = """
import errno
import os
import resource
import tempfile
from pathlib import Path
from prefect.workers._stream_subtasks import _SubtaskProcess

root = Path(os.environ['OWNED_FD_CONTROL_DIRECTORY'])
tempfile.tempdir = str(root)
before = set(os.listdir('/proc/self/fd'))
first_free = next(number for number in range(1024) if str(number) not in before)
_, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (first_free + 2, hard))
try:
    _SubtaskProcess.start(api_url='http://127.0.0.1:9/api', cwd=Path.cwd())
except OSError as exc:
    assert exc.errno == errno.EMFILE
else:
    raise AssertionError('The real descriptor limit did not prevent startup')
finally:
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
assert set(os.listdir('/proc/self/fd')) == before
assert not list(root.iterdir())
"""
    environment = dict(os.environ)
    environment["OWNED_FD_CONTROL_DIRECTORY"] = str(tmp_path)
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        env=environment,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr.decode()


def make_worker(client: PrefectClient) -> SimpleNamespace:
    return SimpleNamespace(
        backend_id=uuid4(),
        work_pool=SimpleNamespace(id=uuid4()),
        client=client,
        _logger=get_logger("workers.stream-subtask-control"),
    )


async def test_missing_optional_worker_endpoint_preserves_ordinary_client(
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
):
    async with PrefectClient(
        bootstrap_endpoint[0], auth_string="owned-worker:transport-control"
    ) as client:
        worker = make_worker(client)
        supervisor = NativeStreamSubtaskSupervisor(worker)
        with fail_after(10):
            await supervisor.run()
        assert not supervisor.runs
        path, scheme = bootstrap_endpoint[1].get_nowait()
        assert path == f"/api/streams/v2/workers/{worker.backend_id}/poll"
        assert scheme == "Basic"
        # This narrow control observes a real HTTP404, then proves the ordinary
        # API client remains usable. Full old-server compatibility is separate.
        assert await client.api_healthcheck() is None


@pytest.mark.parametrize("bootstrap_endpoint", [401, 403], indirect=True)
async def test_worker_auth_refusal_is_not_treated_as_an_old_optional_endpoint(
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
):
    async with PrefectClient(
        bootstrap_endpoint[0], auth_string="owned-worker:transport-control"
    ) as client:
        worker = make_worker(client)
        supervisor = NativeStreamSubtaskSupervisor(worker)
        returned = False

        async def run() -> None:
            nonlocal returned
            await supervisor.run()
            returned = True

        with fail_after(10):
            async with create_task_group() as group:
                group.start_soon(run)
                path, scheme = await run_sync(
                    partial(bootstrap_endpoint[1].get, timeout=5)
                )
                assert path == f"/api/streams/v2/workers/{worker.backend_id}/poll"
                assert scheme == "Basic"
                # A second request proves the first authentication refusal did
                # not trigger the optional-protocol HTTP404 fallback.
                assert await run_sync(
                    partial(bootstrap_endpoint[1].get, timeout=5)
                ) == (
                    path,
                    scheme,
                )
                assert not returned
                assert not supervisor.runs
                supervisor.stop()
        assert returned


@pytest.mark.parametrize("wrong_artifact", [False, True])
def test_same_child_code_preflight_precedes_any_grant_or_task_body(
    tmp_path: Path,
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
    wrong_artifact: bool,
):
    module = tmp_path / "preflight_source.py"
    module.write_text(
        "from pathlib import Path\n"
        "from prefect import task\n"
        "from prefect.streams import StreamSourceConfig, SourceRecord\n"
        "@task(version='worker-preflight-v1', retries=2, stream=StreamSourceConfig())\n"
        "def source(ctx, split: int, directory: str):\n"
        "    Path(directory, 'body-ran').write_text('unexpected')\n"
        "    yield SourceRecord(1, {'ref': str(split)})\n"
    )
    source = import_object(f"{module}:source")
    identity = native_task_code_identity(source.fn)
    configuration = StreamWorkerTaskConfiguration(
        entrypoint=f"{module}:source",
        task_key=identity.task_key,
        role="SOURCE",
        task_version=source.version,
        artifact_digest="00" * 32 if wrong_artifact else identity.artifact_digest,
        context_key="ctx",
        timeout_seconds=None,
        task_policy=TaskRunPolicy(
            retries=source.retries,
            retry_delay=source.retry_delay_seconds,
            retry_jitter_factor=source.retry_jitter_factor,
        ),
        effective_tags=[],
        input_key=None,
        parameters={
            "split": 0,
            "directory": str(tmp_path),
        },
    )
    options = {
        "api_url": bootstrap_endpoint[0],
        "cwd": Path(__file__).resolve().parents[2],
        "stream_output": False,
        "preflight": {
            "role": "SOURCE",
            "configuration": configuration.model_dump(mode="json"),
        },
    }
    if wrong_artifact:
        with pytest.raises(StreamProtocolError, match="refused its code preflight"):
            _SubtaskProcess.start(**options)
    else:
        child = _SubtaskProcess.start(**options)
        try:
            assert child.scope.is_alive(child.process.pid)
            assert child.bootstrap_fd >= 0
            assert not child.released
        finally:
            child.close()
    assert not (tmp_path / "body-ran").exists()
    assert bootstrap_endpoint[1].empty()


async def test_concurrent_identity_acknowledgements_create_one_subtask_owner(
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
):
    async with PrefectClient(
        bootstrap_endpoint[0], auth_string="owned-worker:transport-control"
    ) as client:
        worker = ProcessWorker(work_pool_name="owned-concurrent-startup")
        worker._client = client
        worker._work_pool = WorkPool(
            name="owned-concurrent-startup",
            type="process",
            default_queue_id=uuid4(),
            base_job_template={
                "prefect_native_streaming": {
                    "schema_version": 1,
                    "profile": "bounded-linear-v1",
                }
            },
        )
        worker._has_successfully_synced = True
        release_identity = Event()
        both_started = Event()
        worker_id = uuid4()
        started = 0
        owners: list[NativeStreamSubtaskSupervisor] = []

        class DelayedIdentityChannel(WorkPoolWorkerChannel):
            def __init__(self) -> None:
                self.identity_requests = 0

            async def ensure_worker_identity(self) -> UUID:
                self.identity_requests += 1
                await release_identity.wait()
                worker.backend_id = worker_id
                return worker_id

        channel = DelayedIdentityChannel()
        worker._worker_channel = channel

        async def ensure() -> None:
            nonlocal started
            started += 1
            if started == 2:
                both_started.set()
            await worker._ensure_stream_subtask_supervisor()
            owners.append(worker._stream_subtask_supervisor)

        # Hold only the external identity acknowledgement. The manager, async
        # task group and optional endpoint request all execute normally.
        with fail_after(10):
            async with create_task_group() as group:
                worker._runs_task_group = group
                group.start_soon(ensure)
                group.start_soon(ensure)
                await both_started.wait()
                release_identity.set()
        assert channel.identity_requests == 1
        assert len(owners) == 2
        assert owners[0] is owners[1]
        assert owners[0]._scope is None
        assert not owners[0].runs
        assert bootstrap_endpoint[1].qsize() == 1


def test_failed_start_retains_exact_child_until_a_later_wait_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    bootstrap_endpoint: tuple[str, queue.Queue[tuple[str, str]]],
):
    real_wait = subprocess.Popen.wait
    waits = 0

    def unknown_first_wait(
        process: subprocess.Popen[bytes], timeout: float | None = None
    ) -> int:
        nonlocal waits
        waits += 1
        if waits == 1:
            raise subprocess.TimeoutExpired(process.args, timeout)
        return real_wait(process, timeout=timeout)

    def failed_identity(pid: int) -> tuple[UUID, UUID, int]:
        raise StreamProtocolError("Injected identity setup failure after Popen")

    monkeypatch.setattr(subprocess.Popen, "wait", unknown_first_wait)
    monkeypatch.setattr(worker_subtasks, "linux_process_identity", failed_identity)
    with pytest.raises(_SubtaskCleanupRequired) as result:
        _SubtaskProcess.start(
            api_url=bootstrap_endpoint[0],
            cwd=Path(__file__).resolve().parents[2],
            stream_output=False,
        )
    child = result.value.child
    try:
        assert waits == 1
        assert child.home.exists()
        assert child.scope is not None
        os.fstat(child.scope._descriptor)
        os.fstat(child.bootstrap_fd)
        os.fstat(child.go_fd)
        assert bootstrap_endpoint[1].empty()
        child.close()
        assert waits == 2
        assert child.process.returncode is not None
        assert not Path(f"/proc/{child.process.pid}").exists()
        assert not child.home.exists()
        assert child.bootstrap_fd == child.go_fd == -1
        assert bootstrap_endpoint[1].empty()
    finally:
        child.close()
