"""Kill real API/client processes around accepted native-stream operations."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from prefect import flow, task
from prefect.client.orchestration import SyncPrefectClient
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.settings.context import get_current_settings
from prefect.states import Running
from tests.test_native_stream_http import native_backend as native_backend


class RestartableAPI:
    """A test-owned listener survives API process death; no port is borrowed."""

    auth = "native-test:restart-test"

    def __init__(self, directory: Path) -> None:
        self.socket = socket.socket()
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen()
        self.url = f"http://127.0.0.1:{self.socket.getsockname()[1]}/api"
        self.environment = {
            **os.environ,
            **get_current_settings().to_environment_variables(exclude_unset=True),
            "PREFECT_SERVER_API_AUTH_STRING": self.auth,
            "PREFECT_SERVER_SERVICES_TASK_RUN_RECORDER_ENABLED": "false",
            "PREFECT_API_SERVICES_TASK_RUN_RECORDER_ENABLED": "false",
        }
        self.directory = directory
        self.process: subprocess.Popen[bytes] | None = None
        self.pids: list[int] = []

    def start(self) -> None:
        assert self.process is None
        identity = uuid4().hex
        with (self.directory / f"api-{identity}.log").open("wb") as log:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "--factory",
                    "prefect.server.api.server:create_app",
                    "--fd",
                    str(self.socket.fileno()),
                    "--header",
                    f"X-Native-Recovery-ID:{identity}",
                    "--log-level",
                    "warning",
                ],
                pass_fds=(self.socket.fileno(),),
                env=self.environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        self.pids.append(self.process.pid)
        deadline = time.monotonic() + 60
        with httpx.Client(timeout=0.5, auth=tuple(self.auth.split(":"))) as client:
            while True:
                assert self.process.poll() is None, "Owned API exited during startup"
                try:
                    response = client.get(self.url + "/health")
                except httpx.TransportError:
                    pass
                else:
                    if (
                        response.status_code == 200
                        and response.headers.get("X-Native-Recovery-ID") == identity
                    ):
                        break
                assert time.monotonic() < deadline, "Owned API readiness timed out"
                time.sleep(0.05)

    def stop(self, *, crash: bool = False) -> None:
        if self.process is None:
            return
        try:
            if crash:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        finally:
            self.process = None

    def restart(self) -> None:
        self.stop(crash=True)
        self.start()
        assert self.pids[-1] != self.pids[-2]

    def client(self) -> SyncPrefectClient:
        return SyncPrefectClient(self.url, auth_string=self.auth)


@pytest.fixture
def restartable_api(
    native_backend: None, tmp_path: Path
) -> Generator[RestartableAPI, None, None]:
    if sys.platform == "win32":
        pytest.skip("Owned inherited-listener recovery runs on the Linux lane")
    server = RestartableAPI(tmp_path)
    try:
        server.start()
        yield server
    finally:
        server.stop()
        server.socket.close()


def setup_channel(server: RestartableAPI) -> tuple[UUID, UUID, UUID, UUID]:
    @flow
    def definition():
        pass

    definition_task = task(lambda: None)
    with server.client() as client:
        flow_run = client.create_flow_run(definition, state=Running())
        source = client.create_task_run(
            definition_task, flow_run.id, uuid4().hex, state=Running()
        )
        consumer = client.create_task_run(
            definition_task, flow_run.id, uuid4().hex, state=Running()
        )
        input_id, output_id = uuid4(), uuid4()
        client.create_stream(flow_run_id=flow_run.id, stream_id=input_id)
        binding = client.claim_stream_source(
            stream_id=input_id, task_run_id=source.id, owner_id=uuid4(), attempt=1
        )
        for position, document in enumerate([200, 201], 1):
            client.append_stream_source(
                **binding.model_dump(), position=position, value=document
            )
        client.finish_stream_source(**binding.model_dump())
    return flow_run.id, input_id, output_id, consumer.id


@pytest.mark.parametrize("boundary", ["reserve", "open", "read", "complete", "finish"])
def test_api_process_death_retains_exact_progress(
    restartable_api: RestartableAPI, boundary: str
):
    server = restartable_api
    flow_id, input_id, output_id, task_id = setup_channel(server)
    reservation = dict(flow_run_id=flow_id, stream_id=output_id, task_run_id=task_id)
    opening = dict(
        input_stream_id=input_id,
        output_stream_id=output_id,
        task_run_id=task_id,
        owner_id=uuid4(),
        attempt=1,
    )

    with server.client() as client:
        first_reservation = client.reserve_stream_output(**reservation)
    if boundary == "reserve":
        server.restart()
    with server.client() as client:
        assert client.reserve_stream_output(**reservation) == first_reservation
        binding = client.open_stream_consumer(**opening)
    if boundary == "open":
        server.restart()
    with server.client() as client:
        assert client.open_stream_consumer(**opening) == binding
        delivery = client.read_stream_consumer(**binding.model_dump())
        assert delivery is not None and delivery.value == 200
    if boundary == "read":
        server.restart()
    terminal: dict[str, Any] = dict(
        **delivery.token.model_dump(),
        kind="YIELD",
        value=200,
        managed_state={"count": 1},
    )
    with server.client() as client:
        assert client.read_stream_consumer(**binding.model_dump()) == delivery
        accepted = client.complete_stream_input(**terminal)
    if boundary == "complete":
        server.restart()
    with server.client() as client:
        assert client.complete_stream_input(**terminal) == accepted
        with pytest.raises(StreamRequestError) as drift:
            client.complete_stream_input(**{**terminal, "value": "changed"})
        assert drift.value.code == "IDENTITY_CONFLICT"
        next_delivery = client.read_stream_consumer(**binding.model_dump())
        assert next_delivery is not None and next_delivery.value == 201
        assert next_delivery.managed_state == {"count": 1}
        client.complete_stream_input(
            **next_delivery.token.model_dump(),
            kind="YIELD",
            value=201,
            managed_state={"count": 2},
        )
        client.finish_stream_consumer(**binding.model_dump())
    if boundary == "finish":
        server.restart()
    with server.client() as client:
        client.finish_stream_consumer(**binding.model_dump())
        assert client.read_stream_consumer(**binding.model_dump()) is None
        info = client.read_stream(output_id)
        assert info.state == "COMPLETED" and info.record_count == 2
        assert [
            item.value for item in client.read_stream_prefix(output_id, limit=10)
        ] == [200, 201]
    assert len(server.pids) == 2


def test_client_process_dies_after_terminal_acceptance(restartable_api: RestartableAPI):
    server = restartable_api
    flow_id, input_id, output_id, task_id = setup_channel(server)
    with server.client() as client:
        client.reserve_stream_output(
            flow_run_id=flow_id, stream_id=output_id, task_run_id=task_id
        )
        binding = client.open_stream_consumer(
            input_stream_id=input_id,
            output_stream_id=output_id,
            task_run_id=task_id,
            owner_id=uuid4(),
            attempt=1,
        )
        delivery = client.read_stream_consumer(**binding.model_dump())
    assert delivery is not None
    terminal = dict(
        **delivery.token.model_dump(mode="json"),
        kind="YIELD",
        value=200,
        managed_state={"count": 1},
    )
    worker = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, os, sys
from prefect.client.orchestration import SyncPrefectClient
payload = json.load(sys.stdin)
with SyncPrefectClient(payload['url'], auth_string=payload['auth']) as client:
    client.complete_stream_input(**payload['terminal'])
    os._exit(77)
""",
        ],
        input=json.dumps(dict(url=server.url, auth=server.auth, terminal=terminal)),
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert worker.returncode == 77, worker.stderr
    server.restart()
    with server.client() as replacement:
        replacement.complete_stream_input(**terminal)
        following = replacement.read_stream_consumer(**binding.model_dump())
        assert following is not None and following.value == 201
        assert following.managed_state == {"count": 1}
        assert replacement.read_stream(output_id).record_count == 1


def test_real_http_authentication_precedes_data_mutation(
    restartable_api: RestartableAPI,
):
    server = restartable_api
    flow_id, _, output_id, task_id = setup_channel(server)
    payload = dict(
        flow_run_id=str(flow_id), stream_id=str(output_id), task_run_id=str(task_id)
    )
    for auth in (None, ("incorrect", "incorrect")):
        response = httpx.post(
            server.url + "/streams/v1/outputs/reserve", json=payload, auth=auth
        )
        assert response.status_code == 401
    with server.client() as client:
        with pytest.raises(StreamRequestError) as absent:
            client.read_stream(output_id)
        assert absent.value.code == "NOT_FOUND"
        assert client.reserve_stream_output(**payload).state == "OPEN"
