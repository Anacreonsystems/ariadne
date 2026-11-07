import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator, Callable, Generator, List, Optional, Union
from unittest import mock
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import anyio
import httpx
import pytest
from starlette.status import WS_1008_POLICY_VIOLATION
from websockets.asyncio.server import (
    Server,
    ServerConnection,
    serve,
)
from websockets.exceptions import ConnectionClosed

from prefect._internal.compatibility.async_dispatch import async_dispatch
from prefect.events import Event
from prefect.events.clients import (
    AssertingEventsClient,
    AssertingPassthroughEventsClient,
)
from prefect.events.filters import EventFilter
from prefect.events.worker import EventsWorker
from prefect.server.api.server import SubprocessASGIServer
from prefect.server.events.pipeline import EventsPipeline
from prefect.settings import (
    PREFECT_API_URL,
    PREFECT_SERVER_CSRF_PROTECTION_ENABLED,
    PREFECT_SERVER_EPHEMERAL_ENABLED,
    get_current_settings,
    temporary_settings,
)
from prefect.types._datetime import DateTime, now
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.utilities.processutils import open_process


@pytest.fixture(autouse=True)
def add_prefect_loggers_to_caplog(
    caplog: pytest.LogCaptureFixture,
) -> Generator[None, None, None]:
    import logging

    logger = logging.getLogger("prefect")
    logger.propagate = True

    try:
        yield
    finally:
        logger.propagate = False


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def _reserve_hosted_api_socket(
    unused_tcp_port_factory: Callable[[], int],
) -> tuple[int, socket.socket | None]:
    """Reserve a test API port until Uvicorn inherits it."""
    if sys.platform == "win32":
        return unused_tcp_port_factory(), None

    reserved_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        reserved_socket.bind(("127.0.0.1", 0))
    except BaseException:
        reserved_socket.close()
        raise
    return reserved_socket.getsockname()[1], reserved_socket


@pytest.fixture(scope="session")
async def hosted_api_server(
    unused_tcp_port_factory: Callable[[], int],
    test_database_connection_url: Optional[str],
) -> AsyncGenerator[str, None]:
    """
    Runs an instance of the Prefect API server in a subprocess instead of the using the
    ephemeral application.

    Uses the same database as the rest of the tests.

    Yields:
        The API URL
    """
    # Ensure the per-worker database URL override is applied before we start the server
    _ = test_database_connection_url

    port, reserved_socket = _reserve_hosted_api_socket(unused_tcp_port_factory)
    server_id = str(uuid4())
    print(f"Running hosted API server on port {port}")

    # Will connect to the same database as normal test clients
    settings = get_current_settings().to_environment_variables(exclude_unset=True)

    # We must add creationflags to a dict so it is only passed as a function
    # parameter on Windows, because the presence of creationflags causes
    # errors on Unix even if set to None
    kwargs: dict[str, object] = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

    if reserved_socket is not None:
        kwargs["pass_fds"] = (reserved_socket.fileno(),)

    address_arguments = (
        ["--fd", str(reserved_socket.fileno())]
        if reserved_socket is not None
        else ["--host", "127.0.0.1", "--port", str(port)]
    )

    try:
        async with open_process(
            command=[
                "uvicorn",
                "--factory",
                "prefect.server.api.server:create_app",
                *address_arguments,
                "--header",
                f"X-Prefect-Test-Server-ID:{server_id}",
                "--log-level",
                "info",
            ],
            stdout=sys.stdout,
            stderr=sys.stderr,
            env={
                **os.environ,
                **settings,
            },
            **kwargs,
        ) as process:
            if reserved_socket is not None:
                reserved_socket.close()
                reserved_socket = None

            api_url = f"http://localhost:{port}/api"
            try:
                # Wait for the server we started to be ready. A successful response
                # from another test server on the same port is not sufficient.
                response = None
                readiness_started = time.monotonic()
                attempts = 0
                last_probe_outcome = None
                last_status = None
                owned_header_match = None
                error_type = None

                def readiness_diagnostic() -> str:
                    return json.dumps(
                        {
                            "owned_pid": process.pid,
                            "returncode": process.returncode,
                            "elapsed_seconds": round(
                                time.monotonic() - readiness_started, 3
                            ),
                            "attempts": attempts,
                            "last_probe_outcome": last_probe_outcome,
                            "last_status": last_status,
                            "owned_header_match": owned_header_match,
                            "error_type": error_type,
                            "cancel_scope_cancel_called": cancel_scope.cancel_called,
                        },
                        sort_keys=True,
                    )

                async with httpx.AsyncClient() as client:
                    with anyio.move_on_after(60) as cancel_scope:
                        while True:
                            if process.returncode is not None:
                                raise RuntimeError(
                                    "Hosted test Prefect API process exited before "
                                    f"readiness with code {process.returncode}. "
                                    + readiness_diagnostic()
                                )
                            attempts += 1
                            last_probe_outcome = "REQUEST_IN_PROGRESS"
                            error_type = None
                            try:
                                response = await client.get(api_url + "/health")
                            except httpx.ConnectError as error:
                                last_probe_outcome = "CONNECT_ERROR"
                                error_type = type(error).__name__
                            else:
                                last_probe_outcome = "RESPONSE"
                                last_status = response.status_code
                                owned_header_match = (
                                    response.headers.get("X-Prefect-Test-Server-ID")
                                    == server_id
                                )
                                if (
                                    response.status_code == 200
                                    and response.headers.get("X-Prefect-Test-Server-ID")
                                    == server_id
                                ):
                                    break
                            await anyio.sleep(0.1)

                    if process.returncode is not None:
                        raise RuntimeError(
                            "Hosted test Prefect API process exited before readiness "
                            f"with code {process.returncode}. " + readiness_diagnostic()
                        )
                    if (
                        response is None
                        or response.status_code != 200
                        or response.headers.get("X-Prefect-Test-Server-ID") != server_id
                    ):
                        raise RuntimeError(
                            "Timed out while attempting to connect to the owned "
                            "hosted test Prefect API. " + readiness_diagnostic()
                        )

                yield api_url
            finally:
                # Terminate before leaving open_process: its process context waits for
                # exit and generator cancellation skips statements after a bare yield.
                with anyio.CancelScope(shield=True):
                    try:
                        if process.returncode is None:
                            # In Windows SIGTERM is ineffective for process groups.
                            if sys.platform == "win32":
                                os.kill(process.pid, signal.CTRL_BREAK_EVENT)
                            else:
                                process.terminate()

                            for _ in range(10):
                                if process.returncode is not None:
                                    break
                                await anyio.sleep(1)
                            else:
                                process.kill()
                    except ProcessLookupError:
                        pass
    finally:
        if reserved_socket is not None:
            reserved_socket.close()


@pytest.fixture(autouse=True)
def use_hosted_api_server(hosted_api_server: str) -> Generator[str, None, None]:
    """
    Sets `PREFECT_API_URL` to the test session's hosted API endpoint.
    """
    with temporary_settings(
        {
            PREFECT_API_URL: hosted_api_server,
            PREFECT_SERVER_CSRF_PROTECTION_ENABLED: False,
        }
    ):
        yield hosted_api_server


@pytest.fixture
def disable_hosted_api_server() -> Generator[None, None, None]:
    """
    Disables the hosted API server by setting `PREFECT_API_URL` to `None`.
    """
    with temporary_settings(
        {
            PREFECT_API_URL: None,
        }
    ):
        yield


@pytest.fixture
def enable_ephemeral_server(
    disable_hosted_api_server: None,
) -> Generator[None, None, None]:
    """
    Enables the ephemeral server by setting `PREFECT_SERVER_EPHEMERAL_ENABLED` to `True`.
    """
    with temporary_settings(
        {
            PREFECT_SERVER_EPHEMERAL_ENABLED: True,
        }
    ):
        yield

    SubprocessASGIServer().stop()


@pytest.fixture
def mock_anyio_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[Callable[[float], None], None, None]:
    """
    Mock sleep used to not actually sleep but to set the current time to now + sleep
    delay seconds while still yielding to other tasks in the event loop.

    Provides "assert_sleeps_for" context manager which asserts a sleep time occurred
    within the context while using the actual runtime of the context as a tolerance.
    """
    original_now = now
    original_sleep = anyio.sleep
    time_shift = 0.0

    async def callback(delay_in_seconds: float) -> None:
        nonlocal time_shift
        time_shift += float(delay_in_seconds)
        # Preserve yield effects of sleep
        await original_sleep(0)

    def latest_now(*args: Any) -> DateTime:
        # Fast-forwards the time by the total sleep time
        # timedelta rather than pendulum's .add: on Python 3.13+ `now` returns a
        # standard library datetime.
        return original_now(*args) + timedelta(seconds=time_shift)

    monkeypatch.setattr("prefect.types._datetime.now", latest_now)

    sleep = AsyncMock(side_effect=callback)
    monkeypatch.setattr("anyio.sleep", sleep)

    @contextmanager
    def assert_sleeps_for(
        seconds: Union[int, float], extra_tolerance: Union[int, float] = 0
    ):
        """
        Assert that sleep was called for N seconds during the duration of the context.
        The runtime of the code during the context of the duration is used as an
        upper tolerance to account for sleeps that start based on a time. This is less
        brittle than attempting to freeze the current time.

        If an integer is provided, the upper tolerance will be rounded up to the nearest
        integer. If a float is provided, the upper tolerance will be a float.

        An optional extra tolerance may be provided to account for any other issues.
        This will be applied symmetrically.
        """
        run_t0 = original_now().timestamp()
        sleep_t0 = time_shift
        yield
        run_t1 = original_now().timestamp()
        sleep_t1 = time_shift
        runtime = run_t1 - run_t0
        if isinstance(seconds, int):
            # Round tolerance up to the nearest integer if input is an int
            runtime = int(runtime) + 1
        sleeptime = sleep_t1 - sleep_t0
        assert (
            sleeptime - float(extra_tolerance)
            <= seconds
            <= sleeptime + runtime + extra_tolerance
        ), (
            f"Sleep was called for {sleeptime}; expected {seconds} with tolerance of"
            f" +{runtime + extra_tolerance}, -{extra_tolerance}"
        )

    sleep.assert_sleeps_for = assert_sleeps_for

    return sleep


class Recorder:
    connections: int
    path: Optional[str]
    events: List[Event]
    token: Optional[str]
    filter: Optional[EventFilter]

    def __init__(self):
        self.connections = 0
        self.path = None
        self.events = []


class Puppeteer:
    token: Optional[str]

    hard_auth_failure: bool
    refuse_any_further_connections: bool
    hard_disconnect_after: Optional[UUID]

    outgoing_events: List[Event]

    def __init__(self):
        self.token = None
        self.hard_auth_failure = False
        self.refuse_any_further_connections = False
        self.hard_disconnect_after = None
        self.outgoing_events = []


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


@pytest.fixture
def puppeteer() -> Puppeteer:
    return Puppeteer()


@pytest.fixture
async def events_server(
    unused_tcp_port: int, recorder: Recorder, puppeteer: Puppeteer
) -> AsyncGenerator[Server, None]:
    server: Server

    async def handler(socket: ServerConnection) -> None:
        assert socket.request
        path = socket.request.path
        recorder.connections += 1
        if puppeteer.refuse_any_further_connections:
            raise ValueError("nope")

        recorder.path = path

        if path.endswith("/events/in"):
            await incoming_events(socket)
        elif path.endswith("/events/out"):
            await outgoing_events(socket)

    async def incoming_events(socket: ServerConnection):
        # 1. authentication (required when using the "prefect" subprotocol)
        if socket.subprotocol == "prefect":
            auth_message = json.loads(await socket.recv())

            assert auth_message["type"] == "auth"
            recorder.token = auth_message["token"]
            if puppeteer.token is not None and puppeteer.token != recorder.token:
                if not puppeteer.hard_auth_failure:
                    await socket.send(
                        json.dumps({"type": "auth_failure", "reason": "nope"})
                    )
                await socket.close(WS_1008_POLICY_VIOLATION)
                return

            await socket.send(json.dumps({"type": "auth_success"}))

        # 2. receive events
        while True:
            try:
                message = await socket.recv()
            except ConnectionClosed:
                return

            event = Event.model_validate_json(message)
            recorder.events.append(event)

            if puppeteer.hard_disconnect_after == event.id:
                puppeteer.hard_disconnect_after = None
                raise ValueError("Disconnect after incoming event")

    async def outgoing_events(socket: ServerConnection):
        # 1. authentication
        auth_message = json.loads(await socket.recv())

        assert auth_message["type"] == "auth"
        recorder.token = auth_message["token"]
        if puppeteer.token != recorder.token:
            if not puppeteer.hard_auth_failure:
                await socket.send(
                    json.dumps({"type": "auth_failure", "reason": "nope"})
                )
            await socket.close(WS_1008_POLICY_VIOLATION)
            return

        await socket.send(json.dumps({"type": "auth_success"}))

        # 2. filter
        filter_message = json.loads(await socket.recv())
        assert filter_message["type"] == "filter"
        recorder.filter = EventFilter.model_validate(filter_message["filter"])

        # 3. send events
        for event in puppeteer.outgoing_events:
            await socket.send(
                json.dumps(
                    {
                        "type": "event",
                        "event": event.model_dump(mode="json"),
                    }
                )
            )
            if puppeteer.hard_disconnect_after == event.id:
                puppeteer.hard_disconnect_after = None
                raise ValueError("zonk")

    def select_subprotocol(
        connection: ServerConnection, subprotocols: list[str]
    ) -> str | None:
        # Accept the "prefect" subprotocol if requested
        if "prefect" in subprotocols:
            return "prefect"
        return None

    async with serve(
        handler,
        host="localhost",
        port=unused_tcp_port,
        select_subprotocol=select_subprotocol,
    ) as server:
        yield server


@pytest.fixture
def events_api_url(events_server: Server, unused_tcp_port: int) -> str:
    return f"http://localhost:{unused_tcp_port}"


@pytest.fixture
def events_cloud_api_url(events_server: Server, unused_tcp_port: int) -> str:
    return f"http://localhost:{unused_tcp_port}/accounts/A/workspaces/W"


@pytest.fixture
def mock_should_emit_events(monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
    m = mock.Mock()
    m.return_value = True
    monkeypatch.setattr("prefect.events.utilities.should_emit_events", m)
    return m


@pytest.fixture
def asserting_events_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[EventsWorker, None, None]:
    worker = EventsWorker.instance(AssertingEventsClient)
    # Always yield the asserting worker when new instances are retrieved
    monkeypatch.setattr(EventsWorker, "instance", lambda *_: worker)
    try:
        yield worker
    finally:
        worker.drain()


@pytest.fixture
def asserting_and_emitting_events_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[EventsWorker, None, None]:
    worker = EventsWorker.instance(AssertingPassthroughEventsClient)
    # Always yield the asserting worker when new instances are retrieved
    monkeypatch.setattr(EventsWorker, "instance", lambda *_: worker)
    try:
        yield worker
    finally:
        worker.drain()


@pytest.fixture
async def events_pipeline(
    asserting_events_worker: EventsWorker,
) -> AsyncGenerator[EventsPipeline, None]:
    class AssertingEventsPipeline(EventsPipeline):
        async def aprocess_events(
            self,
            dequeue_events: bool = True,
            min_events: int = 0,
            timeout: int = 10,
        ):
            async def wait_for_min_events():
                while len(asserting_events_worker._client.events) < min_events:
                    await asyncio.sleep(0.1)

            if min_events:
                try:
                    await asyncio.wait_for(wait_for_min_events(), timeout=timeout)
                except TimeoutError:
                    raise TimeoutError(
                        f"Timed out waiting for {min_events} events after {timeout} seconds. Only observed {len(asserting_events_worker._client.events)} events."
                    )
            else:
                asserting_events_worker.wait_until_empty()

            if dequeue_events:
                events = asserting_events_worker._client.pop_events()
            else:
                events = asserting_events_worker._client.events

            messages = self.events_to_messages(events)
            await self.process_messages(messages)

        @async_dispatch(aprocess_events)
        def process_events(
            self,
            dequeue_events: bool = True,
            min_events: int = 0,
            timeout: int = 10,
        ):
            return run_coro_as_sync(
                self.aprocess_events(
                    dequeue_events=dequeue_events,
                    min_events=min_events,
                    timeout=timeout,
                )
            )

    yield AssertingEventsPipeline()


@pytest.fixture
async def emitting_events_pipeline(
    asserting_and_emitting_events_worker: EventsWorker,
) -> AsyncGenerator[EventsPipeline, None]:
    class AssertingAndEmittingEventsPipeline(EventsPipeline):
        async def aprocess_events(self):
            asserting_and_emitting_events_worker.wait_until_empty()
            events = asserting_and_emitting_events_worker._client.pop_events()

            messages = self.events_to_messages(events)
            await self.process_messages(messages)

        @async_dispatch(aprocess_events)
        def process_events(self):
            return run_coro_as_sync(self.aprocess_events())

    yield AssertingAndEmittingEventsPipeline()


@pytest.fixture
def reset_worker_events(
    asserting_events_worker: EventsWorker,
) -> Generator[None, None, None]:
    yield
    assert isinstance(asserting_events_worker._client, AssertingEventsClient)
    asserting_events_worker._client.events = []
