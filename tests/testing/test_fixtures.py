"""
Tests for prefect.testing.fixtures module.
"""

import asyncio
import json
import socket
import subprocess
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

import anyio
import httpx
import pytest
from anyio.abc import Process
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from prefect.testing import fixtures

pytestmark = pytest.mark.clear_db


class TestHostedApiServerWindowsProcessHandling:
    """Tests for Windows-specific process handling in hosted_api_server fixture."""

    @pytest.mark.unix
    async def test_unix_hosted_api_server_does_not_set_creation_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """
        On Unix-like systems, the hosted_api_server fixture should NOT set
        creationflags since it's a Windows-only parameter.
        """
        captured_kwargs: dict = {}
        mock_process = mock.MagicMock()
        mock_process.returncode = None
        mock_process.pid = 12345
        mock_process.terminate = mock.MagicMock()
        mock_process.kill = mock.MagicMock()

        def set_returncode():
            mock_process.returncode = 0

        mock_process.terminate.side_effect = set_returncode

        @asynccontextmanager
        async def mock_open_process(*args, **kwargs):
            captured_kwargs.update(kwargs)
            yield mock_process

        monkeypatch.setattr(fixtures, "open_process", mock_open_process)

        # Mock the HTTP client to simulate server ready
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"X-Prefect-Test-Server-ID": "test-server-id"}
        mock_response.raise_for_status = mock.MagicMock()

        mock_client = mock.MagicMock()
        mock_client.get = mock.AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = mock.AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = mock.AsyncMock(return_value=None)

        monkeypatch.setattr("httpx.AsyncClient", lambda: mock_client)
        monkeypatch.setattr(fixtures, "uuid4", lambda: "test-server-id")

        # Call the underlying generator function directly (not as a fixture)
        gen = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: 8000,
            test_database_connection_url=None,
        )
        try:
            await gen.__anext__()  # Start the fixture

            # Verify open_process was called without creationflags
            assert "creationflags" not in captured_kwargs
            assert captured_kwargs["pass_fds"]
        finally:
            await gen.aclose()

    @pytest.mark.windows
    async def test_windows_hosted_api_server_sets_process_group_creation_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """
        On Windows, the hosted_api_server fixture should create the uvicorn process
        with CREATE_NEW_PROCESS_GROUP flag so that the entire process tree can be
        terminated properly during teardown.
        """
        captured_kwargs: dict = {}
        mock_process = mock.MagicMock()
        mock_process.returncode = None
        mock_process.pid = 12345
        mock_process.kill = mock.MagicMock()

        def set_returncode(*args):
            mock_process.returncode = 0

        monkeypatch.setattr(fixtures.os, "kill", set_returncode)

        @asynccontextmanager
        async def mock_open_process(*args, **kwargs):
            captured_kwargs.update(kwargs)
            yield mock_process

        monkeypatch.setattr(fixtures, "open_process", mock_open_process)

        # Mock the HTTP client to simulate server ready
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"X-Prefect-Test-Server-ID": "test-server-id"}
        mock_response.raise_for_status = mock.MagicMock()

        mock_client = mock.MagicMock()
        mock_client.get = mock.AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = mock.AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = mock.AsyncMock(return_value=None)

        monkeypatch.setattr("httpx.AsyncClient", lambda: mock_client)
        monkeypatch.setattr(fixtures, "uuid4", lambda: "test-server-id")

        # Call the underlying generator function directly (not as a fixture)
        gen = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: 8000,
            test_database_connection_url=None,
        )
        try:
            await gen.__anext__()  # Start the fixture

            # Verify open_process was called with creationflags
            assert "creationflags" in captured_kwargs
            assert (
                captured_kwargs["creationflags"] == subprocess.CREATE_NEW_PROCESS_GROUP
            )
            assert "pass_fds" not in captured_kwargs
        finally:
            await gen.aclose()

    @pytest.mark.unix
    async def test_unix_hosted_api_server_uses_terminate_for_shutdown(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """
        On Unix-like systems, the hosted_api_server fixture should use
        process.terminate() for graceful shutdown.
        """
        mock_process = mock.MagicMock()
        mock_process.returncode = None  # Process still running initially
        mock_process.pid = 12345
        mock_process.terminate = mock.MagicMock()
        mock_process.kill = mock.MagicMock()

        # Make terminate set returncode to simulate process exit
        def set_returncode():
            mock_process.returncode = 0

        mock_process.terminate.side_effect = set_returncode

        @asynccontextmanager
        async def mock_open_process(*args, **kwargs):
            yield mock_process

        monkeypatch.setattr(fixtures, "open_process", mock_open_process)

        # Mock the HTTP client to simulate server ready
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"X-Prefect-Test-Server-ID": "test-server-id"}
        mock_response.raise_for_status = mock.MagicMock()

        mock_client = mock.MagicMock()
        mock_client.get = mock.AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = mock.AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = mock.AsyncMock(return_value=None)

        monkeypatch.setattr("httpx.AsyncClient", lambda: mock_client)
        monkeypatch.setattr(fixtures, "uuid4", lambda: "test-server-id")

        # Call the underlying generator function directly (not as a fixture)
        gen = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: 8000,
            test_database_connection_url=None,
        )
        await gen.__anext__()  # Start the fixture
        try:
            await gen.__anext__()  # Trigger teardown
        except StopAsyncIteration:
            pass

        # Verify process.terminate() was called
        mock_process.terminate.assert_called_once()


class TestHostedApiServerOwnership:
    @pytest.mark.unix
    async def test_closes_reserved_socket_when_spawn_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        reserved_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        reserved_socket.bind(("127.0.0.1", 0))
        port = reserved_socket.getsockname()[1]

        @asynccontextmanager
        async def failing_open_process(*args, **kwargs):
            raise RuntimeError("spawn failed")
            yield

        monkeypatch.setattr(
            fixtures,
            "_reserve_hosted_api_socket",
            lambda _: (port, reserved_socket),
        )
        monkeypatch.setattr(fixtures, "open_process", failing_open_process)

        gen = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: port,
            test_database_connection_url=None,
        )
        with pytest.raises(RuntimeError, match="spawn failed"):
            await gen.__anext__()
        assert reserved_socket.fileno() == -1

    @pytest.mark.unix
    async def test_rejects_healthy_foreign_listener(
        self,
        monkeypatch: pytest.MonkeyPatch,
        test_database_connection_url: str | None,
        disable_hosted_api_server: None,
    ):
        class ForeignHealthHandler(BaseHTTPRequestHandler):
            requests = 0

            def do_GET(self):
                type(self).requests += 1
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"true")

            def log_message(self, format: str, *args: object):
                pass

        foreign_server = ThreadingHTTPServer(("127.0.0.1", 0), ForeignHealthHandler)
        foreign_thread = threading.Thread(
            target=foreign_server.serve_forever, daemon=True
        )
        foreign_thread.start()
        port = foreign_server.server_address[1]
        monkeypatch.setattr(
            fixtures,
            "_reserve_hosted_api_socket",
            lambda _: (port, None),
        )
        open_process = fixtures.open_process
        children: list[Process] = []

        @asynccontextmanager
        async def observe_process(**kwargs: Any) -> AsyncIterator[Process]:
            async with open_process(**kwargs) as child:
                children.append(child)
                yield child

        monkeypatch.setattr(fixtures, "open_process", observe_process)

        gen = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: port,
            test_database_connection_url=test_database_connection_url,
        )
        try:
            # Either child exit or the ownership deadline may refuse startup.
            # Neither path may yield the foreign endpoint or leave our child alive.
            with pytest.raises(RuntimeError):
                await gen.__anext__()
            assert ForeignHealthHandler.requests > 0
            assert len(children) == 1
            assert children[0].returncode is not None
            async with fixtures.httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{port}/api/health")
            assert response.status_code == 200
        finally:
            await gen.aclose()
            foreign_server.shutdown()
            foreign_server.server_close()
            foreign_thread.join(timeout=5)
            assert not foreign_thread.is_alive()

    @pytest.mark.unix
    async def test_concurrent_servers_own_distinct_reserved_ports(
        self,
        test_database_connection_url: str | None,
        disable_hosted_api_server: None,
    ):
        first = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: 0,
            test_database_connection_url=test_database_connection_url,
        )
        second = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: 0,
            test_database_connection_url=test_database_connection_url,
        )
        startup_tasks = [
            asyncio.create_task(first.__anext__()),
            asyncio.create_task(second.__anext__()),
        ]
        urls: list[str] = []

        try:
            first_url, second_url = await asyncio.gather(*startup_tasks)
            urls = [first_url, second_url]
            assert first_url != second_url
            async with fixtures.httpx.AsyncClient() as client:
                responses = await asyncio.gather(
                    client.get(first_url + "/health"),
                    client.get(second_url + "/health"),
                )
            assert [response.status_code for response in responses] == [200, 200]
            assert (
                responses[0].headers["X-Prefect-Test-Server-ID"]
                != responses[1].headers["X-Prefect-Test-Server-ID"]
            )
        finally:
            for task in startup_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*startup_tasks, return_exceptions=True)
            await asyncio.gather(first.aclose(), second.aclose())

        async with fixtures.httpx.AsyncClient() as client:
            for url in urls:
                with pytest.raises(fixtures.httpx.ConnectError):
                    await client.get(url + "/health")


def _unready_test_app() -> Starlette:
    async def health(request: Any) -> PlainTextResponse:
        return PlainTextResponse("private-unready-body", status_code=503)

    return Starlette(routes=[Route("/api/health", health)])


class TestHostedApiReadinessDiagnostic:
    @pytest.mark.unix
    @pytest.mark.parametrize(
        "outcome", ["connect_after_response", "unowned", "request_in_progress"]
    )
    async def test_failure_diagnostic_redacts_probe_details(
        self,
        monkeypatch: pytest.MonkeyPatch,
        disable_hosted_api_server: None,
        outcome: str,
    ):
        owned_nonce = "private-owned-nonce"
        foreign_nonce = "private-foreign-nonce"
        private_body = "private-response-body"
        process = mock.MagicMock()
        process.pid = 12345
        process.returncode = None
        process.terminate.side_effect = lambda: setattr(process, "returncode", 0)

        @asynccontextmanager
        async def fake_process(**kwargs: Any) -> AsyncIterator[Any]:
            yield process

        original_scope = fixtures.anyio.move_on_after
        deadlines: list[float] = []

        def controlled_scope(seconds: float):
            deadlines.append(seconds)
            return original_scope(0.25)

        calls = 0

        async def handle(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if outcome == "request_in_progress":
                await anyio.sleep_forever()
            if outcome == "connect_after_response" and calls > 1:
                raise httpx.ConnectError("private-connect-exception", request=request)
            return httpx.Response(
                200,
                headers={"X-Prefect-Test-Server-ID": foreign_nonce},
                text=private_body,
            )

        original_client = fixtures.httpx.AsyncClient
        monkeypatch.setattr(fixtures, "open_process", fake_process)
        monkeypatch.setattr(fixtures, "uuid4", lambda: owned_nonce)
        monkeypatch.setattr(fixtures.anyio, "move_on_after", controlled_scope)
        monkeypatch.setattr(
            fixtures.httpx,
            "AsyncClient",
            lambda: original_client(
                transport=httpx.MockTransport(handle), trust_env=False
            ),
        )
        gen = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: 0, test_database_connection_url=None
        )
        try:
            with pytest.raises(
                RuntimeError,
                match="Timed out while attempting to connect to the owned hosted test Prefect API",
            ) as refused:
                await gen.__anext__()
            message = str(refused.value)
            details = json.loads("{" + message.partition("{")[2])
            assert deadlines == [60]
            assert details["owned_pid"] == 12345 and details["returncode"] is None
            assert details["attempts"] == calls > 0
            assert details["cancel_scope_cancel_called"] is True
            if outcome == "connect_after_response":
                assert details["last_probe_outcome"] == "CONNECT_ERROR"
                assert (
                    details["last_status"] == 200
                    and details["owned_header_match"] is False
                )
                assert details["error_type"] == "ConnectError"
            elif outcome == "unowned":
                assert details["last_probe_outcome"] == "RESPONSE"
                assert (
                    details["last_status"] == 200
                    and details["owned_header_match"] is False
                )
                assert details["error_type"] is None
            else:
                assert details["last_probe_outcome"] == "REQUEST_IN_PROGRESS"
                assert (
                    details["last_status"]
                    is details["owned_header_match"]
                    is details["error_type"]
                    is None
                )
            assert all(
                secret not in message
                for secret in (
                    owned_nonce,
                    foreign_nonce,
                    private_body,
                    "private-connect-exception",
                )
            )
            process.terminate.assert_called_once()
            process.kill.assert_not_called()
        finally:
            await gen.aclose()

    @pytest.mark.unix
    async def test_owned_unready_listener_times_out_with_safe_diagnostic(
        self,
        monkeypatch: pytest.MonkeyPatch,
        test_database_connection_url: str | None,
        disable_hosted_api_server: None,
        tmp_path: Path,
    ):
        original_open = fixtures.open_process
        children: list[Process] = []
        nonces: list[str] = []

        @asynccontextmanager
        async def unready_process(**kwargs: Any) -> AsyncIterator[Process]:
            command = list(kwargs["command"])
            assert (
                command[command.index("--factory") + 1]
                == "prefect.server.api.server:create_app"
            )
            command[command.index("--factory") + 1] = (
                "tests.testing.test_fixtures:_unready_test_app"
            )
            nonces.append(command[command.index("--header") + 1].split(":", 1)[1])
            async with original_open(**(kwargs | {"command": command})) as child:
                children.append(child)
                yield child

        monkeypatch.setattr(fixtures, "open_process", unready_process)
        gen = fixtures.hosted_api_server.__wrapped__(
            unused_tcp_port_factory=lambda: 0,
            test_database_connection_url=test_database_connection_url,
        )
        started = time.monotonic()
        try:
            with pytest.raises(
                RuntimeError,
                match="Timed out while attempting to connect to the owned hosted test Prefect API",
            ) as refused:
                await gen.__anext__()
            message = str(refused.value)
            details = json.loads("{" + message.partition("{")[2])
            assert 60 <= details["elapsed_seconds"] < 65
            assert 60 <= time.monotonic() - started < 70
            assert len(children) == 1 and details["owned_pid"] == children[0].pid
            assert details["returncode"] is None and children[0].returncode is not None
            assert details["last_probe_outcome"] == "RESPONSE"
            assert (
                details["last_status"] == 503 and details["owned_header_match"] is True
            )
            assert (
                details["cancel_scope_cancel_called"] is True
                and details["attempts"] > 0
            )
            assert details["error_type"] is None
            assert "private-unready-body" not in message and nonces[0] not in message
            (tmp_path / "owned-readiness-diagnostic.json").write_text(
                json.dumps(
                    {
                        "message": message,
                        "details": details,
                        "child_returncode_after_fixture": children[0].returncode,
                        "observed_elapsed_seconds": time.monotonic() - started,
                    },
                    sort_keys=True,
                )
                + "\n"
            )
        finally:
            await gen.aclose()
