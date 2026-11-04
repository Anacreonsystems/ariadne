"""Linux-native replacement controls exercised against real kernel processes."""

from __future__ import annotations

import signal
import subprocess
import sys
from collections.abc import Generator
from types import SimpleNamespace
from uuid import uuid4

import anyio
import pytest

from prefect.runner._native_process import (
    NativePidfdScope,
    verify_native_process_death,
)
from prefect.runner._process_manager import ProcessHandle
from prefect.runner._starter_native import linux_process_identity
from prefect.streams import StreamProtocolError


@pytest.fixture
def native_child() -> Generator[subprocess.Popen[bytes], None, None]:
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)


def _candidate(process: subprocess.Popen[bytes]) -> SimpleNamespace:
    host, boot, ticks = linux_process_identity(process.pid)
    return SimpleNamespace(
        host_id=host,
        boot_id=boot,
        child_process_id=process.pid,
        process_start_ticks=ticks,
    )


def test_live_child_cannot_supply_a_death_witness_without_termination(
    native_child: subprocess.Popen[bytes],
):
    candidate = _candidate(native_child)
    with pytest.raises(StreamProtocolError, match="still alive"):
        verify_native_process_death(candidate, terminate=False)
    assert native_child.poll() is None


def test_pidfd_reports_exact_child_exit_after_kernel_kill(
    native_child: subprocess.Popen[bytes],
):
    candidate = _candidate(native_child)
    witness = verify_native_process_death(candidate)
    assert witness["kind"] == "PIDFD_EXIT"
    assert witness["child_process_id"] == native_child.pid
    assert witness["process_start_ticks"] == candidate.process_start_ticks
    assert witness["observed_at"].tzinfo is not None
    assert native_child.wait(timeout=10) == -signal.SIGKILL


def test_pid_with_different_start_ticks_is_never_signalled(
    native_child: subprocess.Popen[bytes],
):
    candidate = _candidate(native_child)
    candidate.process_start_ticks += 1
    witness = verify_native_process_death(candidate)
    assert witness["kind"] == "IDENTITY_GONE"
    assert native_child.poll() is None


def test_foreign_host_identity_refuses_inspection_and_leaves_child_alive(
    native_child: subprocess.Popen[bytes],
):
    candidate = _candidate(native_child)
    candidate.host_id = uuid4()
    with pytest.raises(StreamProtocolError, match="host/PID namespace"):
        verify_native_process_death(candidate)
    assert native_child.poll() is None


def test_reaped_process_has_positive_identity_gone_witness(
    native_child: subprocess.Popen[bytes],
):
    candidate = _candidate(native_child)
    native_child.kill()
    native_child.wait(timeout=10)
    witness = verify_native_process_death(candidate, terminate=False)
    assert witness["kind"] == "IDENTITY_GONE"


async def test_registered_process_handle_refuses_live_replacement():
    first = await anyio.open_process(
        [sys.executable, "-c", "import time; time.sleep(120)"]
    )
    second = await anyio.open_process(
        [sys.executable, "-c", "import time; time.sleep(120)"]
    )
    first_scope, second_scope = (
        NativePidfdScope(first.pid),
        NativePidfdScope(second.pid),
    )
    handle = ProcessHandle(first, termination_scope=first_scope)
    try:
        with pytest.raises(RuntimeError, match="live process"):
            handle.replace_process(second, second_scope)
        assert handle.pid == first.pid
        assert first.returncode is None and second.returncode is None
        first.kill()
        await first.wait()
        handle.replace_process(second, second_scope)
        assert handle.pid == second.pid
        assert handle.is_alive()
        handle.hard_kill()
        assert await second.wait() == -signal.SIGKILL
    finally:
        for process in (first, second):
            if process.returncode is None:
                process.kill()
            await process.wait()
            await process.aclose()
        first_scope.close()
        second_scope.close()
