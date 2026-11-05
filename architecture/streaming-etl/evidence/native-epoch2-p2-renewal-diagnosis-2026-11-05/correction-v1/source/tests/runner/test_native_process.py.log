"""Linux-native replacement controls exercised against real kernel processes."""

from __future__ import annotations

import signal
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import anyio
import httpx
import pytest

import prefect.runner._starter_native as native_starter
from prefect._internal.stream_execution import (
    StreamExecutionContext,
    get_stream_execution,
    stream_execution_context,
)
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.runner._native_process import (
    NativePidfdScope,
    verify_native_process_death,
)
from prefect.runner._process_manager import ProcessHandle
from prefect.runner._starter_native import NativeProcessStarter, linux_process_identity
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


def _renewal_starter(client: object) -> NativeProcessStarter:
    flow_id = uuid4()
    starter = NativeProcessStarter(
        client=client,
        flow_run=SimpleNamespace(id=flow_id),
        worker_id=uuid4(),
        work_pool_id=uuid4(),
        cwd=Path.cwd(),
        env={},
        entrypoint="flow.py:pipeline",
        stream_output=False,
    )
    starter.execution = StreamExecutionContext(
        flow_run_id=flow_id,
        generation=1,
        holder_id=uuid4(),
        api_url="http://renewal.test/api",
        capability="c" * 64,
        event_private_key=b"s" * 32,
    )
    starter.enrollment = SimpleNamespace(operation_id=uuid4())
    return starter


@pytest.mark.parametrize("status", [500, 502, 503, 599])
async def test_native_renewal_retries_server_error_with_same_identity_and_authority(
    monkeypatch: pytest.MonkeyPatch, status: int
):
    calls = []
    sleeps = []

    async def renew(**kwargs):
        calls.append((kwargs, get_stream_execution()))
        if len(calls) == 1:
            raise StreamRequestError("HTTP_ERROR", "temporary", status_code=status)
        return "renewed"

    async def sleep(delay):
        sleeps.append(delay)

    starter = _renewal_starter(SimpleNamespace(renew_native_stream_launch=renew))
    monkeypatch.setattr(
        native_starter,
        "anyio",
        SimpleNamespace(sleep=sleep, move_on_after=anyio.move_on_after),
    )
    kwargs = {
        "flow_run_id": starter.flow_run.id,
        "generation": 1,
        "enrollment_operation_id": starter.enrollment.operation_id,
    }
    with stream_execution_context(starter.execution):
        assert (
            await starter._request("renew_native_stream_launch", **kwargs) == "renewed"
        )
    assert [call[0] for call in calls] == [kwargs, kwargs]
    assert all(call[1] is starter.execution for call in calls)
    assert sleeps == [0.1]


async def test_native_renewal_lost_ack_reuses_accepted_heartbeat_identity(
    monkeypatch: pytest.MonkeyPatch,
):
    calls = []
    accepted = set()
    sleeps = []

    async def renew(**kwargs):
        calls.append(kwargs)
        accepted.add(tuple(sorted(kwargs.items())))
        if len(calls) == 1:
            raise httpx.ReadTimeout("accepted response was lost")
        if len(calls) == 2:
            raise StreamRequestError("HTTP_ERROR", "temporary", status_code=500)
        return "renewed"

    async def sleep(delay):
        sleeps.append(delay)

    starter = _renewal_starter(SimpleNamespace(renew_native_stream_launch=renew))
    monkeypatch.setattr(
        native_starter,
        "anyio",
        SimpleNamespace(sleep=sleep, move_on_after=anyio.move_on_after),
    )
    kwargs = {
        "flow_run_id": starter.flow_run.id,
        "generation": 1,
        "enrollment_operation_id": starter.enrollment.operation_id,
    }
    assert await starter._request("renew_native_stream_launch", **kwargs) == "renewed"
    assert calls == [kwargs] * 3 and len(accepted) == 1
    assert sleeps == [0.1, 0.2]


@pytest.mark.parametrize(
    "method,code,status",
    [
        ("renew_native_stream_launch", "HTTP_ERROR", 499),
        ("renew_native_stream_launch", "STALE_EXECUTION", 500),
        ("renew_native_stream_launch", "EXECUTION_BINDING_REQUIRED", 401),
        ("renew_native_stream_launch", "IDENTITY_CONFLICT", 403),
        ("read_native_stream_deployment_plan", "HTTP_ERROR", 500),
        ("release_stream_flow", "HTTP_ERROR", 503),
    ],
)
async def test_native_renewal_policy_does_not_retry_authority_or_other_methods(
    monkeypatch: pytest.MonkeyPatch, method: str, code: str, status: int
):
    calls = []

    async def request(**kwargs):
        calls.append(kwargs)
        raise StreamRequestError(code, "refused", status_code=status)

    async def sleep(delay):
        pytest.fail("A non-renewal or authoritative refusal must not be retried")

    starter = _renewal_starter(SimpleNamespace(**{method: request}))
    monkeypatch.setattr(
        native_starter,
        "anyio",
        SimpleNamespace(sleep=sleep, move_on_after=anyio.move_on_after),
    )
    with pytest.raises(StreamRequestError) as caught:
        await starter._request(method, flow_run_id=starter.flow_run.id)
    assert caught.value.code == code and len(calls) == 1


@pytest.mark.parametrize("transport_failure", [False, True])
async def test_native_renewal_exhaustion_keeps_live_process_and_paces_monitor(
    monkeypatch: pytest.MonkeyPatch,
    native_child: subprocess.Popen[bytes],
    transport_failure: bool,
):
    clock = [100.0]
    sleeps = []
    renewals = []
    plans = []
    warnings = []

    async def sleep(delay):
        sleeps.append(delay)
        clock[0] += delay

    async def plan(**kwargs):
        plans.append(kwargs)
        return SimpleNamespace(
            state="RELEASED" if len(plans) == 3 else "ACTIVE", cancel_requested=False
        )

    async def renew(**kwargs):
        renewals.append((clock[0], kwargs))
        if len(plans) == 1:
            if transport_failure:
                raise httpx.ReadTimeout("renewal reply unavailable")
            raise StreamRequestError("HTTP_ERROR", "temporary", status_code=500)

    starter = _renewal_starter(
        SimpleNamespace(
            read_native_stream_deployment_plan=plan, renew_native_stream_launch=renew
        )
    )
    monkeypatch.setattr(
        native_starter, "time", SimpleNamespace(monotonic=lambda: clock[0])
    )
    monkeypatch.setattr(
        native_starter,
        "anyio",
        SimpleNamespace(sleep=sleep, move_on_after=anyio.move_on_after),
    )
    monkeypatch.setattr(
        native_starter,
        "get_logger",
        lambda name: SimpleNamespace(warning=lambda *args: warnings.append(args)),
    )
    await starter._monitor(native_child)
    assert native_child.poll() is None
    assert len(plans) == 3 and len(warnings) == 1
    assert 20.0 <= sum(sleeps) - 1.0 < 20.2
    assert max(sleeps) <= 1.0 and len(renewals) <= 25
    assert renewals[-1][0] >= 120.5
    assert all(row[1] == renewals[0][1] for row in renewals)


async def test_native_renewal_hung_http_call_is_cancelled_by_existing_budget(
    monkeypatch: pytest.MonkeyPatch,
):
    clock = [100.0]
    budgets = []
    completed = []

    @contextmanager
    def bounded_scope(seconds):
        budgets.append(seconds)
        # Exercise real cancellation, while advancing only the unit's injected
        # monotonic clock by the production budget supplied to this scope.
        with anyio.move_on_after(0.01) as scope:
            yield scope
        clock[0] += seconds

    async def hung(**kwargs):
        try:
            await anyio.sleep_forever()
        finally:
            completed.append("cancelled")

    starter = _renewal_starter(SimpleNamespace(renew_native_stream_launch=hung))
    monkeypatch.setattr(
        native_starter, "time", SimpleNamespace(monotonic=lambda: clock[0])
    )
    monkeypatch.setattr(
        native_starter, "anyio", SimpleNamespace(move_on_after=bounded_scope)
    )
    with pytest.raises(httpx.ReadTimeout, match="budget exhausted"):
        await starter._request(
            "renew_native_stream_launch", flow_run_id=starter.flow_run.id
        )
    assert budgets == [20.0] and completed == ["cancelled"]


async def test_native_monitor_success_after_degradation_restores_normal_cadence(
    monkeypatch: pytest.MonkeyPatch, native_child: subprocess.Popen[bytes]
):
    clock = [100.0]
    plans = []
    renewals = []

    async def sleep(seconds):
        clock[0] += seconds

    async def request(method, **kwargs):
        if method == "read_native_stream_deployment_plan":
            plans.append(clock[0])
            return SimpleNamespace(
                state="RELEASED" if len(plans) == 24 else "ACTIVE",
                cancel_requested=False,
            )
        assert method == "renew_native_stream_launch"
        renewals.append(clock[0])
        if len(renewals) == 1:
            clock[0] += 20.0
            raise StreamRequestError("HTTP_ERROR", "budget exhausted", status_code=500)

    starter = _renewal_starter(SimpleNamespace())
    monkeypatch.setattr(starter, "_request", request)
    monkeypatch.setattr(
        native_starter, "time", SimpleNamespace(monotonic=lambda: clock[0])
    )
    monkeypatch.setattr(native_starter, "anyio", SimpleNamespace(sleep=sleep))
    await starter._monitor(native_child)
    assert renewals == [100.0, 120.5, 130.5]
    assert native_child.poll() is None


async def test_native_monitor_coordinator_exit_during_degradation_returns(
    monkeypatch: pytest.MonkeyPatch, native_child: subprocess.Popen[bytes]
):
    calls = []

    async def request(method, **kwargs):
        calls.append(method)
        if method == "read_native_stream_deployment_plan":
            return SimpleNamespace(state="ACTIVE", cancel_requested=False)
        assert method == "renew_native_stream_launch"
        raise StreamRequestError("HTTP_ERROR", "budget exhausted", status_code=500)

    def external_exit(*args):
        native_child.terminate()
        native_child.wait(timeout=5)

    starter = _renewal_starter(SimpleNamespace())
    monkeypatch.setattr(starter, "_request", request)
    monkeypatch.setattr(
        native_starter,
        "get_logger",
        lambda name: SimpleNamespace(warning=external_exit),
    )
    await starter._monitor(native_child)
    assert calls == ["read_native_stream_deployment_plan", "renew_native_stream_launch"]
    assert native_child.returncode == -signal.SIGTERM


async def test_native_renewal_programming_timeout_is_not_advisory():
    async def broken(**kwargs):
        raise TimeoutError("programming failure")

    starter = _renewal_starter(SimpleNamespace(renew_native_stream_launch=broken))
    with pytest.raises(TimeoutError, match="programming failure"):
        await starter._request(
            "renew_native_stream_launch", flow_run_id=starter.flow_run.id
        )


async def test_native_monitor_stale_renewal_still_kills_exact_child(
    native_child: subprocess.Popen[bytes],
):
    async def plan(**kwargs):
        return SimpleNamespace(state="ACTIVE", cancel_requested=False)

    async def renew(**kwargs):
        raise StreamRequestError("STALE_EXECUTION", "fenced")

    starter = _renewal_starter(
        SimpleNamespace(
            read_native_stream_deployment_plan=plan, renew_native_stream_launch=renew
        )
    )
    await starter._monitor(native_child)
    assert native_child.wait(timeout=5) == -signal.SIGKILL


@pytest.mark.parametrize("terminal", [True, False])
async def test_native_monitor_horizon_refusal_still_requires_terminal_authority(
    native_child: subprocess.Popen[bytes], terminal: bool
):
    async def plan(**kwargs):
        return SimpleNamespace(state="ACTIVE", cancel_requested=False)

    async def renew(**kwargs):
        raise StreamRequestError("RECOVERY_HORIZON_EXPIRED", "released")

    async def read(**kwargs):
        return SimpleNamespace(state=SimpleNamespace(is_final=lambda: terminal))

    starter = _renewal_starter(
        SimpleNamespace(
            read_native_stream_deployment_plan=plan,
            renew_native_stream_launch=renew,
            read_flow_run=read,
        )
    )
    if terminal:
        await starter._monitor(native_child)
    else:
        with pytest.raises(StreamRequestError, match="released"):
            await starter._monitor(native_child)
    assert native_child.poll() is None
