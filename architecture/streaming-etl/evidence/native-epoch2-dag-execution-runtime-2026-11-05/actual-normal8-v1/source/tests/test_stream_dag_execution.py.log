"""Finite coordinator state-machine controls with only external RPC substituted."""

import copy
import signal
from typing import Any
from uuid import uuid4

import httpx
import pytest

from prefect._internal import stream_dag_execution as runtime
from prefect._internal.stream_dag_execution import (
    DAGFlowBinding,
    DAGFlowRegistry,
    DAGOperationIntents,
    dag_operation_id,
    dag_registry_context,
    start_stream_graph,
)
from prefect.client.schemas.stream_dag_executions import (
    StreamDAGExecution,
    StreamDAGOperationResult,
)
from prefect.exceptions import CancelledRun
from prefect.streams import SealedStreamGraph, StreamProtocolError
from prefect.types._datetime import now


class RecordingRegistry(DAGFlowRegistry):
    """Exercise real coordinator decisions, replacing the external server only."""

    def __init__(self):
        super().__init__(
            DAGFlowBinding(uuid4(), 1, 1, uuid4(), "http://prefect.test/api"),
            cleanup_timeout=0.03,
        )
        self.calls = []
        self.phase = "PREVIEW"
        self.terminal = None
        self.control = None
        self.ready = 4
        self.activation = None
        self.activation_outcome = "ACTIVE"
        self.losses = set()
        self.read_complete = False
        self.sealed = SealedStreamGraph(
            graph_revision_id=uuid4(), graph_digest="a" * 64
        )
        self.register(uuid4(), self.sealed)

    def _guard(self, *, retry_policy: bool = False):
        pass

    def projection(self):
        terminal = self.phase == "TERMINAL"
        preview = self.phase == "PREVIEW"
        return StreamDAGExecution(
            graph_revision_id=self.sealed.graph_revision_id,
            flow_run_id=self.binding.flow_run_id,
            graph_digest=self.sealed.graph_digest,
            routing_digest=self._graphs[
                self.sealed.graph_revision_id
            ].intents.routing_digest,
            phase=self.phase,
            terminal_state=self.terminal,
            cleanup_complete=terminal,
            counts=dict(
                total=4,
                registered=0 if preview else 4,
                bootstrap_ready=0 if preview else self.ready,
                terminal_tasks=4 if terminal else 0,
                stopped=(0 if self.control == "PREVIEW_ABORT" else 4)
                if terminal
                else 0,
                physical_slots_remaining=0 if terminal or preview else 4,
                tags_remaining=0 if terminal or preview else 4,
            ),
            control=dict(
                operation_id=dag_operation_id(
                    self.sealed.graph_revision_id,
                    "abort-preview"
                    if self.control == "PREVIEW_ABORT"
                    else "cancel"
                    if self.control == "CANCEL"
                    else "fail",
                ),
                kind=self.control,
                state="COMPLETED" if terminal else "STOPPING",
            )
            if self.control
            else None,
            activation_operation_id=self.activation,
            readiness_set_digest="b" * 64 if self.activation else None,
            terminal_operation_id=uuid4() if terminal else None,
            terminal_outcome_digest="c" * 64 if terminal else None,
        )

    async def _dispatch(self, method: str, **body: Any):
        self.calls.append((method, copy.deepcopy(body)))
        if method == "read_stream_dag_execution":
            if self.phase == "STOPPING" or self.read_complete:
                self.phase = "TERMINAL"
                self.terminal = (
                    "CANCELLED"
                    if self.control == "CANCEL"
                    else "FAILED"
                    if self.control
                    else "COMPLETED"
                )
            return self.projection()
        kinds = {
            "start_stream_dag": "START",
            "activate_stream_dag": "ACTIVATE",
            "abort_stream_dag_preview": "ABORT_PREVIEW",
            "cancel_stream_dag": "CANCEL",
            "fail_stream_dag": "FAIL",
        }
        kind = kinds[method]
        if kind == "START" and self.phase == "PREVIEW":
            self.phase = "STARTING"
        if kind == "ACTIVATE":
            self.activation = body["operation_id"]
            self.phase = self.activation_outcome
            if self.phase == "TERMINAL":
                self.terminal = "COMPLETED"
            if self.phase == "STOPPING":
                self.control = "FAIL"
        if kind in {"ABORT_PREVIEW", "CANCEL", "FAIL"}:
            self.control = "PREVIEW_ABORT" if kind == "ABORT_PREVIEW" else kind
            self.phase = "TERMINAL"
            self.terminal = "CANCELLED" if kind == "CANCEL" else "FAILED"
        result = StreamDAGOperationResult(
            receipt=dict(
                operation_id=body["operation_id"],
                graph_revision_id=self.sealed.graph_revision_id,
                kind=kind,
                request_digest="d" * 64,
                outcome_digest="e" * 64,
                accepted_at=now("UTC"),
                effective_control_kind=self.control,
                control_operation_id=dag_operation_id(
                    self.sealed.graph_revision_id,
                    "abort-preview"
                    if self.control == "PREVIEW_ABORT"
                    else "cancel"
                    if self.control == "CANCEL"
                    else "fail",
                )
                if self.control
                else None,
            ),
            execution=self.projection(),
        )
        if method in self.losses:
            self.losses.remove(method)
            raise httpx.ReadTimeout("server committed but acknowledgement was lost")
        return result


@pytest.fixture(autouse=True)
def fast_observer(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(runtime, "METADATA_POLL_INTERVAL_SECONDS", 0.001)


def test_intents_are_detached_stable_and_first_fail_wins():
    graph = uuid4()
    intents = DAGOperationIntents(graph, "a" * 64)
    first = intents.request("cancel", reason="operator requested")
    first["reason"] = "mutated"
    assert (
        intents.request("cancel", reason="operator requested")["reason"]
        == "operator requested"
    )
    with pytest.raises(StreamProtocolError, match="first intent"):
        intents.request("cancel", reason="different")
    original = intents.request("fail", failure_kind="START_TIMEOUT")
    assert intents.request("fail", failure_kind="FLOW_SCOPE_EXCEPTION") == original
    assert original["operation_id"] == str(dag_operation_id(graph, "fail"))


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True, "1"])
async def test_bad_timeout_has_zero_transport(timeout: Any):
    registry = RecordingRegistry()
    with pytest.raises(StreamProtocolError, match="positive and finite"):
        await registry.start(registry.sealed, timeout=timeout)
    assert registry.calls == []


@pytest.mark.parametrize("phase", ["ACTIVE", "STOPPING", "TERMINAL"])
async def test_exact_activation_ack_allows_fast_complete_or_failure(phase: str):
    registry = RecordingRegistry()
    registry.activation_outcome = phase
    registry.losses = {"activate_stream_dag"}
    with dag_registry_context(registry):
        running = await start_stream_graph(registry.sealed, timeout=0.2)
    assert running.graph_revision_id == registry.sealed.graph_revision_id
    calls = [body for method, body in registry.calls if method == "activate_stream_dag"]
    assert len(calls) == 2 and calls[0] == calls[1]
    assert not any(method == "fail_stream_dag" for method, _ in registry.calls)
    if phase == "TERMINAL":
        result = await registry.wait(running, timeout=0.2)
        assert result.terminal_state == "COMPLETED"
    if phase == "STOPPING":
        with pytest.raises(StreamProtocolError, match="failed after"):
            await registry.wait(running, timeout=0.2)


async def test_wait_timeout_never_creates_a_control():
    registry = RecordingRegistry()
    running = await registry.start(registry.sealed, timeout=0.2)
    before = len(registry.calls)
    with pytest.raises(TimeoutError, match="observation"):
        await registry.wait(running, timeout=0.003)
    assert all(
        method == "read_stream_dag_execution" for method, _ in registry.calls[before:]
    )


async def test_start_timeout_freezes_one_fail_body_and_verifies_cleanup():
    registry = RecordingRegistry()
    registry.ready = 0
    with pytest.raises(TimeoutError, match="start timed out"):
        await registry.start(registry.sealed, timeout=0.003)
    fail = [body for method, body in registry.calls if method == "fail_stream_dag"]
    assert len(fail) == 1 and fail[0]["failure_kind"] == "START_TIMEOUT"
    await registry.finalize(ValueError("later failure"))
    assert (
        len([method for method, _ in registry.calls if method == "fail_stream_dag"])
        == 1
    )


async def test_unused_preview_finalizer_aborts_without_start_or_execution():
    registry = RecordingRegistry()
    registry.losses = {"abort_stream_dag_preview"}
    await registry.finalize(None)
    bodies = [
        body for method, body in registry.calls if method == "abort_stream_dag_preview"
    ]
    assert len(bodies) == 2 and bodies[0] == bodies[1]
    assert registry.phase == "TERMINAL" and registry.terminal == "FAILED"
    assert not any(
        method in {"start_stream_dag", "activate_stream_dag", "fail_stream_dag"}
        for method, _ in registry.calls
    )


async def test_abandoned_active_fails_instead_of_implicitly_waiting():
    registry = RecordingRegistry()
    await registry.start(registry.sealed, timeout=0.2)
    with pytest.raises(StreamProtocolError, match="abandoned"):
        await registry.finalize(None)
    fail = [body for method, body in registry.calls if method == "fail_stream_dag"]
    assert len(fail) == 1 and fail[0]["failure_kind"] == "COORDINATOR_ERROR"


async def test_exception_cleanup_preserves_original_exception_and_first_cause():
    registry = RecordingRegistry()
    await registry.start(registry.sealed, timeout=0.2)
    await registry.finalize(ValueError("business failure"))
    fail = [body for method, body in registry.calls if method == "fail_stream_dag"]
    assert len(fail) == 1 and fail[0]["failure_kind"] == "FLOW_SCOPE_EXCEPTION"


async def test_stopping_finalizer_joins_control_without_new_fail():
    registry = RecordingRegistry()
    await registry.start(registry.sealed, timeout=0.2)
    registry.phase = "STOPPING"
    registry.control = "FAIL"
    await registry.finalize(ValueError("business failure"))
    assert not any(method == "fail_stream_dag" for method, _ in registry.calls)


async def test_cancellation_cannot_be_swallowed_by_flow_finalization():
    registry = RecordingRegistry()
    running = await registry.start(registry.sealed, timeout=0.2)
    with pytest.raises(CancelledRun):
        await registry.cancel(running, reason="stop")
    with pytest.raises(CancelledRun):
        await registry.finalize(None)
    assert registry.phase == "TERMINAL"


async def test_unknown_seal_ack_is_recovered_before_preview_cleanup():
    registry = RecordingRegistry()
    registry._graphs.clear()
    operation = uuid4()
    calls = []

    def recover():
        calls.append(1)
        registry.register(operation, registry.sealed)
        return registry.sealed

    registry.register_pending(operation, b"same frozen seal", recover)
    await registry.finalize(ValueError("seal response lost"))
    assert calls == [1] and registry.phase == "TERMINAL"
    assert registry._pending == {}


async def test_handle_identity_refuses_other_graph_before_transport():
    registry = RecordingRegistry()
    with pytest.raises(StreamProtocolError, match="not registered"):
        await registry.start(
            SealedStreamGraph(graph_revision_id=uuid4(), graph_digest="a" * 64),
            timeout=0.2,
        )
    assert registry.calls == []


async def test_missing_registry_refuses_public_start():
    with pytest.raises(StreamProtocolError, match="DAG_LIFECYCLE_SCOPE_REQUIRED"):
        await start_stream_graph(
            SealedStreamGraph(graph_revision_id=uuid4(), graph_digest="a" * 64)
        )


@pytest.mark.parametrize("phase", ["ACTIVE", "STOPPING", "TERMINAL"])
async def test_recovered_start_obtains_own_activation_receipt(phase: str):
    registry = RecordingRegistry()
    registry.activation = dag_operation_id(
        registry.sealed.graph_revision_id, "activate"
    )
    registry.phase = phase
    registry.activation_outcome = phase
    if phase == "STOPPING":
        registry.control = "FAIL"
    if phase == "TERMINAL":
        registry.terminal = "COMPLETED"
    running = await registry.start(registry.sealed, timeout=0.2)
    assert running.graph_revision_id == registry.sealed.graph_revision_id
    assert (
        len([method for method, _ in registry.calls if method == "activate_stream_dag"])
        == 1
    )


@pytest.mark.parametrize(
    "retries,run_count,can_retry",
    [(None, 1, False), (0, 1, False), (2, 1, True), (2, 2, True), (2, 3, False)],
)
def test_flow_retry_boundary(retries: int | None, run_count: int, can_retry: bool):
    assert runtime._flow_can_retry(run_count, retries) is can_retry


async def test_pending_seal_changed_material_refuses_without_transport():
    registry = RecordingRegistry()
    operation = uuid4()
    registry.register_pending(operation, b"original", lambda: registry.sealed)
    with pytest.raises(StreamProtocolError, match="frozen request"):
        registry.register_pending(operation, b"changed", lambda: registry.sealed)
    assert registry.calls == []


async def test_missing_pending_seal_cleanup_is_named_and_not_silently_ignored():
    registry = RecordingRegistry()
    registry._graphs.clear()

    def recover():
        raise httpx.ReadTimeout("still unknown")

    registry.register_pending(uuid4(), b"frozen", recover)
    with pytest.raises(httpx.ReadTimeout):
        await registry.finalize(ValueError("original"))
    assert registry._pending and registry.calls == []


async def test_flow_scope_close_refuses_late_actions_and_new_seals():
    registry = RecordingRegistry()
    await registry.finalize(None)
    before = list(registry.calls)
    with pytest.raises(StreamProtocolError, match="closing"):
        await registry.start(registry.sealed, timeout=0.2)
    with pytest.raises(StreamProtocolError, match="closing"):
        registry.register_pending(uuid4(), b"late seal", lambda: registry.sealed)
    assert registry.calls == before


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_real_flow_engine_adopts_verified_server_terminal_without_state_proposal(
    asynchronous: bool, cancelled: bool
):
    from prefect import flow
    from prefect.client.schemas.objects import FlowRun
    from prefect.flow_engine import AsyncFlowRunEngine, FlowRunEngine
    from prefect.states import Cancelled, Failed, Running
    from prefect.streams import NativeStreamFlowConfig

    @flow(
        version="dag-engine-control-v1",
        streaming=NativeStreamFlowConfig(
            profile="partitioned-dag-v1", partition_count=None
        ),
    )
    def definition():
        raise AssertionError("Only authoritative state adoption is under test")

    initial = FlowRun(
        id=uuid4(), flow_id=uuid4(), name="owned-flow", run_count=1, state=Running()
    )
    terminal = (
        Cancelled(message="verified cleanup")
        if cancelled
        else Failed(message="verified cleanup")
    )
    authoritative = initial.model_copy(
        update={
            "state": terminal,
            "state_type": terminal.type,
            "state_name": terminal.name,
        }
    )
    calls = []

    class Sync:
        def read_flow_run(self, run_id):
            calls.append(run_id)
            return authoritative

    class Async:
        async def read_flow_run(self, run_id):
            calls.append(run_id)
            return authoritative

    engine = (AsyncFlowRunEngine if asynchronous else FlowRunEngine)(
        flow=definition, flow_run=initial
    )
    engine._is_started = True
    engine._client = Async() if asynchronous else Sync()
    original = ValueError("application failed")
    state = (
        await engine._adopt_dag_terminal(original)
        if asynchronous
        else engine._adopt_dag_terminal(original)
    )
    assert state is terminal and engine.flow_run is authoritative
    assert calls == [initial.id]
    assert (
        isinstance(engine._raised, CancelledRun)
        if cancelled
        else engine._raised is original
    )


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_real_flow_engine_does_not_adopt_another_attempt(asynchronous: bool):
    from prefect import flow
    from prefect.client.schemas.objects import FlowRun
    from prefect.flow_engine import AsyncFlowRunEngine, FlowRunEngine
    from prefect.states import Failed, Running
    from prefect.streams import NativeStreamFlowConfig

    @flow(
        version="dag-engine-control-v1",
        streaming=NativeStreamFlowConfig(
            profile="partitioned-dag-v1", partition_count=None
        ),
    )
    def definition():
        raise AssertionError("Flow body is not executed")

    initial = FlowRun(
        id=uuid4(), flow_id=uuid4(), name="owned-flow", run_count=1, state=Running()
    )
    other = initial.model_copy(update={"run_count": 2, "state": Failed()})

    class Sync:
        def read_flow_run(self, run_id):
            return other

    class Async:
        async def read_flow_run(self, run_id):
            return other

    engine = (AsyncFlowRunEngine if asynchronous else FlowRunEngine)(
        flow=definition, flow_run=initial
    )
    engine._is_started = True
    engine._client = Async() if asynchronous else Sync()
    result = (
        await engine._adopt_dag_terminal(ValueError())
        if asynchronous
        else engine._adopt_dag_terminal(ValueError())
    )
    assert result is None and engine.flow_run is initial


@pytest.mark.parametrize("journal_exists", [False, True])
async def test_termination_signal_joins_only_authoritative_cancel_journal(
    journal_exists: bool,
):
    from prefect.exceptions import TerminationSignal

    registry = RecordingRegistry()
    await registry.start(registry.sealed, timeout=0.2)
    if journal_exists:
        registry.phase = "STOPPING"
        registry.control = "CANCEL"
        with pytest.raises(CancelledRun):
            await registry.finalize(TerminationSignal(signal.SIGTERM))
    else:
        await registry.finalize(TerminationSignal(signal.SIGTERM))
    mutations = [
        method
        for method, _ in registry.calls
        if method in {"cancel_stream_dag", "fail_stream_dag"}
    ]
    assert mutations == ([] if journal_exists else ["fail_stream_dag"])


@pytest.mark.parametrize(
    "run_count,retries,permitted",
    [(1, 2, False), (2, 2, False), (3, 2, True), (1, 0, True)],
)
def test_public_pending_seal_retry_guard_precedes_any_registration_or_rpc(
    run_count: int, retries: int, permitted: bool, monkeypatch: pytest.MonkeyPatch
):
    from types import SimpleNamespace

    from prefect._internal.stream_execution import (
        StreamExecutionContext,
        stream_execution_context,
    )
    from prefect.context import FlowRunContext

    flow_id = uuid4()
    authority = StreamExecutionContext(
        flow_id, 1, uuid4(), "http://prefect.test/api", "a" * 64, b"s" * 32
    )
    context = SimpleNamespace(
        flow=SimpleNamespace(
            streaming=SimpleNamespace(profile="partitioned-dag-v1"), retries=retries
        ),
        flow_run=SimpleNamespace(id=flow_id, run_count=run_count),
    )
    monkeypatch.setattr(FlowRunContext, "get", classmethod(lambda cls: context))
    registry = DAGFlowRegistry(
        DAGFlowBinding(flow_id, run_count, 1, authority.holder_id, authority.api_url)
    )
    operation = uuid4()
    sealed = SealedStreamGraph(graph_revision_id=uuid4(), graph_digest="a" * 64)
    with stream_execution_context(authority):
        if permitted:
            registry.register_pending(operation, b"frozen", lambda: sealed)
            assert operation in registry._pending
        else:
            with pytest.raises(StreamProtocolError, match="UNSUPPORTED_FLOW_RETRY"):
                registry.register_pending(operation, b"frozen", lambda: sealed)
            assert registry._pending == {} and registry._graphs == {}


@pytest.mark.parametrize("paused_method", ["start_stream_dag", "activate_stream_dag"])
async def test_finalizer_joins_inflight_rpc_and_prevents_late_running_handle(
    paused_method: str,
):
    import asyncio

    entered = asyncio.Event()
    resume = asyncio.Event()

    class PausedRegistry(RecordingRegistry):
        async def _dispatch(self, method: str, **body: Any):
            result = await super()._dispatch(method, **body)
            if method == paused_method:
                entered.set()
                await resume.wait()
            return result

    registry = PausedRegistry()
    registry.cleanup_timeout = 0.5
    start = asyncio.create_task(registry.start(registry.sealed, timeout=1))
    await asyncio.wait_for(entered.wait(), timeout=1)
    before = list(registry.calls)
    finish = asyncio.create_task(registry.finalize(None))
    try:
        await asyncio.sleep(0)
        assert registry._finalizing
        assert registry.calls == before
        resume.set()
        with pytest.raises(StreamProtocolError, match="DAG_SCOPE_CLOSING"):
            await start
        with pytest.raises(StreamProtocolError, match="abandoned"):
            await finish
        assert registry.phase == "TERMINAL" and registry.terminal == "FAILED"
        fail = [body for method, body in registry.calls if method == "fail_stream_dag"]
        assert len(fail) == 1 and fail[0]["failure_kind"] == "COORDINATOR_ERROR"
        assert registry._inflight == 0 and registry._closed
    finally:
        resume.set()
        await asyncio.gather(start, finish, return_exceptions=True)


@pytest.mark.parametrize("cancel_journal", [False, True])
async def test_nested_caller_cancellation_waits_for_phase_safe_cleanup(
    cancel_journal: bool,
):
    import anyio

    class CallerCancelledRegistry(RecordingRegistry):
        async def _dispatch(self, method: str, **body: Any):
            if method == "start_stream_dag" and not getattr(
                self, "cancel_injected", False
            ):
                self.cancel_injected = True
                await super()._dispatch(method, **body)
                if cancel_journal:
                    self.phase = "STOPPING"
                    self.control = "CANCEL"
                caller_scope.cancel()
                await anyio.sleep(0)
                raise AssertionError(
                    "Caller cancellation did not interrupt admission ACK"
                )
            # A real scheduling checkpoint reproduces level cancellation during
            # cleanup, rather than using immediately-returning transport stubs.
            await anyio.sleep(0)
            return await super()._dispatch(method, **body)

    registry = CallerCancelledRegistry()
    if cancel_journal:
        with pytest.raises(CancelledRun):
            with anyio.CancelScope() as caller_scope:
                await registry.start(registry.sealed, timeout=0.2)
    else:
        with anyio.CancelScope() as caller_scope:
            await registry.start(registry.sealed, timeout=0.2)
    current = registry.projection()
    assert current.phase == "TERMINAL" and current.cleanup_complete
    assert current.counts.physical_slots_remaining == current.counts.tags_remaining == 0
    assert current.terminal_state == ("CANCELLED" if cancel_journal else "FAILED")
    fail = [body for method, body in registry.calls if method == "fail_stream_dag"]
    assert len(fail) == (0 if cancel_journal else 1)
    if fail:
        assert fail[0]["failure_kind"] == "START_EXCEPTION"
    assert not any(method == "cancel_stream_dag" for method, _ in registry.calls)
    with pytest.raises(CancelledRun if cancel_journal else StreamProtocolError):
        await registry.start(registry.sealed, timeout=0.2)


async def test_unknown_ack_commit_after_phase_read_reconciles_before_cleanup():
    from prefect.client.orchestration._streams.client import StreamRequestError

    class LateCommitRegistry(RecordingRegistry):
        first_read = True

        async def _dispatch(self, method: str, **body: Any):
            if method == "read_stream_dag_execution" and self.first_read:
                self.first_read = False
                result = await super()._dispatch(method, **body)
                # The cancelled/unknown-ACK START commits after the observer
                # read but before abort-preview acquires the server graph lock.
                self.phase = "STARTING"
                return result
            if method == "abort_stream_dag_preview" and self.phase == "STARTING":
                self.calls.append((method, copy.deepcopy(body)))
                raise StreamRequestError(
                    "CONTROL_CONFLICT", "START won the graph lock", retryable=False
                )
            return await super()._dispatch(method, **body)

    registry = LateCommitRegistry()
    with pytest.raises(StreamProtocolError, match="abandoned"):
        await registry.finalize(None)
    current = registry.projection()
    assert current.phase == "TERMINAL" and current.terminal_state == "FAILED"
    assert (
        current.cleanup_complete
        and current.counts.physical_slots_remaining
        == current.counts.tags_remaining
        == 0
    )
    mutations = [
        method
        for method, _ in registry.calls
        if method in {"fail_stream_dag", "cancel_stream_dag"}
    ]
    assert mutations == ["fail_stream_dag"]


async def test_phase_conflict_polls_unchanged_metadata_without_repeating_mutation():
    from prefect.client.orchestration._streams.client import StreamRequestError

    class VisibilityLagRegistry(RecordingRegistry):
        lag_reads = 0

        async def _dispatch(self, method: str, **body: Any):
            if method == "abort_stream_dag_preview":
                self.calls.append((method, copy.deepcopy(body)))
                self.phase = "STARTING"
                self.lag_reads = 2
                raise StreamRequestError(
                    "CONTROL_CONFLICT", "Admission already committed", retryable=False
                )
            if method == "read_stream_dag_execution" and self.lag_reads:
                self.calls.append((method, copy.deepcopy(body)))
                self.lag_reads -= 1
                self.phase = "PREVIEW"
                try:
                    return self.projection()
                finally:
                    self.phase = "STARTING"
            return await super()._dispatch(method, **body)

    registry = VisibilityLagRegistry()
    registry.cleanup_timeout = 1
    with pytest.raises(StreamProtocolError, match="abandoned"):
        await registry.finalize(None)
    assert registry.phase == "TERMINAL" and registry.terminal == "FAILED"
    methods = [method for method, _ in registry.calls]
    assert methods == [
        "read_stream_dag_execution",
        "abort_stream_dag_preview",
        "read_stream_dag_execution",
        "read_stream_dag_execution",
        "read_stream_dag_execution",
        "fail_stream_dag",
    ]
    assert registry.projection().cleanup_complete


@pytest.mark.parametrize(
    "code", ["IDENTITY_CONFLICT", "STALE_OWNER", "UNAUTHENTICATED"]
)
async def test_cleanup_does_not_reinterpret_identity_or_authority_refusal(code: str):
    from prefect.client.orchestration._streams.client import StreamRequestError

    class RefusedRegistry(RecordingRegistry):
        async def _dispatch(self, method: str, **body: Any):
            if method == "abort_stream_dag_preview":
                self.calls.append((method, copy.deepcopy(body)))
                raise StreamRequestError(code, "refused", retryable=False)
            return await super()._dispatch(method, **body)

    registry = RefusedRegistry()
    with pytest.raises(StreamRequestError) as error:
        await registry.finalize(None)
    assert error.value.code == code
    assert [method for method, _ in registry.calls] == [
        "read_stream_dag_execution",
        "abort_stream_dag_preview",
    ]
    assert registry.phase == "PREVIEW" and registry.control is None


async def test_unchanged_phase_deadline_retains_slots_and_original_conflict(
    monkeypatch: pytest.MonkeyPatch,
):
    from types import SimpleNamespace

    from prefect.client.orchestration._streams.client import StreamRequestError

    class StaleProjectionRegistry(RecordingRegistry):
        admitted = False

        async def _dispatch(self, method: str, **body: Any):
            if method == "abort_stream_dag_preview":
                self.calls.append((method, copy.deepcopy(body)))
                self.admitted = True
                self.phase = "STARTING"
                raise StreamRequestError(
                    "CONTROL_CONFLICT", "Admission committed", retryable=False
                )
            if method == "read_stream_dag_execution" and self.admitted:
                self.calls.append((method, copy.deepcopy(body)))
                self.phase = "PREVIEW"
                try:
                    return self.projection()
                finally:
                    self.phase = "STARTING"
            return await super()._dispatch(method, **body)

    clock = iter([0.0, 0.0, 0.1])
    monkeypatch.setattr(runtime, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    registry = StaleProjectionRegistry()
    with pytest.raises(StreamProtocolError, match="DAG_CLEANUP_PENDING") as error:
        await registry.finalize(None)
    assert isinstance(error.value.__cause__, StreamRequestError)
    assert error.value.__cause__.code == "CONTROL_CONFLICT"
    actual = registry.projection()
    assert actual.phase == "STARTING" and actual.counts.physical_slots_remaining == 4
    assert actual.cleanup_complete is False and actual.control is None
    assert [method for method, _ in registry.calls] == [
        "read_stream_dag_execution",
        "abort_stream_dag_preview",
        "read_stream_dag_execution",
    ]
