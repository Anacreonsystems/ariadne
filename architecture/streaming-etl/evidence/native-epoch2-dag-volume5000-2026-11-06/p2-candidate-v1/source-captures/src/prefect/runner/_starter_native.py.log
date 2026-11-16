"""Native local-process strategy for the existing FlowRunExecutor."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import socket
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import anyio
import anyio.abc
import httpx
from anyio.to_thread import run_sync
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prefect._internal.attempt_control import AttemptConclusion, EngineOutcomeReceipt
from prefect._internal.stream_execution import (
    StreamExecutionContext,
    stream_execution_context,
)
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.logging import get_logger
from prefect.runner._native_process import NativePidfdScope, verify_native_process_death
from prefect.runner._process_manager import ProcessHandle
from prefect.streams import StreamProtocolError
from prefect.utilities.processutils import sanitize_subprocess_env


def linux_process_identity(pid: int) -> tuple[UUID, UUID, int]:
    """Bind a process to this host/PID namespace and its Linux start ticks."""
    if sys.platform != "linux" or not hasattr(os, "pidfd_open"):
        raise StreamProtocolError("Native workers require the Linux pidfd profile")
    boot = UUID(Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    namespace = os.stat("/proc/self/ns/pid").st_ino
    host = uuid5(
        NAMESPACE_URL, f"prefect-native-host/{socket.gethostname()}/{namespace}"
    )
    stat = Path(f"/proc/{pid}/stat").read_text()
    # comm may contain spaces and parentheses. Fields after its final ')' start
    # at state (field3); starttime is field22.
    fields = stat[stat.rfind(")") + 2 :].split()
    return host, boot, int(fields[19])


def create_native_stream_credentials() -> tuple[UUID, str, bytes, str]:
    """Create one holder identity, opaque capability and Ed25519 signing pair."""
    private = Ed25519PrivateKey.generate()
    private_bytes = private.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    public = (
        private.public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        .hex()
    )
    return uuid4(), secrets.token_hex(32), private_bytes, public


class NativeProcessStarter:
    """Register a blocked child before one-use private bootstrap delivery."""

    def __init__(
        self,
        *,
        client: Any,
        flow_run: Any,
        worker_id: UUID,
        work_pool_id: UUID,
        cwd: Path,
        env: dict[str, str | None],
        entrypoint: str,
        stream_output: bool,
    ) -> None:
        self.client: Any = client
        self.flow_run: Any = flow_run
        self.worker_id: UUID = worker_id
        self.work_pool_id: UUID = work_pool_id
        self.cwd: Path = cwd
        self.env: dict[str, str | None] = env
        self.entrypoint: str = entrypoint
        self.stream_output: bool = stream_output
        self.launcher_id: UUID = uuid4()
        self.enrollment: Any = None
        self.execution: StreamExecutionContext | None = None
        self.control_channel: Any = None
        self._started = False
        self._verified_conclusion: AttemptConclusion | None = None
        self.cancellation_hooks: Callable[[Any, Any], Awaitable[None]] | None = None
        self._supervisor_id = uuid4()
        self._supervisor_capability = secrets.token_hex(32)

    def get_conclusion(self, flow_run_id: UUID) -> AttemptConclusion | None:
        """Expose only a server-verified terminal state to the shared executor."""
        if flow_run_id != self.flow_run.id:
            return None
        if self._verified_conclusion is not None:
            return self._verified_conclusion
        if self.control_channel is not None:
            return self.control_channel.get_conclusion(flow_run_id)
        return None

    def _record_terminal(self, flow_run: Any) -> None:
        if flow_run.state is None or not flow_run.state.is_final():
            raise StreamProtocolError("Native terminal evidence requires a final state")
        self._verified_conclusion = EngineOutcomeReceipt.state_reported(
            state_id=flow_run.state.id,
            state_type=flow_run.state.type.value,
            state_name=flow_run.state.name,
        )

    async def request_cancellation(self, flow_run_id: UUID) -> None:
        """Keep native cancellation bound to the current execution generation."""
        if self.execution is None or flow_run_id != self.flow_run.id:
            return
        with stream_execution_context(self.execution):
            await self._request(
                "cancel_native_stream",
                flow_run_id=flow_run_id,
                operation_id=uuid5(
                    NAMESPACE_URL,
                    f"prefect-native-worker-cancel/{flow_run_id}/{self.execution.generation}",
                ),
                expected_generation=self.execution.generation,
                reason="Cancellation observed by the native process worker",
            )

    async def _request(self, method: str, **kwargs: Any) -> Any:
        """Replay stable journal requests across a bounded transport outage."""
        deadline = time.monotonic() + 20.0
        renewal_delay = 0.1
        while True:
            try:
                if method == "renew_native_stream_launch":
                    # Bound even the HTTP client's own retry loop. A late or
                    # lost renewal reply is safe to repeat with this identity.
                    remaining = max(0.0, deadline - time.monotonic())
                    result: Any = None
                    with anyio.move_on_after(remaining) as renewal_scope:
                        result = await getattr(self.client, method)(**kwargs)
                    if renewal_scope.cancel_called:
                        raise httpx.ReadTimeout(
                            "Native launch renewal budget exhausted"
                        )
                    return result
                return await getattr(self.client, method)(**kwargs)
            except StreamRequestError as refusal:
                if (
                    method == "renew_native_stream_launch"
                    and refusal.code == "HTTP_ERROR"
                    and 500 <= refusal.status_code <= 599
                ):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise
                    await anyio.sleep(min(renewal_delay, remaining))
                    if time.monotonic() >= deadline:
                        raise
                    renewal_delay = min(renewal_delay * 2, 1.0)
                    continue
                if not (
                    method
                    in {"release_stream_flow", "retire_verified_native_stream_launch"}
                    and refusal.code == "METADATA_NOT_READY"
                    and refusal.retryable
                    and time.monotonic() < deadline
                ):
                    raise
                await anyio.sleep(0.1)
            except httpx.TransportError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                if method == "renew_native_stream_launch":
                    await anyio.sleep(min(renewal_delay, remaining))
                    if time.monotonic() >= deadline:
                        raise
                    renewal_delay = min(renewal_delay * 2, 1.0)
                    continue
                await anyio.sleep(0.1)

    @staticmethod
    def _credentials() -> tuple[UUID, str, bytes, str]:
        return create_native_stream_credentials()

    def _bind_execution(self, holder: UUID, capability: str, private: bytes) -> None:
        self.execution = StreamExecutionContext(
            self.flow_run.id,
            self.enrollment.generation,
            holder,
            str(self.client.api_url).rstrip("/"),
            capability,
            private,
        )

    async def enroll(self) -> None:
        launcher_host, launcher_boot, launcher_ticks = linux_process_identity(
            os.getpid()
        )
        plan = await self._request(
            "read_native_stream_deployment_plan", flow_run_id=self.flow_run.id
        )
        if plan.work_pool_id != self.work_pool_id:
            raise StreamProtocolError("Native deployment belongs to another work pool")
        holder, capability, private, public = create_native_stream_credentials()
        self.enrollment = await self._request(
            "enroll_native_stream_deployment",
            flow_run_id=self.flow_run.id,
            operation_id=uuid5(
                NAMESPACE_URL,
                f"prefect-native-enroll/{self.flow_run.id}/{self.launcher_id}",
            ),
            declaration_digest=plan.declaration_digest,
            worker_id=self.worker_id,
            work_pool_id=self.work_pool_id,
            launcher_instance_id=self.launcher_id,
            launcher_host_id=launcher_host,
            launcher_boot_id=launcher_boot,
            launcher_process_id=os.getpid(),
            launcher_start_ticks=launcher_ticks,
            holder_id=holder,
            holder_capability=capability,
            holder_event_public_key=public,
            supervisor_id=self._supervisor_id,
            supervisor_capability=self._supervisor_capability,
        )
        self._bind_execution(holder, capability, private)

    async def recover(
        self,
        candidate: Any,
        witness: dict[str, Any] | None = None,
        *,
        startup_failed: bool = False,
    ) -> bool:
        """Authorize first, verify kernel death, then acquire the next generation."""
        if candidate.identity_kind == "LAUNCHER":
            # An enrolled launcher may supervise other flows. Read-only gone
            # verification precedes authorization, so a live launcher neither
            # receives a signal nor accumulates durable operations per poll.
            await run_sync(
                partial(verify_native_process_death, candidate, terminate=False)
            )
        challenge = secrets.token_hex(32)
        operation = uuid5(
            NAMESPACE_URL,
            f"prefect-native-replace/{self.flow_run.id}/{candidate.generation}/{self.launcher_id}",
        )
        await self._request(
            "authorize_native_stream_replacement",
            flow_run_id=self.flow_run.id,
            operation_id=operation,
            expected_generation=candidate.generation,
            enrollment_operation_id=candidate.enrollment_operation_id,
            worker_id=self.worker_id,
            work_pool_id=self.work_pool_id,
            host_id=candidate.host_id,
            identity_kind=candidate.identity_kind,
            challenge_capability=challenge,
        )
        if witness is None:
            witness = await run_sync(
                partial(
                    verify_native_process_death,
                    candidate,
                    terminate=candidate.identity_kind == "CHILD",
                )
            )
        plan = await self._request(
            "read_native_stream_deployment_plan", flow_run_id=self.flow_run.id
        )
        if plan.cancel_requested:
            observed = await self._request(
                "read_flow_run", flow_run_id=self.flow_run.id
            )
            if self.cancellation_hooks is not None:
                await self.cancellation_hooks(observed, observed.state)
            await self._request(
                "finalize_native_stream_cancellation",
                flow_run_id=self.flow_run.id,
                operation_id=operation,
                expected_generation=candidate.generation,
                enrollment_operation_id=candidate.enrollment_operation_id,
                challenge_capability=challenge,
                death_witness=witness,
            )
            self._record_terminal(
                await self._request("read_flow_run", flow_run_id=self.flow_run.id)
            )
            return False
        observed = await self._request("read_flow_run", flow_run_id=self.flow_run.id)

        async def retire() -> bool:
            await self._request(
                "retire_verified_native_stream_launch",
                flow_run_id=self.flow_run.id,
                operation_id=operation,
                expected_generation=candidate.generation,
                enrollment_operation_id=candidate.enrollment_operation_id,
                challenge_capability=challenge,
                death_witness=witness,
                error=(
                    "Native process exited before starting its FlowRun"
                    if startup_failed or candidate.identity_kind == "LAUNCHER"
                    else "Native process verified for terminal resource retirement"
                ),
            )
            self._record_terminal(
                await self._request("read_flow_run", flow_run_id=self.flow_run.id)
            )
            return False

        if (
            startup_failed
            or candidate.identity_kind == "LAUNCHER"
            or (
                observed.state is not None
                and (observed.state.is_pending() or observed.state.is_final())
            )
        ):
            return await retire()
        holder, capability, private, public = create_native_stream_credentials()
        launcher = uuid4()
        supervisor, supervisor_capability = uuid4(), secrets.token_hex(32)
        launcher_host, launcher_boot, launcher_ticks = linux_process_identity(
            os.getpid()
        )
        try:
            self.enrollment = await self._request(
                "confirm_native_stream_replacement",
                flow_run_id=self.flow_run.id,
                operation_id=operation,
                challenge_capability=challenge,
                launcher_instance_id=launcher,
                launcher_host_id=launcher_host,
                launcher_boot_id=launcher_boot,
                launcher_process_id=os.getpid(),
                launcher_start_ticks=launcher_ticks,
                new_holder_id=holder,
                new_holder_capability=capability,
                new_holder_event_public_key=public,
                new_supervisor_id=supervisor,
                new_supervisor_capability=supervisor_capability,
                death_witness=witness,
            )
        except StreamRequestError as refusal:
            if refusal.code not in {"RECOVERY_POLICY_EXHAUSTED", "MANIFEST_NOT_SEALED"}:
                raise
            return await retire()
        self.launcher_id = launcher
        self._supervisor_id, self._supervisor_capability = (
            supervisor,
            supervisor_capability,
        )
        self._bind_execution(holder, capability, private)
        get_logger("runner.native").info(
            "Confirmed native replacement for flow run %s, generation %s",
            self.flow_run.id,
            self.enrollment.generation,
        )
        return True

    async def _monitor(self, process: anyio.abc.Process) -> None:
        assert self.execution is not None
        renewed_at = 0.0
        while process.returncode is None:
            try:
                plan = await self._request(
                    "read_native_stream_deployment_plan", flow_run_id=self.flow_run.id
                )
                if plan.state == "RELEASED":
                    return
                if plan.cancel_requested:
                    if process.returncode is None:
                        process.kill()
                    return
                if time.monotonic() - renewed_at >= 10.0:
                    try:
                        await self._request(
                            "renew_native_stream_launch",
                            flow_run_id=self.flow_run.id,
                            generation=self.execution.generation,
                            enrollment_operation_id=self.enrollment.operation_id,
                        )
                    except (StreamRequestError, httpx.TransportError) as error:
                        if isinstance(error, StreamRequestError) and not (
                            error.code == "HTTP_ERROR"
                            and 500 <= error.status_code <= 599
                        ):
                            raise
                        get_logger("runner.native").warning(
                            "Native launch renewal is degraded for flow run %s; "
                            "continuing to monitor its live coordinator",
                            self.flow_run.id,
                        )
                    else:
                        renewed_at = time.monotonic()
            except StreamRequestError as refusal:
                if refusal.code == "RECOVERY_HORIZON_EXPIRED":
                    current = await self._request(
                        "read_flow_run", flow_run_id=self.flow_run.id
                    )
                    if current.state is not None and current.state.is_final():
                        return
                    raise
                if refusal.code == "STALE_EXECUTION":
                    if process.returncode is None:
                        process.kill()
                    return
                if refusal.code != "RECOVERY_NOT_READY":
                    raise
            await anyio.sleep(0.5)

    async def start(
        self,
        flow_run: Any,
        task_status: anyio.abc.TaskStatus[ProcessHandle] = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        if self.execution is None or self.enrollment is None or not self.entrypoint:
            raise StreamProtocolError("Native worker must enroll before child launch")
        handle: ProcessHandle | None = None
        while True:
            assert self.execution is not None
            with stream_execution_context(self.execution):
                handle, identity = await self._start_attempt(
                    flow_run, handle, task_status
                )
                observed = await self._request("read_flow_run", flow_run_id=flow_run.id)
                if observed.state is not None and observed.state.is_final():
                    self._record_terminal(observed)
                    # Owned waitpid completed. Retire platform payloads after
                    # the final outcome; external references remain client-owned.
                    await self._request("release_stream_flow", flow_run_id=flow_run.id)
                    return
                plan = await self._request(
                    "read_native_stream_deployment_plan", flow_run_id=flow_run.id
                )
                if plan.cancel_requested:
                    host, boot, ticks = identity
                    if self.cancellation_hooks is not None:
                        await self.cancellation_hooks(observed, observed.state)
                    await self._request(
                        "finalize_native_stream_cancellation",
                        flow_run_id=flow_run.id,
                        operation_id=uuid5(
                            NAMESPACE_URL,
                            f"prefect-native-cancel-final/{flow_run.id}/{self.execution.generation}",
                        ),
                        expected_generation=self.execution.generation,
                        enrollment_operation_id=self.enrollment.operation_id,
                        death_witness={
                            "kind": "WAITPID",
                            "observed_at": datetime.now(UTC),
                            "host_id": host,
                            "boot_id": boot,
                            "child_process_id": handle.pid,
                            "process_start_ticks": ticks,
                            "exit_code": handle.returncode,
                        },
                    )
                    terminal = await self._request(
                        "read_flow_run", flow_run_id=flow_run.id
                    )
                    self._record_terminal(terminal)
                    return
                if handle.returncode == 0:
                    raise StreamProtocolError(
                        "Native child exited without a terminal FlowRun"
                    )
                host, boot, ticks = identity
                candidate = SimpleNamespace(
                    identity_kind="CHILD",
                    generation=self.execution.generation,
                    enrollment_operation_id=self.enrollment.operation_id,
                    host_id=host,
                    boot_id=boot,
                    child_process_id=handle.pid,
                    process_start_ticks=ticks,
                )
                replaced = await self.recover(
                    candidate,
                    {
                        "kind": "WAITPID",
                        "observed_at": datetime.now(UTC),
                        "host_id": host,
                        "boot_id": boot,
                        "child_process_id": handle.pid,
                        "process_start_ticks": ticks,
                        "exit_code": handle.returncode,
                    },
                )
                if not replaced:
                    return

    async def _start_attempt(
        self,
        flow_run: Any,
        handle: ProcessHandle | None,
        task_status: anyio.abc.TaskStatus[ProcessHandle],
    ) -> tuple[ProcessHandle, tuple[UUID, UUID, int]]:
        assert self.execution is not None
        read_fd, write_fd = os.pipe()
        process: anyio.abc.Process | None = None
        identity: tuple[UUID, UUID, int] | None = None
        registered = False
        handed_off = False
        try:
            environment = {
                key: value for key, value in self.env.items() if value is not None
            }
            environment.update(
                PREFECT__FLOW_RUN_ID=str(flow_run.id),
                PREFECT__STREAM_EXECUTION_FD=str(read_fd),
                PREFECT__ENABLE_CANCELLATION_AND_CRASHED_HOOKS="false",
            )
            if self.control_channel is not None:
                self.control_channel.unregister(flow_run.id)
                port, token = self.control_channel.register(flow_run.id)
                environment.update(
                    PREFECT__CONTROL_PORT=str(port), PREFECT__CONTROL_TOKEN=token
                )
            process = await anyio.open_process(
                [sys.executable, "-m", "prefect.flow_engine", self.entrypoint],
                env=sanitize_subprocess_env(environment),
                cwd=self.cwd,
                pass_fds=(read_fd,),
                start_new_session=True,
            )
            os.close(read_fd)
            read_fd = -1
            identity = linux_process_identity(process.pid)
            host, boot, ticks = identity
            await self._request(
                "register_native_stream_process",
                flow_run_id=flow_run.id,
                generation=self.execution.generation,
                operation_id=uuid5(
                    NAMESPACE_URL,
                    f"prefect-native-register/{flow_run.id}/{self.launcher_id}",
                ),
                enrollment_operation_id=self.enrollment.operation_id,
                launcher_instance_id=self.launcher_id,
                host_id=host,
                boot_id=boot,
                child_process_id=process.pid,
                process_start_ticks=ticks,
                launch_nonce_digest=hashlib.sha256(secrets.token_bytes(32)).hexdigest(),
            )
            registered = True
            await self._request(
                "update_flow_run",
                flow_run_id=flow_run.id,
                infrastructure_pid=str(process.pid),
            )
            scope = NativePidfdScope(process.pid)
            if handle is None:
                handle = ProcessHandle(process, termination_scope=scope)
            else:
                handle.replace_process(process, scope)
            payload = {
                "schema": "native-stream-execution-v1",
                "flow_run_id": str(flow_run.id),
                "generation": self.execution.generation,
                "holder_id": str(self.execution.holder_id),
                "api_url": self.execution.api_url,
                "holder_capability": self.execution.capability,
                "event_private_key": self.execution.event_private_key.hex(),
            }
            encoded = json.dumps(payload).encode()
            if len(encoded) > 4096:
                raise StreamProtocolError("Native child bootstrap exceeds its bound")
            if os.write(write_fd, encoded) != len(encoded):
                raise StreamProtocolError("Native child bootstrap was not delivered")
            os.close(write_fd)
            write_fd = -1
            if self.execution.generation == 1 or not self._started:
                task_status.started(handle)
                self._started = True
            handed_off = True

            async def forward(stream: Any, destination: Any) -> None:
                if stream is not None:
                    async for chunk in stream:
                        if self.stream_output:
                            destination.write(chunk.decode("utf-8", errors="replace"))
                            destination.flush()

            async with anyio.create_task_group() as group:
                group.start_soon(forward, process.stdout, sys.stdout)
                group.start_soon(forward, process.stderr, sys.stderr)
                group.start_soon(self._monitor, process)
                await process.wait()
                group.cancel_scope.cancel()
            return handle, identity
        finally:
            for descriptor in (read_fd, write_fd):
                if descriptor >= 0:
                    os.close(descriptor)
            with anyio.CancelScope(shield=True):
                if process is not None and process.returncode is None:
                    process.kill()
                    await process.wait()
                if process is not None:
                    await process.aclose()
                if not registered:
                    await self._request(
                        "retire_unstarted_native_stream_launch",
                        flow_run_id=flow_run.id,
                        operation_id=uuid5(
                            NAMESPACE_URL,
                            f"prefect-native-spawn-failed/{flow_run.id}/{self.launcher_id}",
                        ),
                        enrollment_operation_id=self.enrollment.operation_id,
                        error="Native worker could not start and register its child",
                    )
                elif not handed_off and process is not None:
                    assert identity is not None
                    host, boot, ticks = identity
                    await self.recover(
                        SimpleNamespace(
                            identity_kind="CHILD",
                            generation=self.execution.generation,
                            enrollment_operation_id=self.enrollment.operation_id,
                            host_id=host,
                            boot_id=boot,
                            child_process_id=process.pid,
                            process_start_ticks=ticks,
                        ),
                        {
                            "kind": "WAITPID",
                            "observed_at": datetime.now(UTC),
                            "host_id": host,
                            "boot_id": boot,
                            "child_process_id": process.pid,
                            "process_start_ticks": ticks,
                            "exit_code": process.returncode,
                        },
                        startup_failed=True,
                    )
            if not handed_off:
                if handle is not None:
                    handle.close()
                if self.control_channel is not None:
                    self.control_channel.unregister(flow_run.id)
