"""Worker-owned processes for assigned native streaming subtasks."""

from __future__ import annotations

import json
import os
import secrets
import select
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid5

import anyio
import httpx
from anyio.to_thread import run_sync

from prefect._internal.stream_subtask_entrypoint import subtask_environment
from prefect._internal.stream_subtask_protocol import (
    METADATA_POLL_INTERVAL_SECONDS,
    PREFLIGHT_MAX_BYTES,
    PREFLIGHT_READY,
    PREFLIGHT_TIMEOUT_SECONDS,
)
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.client.schemas import stream_subtasks as s
from prefect.client.schemas.objects import StateType
from prefect.runner._native_process import NativePidfdScope, verify_native_process_death
from prefect.runner._starter_native import (
    create_native_stream_credentials,
    linux_process_identity,
)
from prefect.streams import StreamProtocolError

if TYPE_CHECKING:
    from prefect.workers._stream_dag_subtasks import NativeDAGSubtaskSupervisor


class _SubtaskCleanupRequired(StreamProtocolError):
    """A failed start still owns an exact child whose stop is unconfirmed."""

    def __init__(self, child: _SubtaskProcess) -> None:
        self.child = child
        super().__init__("Physical startup retains an unconfirmed child process")


@dataclass
class _SubtaskProcess:
    """Pin one child and retain its private startup descriptors on the worker."""

    process: subprocess.Popen[bytes]
    scope: NativePidfdScope | None
    home: Path
    bootstrap_fd: int
    go_fd: int
    process_start_ticks: int
    released: bool = False

    @classmethod
    def start(
        cls,
        *,
        api_url: str,
        cwd: Path,
        stream_output: bool = True,
        preflight: dict[str, Any] | None = None,
        preflight_timeout: float = PREFLIGHT_TIMEOUT_SECONDS,
    ) -> _SubtaskProcess:
        home = Path(tempfile.mkdtemp(prefix="prefect-worker-subtask-"))
        descriptors: list[int] = []
        process: subprocess.Popen[bytes] | None = None
        child: _SubtaskProcess | None = None
        preflight_write = -1
        ready_read = -1
        try:
            bootstrap_read, bootstrap_write = os.pipe()
            descriptors.extend((bootstrap_read, bootstrap_write))
            go_read, go_write = os.pipe()
            descriptors.extend((go_read, go_write))
            child_descriptors = [bootstrap_read, go_read]
            arguments = [
                sys.executable,
                "-m",
                "prefect._internal.stream_subtask_entrypoint",
                str(bootstrap_read),
                str(go_read),
            ]
            if preflight is not None:
                preflight_read, preflight_write = os.pipe()
                ready_read, ready_write = os.pipe()
                descriptors.extend(
                    (preflight_read, preflight_write, ready_read, ready_write)
                )
                child_descriptors.extend((preflight_read, ready_write))
                arguments.extend((str(preflight_read), str(ready_write)))
            process = subprocess.Popen(
                arguments,
                cwd=cwd,
                env=subtask_environment(home, api_url),
                pass_fds=tuple(child_descriptors),
                stdin=subprocess.DEVNULL,
                stdout=None if stream_output else subprocess.DEVNULL,
                stderr=None if stream_output else subprocess.STDOUT,
            )
            # Ownership transfers as soon as Popen returns. Initialization may
            # fail before pidfd/identity/preflight, but the child must remain
            # reachable until an exact wait succeeds.
            child = cls(process, None, home, bootstrap_write, go_write, 0)
            descriptors.remove(bootstrap_write)
            descriptors.remove(go_write)
            child.scope = NativePidfdScope(process.pid)
            _, _, child.process_start_ticks = linux_process_identity(process.pid)
            for descriptor in child_descriptors:
                os.close(descriptor)
                descriptors.remove(descriptor)
            if preflight is not None:
                encoded = json.dumps(
                    preflight,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                ).encode()
                if not encoded or len(encoded) > PREFLIGHT_MAX_BYTES:
                    raise StreamProtocolError(
                        "Physical preflight exceeds its private transport"
                    )
                deadline = time.monotonic() + preflight_timeout
                os.set_blocking(preflight_write, False)
                offset = 0
                while offset < len(encoded):
                    remaining = max(0, deadline - time.monotonic())
                    if not select.select([], [preflight_write], [], remaining)[1]:
                        raise StreamProtocolError(
                            "Physical preflight delivery timed out"
                        )
                    offset += os.write(preflight_write, encoded[offset:])
                os.close(preflight_write)
                descriptors.remove(preflight_write)
                remaining = max(0, deadline - time.monotonic())
                if not select.select([ready_read], [], [], remaining)[0]:
                    raise StreamProtocolError("Physical preflight readiness timed out")
                if os.read(ready_read, 1) != PREFLIGHT_READY:
                    raise StreamProtocolError(
                        "Physical child refused its code preflight"
                    )
                os.close(ready_read)
                descriptors.remove(ready_read)
            return child
        except BaseException:
            if child is not None:
                try:
                    child.close()
                except BaseException as cleanup_error:
                    raise _SubtaskCleanupRequired(child) from cleanup_error
            else:
                if process is not None:
                    process.kill()
                    process.wait(timeout=10)
                shutil.rmtree(home, ignore_errors=True)
            raise
        finally:
            for descriptor in descriptors:
                os.close(descriptor)

    def write_bootstrap(self, payload: dict[str, Any]) -> None:
        """Deliver a bounded child grant; no credential enters argv or env."""
        if self.bootstrap_fd < 0:
            raise StreamProtocolError("Physical bootstrap was already delivered")
        if (
            payload.get("child_process_id") != self.process.pid
            or payload.get("process_start_ticks") != self.process_start_ticks
        ):
            raise StreamProtocolError("Physical bootstrap has another process identity")
        encoded = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
        if not encoded or len(encoded) > 16 * 1024:
            raise StreamProtocolError(
                "Physical bootstrap exceeds its private transport"
            )
        with os.fdopen(self.bootstrap_fd, "wb") as pipe:
            self.bootstrap_fd = -1
            pipe.write(encoded)

    def release(self) -> None:
        """Release only after the trusted worker observes durable ACTIVE."""
        if self.released:
            return
        if self.bootstrap_fd >= 0 or self.go_fd < 0:
            raise StreamProtocolError("Physical startup has no delivered bootstrap")
        try:
            if os.write(self.go_fd, b"G") != 1:
                raise StreamProtocolError("Physical startup GO was not delivered")
            self.released = True
        finally:
            os.close(self.go_fd)
            self.go_fd = -1

    def close(self) -> None:
        """Kill/reap the pinned child before discarding its local resources."""
        if self.scope is not None:
            self.scope.hard_kill(self.process.pid)
        elif self.process.poll() is None:
            # Popen still owns this unreaped child; its PID cannot have been
            # reused. This only covers pidfd-open failure during startup.
            self.process.kill()
        # An unknown wait leaves *all* resources retained. No caller may issue
        # a stop receipt from the elapsed cleanup deadline.
        self.process.wait(timeout=10)
        for name in ("bootstrap_fd", "go_fd"):
            descriptor = getattr(self, name)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, name, -1)
        if self.scope is not None:
            self.scope.close()
        shutil.rmtree(self.home, ignore_errors=True)


@dataclass
class _DeathOperation:
    operation_id: UUID
    challenge: str = field(repr=False)
    authorization: Any = None
    witness: dict[str, Any] | None = None
    retiring: bool | None = None
    preflight_failed: bool = False
    credentials: tuple[UUID, str, bytes, str] | None = field(default=None, repr=False)
    child: _SubtaskProcess | None = None
    confirmation: dict[str, Any] | None = field(default=None, repr=False)
    retirement: dict[str, Any] | None = field(default=None, repr=False)


@dataclass
class _ManagedSubtask:
    item: s.StreamWorkerAssignment
    credentials: tuple[UUID, str, bytes, str] = field(repr=False)
    claim: dict[str, Any] = field(repr=False)
    grant: s.StreamSubtaskGrant | None = None
    child: _SubtaskProcess | None = None
    death: _DeathOperation | None = None
    stop: dict[str, Any] | None = None
    last_renewed: float = 0
    last_terminal_read: float = 0
    terminal_seen_at: float | None = None
    fenced: bool = False
    startup_failed: bool = False


class NativeStreamSubtaskSupervisor:
    """Run only the server-assigned slots of one registered ProcessWorker.

    Worker polling uses its ordinary trusted API client. Child credentials are
    generated locally, retained across unknown replies and delivered only after
    exact process registration. The Flow coordinator owns global activation.
    """

    terminal_exit_grace_seconds = 5.0

    def __init__(self, worker: Any) -> None:
        if worker.backend_id is None:
            raise StreamProtocolError("Assigned subtasks require a registered worker")
        self.worker = worker
        self.worker_id: UUID = worker.backend_id
        host, boot, ticks = linux_process_identity(os.getpid())
        self.principal: dict[str, Any] = s.StreamWorkerPrincipal(
            work_pool_id=worker.work_pool.id,
            host_id=host,
            boot_id=boot,
            launcher_process_id=os.getpid(),
            launcher_start_ticks=ticks,
        ).model_dump(mode="json")
        self.api_url = str(worker.client.api_url).rstrip("/")
        self.runs: dict[UUID, _ManagedSubtask] = {}
        self._scope: anyio.CancelScope | None = None
        self._stopping = False
        self._warnings: set[tuple[UUID | None, str]] = set()
        self._dag_supervisor: NativeDAGSubtaskSupervisor | None = None

    def enable_dag(self) -> None:
        """Compose the optional DAG protocol under this single process owner."""
        if self._dag_supervisor is None:
            from prefect.workers._stream_dag_subtasks import NativeDAGSubtaskSupervisor

            self._dag_supervisor = NativeDAGSubtaskSupervisor(
                self.worker,
                principal=self.principal,
                other_live_children=self._local_live_children,
            )

    def stop(self) -> None:
        self._stopping = True
        if self._scope is not None:
            self._scope.cancel()

    def _warn(self, subtask_id: UUID | None, reason: str) -> None:
        key = (subtask_id, reason)
        if key not in self._warnings:
            self._warnings.add(key)
            self.worker._logger.warning(
                "Assigned stream subtask %s did not advance: %s", subtask_id, reason
            )

    async def run(self) -> None:
        try:
            with anyio.CancelScope() as scope:
                self._scope = scope
                while not self._stopping:
                    try:
                        items = await self.worker.client.poll_worker_stream_subtasks(
                            worker_id=self.worker_id,
                            principal=self.principal,
                            active_subtask_ids=list(self.runs),
                            limit=64,
                        )
                        if len(items) > 128:
                            raise StreamProtocolError(
                                "Worker assignment poll is oversized"
                            )
                        for item in items:
                            await self._drive_safely(item)
                    except httpx.TransportError:
                        self._warn(None, "API_TRANSPORT_UNAVAILABLE")
                    except StreamRequestError as exc:
                        self._warn(None, exc.code)
                        if exc.status_code == 404 and not self.runs:
                            # A native-marked pool can serve an older bounded
                            # server. Its existing protocol remains usable;
                            # the new profile will refuse admission there.
                            return
                    if self._dag_supervisor is not None:
                        await self._dag_supervisor.poll()
                    await anyio.sleep(METADATA_POLL_INTERVAL_SECONDS)
        finally:
            # Shutdown never invents a server stop receipt. If the worker exits
            # during live execution, its slots remain fenced until a separately
            # supported verified recovery protocol resolves them.
            with anyio.CancelScope(shield=True):
                try:
                    if self._dag_supervisor is not None:
                        await self._dag_supervisor.close()
                finally:
                    for run in self.runs.values():
                        children = [run.child, run.death.child if run.death else None]
                        for child in children:
                            if child is not None:
                                await run_sync(child.close)
            self._scope = None

    async def _drive_safely(self, item: s.StreamWorkerAssignment) -> None:
        subtask_id = item.assignment.id
        try:
            await self._drive(item)
        except _SubtaskCleanupRequired as exc:
            run = self.runs.get(subtask_id)
            if run is None:
                raise
            if run.death is None:
                run.child = exc.child
                run.startup_failed = True
            else:
                run.death.child = exc.child
                run.death.preflight_failed = True
                run.death.retiring = True
            self._warn(subtask_id, "PHYSICAL_STOP_UNCONFIRMED")
            if item.phase == "WAITING":
                await self._abort_startup(item)
        except subprocess.TimeoutExpired:
            self._warn(subtask_id, "PHYSICAL_STOP_UNCONFIRMED")
        except httpx.TransportError:
            # Every mutating request is retained on its run/death/stop record.
            # The next poll repeats exactly that request, including credentials.
            self._warn(subtask_id, "API_REPLY_UNKNOWN")
        except StreamRequestError as exc:
            self._warn(subtask_id, exc.code)
            if exc.code in {
                "SUBTASK_FENCED",
                "SUBTASK_SCOPE_DENIED",
                "IDENTITY_CONFLICT",
            }:
                run = self.runs.get(subtask_id)
                if run is not None:
                    run.fenced = True
                    await self._dispose(run)
        except (OSError, StreamProtocolError) as exc:
            self._warn(subtask_id, type(exc).__name__)
            run = self.runs.get(subtask_id)
            if run is not None and run.child is not None:
                await run_sync(run.child.close)
            if item.phase == "WAITING":
                if run is not None:
                    run.startup_failed = True
                await self._abort_startup(item)

    def _check_assignment(self, item: s.StreamWorkerAssignment) -> None:
        assignment = item.assignment
        if (
            assignment.assigned_worker_id != self.worker_id
            or assignment.manifest.flow_run_id != item.flow_run_id
            or not Path(item.working_directory).is_absolute()
        ):
            raise StreamProtocolError("The server assignment is outside this worker")
        grant = item.current_grant
        if grant is not None and (
            grant.subtask_id != assignment.id
            or grant.graph_revision_id != assignment.graph_revision_id
            or grant.launcher_binding_kind != "ASSIGNED_WORKER"
            or grant.worker_id != self.worker_id
            or str(grant.work_pool_id) != self.principal["work_pool_id"]
            or str(grant.host_id) != self.principal["host_id"]
            or str(grant.boot_id) != self.principal["boot_id"]
            or grant.launcher_process_id != self.principal["launcher_process_id"]
            or grant.launcher_start_ticks != self.principal["launcher_start_ticks"]
        ):
            raise StreamProtocolError("The physical grant has another launcher")

    def _new_run(self, item: s.StreamWorkerAssignment) -> _ManagedSubtask:
        credentials = create_native_stream_credentials()
        holder, capability, _, public = credentials
        claim = {
            "operation_id": uuid5(item.assignment.id, "assigned-worker-claim-v1"),
            "graph_digest": item.graph_digest,
            "holder_id": holder,
            "capability": capability,
            "event_public_key": public,
            **self.principal,
        }
        return _ManagedSubtask(item, credentials, claim)

    def _local_live_children(self) -> int:
        return sum(
            child is not None and child.process.poll() is None
            for run in self.runs.values()
            for child in (run.child, run.death.child if run.death else None)
        )

    def _live_children(self) -> int:
        return self._local_live_children() + (
            self._dag_supervisor.live_children()
            if self._dag_supervisor is not None
            else 0
        )

    async def _drive(self, item: s.StreamWorkerAssignment) -> None:
        self._check_assignment(item)
        subtask_id = item.assignment.id
        run = self.runs.get(subtask_id)
        if item.phase == "RELEASED":
            if run is not None:
                await self._dispose(run)
                self.runs.pop(subtask_id, None)
            return
        if run is None:
            if item.current_grant is not None:
                if not item.assignment.physical_slot_released:
                    self._warn(subtask_id, "UNKNOWN_LOCAL_SUPERVISOR")
                return
            if item.phase == "ABORTING":
                return
            if len(self.runs) >= 64:
                raise StreamProtocolError("Worker local assignment registry is full")
            run = self._new_run(item)
            self.runs[subtask_id] = run
        run.item = item
        if item.phase == "ABORTING":
            await self._stop_startup(run)
            return
        if run.fenced:
            return
        if run.startup_failed:
            await self._abort_startup(item)
            return
        if run.death is not None:
            await self._resolve_death(run)
            return
        if run.grant is None:
            run.grant = await self.worker.client.claim_worker_stream_subtask(
                worker_id=self.worker_id, subtask_id=subtask_id, **run.claim
            )
        assert run.grant is not None
        if run.child is None:
            if self._live_children() >= 2:
                return
            run.child = await run_sync(
                partial(
                    _SubtaskProcess.start,
                    api_url=self.api_url,
                    cwd=Path(item.working_directory),
                    preflight=self._preflight(item),
                )
            )
        child = run.child
        if child.process.poll() is not None:
            if item.phase == "WAITING":
                await self._abort_startup(item)
            else:
                await self._resolve_death(run)
            return
        if run.grant.state == "CLAIMED":
            run.grant = await self.worker.client.register_worker_stream_subtask_process(
                worker_id=self.worker_id,
                subtask_id=subtask_id,
                principal=self.principal,
                registration={
                    "operation_id": uuid5(run.grant.id, "register"),
                    "grant_id": run.grant.id,
                    "child_process_id": child.process.pid,
                    "process_start_ticks": child.process_start_ticks,
                },
            )
        assert run.grant is not None
        if child.bootstrap_fd >= 0:
            await run_sync(
                child.write_bootstrap,
                self._bootstrap(run, run.grant, child, run.credentials),
            )
        if item.phase == "ACTIVE" and not child.released:
            child.release()
        now = time.monotonic()
        if now - run.last_renewed >= 15:
            await self.worker.client.renew_worker_stream_subtask(
                worker_id=self.worker_id,
                subtask_id=subtask_id,
                principal=self.principal,
                grant_id=run.grant.id,
            )
            run.last_renewed = now
        if child.released and now - run.last_terminal_read >= 1:
            run.last_terminal_read = now
            if item.task_state in {
                StateType.COMPLETED,
                StateType.FAILED,
                StateType.CRASHED,
                StateType.CANCELLED,
            }:
                if item.output_state != "OPEN":
                    if run.terminal_seen_at is None:
                        run.terminal_seen_at = now
                    elif now - run.terminal_seen_at >= self.terminal_exit_grace_seconds:
                        # The deadline authorizes a signal, never a handoff. The
                        # exact waitpid and server death receipt still follow.
                        await run_sync(child.close)
                        await self._resolve_death(run)

    def _bootstrap(
        self,
        run: _ManagedSubtask,
        grant: s.StreamSubtaskGrant,
        child: _SubtaskProcess,
        credentials: tuple[UUID, str, bytes, str],
    ) -> dict[str, Any]:
        assignment = run.item.assignment
        holder, capability, private, _ = credentials
        self._check_assignment(run.item.model_copy(update={"current_grant": grant}))
        if (
            grant.holder_id != holder
            or grant.subtask_id != assignment.id
            or grant.child_process_id != child.process.pid
            or grant.process_start_ticks != child.process_start_ticks
            or grant.state != "REGISTERED"
        ):
            raise StreamProtocolError("Registered grant differs from the blocked child")
        return {
            "flow_run_id": str(run.item.flow_run_id),
            "graph_revision_id": str(assignment.graph_revision_id),
            "assignment_epoch": assignment.assignment_epoch,
            "subtask_id": str(assignment.id),
            "grant_id": str(grant.id),
            "physical_epoch": grant.physical_epoch,
            "holder_id": str(holder),
            "scope_digest": grant.scope_digest,
            "manifest_digest": assignment.manifest.manifest_digest,
            "task_run_id": str(assignment.manifest.task_run_id),
            "input_stream_id": str(assignment.input_stream_id)
            if assignment.input_stream_id
            else None,
            "output_stream_id": str(assignment.manifest.stream_id),
            "child_process_id": child.process.pid,
            "process_start_ticks": child.process_start_ticks,
            "api_url": self.api_url,
            "capability": capability,
            "event_private_key": private.hex(),
        }

    @staticmethod
    def _preflight(item: s.StreamWorkerAssignment) -> dict[str, Any]:
        return {
            "configuration": item.task_configuration.model_dump(mode="json"),
            "role": item.assignment.manifest.role,
        }

    async def _abort_startup(self, item: s.StreamWorkerAssignment) -> None:
        try:
            await self.worker.client.abort_worker_stream_graph_startup(
                worker_id=self.worker_id,
                graph_revision_id=item.assignment.graph_revision_id,
                principal=self.principal,
            )
        except StreamRequestError as exc:
            if exc.code != "ACTIVATION_ALREADY_COMMITTED":
                raise

    async def _stop_startup(self, run: _ManagedSubtask) -> None:
        item = run.item
        grant = item.current_grant or run.grant
        if grant is None:
            return
        if item.abort_operation_id is None:
            raise StreamProtocolError("ABORTING assignment has no abort operation")
        if run.stop is None:
            if run.child is not None:
                await run_sync(run.child.close)
            witness = None
            if grant.state == "REGISTERED":
                witness = verify_native_process_death(grant, terminate=False)
            run.stop = {
                "operation_id": uuid5(grant.id, "assigned-worker-startup-stop-v1"),
                "abort_operation_id": item.abort_operation_id,
                "grant_id": grant.id,
                "expected_physical_epoch": grant.physical_epoch,
                "death_witness": witness,
                "unstarted": witness is None,
            }
        await self.worker.client.acknowledge_worker_stream_subtask_startup_stop(
            worker_id=self.worker_id,
            subtask_id=item.assignment.id,
            principal=self.principal,
            stop=run.stop,
        )
        await self._dispose(run)
        self.runs.pop(item.assignment.id, None)

    async def _resolve_death(self, run: _ManagedSubtask) -> None:
        assert run.grant is not None and run.child is not None
        grant = run.grant
        if run.death is None:
            run.death = _DeathOperation(
                uuid5(grant.id, "assigned-worker-death-v1"), secrets.token_hex(32)
            )
        operation = run.death
        common = {
            "worker_id": self.worker_id,
            "subtask_id": run.item.assignment.id,
            "principal": self.principal,
        }
        if operation.authorization is None:
            operation.authorization = (
                await self.worker.client.authorize_worker_stream_subtask_replacement(
                    **common,
                    replacement={
                        "operation_id": operation.operation_id,
                        "expected_grant_id": grant.id,
                        "expected_physical_epoch": grant.physical_epoch,
                        "host_id": self.principal["host_id"],
                        "challenge_capability": operation.challenge,
                    },
                )
            )
        if operation.witness is None:
            operation.witness = verify_native_process_death(
                operation.authorization, terminate=False
            )
        if operation.retiring is None:
            operation.retiring = (
                run.item.output_state != "OPEN"
                or run.item.task_run_count
                >= (run.item.task_configuration.task_policy.retries or 0) + 1
                or grant.physical_epoch >= 64
                or run.item.task_state in {StateType.FAILED, StateType.CRASHED}
                and run.item.task_retriable is False
            )
        if operation.retiring:
            if operation.preflight_failed and operation.child is not None:
                await run_sync(operation.child.close)
                operation.child = None
            if operation.retirement is None:
                operation.retirement = {
                    "operation_id": operation.operation_id,
                    "challenge_capability": operation.challenge,
                    "death_witness": operation.witness,
                    "error": "Verified physical process reached its terminal invocation",
                    "reason": "REPLACEMENT_PREFLIGHT_FAILED"
                    if operation.preflight_failed
                    else "TERMINAL_RECONCILIATION",
                }
            await self.worker.client.retire_verified_worker_stream_subtask(
                **common, retirement=operation.retirement
            )
            await self._dispose(run)
            self.runs.pop(run.item.assignment.id, None)
            return
        if operation.credentials is None:
            operation.credentials = create_native_stream_credentials()
        if operation.child is None:
            if self._live_children() >= 2:
                return
            try:
                operation.child = await run_sync(
                    partial(
                        _SubtaskProcess.start,
                        api_url=self.api_url,
                        cwd=Path(run.item.working_directory),
                        preflight=self._preflight(run.item),
                    )
                )
            except _SubtaskCleanupRequired:
                raise
            except (OSError, StreamProtocolError):
                # The start helper kills/reaps the unregistered candidate.
                # One authorization permits exactly one preflight; it must not
                # reset the setup budget by repeatedly spawning new children.
                operation.preflight_failed = True
                operation.retiring = True
                await self._resolve_death(run)
                return
        replacement = operation.child
        if operation.confirmation is None:
            holder, capability, _, public = operation.credentials
            operation.confirmation = {
                "operation_id": operation.operation_id,
                "challenge_capability": operation.challenge,
                "death_witness": operation.witness,
                "new_holder_id": holder,
                "new_capability": capability,
                "new_event_public_key": public,
                "new_child_process_id": replacement.process.pid,
                "new_process_start_ticks": replacement.process_start_ticks,
                "host_id": self.principal["host_id"],
                "boot_id": self.principal["boot_id"],
            }
        try:
            new_grant = (
                await self.worker.client.confirm_worker_stream_subtask_replacement(
                    **common, replacement=operation.confirmation
                )
            )
        except StreamRequestError as exc:
            if exc.code not in {"TERMINAL_RECONCILIATION_REQUIRED", "RETRY_EXHAUSTED"}:
                raise
            await run_sync(replacement.close)
            operation.child = None
            operation.retiring = True
            await self._resolve_death(run)
            return
        old_child = run.child
        run.grant = new_grant
        run.credentials = operation.credentials
        run.child = replacement
        run.death = None
        run.last_renewed = 0
        run.terminal_seen_at = None
        await run_sync(old_child.close)
        await run_sync(
            replacement.write_bootstrap,
            self._bootstrap(run, new_grant, replacement, run.credentials),
        )
        replacement.release()

    async def _dispose(self, run: _ManagedSubtask) -> None:
        for child in (run.child, run.death.child if run.death else None):
            if child is not None:
                await run_sync(child.close)
