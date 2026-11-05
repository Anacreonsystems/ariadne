"""Worker-owned DAG launches; durable fences precede every process birth."""

from __future__ import annotations

import os
import secrets
import select
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid5

import httpx
from anyio.to_thread import run_sync

from prefect._internal.stream_dag_subtask_protocol import (
    DAG_BOOTSTRAP_MAX_BYTES,
    DAG_BOOTSTRAP_READY,
    DAG_CODE_READY,
    DAG_GO,
    DAG_PREFLIGHT_MAX_BYTES,
    DAG_RECORD_ALLOWED,
    DAG_STARTUP_TIMEOUT_SECONDS,
    dag_launch_close_operation_id,
    dag_launch_prepare_operation_id,
    dag_launch_record_operation_id,
    dag_subtask_stop_operation_id,
)
from prefect._internal.stream_event_auth import dag_worker_generation_digest
from prefect._internal.stream_route_plan import canonical_route_json
from prefect._internal.stream_subtask_entrypoint import subtask_environment
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.client.schemas import stream_dag_executions as d
from prefect.client.schemas import stream_subtasks as s
from prefect.client.schemas.objects import StateType
from prefect.runner._native_process import NativePidfdScope, verify_native_process_death
from prefect.runner._starter_native import (
    create_native_stream_credentials,
    linux_process_identity,
)
from prefect.settings import PREFECT_HOME
from prefect.streams import StreamProtocolError
from prefect.workers._stream_dag_launches import (
    DAGLaunchJournal,
    DAGLaunchJournalError,
)


class DAGSubtaskStartUnknown(StreamProtocolError):
    """A process was born but its next durable transition remains unknown."""

    def __init__(self, child: DAGSubtaskProcess) -> None:
        self.child = child
        super().__init__("DAG launch retains its exact process until verified cleanup")


@dataclass
class DAGSubtaskProcess:
    """One exact process with separate record, code, bootstrap and GO gates."""

    process: subprocess.Popen[bytes]
    home: Path
    record_fd: int
    preflight_fd: int
    ready_fd: int
    bootstrap_fd: int
    go_fd: int
    process_start_ticks: int = 0
    scope: NativePidfdScope | None = field(default=None, repr=False)
    code_ready: bool = False
    bootstrap_ready: bool = False
    released: bool = False
    bootstrap_sent_at: float | None = None

    @classmethod
    def start(
        cls,
        *,
        journal: DAGLaunchJournal,
        operation_id: UUID,
        api_url: str,
        cwd: Path,
        stream_output: bool = True,
    ) -> DAGSubtaskProcess:
        """Reserve one birth and persist its PID before any user import gate."""
        descriptors: list[int] = []
        home = Path(tempfile.mkdtemp(prefix="prefect-worker-dag-subtask-"))
        child: DAGSubtaskProcess | None = None
        try:
            record_read, record_write = os.pipe()
            preflight_read, preflight_write = os.pipe()
            ready_read, ready_write = os.pipe()
            bootstrap_read, bootstrap_write = os.pipe()
            go_read, go_write = os.pipe()
            descriptors.extend(
                (
                    record_read,
                    record_write,
                    preflight_read,
                    preflight_write,
                    ready_read,
                    ready_write,
                    bootstrap_read,
                    bootstrap_write,
                    go_read,
                    go_write,
                )
            )
            inherited = (
                record_read,
                preflight_read,
                ready_write,
                bootstrap_read,
                go_read,
            )
            with journal.spawn_stop_mutex():
                journal.reserve_birth(operation_id)
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "prefect._internal.stream_dag_subtask_entrypoint",
                        *(str(fd) for fd in inherited),
                    ],
                    cwd=cwd,
                    env=subtask_environment(home, api_url),
                    pass_fds=inherited,
                    stdin=subprocess.DEVNULL,
                    stdout=None if stream_output else subprocess.DEVNULL,
                    stderr=None if stream_output else subprocess.STDOUT,
                )
                # Retain ownership immediately. A PID/identity/fsync exception
                # may never erase a child or justify CLOSED_UNSTARTED.
                child = cls(
                    process,
                    home,
                    record_write,
                    preflight_write,
                    ready_read,
                    bootstrap_write,
                    go_write,
                )
                for descriptor in (
                    record_write,
                    preflight_write,
                    ready_read,
                    bootstrap_write,
                    go_write,
                ):
                    descriptors.remove(descriptor)
                for descriptor in inherited:
                    os.close(descriptor)
                    descriptors.remove(descriptor)
                child.scope = NativePidfdScope(process.pid)
                _, _, child.process_start_ticks = linux_process_identity(process.pid)
                journal.record_process(
                    operation_id, process.pid, child.process_start_ticks
                )
            return child
        except BaseException:
            if child is not None:
                # The caller retains this handle even when cleanup succeeds:
                # local durability may still be unknown and must hold capacity.
                try:
                    child.close()
                except BaseException as error:
                    raise DAGSubtaskStartUnknown(child) from error
                raise DAGSubtaskStartUnknown(child)
            shutil.rmtree(home, ignore_errors=True)
            raise
        finally:
            for descriptor in descriptors:
                os.close(descriptor)

    def _write(self, name: str, value: bytes, *, timeout: float) -> None:
        descriptor: int = getattr(self, name)
        if descriptor < 0:
            raise StreamProtocolError("DAG private message was already delivered")
        deadline = time.monotonic() + timeout
        os.set_blocking(descriptor, False)
        offset = 0
        try:
            while offset < len(value):
                remaining = max(0.0, deadline - time.monotonic())
                if not select.select([], [descriptor], [], remaining)[1]:
                    raise StreamProtocolError("DAG private message delivery timed out")
                offset += os.write(descriptor, value[offset:])
        finally:
            os.close(descriptor)
            setattr(self, name, -1)

    def allow_preflight(
        self,
        configuration: dict[str, Any],
        *,
        timeout: float = DAG_STARTUP_TIMEOUT_SECONDS,
    ) -> None:
        """Call only after exact durable RECORD returned PREFLIGHT_ALLOWED."""
        if self.code_ready:
            return
        encoded = canonical_route_json(configuration)
        if not encoded or len(encoded) > DAG_PREFLIGHT_MAX_BYTES:
            raise StreamProtocolError("DAG code preflight is oversized")
        self._write("record_fd", DAG_RECORD_ALLOWED, timeout=timeout)
        self._write("preflight_fd", encoded, timeout=timeout)
        if not select.select([self.ready_fd], [], [], timeout)[0]:
            raise StreamProtocolError("DAG code preflight readiness timed out")
        if os.read(self.ready_fd, 1) != DAG_CODE_READY:
            raise StreamProtocolError("DAG child refused its code preflight")
        self.code_ready = True

    def write_bootstrap(self, payload: dict[str, Any]) -> None:
        """Deliver an exact registered grant only to the retained code-ready child."""
        if (
            not self.code_ready
            or payload.get("child_process_id") != self.process.pid
            or payload.get("process_start_ticks") != self.process_start_ticks
        ):
            raise StreamProtocolError("DAG bootstrap has another process identity")
        encoded = canonical_route_json(payload)
        if not encoded or len(encoded) > DAG_BOOTSTRAP_MAX_BYTES:
            raise StreamProtocolError("DAG private bootstrap is oversized")
        self._write("bootstrap_fd", encoded, timeout=DAG_STARTUP_TIMEOUT_SECONDS)
        self.bootstrap_sent_at = time.monotonic()

    def observe_bootstrap_ready(self) -> bool:
        """Observe the child's signed-readiness ACK without opening the GO gate."""
        if self.bootstrap_ready:
            return True
        if self.bootstrap_sent_at is None:
            return False
        if select.select([self.ready_fd], [], [], 0)[0]:
            if os.read(self.ready_fd, 1) != DAG_BOOTSTRAP_READY:
                raise StreamProtocolError("DAG child refused its scoped bootstrap")
            self.bootstrap_ready = True
            os.close(self.ready_fd)
            self.ready_fd = -1
            return True
        if time.monotonic() - self.bootstrap_sent_at >= DAG_STARTUP_TIMEOUT_SECONDS:
            raise StreamProtocolError("DAG bootstrap acknowledgement remains unknown")
        return False

    def release(self) -> None:
        """Call only after the worker observes ACTIVE and own BOOTSTRAP_READY."""
        if self.released:
            return
        if not self.bootstrap_ready or self.bootstrap_fd >= 0:
            raise StreamProtocolError("DAG startup has no verified private bootstrap")
        self._write("go_fd", DAG_GO, timeout=DAG_STARTUP_TIMEOUT_SECONDS)
        self.released = True

    def close(self) -> None:
        """Retain every resource until exact pinned-child wait has succeeded."""
        if self.scope is not None:
            self.scope.hard_kill(self.process.pid)
        elif self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        for name in (
            "record_fd",
            "preflight_fd",
            "ready_fd",
            "bootstrap_fd",
            "go_fd",
        ):
            descriptor: int = getattr(self, name)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, name, -1)
        if self.scope is not None:
            self.scope.close()
        shutil.rmtree(self.home, ignore_errors=True)


@dataclass
class _DAGLaunch:
    operation_id: UUID
    replacement_operation_id: UUID | None = None
    attempt: d.StreamDAGLaunchAttempt | None = None
    child: DAGSubtaskProcess | None = None
    credentials: tuple[UUID, str, bytes, str] | None = field(default=None, repr=False)
    stopped: bool = False


@dataclass
class _DAGDeath:
    operation_id: UUID
    challenge: str = field(repr=False)
    authorization: Any = None
    witness: dict[str, Any] | None = None
    candidate: _DAGLaunch | None = None
    confirmation: dict[str, Any] | None = field(default=None, repr=False)
    retirement: dict[str, Any] | None = field(default=None, repr=False)
    preflight_failed: bool = False


@dataclass
class _ManagedDAG:
    item: d.StreamDAGWorkerAssignment
    journal: DAGLaunchJournal
    launch: _DAGLaunch | None = None
    grant: s.StreamSubtaskGrant | None = None
    claim: dict[str, Any] | None = field(default=None, repr=False)
    registration: dict[str, Any] | None = None
    close_proofs: dict[UUID, dict[str, Any]] = field(
        default_factory=dict[UUID, dict[str, Any]]
    )
    closed_attempts: set[UUID] = field(default_factory=set[UUID])
    death: _DAGDeath | None = None
    recovering: bool = False
    startup_failed: bool = False
    fenced: bool = False
    last_renewed: float = 0
    terminal_seen_at: float | None = None


class NativeDAGSubtaskSupervisor:
    """A companion polled sequentially by the existing worker child owner.

    Its caller supplies the other profile's live-child count. Both profiles
    therefore check the same two-child budget in one serialized polling loop.
    Durable server reservations remain authoritative for admission and release.
    """

    terminal_exit_grace_seconds = 5.0

    def __init__(
        self,
        worker: Any,
        *,
        principal: dict[str, Any],
        other_live_children: Callable[[], int],
        journal_directory: Path | None = None,
    ) -> None:
        if worker.backend_id is None:
            raise StreamProtocolError("DAG subtasks require a registered worker")
        self.worker = worker
        self.worker_id: UUID = worker.backend_id
        parsed = s.StreamWorkerPrincipal.model_validate(principal)
        self.principal = parsed.model_dump(mode="json")
        self.generation = dag_worker_generation_digest(
            worker_id=self.worker_id,
            work_pool_id=parsed.work_pool_id,
            host_id=parsed.host_id,
            boot_id=parsed.boot_id,
            launcher_process_id=parsed.launcher_process_id,
            launcher_start_ticks=parsed.launcher_start_ticks,
        )
        self.api_url = str(worker.client.api_url).rstrip("/")
        self.other_live_children = other_live_children
        self.directory = (
            (
                journal_directory
                if journal_directory is not None
                else PREFECT_HOME.value() / "stream-dag-launches"
            )
            / str(self.worker_id)
            / self.generation
        )
        self.runs: dict[UUID, _ManagedDAG] = {}
        self.disabled = False
        self.warnings: set[tuple[UUID | None, str]] = set()

    def _warn(self, identity: UUID | None, reason: str) -> None:
        if (identity, reason) not in self.warnings:
            self.warnings.add((identity, reason))
            self.worker._logger.warning(
                "Assigned DAG subtask %s did not advance: %s", identity, reason
            )

    def live_children(self) -> int:
        return sum(
            child.process.poll() is None
            for run in self.runs.values()
            for child in self._children(run)
        )

    def _has_capacity(self) -> bool:
        return self.live_children() + self.other_live_children() < 2

    @staticmethod
    def _children(run: _ManagedDAG) -> tuple[DAGSubtaskProcess, ...]:
        launches = (run.launch, run.death.candidate if run.death else None)
        return tuple(
            launch.child
            for launch in launches
            if launch is not None and launch.child is not None
        )

    async def close(self) -> None:
        """Local shutdown proves no server release and retains durable journals."""
        for run in self.runs.values():
            for child in self._children(run):
                await run_sync(child.close)

    async def poll(self) -> None:
        if self.disabled:
            return
        try:
            items = await self.worker.client.poll_worker_stream_dag_subtasks(
                worker_id=self.worker_id,
                principal=self.principal,
                active_subtask_ids=list(self.runs),
                limit=64,
            )
            if len(items) > 128 or len({item.assignment.id for item in items}) != len(
                items
            ):
                raise StreamProtocolError(
                    "DAG worker assignment poll is oversized or aliased"
                )
            for item in items:
                await self._drive_safely(item)
        except httpx.TransportError:
            self._warn(None, "API_TRANSPORT_UNAVAILABLE")
        except StreamRequestError as error:
            if (
                error.status_code == 404
                and error.code == "UNSUPPORTED_BACKEND"
                and not self.runs
            ):
                # Optional DAG capability absent on a previously accepted P2
                # server. Authentication/ownership failures never fall back.
                self.disabled = True
                return
            self._warn(None, error.code)

    def _check_item(self, item: d.StreamDAGWorkerAssignment) -> None:
        if item.assignment.assigned_worker_id != self.worker_id or any(
            row.worker_generation_digest != self.generation
            for row in item.launch_attempts
        ):
            raise StreamProtocolError(
                "DAG assignment has another exact worker generation"
            )
        grant = item.current_grant
        if grant is not None:
            self._check_grant(item, grant)

    def _check_grant(
        self, item: d.StreamDAGWorkerAssignment, grant: s.StreamSubtaskGrant
    ) -> None:
        if (
            grant.subtask_id != item.assignment.id
            or grant.graph_revision_id != item.assignment.graph_revision_id
            or grant.assignment_epoch != item.assignment.assignment_epoch
            or grant.launcher_binding_kind != "ASSIGNED_WORKER"
            or grant.worker_id != self.worker_id
            or str(grant.work_pool_id) != self.principal["work_pool_id"]
            or str(grant.host_id) != self.principal["host_id"]
            or str(grant.boot_id) != self.principal["boot_id"]
            or grant.launcher_process_id != self.principal["launcher_process_id"]
            or grant.launcher_start_ticks != self.principal["launcher_start_ticks"]
        ):
            raise StreamProtocolError("DAG grant has another exact launcher")

    def _new_run(self, item: d.StreamDAGWorkerAssignment) -> _ManagedDAG:
        journal = DAGLaunchJournal(
            self.directory
            / str(item.assignment.graph_revision_id)
            / str(item.assignment.id),
            worker_id=self.worker_id,
            worker_generation_digest=self.generation,
            graph_revision_id=item.assignment.graph_revision_id,
            subtask_id=item.assignment.id,
        )
        expected = {
            dag_launch_prepare_operation_id(
                item.assignment.id, row.start_operation_id, row.replacement_operation_id
            ): (row.id, row.attempt_index)
            for row in item.launch_attempts
        }
        journal.reconcile(expected)
        recovering = bool(journal.records())
        for operation, (attempt_id, index) in expected.items():
            journal.bind(operation, attempt_id, index)
        return _ManagedDAG(item, journal, recovering=recovering)

    async def _drive_safely(self, item: d.StreamDAGWorkerAssignment) -> None:
        identity = item.assignment.id
        try:
            await self._drive(item)
        except DAGSubtaskStartUnknown as error:
            run = self.runs.get(identity)
            if run is None:
                raise
            launch = run.death.candidate if run.death else run.launch
            if launch is None:
                raise
            launch.child = error.child
            run.startup_failed = True
            if run.death is not None:
                run.death.preflight_failed = True
            self._warn(identity, "LOCAL_LAUNCH_DURABILITY_UNKNOWN")
            if item.phase == "STARTING":
                await self._abort_startup(item)
        except subprocess.TimeoutExpired:
            self._warn(identity, "PHYSICAL_STOP_UNCONFIRMED")
        except httpx.TransportError:
            self._warn(identity, "API_REPLY_UNKNOWN")
        except StreamRequestError as error:
            self._warn(identity, error.code)
            if error.code in {
                "SUBTASK_FENCED",
                "SUBTASK_SCOPE_DENIED",
                "IDENTITY_CONFLICT",
            }:
                run = self.runs.get(identity)
                if run is not None:
                    run.fenced = True
                    for child in self._children(run):
                        await run_sync(child.close)
        except (OSError, StreamProtocolError, DAGLaunchJournalError) as error:
            self._warn(identity, type(error).__name__)
            run = self.runs.get(identity)
            if run is not None:
                run.startup_failed = True
                if run.death is not None:
                    run.death.preflight_failed = True
                if item.phase == "STARTING":
                    await self._abort_startup(item)

    async def _drive(self, item: d.StreamDAGWorkerAssignment) -> None:
        self._check_item(item)
        identity = item.assignment.id
        run = self.runs.get(identity)
        if item.assignment.physical_slot_released:
            if run is not None:
                for child in self._children(run):
                    await run_sync(child.close)
                self.runs.pop(identity)
            return
        if run is None:
            if len(self.runs) >= 64:
                raise StreamProtocolError("DAG worker local registry is full")
            run = self._new_run(item)
            self.runs[identity] = run
        run.item = item
        if item.phase == "STOPPING":
            await self._stop(run)
            return
        if run.fenced:
            return
        if run.recovering:
            # Lost private capability/GO state is never reconstructed. Cleanup
            # only; a later explicit control can release the exact closed set.
            await self._close_all(run)
            if item.phase == "STARTING":
                await self._abort_startup(item)
            else:
                self._warn(identity, "RECOVERY_CONTROL_REQUIRED")
            return
        if run.startup_failed and item.phase == "STARTING":
            await self._abort_startup(item)
            return
        if run.death is not None:
            await self._resolve_death(run)
            return
        if run.launch is None:
            intent = run.journal.intent(item.start_operation_id)
            run.launch = _DAGLaunch(intent.operation_id)
        launch = run.launch
        if not await self._advance_launch(run, launch):
            return
        assert launch.child is not None and launch.credentials is not None
        child = launch.child
        if child.process.poll() is not None:
            if item.phase == "STARTING":
                await self._abort_startup(item)
            elif run.grant is not None:
                await self._resolve_death(run)
            return
        if run.grant is None:
            holder, capability, _, public = launch.credentials
            if run.claim is None:
                run.claim = {
                    "operation_id": uuid5(identity, "assigned-worker-claim-v1"),
                    "graph_digest": item.graph_digest,
                    "holder_id": holder,
                    "capability": capability,
                    "event_public_key": public,
                    **self.principal,
                }
            run.grant = await self.worker.client.claim_worker_stream_subtask(
                worker_id=self.worker_id, subtask_id=identity, **run.claim
            )
            assert run.grant is not None
            self._check_grant(item, run.grant)
        assert run.grant is not None
        if run.grant.state == "CLAIMED":
            if run.registration is None:
                run.registration = {
                    "operation_id": uuid5(run.grant.id, "register"),
                    "grant_id": run.grant.id,
                    "child_process_id": child.process.pid,
                    "process_start_ticks": child.process_start_ticks,
                }
            run.grant = await self.worker.client.register_worker_stream_subtask_process(
                worker_id=self.worker_id,
                subtask_id=identity,
                principal=self.principal,
                registration=run.registration,
            )
        await self._observe_live(run)

    async def _advance_launch(self, run: _ManagedDAG, launch: _DAGLaunch) -> bool:
        item = run.item
        if launch.stopped:
            return False
        if launch.attempt is None:
            result = await self.worker.client.prepare_worker_stream_dag_launch(
                worker_id=self.worker_id,
                subtask_id=item.assignment.id,
                principal=self.principal,
                operation_id=launch.operation_id,
                start_operation_id=item.start_operation_id,
                replacement_operation_id=launch.replacement_operation_id,
            )
            attempt = result.current
            if (
                result.receipt.operation_id != launch.operation_id
                or result.receipt.kind != "PREPARE"
                or attempt.subtask_id != item.assignment.id
                or attempt.graph_revision_id != item.assignment.graph_revision_id
                or attempt.worker_id != self.worker_id
                or attempt.worker_generation_digest != self.generation
                or attempt.start_operation_id != item.start_operation_id
                or attempt.replacement_operation_id != launch.replacement_operation_id
            ):
                raise StreamProtocolError("DAG PREPARE returned another exact launch")
            run.journal.bind(launch.operation_id, attempt.id, attempt.attempt_index)
            launch.attempt = attempt
            if result.disposition != "SPAWN_ALLOWED":
                run.startup_failed = True
                launch.stopped = True
                return False
        attempt = launch.attempt
        assert attempt is not None
        if launch.child is None:
            if not self._has_capacity():
                return False
            launch.child = await run_sync(
                partial(
                    DAGSubtaskProcess.start,
                    journal=run.journal,
                    operation_id=launch.operation_id,
                    api_url=self.api_url,
                    cwd=Path(item.working_directory),
                )
            )
        child = launch.child
        if not child.code_ready:
            local = run.journal.read(launch.operation_id)
            result = await self.worker.client.record_worker_stream_dag_launch(
                worker_id=self.worker_id,
                subtask_id=item.assignment.id,
                principal=self.principal,
                operation_id=dag_launch_record_operation_id(attempt.id),
                launch_attempt_id=attempt.id,
                child_process_id=local.child_process_id,
                process_start_ticks=local.process_start_ticks,
                local_journal_digest=run.journal.process_record_digest(
                    launch.operation_id
                ),
            )
            if (
                result.receipt.operation_id
                != dag_launch_record_operation_id(attempt.id)
                or result.receipt.kind != "RECORD"
                or result.current.id != attempt.id
                or (result.current.child_process_id, result.current.process_start_ticks)
                != (child.process.pid, child.process_start_ticks)
            ):
                raise StreamProtocolError("DAG RECORD returned another exact process")
            launch.attempt = result.current
            if result.disposition != "PREFLIGHT_ALLOWED":
                run.startup_failed = True
                launch.stopped = True
                return False
            await run_sync(
                partial(
                    child.allow_preflight,
                    {
                        "configuration": item.task_configuration.model_dump(
                            mode="json"
                        ),
                        "role": item.assignment.manifest.role,
                    },
                )
            )
        if launch.credentials is None:
            launch.credentials = create_native_stream_credentials()
        return True

    def _bootstrap(self, run: _ManagedDAG) -> dict[str, Any]:
        launch, grant = run.launch, run.grant
        assert launch is not None and launch.child is not None
        assert (
            launch.credentials is not None
            and launch.attempt is not None
            and grant is not None
        )
        child = launch.child
        self._check_grant(run.item, grant)
        holder, capability, private, _ = launch.credentials
        if (
            grant.state != "REGISTERED"
            or grant.holder_id != holder
            or (grant.child_process_id, grant.process_start_ticks)
            != (child.process.pid, child.process_start_ticks)
        ):
            raise StreamProtocolError(
                "DAG bootstrap is not registered to its retained child"
            )
        assignment = run.item.assignment
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
            "output_stream_id": str(assignment.output_stream_id),
            "child_process_id": child.process.pid,
            "process_start_ticks": child.process_start_ticks,
            "api_url": self.api_url,
            "capability": capability,
            "event_private_key": private.hex(),
            "launch_attempt_id": str(launch.attempt.id),
            "worker_generation_digest": self.generation,
        }

    async def _observe_live(self, run: _ManagedDAG) -> None:
        assert (
            run.launch is not None
            and run.launch.child is not None
            and run.grant is not None
        )
        child, item = run.launch.child, run.item
        if child.bootstrap_fd >= 0:
            await run_sync(child.write_bootstrap, self._bootstrap(run))
        ready = child.observe_bootstrap_ready()
        if (
            ready
            and item.bootstrap_ready
            and item.phase == "ACTIVE"
            and item.current_grant is not None
            and item.current_grant.id == run.grant.id
            and not child.released
        ):
            child.release()
        current = time.monotonic()
        if current - run.last_renewed >= 15:
            await self.worker.client.renew_worker_stream_subtask(
                worker_id=self.worker_id,
                subtask_id=item.assignment.id,
                principal=self.principal,
                grant_id=run.grant.id,
            )
            run.last_renewed = current
        if (
            child.released
            and item.task_state
            in {
                StateType.COMPLETED,
                StateType.FAILED,
                StateType.CRASHED,
                StateType.CANCELLED,
            }
            and item.output_state != "OPEN"
        ):
            if run.terminal_seen_at is None:
                run.terminal_seen_at = current
            elif current - run.terminal_seen_at >= self.terminal_exit_grace_seconds:
                await run_sync(child.close)
                await self._resolve_death(run)

    async def _abort_startup(self, item: d.StreamDAGWorkerAssignment) -> None:
        try:
            await self.worker.client.abort_worker_stream_dag_startup(
                worker_id=self.worker_id,
                graph_revision_id=item.assignment.graph_revision_id,
                principal=self.principal,
                start_operation_id=item.start_operation_id,
                graph_digest=item.graph_digest,
                routing_digest=item.routing_digest,
            )
        except StreamRequestError as error:
            if error.code != "ACTIVATION_ALREADY_COMMITTED":
                raise

    async def _close_attempt(
        self, run: _ManagedDAG, attempt: d.StreamDAGLaunchAttempt
    ) -> None:
        if attempt.id in run.closed_attempts:
            return
        operation = dag_launch_prepare_operation_id(
            run.item.assignment.id,
            attempt.start_operation_id,
            attempt.replacement_operation_id,
        )
        local = run.journal.bind(operation, attempt.id, attempt.attempt_index)
        if local.birth_reserved and local.child_process_id is None:
            raise DAGLaunchJournalError("Reserved process birth has no exact PID proof")
        if attempt.state == "CLOSED":
            if not local.spawn_fence_closed:
                raise DAGLaunchJournalError(
                    "Closed server attempt lacks its local fence"
                )
            run.closed_attempts.add(attempt.id)
            return
        if attempt.id not in run.close_proofs:
            # close_unstarted/reserve_birth serialize under the same journal
            # mutex. A reserved birth is already immutable; do not retain a
            # thread lock across HTTP or a worker-thread wait.
            if local.child_process_id is None:
                closed = run.journal.close_unstarted(operation)
                proof = {
                    "kind": "CLOSED_UNSTARTED",
                    "local_journal_digest": run.journal.digest(closed),
                    "recovery_complete": True,
                    "spawn_fence_closed": True,
                }
            else:
                assert local.process_start_ticks is not None
                if attempt.state == "PREPARED":
                    await self.worker.client.record_worker_stream_dag_launch(
                        worker_id=self.worker_id,
                        subtask_id=run.item.assignment.id,
                        principal=self.principal,
                        operation_id=dag_launch_record_operation_id(attempt.id),
                        launch_attempt_id=attempt.id,
                        child_process_id=local.child_process_id,
                        process_start_ticks=local.process_start_ticks,
                        local_journal_digest=run.journal.process_record_digest(
                            operation
                        ),
                    )
                elif (attempt.child_process_id, attempt.process_start_ticks) != (
                    local.child_process_id,
                    local.process_start_ticks,
                ):
                    raise DAGLaunchJournalError(
                        "Server and local process identity differ"
                    )
                for child in self._children(run):
                    if (
                        child.process.pid == local.child_process_id
                        and child.process_start_ticks == local.process_start_ticks
                    ):
                        await run_sync(child.close)
                candidate = SimpleNamespace(
                    host_id=UUID(self.principal["host_id"]),
                    boot_id=UUID(self.principal["boot_id"]),
                    child_process_id=local.child_process_id,
                    process_start_ticks=local.process_start_ticks,
                )
                witness = await run_sync(
                    partial(verify_native_process_death, candidate, terminate=True)
                )
                try:
                    os.waitpid(local.child_process_id, os.WNOHANG)
                except ChildProcessError:
                    pass
                run.journal.close_after_verified_death(
                    operation, local.child_process_id, local.process_start_ticks
                )
                proof = {"kind": "DEATH", "death_witness": witness}
            run.close_proofs[attempt.id] = proof
        result = await self.worker.client.close_worker_stream_dag_launch(
            worker_id=self.worker_id,
            subtask_id=run.item.assignment.id,
            principal=self.principal,
            operation_id=dag_launch_close_operation_id(attempt.id),
            launch_attempt_id=attempt.id,
            proof=run.close_proofs[attempt.id],
        )
        if (result.operation_id, result.launch_attempt_id, result.subtask_id) != (
            dag_launch_close_operation_id(attempt.id),
            attempt.id,
            run.item.assignment.id,
        ):
            raise StreamProtocolError("DAG CLOSE receipt has another exact attempt")
        run.closed_attempts.add(attempt.id)

    async def _close_all(self, run: _ManagedDAG) -> None:
        attempts = {row.id: row for row in run.item.launch_attempts}
        for launch in (run.launch, run.death.candidate if run.death else None):
            if launch is not None and launch.attempt is not None:
                attempts.setdefault(launch.attempt.id, launch.attempt)
        for attempt in sorted(attempts.values(), key=lambda row: row.attempt_index):
            await self._close_attempt(run, attempt)
        for local in run.journal.records():
            if local.launch_attempt_id is None:
                run.journal.close_unstarted(local.operation_id)

    async def _stop(self, run: _ManagedDAG) -> None:
        control = run.item.control
        if control is None:
            raise StreamProtocolError("DAG STOPPING lacks its immutable control")
        await self._close_all(run)
        result = await self.worker.client.stop_worker_stream_dag_subtask(
            worker_id=self.worker_id,
            subtask_id=run.item.assignment.id,
            principal=self.principal,
            operation_id=dag_subtask_stop_operation_id(
                control.operation_id, run.item.assignment.id
            ),
            control_operation_id=control.operation_id,
        )
        if (
            not result.physical_slot_released
            or result.subtask_id != run.item.assignment.id
        ):
            raise StreamProtocolError(
                "DAG STOP did not release its exact proven target"
            )
        for child in self._children(run):
            await run_sync(child.close)
        self.runs.pop(run.item.assignment.id, None)

    async def _resolve_death(self, run: _ManagedDAG) -> None:
        assert (
            run.grant is not None
            and run.launch is not None
            and run.launch.attempt is not None
        )
        grant = run.grant
        if run.death is None:
            run.death = _DAGDeath(
                uuid5(grant.id, "assigned-worker-death-v1"), secrets.token_hex(32)
            )
        death = run.death
        common = {
            "worker_id": self.worker_id,
            "subtask_id": run.item.assignment.id,
            "principal": self.principal,
        }
        if death.authorization is None:
            death.authorization = (
                await self.worker.client.authorize_worker_stream_subtask_replacement(
                    **common,
                    replacement={
                        "operation_id": death.operation_id,
                        "expected_grant_id": grant.id,
                        "expected_physical_epoch": grant.physical_epoch,
                        "host_id": self.principal["host_id"],
                        "challenge_capability": death.challenge,
                    },
                )
            )
        await self._close_attempt(run, run.launch.attempt)
        if death.witness is None:
            death.witness = verify_native_process_death(
                death.authorization, terminate=False
            )
        item = run.item
        retiring = (
            death.preflight_failed
            or item.output_state != "OPEN"
            or item.task_run_count
            >= (item.task_configuration.task_policy.retries or 0) + 1
            or grant.physical_epoch >= 64
            or item.task_state in {StateType.FAILED, StateType.CRASHED}
            and item.task_retriable is False
        )
        if retiring:
            await self._close_all(run)
            if death.retirement is None:
                death.retirement = {
                    "operation_id": death.operation_id,
                    "challenge_capability": death.challenge,
                    "death_witness": death.witness,
                    "error": "Verified physical process reached its terminal invocation",
                    "reason": "REPLACEMENT_PREFLIGHT_FAILED"
                    if death.preflight_failed
                    else "TERMINAL_RECONCILIATION",
                }
            await self.worker.client.retire_verified_worker_stream_subtask(
                **common, retirement=death.retirement
            )
            for child in self._children(run):
                await run_sync(child.close)
            self.runs.pop(item.assignment.id, None)
            return
        if death.candidate is None:
            local = run.journal.intent(item.start_operation_id, death.operation_id)
            death.candidate = _DAGLaunch(local.operation_id, death.operation_id)
        candidate = death.candidate
        try:
            if not await self._advance_launch(run, candidate):
                return
        except StreamRequestError as error:
            if error.code not in {
                "TERMINAL_RECONCILIATION_REQUIRED",
                "RETRY_EXHAUSTED",
            }:
                raise
            # Poll hints can predate EOF. A current PREPARE refusal must
            # retire the proven old process before any candidate is born.
            death.preflight_failed = True
            await self._resolve_death(run)
            return
        assert candidate.child is not None and candidate.credentials is not None
        if death.confirmation is None:
            holder, capability, _, public = candidate.credentials
            death.confirmation = {
                "operation_id": death.operation_id,
                "challenge_capability": death.challenge,
                "death_witness": death.witness,
                "new_holder_id": holder,
                "new_capability": capability,
                "new_event_public_key": public,
                "new_child_process_id": candidate.child.process.pid,
                "new_process_start_ticks": candidate.child.process_start_ticks,
                "host_id": self.principal["host_id"],
                "boot_id": self.principal["boot_id"],
            }
        try:
            new_grant = (
                await self.worker.client.confirm_worker_stream_subtask_replacement(
                    **common, replacement=death.confirmation
                )
            )
        except StreamRequestError as error:
            if error.code not in {
                "TERMINAL_RECONCILIATION_REQUIRED",
                "RETRY_EXHAUSTED",
            }:
                raise
            death.preflight_failed = True
            await self._resolve_death(run)
            return
        self._check_grant(item, new_grant)
        run.launch = candidate
        run.grant = new_grant
        run.death = None
        run.startup_failed = False
        run.last_renewed = 0
        run.terminal_seen_at = None
        await self._observe_live(run)
