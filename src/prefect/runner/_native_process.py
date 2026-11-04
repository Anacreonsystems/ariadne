"""Linux process identity and pidfd control for the native local profile."""

from __future__ import annotations

import os
import select
import signal
from datetime import UTC, datetime
from typing import Any

from prefect.streams import StreamProtocolError


class NativePidfdScope:
    wait_after_graceful_kill = True

    def __init__(self, pid: int) -> None:
        self.pid: int = pid
        self._descriptor: int = os.pidfd_open(pid)

    def is_alive(self, pid: int) -> bool:
        if pid != self.pid or self._descriptor < 0:
            return False
        poller = select.poll()
        poller.register(self._descriptor, select.POLLIN)
        return not poller.poll(0)

    def _signal(self, pid: int, value: int) -> None:
        if pid == self.pid and self.is_alive(pid):
            try:
                signal.pidfd_send_signal(self._descriptor, value)
            except ProcessLookupError:
                pass

    def graceful_kill(self, pid: int) -> None:
        self._signal(pid, signal.SIGTERM)

    def hard_kill(self, pid: int) -> None:
        self._signal(pid, signal.SIGKILL)

    def close(self) -> None:
        if self._descriptor >= 0:
            os.close(self._descriptor)
            self._descriptor = -1

    def wait_for_exit(self, timeout: float) -> bool:
        """Wait for the kernel to report exit of this pinned identity."""
        if self._descriptor < 0:
            return True
        poller = select.poll()
        poller.register(self._descriptor, select.POLLIN)
        return bool(poller.poll(int(timeout * 1000)))


def verify_native_process_death(
    candidate: Any, *, terminate: bool = True, timeout: float = 10.0
) -> dict[str, Any]:
    """Observe exact process death; an elapsed clock never substitutes for it."""
    # Starter imports this scope, so identity's inverse dependency stays local.
    from prefect.runner._starter_native import linux_process_identity

    host, boot, _ = linux_process_identity(os.getpid())
    if host != candidate.host_id:
        raise StreamProtocolError(
            "Native replacement cannot inspect the recorded host/PID namespace"
        )
    result: dict[str, Any] = {
        "host_id": host,
        "boot_id": candidate.boot_id,
        "child_process_id": candidate.child_process_id,
        "process_start_ticks": candidate.process_start_ticks,
        "observed_at": datetime.now(UTC),
    }
    if boot != candidate.boot_id:
        return result | {"kind": "BOOT_CHANGED", "observed_boot_id": boot}
    pid = candidate.child_process_id
    try:
        _, _, ticks = linux_process_identity(pid)
    except FileNotFoundError:
        return result | {"kind": "IDENTITY_GONE"}
    if ticks != candidate.process_start_ticks:
        # A reused PID is positive evidence that the old identity is gone. It
        # never authorizes a signal to the unrelated replacement process.
        return result | {"kind": "IDENTITY_GONE"}
    if pid in (1, os.getpid()):
        raise StreamProtocolError(
            "A native recovery cannot terminate its own supervisor"
        )
    try:
        scope = NativePidfdScope(pid)
    except ProcessLookupError:
        return result | {"kind": "IDENTITY_GONE"}
    try:
        try:
            _, _, after_open_ticks = linux_process_identity(pid)
        except FileNotFoundError:
            return result | {"kind": "IDENTITY_GONE"}
        if after_open_ticks != candidate.process_start_ticks:
            return result | {"kind": "IDENTITY_GONE"}
        if scope.is_alive(pid):
            if not terminate:
                raise StreamProtocolError("The recorded native child is still alive")
            scope.hard_kill(pid)
        if not scope.wait_for_exit(timeout):
            raise StreamProtocolError("Native pidfd exit was not observed")
        return result | {"kind": "PIDFD_EXIT", "observed_at": datetime.now(UTC)}
    finally:
        scope.close()
