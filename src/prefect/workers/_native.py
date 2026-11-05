"""Native worker enrollment composed with Prefect's ordinary executor."""

from __future__ import annotations

# This coordinator composes internal storage/executor boundaries.
# pyright: reportPrivateUsage=false
import os
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import anyio

from prefect._internal.stream_execution import stream_execution_context
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.flows import load_flow_from_flow_run
from prefect.runner._flow_run_executor import FlowRunExecutorContext
from prefect.runner._hook_runner import HookRunner
from prefect.runner._starter_native import NativeProcessStarter, linux_process_identity
from prefect.streams import StreamProtocolError


async def _starter(
    worker: Any, flow_run: Any, configuration: Any
) -> NativeProcessStarter:
    if worker.backend_id is None and worker._worker_channel is not None:
        await worker._worker_channel.ensure_worker_identity()
    if flow_run.deployment_id is None or worker.backend_id is None:
        raise StreamProtocolError(
            "Native deployment launch requires a registered worker"
        )
    deployment = await worker.client.read_deployment(flow_run.deployment_id)
    if configuration._command_configured or deployment.pull_steps:
        raise StreamProtocolError(
            "Native local process profile requires a local deployment and default command"
        )
    if not deployment.entrypoint or configuration.working_dir is None:
        raise StreamProtocolError(
            "Native local process profile requires an explicit local workspace and entrypoint"
        )
    return NativeProcessStarter(
        client=worker.client,
        flow_run=flow_run,
        worker_id=worker.backend_id,
        work_pool_id=worker.work_pool.id,
        cwd=Path(configuration.working_dir).resolve(),
        env=configuration.env,
        entrypoint=deployment.entrypoint,
        stream_output=configuration.stream_output,
    )


async def _execute(
    starter: NativeProcessStarter, flow_run: Any, task_status: Any
) -> Any:
    # ProcessWorker imports this coordinator; keep its result dependency lazy.
    from prefect.workers.process import ProcessWorkerResult

    assert starter.execution is not None
    hooks = HookRunner(resolve_flow=load_flow_from_flow_run)
    starter.cancellation_hooks = hooks.run_cancellation_hooks
    with stream_execution_context(starter.execution):
        async with FlowRunExecutorContext(
            cancellation_handler=starter.request_cancellation,
        ) as context:
            starter.control_channel = context.control_channel
            executor = context.create_executor(
                flow_run,
                starter,
                hook_runner=hooks,
                propose_submitting=False,
                get_attempt_conclusion=starter.get_conclusion,
            )
            result: Any = None

            async def execute(*, task_status: Any = anyio.TASK_STATUS_IGNORED) -> None:
                nonlocal result
                result = await executor.submit(task_status=task_status)

            async with anyio.create_task_group() as group:
                handle = await group.start(execute)
                if handle.pid is None:
                    raise StreamProtocolError(
                        "Native process returned no child identity"
                    )
                task_status.started(handle.pid)
    if result is None or result.status_code is None:
        raise StreamProtocolError(
            "Native flow process did not return its execution outcome"
        )
    return ProcessWorkerResult(
        status_code=result.status_code, identifier=str(result.handle.pid)
    )


async def run_native_process_worker(
    worker: Any,
    flow_run: Any,
    configuration: Any,
    task_status: Any,
) -> Any:
    starter = await _starter(worker, flow_run, configuration)
    await starter.enroll()
    worker._native_active_run_ids.add(flow_run.id)
    try:
        return await _execute(starter, flow_run, task_status)
    finally:
        worker._native_active_run_ids.discard(flow_run.id)


async def _recover_run(worker: Any, candidate: Any) -> None:
    """Recover within the ordinary worker's task group and local run limit."""
    flow_run: Any = None
    try:
        flow_run = await worker.client.read_flow_run(candidate.flow_run_id)
        cleanup_only = candidate.identity_kind == "LAUNCHER" or (
            flow_run.state is not None
            and (
                flow_run.state.is_final()
                or flow_run.state.is_cancelling()
                or flow_run.state.is_pending()
            )
        )
        if cleanup_only:
            # Kernel-verified retirement does not need the deployment artifact.
            # A deleted deployment must not prevent cancelling a known child or
            # releasing an abandoned final run.
            starter = NativeProcessStarter(
                client=worker.client,
                flow_run=flow_run,
                worker_id=worker.backend_id,
                work_pool_id=worker.work_pool.id,
                cwd=Path.cwd(),
                env={},
                entrypoint="",
                stream_output=False,
            )
        else:
            configuration = await worker.job_configuration.resolve_for_flow_run(
                flow_run,
                client=worker.client,
                work_pool=worker.work_pool,
                worker_name=worker.name,
                worker_id=worker.backend_id,
            )
            starter = await _starter(worker, flow_run, configuration)
        # A replacement retains the existing deployment lease. In particular,
        # do not propose Pending/Submitting and acquire that slot a second time.
        starter.cancellation_hooks = HookRunner(
            resolve_flow=load_flow_from_flow_run
        ).run_cancellation_hooks
        if not await starter.recover(candidate):
            return
        if cleanup_only:
            # Discovery may race with entry into Running. A legitimate restart
            # resolves its workspace after the kernel-verified decision.
            try:
                configuration = await worker.job_configuration.resolve_for_flow_run(
                    flow_run,
                    client=worker.client,
                    work_pool=worker.work_pool,
                    worker_name=worker.name,
                    worker_id=worker.backend_id,
                )
                prepared = await _starter(worker, flow_run, configuration)
                starter.cwd, starter.env = prepared.cwd, prepared.env
                starter.entrypoint = prepared.entrypoint
                starter.stream_output = prepared.stream_output
            except Exception:
                assert starter.execution is not None
                with stream_execution_context(starter.execution):
                    await worker.client.retire_unstarted_native_stream_launch(
                        flow_run_id=flow_run.id,
                        operation_id=uuid5(
                            NAMESPACE_URL,
                            f"prefect-native-recovery-setup-failed/{flow_run.id}/{starter.execution.generation}",
                        ),
                        enrollment_operation_id=starter.enrollment.operation_id,
                        error="Native replacement workspace could not be resolved",
                    )
                raise
        await _execute(starter, flow_run, anyio.TASK_STATUS_IGNORED)
    except Exception:
        worker._logger.exception(
            "Native recovery could not complete for flow run %s", candidate.flow_run_id
        )
    finally:
        worker._native_active_run_ids.discard(candidate.flow_run_id)
        worker._release_limit_slot(candidate.flow_run_id)


async def recover_native_process_runs(worker: Any) -> list[Any]:
    """Discover only marked native pools; expired renewal is not death proof."""
    if worker.backend_id is None and worker._worker_channel is not None:
        await worker._worker_channel.ensure_worker_identity()
    if worker.backend_id is None:
        raise StreamProtocolError("Native recovery requires a registered worker")
    # Host identity is checked again immediately before any signal is sent.
    host, _, _ = linux_process_identity(os.getpid())
    try:
        candidates = await worker.client.read_native_stream_recovery_candidates(
            worker_id=worker.backend_id,
            work_pool_id=worker.work_pool.id,
            host_id=host,
        )
    except StreamRequestError as error:
        status_code: int | None = getattr(error, "status_code", None)
        if (
            error.code != "HTTP_ERROR"
            or status_code is None
            or not 500 <= status_code < 600
        ):
            raise
        worker._logger.warning(
            "Native recovery discovery is temporarily unavailable (HTTP %s); "
            "retrying on the next poll",
            status_code,
        )
        return []
    submitted: list[Any] = []
    for candidate in candidates:
        if candidate.flow_run_id in worker._native_active_run_ids:
            continue
        try:
            if worker._limiter:
                worker._limiter.acquire_on_behalf_of_nowait(candidate.flow_run_id)
        except (anyio.WouldBlock, RuntimeError):
            continue
        worker._native_active_run_ids.add(candidate.flow_run_id)
        worker._runs_task_group.start_soon(_recover_run, worker, candidate)
        submitted.append(candidate)
    return submitted
