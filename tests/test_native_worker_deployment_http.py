"""A real ProcessWorker obtains authority before launching a native deployment."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import sqlalchemy as sa

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.deployments.runner import RunnerDeployment
from prefect.flows import load_flow_from_entrypoint
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_URL,
    temporary_settings,
)
from prefect.workers.process import ProcessWorker
from tests.test_native_execution_http import execution_api as execution_api
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI


@pytest.mark.parametrize(
    "fault,count,asynchronous",
    [
        ("none", 8, False),
        ("prefix-before", 8, False),
        ("prefix-receipt", 8, False),
        ("before-yield", 500, False),
        ("after-yield", 500, False),
        ("tail-receipt", 8, False),
        ("none", 500, True),
        ("always-before-yield", 500, False),
    ],
)
@pytest.mark.timeout(300)
async def test_actual_process_worker_launches_declared_mixed_deployment(
    execution_api: RestartableAPI,
    tmp_path: Path,
    db: Any,
    fault: str,
    count: int,
    asynchronous: bool,
):
    server = execution_api
    dataset, coordination = tmp_path / "dataset", tmp_path / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for number in range(1, count + 1):
        (dataset / f"{number:04}.json").write_text(json.dumps({"number": number}))
    definition = load_flow_from_entrypoint(
        "tests/fixtures/native_stream_mixed_flow.py:"
        + ("deployed_async_mixed_flow" if asynchronous else "deployed_mixed_flow")
    )
    pool = "native-" + uuid4().hex
    with temporary_settings(
        {PREFECT_API_URL: server.url, PREFECT_API_AUTH_STRING: server.auth}
    ):
        async with get_client() as client:
            await client.create_work_pool(
                WorkPoolCreate(
                    name=pool,
                    type="process",
                    base_job_template=ProcessWorker.get_default_base_job_template(),
                )
            )
            deployment = RunnerDeployment.from_flow(
                definition,
                name="mixed",
                work_pool_name=pool,
                concurrency_limit=1,
                job_variables={"working_dir": str(Path.cwd()), "stream_output": True},
            )
            deployment_id = await deployment.aapply()
            run = await client.create_flow_run_from_deployment(
                deployment_id,
                parameters={
                    "dataset": str(dataset),
                    "coordination": str(coordination),
                    "count": count,
                    "fault": fault,
                },
            )
    environment = dict(os.environ)
    for name in (
        "PREFECT_SERVER_DATABASE_CONNECTION_URL",
        "PREFECT_API_DATABASE_CONNECTION_URL",
    ):
        environment.pop(name, None)
    environment.update(
        PREFECT_API_URL=server.url,
        PREFECT_API_AUTH_STRING=server.auth,
        PREFECT_LOGGING_LEVEL="INFO",
        PREFECT_LOGGING_INTERNAL_LEVEL="WARNING",
    )
    log = tmp_path / "worker.log"
    with log.open("wb") as output:
        child = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "prefect",
                "worker",
                "start",
                "--pool",
                pool,
                "--type",
                "process",
                "--run-once",
            ],
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
    try:
        assert child.wait(timeout=240) == 0, log.read_text()[-20000:]
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    with server.client() as client:
        completed = client.read_flow_run(run.id)
        exhausted = fault == "always-before-yield"
        assert (
            completed.state.is_failed() if exhausted else completed.state.is_completed()
        ), log.read_text()[-20000:]
        plan = client.read_native_stream_deployment_plan(flow_run_id=run.id)
        assert plan.current_launch is not None
        assert plan.current_launch.state == "RELEASED"
        assert plan.state == "RELEASED"
        assert plan.declaration.flow_version == "native-worker-v1"
        attempts = 3 if exhausted else (1 if fault == "none" else 2)
        assert plan.current_launch.generation == attempts
        assert completed.run_count == attempts
    async with db.session_context() as session:
        assert await session.scalar(
            sa.text("SELECT count(*) FROM task_run WHERE flow_run_id=:flow"),
            {"flow": run.id},
        ) == (3 if exhausted else 4)
        assert (
            await session.scalar(
                sa.text(
                    "SELECT count(*) FROM stream_partition WHERE flow_run_id=:flow"
                ),
                {"flow": run.id},
            )
            == 2
        )
    assert (coordination / "prefix-bodies").read_text().splitlines() == ["prepare"] * (
        2 if fault == "prefix-before" else 1
    )
    if exhausted:
        assert not (coordination / "tail-bodies").exists()
        async with db.session_context() as session:
            assert (
                await session.scalar(
                    sa.text(
                        "SELECT count(*) FROM native_stream_control_operation WHERE flow_run_id=:flow AND kind='VERIFIED_RETIRED'"
                    ),
                    {"flow": run.id},
                )
                == 1
            )
    else:
        assert (coordination / "tail-bodies").read_text().splitlines() == ["summarize"]
    assert len(list((coordination / "signed").glob("*.json"))) == (
        199 if exhausted else count
    )


def test_ordinary_flow_keeps_fractional_retry_delay_on_actual_api(
    execution_api: RestartableAPI, tmp_path: Path
):
    marker = tmp_path / "attempt"

    @flow(retries=1, retry_delay_seconds=0.05)
    def ordinary() -> str:
        if not marker.exists():
            marker.touch()
            raise ValueError("Exercise ordinary fractional retry policy")
        return "ordinary-complete"

    with temporary_settings(
        {
            PREFECT_API_URL: execution_api.url,
            PREFECT_API_AUTH_STRING: execution_api.auth,
        }
    ):
        state = ordinary(return_state=True)
    assert state.is_completed()
    with execution_api.client() as client:
        observed = client.read_flow_run(state.state_details.flow_run_id)
    assert observed.run_count == 2
    assert observed.empirical_policy.retry_delay == 0.05
