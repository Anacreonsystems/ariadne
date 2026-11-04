"""Mixed SDK execution on the actual flow-engine entrypoint and PostgreSQL."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import sqlalchemy as sa

from prefect._internal.stream_execution import (
    StreamExecutionContext,
)
from prefect.flows import load_flow_from_entrypoint
from prefect.states import Pending
from tests.test_native_execution_http import _takeover_request
from tests.test_native_execution_http import execution_api as execution_api
from tests.test_native_flow_recovery_http import _launch, _start_flow
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("count", [8, 500])
async def test_mixed_direct_tasks_execute_through_normal_flow_engine(
    execution_api: RestartableAPI, tmp_path: Path, db: Any, mode: str, count: int
):
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    coordination = tmp_path / "coordination"
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for position in range(1, count + 1):
        (dataset / f"{position:04}.json").write_text(
            json.dumps({"number": position, "value": f"document-{position}"})
        )
    name = "mixed_flow" if mode == "sync" else "async_mixed_flow"
    entrypoint = f"tests/fixtures/native_stream_mixed_flow.py:{name}"
    definition = load_flow_from_entrypoint(entrypoint)
    with execution_api.client() as client:
        run = client.create_flow_run(
            definition,
            parameters={
                "dataset": str(dataset),
                "coordination": str(coordination),
                "count": count,
            },
            state=Pending(),
        )
    launch = _launch(execution_api, run.id)
    output = tmp_path / "flow.log"
    child = _start_flow(execution_api, entrypoint, launch.context, output)
    try:
        assert child.wait(timeout=180) == 0, output.read_text()[-16000:]
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    with execution_api.client() as client:
        completed = client.read_flow_run(run.id)
        assert completed.state.is_completed(), output.read_text()[-16000:]
    async with db.session_context() as session:
        for table, expected in (
            ("task_run", 4),
            ("stream_partition", 2),
            ("stream_finite_submission", 2),
        ):
            assert (
                await session.scalar(
                    sa.text(f"SELECT count(*) FROM {table} WHERE flow_run_id=:flow"),
                    {"flow": run.id},
                )
                == expected
            )
        assert (
            await session.scalar(
                sa.text(
                    "SELECT count(*) FROM stream_finite_receipt r JOIN stream_finite_submission s ON s.id=r.finite_submission_id WHERE s.flow_run_id=:flow"
                ),
                {"flow": run.id},
            )
            == 2
        )
    assert (coordination / "prefix-bodies").read_text().splitlines() == ["prepare"]
    assert (coordination / "tail-bodies").read_text().splitlines() == ["summarize"]


@pytest.mark.timeout(300)
@pytest.mark.parametrize(
    "fault",
    ["prefix-before", "prefix-receipt", "before-yield", "after-yield", "tail-receipt"],
)
async def test_mixed_process_loss_preserves_task_ids_and_durable_boundary(
    execution_api: RestartableAPI, tmp_path: Path, db: Any, fault: str
):
    dataset, coordination = tmp_path / "dataset", tmp_path / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for position in range(1, 501):
        (dataset / f"{position:04}.json").write_text(
            json.dumps({"number": position, "value": f"document-{position}"})
        )
    entrypoint = "tests/fixtures/native_stream_mixed_flow.py:mixed_flow"
    definition = load_flow_from_entrypoint(entrypoint)
    with execution_api.client() as client:
        original = client.create_flow_run(
            definition,
            parameters={
                "dataset": str(dataset),
                "coordination": str(coordination),
                "fault": fault,
                "count": 500,
            },
            state=Pending(),
        )
    launch = _launch(execution_api, original.id)
    first_log, second_log = tmp_path / "first.log", tmp_path / "second.log"
    first = _start_flow(execution_api, entrypoint, launch.context, first_log)
    try:
        assert first.wait(timeout=200) == 77, first_log.read_text()[-16000:]
    finally:
        if first.poll() is None:
            first.kill()
            first.wait(timeout=10)
    async with db.session_context() as session:
        existing_ids = set(
            (
                await session.execute(
                    sa.text("SELECT id FROM task_run WHERE flow_run_id=:flow"),
                    {"flow": original.id},
                )
            ).scalars()
        )
        if fault in {"before-yield", "after-yield"}:
            before = (
                (
                    await session.execute(
                        sa.text(
                            "SELECT c.completed_through FROM stream_consumer c JOIN stream_partition p ON c.output_stream_id=p.id WHERE p.flow_run_id=:flow"
                        ),
                        {"flow": original.id},
                    )
                )
                .scalars()
                .all()
            )
            assert before == [199 if fault == "before-yield" else 200]
    request, private = _takeover_request(
        launch,
        {
            "launcher_instance_id": uuid4(),
            "child_process_id": first.pid,
            "exit_code": first.returncode,
            "observed_at": datetime.now(UTC),
            "kind": "WAITPID",
        },
    )
    with execution_api.client() as client:
        replacement = client.takeover_stream_execution(**request)
    current = StreamExecutionContext(
        original.id,
        replacement.generation,
        replacement.holder_id,
        execution_api.url,
        request["new_holder_capability"],
        private,
    )
    second = _start_flow(execution_api, entrypoint, current, second_log)
    try:
        assert second.wait(timeout=200) == 0, second_log.read_text()[-20000:]
    finally:
        if second.poll() is None:
            second.kill()
            second.wait(timeout=10)
    with execution_api.client() as client:
        assert client.read_flow_run(original.id).state.is_completed(), (
            second_log.read_text()[-20000:]
        )
    async with db.session_context() as session:
        final_ids = set(
            (
                await session.execute(
                    sa.text("SELECT id FROM task_run WHERE flow_run_id=:flow"),
                    {"flow": original.id},
                )
            ).scalars()
        )
        assert existing_ids <= final_ids and len(final_ids) == 4
        assert (
            await session.scalar(
                sa.text(
                    "SELECT count(*) FROM stream_partition WHERE flow_run_id=:flow"
                ),
                {"flow": original.id},
            )
            == 2
        )
    assert len((coordination / "prefix-bodies").read_text().splitlines()) == (
        2 if fault == "prefix-before" else 1
    )
    assert (coordination / "tail-bodies").read_text().splitlines() == ["summarize"]
    assert len(list((coordination / "signed").glob("*.json"))) == 500


@pytest.mark.timeout(240)
async def test_large_collection_persists_bounded_descriptor_not_document_array(
    execution_api: RestartableAPI, tmp_path: Path, db: Any
):
    dataset, coordination = tmp_path / "dataset", tmp_path / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for position in range(1, 501):
        (dataset / f"{position:04}.json").write_text(json.dumps({"number": position}))
    entrypoint = "tests/fixtures/native_stream_mixed_flow.py:collection_flow"
    definition = load_flow_from_entrypoint(entrypoint)
    with execution_api.client() as client:
        run = client.create_flow_run(
            definition,
            parameters={"dataset": str(dataset), "coordination": str(coordination)},
            state=Pending(),
        )
    launch = _launch(execution_api, run.id)
    log = tmp_path / "flow.log"
    child = _start_flow(execution_api, entrypoint, launch.context, log)
    try:
        assert child.wait(timeout=200) == 0, log.read_text()[-16000:]
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    with execution_api.client() as client:
        assert client.read_flow_run(run.id).state.is_completed(), log.read_text()[
            -16000:
        ]
    async with db.session_context() as session:
        row = (
            await session.execute(
                sa.text(
                    "SELECT parameters, canonical_size FROM stream_finite_submission WHERE flow_run_id=:flow AND phase='TAIL'"
                ),
                {"flow": run.id},
            )
        ).one()
        assert row.canonical_size < 4096
        parameters = row.parameters
        if isinstance(parameters, str):
            parameters = json.loads(parameters)
        witness = parameters["binding"]["values"]["values"]
        assert witness["kind"] == "STREAM_COLLECTION" and witness["limit"] == 500
        assert len(witness["result_digest"]) == 64


@pytest.mark.parametrize(
    ("fault", "tasks", "channels"),
    [
        ("missing-key", 0, 0),
        ("submit", 0, 0),
        ("invalid-result", 1, 0),
        ("mid-graph", 2, 1),
    ],
)
async def test_invalid_mixed_calls_refuse_without_unsafe_admission(
    execution_api: RestartableAPI,
    tmp_path: Path,
    db: Any,
    fault: str,
    tasks: int,
    channels: int,
):
    dataset, coordination = tmp_path / "dataset", tmp_path / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    entrypoint = "tests/fixtures/native_stream_mixed_flow.py:invalid_flow"
    definition = load_flow_from_entrypoint(entrypoint)
    with execution_api.client() as client:
        run = client.create_flow_run(
            definition,
            parameters={
                "dataset": str(dataset),
                "coordination": str(coordination),
                "fault": fault,
            },
            state=Pending(),
        )
    launch = _launch(execution_api, run.id)
    log = tmp_path / "flow.log"
    child = _start_flow(execution_api, entrypoint, launch.context, log)
    try:
        child.wait(timeout=90)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    with execution_api.client() as client:
        final = client.read_flow_run(run.id)
        assert final.state.is_failed(), log.read_text()[-16000:]
        assert final.run_count == (3 if fault in {"missing-key", "submit"} else 1), (
            log.read_text()[-16000:]
        )
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.text("SELECT count(*) FROM task_run WHERE flow_run_id=:flow"),
                {"flow": run.id},
            )
            == tasks
        )
        assert (
            await session.scalar(
                sa.text(
                    "SELECT count(*) FROM stream_partition WHERE flow_run_id=:flow"
                ),
                {"flow": run.id},
            )
            == channels
        )
        assert (
            await session.scalar(
                sa.text(
                    "SELECT count(*) FROM stream_record r JOIN stream_partition p ON r.stream_id=p.id WHERE p.flow_run_id=:flow"
                ),
                {"flow": run.id},
            )
            == 0
        )


@pytest.mark.timeout(90)
async def test_partial_collection_does_not_admit_tail_before_durable_eof(
    execution_api: RestartableAPI, tmp_path: Path, db: Any
):
    dataset, coordination = tmp_path / "dataset", tmp_path / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for position in range(1, 501):
        (dataset / f"{position:04}.json").write_text(json.dumps({"number": position}))
    entrypoint = "tests/fixtures/native_stream_mixed_flow.py:partial_collection_flow"
    definition = load_flow_from_entrypoint(entrypoint)
    with execution_api.client() as client:
        run = client.create_flow_run(
            definition,
            parameters={"dataset": str(dataset), "coordination": str(coordination)},
            state=Pending(),
        )
    launch = _launch(execution_api, run.id)
    log = tmp_path / "flow.log"
    child = _start_flow(execution_api, entrypoint, launch.context, log)
    try:
        child.wait(timeout=60)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    with execution_api.client() as client:
        observed = client.read_flow_run(run.id)
    assert observed.state.is_failed(), log.read_text()[-12000:]
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.text("SELECT count(*) FROM task_run WHERE flow_run_id=:flow"),
                {"flow": run.id},
            )
            == 3
        )
        assert (
            await session.scalar(
                sa.text(
                    "SELECT count(*) FROM stream_finite_submission WHERE flow_run_id=:flow AND phase='TAIL'"
                ),
                {"flow": run.id},
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.text(
                    "SELECT count(*) FROM stream_record r JOIN stream_partition p ON p.id=r.stream_id WHERE p.flow_run_id=:flow"
                ),
                {"flow": run.id},
            )
            > 0
        )
