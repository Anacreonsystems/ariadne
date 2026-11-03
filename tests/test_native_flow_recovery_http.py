"""Whole flow-engine replacement on real document references and PostgreSQL."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import sqlalchemy as sa

from prefect._internal.stream_execution import StreamExecutionContext
from prefect.flows import load_flow_from_entrypoint
from prefect.states import Pending
from tests.test_native_execution_http import Launch, _key_pair, _takeover_request
from tests.test_native_execution_http import execution_api as execution_api
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI


def _poll(predicate: Any, message: str, timeout: float = 120) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        assert time.monotonic() < deadline, message
        time.sleep(0.05)


def _child_marker(path: Path, child: subprocess.Popen[Any], log: Path) -> bool:
    if path.exists():
        return True
    assert child.poll() is None, log.read_text()[-9000:]
    return False


def _launch(server: RestartableAPI, run_id: Any) -> Launch:
    private, public = _key_pair()
    request = dict(
        flow_run_id=run_id,
        operation_id=uuid4(),
        holder_id=uuid4(),
        holder_capability=secrets.token_hex(32),
        holder_event_public_key=public,
        supervisor_id=uuid4(),
        supervisor_capability=secrets.token_hex(32),
    )
    with server.client() as client:
        binding = client.claim_stream_execution(**request)
    return Launch(
        run_id,
        request,
        StreamExecutionContext(
            run_id,
            binding.generation,
            binding.holder_id,
            server.url,
            request["holder_capability"],
            private,
        ),
    )


def _start_flow(
    server: RestartableAPI,
    entrypoint: str,
    execution: StreamExecutionContext,
    output: Path,
    *,
    payload_overrides: dict[str, Any] | None = None,
) -> subprocess.Popen[Any]:
    # Dedicated inherited descriptor contains holder authority only. The normal
    # flow-engine command has no alternate task/channel-ID launch arguments.
    read_fd, write_fd = os.pipe()
    assert not os.get_inheritable(read_fd) and not os.get_inheritable(write_fd)
    environment = dict(os.environ)
    for key in (
        "PREFECT_SERVER_DATABASE_CONNECTION_URL",
        "PREFECT_API_DATABASE_CONNECTION_URL",
    ):
        environment.pop(key, None)
    environment.update(
        PREFECT_API_URL=server.url,
        PREFECT_API_AUTH_STRING=server.auth,
        PREFECT__FLOW_RUN_ID=str(execution.flow_run_id),
        PREFECT__STREAM_EXECUTION_FD=str(read_fd),
        PREFECT_LOGGING_LEVEL="INFO",
        PREFECT_LOGGING_INTERNAL_LEVEL="WARNING",
    )
    payload = dict(
        schema="native-stream-execution-v1",
        flow_run_id=str(execution.flow_run_id),
        generation=execution.generation,
        holder_id=str(execution.holder_id),
        api_url=server.url,
        holder_capability=execution.capability,
        event_private_key=execution.event_private_key.hex(),
    )
    assert "supervisor_capability" not in payload
    payload.update(payload_overrides or {})
    encoded = json.dumps(payload).encode()
    assert len(encoded) < 4096
    try:
        with output.open("wb") as log:
            child = subprocess.Popen(
                [sys.executable, "-m", "prefect.flow_engine", entrypoint],
                env=environment,
                pass_fds=(read_fd,),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        os.close(read_fd)
        read_fd = -1
        assert os.write(write_fd, encoded) == len(encoded)
        return child
    finally:
        if read_fd >= 0:
            os.close(read_fd)
        os.close(write_fd)


@pytest.mark.timeout(240)
@pytest.mark.parametrize(
    ("fault", "mode"),
    [
        ("before-yield", "sync"),
        ("after-yield", "sync"),
        ("source-progress", "sync"),
        ("source-eof", "sync"),
        ("source-progress", "async"),
    ],
)
async def test_whole_flow_recovers_document_references_from_server_keys(
    execution_api: RestartableAPI, tmp_path: Path, db: Any, fault: str, mode: str
):
    server = execution_api
    dataset, coordination = tmp_path / "documents", tmp_path / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for number in range(1, 501):
        (dataset / f"{number:04}.json").write_text(
            json.dumps(
                dict(
                    number=number,
                    document_id=f"invoice-{number:04}",
                    amount_cents=1200 + number,
                    text=f"Document {number} for streaming signature acceptance",
                ),
                ensure_ascii=False,
            )
        )
    fixture = (
        Path(__file__).parent
        / "fixtures"
        / (
            "native_stream_document_flow.py"
            if mode == "sync"
            else "native_stream_async_document_flow.py"
        )
    )
    entrypoint = str(fixture) + ":documents_flow"
    definition = load_flow_from_entrypoint(entrypoint)
    with server.client() as client:
        original = client.create_flow_run(
            definition,
            parameters=dict(
                dataset=str(dataset), coordination=str(coordination), fault=fault
            ),
            state=Pending(),
        )
    launch = _launch(server, original.id)
    first = _start_flow(server, entrypoint, launch.context, tmp_path / "attempt1.log")
    second = None
    try:
        _poll(
            lambda: _child_marker(
                coordination / "source-held-open", first, tmp_path / "attempt1.log"
            ),
            "No early downstream consumption",
        )
        with server.client() as client:
            source = client.read_stream_submission(
                flow_run_id=original.id, submission_key="documents.ingest"
            )
            operator = client.read_stream_submission(
                flow_run_id=original.id, submission_key="documents.sign"
            )
            assert client.read_task_run(source.task_run_id).state.is_running()
            assert client.read_stream(source.stream_id).producer_position == 1
            assert client.read_stream(operator.stream_id).record_count == 1
        (coordination / "continue-source").touch()
        if fault in {"before-yield", "after-yield"}:
            _poll(
                lambda: _child_marker(
                    coordination / "operator-at-200", first, tmp_path / "attempt1.log"
                ),
                "Operator did not reach document200",
            )
            with server.client() as client:
                _poll(
                    lambda: client.read_stream(source.stream_id).state == "COMPLETED",
                    "Source EOF not accepted",
                )
            (coordination / "terminate-at-200").touch()
        assert first.wait(timeout=180) == 77, (tmp_path / "attempt1.log").read_text()[
            -6000:
        ]
        async with db.session_context() as session:
            before = (
                await session.execute(
                    sa.text(
                        "SELECT completed_through FROM stream_consumer WHERE output_stream_id=:stream_id"
                    ),
                    {"stream_id": operator.stream_id},
                )
            ).scalar_one()
        if fault == "before-yield":
            assert before == 199
        if fault == "after-yield":
            assert before == 200
        with server.client() as client:
            old_source = client.read_task_run(source.task_run_id)
            if fault == "source-eof":
                assert old_source.state.is_running() and old_source.run_count == 1
                assert client.read_stream(source.stream_id).state == "COMPLETED"
        death = dict(
            launcher_instance_id=uuid4(),
            child_process_id=first.pid,
            exit_code=first.returncode,
            observed_at=datetime.now(UTC),
            kind="WAITPID",
        )
        request, private = _takeover_request(launch, death)
        with server.client() as client:
            replacement = client.takeover_stream_execution(**request)
        context = StreamExecutionContext(
            original.id,
            replacement.generation,
            replacement.holder_id,
            server.url,
            request["new_holder_capability"],
            private,
        )
        # No source/operator TaskRun or channel IDs enter this new process.
        second = _start_flow(server, entrypoint, context, tmp_path / "attempt2.log")
        assert second.wait(timeout=240) == 0, (tmp_path / "attempt2.log").read_text()[
            -9000:
        ]
        with server.client() as client:
            assert client.read_flow_run(original.id).state.is_completed()
            assert client.read_flow_run(original.id).run_count == 2
            assert (
                client.read_stream_submission(
                    flow_run_id=original.id, submission_key="documents.ingest"
                )
                == source
            )
            assert (
                client.read_stream_submission(
                    flow_run_id=original.id, submission_key="documents.sign"
                )
                == operator
            )
            for manifest in [source, operator]:
                _poll(
                    lambda manifest=manifest: client.read_task_run(
                        manifest.task_run_id
                    ).state.is_completed(),
                    "Task event not reconciled",
                    20,
                )
                info = client.read_stream(manifest.stream_id)
                assert info.state == "COMPLETED" and info.record_count == 500
            assert client.read_task_run(operator.task_run_id).run_count == 2
            assert client.read_task_run(source.task_run_id).run_count == (
                2 if fault == "source-progress" else 1
            )
            assert (coordination / "source-future-state-id").read_text() == str(
                client.read_task_run(source.task_run_id).state.id
            )
        replay = [
            int(line)
            for line in (coordination / "operator-replay").read_text().splitlines()
        ]
        assert replay == list(range(before + 1, 501))
        expected_starts = ["1:0", "2:200"] if fault == "source-progress" else ["1:0"]
        assert (
            coordination / "source-starts"
        ).read_text().splitlines() == expected_starts
        async with db.session_context() as session:
            counts = (
                await session.execute(
                    sa.text(
                        "SELECT (SELECT count(*) FROM task_run WHERE flow_run_id=:flow_id), (SELECT count(*) FROM stream_partition WHERE flow_run_id=:flow_id), (SELECT count(*) FROM stream_submission WHERE flow_run_id=:flow_id)"
                    ),
                    {"flow_id": original.id},
                )
            ).one()
            assert tuple(counts) == (2, 2, 2)
            state_count = (
                await session.execute(
                    sa.text(
                        "SELECT count(*) FROM task_run_state WHERE task_run_id IN "
                        "(SELECT id FROM task_run WHERE flow_run_id=:flow_id)"
                    ),
                    {"flow_id": original.id},
                )
            ).scalar_one()
            assert 1 <= state_count <= 12

            cursor, managed = (
                await session.execute(
                    sa.text(
                        "SELECT completed_through, managed_state FROM stream_consumer WHERE output_stream_id=:stream_id"
                    ),
                    {"stream_id": operator.stream_id},
                )
            ).one()
            assert cursor == 500 and managed["processed"] == 500
            values = (
                (
                    await session.execute(
                        sa.text(
                            "SELECT value FROM stream_record WHERE stream_id=:stream_id ORDER BY sequence"
                        ),
                        {"stream_id": operator.stream_id},
                    )
                )
                .scalars()
                .all()
            )
        actual = []
        for reference in values:
            from urllib.parse import unquote, urlparse

            raw = Path(unquote(urlparse(reference["ref"]).path)).read_bytes()
            assert hashlib.sha256(raw).hexdigest() == reference["sha256"]
            actual.append(json.loads(raw)["number"])
        assert actual == list(range(1, 501))
        assert len(list((coordination / "signed").glob("*.json"))) == 500
    finally:
        for child in [first, second]:
            if child is not None and child.poll() is None:
                child.kill()
                child.wait(timeout=10)


@pytest.mark.timeout(240)
@pytest.mark.parametrize(
    "kind", ["construction-failure", "application-failure", "lost-seal-response"]
)
async def test_native_flow_failure_and_lost_seal_response(
    execution_api: RestartableAPI, tmp_path: Path, db: Any, kind: str
):
    server = execution_api
    dataset, coordination = tmp_path / "documents", tmp_path / "coordination"
    dataset.mkdir()
    coordination.mkdir()
    (coordination / "signed").mkdir()
    for number in range(1, 501):
        (dataset / f"{number:04}.json").write_text(
            json.dumps({"number": number, "text": f"Document {number}"})
        )
    entrypoint = (
        str(Path(__file__).parent / "fixtures" / "native_stream_failure_flow.py")
        + ":failure_flow"
    )
    definition = load_flow_from_entrypoint(entrypoint)
    with server.client() as client:
        run = client.create_flow_run(
            definition,
            parameters={
                "dataset": str(dataset),
                "coordination": str(coordination),
                "kind": kind,
            },
            state=Pending(),
        )
    launch = _launch(server, run.id)
    child = _start_flow(
        server, entrypoint, launch.context, tmp_path / "failure-control.log"
    )
    try:
        if kind != "construction-failure":
            _poll(
                lambda: _child_marker(
                    coordination / "source-held-open",
                    child,
                    tmp_path / "failure-control.log",
                ),
                "No early downstream consumption",
            )
            (coordination / "continue-source").touch()
        code = child.wait(timeout=180)
        # The ordinary entrypoint records application failure in FlowRun state;
        # its handled execution path can exit zero. State is authoritative below.
        assert code == 0, (tmp_path / "failure-control.log").read_text()[-7000:]
        with server.client() as client:
            observed = client.read_flow_run(run.id)
            assert observed.run_count == (3 if kind == "construction-failure" else 1)
            if kind == "lost-seal-response":
                assert observed.state.is_completed()
                ids = (coordination / "seal-operation-ids").read_text().splitlines()
                assert len(ids) == 2 and ids[0] == ids[1]
            else:
                assert observed.state.is_failed()
            for key in ["documents.ingest", "documents.sign"]:
                manifest = client.read_stream_submission(
                    flow_run_id=run.id, submission_key=key
                )
                task_run = client.read_task_run(manifest.task_run_id)
                info = client.read_stream(manifest.stream_id)
                if kind == "construction-failure":
                    assert task_run.run_count == 0 and task_run.state.is_failed()
                    assert info.record_count == 0 and info.state == "FAILED"
                else:
                    _poll(
                        lambda task_id=task_run.id: client.read_task_run(
                            task_id
                        ).state.is_completed(),
                        "Task completion event not recorded",
                        20,
                    )
                    assert (
                        task_run.run_count == 1
                        and info.record_count == 500
                        and info.state == "COMPLETED"
                    )
        if kind == "construction-failure":
            assert (coordination / "hook-observed-abort").exists()
            assert not (coordination / "source-starts").exists()
            async with db.session_context() as session:
                receipt = (
                    await session.execute(
                        sa.text(
                            "SELECT abort_operation_id,abort_outcome,abort_outcome_digest FROM stream_execution_attempt WHERE flow_run_id=:flow_id AND generation=1"
                        ),
                        {"flow_id": run.id},
                    )
                ).one()
                assert all(value is not None for value in receipt)
        else:
            assert len(list((coordination / "signed").glob("*.json"))) == 500
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)


@pytest.mark.parametrize(
    "overrides",
    [
        {"unexpected": "denied"},
        {"generation": True},
        {"flow_run_id": str(uuid4())},
        {"holder_capability": "invalid"},
        {"api_url": "https://example.invalid/api"},
    ],
)
async def test_invalid_private_bootstrap_cannot_start_flow_or_expose_credentials(
    execution_api: RestartableAPI, tmp_path: Path, db: Any, overrides: dict[str, Any]
):
    from tests.test_native_execution_http import _claim

    server = execution_api
    launch = _claim(server)
    log = tmp_path / "rejected-bootstrap.log"
    child = _start_flow(
        server, "missing.py:missing", launch.context, log, payload_overrides=overrides
    )
    try:
        assert child.wait(timeout=30) != 0
        text = log.read_text()
        assert "Invalid native execution bootstrap" in text
        assert launch.context.capability not in text
        assert launch.context.event_private_key.hex() not in text
        assert launch.claim["supervisor_capability"] not in text
        with server.client() as client:
            run = client.read_flow_run(launch.flow_run_id)
            assert run.state.is_pending() and run.run_count == 0
        async with db.session_context() as session:
            count = (
                await session.execute(
                    sa.text(
                        "SELECT count(*) FROM stream_submission WHERE flow_run_id=:flow_id"
                    ),
                    {"flow_id": run.id},
                )
            ).scalar_one()
            assert count == 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)


@pytest.mark.parametrize("kind", ["call", "submit", "map", "apply-async"])
async def test_finite_entrypoint_refuses_before_native_collection_can_start(
    execution_api: RestartableAPI, tmp_path: Path, db: Any, kind: str
):
    server = execution_api
    coordination = tmp_path / "coordination"
    coordination.mkdir()
    entrypoint = (
        str(Path(__file__).parent / "fixtures" / "native_stream_unsupported_flow.py")
        + ":unsupported_flow"
    )
    definition = load_flow_from_entrypoint(entrypoint)
    with server.client() as client:
        run = client.create_flow_run(
            definition,
            parameters={
                "dataset": str(tmp_path / "missing-documents"),
                "coordination": str(coordination),
                "kind": kind,
            },
            state=Pending(),
        )
    launch = _launch(server, run.id)
    log = tmp_path / "unsupported.log"
    child = _start_flow(server, entrypoint, launch.context, log)
    try:
        assert child.wait(timeout=40) == 0
        assert "only keyed submit_stream declarations" in log.read_text()
        assert not (coordination / "source-starts").exists()
        with server.client() as client:
            observed = client.read_flow_run(run.id)
            assert observed.state.is_failed() and observed.run_count == 2
            manifest = client.read_stream_submission(
                flow_run_id=run.id, submission_key="documents.ingest"
            )
            task_run = client.read_task_run(manifest.task_run_id)
            info = client.read_stream(manifest.stream_id)
            assert task_run.state.is_failed() and task_run.run_count == 0
            assert info.state == "FAILED" and info.record_count == 0
        async with db.session_context() as session:
            count = (
                await session.execute(
                    sa.text("SELECT count(*) FROM task_run WHERE flow_run_id=:flow_id"),
                    {"flow_id": run.id},
                )
            ).scalar_one()
            assert count == 1
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
