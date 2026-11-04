"""Finite-task authority on real HTTP/PostgreSQL; mixed SDK execution follows."""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa

from prefect._internal.stream_execution import (
    StreamExecutionContext,
    stream_execution_context,
)
from prefect.client.orchestration import PrefectClient
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.states import Completed, Failed, Running
from tests.test_native_execution_http import Launch, _claim, _start, _takeover_request
from tests.test_native_execution_http import execution_api as execution_api
from tests.test_native_execution_recovery_http import _publish, _signed_event
from tests.test_native_stream_http import native_backend as native_backend
from tests.test_native_stream_recovery_http import RestartableAPI


def _verify_receipt(manifest: Any, receipt: Any) -> None:
    def digest(value: Any) -> str:
        encoded = json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    assert receipt.result_digest == digest(receipt.result)
    assert receipt.outcome_digest == digest(
        {
            "schema": "native-finite-receipt-outcome-v1",
            "flow_run_id": str(receipt.flow_run_id),
            "phase": receipt.phase,
            "submission_key": receipt.submission_key,
            "manifest_digest": manifest.manifest_digest,
            "task_run_id": str(receipt.task_run_id),
            "operation_id": str(receipt.operation_id),
            "run_count": receipt.run_count,
            "result_digest": receipt.result_digest,
        }
    )


def _request(launch: Launch) -> dict[str, Any]:
    return {
        "flow_run_id": launch.flow_run_id,
        "phase": "PREFIX",
        "submission_key": "documents.prepare",
        "task_name": "prepare",
        "task_key": "prepare-v1",
        "artifact_digest": "a" * 64,
        "task_version": "generation-v1",
        "flow_version": "generation-v1",
        "parameters": {"ref": "object://documents/configuration"},
        "task_policy": {"retries": 2},
    }


def _prefix(server: RestartableAPI) -> tuple[Launch, Any]:
    launch = _claim(server)
    with stream_execution_context(launch.context), server.client() as client:
        assert client.set_flow_run_state(
            launch.flow_run_id, Running()
        ).state.is_running()
        manifest = client.reserve_stream_finite_submission(**_request(launch))
    launch.context.register_manifest(manifest.task_run_id, manifest.manifest_digest)
    return launch, manifest


def _running(server: RestartableAPI, launch: Launch, manifest: Any) -> None:
    with stream_execution_context(launch.context), server.client() as client:
        assert client.set_task_run_state(
            manifest.task_run_id, Running()
        ).state.is_running()
        assert client.read_task_run(manifest.task_run_id).run_count == 1


async def test_finite_reservation_is_exact_without_allocating_channels(
    execution_api: RestartableAPI, db: Any
):
    server = execution_api
    launch, original = _prefix(server)

    def reserve() -> Any:
        with stream_execution_context(launch.context), server.client() as client:
            return client.reserve_stream_finite_submission(**_request(launch))

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        copies = list(pool.map(lambda _: reserve(), range(8)))
    assert copies == [original] * 8
    async with db.session_context() as session:
        for table, expected in (("task_run", 1), ("stream_partition", 0)):
            count = await session.scalar(
                sa.text(f"SELECT count(*) FROM {table} WHERE flow_run_id=:flow"),
                {"flow": launch.flow_run_id},
            )
            assert count == expected


@pytest.mark.parametrize(
    "field", ["parameters", "task_version", "artifact_digest", "phase"]
)
def test_finite_identity_drift_refuses_without_replacing_the_task(
    execution_api: RestartableAPI, field: str
):
    launch, original = _prefix(execution_api)
    request = _request(launch)
    request[field] = {
        "parameters": {"ref": "object://different/configuration"},
        "task_version": "changed-v2",
        "artifact_digest": "b" * 64,
        "phase": "TAIL",
    }[field]
    with stream_execution_context(launch.context), execution_api.client() as client:
        with pytest.raises(StreamRequestError) as error:
            client.reserve_stream_finite_submission(**request)
        assert error.value.code == "IDENTITY_CONFLICT"
        assert (
            client.read_stream_finite_submission(
                flow_run_id=launch.flow_run_id,
                submission_key=original.submission_key,
            )
            == original
        )
        task = client.read_task_run(original.task_run_id)
        assert task.run_count == 0 and task.state.is_pending()


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_finite_completion_survives_api_crash_and_exact_replay(
    execution_api: RestartableAPI, mode: str
):
    server = execution_api
    launch, manifest = _prefix(server)
    _running(server, launch, manifest)
    arguments = {
        "flow_run_id": launch.flow_run_id,
        "task_run_id": manifest.task_run_id,
        "operation_id": uuid4(),
        "run_count": 1,
        "result": {"ref": "object://documents/configuration", "sha256": "c" * 64},
    }
    with stream_execution_context(launch.context):
        if mode == "async":
            async with PrefectClient(server.url, auth_string=server.auth) as client:
                accepted = await client.complete_stream_finite_task(**arguments)
        else:
            with server.client() as client:
                accepted = client.complete_stream_finite_task(**arguments)
    _verify_receipt(manifest, accepted)
    server.restart()
    with stream_execution_context(launch.context), server.client() as client:
        assert client.complete_stream_finite_task(**arguments) == accepted
        assert (
            client.read_stream_finite_receipt(task_run_id=manifest.task_run_id)
            == accepted
        )
        assert client.read_task_run(manifest.task_run_id).state.is_running()
        assert client.set_task_run_state(
            manifest.task_run_id, Completed()
        ).state.is_completed()


def test_finite_completion_requires_receipt_before_task_success(
    execution_api: RestartableAPI,
):
    launch, manifest = _prefix(execution_api)
    _running(execution_api, launch, manifest)
    with stream_execution_context(launch.context), execution_api.client() as client:
        with pytest.raises(httpx.HTTPStatusError) as error:
            client.set_task_run_state(manifest.task_run_id, Completed())
        assert error.value.response.json()["detail"]["code"] == "TERMINAL_CONFLICT"
        assert client.read_task_run(manifest.task_run_id).state.is_running()


@pytest.mark.parametrize("field", ["result", "run_count", "operation_id"])
def test_finite_receipt_conflict_cannot_replace_durable_result(
    execution_api: RestartableAPI, field: str
):
    launch, manifest = _prefix(execution_api)
    _running(execution_api, launch, manifest)
    original = dict(
        flow_run_id=launch.flow_run_id,
        task_run_id=manifest.task_run_id,
        operation_id=uuid4(),
        run_count=1,
        result={"prepared": True},
    )
    with stream_execution_context(launch.context), execution_api.client() as client:
        accepted = client.complete_stream_finite_task(**original)
        changed = {
            **original,
            field: {
                "result": {"prepared": False},
                "run_count": 2,
                "operation_id": uuid4(),
            }[field],
        }
        with pytest.raises(StreamRequestError):
            client.complete_stream_finite_task(**changed)
        assert (
            client.read_stream_finite_receipt(task_run_id=manifest.task_run_id)
            == accepted
        )


@pytest.mark.timeout(120)
@pytest.mark.parametrize("result", [{"ref": "object://documents/prepared"}, None])
def test_prefix_receipt_and_task_identity_survive_real_client_process_loss(
    execution_api: RestartableAPI, tmp_path: Path, result: Any
):
    server = execution_api
    launch, manifest = _prefix(server)
    _running(server, launch, manifest)
    operation = uuid4()
    payload = {
        "flow_run_id": str(launch.flow_run_id),
        "task_run_id": str(manifest.task_run_id),
        "operation_id": str(operation),
        "holder_id": str(launch.context.holder_id),
        "capability": launch.context.capability,
        "event_private_key": launch.context.event_private_key.hex(),
        "api_url": server.url,
        "auth": server.auth,
        "result": result,
    }
    script = """
import json,os,sys
from uuid import UUID
from prefect._internal.stream_execution import StreamExecutionContext,stream_execution_context
from prefect.client.orchestration import SyncPrefectClient
p=json.load(sys.stdin)
c=StreamExecutionContext(UUID(p['flow_run_id']),1,UUID(p['holder_id']),p['api_url'],p['capability'],bytes.fromhex(p['event_private_key']))
with stream_execution_context(c),SyncPrefectClient(p['api_url'],auth_string=p['auth']) as client:
    client.complete_stream_finite_task(flow_run_id=c.flow_run_id,task_run_id=UUID(p['task_run_id']),operation_id=UUID(p['operation_id']),run_count=1,result=p['result'])
print('RECEIPT_ACCEPTED',flush=True)
os._exit(77)
"""
    environment = dict(os.environ)
    for name in (
        "PREFECT_SERVER_DATABASE_CONNECTION_URL",
        "PREFECT_API_DATABASE_CONNECTION_URL",
    ):
        environment.pop(name, None)
    log = tmp_path / "finite-client.log"
    with log.open("wb") as output:
        child = subprocess.Popen(
            [sys.executable, "-c", script],
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=environment,
        )
        child.communicate(json.dumps(payload).encode(), timeout=60)
    assert child.returncode == 77 and "RECEIPT_ACCEPTED" in log.read_text()
    assert launch.context.capability not in log.read_text()
    server.restart()
    with stream_execution_context(launch.context), server.client() as client:
        original = client.read_stream_finite_receipt(task_run_id=manifest.task_run_id)
        assert original.result == result
        _verify_receipt(manifest, original)
        assert client.read_task_run(manifest.task_run_id).state.is_running()
    request, private = _takeover_request(
        launch,
        {
            "launcher_instance_id": uuid4(),
            "child_process_id": child.pid,
            "exit_code": child.returncode,
            "observed_at": datetime.now(UTC),
            "kind": "WAITPID",
        },
    )
    with server.client() as client:
        replacement = client.takeover_stream_execution(**request)
    current = StreamExecutionContext(
        launch.flow_run_id,
        replacement.generation,
        replacement.holder_id,
        server.url,
        request["new_holder_capability"],
        private,
    )
    current.register_manifest(manifest.task_run_id, manifest.manifest_digest)
    with stream_execution_context(current), server.client() as client:
        client.prepare_stream_recovery(
            flow_run_id=launch.flow_run_id,
            operation_id=uuid4(),
            manifests=[],
            finite_manifests=[
                {
                    "submission_key": manifest.submission_key,
                    "manifest_digest": manifest.manifest_digest,
                    "phase": manifest.phase,
                }
            ],
        )
        assert client.read_task_run(manifest.task_run_id).state.is_completed()
        assert (
            client.read_stream_finite_receipt(task_run_id=manifest.task_run_id)
            == original
        )
        assert (
            client.read_stream_finite_submission(
                flow_run_id=launch.flow_run_id, submission_key=manifest.submission_key
            ).task_run_id
            == manifest.task_run_id
        )
    with stream_execution_context(launch.context), server.client() as client:
        with pytest.raises(StreamRequestError) as error:
            client.complete_stream_finite_task(
                flow_run_id=launch.flow_run_id,
                task_run_id=manifest.task_run_id,
                operation_id=operation,
                run_count=1,
                result=original.result,
            )
        assert error.value.code == "STALE_EXECUTION"


async def test_finite_events_from_a_live_old_holder_are_fenced_by_actual_recorder(
    execution_api: RestartableAPI, db: Any
):
    server = execution_api
    launch, manifest = _prefix(server)
    _running(server, launch, manifest)
    with stream_execution_context(launch.context), server.client() as client:
        old_task = client.read_task_run(manifest.task_run_id)
    stale_state = Completed(timestamp=datetime.now(UTC) + timedelta(days=1))
    stale_event = _signed_event(old_task, stale_state, launch.context)
    request, private = _takeover_request(launch)
    with server.client() as client:
        replacement = client.takeover_stream_execution(**request)
    current = StreamExecutionContext(
        launch.flow_run_id,
        replacement.generation,
        replacement.holder_id,
        server.url,
        request["new_holder_capability"],
        private,
    )
    current.register_manifest(manifest.task_run_id, manifest.manifest_digest)
    with stream_execution_context(current), server.client() as client:
        prepared = client.prepare_stream_recovery(
            flow_run_id=launch.flow_run_id,
            operation_id=uuid4(),
            manifests=[],
            finite_manifests=[
                {
                    "submission_key": manifest.submission_key,
                    "manifest_digest": manifest.manifest_digest,
                    "phase": "PREFIX",
                }
            ],
        )
        assert prepared.finite_items[0].next_run_count == 2
        assert client.set_flow_run_state(
            launch.flow_run_id, Running()
        ).state.is_running()
        assert client.set_task_run_state(
            manifest.task_run_id, Running()
        ).state.is_running()
        active = client.read_task_run(manifest.task_run_id)
        assert active.run_count == 2
        client.complete_stream_finite_task(
            flow_run_id=launch.flow_run_id,
            task_run_id=manifest.task_run_id,
            operation_id=uuid4(),
            run_count=2,
            result={"prepared": True},
        )
    accepted_state = Completed()
    accepted_event = _signed_event(active, accepted_state, current)
    late_failure = _signed_event(
        active, Failed(timestamp=datetime.now(UTC) + timedelta(days=2)), current
    )
    _publish(server, stale_event)
    _publish(server, late_failure)
    _publish(server, accepted_event)
    deadline = time.monotonic() + 30
    with server.client() as client:
        while True:
            task = client.read_task_run(manifest.task_run_id)
            if task.state.id == accepted_state.id:
                break
            assert time.monotonic() < deadline, task
            time.sleep(0.05)
        assert task.run_count == 2 and task.state.is_completed()
    async with db.session_context() as session:
        assert (
            await session.scalar(
                sa.text("SELECT count(*) FROM task_run_state WHERE id=:state_id"),
                {"state_id": stale_state.id},
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.text("SELECT count(*) FROM task_run_state WHERE id=:state_id"),
                {"state_id": late_failure.id},
            )
            == 0
        )


def test_tail_admission_and_flow_success_require_durable_stream_and_finite_results(
    execution_api: RestartableAPI,
):
    server = execution_api
    launch, prefix = _prefix(server)
    _running(server, launch, prefix)
    with stream_execution_context(launch.context), server.client() as client:
        receipt = client.complete_stream_finite_task(
            flow_run_id=launch.flow_run_id,
            task_run_id=prefix.task_run_id,
            operation_id=uuid4(),
            run_count=1,
            result={"prepared": True},
        )
        assert client.set_task_run_state(
            prefix.task_run_id, Completed()
        ).state.is_completed()
        source = client.reserve_stream_submission(
            flow_run_id=launch.flow_run_id,
            submission_key="documents.ingest",
            role="SOURCE",
            protocol="source-v1",
            task_name="ingest",
            task_key="ingest-v1",
            task_version="generation-v1",
            flow_version="generation-v1",
            parameters={"ref": "object://documents"},
            task_policy={"retries": 2},
        )
        witnesses = [
            {
                "submission_key": source.submission_key,
                "manifest_digest": source.manifest_digest,
            }
        ]
        with pytest.raises(StreamRequestError):
            client.seal_stream_manifest(
                flow_run_id=launch.flow_run_id,
                operation_id=uuid4(),
                manifests=witnesses,
            )
        client.seal_stream_manifest(
            flow_run_id=launch.flow_run_id,
            operation_id=uuid4(),
            manifests=witnesses,
            finite_prefix={
                "submission_key": prefix.submission_key,
                "manifest_digest": prefix.manifest_digest,
                "phase": "PREFIX",
                "task_run_id": str(prefix.task_run_id),
                "result_digest": receipt.result_digest,
            },
        )
        assert client.set_task_run_state(
            source.task_run_id, Running()
        ).state.is_running()
        binding = client.open_stream_source(
            stream_id=source.stream_id,
            task_run_id=source.task_run_id,
            owner_id=uuid4(),
            attempt=1,
        ).binding
        client.append_stream_source(
            **binding.model_dump(), position=1, value={"document": 200}
        )
        tail_request = {
            **_request(launch),
            "phase": "TAIL",
            "submission_key": "documents.summary",
            "task_name": "summary",
            "task_key": "summary-v1",
            "parameters": {"stream_id": str(source.stream_id)},
        }
        with pytest.raises(StreamRequestError):
            client.reserve_stream_finite_submission(**tail_request)
        client.close_stream_source(**binding.model_dump(), expected_position=1)
        assert client.set_task_run_state(
            source.task_run_id, Completed()
        ).state.is_completed()
        with pytest.raises(httpx.HTTPStatusError):
            client.set_flow_run_state(launch.flow_run_id, Completed())
        tail = client.reserve_stream_finite_submission(**tail_request)
        assert client.set_task_run_state(tail.task_run_id, Running()).state.is_running()
        with pytest.raises(httpx.HTTPStatusError):
            client.set_task_run_state(tail.task_run_id, Completed())
        final = client.complete_stream_finite_task(
            flow_run_id=launch.flow_run_id,
            task_run_id=tail.task_run_id,
            operation_id=uuid4(),
            run_count=1,
            result={"processed": 1},
        )
        assert final.phase == "TAIL"
        assert client.set_task_run_state(
            tail.task_run_id, Completed()
        ).state.is_completed()
        assert client.set_flow_run_state(
            launch.flow_run_id, Completed()
        ).state.is_completed()


def test_durable_finite_receipt_prevents_late_failure_from_advancing_attempt(
    execution_api: RestartableAPI,
):
    launch, manifest = _prefix(execution_api)
    _running(execution_api, launch, manifest)
    with stream_execution_context(launch.context), execution_api.client() as client:
        accepted = client.complete_stream_finite_task(
            flow_run_id=launch.flow_run_id,
            task_run_id=manifest.task_run_id,
            operation_id=uuid4(),
            run_count=1,
            result=None,
        )
        with pytest.raises(httpx.HTTPStatusError) as error:
            client.set_task_run_state(
                manifest.task_run_id,
                Failed(message="Completion acknowledgement was lost"),
            )
        assert error.value.response.json()["detail"]["code"] == "TERMINAL_CONFLICT"
        task = client.read_task_run(manifest.task_run_id)
        assert task.run_count == 1 and task.state.is_running()
        assert (
            client.read_stream_finite_receipt(task_run_id=manifest.task_run_id)
            == accepted
        )
        assert client.set_task_run_state(
            manifest.task_run_id, Completed()
        ).state.is_completed()


def test_stream_only_recovery_omits_finite_extension_on_first_response_and_replay(
    execution_api: RestartableAPI,
):
    server = execution_api
    launch = _start(server)
    request, private = _takeover_request(launch)
    with server.client() as client:
        replacement = client.takeover_stream_execution(**request)
    current = StreamExecutionContext(
        launch.flow_run_id,
        replacement.generation,
        replacement.holder_id,
        server.url,
        request["new_holder_capability"],
        private,
    )
    arguments = {
        "flow_run_id": launch.flow_run_id,
        "operation_id": uuid4(),
        "manifests": [
            {
                "submission_key": launch.manifest.submission_key,
                "manifest_digest": launch.manifest.manifest_digest,
            }
        ],
    }
    with stream_execution_context(current), server.client() as client:
        first = client.prepare_stream_recovery(**arguments)
        assert "finite_items" not in first.model_fields_set
    server.restart()
    with stream_execution_context(current), server.client() as client:
        replay = client.prepare_stream_recovery(**arguments)
        assert replay == first
        assert "finite_items" not in replay.model_fields_set
