"""Exercise the built, stripped client wheel against a real native API."""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from zipfile import ZipFile

from tests.test_native_stream_http import hosted_api_server as hosted_api_server
from tests.test_native_stream_http import native_backend as native_backend


def test_stripped_client_wheel_executes_native_task(
    native_backend: None, hosted_api_server: str, tmp_path: Path
):
    repository = Path(__file__).resolve().parents[1]
    seed, stage, wheels = (tmp_path / name for name in ("seed", "stage", "wheels"))
    seed.mkdir()
    shutil.copytree(
        repository / "src/prefect",
        seed / "src/prefect",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    for directory in ("client", "tools"):
        shutil.copytree(
            repository / directory,
            seed / directory,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
    shutil.copy(repository / "LICENSE", seed / "LICENSE")
    environment = {**os.environ, "TMPDIR": str(stage)}
    stage.mkdir()
    prepared = subprocess.run(
        ["bash", str(repository / "client/build_client.sh")],
        cwd=seed,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert prepared.returncode == 0, prepared.stdout + prepared.stderr
    built = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(wheels), str(stage)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert built.returncode == 0, built.stdout + built.stderr
    (wheel,) = wheels.glob("*.whl")
    with ZipFile(wheel) as archive:
        files = archive.namelist()
        assert "prefect/client/stream_storage.py" in files
        assert "prefect/client/schemas/streams.py" in files
        assert not any(name.startswith("prefect/server/models/") for name in files)
        assert not any(name.startswith("prefect/server/database/") for name in files)
    script = tmp_path / "native_client_smoke.py"
    script.write_text(
        """
import asyncio
from pathlib import Path
from uuid import uuid4
import prefect
from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.client.stream_storage import PrefectStreamStorage
from prefect.context import FlowRunContext
from prefect.states import Completed, Running
from prefect.streams import Stream, StreamTaskConfig, StreamSourceConfig, SourceRecord
from prefect.utilities.asyncutils import run_coro_as_sync

assert not (Path(prefect.__file__).parent / 'server/models').exists()
assert not (Path(prefect.__file__).parent / 'server/database').exists()

@task(stream=StreamTaskConfig(input='document'))
async def sign(document, *, ctx):
    ctx.managed_state['count'] = ctx.managed_state.get('count', 0) + 1
    yield {'document': document, 'signed': True}

@flow
def pipeline():
    flow_id = FlowRunContext.get().flow_run.id
    with get_client(sync_client=True) as client:
        source = client.create_task_run(task(lambda: None), flow_id, uuid4().hex, state=Running())
        stream_id = uuid4()
        client.create_stream(flow_run_id=flow_id, stream_id=stream_id)
        binding = client.claim_stream_source(stream_id=stream_id, task_run_id=source.id, owner_id=uuid4(), attempt=1)
        for position, value in enumerate([200, 201], 1):
            client.append_stream_source(**binding.model_dump(), position=position, value=value)
        client.finish_stream_source(**binding.model_dump())
        client.set_task_run_state(source.id, Completed())
    output = sign.submit_stream(Stream(PrefectStreamStorage(flow_id), str(stream_id), producer_task_run_id=source.id))
    output.wait()
    return run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10))

assert pipeline() == [{'document': 200, 'signed': True}, {'document': 201, 'signed': True}]

@task(stream=StreamSourceConfig())
async def native_source(*, ctx):
    for position, document in enumerate([200, 201], 1):
        if position > ctx.accepted_source_position:
            yield SourceRecord(position, document)

@flow
def native_source_pipeline():
    incoming = native_source.submit_stream()
    output = sign.submit_stream(incoming)
    output.wait()
    assert incoming.wait().accepted_source_position == 2
    return run_coro_as_sync(output.storage.read_prefix(output.stream_id, 10))

assert native_source_pipeline() == [{'document': 200, 'signed': True}, {'document': 201, 'signed': True}]
print('STRIPPED_NATIVE_CLIENT_PASS')
""",
        encoding="utf-8",
    )
    child_environment = {
        **os.environ,
        "PREFECT_API_URL": hosted_api_server,
        "PREFECT_SERVER_EPHEMERAL_ENABLED": "false",
    }
    child_environment.pop("PYTHONPATH", None)
    child_environment.pop("PREFECT_SERVER_DATABASE_CONNECTION_URL", None)
    child_environment.pop("PREFECT_API_DATABASE_CONNECTION_URL", None)
    result = subprocess.run(
        [
            "uv",
            "run",
            "--isolated",
            "--no-project",
            "--python",
            sys.executable,
            "--with",
            str(wheel),
            str(script),
        ],
        cwd=tmp_path,
        env=child_environment,
        capture_output=True,
        text=True,
        timeout=150,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "STRIPPED_NATIVE_CLIENT_PASS" in result.stdout
