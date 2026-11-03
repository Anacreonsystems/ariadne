"""Real document references, ordinary Prefect Tasks and Flow."""

import hashlib
import json
import os
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

from prefect import flow, task
from prefect.streams import SourceRecord, StreamSourceConfig, StreamTaskConfig
from prefect.task_runners import ThreadPoolTaskRunner


def wait_for(path):
    deadline = time.monotonic() + 90
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Document acceptance synchronization expired")
        time.sleep(0.02)


@task(version="documents-v1", retries=2, stream=StreamSourceConfig())
def petya(ctx, dataset, coordination, fault):
    directory = Path(coordination)
    with (directory / "source-starts").open("a") as log:
        log.write(f"{ctx.attempt}:{ctx.accepted_source_position}\n")
    for position in range(ctx.accepted_source_position + 1, 501):
        path = Path(dataset) / f"{position:04}.json"
        raw = path.read_bytes()
        yield SourceRecord(
            position, {"ref": path.as_uri(), "sha256": hashlib.sha256(raw).hexdigest()}
        )
        if position == 1 and ctx.attempt == 1:
            wait_for(directory / "operator-saw-first")
            (directory / "source-held-open").touch()
            wait_for(directory / "continue-source")
        if position == 200 and ctx.attempt == 1 and fault == "source-eof":
            wait_for(directory / "operator-at-200")
        if position == 200 and ctx.attempt == 1 and fault == "source-progress":
            os._exit(77)


@task(version="documents-v1", retries=2, stream=StreamTaskConfig(input="document"))
def anya(ctx, document, coordination, fault):
    directory = Path(coordination)
    raw = Path(unquote(urlparse(document["ref"]).path)).read_bytes()
    if hashlib.sha256(raw).hexdigest() != document["sha256"]:
        raise ValueError("Document content does not match its reference")
    content = json.loads(raw)
    position = content["number"]
    if position == 1:
        (directory / "operator-saw-first").touch()
    if ctx.attempt > 1:
        with (directory / "operator-replay").open("a") as log:
            log.write(f"{position}\n")
    # Idempotent external file effect is deliberately implemented by the client.
    result = json.dumps({**content, "signed_by": "anya"}, sort_keys=True).encode()
    target = directory / "signed" / f"{position:04}.json"
    target.write_bytes(result)
    ctx.managed_state["processed"] = ctx.managed_state.get("processed", 0) + 1
    reference = {"ref": target.as_uri(), "sha256": hashlib.sha256(result).hexdigest()}
    if position == 200 and ctx.attempt == 1 and fault == "source-eof":
        (directory / "operator-at-200").touch()
        wait_for(directory / "terminate-at-200")
    if (
        position == 200
        and ctx.attempt == 1
        and fault in {"before-yield", "after-yield"}
    ):
        (directory / "operator-at-200").touch()
        wait_for(directory / "terminate-at-200")
        if fault == "before-yield":
            os._exit(77)
        try:
            yield reference
        finally:
            # The engine closes the generator only after durable YIELD acceptance.
            os._exit(77)
    else:
        yield reference


@flow(
    version="documents-v1", retries=2, task_runner=ThreadPoolTaskRunner(max_workers=2)
)
def documents_flow(dataset, coordination, fault):
    if fault == "source-eof":
        from prefect._internal.stream_execution import get_stream_execution
        from prefect.client.orchestration import SyncPrefectClient

        if get_stream_execution().generation == 1:
            original_close = SyncPrefectClient.close_stream_source

            def close_and_lose_response(self, **kwargs):
                original_close(self, **kwargs)
                os._exit(77)

            SyncPrefectClient.close_stream_source = close_and_lose_response
    source = petya.submit_stream(
        dataset, coordination, fault, submission_key="documents.ingest"
    )
    output = anya.submit_stream(
        source, coordination, fault, submission_key="documents.sign"
    )
    output.wait()
    assert source.wait().accepted_source_position == 500
    assert source.completion is not None
    Path(coordination, "source-future-state-id").write_text(
        str(source.completion.state.id)
    )
    if fault == "application-failure":
        raise ValueError("Post-admission application failure")
    return "documents-complete"
