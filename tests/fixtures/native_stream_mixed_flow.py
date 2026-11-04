"""Ordinary preparation and summary around actual native document Tasks."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from prefect import flow, task
from prefect._internal.stream_execution import get_stream_execution
from prefect.client.orchestration import SyncPrefectClient
from prefect.streams import (
    NativeStreamFlowConfig,
    SourceContext,
    SourceRecord,
    StreamContext,
    StreamSourceConfig,
    StreamTaskConfig,
)
from prefect.task_runners import ThreadPoolTaskRunner


def _log(directory: str, name: str, value: str) -> None:
    with Path(directory, name).open("a") as file:
        file.write(value + "\n")


@task(version="mixed-v1", retries=2, stream_key="documents.prepare")
def prepare(dataset: str, coordination: str, fault: str, count: int) -> dict[str, Any]:
    _log(coordination, "prefix-bodies", "prepare")
    if fault == "prefix-before" and get_stream_execution().generation == 1:
        os._exit(77)
    return {
        "dataset": dataset,
        "coordination": coordination,
        "fault": fault,
        "count": count,
    }


@task(version="mixed-v1", retries=2, stream=StreamSourceConfig())
def ingest(
    ctx: SourceContext, config: dict[str, Any]
) -> Generator[SourceRecord[dict[str, str]], None, None]:
    _log(config["coordination"], "source-bodies", str(ctx.accepted_source_position))
    for position in range(ctx.accepted_source_position + 1, config["count"] + 1):
        if config["fault"] == "slow":
            time.sleep(0.01)
        if (
            position == 201
            and config["fault"] in {"orphan", "drain", "cancel"}
            and get_stream_execution().generation == 1
        ):
            directory = Path(config["coordination"])
            (directory / "source-paused").write_text(str(os.getpid()))
            while not (directory / "release").exists():
                time.sleep(0.02)
        path = Path(config["dataset"], f"{position:04}.json")
        raw = path.read_bytes()
        yield SourceRecord(
            position, {"ref": path.as_uri(), "sha256": hashlib.sha256(raw).hexdigest()}
        )


@task(version="mixed-v1", retries=2, stream=StreamTaskConfig(input="document"))
def sign(
    ctx: StreamContext, document: dict[str, Any], coordination: str, fault: str
) -> Generator[dict[str, str], None, None]:
    raw = Path(unquote(urlparse(document["ref"]).path)).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == document["sha256"]
    value = json.loads(raw)
    position = value["number"]
    _log(coordination, "operator-inputs", str(position))
    if position == 200 and (
        (ctx.attempt == 1 and fault == "before-yield") or fault == "always-before-yield"
    ):
        os._exit(77)
    # The task author supplies idempotence for its external file writes.
    target = Path(coordination, "signed", f"{position:04}.json")
    content = json.dumps({**value, "signed_by": "anya"}, sort_keys=True).encode()
    target.write_bytes(content)
    ctx.managed_state["processed"] = ctx.managed_state.get("processed", 0) + 1
    try:
        yield {"ref": target.as_uri(), "sha256": hashlib.sha256(content).hexdigest()}
    finally:
        if position == 200 and ctx.attempt == 1 and fault == "after-yield":
            os._exit(77)


@task(version="mixed-v1", retries=2, stream_key="documents.summary")
def summarize(coordination: str, count: int) -> dict[str, Any]:
    _log(coordination, "tail-bodies", "summarize")
    paths = sorted(Path(coordination, "signed").glob("*.json"))
    assert len(paths) == count
    digest = hashlib.sha256(b"".join(path.read_bytes() for path in paths)).hexdigest()
    return {"processed": count, "sha256": digest}


@flow(version="mixed-v1", retries=2, task_runner=ThreadPoolTaskRunner(max_workers=2))
def mixed_flow(
    dataset: str, coordination: str, fault: str = "none", count: int = 8
) -> dict[str, Any]:
    if (
        fault in {"prefix-receipt", "tail-receipt"}
        and get_stream_execution().generation == 1
    ):
        original = SyncPrefectClient.complete_stream_finite_task

        def complete_and_lose_response(self: Any, **kwargs: Any) -> Any:
            receipt = original(self, **kwargs)
            if receipt.phase == ("PREFIX" if fault == "prefix-receipt" else "TAIL"):
                os._exit(77)
            return receipt

        SyncPrefectClient.complete_stream_finite_task = complete_and_lose_response
    config = prepare(dataset, coordination, fault, count)
    source = ingest.submit_stream(config, submission_key="documents.ingest")
    output = sign.submit_stream(
        source, coordination, fault, submission_key="documents.sign"
    )
    output.wait()
    return summarize(coordination, count)


@task(version="mixed-v1", retries=2, stream_key="documents.prepare")
async def async_prepare(
    dataset: str, coordination: str, fault: str, count: int
) -> dict[str, Any]:
    return prepare.fn(dataset, coordination, fault, count)


@task(version="mixed-v1", retries=2, stream=StreamSourceConfig())
async def async_ingest(
    ctx: SourceContext, config: dict[str, Any]
) -> AsyncGenerator[SourceRecord[dict[str, str]], None]:
    for record in ingest.fn(ctx, config):
        yield record


@task(version="mixed-v1", retries=2, stream=StreamTaskConfig(input="document"))
async def async_sign(
    ctx: StreamContext, document: dict[str, Any], coordination: str, fault: str
) -> AsyncGenerator[dict[str, str], None]:
    for record in sign.fn(ctx, document, coordination, fault):
        yield record


@task(version="mixed-v1", retries=2, stream_key="documents.summary")
async def async_summarize(coordination: str, count: int) -> dict[str, Any]:
    return summarize.fn(coordination, count)


@flow(version="mixed-v1", retries=2, task_runner=ThreadPoolTaskRunner(max_workers=2))
async def async_mixed_flow(
    dataset: str, coordination: str, fault: str = "none", count: int = 8
) -> dict[str, Any]:
    config = await async_prepare(dataset, coordination, fault, count)
    source = async_ingest.submit_stream(config, submission_key="documents.ingest")
    output = async_sign.submit_stream(
        source, coordination, fault, submission_key="documents.sign"
    )
    output.wait()
    return await async_summarize(coordination, count)


@task(version="mixed-v1", retries=2, stream_key="documents.summary")
def collection_summary(values: list[dict[str, str]], count: int) -> dict[str, Any]:
    assert len(values) == count
    return {"processed": len(values)}


@flow(version="mixed-v1", retries=2, task_runner=ThreadPoolTaskRunner(max_workers=2))
def collection_flow(
    dataset: str, coordination: str, count: int = 500
) -> dict[str, Any]:
    config = prepare(dataset, coordination, "none", count)
    source = ingest.submit_stream(config, submission_key="documents.ingest")
    output = sign.submit_stream(
        source, coordination, "none", submission_key="documents.sign"
    )
    output.wait()
    return collection_summary(output.collect(limit=count), count)


@flow(version="mixed-v1", retries=2, task_runner=ThreadPoolTaskRunner(max_workers=2))
def partial_collection_flow(dataset: str, coordination: str) -> dict[str, Any]:
    config = prepare(dataset, coordination, "slow", 500)
    source = ingest.submit_stream(config, submission_key="documents.ingest")
    output = sign.submit_stream(
        source, coordination, "slow", submission_key="documents.sign"
    )
    # A bounded prefix is available before EOF. It cannot admit a finite tail.
    return collection_summary(output.collect(limit=2), 2)


@task(version="mixed-v1")
def unkeyed() -> int:
    raise AssertionError("An unkeyed task must never enter user code")


@task(version="mixed-v1", retries=2, stream_key="documents.prepare")
def invalid_result() -> set[int]:
    return {1, 2}


@flow(version="mixed-v1", retries=2, task_runner=ThreadPoolTaskRunner(max_workers=2))
def invalid_flow(dataset: str, coordination: str, fault: str) -> Any:
    if fault == "missing-key":
        return unkeyed()
    if fault == "submit":
        return prepare.submit(dataset, coordination, fault, 8)
    if fault == "invalid-result":
        return invalid_result()
    config = prepare(dataset, coordination, fault, 8)
    ingest.submit_stream(config, submission_key="documents.ingest")
    return summarize(coordination, 8)


@flow(
    version="native-worker-v1",
    retries=2,
    task_runner=ThreadPoolTaskRunner(max_workers=2),
    streaming=NativeStreamFlowConfig(),
)
def deployed_mixed_flow(
    dataset: str, coordination: str, count: int = 8, fault: str = "none"
) -> dict[str, Any]:
    if fault in {"orphan", "drain", "cancel"}:
        config = prepare(dataset, coordination, fault, count)
        source = ingest.submit_stream(config, submission_key="documents.ingest")
        output = sign.submit_stream(
            source, coordination, fault, submission_key="documents.sign"
        )
        output.wait()
        accepted = source.wait().accepted_source_position
        return summarize(coordination, accepted)
    return mixed_flow.fn(dataset, coordination, fault, count)


@flow(
    version="native-worker-v1",
    retries=2,
    task_runner=ThreadPoolTaskRunner(max_workers=2),
    streaming=NativeStreamFlowConfig(),
)
async def deployed_async_mixed_flow(
    dataset: str, coordination: str, count: int = 500, fault: str = "none"
) -> dict[str, Any]:
    return await async_mixed_flow.fn(dataset, coordination, fault, count)


@flow(
    version="native-worker-v1",
    retries=2,
    task_runner=ThreadPoolTaskRunner(max_workers=2),
    streaming=NativeStreamFlowConfig(),
)
def deployed_pre_manifest_failure_flow(
    dataset: str, coordination: str, count: int = 500, fault: str = "none"
) -> None:
    """Exit after the real Flow Running transition but before any Task reserve."""
    Path(coordination, "pre-manifest-exit").write_text(str(os.getpid()))
    os._exit(77)
