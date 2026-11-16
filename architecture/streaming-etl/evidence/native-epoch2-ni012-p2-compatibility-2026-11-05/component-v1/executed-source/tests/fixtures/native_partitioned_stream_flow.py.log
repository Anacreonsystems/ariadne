"""Versioned fixed-P2 Tasks used by real ProcessWorker acceptance."""

import asyncio
import atexit
import json
import os
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx

from prefect import flow, task
from prefect._internal.stream_execution import (
    get_stream_execution,
    get_stream_subtask_execution,
    stream_subtask_headers,
)
from prefect.streams import (
    NativeStreamFlowConfig,
    SourceContext,
    SourceRecord,
    StreamContext,
    StreamPartitionCall,
    StreamSourceConfig,
    StreamTaskConfig,
)

if (
    os.environ.get("NATIVE_PARTITIONED_IMPORT_PROBE")
    and Path(sys.argv[0]).name == "stream_subtask_entrypoint.py"
):
    probe_directory = Path(os.environ["NATIVE_PARTITIONED_IMPORT_DIRECTORY"])
    probe_path = probe_directory / f"import-{os.getpid()}.json"
    prior_imports = (
        json.loads(probe_path.read_text())["import_count"] if probe_path.exists() else 0
    )
    probe_api = os.environ["PREFECT_API_URL"]
    probe = {
        "import_count": prior_imports + 1,
        "physical_context": get_stream_subtask_execution() is not None,
        "flow_context": get_stream_execution() is not None,
        "headers_present": bool(
            stream_subtask_headers(probe_api, probe_api + "/streams/v1/create", "POST")
        ),
        "auth_present": any(
            os.environ.get(name)
            for name in (
                "PREFECT_API_AUTH_STRING",
                "PREFECT_SERVER_API_AUTH_STRING",
                "PREFECT_API_KEY",
            )
        ),
    }
    probe_response = httpx.post(
        probe_api + "/streams/v1/create",
        json={
            "flow_run_id": os.environ.get(
                "NATIVE_PARTITIONED_IMPORT_FLOW", str(uuid4())
            ),
            "stream_id": str(uuid4()),
        },
        timeout=5,
    )
    probe["http_status"] = probe_response.status_code
    probe_path.write_text(json.dumps(probe))
    assert not any(
        probe[key]
        for key in (
            "physical_context",
            "flow_context",
            "headers_present",
            "auth_present",
        )
    )
    assert probe_response.status_code == 401
    if os.environ["NATIVE_PARTITIONED_IMPORT_PROBE"] == "exit":
        raise SystemExit(73)


@task(
    version="native-partitioned-v1",
    retries=2,
    tags={"partitioned-fixed-capacity"},
    stream=StreamSourceConfig(),
)
def partitioned_source(
    ctx: SourceContext, split: int, count: int, directory: str, fault: str = "none"
):
    target = Path(directory)
    (target / f"source-{split}-{ctx.attempt}.pid").write_text(str(os.getpid()))
    (target / f"source-{split}-{ctx.attempt}.ppid").write_text(str(os.getppid()))
    for position in range(ctx.accepted_source_position + 1, count // 2 + 1):
        number = 2 * (position - 1) + split + 1
        yield SourceRecord(
            position,
            {"number": number, "document_ref": str(target / f"document-{number}.json")},
        )
        if position == 1:
            (target / f"source-first-accepted-{split}.time").write_text(
                str(time.monotonic_ns())
            )
    if fault in {"operator-loss-200", "operator-before-yield-200"}:
        deadline = time.monotonic() + 90
        while not (target / "replacement-started").exists():
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "Replacement did not start while healthy sources waited"
                )
            time.sleep(0.02)


@task(
    version="native-partitioned-v1",
    retries=2,
    tags={"partitioned-fixed-capacity"},
    stream=StreamTaskConfig(input="document"),
)
def partitioned_operator(
    ctx: StreamContext, document: dict, directory: str, fault: str = "none"
):
    target = Path(directory)
    if (
        fault == "terminal-exit-stall"
        and ctx.partition_index == 0
        and ctx.attempt == 1
        and ctx.input_sequence == 1
    ):

        def stall_after_terminal() -> None:
            (target / "terminal-stall-entered").write_text(str(os.getpid()))
            time.sleep(30)
            (target / "terminal-stall-natural-exit").touch()

        atexit.register(stall_after_terminal)
    (target / f"operator-{ctx.partition_index}-{ctx.attempt}.pid").write_text(
        str(os.getpid())
    )
    (target / f"operator-{ctx.partition_index}-{ctx.attempt}.ppid").write_text(
        str(os.getppid())
    )
    if (
        ctx.partition_index == 0
        and ctx.attempt > 1
        and not (target / "replacement-started").exists()
    ):
        if fault in {"operator-loss-200", "operator-before-yield-200"}:
            deadline = time.monotonic() + 30
            while not (target / "cut-observed").exists():
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        "Recovery observer did not record the durable cursor"
                    )
                time.sleep(0.02)
        (target / "replacement-started").write_text(str(ctx.input_sequence))
    if (
        fault == "operator-loss-200"
        and ctx.partition_index == 0
        and ctx.attempt == 1
        and ctx.input_sequence == 201
    ):
        (target / "operator-loss").write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "task_run_id": str(ctx.task_run_id),
                    "monotonic_ns": time.monotonic_ns(),
                }
            )
        )
        os._exit(77)
    with (target / f"calls-{ctx.partition_index}-{ctx.attempt}.jsonl").open(
        "a"
    ) as calls:
        calls.write(
            json.dumps(
                {
                    "sequence": ctx.input_sequence,
                    "task_run_id": str(ctx.task_run_id),
                    "pid": os.getpid(),
                    "assignment_epoch": ctx.assignment_epoch,
                    "operator_uid": ctx.operator_uid,
                    "monotonic_ns": time.monotonic_ns(),
                }
            )
            + "\n"
        )
    value = json.loads(Path(document["document_ref"]).read_text())
    signed = {
        **value,
        "signed": True,
        "partition": ctx.partition_index,
        "input_sequence": ctx.input_sequence,
    }
    (target / f"signed-{value['number']}.json").write_text(json.dumps(signed))
    if (
        fault == "operator-before-yield-200"
        and ctx.partition_index == 0
        and ctx.attempt == 1
        and ctx.input_sequence == 200
    ):
        # The stable document filename is client-owned idempotence. This effect
        # can repeat because the platform has not accepted the terminal yield.
        (target / "operator-loss").write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "task_run_id": str(ctx.task_run_id),
                    "monotonic_ns": time.monotonic_ns(),
                }
            )
        )
        os._exit(77)
    yield {
        "number": value["number"],
        "signed_ref": str(target / f"signed-{value['number']}.json"),
    }


@task(
    version="native-partitioned-v1",
    retries=2,
    tags={"partitioned-fixed-capacity"},
    stream=StreamSourceConfig(),
)
async def partitioned_source_async(
    ctx: SourceContext, split: int, count: int, directory: str, fault: str = "none"
):
    for record in partitioned_source.fn(ctx, split, count, directory, fault):
        await asyncio.sleep(0)
        yield record


@task(
    version="native-partitioned-v1",
    retries=2,
    tags={"partitioned-fixed-capacity"},
    stream=StreamTaskConfig(input="document"),
)
async def partitioned_operator_async(
    ctx: StreamContext, document: dict, directory: str, fault: str = "none"
):
    await asyncio.sleep(0)
    for value in partitioned_operator.fn(ctx, document, directory, fault):
        yield value


def _partitioned_graph(directory: str, count: int, fault: str, asynchronous: bool):
    target = Path(directory)
    if fault == "before-declaration" and not (target / "before-declaration").exists():
        (target / "before-declaration").touch()
        deadline = time.monotonic() + 30
        while not (target / "declaration-observed").exists():
            if time.monotonic() > deadline:
                raise TimeoutError("No pre-declaration observation")
            time.sleep(0.02)
        raise RuntimeError("Application error before graph declaration")
    source_task = partitioned_source_async if asynchronous else partitioned_source
    operator_task = partitioned_operator_async if asynchronous else partitioned_operator
    source = source_task.submit_partitioned_stream(
        operator_uid="source.documents",
        partitions=(
            StreamPartitionCall(
                parameters={
                    "split": 0,
                    "count": count,
                    "directory": directory,
                    "fault": fault,
                }
            ),
            StreamPartitionCall(
                parameters={
                    "split": 1,
                    "count": count,
                    "directory": directory,
                    "fault": fault,
                }
            ),
        ),
    )
    output = operator_task.submit_partitioned_stream(
        operator_uid="operator.sign",
        upstream=source,
        partitions=(
            StreamPartitionCall(parameters={"directory": directory, "fault": fault}),
            StreamPartitionCall(parameters={"directory": directory, "fault": fault}),
        ),
    )
    if fault == "after-activation":
        raise RuntimeError("Application error after graph activation")
    return output


@flow(
    version="native-partitioned-v1",
    retries=2,
    streaming=NativeStreamFlowConfig(profile="partitioned-fixed-v1", partition_count=2),
)
def partitioned_documents(directory: str, count: int = 8, fault: str = "none"):
    _partitioned_graph(directory, count, fault, False).wait()
    return {"documents": count}


@flow(
    version="native-partitioned-v1",
    retries=2,
    streaming=NativeStreamFlowConfig(profile="partitioned-fixed-v1", partition_count=2),
)
async def partitioned_documents_async(
    directory: str, count: int = 8, fault: str = "none"
):
    output = _partitioned_graph(directory, count, fault, True)
    await asyncio.to_thread(output.wait)
    return {"documents": count}
