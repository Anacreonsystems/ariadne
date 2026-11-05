"""Controls for fixed-P2 declarations and physical task event delivery."""

import inspect
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from typing import Any, get_type_hints
from uuid import UUID, uuid4

import httpx
import pytest

from prefect import task
from prefect._internal.stream_execution import (
    SubtaskExecutionContext,
    stream_subtask_execution_context,
)
from prefect._internal.stream_partitioned import (
    PartitionedStreamReconstruction,
    bind_partition_calls,
    subtask_environment,
)
from prefect._internal.stream_subtask_entrypoint import (
    PhysicalStreamExecutionStopped,
    aemit_physical_task_event,
    emit_physical_task_event,
)
from prefect.client.schemas.objects import TaskRun
from prefect.context import TagsContext
from prefect.runner._starter_native import linux_process_identity
from prefect.states import Pending, Running
from prefect.streams import (
    NativeStreamFlowConfig,
    PartitionedStream,
    SourceContext,
    SourceRecord,
    Stream,
    StreamContext,
    StreamPartitionCall,
    StreamProtocolError,
    StreamSourceConfig,
    StreamTaskConfig,
)
from prefect.tasks import Task
from prefect.utilities.engine import build_task_run_state_change_event


@task(version="p2-v1", retries=2, stream=StreamSourceConfig())
def split_source(ctx: SourceContext, split: str, limit: int = 2):
    for position in range(ctx.accepted_source_position + 1, limit + 1):
        yield SourceRecord(position, {"split": split, "position": position})


@task(version="p2-v1", retries=2, stream=StreamTaskConfig(input="document"))
def split_operator(ctx: StreamContext, document: dict[str, Any], suffix: str = "!"):
    yield {**document, "suffix": suffix}


def physical_context() -> SubtaskExecutionContext:
    return SubtaskExecutionContext(
        flow_run_id=uuid4(),
        graph_revision_id=uuid4(),
        assignment_epoch=1,
        subtask_id=uuid4(),
        grant_id=uuid4(),
        physical_epoch=1,
        holder_id=uuid4(),
        scope_digest="a" * 64,
        manifest_digest="b" * 64,
        task_run_id=uuid4(),
        input_stream_id=None,
        output_stream_id=uuid4(),
        child_process_id=os.getpid(),
        process_start_ticks=1,
        api_url="http://localhost:4200/api",
        capability="c" * 64,
        event_private_key=bytes(range(32)),
    )


def physical_event(execution: SubtaskExecutionContext):
    initial = Pending()
    running = Running()
    run = TaskRun(
        id=execution.task_run_id,
        flow_run_id=execution.flow_run_id,
        name="split-source[0]",
        task_key="key",
        dynamic_key="physical/source/0",
        state=running,
        run_count=1,
    )
    return build_task_run_state_change_event(run, initial, running)


def receipt(event: Any, *, accepted: bool = True) -> Any:
    return SimpleNamespace(
        accepted=accepted,
        event_id=event.id,
        task_run_id=uuid4() if not accepted else UUID_FROM_RESOURCE(event),
        run_count=event.payload["task_run"]["run_count"],
        state_type=SimpleNamespace(value=event.payload["validated_state"]["type"]),
    )


def UUID_FROM_RESOURCE(event: Any):
    from uuid import UUID

    return UUID(event.resource["prefect.resource.id"].rsplit(".", 1)[1])


def test_existing_submit_stream_annotation_is_unchanged():
    assert get_type_hints(Task.submit_stream)["return"] == Stream[Any]
    annotation = get_type_hints(Task.submit_partitioned_stream)["return"]
    assert annotation.__origin__ is PartitionedStream
    assert annotation.__args__[0].__name__ == "R"
    signature = inspect.signature(Task.submit_partitioned_stream)
    assert all(
        p.kind == p.KEYWORD_ONLY
        for name, p in signature.parameters.items()
        if name != "self"
    )


def test_native_defaults_preserve_legacy_profile():
    assert NativeStreamFlowConfig().partition_count == 1
    assert (
        NativeStreamFlowConfig(
            profile="partitioned-fixed-v1", partition_count=2
        ).partition_count
        == 2
    )


@pytest.mark.parametrize(
    "profile,count",
    [
        ("bounded-linear-v1", 2),
        ("partitioned-fixed-v1", 1),
        ("partitioned-fixed-v1", True),
        ("unknown", 2),
    ],
)
def test_incompatible_profile_count_is_refused(profile: str, count: Any):
    with pytest.raises(StreamProtocolError):
        NativeStreamFlowConfig(profile=profile, partition_count=count)


def test_partition_parameter_snapshot_is_frozen_and_defensively_copied():
    original: dict[str, Any] = {"split": {"ids": [1, 2]}}
    call = StreamPartitionCall(parameters=original)
    original["split"]["ids"].append(3)
    copy = call.parameters
    copy["split"]["ids"].append(4)
    assert call.parameters == {"split": {"ids": [1, 2]}}
    with pytest.raises(FrozenInstanceError):
        call._parameters = b"{}"


@pytest.mark.parametrize(
    "value",
    [
        [],
        {1: "x"},
        {"x": float("nan")},
        {"x": (1, 2)},
        {"x": b"value"},
        {"x": object()},
        {"x": "\ud800"},
    ],
)
def test_non_json_partition_parameters_are_refused(value: Any):
    with pytest.raises(StreamProtocolError):
        StreamPartitionCall(parameters=value)


def test_source_binds_defaults_and_requires_explicit_distinct_splits():
    calls = (
        StreamPartitionCall(parameters={"split": "left"}),
        StreamPartitionCall(parameters={"split": "right"}),
    )
    assert bind_partition_calls(split_source, calls) == [
        {"split": "left", "limit": 2},
        {"split": "right", "limit": 2},
    ]
    with pytest.raises(StreamProtocolError):
        bind_partition_calls(
            split_source,
            (calls[0], StreamPartitionCall(parameters={"split": "left", "limit": 2})),
        )


@pytest.mark.parametrize(
    "option,value",
    [
        ("retry_condition_fn", lambda *args: False),
        ("on_completion_hooks", [lambda *args: None]),
        ("on_failure_hooks", [lambda *args: None]),
        ("on_running_hooks", [lambda *args: None]),
        ("on_commit_hooks", [lambda *args: None]),
        ("on_rollback_hooks", [lambda *args: None]),
        ("persist_result", True),
        ("result_storage", "storage-ref"),
        ("result_serializer", "json"),
        ("refresh_cache", True),
        ("task_run_name", "custom-{split}"),
    ],
)
def test_unsupported_mutated_task_options_fail_before_metadata(
    option: str, value: Any, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(split_source, option, value)
    calls = (
        StreamPartitionCall(parameters={"split": "a"}),
        StreamPartitionCall(parameters={"split": "b"}),
    )
    with pytest.raises(StreamProtocolError):
        bind_partition_calls(split_source, calls)


def test_source_is_only_a_local_declaration_until_operator(
    monkeypatch: pytest.MonkeyPatch,
):
    execution = physical_context()
    graph = PartitionedStreamReconstruction(
        execution,
        SimpleNamespace(id=execution.flow_run_id, run_count=1),
        SimpleNamespace(version="v1"),
    )
    calls = []

    def refuse_metadata(*args: Any, **kwargs: Any):
        calls.append((args, kwargs))
        raise AssertionError("Incomplete graph mutated the server")

    monkeypatch.setattr(graph, "request", refuse_metadata)
    source = graph.declare(
        split_source,
        operator_uid="source.documents",
        partitions=(
            StreamPartitionCall(parameters={"split": "left"}),
            StreamPartitionCall(parameters={"split": "right"}),
        ),
        upstream=None,
    )
    with pytest.raises(StreamProtocolError):
        _ = source.partitions
    with pytest.raises(StreamProtocolError):
        graph.finish()
    assert calls == []


def test_aliased_task_binding_is_refused_before_metadata(
    monkeypatch: pytest.MonkeyPatch,
):
    execution = physical_context()
    graph = PartitionedStreamReconstruction(
        execution,
        SimpleNamespace(id=execution.flow_run_id, run_count=1),
        SimpleNamespace(version="v1"),
    )
    alias = split_source.with_options(name="alias")
    with pytest.raises(StreamProtocolError, match="module-level"):
        graph.declare(
            alias,
            operator_uid="source.documents",
            partitions=(
                StreamPartitionCall(parameters={"split": "left"}),
                StreamPartitionCall(parameters={"split": "right"}),
            ),
            upstream=None,
        )
    assert graph.plan is None
    assert graph.entries == {}


def test_witness_failure_leaves_no_partial_local_declaration(
    monkeypatch: pytest.MonkeyPatch,
):
    execution = physical_context()
    graph = PartitionedStreamReconstruction(
        execution,
        SimpleNamespace(id=execution.flow_run_id, run_count=1),
        SimpleNamespace(version="v1"),
    )

    def refuse_witness(task: Any) -> bytes:
        raise StreamProtocolError("Definition changed during validation")

    monkeypatch.setattr(
        "prefect._internal.stream_partitioned._direct_task_witness", refuse_witness
    )
    with pytest.raises(StreamProtocolError, match="changed during validation"):
        graph.declare(
            split_source,
            operator_uid="source.documents",
            partitions=(
                StreamPartitionCall(parameters={"split": "left"}),
                StreamPartitionCall(parameters={"split": "right"}),
            ),
            upstream=None,
        )
    assert graph.entries == {}
    assert graph._task_witnesses == {}
    assert graph.handles == []
    assert graph.specifications == []
    assert graph.plan is None
    assert graph.admission_attempted is False
    assert not graph.ready.is_set()


@pytest.mark.parametrize(
    "mutation",
    [
        "hooks",
        "context",
        "version",
        "tags",
        "retries",
        "timeout",
        "name",
        "code",
        "binding",
    ],
)
def test_source_definition_drift_between_declarations_never_calls_metadata(
    mutation: str, monkeypatch: pytest.MonkeyPatch
):
    execution = physical_context()
    graph = PartitionedStreamReconstruction(
        execution,
        SimpleNamespace(
            id=execution.flow_run_id,
            run_count=1,
            deployment_id=uuid4(),
            deployment_version="v1",
        ),
        SimpleNamespace(version="v1"),
    )
    calls = []

    def refuse_metadata(*args: Any, **kwargs: Any):
        calls.append((args, kwargs))
        raise AssertionError("Changed source crossed the metadata boundary")

    monkeypatch.setattr(graph, "request", refuse_metadata)
    source = graph.declare(
        split_source,
        operator_uid="source.documents",
        partitions=(
            StreamPartitionCall(parameters={"split": "left"}),
            StreamPartitionCall(parameters={"split": "right"}),
        ),
        upstream=None,
    )
    if mutation == "code":
        code = split_source.fn.__code__
        monkeypatch.setattr(
            split_source.fn,
            "__code__",
            code.replace(co_consts=(*code.co_consts, "drift")),
        )
    elif mutation == "binding":
        monkeypatch.setattr(
            sys.modules[split_source.fn.__module__],
            "split_source",
            split_source.with_options(name="different-binding"),
        )
    else:
        option, value = {
            "hooks": ("on_completion_hooks", [lambda *args: None]),
            "context": ("stream", StreamSourceConfig(context="different")),
            "version": ("version", "changed-version"),
            "tags": ("tags", {"different-tag"}),
            "retries": ("retries", 3),
            "timeout": ("timeout_seconds", 3.0),
            "name": ("name", "different-name"),
        }[mutation]
        monkeypatch.setattr(split_source, option, value)
    with pytest.raises(StreamProtocolError):
        graph.declare(
            split_operator,
            operator_uid="operator.sign",
            partitions=(StreamPartitionCall(parameters={}),) * 2,
            upstream=source,
        )
    assert calls == []
    assert graph.plan is None
    assert graph.admission_attempted is False


def test_ambient_tags_change_preserves_declared_source_tags(
    monkeypatch: pytest.MonkeyPatch,
):
    execution = physical_context()
    graph = PartitionedStreamReconstruction(
        execution,
        SimpleNamespace(
            id=execution.flow_run_id,
            run_count=1,
            deployment_id=uuid4(),
            deployment_version="v1",
        ),
        SimpleNamespace(version="v1"),
    )
    captured = []

    def observe_seal(method: str, **kwargs: Any):
        captured.append((method, kwargs))
        raise RuntimeError("Observed the first real metadata boundary")

    monkeypatch.setattr(graph, "request", observe_seal)
    with TagsContext(current_tags={"source-context"}):
        source = graph.declare(
            split_source,
            operator_uid="source.documents",
            partitions=(
                StreamPartitionCall(parameters={"split": "left"}),
                StreamPartitionCall(parameters={"split": "right"}),
            ),
            upstream=None,
        )
    with TagsContext(current_tags={"operator-context"}):
        with pytest.raises(RuntimeError, match="first real metadata boundary"):
            graph.declare(
                split_operator,
                operator_uid="operator.sign",
                partitions=(StreamPartitionCall(parameters={}),) * 2,
                upstream=source,
            )
    assert len(captured) == 1
    assert captured[0][0] == "seal_stream_graph"
    source_spec, operator_spec = captured[0][1]["operators"]
    assert source_spec["tags"] == ["source-context"]
    assert operator_spec["tags"] == ["operator-context"]


@pytest.mark.parametrize(
    "calls",
    [
        [StreamPartitionCall(parameters={"split": "a"})] * 2,
        (),
        ({"parameters": {"split": "a"}}, {"parameters": {"split": "b"}}),
    ],
)
def test_descriptors_are_not_coerced(calls: Any):
    with pytest.raises(StreamProtocolError):
        bind_partition_calls(split_source, calls)


@pytest.mark.parametrize("name", ["ctx", "document"])
def test_operator_injected_parameters_cannot_be_overridden(name: str):
    call = StreamPartitionCall(parameters={name: None})
    with pytest.raises(StreamProtocolError):
        bind_partition_calls(split_operator, (call, call))


def test_operator_parameters_may_be_equal():
    call = StreamPartitionCall(parameters={})
    assert bind_partition_calls(split_operator, (call, call)) == [{"suffix": "!"}] * 2


def test_pending_handle_has_no_provisional_ids_or_global_collection():
    handle = PartitionedStream()
    with pytest.raises(StreamProtocolError):
        _ = handle.partitions
    with pytest.raises(StreamProtocolError):
        handle.wait()
    assert not hasattr(handle, "collect")
    assert not hasattr(handle, "stream_id")


def test_physical_binding_is_single_assignment_under_concurrency():
    handle = PartitionedStream()
    streams = (Stream(None, str(uuid4())), Stream(None, str(uuid4())))

    def bind() -> bool:
        try:
            handle._bind(streams)
            return True
        except StreamProtocolError:
            return False

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(lambda _: bind(), range(16))) == 1
    assert handle.partitions == streams


@pytest.mark.parametrize(
    "context",
    [SourceContext(uuid4(), "stream", 1, 0), StreamContext(uuid4(), "consumer", 1, 1)],
)
def test_legacy_context_has_no_partition_epoch_or_operator(context: Any):
    assert (
        context.partition_index,
        context.partition_count,
        context.operator_uid,
        context.assignment_epoch,
    ) == (0, 1, None, None)
    with pytest.raises(AttributeError):
        context.partition_index = 1


def test_child_environment_drops_all_platform_auth_and_profiles(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    for name in (
        "PREFECT_API_KEY",
        "PREFECT_API_AUTH_STRING",
        "PREFECT_SERVER_API_AUTH_STRING",
        "PREFECT_PROFILES_PATH",
        "PREFECT_STREAM_CAPABILITY",
    ):
        monkeypatch.setenv(name, "private-value")
    env = subtask_environment(tmp_path, "http://localhost:4200/api")
    assert "private-value" not in env.values()
    assert env["PREFECT_LOGGING_TO_API_ENABLED"] == "false"
    assert env["PREFECT_PROFILES_PATH"] == str(tmp_path / "profiles.toml")


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_ack_loss_reuses_exact_signed_event(asynchronous: bool):
    execution = physical_context()
    with stream_subtask_execution_context(execution):
        event = physical_event(execution)
        attempts = []

        def deliver(**kwargs: Any):
            attempts.append(kwargs["event"])
            if len(attempts) == 1:
                raise httpx.ReadError("ack lost")
            return receipt(event)

        async def adeliver(**kwargs: Any):
            return deliver(**kwargs)

        client = SimpleNamespace(
            emit_stream_subtask_event=adeliver if asynchronous else deliver
        )
        result = (
            await aemit_physical_task_event(client, event)
            if asynchronous
            else emit_physical_task_event(client, event)
        )
        assert result is event
        assert len(attempts) == 2
        assert attempts[0] is attempts[1]
        assert attempts[0] == event.model_dump(mode="json")
        assert attempts[0]["resource"]["prefect.stream-event-signature"]
        assert attempts[0]["related"][-1] == {
            "prefect.resource.id": f"prefect.flow-run.{execution.flow_run_id}",
            "prefect.resource.role": "flow-run",
        }


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("recorded", [False, True])
async def test_unknown_ack_resolves_only_exact_recorded_state(
    asynchronous: bool, recorded: bool
):
    execution = physical_context()
    with stream_subtask_execution_context(execution):
        event = physical_event(execution)
        attempts = []

        def deliver(**kwargs: Any):
            attempts.append(kwargs["event"])
            raise httpx.ReadError("lost response")

        def read(**kwargs: Any):
            return SimpleNamespace(
                id=execution.task_run_id,
                state=Running(id=event.id if recorded else uuid4()),
                run_count=1,
            )

        async def adeliver(**kwargs: Any):
            return deliver(**kwargs)

        async def aread(**kwargs: Any):
            return read(**kwargs)

        client = SimpleNamespace(
            emit_stream_subtask_event=adeliver if asynchronous else deliver,
            read_stream_subtask_task_run=aread if asynchronous else read,
        )
        if recorded:
            result = (
                await aemit_physical_task_event(client, event, timeout=0)
                if asynchronous
                else emit_physical_task_event(client, event, timeout=0)
            )
            assert result is event
        else:
            with pytest.raises(PhysicalStreamExecutionStopped):
                if asynchronous:
                    await aemit_physical_task_event(client, event, timeout=0)
                else:
                    emit_physical_task_event(client, event, timeout=0)
        assert len(attempts) == 1
        assert attempts[0]["event"] == "prefect.task-run.Running"


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_http_200_fenced_receipt_stops_without_failed_event(asynchronous: bool):
    execution = physical_context()
    with stream_subtask_execution_context(execution):
        event = physical_event(execution)
        attempts = []

        def deliver(**kwargs: Any):
            attempts.append(kwargs["event"])
            return receipt(event, accepted=False)

        async def adeliver(**kwargs: Any):
            return deliver(**kwargs)

        client = SimpleNamespace(
            emit_stream_subtask_event=adeliver if asynchronous else deliver
        )
        with pytest.raises(PhysicalStreamExecutionStopped):
            if asynchronous:
                await aemit_physical_task_event(client, event)
            else:
                emit_physical_task_event(client, event)
        assert [attempt["event"] for attempt in attempts] == [
            "prefect.task-run.Running"
        ]


@pytest.mark.skipif(sys.platform != "linux", reason="Fixed-P2 requires Linux identity")
@pytest.mark.parametrize("identity", ["another-pid", "another-start", "current"])
def test_actual_child_checks_exact_kernel_identity_before_go(
    tmp_path: Path, identity: str
):
    execution = physical_context()
    payload = {
        key: str(value) if isinstance(value, UUID) else value
        for key, value in vars(execution).items()
    }
    payload["event_private_key"] = execution.event_private_key.hex()
    read_fd, write_fd = os.pipe()
    go_read, go_write = os.pipe()
    child = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "prefect._internal.stream_subtask_entrypoint",
            str(read_fd),
            str(go_read),
        ],
        pass_fds=(read_fd, go_read),
        env=subtask_environment(tmp_path, execution.api_url),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    os.close(read_fd)
    os.close(go_read)
    _, _, ticks = linux_process_identity(child.pid)
    payload["child_process_id"] = (
        os.getpid() if identity == "another-pid" else child.pid
    )
    payload["process_start_ticks"] = ticks + (identity == "another-start")
    with os.fdopen(write_fd, "wb") as pipe:
        pipe.write(json.dumps(payload).encode())
    os.close(go_write)
    stdout, stderr = child.communicate(timeout=20)
    assert child.returncode != 0
    if identity == "current":
        assert b"refused the physical startup barrier" in stderr
        assert b"delivered to a different process" not in stderr
    else:
        assert b"delivered to a different process" in stderr
        assert b"refused the physical startup barrier" not in stderr
    assert b"Traceback" in stderr
    assert stdout == b""
