"""Private DAG FD identity and unknown signed-readiness ACK controls."""

import copy
import os
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4, uuid5

import httpx

from prefect._internal.stream_dag_subtask_entrypoint import (
    DAGCodePreflight,
    DAGPrivateBootstrap,
    DAGTaskInitializationError,
    execute_dag_bootstrap,
    mark_dag_bootstrap_ready,
    read_dag_bootstrap,
    read_dag_preflight,
)
from prefect._internal.stream_dag_subtask_protocol import (
    DAGChildGateError,
    dag_bootstrap_ready_operation_id,
    dag_launch_close_operation_id,
    dag_launch_prepare_operation_id,
    dag_launch_record_operation_id,
    dag_startup_abort_operation_id,
    dag_subtask_stop_operation_id,
    read_dag_private_bytes,
    read_dag_private_gate,
)
from prefect._internal.stream_execution import (
    get_stream_subtask_execution,
    stream_subtask_execution_context,
)
from prefect._internal.stream_route_plan import canonical_route_json
from prefect._internal.stream_subtask_entrypoint import PhysicalStreamExecutionStopped
from prefect.client.schemas.objects import StateType, TaskRun, TaskRunPolicy
from prefect.client.schemas.stream_subtasks import StreamSubtaskEventResult
from prefect.runner._starter_native import (
    create_native_stream_credentials,
    linux_process_identity,
)
from prefect.states import Pending
from prefect.streams import StreamProtocolError


def private_material() -> dict[str, Any]:
    holder, capability, key, _ = create_native_stream_credentials()
    _, _, ticks = linux_process_identity(os.getpid())
    return {
        "flow_run_id": str(uuid4()),
        "graph_revision_id": str(uuid4()),
        "assignment_epoch": 1,
        "subtask_id": str(uuid4()),
        "grant_id": str(uuid4()),
        "physical_epoch": 1,
        "holder_id": str(holder),
        "scope_digest": "a" * 64,
        "manifest_digest": "b" * 64,
        "task_run_id": str(uuid4()),
        "input_stream_id": None,
        "output_stream_id": str(uuid4()),
        "child_process_id": os.getpid(),
        "process_start_ticks": ticks,
        "api_url": "http://127.0.0.1:1/api",
        "capability": capability,
        "event_private_key": key.hex(),
        "launch_attempt_id": str(uuid4()),
        "worker_generation_digest": "c" * 64,
    }


def pipe_bytes(value: bytes) -> int:
    read, write = os.pipe()
    with os.fdopen(write, "wb") as output:
        output.write(value)
    return read


def pending_task_bootstrap(private: DAGPrivateBootstrap) -> Any:
    """Own identity/config material for the post-GO initialization seam."""
    execution = private.execution
    run = TaskRun(
        id=execution.task_run_id,
        flow_run_id=execution.flow_run_id,
        task_key="pending-task",
        task_version="1",
        dynamic_key="source/0",
        name="pending-dag-task",
        flow_run_run_count=1,
        run_count=0,
        empirical_policy=TaskRunPolicy(retries=2),
        state=Pending(),
    )
    return SimpleNamespace(
        task_run=run,
        flow_run_id=execution.flow_run_id,
        launch_attempt_id=private.launch_attempt_id,
        worker_generation_digest=private.worker_generation_digest,
        assignment=SimpleNamespace(
            graph_revision_id=execution.graph_revision_id,
            assignment_epoch=1,
            id=execution.subtask_id,
            input_stream_id=None,
            manifest=SimpleNamespace(
                role="SOURCE", manifest_digest=execution.manifest_digest
            ),
        ),
        output=SimpleNamespace(stream_id=execution.output_stream_id),
        grant=SimpleNamespace(
            id=execution.grant_id,
            physical_epoch=execution.physical_epoch,
            holder_id=execution.holder_id,
            scope_digest=execution.scope_digest,
            event_key_id=execution.event_key_id,
            child_process_id=execution.child_process_id,
            process_start_ticks=execution.process_start_ticks,
        ),
        event_public_key=execution.event_public_key.hex(),
        entrypoint="fixture:source",
        task_key=run.task_key,
        task_version="1",
        artifact_digest="d" * 64,
        context_key="ctx",
        timeout_seconds=None,
        task_policy=run.empirical_policy,
        effective_tags=[],
        input_key=None,
        parameters={},
    )


class TestDAGPostGOFailure(unittest.TestCase):
    def test_deterministic_config_failure_emits_one_failed_without_body_or_retry(self):
        for lost in (False, True):
            with self.subTest(lost=lost):
                private = read_dag_bootstrap(
                    pipe_bytes(canonical_route_json(private_material()))
                )
                bootstrap = pending_task_bootstrap(private)
                preflight = DAGCodePreflight(None, "SOURCE", b"{}")
                calls: list[dict[str, Any]] = []

                def send(**kwargs: Any) -> StreamSubtaskEventResult:
                    calls.append(copy.deepcopy(kwargs))
                    if lost and len(calls) == 1:
                        kwargs["event"]["payload"].clear()
                        raise httpx.ReadTimeout("Accepted event ACK lost")
                    return StreamSubtaskEventResult(
                        event_id=kwargs["event"]["id"],
                        accepted=True,
                        task_run_id=bootstrap.task_run.id,
                        run_count=0,
                        state_type=StateType.FAILED,
                    )

                client = SimpleNamespace(emit_stream_subtask_event=send)
                with stream_subtask_execution_context(private.execution):
                    with patch(
                        "prefect.client.orchestration.get_client",
                        return_value=nullcontext(client),
                    ) as get_client:
                        with patch("prefect.task_engine.run_task_sync") as body:
                            with self.assertRaises(DAGTaskInitializationError):
                                execute_dag_bootstrap(
                                    private, preflight, bootstrap, None
                                )
                            with self.assertRaises(DAGTaskInitializationError):
                                execute_dag_bootstrap(
                                    private, preflight, bootstrap, None
                                )
                            body.assert_not_called()
                        get_client.assert_called_once_with(sync_client=True)
                self.assertEqual(len(calls), 2 if lost else 1)
                self.assertEqual(len({call["event"]["id"] for call in calls}), 1)
                if lost:
                    self.assertEqual(calls[0], calls[1])
                self.assertTrue(bootstrap.task_run.state.is_failed())
                self.assertFalse(bootstrap.task_run.state.state_details.retriable)
                self.assertEqual(bootstrap.task_run.run_count, 0)
                self.assertEqual(bootstrap.task_run.empirical_policy.retries, 2)

    def test_authority_mismatch_fences_without_pending_failure_event(self):
        private = read_dag_bootstrap(
            pipe_bytes(canonical_route_json(private_material()))
        )
        bootstrap = pending_task_bootstrap(private)
        bootstrap.grant.id = uuid4()
        with stream_subtask_execution_context(private.execution):
            with patch("prefect.client.orchestration.get_client") as client:
                with self.assertRaises(PhysicalStreamExecutionStopped):
                    execute_dag_bootstrap(
                        private,
                        DAGCodePreflight(None, "SOURCE", b"{}"),
                        bootstrap,
                        None,
                    )
                client.assert_not_called()
        self.assertTrue(bootstrap.task_run.state.is_pending())
        self.assertEqual(bootstrap.task_run.run_count, 0)


class TestDAGPrivateBootstrap(unittest.TestCase):
    def test_exact_kernel_identity_roundtrip_does_not_enter_authority(self):
        material = private_material()
        result = read_dag_bootstrap(pipe_bytes(canonical_route_json(material)))
        self.assertEqual(result.execution.child_process_id, os.getpid())
        self.assertEqual(result.launch_attempt_id, UUID(material["launch_attempt_id"]))
        self.assertIsNone(get_stream_subtask_execution())
        self.assertNotIn(material["capability"], repr(result))
        self.assertNotIn(material["event_private_key"], repr(result))

    def test_wrong_pid_or_ticks_refuses_before_constructing_authority(self):
        for field, value in (
            ("child_process_id", os.getpid() + 1),
            ("process_start_ticks", 1),
            ("child_process_id", True),
            ("process_start_ticks", 1.0),
        ):
            with self.subTest(field=field, value=value):
                material = private_material() | {field: value}
                with patch(
                    "prefect._internal.stream_dag_subtask_entrypoint.DAGSubtaskExecutionContext"
                ) as constructor:
                    with self.assertRaises(StreamProtocolError):
                        read_dag_bootstrap(pipe_bytes(canonical_route_json(material)))
                    constructor.assert_not_called()

    def test_duplicate_noncanonical_or_foreign_fields_are_refused(self):
        material = private_material()
        raw = canonical_route_json(material)
        values = [
            b" " + raw,
            raw[:-1] + b',"physical_epoch":1}',
            canonical_route_json(material | {"worker_capability": "forbidden"}),
            canonical_route_json(material | {"worker_generation_digest": "A" * 64}),
            canonical_route_json(material | {"event_private_key": "a" * 63}),
            canonical_route_json(
                material | {"grant_id": "ABCDEF01-2345-4678-9ABC-DEF012345678"}
            ),
        ]
        for value in values:
            with self.subTest(length=len(value)):
                with self.assertRaises((ValueError, StreamProtocolError)):
                    read_dag_bootstrap(pipe_bytes(value))
        self.assertIsNone(get_stream_subtask_execution())

    def test_preflight_raw_object_errors_never_import_application(self):
        for raw in (
            b"[]",
            b'{"role":"SOURCE","role":"SOURCE"}',
            b' {"configuration":{},"role":"SOURCE"}',
            b'{"configuration":{},"role":"SOURCE","foreign":0}',
        ):
            with self.subTest(raw=raw):
                with patch(
                    "prefect._internal.stream_dag_subtask_entrypoint.import_object"
                ) as importer:
                    with self.assertRaises((ValueError, StreamProtocolError)):
                        read_dag_preflight(pipe_bytes(raw))
                    importer.assert_not_called()

    def test_private_pipe_rejects_files_empty_oversized_and_wrong_gate(self):
        with tempfile.TemporaryDirectory(prefix="dag-private-message-") as temporary:
            path = Path(temporary) / "not-pipe"
            path.write_bytes(b"P")
            with path.open("rb") as file:
                with self.assertRaises(DAGChildGateError):
                    read_dag_private_bytes(file.fileno(), 1)
        for raw in (b"", b"X", b"PP"):
            with self.subTest(raw=raw):
                with self.assertRaises(DAGChildGateError):
                    read_dag_private_gate(pipe_bytes(raw), b"P")
        read_dag_private_gate(pipe_bytes(b"P"), b"P")

    def test_operation_ids_match_frozen_domains_and_reject_coercion(self):
        subtask, start, attempt, control = uuid4(), uuid4(), uuid4(), uuid4()
        self.assertEqual(
            dag_launch_prepare_operation_id(subtask, start),
            uuid5(subtask, "prefect-stream-dag-launch-v1/" + str(start)),
        )
        self.assertEqual(
            dag_launch_prepare_operation_id(subtask, start, control),
            uuid5(subtask, "prefect-stream-dag-launch-v1/" + str(control)),
        )
        self.assertEqual(
            dag_launch_record_operation_id(attempt),
            uuid5(attempt, "prefect-stream-dag-record-v1"),
        )
        self.assertEqual(
            dag_launch_close_operation_id(attempt),
            uuid5(attempt, "prefect-stream-dag-close-v1"),
        )
        self.assertEqual(
            dag_subtask_stop_operation_id(control, subtask),
            uuid5(control, "prefect-stream-dag-stop-v1/" + str(subtask)),
        )
        self.assertEqual(
            dag_bootstrap_ready_operation_id(attempt),
            uuid5(attempt, "prefect-stream-dag-bootstrap-ready-v1"),
        )
        self.assertEqual(
            dag_startup_abort_operation_id(start),
            uuid5(start, "prefect-stream-dag-startup-abort-v1"),
        )
        with self.assertRaises(ValueError):
            invalid: Any = str(attempt)
            dag_launch_record_operation_id(invalid)

    def test_readiness_unknown_reply_retries_exact_signature_and_identity(self):
        private = read_dag_bootstrap(
            pipe_bytes(canonical_route_json(private_material()))
        )
        calls: list[dict[str, Any]] = []
        bootstrap: Any = SimpleNamespace(
            flow_run_run_count=1,
            flow_generation=1,
            flow_holder_id=uuid4(),
            context=SimpleNamespace(graph_digest="d" * 64, routing_digest="e" * 64),
            bootstrap_digest="f" * 64,
        )

        def send(**kwargs: Any) -> Any:
            calls.append(kwargs)
            if len(calls) == 1:
                raise httpx.ReadTimeout("Injected unknown ready acknowledgement")
            return SimpleNamespace(
                operation_id=kwargs["operation_id"],
                graph_revision_id=private.execution.graph_revision_id,
                subtask_id=private.execution.subtask_id,
                grant_id=private.execution.grant_id,
                physical_epoch=private.execution.physical_epoch,
                bootstrap_digest=bootstrap.bootstrap_digest,
            )

        mark_dag_bootstrap_ready(
            SimpleNamespace(mark_stream_dag_subtask_bootstrap_ready=send),
            private,
            bootstrap,
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(len(calls[0]["signature"]), 128)
        self.assertNotIn(private.execution.capability, repr(calls))

    def test_readiness_unknown_or_foreign_ack_fences_without_task_get(self):
        private = read_dag_bootstrap(
            pipe_bytes(canonical_route_json(private_material()))
        )
        bootstrap: Any = SimpleNamespace(
            flow_run_run_count=1,
            flow_generation=1,
            flow_holder_id=uuid4(),
            context=SimpleNamespace(graph_digest="d" * 64, routing_digest="e" * 64),
            bootstrap_digest="f" * 64,
        )

        def unknown(**kwargs: Any) -> Any:
            raise httpx.ReadTimeout("Injected unknown ready acknowledgement")

        client = SimpleNamespace(mark_stream_dag_subtask_bootstrap_ready=unknown)
        with self.assertRaises(PhysicalStreamExecutionStopped):
            mark_dag_bootstrap_ready(client, private, bootstrap, timeout=0)

        def foreign(**kwargs: Any) -> Any:
            return SimpleNamespace(
                operation_id=uuid4(),
                graph_revision_id=private.execution.graph_revision_id,
                subtask_id=private.execution.subtask_id,
                grant_id=private.execution.grant_id,
                physical_epoch=private.execution.physical_epoch,
                bootstrap_digest=bootstrap.bootstrap_digest,
            )

        client.mark_stream_dag_subtask_bootstrap_ready = foreign
        with self.assertRaises(PhysicalStreamExecutionStopped):
            mark_dag_bootstrap_ready(client, private, bootstrap)
