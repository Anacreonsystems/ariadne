"""Stripped distribution import and real-loopback scoped transport component proof.

The HTTP fixture below is a protocol peer, not a Prefect server. This does not
execute business code, verify kernel death or satisfy native acceptance.
"""

# Import order is deliberate: the guard must precede every package import.
# ruff: noqa: E402

import asyncio
import importlib
import importlib.abc
import json
import os
import sys
import threading
from collections.abc import Sequence
from dataclasses import fields
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from uuid import UUID, uuid4

import cloudpickle


class _NoServerImports(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> None:
        if fullname.startswith(
            (
                "prefect.server.database",
                "prefect.server.models",
                "prefect.server.schemas",
                "prefect.server.services",
                "prefect.server.orchestration",
                "prefect.server.utilities",
            )
        ):
            raise ImportError("A stripped distribution imported a server-only module")
        return None


sys.meta_path.insert(0, _NoServerImports())

import prefect
from prefect._internal.stream_event_auth import dag_worker_generation_digest
from prefect._internal.stream_execution import (
    DAGSubtaskExecutionContext,
    SubtaskExecutionContext,
    stream_subtask_execution_context,
)
from prefect._internal.stream_key_wire import (
    decode_tagged_key_wire_v1,
    encode_tagged_key_wire_v1,
)
from prefect._internal.stream_routed_storage import PrefectRoutedStreamStorage
from prefect.client.orchestration import PrefectClient, SyncPrefectClient
from prefect.client.schemas.native_deployments import NativeStreamDeclaration
from prefect.client.schemas.objects import TaskRun
from prefect.client.schemas.stream_dag_executions import (
    StreamDAGExecution,
    StreamDAGLaunchPrepareRequest,
    StreamDAGStartRequest,
    StreamDAGSubtaskBootstrap,
)
from prefect.client.schemas.stream_dags import (
    StreamDAGPlan,
    StreamDAGSealRequest,
    StreamPortSpec,
)
from prefect.client.schemas.stream_routes import (
    StreamDAGConsumerCompleteRequest,
    StreamDAGRouteContext,
    StreamDAGRouteOutcome,
    StreamDAGSourceAppendRequest,
)
from prefect.client.schemas.stream_subtasks import (
    StreamOperatorSpec,
    StreamSubtaskVerifiedRetirement,
)
from prefect.states import Running
from prefect.streams import (
    KeyedStreamOutput,
    NativeStreamFlowConfig,
    RunningStreamGraph,
    SealedStreamGraph,
    StreamExchange,
    StreamGraph,
    StreamInput,
    StreamOperatorHandle,
    StreamPort,
)
from prefect.task_engine import AsyncTaskRunEngine, SyncTaskRunEngine

package_root = Path(prefect.__file__).resolve().parent
repository = Path(sys.argv[1]).resolve()
assert not package_root.is_relative_to(repository)
for directory in (
    "models",
    "database",
    "schemas",
    "services",
    "orchestration",
    "utilities",
):
    assert not (package_root / "server" / directory).exists()

for module in (
    "prefect._internal.stream_dag",
    "prefect._internal.stream_partitioned",
    "prefect._internal.stream_subtask_entrypoint",
    "prefect._internal.stream_routing",
    "prefect.workers.process",
    "prefect.workers._stream_subtasks",
):
    imported = importlib.import_module(module)
    assert Path(imported.__file__).resolve().is_relative_to(package_root)

local_graph_identity = object()
declared_output = StreamOperatorHandle(
    _graph_token=local_graph_identity,
    operator_uid="A",
    output_port=StreamPort("documents"),
).output("documents")
assert declared_output.graph_identity is local_graph_identity
assert (
    declared_output.port.schema_digest
    == "d93f54187397ae62d4e1b98c56edab2a67a9d10422b028dfdea87ba6616a849c"
)
declared_input = StreamInput(
    port=StreamPort("input"),
    upstream=declared_output,
    edge_uid="A-B",
    subscriber_uid="B-input",
    exchange=StreamExchange("forward"),
)
assert declared_input.upstream is declared_output
assert KeyedStreamOutput({"ref": "object://document"}, None).key is None
with StreamGraph():
    pass
sealed_metadata = SealedStreamGraph(graph_revision_id=uuid4(), graph_digest="a" * 64)
assert hasattr(sealed_metadata, "start")
assert hasattr(sealed_metadata, "abort_preview")
assert not hasattr(sealed_metadata, "wait")
running_metadata = RunningStreamGraph(
    graph_revision_id=sealed_metadata.graph_revision_id,
    graph_digest=sealed_metadata.graph_digest,
    routing_digest="b" * 64,
)
assert hasattr(running_metadata, "wait")
assert hasattr(running_metadata, "cancel")
assert not hasattr(running_metadata, "start")
assert (
    StreamPortSpec(uid="documents").schema_digest == declared_output.port.schema_digest
)
assert "execution_ready" in StreamDAGPlan.model_json_schema()["properties"]
assert (
    StreamDAGSealRequest.model_json_schema()["properties"]["operators"]["minItems"] == 2
)
assert StreamDAGSealRequest.model_json_schema()["properties"]["edges"]["minItems"] == 1
assert (
    StreamDAGPlan.model_json_schema()["properties"]["leaf_operator_uids"]["minItems"]
    == 1
)
assert "partitioned-dag-v1" in json.dumps(NativeStreamDeclaration.model_json_schema())
for client_type in (PrefectClient, SyncPrefectClient):
    assert hasattr(client_type, "seal_stream_dag")
    assert hasattr(client_type, "read_stream_dag")
    assert hasattr(client_type, "append_stream_dag_source")
    assert hasattr(client_type, "complete_stream_dag_consumer")
    assert not hasattr(client_type, "admit_stream_dag_region")
    for method in (
        "start_stream_dag",
        "read_stream_dag_execution",
        "abort_stream_dag_preview",
        "cancel_stream_dag",
        "activate_stream_dag",
        "fail_stream_dag",
        "read_stream_dag_subtask_bootstrap",
        "mark_stream_dag_subtask_bootstrap_ready",
        "read_stream_dag_subtask_activation",
        "open_stream_dag_source",
        "open_stream_dag_consumer",
        "read_stream_dag_consumer",
        "finish_stream_dag_source",
        "finish_stream_dag_consumer",
        "poll_worker_stream_dag_subtasks",
        "prepare_worker_stream_dag_launch",
        "record_worker_stream_dag_launch",
        "close_worker_stream_dag_launch",
        "stop_worker_stream_dag_subtask",
        "abort_worker_stream_dag_startup",
        "read_stream_dag_input_status",
        "read_stream_dag_output_status",
    ):
        assert callable(getattr(client_type, method))
assert DAGSubtaskExecutionContext.profile == "partitioned-dag-v1"
assert "grant_ids" not in StreamDAGStartRequest.model_fields
assert "expected_grant_id" not in StreamDAGLaunchPrepareRequest.model_fields
assert "bootstrap_digest" in StreamDAGSubtaskBootstrap.model_fields
assert "terminal_outcome_digest" in StreamDAGExecution.model_fields
assert (
    dag_worker_generation_digest(
        worker_id=UUID(int=1),
        work_pool_id=UUID(int=2),
        host_id=UUID(int=3),
        boot_id=UUID(int=4),
        launcher_process_id=11,
        launcher_start_ticks=13,
    )
    == "ee3993ac0e59c06f554f8b7467248360385c78703b180f586f39b576041484c8"
)

# These are cold-import/validation controls, not DAG execution.
route_context = StreamDAGRouteContext(
    graph_revision_id=uuid4(),
    graph_digest="a" * 64,
    routing_digest="b" * 64,
    producer_subtask_id=uuid4(),
    operator_uid="A",
    output_port_uid="documents",
    subtask_index=0,
    has_keyed_outgoing=True,
)
route_flow_id = uuid4()
route_binding = {
    "flow_run_id": str(route_flow_id),
    "stream_id": str(uuid4()),
    "task_run_id": str(uuid4()),
    "owner_id": str(uuid4()),
    "attempt": 1,
    "owner_epoch": 1,
}
routed_storage = PrefectRoutedStreamStorage(
    flow_run_id=route_flow_id,
    api_url="http://127.0.0.1:1/api",
    route_context=route_context,
)
assert routed_storage.route_context == route_context
assert routed_storage.route_context is not route_context
for scalar_key in (None, False, -9223372036854775808, "документ", b"\x00"):
    tagged_key = encode_tagged_key_wire_v1(scalar_key)
    assert decode_tagged_key_wire_v1(tagged_key) == scalar_key
    route_request = StreamDAGSourceAppendRequest.model_validate(
        {
            "context": route_context.model_dump(mode="json"),
            "binding": route_binding,
            "position": 200,
            "value": {"ref": "object://documents/200"},
            "output_key": tagged_key,
        }
    )
    assert (
        StreamDAGSourceAppendRequest.model_validate(
            route_request.model_dump(mode="json")
        )
        == route_request
    )
assert "input_payload_record_id" in StreamDAGRouteOutcome.model_fields
assert "managed_state" in StreamDAGConsumerCompleteRequest.model_fields
assert (
    StreamDAGRouteOutcome.model_json_schema()["properties"]["references"]["maxItems"]
    == 128
)
dag_declaration = NativeStreamFlowConfig(
    profile="partitioned-dag-v1", partition_count=None
)


@prefect.flow(version="v1", retries=2, streaming=dag_declaration)
def declared_dag_flow():
    raise AssertionError("A package metadata probe must not execute business code")


assert (
    cloudpickle.loads(cloudpickle.dumps(declared_dag_flow)).streaming == dag_declaration
)

operator = StreamOperatorSpec.model_validate(
    {
        "operator_uid": "source",
        "role": "SOURCE",
        "task_name": "source",
        "task_key": "source-key",
        "task_version": "v1",
        "artifact_digest": "a" * 64,
        "entrypoint": "flow.py:source",
        "context_key": "ctx",
        "parameters": [{"split": 0}, {"split": 1}],
        "task_policy": {"retries": 2},
    }
)
assert operator.parallelism == 2
assert (
    AsyncTaskRunEngine.__module__
    == SyncTaskRunEngine.__module__
    == "prefect.task_engine"
)
assert "COMPLETED_RECONCILED" in json.dumps(
    StreamSubtaskVerifiedRetirement.model_json_schema()
)

flow, graph, logical, subtask, grant, task_id, incoming, outgoing, holder = (
    uuid4() for _ in range(9)
)
task_run = TaskRun(
    id=task_id,
    flow_run_id=flow,
    name="packaged-subtask",
    task_key="operator-key",
    task_version="v1",
    dynamic_key="operator/0",
    empirical_policy={"retries": 2},
    flow_run_run_count=1,
    tags=["etl"],
    state=Running(),
)
stamp = "2026-11-04T12:00:00Z"
requests = []
transport_errors = []
context = None
bootstrap = None
DAG_CAPABILITY = "e" * 64


class _ProtocolPeer(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def _respond(self) -> None:
        try:
            expected_capability = (
                DAG_CAPABILITY if "/dags/" in self.path else context.capability
            )
            assert (
                self.headers["Authorization"] == "PrefectSubtask " + expected_capability
            )
            assert self.headers["X-Prefect-Subtask-Grant"] == str(grant)
            assert self.headers.get("Cookie") is None
            assert not any(
                key.lower().startswith("x-prefect-stream-execution-")
                for key in self.headers
            )
            assert self.headers.get("Prefect-Csrf-Token") is None
            requests.append((self.command, self.path))
            if self.command == "GET":
                if self.path == f"/api/streams/v2/dags/subtasks/{subtask}/activation":
                    response = dict(
                        graph_revision_id=str(graph),
                        subtask_id=str(subtask),
                        grant_id=str(grant),
                        physical_epoch=1,
                        phase="WAITING",
                        activation_operation_id=None,
                    )
                else:
                    assert self.path == f"/api/streams/v2/subtasks/{subtask}/bootstrap"
                    response = bootstrap
                encoded = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
            else:
                assert self.path == f"/api/streams/v2/subtasks/{subtask}/lease/renew"
                assert self.rfile.read(int(self.headers["Content-Length"])) == b"{}"
                self.send_response(204)
                self.end_headers()
        except Exception as error:
            transport_errors.append(type(error).__name__)
            self.send_response(500)
            self.end_headers()

    do_GET = _respond
    do_POST = _respond


with ThreadingHTTPServer(("127.0.0.1", 0), _ProtocolPeer) as server:
    endpoint = f"http://127.0.0.1:{server.server_port}/api"
    context = SubtaskExecutionContext(
        flow_run_id=flow,
        graph_revision_id=graph,
        assignment_epoch=1,
        subtask_id=subtask,
        grant_id=grant,
        physical_epoch=1,
        holder_id=holder,
        scope_digest="b" * 64,
        manifest_digest="c" * 64,
        task_run_id=task_id,
        input_stream_id=incoming,
        output_stream_id=outgoing,
        child_process_id=os.getpid(),
        process_start_ticks=1,
        api_url=endpoint,
        capability="d" * 64,
        event_private_key=b"p" * 32,
    )
    bootstrap = {
        "grant": {
            "id": str(grant),
            "subtask_id": str(subtask),
            "graph_revision_id": str(graph),
            "assignment_epoch": 1,
            "physical_epoch": 1,
            "holder_id": str(holder),
            "launcher_binding_kind": "FLOW_AUTHORITY_LOCAL",
            "launcher_process_id": os.getpid(),
            "launcher_start_ticks": 1,
            "event_key_id": context.event_key_id,
            "scope_digest": context.scope_digest,
            "state": "REGISTERED",
            "host_id": str(uuid4()),
            "boot_id": str(uuid4()),
            "child_process_id": context.child_process_id,
            "process_start_ticks": context.process_start_ticks,
            "created_at": stamp,
            "last_renewed_at": stamp,
        },
        "assignment": {
            "id": str(subtask),
            "graph_revision_id": str(graph),
            "operator_run_id": str(logical),
            "operator_uid": "operator",
            "subtask_index": 0,
            "assignment_epoch": 1,
            "input_stream_id": str(incoming),
            "current_grant_id": str(grant),
            "physical_epoch": 1,
            "manifest": {
                "flow_run_id": str(flow),
                "submission_key": "operator/0",
                "manifest_digest": context.manifest_digest,
                "task_run_id": str(task_id),
                "stream_id": str(outgoing),
                "dynamic_key": "operator/0",
                "role": "OPERATOR",
                "protocol": "yield-v2",
                "state": "ACTIVE",
                "created_at": stamp,
            },
        },
        "flow_run_id": str(flow),
        "flow_run_run_count": 1,
        "entrypoint": "flow.py:operator",
        "context_key": "ctx",
        "task_version": "v1",
        "artifact_digest": "a" * 64,
        "task_policy": task_run.empirical_policy.model_dump(mode="json"),
        "effective_tags": ["etl"],
        "parameters": {"split": 0},
        "input_key": "document",
        "task_run": task_run.model_dump(mode="json"),
        "reserved_tag_names": ["tag:etl"],
    }
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with stream_subtask_execution_context(context):
            with SyncPrefectClient(endpoint, auth_string="forbidden:basic") as client:
                assert (
                    client.read_stream_subtask_bootstrap(subtask_id=subtask).grant.id
                    == grant
                )
                client.renew_stream_subtask_lease(subtask_id=subtask)
                try:
                    client._client.get(f"/task_runs/{task_id}")
                except ValueError:
                    pass
                else:
                    raise AssertionError("An off-scope request reached the transport")

        async def asynchronous() -> None:
            with stream_subtask_execution_context(context):
                async with PrefectClient(
                    endpoint, api_key="forbidden-api-key"
                ) as client:
                    assert (
                        await client.read_stream_subtask_bootstrap(subtask_id=subtask)
                    ).grant.id == grant
                    await client.renew_stream_subtask_lease(subtask_id=subtask)

        asyncio.run(asynchronous())

        dag_context = DAGSubtaskExecutionContext(
            **{
                **{
                    field.name: getattr(context, field.name)
                    for field in fields(context)
                },
                "capability": DAG_CAPABILITY,
            }
        )
        with stream_subtask_execution_context(dag_context):
            with SyncPrefectClient(endpoint, auth_string="forbidden:basic") as client:
                observed = client.read_stream_dag_subtask_activation(subtask_id=subtask)
                assert observed.phase == "WAITING"
                assert observed.grant_id == grant
                for offscope in (
                    f"/task_runs/{task_id}",
                    f"/streams/v2/subtasks/{subtask}/task-run",
                    f"/streams/v1/{incoming}",
                ):
                    try:
                        client._client.get(offscope)
                    except ValueError:
                        pass
                    else:
                        raise AssertionError(
                            "DAG transport admitted a legacy/general read"
                        )
                try:
                    client.renew_stream_subtask_lease(subtask_id=subtask)
                except ValueError:
                    pass
                else:
                    raise AssertionError("DAG transport admitted a legacy lease")

        async def asynchronous_dag() -> None:
            with stream_subtask_execution_context(dag_context):
                async with PrefectClient(
                    endpoint, api_key="forbidden-api-key"
                ) as client:
                    observed = await client.read_stream_dag_subtask_activation(
                        subtask_id=subtask
                    )
                    assert observed.phase == "WAITING"
                    assert observed.grant_id == grant

        asyncio.run(asynchronous_dag())
    finally:
        server.shutdown()
        thread.join(timeout=5)
assert not transport_errors
assert len(requests) == 6
print(
    json.dumps(
        {
            "verdict": "PASS_STRIPPED_DISTRIBUTION_COMPONENT",
            "http_requests": len(requests),
            "offscope_denied_before_transport": True,
            "server_only_imports_blocked": True,
            "business_calls": 0,
            "routed_key_variants_validated": 5,
            "routed_adapter_constructed": True,
            "dag_lifecycle_dtos_validated": True,
            "dag_scoped_activation_requests": 2,
            "worker_generation_vector_validated": True,
            "routed_execution": False,
            "native_acceptance": False,
        }
    )
)
