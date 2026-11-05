"""Local authoring of an immutable finite single-input streaming DAG.

No declaration or context-manager operation invokes a Task or transport. Only
explicit seal submits the complete metadata declaration; execution stays fenced.
"""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from prefect._internal.stream_dag_execution import current_dag_registry
from prefect._internal.stream_execution import get_stream_execution
from prefect._internal.stream_partitioned import (
    bind_partition_parameters,
    stream_task_definition_witness,
)
from prefect.client.orchestration import get_client
from prefect.client.schemas.stream_dags import (
    StreamDAGOperatorSpec,
    StreamDAGSealRequest,
    StreamEdgeSpec,
)
from prefect.context import FlowRunContext, TagsContext, TaskRunContext
from prefect.settings import PREFECT_API_URL
from prefect.streams import (
    SealedStreamGraph,
    StreamInput,
    StreamOperatorHandle,
    StreamPartitionCall,
    StreamPort,
    StreamProtocolError,
    StreamSourceConfig,
    StreamTaskConfig,
)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _port(port: StreamPort) -> dict[str, str | int]:
    if type(port) is not StreamPort:
        raise StreamProtocolError("A DAG requires one declared StreamPort")
    port.__post_init__()
    return {
        "uid": port.uid,
        "schema_id": port.schema_id,
        "schema_version": port.schema_version,
        "schema_digest": port.schema_digest,
    }


@dataclass(frozen=True)
class _OperatorDraft:
    task: Any
    task_witness: bytes
    definition: bytes


class StreamGraphAuthoring:
    """Collect a bounded rooted DAG before any authoritative mutation."""

    def __init__(self) -> None:
        self.graph_token = object()
        self._lock = threading.RLock()
        self._entered = False
        self._operators: dict[str, _OperatorDraft] = {}
        self._edge_uids: set[str] = set()
        self._subscriber_uids: set[str] = set()
        self._seal_intent: bytes | None = None
        self._sealed: SealedStreamGraph | None = None
        self._seal_request: bytes | None = None
        self._seal_api_url: str | None = None

    def enter(self) -> None:
        with self._lock:
            if self._entered:
                raise StreamProtocolError("A StreamGraph context cannot enter twice")
            self._entered = True

    def exit(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Explicit seal is the only metadata boundary, including on exceptions.
        with self._lock:
            self._entered = False

    def add(
        self,
        task: Any,
        *,
        operator_uid: str,
        partitions: tuple[StreamPartitionCall, ...],
        input: StreamInput | None,
        output: StreamPort,
    ) -> StreamOperatorHandle[Any]:
        # Task imports public StreamGraph descriptors during initialization.
        from prefect.tasks import Task

        if not isinstance(task, Task):
            raise StreamProtocolError("DAG operators require ordinary declared Tasks")
        if not isinstance(task.stream, (StreamSourceConfig, StreamTaskConfig)):
            raise StreamProtocolError(
                "DAG Tasks require a source or single-input stream contract"
            )
        if (
            type(operator_uid) is not str
            or re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", operator_uid) is None
        ):
            raise StreamProtocolError("A DAG operator requires an explicit stable UID")
        if (
            type(partitions) is not tuple
            or not 1 <= len(partitions) <= 16
            or any(type(call) is not StreamPartitionCall for call in partitions)
        ):
            raise StreamProtocolError(
                "A DAG operator requires 1..16 explicit StreamPartitionCall values"
            )
        output_port = _port(output)
        witness = stream_task_definition_witness(task)
        source = isinstance(task.stream, StreamSourceConfig)
        parameters = [
            bind_partition_parameters(task, call.parameters) for call in partitions
        ]
        if (
            source
            and len(parameters) > 1
            and (
                not all(parameters)
                or len({_canonical(item) for item in parameters}) != len(parameters)
            )
        ):
            raise StreamProtocolError(
                "A source requires distinct nonempty bound split snapshots"
            )
        with self._lock:
            if self._seal_intent is not None:
                raise StreamProtocolError("No declaration may follow the graph seal")
            if operator_uid in self._operators:
                raise StreamProtocolError("DAG operator UIDs must be unique")
            if len(self._operators) >= 16:
                raise StreamProtocolError("A DAG supports at most 16 operators")
            existing = [
                json.loads(item.definition) for item in self._operators.values()
            ]
            if sum(item["parallelism"] for item in existing) + len(parameters) > 128:
                raise StreamProtocolError(
                    "A DAG supports at most 128 physical subtasks"
                )
            edge: dict[str, Any] | None = None
            input_port = None
            if source:
                if input is not None or any(
                    item["role"] == "SOURCE" for item in existing
                ):
                    raise StreamProtocolError("A DAG requires exactly one source root")
            else:
                if type(input) is not StreamInput:
                    raise StreamProtocolError(
                        "An operator requires exactly one StreamInput"
                    )
                input.__post_init__()
                input.exchange.__post_init__()
                input_port = _port(input.port)
                if input_port["uid"] == output_port["uid"]:
                    raise StreamProtocolError("Input and output port UIDs must differ")
                if input.upstream.graph_identity is not self.graph_token:
                    raise StreamProtocolError(
                        "An input endpoint belongs to another graph"
                    )
                producer = self._operators.get(input.upstream.operator_uid)
                if producer is None:
                    raise StreamProtocolError(
                        "Declare the upstream operator before its edge"
                    )
                upstream = json.loads(producer.definition)
                if _port(input.upstream.port) != upstream["output_port"]:
                    raise StreamProtocolError(
                        "The upstream output port differs from its declaration"
                    )
                if any(
                    input_port[key] != upstream["output_port"][key]
                    for key in ("schema_id", "schema_version", "schema_digest")
                ):
                    raise StreamProtocolError(
                        "An edge requires matching portable schemas"
                    )
                if input.edge_uid in self._edge_uids:
                    raise StreamProtocolError("DAG edge UIDs must be unique")
                if input.subscriber_uid in self._subscriber_uids:
                    raise StreamProtocolError("DAG subscriber UIDs must be unique")
                if len(self._edge_uids) >= 32:
                    raise StreamProtocolError("A DAG supports at most 32 edges")
                if input.exchange.kind == "forward" and upstream["parallelism"] != len(
                    parameters
                ):
                    raise StreamProtocolError("Forward edges require equal parallelism")
                if input.exchange.kind == "keyed" and (
                    input.exchange.key_group_count is None
                    or not len(parameters)
                    <= input.exchange.key_group_count
                    <= 2**31 - 1
                ):
                    raise StreamProtocolError(
                        "Key groups must cover every target within the DAG profile bound"
                    )
                edge = {
                    "uid": input.edge_uid,
                    "subscriber_uid": input.subscriber_uid,
                    "producer_operator_uid": input.upstream.operator_uid,
                    "producer_port_uid": input.upstream.port.uid,
                    "subscriber_operator_uid": operator_uid,
                    "subscriber_port_uid": input.port.uid,
                    "exchange": {
                        "kind": input.exchange.kind,
                        "routing_version": input.exchange.routing_version,
                        "key_group_count": input.exchange.key_group_count,
                    },
                }
            task_definition = json.loads(witness)
            effective_tags = sorted(set(task.tags) | TagsContext.get().current_tags)
            if len(effective_tags) > 64 or any(
                type(tag) is not str or not tag or len(tag.encode("utf-8")) > 255
                for tag in effective_tags
            ):
                raise StreamProtocolError("DAG Task tags exceed the metadata profile")
            definition = _canonical(
                {
                    "operator_uid": operator_uid,
                    "role": "SOURCE" if source else "OPERATOR",
                    "parallelism": len(parameters),
                    "task": task_definition,
                    "parameters": parameters,
                    "effective_tags": effective_tags,
                    "input_key": task.stream.input
                    if isinstance(task.stream, StreamTaskConfig)
                    else None,
                    "input_port": input_port,
                    "output_port": output_port,
                    "edge": edge,
                }
            )
            handle: StreamOperatorHandle[Any] = StreamOperatorHandle(
                _graph_token=self.graph_token,
                operator_uid=operator_uid,
                output_port=output,
            )
            self._operators[operator_uid] = _OperatorDraft(task, witness, definition)
            if edge is not None:
                self._edge_uids.add(edge["uid"])
                self._subscriber_uids.add(edge["subscriber_uid"])
            return handle

    def definition(self) -> dict[str, Any]:
        """Return a detached local compiler input, never an executing wire plan."""
        with self._lock:
            operators = [
                json.loads(draft.definition)
                for _, draft in sorted(self._operators.items())
            ]
            if len(operators) < 2 or len(self._edge_uids) < 1:
                raise StreamProtocolError(
                    "A DAG requires at least two operators and one edge"
                )
            roots = [
                item["operator_uid"] for item in operators if item["role"] == "SOURCE"
            ]
            if len(roots) != 1:
                raise StreamProtocolError("A DAG requires exactly one source root")
            children: dict[str, list[str]] = {
                item["operator_uid"]: [] for item in operators
            }
            for operator in operators:
                edge = operator["edge"]
                if edge is not None:
                    children[edge["producer_operator_uid"]].append(
                        operator["operator_uid"]
                    )
            reached: set[str] = set()
            pending: list[str] = [roots[0]]
            while pending:
                uid = pending.pop()
                if uid in reached:
                    raise StreamProtocolError(
                        "DAG topology contains a cycle or duplicate input"
                    )
                reached.add(uid)
                pending.extend(children[uid])
            if reached != set(children):
                raise StreamProtocolError(
                    "Every operator must be reachable from its source"
                )
            for draft in self._operators.values():
                if stream_task_definition_witness(draft.task) != draft.task_witness:
                    raise StreamProtocolError(
                        "Task definition changed after DAG declaration"
                    )
            return {
                "schema": "prefect-stream-dag-authoring-v1",
                "profile": "partitioned-dag-v1",
                "operators": operators,
            }

    def seal(self) -> SealedStreamGraph:
        with self._lock:
            if self._sealed is not None:
                return self._sealed
            snapshot = _canonical(self.definition())
            if self._seal_intent is None:
                self._seal_intent = snapshot
            elif snapshot != self._seal_intent:
                raise StreamProtocolError(
                    "A graph seal cannot change its frozen definition"
                )
            self._sealed = self._seal_snapshot(snapshot)
            return self._sealed

    def _seal_snapshot(self, snapshot: bytes) -> SealedStreamGraph:
        context = FlowRunContext.get()
        execution = get_stream_execution()
        if (
            context is None
            or context.flow is None
            or context.flow_run is None
            or execution is None
            or TaskRunContext.get() is not None
            or execution.flow_run_id != context.flow_run.id
            or not context.flow.version
            or context.flow.streaming is None
            or context.flow.streaming.profile != "partitioned-dag-v1"
            or context.flow_run.deployment_id is None
            or context.flow_run.deployment_version is None
            or (context.flow_run.labels or {}).get("prefect.native-streaming")
            != "partitioned-dag-v1"
        ):
            raise StreamProtocolError(
                "DAG seal requires its matching deployed native Flow authority"
            )
        configured = PREFECT_API_URL.value()
        if not configured or configured.rstrip("/") != execution.api_url:
            raise StreamProtocolError("DAG seal API differs from its Flow authority")
        if self._seal_request is None:
            operators, edges = self._wire_definition(snapshot)
            try:
                request = StreamDAGSealRequest(
                    flow_run_id=context.flow_run.id,
                    operation_id=uuid5(
                        NAMESPACE_URL, f"partitioned-dag-v1/{context.flow_run.id}"
                    ),
                    flow_version=context.flow.version,
                    deployment_id=context.flow_run.deployment_id,
                    deployment_version=context.flow_run.deployment_version,
                    working_directory=str(Path.cwd().resolve()),
                    operators=[
                        StreamDAGOperatorSpec.model_validate(item) for item in operators
                    ],
                    edges=[StreamEdgeSpec.model_validate(item) for item in edges],
                )
            except ValidationError as exc:
                raise StreamProtocolError(
                    "DAG declaration exceeds its wire contract"
                ) from exc
            self._seal_request = _canonical(request.model_dump(mode="json"))
            self._seal_api_url = execution.api_url
        frozen_request = StreamDAGSealRequest.model_validate_json(self._seal_request)
        payload = frozen_request.model_dump(mode="json")
        if (
            payload["flow_run_id"] != str(context.flow_run.id)
            or payload["flow_version"] != context.flow.version
            or payload["deployment_id"] != str(context.flow_run.deployment_id)
            or payload["deployment_version"] != context.flow_run.deployment_version
            or self._seal_api_url != execution.api_url
        ):
            raise StreamProtocolError(
                "A seal retry belongs to another Flow declaration"
            )
        registry = current_dag_registry()
        registry.register_pending(
            frozen_request.operation_id, self._seal_request, lambda: self.seal()
        )
        client = get_client(sync_client=True)
        if str(client.api_url).rstrip("/") != execution.api_url:
            raise StreamProtocolError("DAG seal client differs from its Flow authority")
        with client:
            plan = client.seal_stream_dag(
                flow_run_id=frozen_request.flow_run_id,
                operation_id=frozen_request.operation_id,
                flow_version=frozen_request.flow_version,
                operators=[
                    item.model_dump(mode="json") for item in frozen_request.operators
                ],
                edges=[item.model_dump(mode="json") for item in frozen_request.edges],
                deployment_id=frozen_request.deployment_id,
                deployment_version=frozen_request.deployment_version,
                working_directory=frozen_request.working_directory,
                profile=frozen_request.profile,
                assignment_epoch=frozen_request.assignment_epoch,
            )
        expected = {item["operator_uid"]: item for item in payload["operators"]}
        if (
            plan.flow_run_id != context.flow_run.id
            or str(plan.operation_id) != payload["operation_id"]
            or plan.profile != "partitioned-dag-v1"
            or plan.assignment_epoch != 1
            or plan.execution_ready is not False
            or {item.operator_uid for item in plan.operators} != set(expected)
            or any(
                item.role != expected[item.operator_uid]["role"]
                or item.parallelism != expected[item.operator_uid]["parallelism"]
                or item.output_port.model_dump(exclude={"id", "direction"})
                != expected[item.operator_uid]["output_port"]
                or (
                    item.input_port.model_dump(exclude={"id", "direction"})
                    if item.input_port is not None
                    else None
                )
                != expected[item.operator_uid]["input_port"]
                for item in plan.operators
            )
            or [
                item.model_dump(mode="json", exclude={"id", "partitions"})
                for item in plan.edges
            ]
            != payload["edges"]
        ):
            raise StreamProtocolError(
                "Sealed metadata differs from the DAG declaration"
            )
        sealed = SealedStreamGraph(
            graph_revision_id=plan.id, graph_digest=plan.graph_digest
        )
        registry.register(frozen_request.operation_id, sealed)
        return sealed

    @staticmethod
    def _wire_definition(
        snapshot: bytes,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        definition = json.loads(snapshot)
        operators: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        for operator in definition["operators"]:
            task = operator["task"]
            operators.append(
                {
                    "operator_uid": operator["operator_uid"],
                    "role": operator["role"],
                    "parallelism": operator["parallelism"],
                    "task_name": task["task_name"],
                    "task_key": task["task_key"],
                    "task_version": task["task_version"],
                    "artifact_digest": task["artifact_digest"],
                    "entrypoint": task["entrypoint"],
                    "context_key": task["stream"]["context"],
                    "parameters": operator["parameters"],
                    "input_key": operator["input_key"],
                    "input_port": operator["input_port"],
                    "output_port": operator["output_port"],
                    "task_policy": task["task_policy"],
                    "timeout_seconds": task["timeout_seconds"],
                    "tags": operator["effective_tags"],
                }
            )
            edge = operator["edge"]
            if edge is not None:
                exchange = edge["exchange"]
                keyed = exchange["kind"] == "keyed"
                edges.append(
                    {
                        "edge_uid": edge["uid"],
                        "producer_operator_uid": edge["producer_operator_uid"],
                        "output_port_uid": edge["producer_port_uid"],
                        "subscriber_uid": edge["subscriber_uid"],
                        "consumer_operator_uid": edge["subscriber_operator_uid"],
                        "input_port_uid": edge["subscriber_port_uid"],
                        "exchange": exchange["kind"],
                        "routing_version": exchange["routing_version"],
                        "key_group_count": exchange["key_group_count"],
                        "key_codec": "tagged-key-v1" if keyed else None,
                        "hash_algorithm": "sha256" if keyed else None,
                    }
                )
        return operators, sorted(edges, key=lambda item: item["edge_uid"])
