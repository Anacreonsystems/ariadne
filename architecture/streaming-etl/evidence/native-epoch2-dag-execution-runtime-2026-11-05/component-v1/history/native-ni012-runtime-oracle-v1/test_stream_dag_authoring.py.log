"""Pure DAG authoring controls; no server or executing stream is substituted."""

import asyncio
import json
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx

from prefect import task
from prefect._internal.stream_dag import StreamGraphAuthoring
from prefect._internal.stream_execution import StreamExecutionContext
from prefect._internal.stream_finite import _reserve
from prefect._internal.stream_reconstruction import (
    async_stream_flow_scope,
    stream_flow_scope,
    submit_reconstructed_stream,
)
from prefect.client.schemas.stream_dags import StreamDAGSealRequest
from prefect.context import TagsContext
from prefect.streams import (
    SealedStreamGraph,
    SourceContext,
    SourceRecord,
    StreamContext,
    StreamExchange,
    StreamGraph,
    StreamInput,
    StreamOperatorHandle,
    StreamPartitionCall,
    StreamPort,
    StreamProtocolError,
    StreamSourceConfig,
    StreamTaskConfig,
)


@task(version="dag-authoring-v1", retries=2, stream=StreamSourceConfig())
def dag_source(ctx: SourceContext, split: str):
    raise AssertionError("Authoring executed the source body")
    yield SourceRecord(1, {"split": split})


@task(version="dag-authoring-v1", retries=2, stream=StreamSourceConfig())
def dag_bare_source(ctx: SourceContext):
    raise AssertionError("Authoring executed the parameterless source body")
    yield SourceRecord(1, {})


@task(version="dag-authoring-v1", retries=2, stream=StreamTaskConfig(input="document"))
def dag_operator(ctx: StreamContext, document: Any):
    raise AssertionError("Authoring executed an operator body")
    yield document


@task(version="dag-authoring-v1", retries=2)
def ordinary_task():
    raise AssertionError("Authoring executed an ordinary body")


def source(
    authoring: StreamGraphAuthoring, count: int = 1, uid: str = "A"
) -> StreamOperatorHandle[Any]:
    return authoring.add(
        dag_source,
        operator_uid=uid,
        partitions=tuple(
            StreamPartitionCall(parameters={"split": str(index)})
            for index in range(count)
        ),
        input=None,
        output=StreamPort("out"),
    )


def branch(
    authoring: StreamGraphAuthoring,
    upstream: StreamOperatorHandle[Any],
    uid: str,
    count: int = 1,
    kind: str = "forward",
    edge_uid: str | None = None,
    subscriber_uid: str | None = None,
    input_uid: str = "in",
) -> StreamOperatorHandle[Any]:
    return authoring.add(
        dag_operator,
        operator_uid=uid,
        partitions=(StreamPartitionCall(parameters={}),) * count,
        input=StreamInput(
            port=StreamPort(input_uid),
            upstream=upstream.output("out"),
            edge_uid=edge_uid or f"A-{uid}",
            subscriber_uid=subscriber_uid or f"subscriber-{uid}",
            exchange=StreamExchange(kind=kind),
        ),
        output=StreamPort("out"),
    )


def fork(authoring: StreamGraphAuthoring) -> None:
    upstream = source(authoring)
    branch(authoring, upstream, "B")
    branch(authoring, upstream, "C", 2, "broadcast")


class RecordingMetadataBoundary(StreamGraphAuthoring):
    """Observe compiler submissions; this is not an HTTP acceptance fixture."""

    def __init__(self) -> None:
        super().__init__()
        self.snapshots: list[bytes] = []
        self.reply = SealedStreamGraph(graph_revision_id=uuid4(), graph_digest="d" * 64)
        self.fail_once = False

    def _seal_snapshot(self, snapshot: bytes) -> SealedStreamGraph:
        self.snapshots.append(snapshot)
        if self.fail_once:
            self.fail_once = False
            raise httpx.ReadTimeout("Metadata acknowledgement unknown")
        return self.reply


class TestStreamDagAuthoring(unittest.TestCase):
    def test_dag_profile_never_enters_legacy_reconstruction_on_scope_exit(self):
        execution = StreamExecutionContext(
            flow_run_id=uuid4(),
            generation=1,
            holder_id=uuid4(),
            api_url="http://127.0.0.1:4200/api",
            capability="a" * 64,
            event_private_key=b"b" * 32,
        )
        run = SimpleNamespace(id=execution.flow_run_id, run_count=1)
        context = SimpleNamespace(
            flow=SimpleNamespace(
                streaming=SimpleNamespace(profile="partitioned-dag-v1")
            )
        )
        proposals = []

        async def run_async_scope():
            async with async_stream_flow_scope(run):
                metadata = RecordingMetadataBoundary()
                fork(metadata)
                metadata.seal()
            proposals.append("async terminal proposal reachable")

        with (
            patch("prefect.context.FlowRunContext.get", return_value=context),
            patch(
                "prefect._internal.stream_reconstruction.get_stream_execution",
                return_value=execution,
            ),
            patch(
                "prefect._internal.stream_reconstruction.StreamReconstruction"
            ) as legacy,
        ):
            with stream_flow_scope(run):
                metadata = RecordingMetadataBoundary()
                fork(metadata)
                metadata.seal()
            proposals.append("sync terminal proposal reachable")
            asyncio.run(run_async_scope())
            legacy.assert_not_called()
        self.assertEqual(len(proposals), 2)

    def test_legacy_finite_and_stream_submissions_refuse_before_graph_lookup(self):
        context = SimpleNamespace(
            flow=SimpleNamespace(
                streaming=SimpleNamespace(profile="partitioned-dag-v1")
            )
        )
        with (
            patch("prefect.context.FlowRunContext.get", return_value=context),
            patch("prefect._internal.stream_finite.reconstruction") as finite_lookup,
            patch(
                "prefect._internal.stream_reconstruction.reconstruction"
            ) as legacy_lookup,
        ):
            with self.assertRaisesRegex(
                StreamProtocolError, "topology-only DAG profile"
            ):
                asyncio.run(_reserve(None, (), {}))
            with self.assertRaisesRegex(StreamProtocolError, "StreamGraph"):
                submit_reconstructed_stream(None, (), {}, None)
            finite_lookup.assert_not_called()
            legacy_lookup.assert_not_called()

    def test_real_seal_requires_its_flow_authority_before_transport(self):
        boundary = StreamGraphAuthoring()
        fork(boundary)
        with patch("prefect._internal.stream_dag.get_client") as transport:
            with self.assertRaisesRegex(StreamProtocolError, "Flow authority"):
                boundary.seal()
            transport.assert_not_called()

    def test_constructor_add_and_context_exit_are_local_only(self):
        boundary = RecordingMetadataBoundary()
        with patch(
            "prefect._internal.stream_dag.StreamGraphAuthoring", return_value=boundary
        ):
            with StreamGraph() as graph:
                a = graph.add(
                    dag_source,
                    operator_uid="A",
                    partitions=(StreamPartitionCall(parameters={"split": "one"}),),
                    output=StreamPort("out"),
                )
                self.assertIs(a.output("out").graph_identity, boundary.graph_token)
            self.assertEqual(boundary.snapshots, [])
        with self.assertRaisesRegex(StreamProtocolError, "two operators"):
            boundary.seal()
        self.assertEqual(boundary.snapshots, [])

    def test_context_exception_never_seals(self):
        boundary = RecordingMetadataBoundary()
        with patch(
            "prefect._internal.stream_dag.StreamGraphAuthoring", return_value=boundary
        ):
            with self.assertRaisesRegex(RuntimeError, "application failure"):
                with StreamGraph():
                    raise RuntimeError("application failure")
        self.assertEqual(boundary.snapshots, [])

    def test_complete_fork_freezes_four_physical_tasks_without_execution(self):
        boundary = RecordingMetadataBoundary()
        fork(boundary)
        definition = boundary.definition()
        self.assertEqual(
            [
                (item["operator_uid"], item["parallelism"])
                for item in definition["operators"]
            ],
            [("A", 1), ("B", 1), ("C", 2)],
        )
        self.assertIs(boundary.seal(), boundary.reply)
        self.assertIs(boundary.seal(), boundary.reply)
        self.assertEqual(len(boundary.snapshots), 1)
        self.assertFalse(hasattr(boundary.reply, "start"))
        self.assertFalse(hasattr(boundary.reply, "wait"))
        self.assertFalse(hasattr(boundary.reply, "admit"))

    def test_generic_six_task_graph_is_not_hardcoded_to_four(self):
        boundary = RecordingMetadataBoundary()
        a = source(boundary, 2)
        branch(boundary, a, "B", 2)
        branch(boundary, a, "C", 2, "broadcast")
        self.assertEqual(
            sum(item["parallelism"] for item in boundary.definition()["operators"]), 6
        )
        boundary.seal()
        self.assertEqual(len(boundary.snapshots), 1)

    def test_branch_declaration_order_does_not_change_canonical_definition(self):
        left = RecordingMetadataBoundary()
        right = RecordingMetadataBoundary()
        fork(left)
        a = source(right)
        branch(right, a, "C", 2, "broadcast")
        branch(right, a, "B")
        left.seal()
        right.seal()
        self.assertEqual(left.snapshots, right.snapshots)

    def test_definition_and_parameter_snapshots_are_defensive(self):
        boundary = RecordingMetadataBoundary()
        parameters = {"split": "original"}
        a = boundary.add(
            dag_source,
            operator_uid="A",
            partitions=(StreamPartitionCall(parameters=parameters),),
            input=None,
            output=StreamPort("out"),
        )
        parameters["split"] = "changed"
        branch(boundary, a, "B")
        branch(boundary, a, "C")
        first = boundary.definition()
        first["operators"][0]["parameters"][0]["split"] = "changed-again"
        self.assertEqual(
            boundary.definition()["operators"][0]["parameters"], [{"split": "original"}]
        )

    def test_parameterless_source_is_valid_only_at_one_partition(self):
        boundary = RecordingMetadataBoundary()
        a = boundary.add(
            dag_bare_source,
            operator_uid="A",
            partitions=(StreamPartitionCall(parameters={}),),
            input=None,
            output=StreamPort("out"),
        )
        branch(boundary, a, "B")
        branch(boundary, a, "C")
        boundary.seal()
        other = RecordingMetadataBoundary()
        with self.assertRaisesRegex(StreamProtocolError, "distinct nonempty"):
            other.add(
                dag_bare_source,
                operator_uid="A",
                partitions=(StreamPartitionCall(parameters={}),) * 2,
                input=None,
                output=StreamPort("out"),
            )
        self.assertEqual(other.snapshots, [])

    def test_only_stream_task_contracts_are_accepted(self):
        for value in (ordinary_task, object(), SimpleNamespace(stream=None)):
            with self.subTest(task_type=type(value).__name__):
                boundary = RecordingMetadataBoundary()
                with self.assertRaises(StreamProtocolError):
                    boundary.add(
                        value,
                        operator_uid="A",
                        partitions=(StreamPartitionCall(parameters={}),),
                        input=None,
                        output=StreamPort("out"),
                    )
                self.assertEqual(boundary.snapshots, [])

    def test_strict_partition_descriptors_and_parallelism_bounds(self):
        for descriptors in (
            [],
            (),
            (object(),),
            (StreamPartitionCall(parameters={"split": "x"}),) * 17,
        ):
            with self.subTest(
                descriptors=type(descriptors).__name__, count=len(descriptors)
            ):
                boundary = RecordingMetadataBoundary()
                with self.assertRaises(StreamProtocolError):
                    boundary.add(
                        dag_source,
                        operator_uid="A",
                        partitions=descriptors,
                        input=None,
                        output=StreamPort("out"),
                    )
                self.assertEqual(boundary.snapshots, [])

    def test_duplicate_source_splits_operator_edges_and_subscribers_are_refused(self):
        for duplicate in ("source", "operator", "edge", "subscriber"):
            with self.subTest(duplicate=duplicate):
                boundary = RecordingMetadataBoundary()
                a = source(boundary)
                branch(boundary, a, "B")
                with self.assertRaises(StreamProtocolError):
                    if duplicate == "source":
                        source(boundary, uid="second-root")
                    elif duplicate == "operator":
                        branch(boundary, a, "B")
                    elif duplicate == "edge":
                        branch(boundary, a, "C", edge_uid="A-B")
                    else:
                        branch(boundary, a, "C", subscriber_uid="subscriber-B")
                branch(boundary, a, "C")
                boundary.seal()
                self.assertEqual(len(boundary.snapshots), 1)

    def test_cross_graph_and_forged_unknown_upstream_are_refused(self):
        left = RecordingMetadataBoundary()
        foreign = source(left)
        right = RecordingMetadataBoundary()
        source(right)
        with self.assertRaisesRegex(StreamProtocolError, "another graph"):
            branch(right, foreign, "B")
        forged = StreamOperatorHandle(
            _graph_token=right.graph_token,
            operator_uid="missing",
            output_port=StreamPort("out"),
        )
        with self.assertRaisesRegex(StreamProtocolError, "upstream operator"):
            branch(right, forged, "B")
        self.assertEqual(right.snapshots, [])

    def test_forward_requires_equal_parallelism(self):
        boundary = RecordingMetadataBoundary()
        a = source(boundary)
        with self.assertRaisesRegex(StreamProtocolError, "equal parallelism"):
            branch(boundary, a, "B", 2)
        self.assertEqual(boundary.snapshots, [])

    def test_port_identity_is_qualified_but_input_output_must_differ(self):
        boundary = RecordingMetadataBoundary()
        a = source(boundary)
        with self.assertRaisesRegex(StreamProtocolError, "port UIDs must differ"):
            branch(boundary, a, "B", input_uid="out")
        branch(boundary, a, "B")
        branch(boundary, a, "C")
        boundary.seal()

    def test_schema_digest_mismatch_is_refused_before_metadata(self):
        boundary = RecordingMetadataBoundary()
        a = source(boundary)
        digest = StreamPort("out").schema_digest
        with patch.object(
            StreamPort,
            "schema_digest",
            property(lambda port: "0" * 64 if port.uid == "in" else digest),
        ):
            with self.assertRaisesRegex(
                StreamProtocolError, "matching portable schemas"
            ):
                branch(boundary, a, "B")
        self.assertEqual(boundary.snapshots, [])

    def test_source_only_refuses_but_connected_linear_graph_seals(self):
        boundary = RecordingMetadataBoundary()
        a = source(boundary)
        with self.assertRaises(StreamProtocolError):
            boundary.seal()
        self.assertEqual(boundary.snapshots, [])
        b = branch(boundary, a, "B")
        branch(boundary, b, "C")
        boundary.seal()
        self.assertEqual(len(boundary.snapshots), 1)

    def test_differing_parallelism_linear_graph_uses_generic_dag_wire(self):
        for source_count, target_count, exchange in (
            (1, 1, "forward"),
            (2, 3, "rebalance"),
        ):
            with self.subTest(
                source=source_count, target=target_count, exchange=exchange
            ):
                boundary = RecordingMetadataBoundary()
                a = source(boundary, source_count)
                branch(boundary, a, "B", target_count, exchange)
                operators, edges = boundary._wire_definition(
                    json.dumps(boundary.definition()).encode()
                )
                request = StreamDAGSealRequest.model_validate(
                    {
                        "flow_run_id": str(uuid4()),
                        "operation_id": str(uuid4()),
                        "flow_version": "v1",
                        "operators": operators,
                        "edges": edges,
                    }
                )
                self.assertEqual(len(request.operators), 2)
                self.assertEqual(
                    sum(op.parallelism for op in request.operators),
                    source_count + target_count,
                )
                self.assertEqual(len(request.edges), 1)
                boundary.seal()
                self.assertEqual(len(boundary.snapshots), 1)

    def test_full_128_subtask_bound_and_operator_bound(self):
        boundary = RecordingMetadataBoundary()
        a = source(boundary, 8)
        for index in range(15):
            branch(boundary, a, f"B{index:02}", 8)
        self.assertEqual(
            sum(item["parallelism"] for item in boundary.definition()["operators"]), 128
        )
        with self.assertRaisesRegex(StreamProtocolError, "16 operators"):
            branch(boundary, a, "extra", 1, "broadcast")
        boundary.seal()
        larger = RecordingMetadataBoundary()
        a = source(larger, 9)
        for index in range(14):
            branch(larger, a, f"B{index:02}", 8, "broadcast")
        with self.assertRaisesRegex(StreamProtocolError, "128 physical"):
            branch(larger, a, "overflow", 8, "broadcast")
        self.assertEqual(larger.snapshots, [])

    def test_changed_task_is_refused_before_seal(self):
        for option, value in (
            ("version", "changed"),
            ("tags", {"changed"}),
            ("retries", 3),
            ("timeout_seconds", 1.0),
            ("on_completion_hooks", [lambda *args: None]),
        ):
            with self.subTest(option=option):
                boundary = RecordingMetadataBoundary()
                fork(boundary)
                with patch.object(dag_source, option, value):
                    with self.assertRaises(StreamProtocolError):
                        boundary.seal()
                self.assertEqual(boundary.snapshots, [])

    def test_contextual_tags_are_frozen_per_declaration(self):
        boundary = RecordingMetadataBoundary()
        with TagsContext(current_tags={"source-context"}):
            a = source(boundary)
        with TagsContext(current_tags={"branch-context"}):
            branch(boundary, a, "B")
            branch(boundary, a, "C")
        boundary.seal()
        nodes = json.loads(boundary.snapshots[0])["operators"]
        self.assertEqual(nodes[0]["effective_tags"], ["source-context"])
        self.assertEqual(nodes[1]["effective_tags"], ["branch-context"])

    def test_uncertain_seal_retries_the_same_intent_and_refuses_new_declarations(self):
        boundary = RecordingMetadataBoundary()
        fork(boundary)
        boundary.fail_once = True
        with self.assertRaises(httpx.ReadTimeout):
            boundary.seal()
        with self.assertRaisesRegex(StreamProtocolError, "follow the graph seal"):
            source(boundary, uid="late-source")
        self.assertIs(boundary.seal(), boundary.reply)
        self.assertEqual(boundary.snapshots[0], boundary.snapshots[1])

    def test_compiler_output_satisfies_the_paired_wire_without_rpc(self):
        boundary = StreamGraphAuthoring()
        fork(boundary)
        operators, edges = boundary._wire_definition(
            json.dumps(boundary.definition()).encode()
        )
        request = StreamDAGSealRequest.model_validate(
            {
                "flow_run_id": str(uuid4()),
                "operation_id": str(uuid4()),
                "flow_version": "v1",
                "operators": operators,
                "edges": edges,
            }
        )
        self.assertEqual(sum(item.parallelism for item in request.operators), 4)
        self.assertEqual(
            [item.exchange for item in request.edges], ["forward", "broadcast"]
        )

    def test_recovered_authoring_and_unknown_ack_use_same_flow_operation(self):
        flow_id = uuid4()
        api_url = "http://127.0.0.1:4200/api"
        context = SimpleNamespace(
            flow=SimpleNamespace(
                version="v1", streaming=SimpleNamespace(profile="partitioned-dag-v1")
            ),
            flow_run=SimpleNamespace(
                id=flow_id,
                deployment_id=uuid4(),
                deployment_version="deployed-v1",
                labels={"prefect.native-streaming": "partitioned-dag-v1"},
            ),
        )
        execution = StreamExecutionContext(
            flow_run_id=flow_id,
            generation=1,
            holder_id=uuid4(),
            api_url=api_url,
            capability="a" * 64,
            event_private_key=b"b" * 32,
        )
        calls = []

        class LostMetadataAck:
            def __init__(self) -> None:
                self.api_url = api_url

            def __enter__(self):
                return self

            def __exit__(self, *args: Any):
                return False

            def seal_stream_dag(self, **kwargs: Any):
                calls.append(kwargs)
                raise httpx.ReadTimeout("Metadata result is unknown")

        with (
            patch(
                "prefect._internal.stream_dag.FlowRunContext.get", return_value=context
            ),
            patch(
                "prefect._internal.stream_dag.get_stream_execution",
                return_value=execution,
            ),
            patch(
                "prefect._internal.stream_dag.PREFECT_API_URL",
                SimpleNamespace(value=lambda: api_url),
            ),
            patch(
                "prefect._internal.stream_dag.get_client",
                return_value=LostMetadataAck(),
            ),
        ):
            original = StreamGraphAuthoring()
            fork(original)
            for _ in range(2):
                with self.assertRaises(httpx.ReadTimeout):
                    original.seal()
            recovered = StreamGraphAuthoring()
            fork(recovered)
            with self.assertRaises(httpx.ReadTimeout):
                recovered.seal()
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(calls[0], calls[2])
        self.assertEqual(
            calls[0]["operation_id"],
            uuid5(NAMESPACE_URL, f"partitioned-dag-v1/{flow_id}"),
        )


if __name__ == "__main__":
    unittest.main()
