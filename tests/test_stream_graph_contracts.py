"""Public DAG metadata contracts without a server or business execution."""

import hashlib
import inspect
import json
import unittest
from dataclasses import FrozenInstanceError
from typing import Any, get_args, get_origin, get_type_hints
from uuid import uuid4

import cloudpickle

from prefect import Task, flow
from prefect.streams import (
    KeyedStreamOutput,
    NativeStreamFlowConfig,
    PartitionedStream,
    SealedStreamGraph,
    StreamExchange,
    StreamGraph,
    StreamInput,
    StreamOperatorHandle,
    StreamOutput,
    StreamPartitionCall,
    StreamPort,
    StreamProtocolError,
)


class TestStreamGraphPublicContracts(unittest.TestCase):
    def endpoint(self) -> StreamOutput[Any]:
        operator: StreamOperatorHandle[Any] = StreamOperatorHandle(
            _graph_token=object(),
            operator_uid="A",
            output_port=StreamPort("documents"),
        )
        return operator.output("documents")

    def test_port_schema_has_the_frozen_portable_identity(self):
        descriptor = {"id": "json-value-v1", "type": "json-value", "version": 1}
        encoded = json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        port = StreamPort("documents")
        self.assertEqual(port.schema_id, "json-value-v1")
        self.assertEqual(port.schema_version, 1)
        self.assertEqual(
            port.schema_digest,
            "d93f54187397ae62d4e1b98c56edab2a67a9d10422b028dfdea87ba6616a849c",
        )
        self.assertEqual(port.schema_digest, hashlib.sha256(encoded).hexdigest())

    def test_dag_flow_declares_per_operator_parallelism_with_none(self):
        config = NativeStreamFlowConfig(
            profile="partitioned-dag-v1", partition_count=None
        )
        self.assertIsNone(config.partition_count)
        self.assertEqual(NativeStreamFlowConfig().partition_count, 1)
        self.assertEqual(
            NativeStreamFlowConfig(
                profile="partitioned-fixed-v1", partition_count=2
            ).partition_count,
            2,
        )

    def test_flow_profiles_refuse_incompatible_or_coerced_partition_counts(self):
        for profile, counts in (
            ("partitioned-dag-v1", (0, 1, 2, True, False, 1.0, "1")),
            ("bounded-linear-v1", (None, 0, 2, True, 1.0, "1")),
            ("partitioned-fixed-v1", (None, 0, 1, True, 2.0, "2")),
        ):
            for count in counts:
                with self.subTest(profile=profile, count=count):
                    with self.assertRaises(StreamProtocolError):
                        NativeStreamFlowConfig(profile=profile, partition_count=count)  # type: ignore[arg-type]
        with self.assertRaises(StreamProtocolError):
            NativeStreamFlowConfig(profile="partitioned-dag-v1")

    def test_dag_flow_construction_and_serialization_execute_no_business_code(self):
        calls = []
        config = NativeStreamFlowConfig(
            profile="partitioned-dag-v1", partition_count=None
        )

        @flow(version="v1", retries=2, streaming=config)
        def dag_definition():
            calls.append("business")

        restored = cloudpickle.loads(cloudpickle.dumps(dag_definition))
        self.assertEqual(restored.streaming, config)
        self.assertEqual(restored.with_options(name="renamed").streaming, config)
        self.assertEqual(calls, [])

    def test_port_refuses_nonportable_schema_names(self):
        for schema in (None, "dict", "builtins.dict", "json-value-v2", 1):
            with self.subTest(schema=schema):
                with self.assertRaises(StreamProtocolError):
                    StreamPort("documents", schema_id=schema)  # type: ignore[arg-type]

    def test_port_refuses_invalid_stable_names(self):
        for name in ("", "a" * 65, "a b", "a/b", "é", 1, True, None):
            with self.subTest(name=name):
                with self.assertRaises(StreamProtocolError):
                    StreamPort(name)  # type: ignore[arg-type]
        self.assertEqual(StreamPort("input_1.v1-A").uid, "input_1.v1-A")

    def test_port_is_immutable(self):
        port = StreamPort("documents")
        with self.assertRaises(FrozenInstanceError):
            port.uid = "changed"  # type: ignore[misc]

    def test_nonkeyed_exchanges_carry_no_key_groups(self):
        for kind in ("forward", "rebalance", "broadcast"):
            with self.subTest(kind=kind):
                exchange = StreamExchange(kind)  # type: ignore[arg-type]
                self.assertEqual(exchange.routing_version, 1)
                self.assertIsNone(exchange.key_group_count)
                with self.assertRaises(StreamProtocolError):
                    StreamExchange(kind, key_group_count=16)  # type: ignore[arg-type]

    def test_keyed_exchange_requires_exact_positive_group_count(self):
        for groups in (None, 0, -1, True, 1.0, "16", 2**31):
            with self.subTest(groups=groups):
                with self.assertRaises(StreamProtocolError):
                    StreamExchange("keyed", key_group_count=groups)  # type: ignore[arg-type]
        self.assertEqual(
            StreamExchange("keyed", key_group_count=16).key_group_count, 16
        )

        self.assertEqual(
            StreamExchange("keyed", key_group_count=2**31 - 1).key_group_count,
            2**31 - 1,
        )

    def test_exchange_refuses_unknown_or_coerced_versions(self):
        for kind in ("unknown", "forward-v1", None):
            with self.subTest(kind=kind):
                with self.assertRaises(StreamProtocolError):
                    StreamExchange(kind)  # type: ignore[arg-type]
        for version in (True, 1.0, "1", 0, 2):
            with self.subTest(version=version):
                with self.assertRaises(StreamProtocolError):
                    StreamExchange("forward", routing_version=version)  # type: ignore[arg-type]

    def test_endpoint_is_stable_and_bound_to_its_graph_identity(self):
        token = object()
        operator: StreamOperatorHandle[Any] = StreamOperatorHandle(
            _graph_token=token,
            operator_uid="A",
            output_port=StreamPort("documents"),
        )
        first = operator.output("documents")
        self.assertIs(first, operator.output("documents"))
        self.assertIs(first.graph_identity, token)
        self.assertEqual(first.operator_uid, "A")
        self.assertEqual(first.port.uid, "documents")
        self.assertIsNot(first, self.endpoint())

    def test_endpoint_has_no_physical_execution_surface(self):
        endpoint = self.endpoint()
        for name in ("partitions", "wait", "collect", "start", "admit"):
            with self.subTest(name=name):
                self.assertFalse(hasattr(endpoint, name))

    def test_operator_refuses_an_undeclared_output(self):
        operator: StreamOperatorHandle[Any] = StreamOperatorHandle(
            _graph_token=object(),
            operator_uid="A",
            output_port=StreamPort("documents"),
        )
        with self.assertRaises(StreamProtocolError):
            operator.output("missing")

    def test_input_keeps_subscriber_and_edge_identity_separate(self):
        endpoint = self.endpoint()
        edge = StreamInput(
            port=StreamPort("input-documents"),
            upstream=endpoint,
            edge_uid="A-B",
            subscriber_uid="B-documents",
            exchange=StreamExchange("forward"),
        )
        self.assertIs(edge.upstream, endpoint)
        self.assertEqual(edge.port.uid, "input-documents")
        self.assertEqual(edge.edge_uid, "A-B")
        self.assertEqual(edge.subscriber_uid, "B-documents")
        with self.assertRaises(FrozenInstanceError):
            edge.subscriber_uid = "changed"  # type: ignore[misc]

    def test_input_refuses_implicit_endpoints_and_bad_ids(self):
        valid = {
            "port": StreamPort("input"),
            "upstream": self.endpoint(),
            "edge_uid": "A-B",
            "subscriber_uid": "B-input",
            "exchange": StreamExchange("forward"),
        }
        for field, value in (
            ("port", "input"),
            ("upstream", "A"),
            ("exchange", "forward"),
            ("edge_uid", ""),
            ("subscriber_uid", "has space"),
        ):
            with self.subTest(field=field):
                with self.assertRaises(StreamProtocolError):
                    StreamInput(**{**valid, field: value})  # type: ignore[arg-type]

    def test_sealed_handle_is_metadata_only(self):
        graph_id = uuid4()
        handle = SealedStreamGraph(graph_revision_id=graph_id, graph_digest="a" * 64)
        self.assertEqual(handle.graph_revision_id, graph_id)
        self.assertEqual(handle.profile, "partitioned-dag-v1")
        for name in ("admit", "start", "wait", "partitions", "collect"):
            with self.subTest(name=name):
                self.assertFalse(hasattr(handle, name))
                self.assertFalse(hasattr(StreamGraph, name))
        with self.assertRaises(FrozenInstanceError):
            handle.graph_digest = "b" * 64  # type: ignore[misc]

    def test_sealed_handle_refuses_coerced_identity_or_digest(self):
        with self.assertRaises(StreamProtocolError):
            SealedStreamGraph(graph_revision_id=str(uuid4()), graph_digest="a" * 64)  # type: ignore[arg-type]
        for digest in ("A" * 64, "a" * 63, "z" * 64, None):
            with self.subTest(digest=digest):
                with self.assertRaises(StreamProtocolError):
                    SealedStreamGraph(graph_revision_id=uuid4(), graph_digest=digest)  # type: ignore[arg-type]

    def test_keyed_output_distinguishes_null_from_absent_key(self):
        value = {"ref": "object://documents/200", "key": "business-owned"}
        output = KeyedStreamOutput(value, None)
        self.assertIs(output.value, value)
        self.assertIsNone(output.key)
        with self.assertRaises(TypeError):
            KeyedStreamOutput(value)  # type: ignore[call-arg]

    def test_keyed_output_preserves_exact_supported_key_types(self):
        for key in (None, False, True, -(1 << 63), (1 << 63) - 1, "é", b"\0"):
            with self.subTest(key=key):
                output = KeyedStreamOutput({"ref": "object://document"}, key)
                self.assertEqual(output.key, key)
                self.assertIs(type(output.key), type(key))

    def test_keyed_output_refuses_noncanonical_and_oversized_keys(self):
        for key in (
            1.0,
            float("nan"),
            float("inf"),
            -0.0,
            1 << 63,
            -(1 << 63) - 1,
            [],
            {},
            object(),
            "\ud800",
            "a" * 4092,
            b"a" * 4092,
        ):
            with self.subTest(key_type=type(key).__name__):
                with self.assertRaises(StreamProtocolError):
                    KeyedStreamOutput(None, key)  # type: ignore[arg-type]
        KeyedStreamOutput(None, b"a" * 4091)

    def test_graph_add_accepts_variable_partition_declarations(self):
        annotation = get_type_hints(StreamGraph.add, localns={"Task": Task})[
            "partitions"
        ]
        self.assertIs(get_origin(annotation), tuple)
        self.assertEqual(get_args(annotation), (StreamPartitionCall, Ellipsis))
        self.assertEqual(
            list(inspect.signature(StreamGraph.add).parameters),
            ["self", "task", "operator_uid", "partitions", "output", "input"],
        )

    def test_fixed_p2_submission_retains_its_original_typing(self):
        annotations = get_type_hints(Task.submit_partitioned_stream)
        self.assertEqual(
            get_args(annotations["partitions"]),
            (StreamPartitionCall, StreamPartitionCall),
        )
        self.assertIs(get_origin(annotations["return"]), PartitionedStream)
        partition_types = get_args(
            get_type_hints(PartitionedStream.partitions.fget)["return"]
        )
        self.assertEqual(len(partition_types), 2)
        self.assertEqual(partition_types[0], partition_types[1])


if __name__ == "__main__":
    unittest.main()
