"""Exact DAG routed wire shapes, key tags and immutable transport snapshots."""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from prefect.server.schemas import stream_routes as s
from prefect.server.schemas.streams import SourceBinding


def context(*, keyed: bool = False) -> s.StreamDAGRouteContext:
    return s.StreamDAGRouteContext(
        graph_revision_id=uuid4(),
        graph_digest="11" * 32,
        routing_digest="22" * 32,
        producer_subtask_id=uuid4(),
        operator_uid="A",
        output_port_uid="out",
        subtask_index=0,
        has_keyed_outgoing=keyed,
    )


def source_body(*, keyed: bool = False) -> dict[str, object]:
    binding = SourceBinding(
        flow_run_id=uuid4(),
        stream_id=uuid4(),
        task_run_id=uuid4(),
        owner_id=uuid4(),
        attempt=1,
        owner_epoch=1,
    )
    return {
        "context": context(keyed=keyed).model_dump(mode="json"),
        "binding": binding.model_dump(mode="json"),
        "position": 1,
        "value": {"documents": [1]},
    }


def outcome_body(*, role: str = "SOURCE", count: int = 1) -> dict[str, object]:
    payload_id = uuid4()
    refs = [
        s.StreamDAGRouteReference(
            edge_id=UUID(int=1),
            edge_partition_id=UUID(int=i + 10),
            stream_id=UUID(int=i + 1000),
            sequence=1,
            record_id=UUID(int=i + 10000),
            payload_record_id=payload_id,
        ).model_dump(mode="json")
        for i in range(count)
    ]
    return {
        "receipt_id": str(uuid4()),
        "graph_revision_id": str(uuid4()),
        "routing_digest": "11" * 32,
        "semantic_digest": "22" * 32,
        "outcome_digest": "33" * 32,
        "producer_stream_id": str(uuid4()),
        "producer_position": 1,
        "producer_role": role,
        "kind": "YIELD",
        "input_stream_id": None if role == "SOURCE" else str(uuid4()),
        "input_record_id": None if role == "SOURCE" else str(uuid4()),
        "input_payload_record_id": None if role == "SOURCE" else str(uuid4()),
        "input_payload_value_digest": None if role == "SOURCE" else "44" * 32,
        "input_sequence": None if role == "SOURCE" else 1,
        "consumer_id": None if role == "SOURCE" else str(uuid4()),
        "producer_sequence": 1,
        "output_ordinal": 0,
        "payload_record_id": str(payload_id),
        "payload_value_digest": "55" * 32,
        "payload_size_bytes": 1,
        "references": refs,
    }


def test_output_key_schema_is_exact_discriminated_union():
    schema = s.StreamDAGSourceAppendRequest.model_json_schema()
    union = next(
        item
        for item in schema["properties"]["output_key"]["anyOf"]
        if "discriminator" in item
    )
    assert union["discriminator"]["propertyName"] == "kind"
    assert set(union["discriminator"]["mapping"]) == {"N", "B", "I", "S", "Y"}
    for target in union["discriminator"]["mapping"].values():
        assert (
            schema["$defs"][target.rsplit("/", 1)[1]]["additionalProperties"] is False
        )


@pytest.mark.parametrize(
    "key",
    [
        {"kind": "N"},
        {"kind": "B", "value": True},
        {"kind": "I", "value": "-9223372036854775808"},
        {"kind": "S", "value": ""},
        {"kind": "Y", "value": "AA=="},
    ],
)
def test_canonical_keys_model_dump_validate_roundtrip(key: dict[str, object]):
    body = source_body(keyed=True) | {"output_key": key}
    model = s.StreamDAGSourceAppendRequest.model_validate(body)
    assert s.StreamDAGSourceAppendRequest.model_validate(
        model.model_dump(mode="json")
    ).model_dump(mode="json") == model.model_dump(mode="json")


@pytest.mark.parametrize(
    "key",
    [
        {"kind": "N", "value": None},
        {"kind": "B", "value": 1},
        {"kind": "I", "value": "-0"},
        {"kind": "I", "value": "9223372036854775808"},
        {"kind": "S", "value": "\ud800"},
        {"kind": "Y", "value": "AB=="},
        {"kind": "Y", "value": "AA"},
        {"kind": "S", "value": "x", "extra": 1},
    ],
)
def test_malformed_or_extra_key_fields_are_refused(key: dict[str, object]):
    with pytest.raises(ValidationError):
        s.StreamDAGSourceAppendRequest.model_validate(
            source_body(keyed=True) | {"output_key": key}
        )


def test_request_copies_nested_inputs_and_key_model_is_frozen():
    value: dict[str, object] = {"items": [1]}
    key: dict[str, object] = {"kind": "I", "value": "12"}
    request = s.StreamDAGSourceAppendRequest.model_validate(
        source_body(keyed=True) | {"value": value, "output_key": key}
    )
    value["items"] = [999]
    key["value"] = "999"
    assert request.value == {"items": [1]}
    assert (
        isinstance(request.output_key, s.StreamIntegerOutputKey)
        and request.output_key.value == "12"
    )
    with pytest.raises(ValidationError, match="frozen"):
        request.output_key.value = "13"


@pytest.mark.parametrize("role", ["SOURCE", "OPERATOR"])
def test_outcome_source_and_operator_identity_roundtrip(role: str):
    model = s.StreamDAGRouteOutcome.model_validate(outcome_body(role=role))
    assert (
        s.StreamDAGRouteOutcome.model_validate(model.model_dump(mode="json")) == model
    )
    bad = model.model_dump(mode="json")
    bad["input_payload_record_id"] = str(uuid4()) if role == "SOURCE" else None
    with pytest.raises(ValidationError):
        s.StreamDAGRouteOutcome.model_validate(bad)


def test_outcome_requires_canonical_order_and_unique_refs():
    body = outcome_body(count=2)
    model = s.StreamDAGRouteOutcome.model_validate(body)
    with pytest.raises(ValidationError, match="canonical"):
        s.StreamDAGRouteOutcome.model_validate(
            body | {"references": list(reversed(model.references))}
        )
    with pytest.raises(ValidationError, match="distinct"):
        s.StreamDAGRouteOutcome.model_validate(
            body | {"references": [model.references[0], model.references[0]]}
        )


def test_route_outcome_accepts_128_refs_and_refuses_129():
    assert (
        len(s.StreamDAGRouteOutcome.model_validate(outcome_body(count=128)).references)
        == 128
    )
    with pytest.raises(ValidationError):
        s.StreamDAGRouteOutcome.model_validate(outcome_body(count=129))


def test_return_is_state_only_even_with_keyed_outgoing_edges():
    producer = context(keyed=True)
    binding = {
        "flow_run_id": str(uuid4()),
        "consumer_id": str(uuid4()),
        "input_stream_id": str(uuid4()),
        "output_stream_id": str(uuid4()),
        "owner_id": str(uuid4()),
        "attempt": 1,
        "owner_epoch": 1,
    }
    token = binding | {
        "record_id": str(uuid4()),
        "sequence": 1,
        "delivery_id": str(uuid4()),
    }
    state = {"nested": [1]}
    request = s.StreamDAGConsumerCompleteRequest.model_validate(
        {
            "context": producer.model_dump(mode="json"),
            "binding": binding,
            "token": token,
            "kind": "RETURN",
            "value": None,
            "managed_state": state,
        }
    )
    state["nested"] = [999]
    assert request.managed_state == {"nested": [1]} and request.output_key is None
    with pytest.raises(ValidationError, match="no output"):
        s.StreamDAGConsumerCompleteRequest.model_validate(
            request.model_dump(mode="json") | {"output_key": {"kind": "N"}}
        )
    yielded = request.model_dump(mode="json") | {
        "kind": "YIELD",
        "output_key": {"kind": "N"},
    }
    assert s.StreamDAGConsumerCompleteRequest.model_validate(yielded).value is None


def test_key_is_required_exactly_when_a_payload_has_keyed_outgoing():
    with pytest.raises(ValidationError):
        s.StreamDAGSourceAppendRequest.model_validate(source_body(keyed=True))
    with pytest.raises(ValidationError):
        s.StreamDAGSourceAppendRequest.model_validate(
            source_body() | {"output_key": {"kind": "N"}}
        )
    assert s.StreamDAGSourceAppendRequest.model_validate(source_body()).value == {
        "documents": [1]
    }
