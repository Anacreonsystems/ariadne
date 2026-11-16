"""Pure bounded DAG terminal routing and versioned immutable digest witnesses."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast
from uuid import UUID

from prefect._internal.stream_key_wire import StreamKey
from prefect._internal.stream_routing import KeyEncodingError, encode_tagged_key_v1

MAX_ROUTE_REFERENCES = 128
_DIGEST = re.compile(r"[0-9a-f]{64}")
_EPHEMERAL_FIELDS = frozenset(
    {
        "attempt",
        "owner_id",
        "owner_epoch",
        "grant_id",
        "run_count",
        "delivery_id",
        "worker_id",
        "process_id",
        "physical_epoch",
        "assignment_epoch",
        "flow_generation",
        "holder_id",
        "capability",
        "event_key_id",
        "child_process_id",
        "process_start_ticks",
        "lease_id",
        "credential_version",
        "task_run_run_count",
    }
)


class RoutePlanningError(ValueError):
    """A graph/terminal witness is outside the sealed routing protocol."""


def _digest(value: str) -> None:
    if type(value) is not str or _DIGEST.fullmatch(value) is None:
        raise RoutePlanningError("A canonical lowercase SHA-256 digest is required")


def _positive(value: int, maximum: int | None = None) -> None:
    if type(value) is not int or value < 1 or (maximum is not None and value > maximum):
        raise RoutePlanningError("A bounded exact positive integer is required")


def _validate_json(value: object, active: set[int], *, semantic: bool) -> None:
    kind = type(value)
    if value is None or kind is bool or kind is int:
        return
    if kind is str:
        cast(str, value).encode("utf-8", errors="strict")
        return
    if kind is float:
        if not math.isfinite(cast(float, value)):
            raise RoutePlanningError("Canonical finite UTF-8 JSON is required")
        return
    if kind is not list and kind is not dict:
        raise RoutePlanningError("Canonical exact JSON values are required")
    identity = id(value)
    if identity in active:
        raise RoutePlanningError("Canonical JSON cannot contain cycles")
    active.add(identity)
    try:
        if kind is dict:
            for key, item in cast(dict[object, object], value).items():
                if type(key) is not str:
                    raise RoutePlanningError("JSON object names must be exact strings")
                _validate_json(key, active, semantic=semantic)
                if semantic and key in _EPHEMERAL_FIELDS:
                    raise RoutePlanningError(
                        "Ephemeral execution identity is not a semantic turn coordinate"
                    )
                _validate_json(item, active, semantic=semantic)
        else:
            for item in cast(list[object], value):
                _validate_json(item, active, semantic=semantic)
    finally:
        active.remove(identity)


def _canonical_json(value: object, *, semantic: bool = False) -> bytes:
    try:
        _validate_json(value, set(), semantic=semantic)
        return json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")
    except RoutePlanningError:
        raise
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise RoutePlanningError("Canonical finite UTF-8 JSON is required") from error


def canonical_route_json(value: object) -> bytes:
    """Encode exact JSON values without coercing keys, types or Unicode."""
    return _canonical_json(value)


def graph_routing_digest(graph_digest: str) -> str:
    """Bind every exchange/parallelism/key rule already sealed by graph_digest."""
    _digest(graph_digest)
    return hashlib.sha256(
        b"prefect-dag-terminal-routing-v1\0" + bytes.fromhex(graph_digest)
    ).hexdigest()


@dataclass(frozen=True)
class RouteTarget:
    edge_id: UUID
    edge_partition_id: UUID
    stream_id: UUID
    partition_index: int

    def __post_init__(self) -> None:
        if type(self) is not RouteTarget or any(
            type(value) is not UUID
            for value in (self.edge_id, self.edge_partition_id, self.stream_id)
        ):
            raise RoutePlanningError(
                "Route target identities must be exact UUID values"
            )
        if type(self.partition_index) is not int or not 0 <= self.partition_index < 16:
            raise RoutePlanningError(
                "Route target index must be an exact bounded integer"
            )


@dataclass(frozen=True)
class RouteEdge:
    edge_id: UUID
    exchange: Literal["forward", "rebalance", "broadcast", "keyed"]
    producer_parallelism: int
    target_parallelism: int
    targets: tuple[RouteTarget, ...]
    routing_version: Literal[1] = 1
    key_group_count: int | None = None

    def __post_init__(self) -> None:
        # Validate exact bounds before range(), tuple traversal or user callbacks.
        _positive(self.producer_parallelism, 16)
        _positive(self.target_parallelism, 16)
        if (
            type(self) is not RouteEdge
            or type(self.edge_id) is not UUID
            or type(self.targets) is not tuple
            or len(self.targets) != self.target_parallelism
            or any(type(target) is not RouteTarget for target in self.targets)
        ):
            raise RoutePlanningError(
                "Sealed edges require an exact UUID and immutable target tuple"
            )
        if tuple(target.partition_index for target in self.targets) != tuple(
            range(self.target_parallelism)
        ):
            raise RoutePlanningError(
                "Target tuple must have canonical partition-index order"
            )
        if type(self.routing_version) is not int or self.routing_version != 1:
            raise RoutePlanningError("Only routing version one is supported")
        if type(self.exchange) is not str or self.exchange not in {
            "forward",
            "rebalance",
            "broadcast",
            "keyed",
        }:
            raise RoutePlanningError("Unknown exchange")
        if any(
            type(target.partition_index) is not int or target.edge_id != self.edge_id
            for target in self.targets
        ):
            raise RoutePlanningError("Target identity does not match its sealed edge")
        if len({target.edge_partition_id for target in self.targets}) != len(
            self.targets
        ) or len({target.stream_id for target in self.targets}) != len(self.targets):
            raise RoutePlanningError(
                "Target partition and stream identities must be distinct"
            )
        if (
            self.exchange == "forward"
            and self.producer_parallelism != self.target_parallelism
        ):
            raise RoutePlanningError("Forward requires equal parallelism")
        if self.exchange == "keyed":
            if self.key_group_count is None:
                raise RoutePlanningError(
                    "Keyed routing requires its sealed group count"
                )
            _positive(self.key_group_count, (1 << 31) - 1)
            if self.key_group_count < self.target_parallelism:
                raise RoutePlanningError("Key groups must cover every target")
        elif self.key_group_count is not None:
            raise RoutePlanningError("Only keyed edges declare key groups")


def plan_route_targets(
    edges: tuple[RouteEdge, ...],
    producer_index: int,
    producer_sequence: int,
    *,
    physical_subtask_count: int,
    key_present: bool,
    key: StreamKey = None,
) -> tuple[RouteTarget, ...]:
    """Select from immutable target manifests, never current placement or counters."""
    if type(edges) is not tuple or any(type(edge) is not RouteEdge for edge in edges):
        raise RoutePlanningError("Outgoing edges must be an immutable sealed tuple")
    _positive(producer_sequence)
    _positive(physical_subtask_count, MAX_ROUTE_REFERENCES)
    if (
        physical_subtask_count < 2
        or type(producer_index) is not int
        or not 0 <= producer_index < 16
        or producer_index >= physical_subtask_count
        or type(key_present) is not bool
    ):
        raise RoutePlanningError("Invalid producer/graph coordinate")
    if len({edge.edge_id for edge in edges}) != len(edges):
        raise RoutePlanningError("Outgoing edge identities must be distinct")
    if any(producer_index >= edge.producer_parallelism for edge in edges):
        raise RoutePlanningError("Producer index is outside its sealed parallelism")
    if edges and len({edge.producer_parallelism for edge in edges}) != 1:
        raise RoutePlanningError(
            "All outgoing edges belong to the same producer operator"
        )
    all_targets = [target for edge in edges for target in edge.targets]
    if len({target.edge_partition_id for target in all_targets}) != len(
        all_targets
    ) or len({target.stream_id for target in all_targets}) != len(all_targets):
        raise RoutePlanningError("An edge target cannot alias another subscription")
    # Connected single-input graphs assign each target subtask exactly one input.
    # Therefore worst-case fanout is <= N-ownP <=127, without a hidden limit32.
    own_parallelism = edges[0].producer_parallelism if edges else 1
    if len(all_targets) > physical_subtask_count - own_parallelism:
        raise RoutePlanningError(
            "Outgoing targets exceed the sealed single-input graph"
        )
    keyed = any(edge.exchange == "keyed" for edge in edges)
    if keyed != key_present or (not key_present and key is not None):
        raise RoutePlanningError(
            "A key is required exactly when an outgoing edge is keyed"
        )
    try:
        key_bytes = encode_tagged_key_v1(key) if key_present else None
    except KeyEncodingError as error:
        raise RoutePlanningError(
            "Canonical tagged-key-v1 routing keys are required"
        ) from error
    key_hash = (
        int.from_bytes(hashlib.sha256(key_bytes).digest(), "big")
        if key_bytes is not None
        else None
    )
    selected: list[RouteTarget] = []
    for edge in edges:
        if edge.exchange == "broadcast":
            selected.extend(edge.targets)
            continue
        if edge.exchange == "forward":
            index = producer_index
        elif edge.exchange == "rebalance":
            index = (producer_index + producer_sequence - 1) % edge.target_parallelism
        else:
            assert key_hash is not None and edge.key_group_count is not None
            group = key_hash % edge.key_group_count
            index = (group * edge.target_parallelism) // edge.key_group_count
        selected.append(
            next(target for target in edge.targets if target.partition_index == index)
        )
    if len(selected) > MAX_ROUTE_REFERENCES:
        raise RoutePlanningError("Selected references exceed the 128-reference bound")
    return tuple(
        sorted(
            selected,
            key=lambda target: (
                target.edge_id.bytes,
                target.edge_partition_id.bytes,
                target.stream_id.bytes,
            ),
        )
    )


def route_semantic_digest(material: Mapping[str, object]) -> str:
    """Hash an explicit stable material assembled by the authenticated store."""
    if type(material) is not dict:
        raise RoutePlanningError("Semantic material must be an exact JSON object")
    return hashlib.sha256(
        b"prefect-dag-route-semantic-v1\0"
        + _canonical_json(cast(dict[str, object], material).copy(), semantic=True)
    ).hexdigest()


def route_outcome_digest(outcome: Mapping[str, object]) -> str:
    if type(outcome) is not dict:
        raise RoutePlanningError("A route outcome must be an exact JSON object")
    material = cast(dict[str, object], outcome).copy()
    material.pop("outcome_digest", None)
    return hashlib.sha256(
        b"prefect-dag-route-outcome-v1\0" + canonical_route_json(material)
    ).hexdigest()
