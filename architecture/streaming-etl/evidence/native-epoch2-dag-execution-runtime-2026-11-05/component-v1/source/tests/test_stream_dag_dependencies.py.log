"""Real ordinary Task engines refuse unstarted DAG metadata dependencies."""

from typing import Any
from uuid import uuid4

import pytest

from prefect import flow, task
from prefect._internal.stream_finite import _reserve
from prefect.cache_policies import NO_CACHE
from prefect.streams import (
    PartitionedStream,
    RunningStreamGraph,
    SealedStreamGraph,
    Stream,
    StreamGraph,
    StreamOperatorHandle,
    StreamPort,
    StreamProtocolError,
)
from prefect.utilities.annotations import opaque, quote
from prefect.utilities.engine import refuse_stream_graph_dependencies


def graph_metadata() -> list[Any]:
    operator = StreamOperatorHandle(
        _graph_token=object(), operator_uid="A", output_port=StreamPort("out")
    )
    return [
        StreamGraph(),
        operator,
        operator.output("out"),
        SealedStreamGraph(graph_revision_id=uuid4(), graph_digest="a" * 64),
        RunningStreamGraph(
            graph_revision_id=uuid4(),
            graph_digest="a" * 64,
            routing_digest="b" * 64,
        ),
    ]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.timeout(180)
async def test_ordinary_task_body_is_not_entered_for_dag_metadata(asynchronous: bool):
    entered = []

    @task(cache_policy=NO_CACHE, persist_result=False)
    def sync_body(value: Any):
        entered.append(value)
        raise AssertionError("A Task body received graph declaration metadata")

    @task(cache_policy=NO_CACHE, persist_result=False)
    async def async_body(value: Any):
        entered.append(value)
        raise AssertionError("An async Task body received graph declaration metadata")

    for metadata in graph_metadata():
        for value in (
            metadata,
            {"nested": [({"input": metadata},)]},
            quote(metadata),
            opaque(metadata),
            {metadata},
            frozenset({metadata}),
            {"nested": [{"values": {metadata}}]},
        ):
            with pytest.raises(StreamProtocolError, match="metadata"):
                if asynchronous:
                    await async_body(value)
                else:
                    sync_body(value)
    assert entered == []


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("metadata_index", [2, 4])
@pytest.mark.timeout(180)
async def test_task_future_result_cannot_deliver_metadata_to_a_body(
    asynchronous: bool, metadata_index: int
):
    entered = []
    metadata = graph_metadata()[metadata_index]

    @task(cache_policy=NO_CACHE, persist_result=False)
    def producer():
        return {"nested": [(metadata,)]}

    @task(cache_policy=NO_CACHE, persist_result=False)
    def sync_consumer(value: Any):
        entered.append(value)

    @task(cache_policy=NO_CACHE, persist_result=False)
    async def async_consumer(value: Any):
        entered.append(value)

    @flow(persist_result=False)
    def sync_pipeline():
        return sync_consumer(producer.submit())

    @flow(persist_result=False)
    async def async_pipeline():
        return await async_consumer(producer.submit())

    with pytest.raises(StreamProtocolError, match="metadata"):
        if asynchronous:
            await async_pipeline()
        else:
            sync_pipeline()
    assert entered == []


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_ordinary_task_parameters_still_enter_the_body(asynchronous: bool):
    entered = []
    value = {
        "records": [1, {"reference": "object://document"}],
        "flags": (None, True),
        "sets": {1, 2, "ordinary"},
        "frozen": frozenset({1, "ordinary"}),
    }

    @task(cache_policy=NO_CACHE, persist_result=False)
    def sync_body(parameter: Any):
        entered.append(parameter)
        return parameter

    @task(cache_policy=NO_CACHE, persist_result=False)
    async def async_body(parameter: Any):
        entered.append(parameter)
        return parameter

    result = await async_body(value) if asynchronous else sync_body(value)
    assert result == value
    assert entered == [value]


@pytest.mark.parametrize("metadata_index", range(5))
async def test_finite_reservation_refuses_metadata_before_flow_or_task_lookup(
    metadata_index: int,
):
    # A missing Task/Flow would raise another error if reservation were reached.
    # The shared dependency check must run before any manifest reservation.
    metadata = graph_metadata()[metadata_index]
    with pytest.raises(StreamProtocolError, match="metadata"):
        await _reserve(None, (), {"nested": [({"input": metadata},)]})


@pytest.mark.parametrize("metadata_index", range(5))
def test_dependency_guard_handles_container_cycles_and_ordinary_values(
    metadata_index: int,
):
    cycle: list[Any] = []
    cycle.append(cycle)
    refuse_stream_graph_dependencies(
        {"plain": [1, "data", (None, True)], "cycle": cycle}
    )
    cycle.append(graph_metadata()[metadata_index])
    with pytest.raises(StreamProtocolError, match="metadata"):
        refuse_stream_graph_dependencies(cycle)


def test_guard_preserves_existing_physical_and_partitioned_stream_types():
    stream = Stream(object(), str(uuid4()))
    refuse_stream_graph_dependencies(
        {
            "physical": stream,
            "bounded": stream.collect(limit=1),
            "p2": PartitionedStream(),
        }
    )
