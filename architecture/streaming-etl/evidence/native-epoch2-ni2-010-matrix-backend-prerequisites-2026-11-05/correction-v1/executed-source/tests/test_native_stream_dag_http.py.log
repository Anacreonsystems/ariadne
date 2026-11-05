"""Public DAG authoring against protected TCP/PostgreSQL metadata APIs.

Enrollment uses the metadata fixture's synthetic launcher registration. No
ProcessWorker, subtask process, Flow body or stream Task body executes here.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy.ext.asyncio import AsyncSession

from prefect import flow
from prefect._internal.stream_execution import (
    StreamExecutionContext,
    stream_execution_context,
)
from prefect._internal.stream_reconstruction import (
    async_stream_flow_scope,
    stream_flow_scope,
)
from prefect.client.orchestration import SyncPrefectClient
from prefect.client.orchestration._streams.client import StreamRequestError
from prefect.client.schemas.objects import FlowRun
from prefect.context import FlowRunContext
from prefect.results import ResultStore
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_URL,
    PREFECT_CLIENT_CSRF_SUPPORT_ENABLED,
    temporary_settings,
)
from prefect.states import Cancelling, Completed, Failed
from prefect.streams import (
    NativeStreamFlowConfig,
    StreamExchange,
    StreamGraph,
    StreamInput,
    StreamPartitionCall,
    StreamPort,
    StreamProtocolError,
)
from tests.server.api.test_stream_dags import deployed_dag_request, topology_counts
from tests.test_native_dag_client_http import hosted_api_server as hosted_api_server
from tests.test_stream_dag_authoring import dag_bare_source, dag_operator, dag_source

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


@pytest.fixture(autouse=True)
async def dag_metadata_backend(db: PrefectDBInterface, session: AsyncSession) -> None:
    connection = await session.connection()
    version = connection.dialect.server_version_info or ()
    if db.dialect.name != "postgresql" or not version or version[0] != 17:
        pytest.skip("DAG metadata HTTP integration requires PostgreSQL 17")


def declared_graph(source_port: str = "out", *, shape: str = "fanout") -> StreamGraph:
    source_count = 2 if shape == "rebalance" else 1
    branches = (
        (("B", 1, "forward"), ("C", 2, "broadcast"))
        if shape == "fanout"
        else (("B", 3, "rebalance"),)
        if shape == "rebalance"
        else (("B", 1, "forward"),)
    )
    with StreamGraph() as graph:
        source = graph.add(
            dag_source if source_count > 1 else dag_bare_source,
            operator_uid="A",
            partitions=tuple(
                StreamPartitionCall(
                    parameters={"split": str(index)} if source_count > 1 else {}
                )
                for index in range(source_count)
            ),
            output=StreamPort(source_port),
        )
        for uid, count, exchange in branches:
            graph.add(
                dag_operator,
                operator_uid=uid,
                partitions=(StreamPartitionCall(parameters={}),) * count,
                input=StreamInput(
                    port=StreamPort("in"),
                    upstream=source.output(source_port),
                    edge_uid=f"A-{uid}",
                    subscriber_uid=f"subscriber-{uid}",
                    exchange=StreamExchange(kind=exchange),
                ),
                output=StreamPort("out"),
            )
    return graph


@contextmanager
def parent_context(
    server: str, request: Any, headers: dict[str, str], *, wrong_profile: bool = False
) -> Iterator[tuple[SyncPrefectClient, FlowRun]]:
    execution = StreamExecutionContext(
        flow_run_id=request.flow_run_id,
        generation=int(headers["X-Prefect-Stream-Execution-Generation"]),
        holder_id=UUID(headers["X-Prefect-Stream-Execution-Holder"]),
        capability=headers["X-Prefect-Stream-Execution-Capability"],
        api_url=server,
        event_private_key=b"s" * 32,
    )

    @flow(
        version=request.flow_version,
        streaming=NativeStreamFlowConfig()
        if wrong_profile
        else NativeStreamFlowConfig(profile="partitioned-dag-v1", partition_count=None),
        persist_result=False,
    )
    def metadata_only_definition():
        raise AssertionError("Metadata publication executed its Flow body")

    with temporary_settings(
        {
            PREFECT_API_URL: server,
            PREFECT_API_AUTH_STRING: "native-dag:test",
            PREFECT_CLIENT_CSRF_SUPPORT_ENABLED: False,
        }
    ):
        with SyncPrefectClient(server, auth_string="native-dag:test") as client:
            run: FlowRun = client.read_flow_run(request.flow_run_id)
            with (
                stream_execution_context(execution),
                FlowRunContext(
                    flow=metadata_only_definition,
                    flow_run=run,
                    client=client,
                    task_runner=metadata_only_definition.task_runner,
                    result_store=ResultStore(),
                    persist_result=False,
                ),
            ):
                yield client, run


async def enrolled_parent(
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
    server: str,
) -> tuple[Any, dict[str, str]]:
    engine = await db.engine()
    assert db.dialect.name == "postgresql"
    assert engine.dialect.server_version_info[0] == 17
    public = Ed25519PrivateKey.from_private_bytes(b"s" * 32).public_key()
    async with httpx.AsyncClient(
        base_url=server, auth=("native-dag", "test"), timeout=30
    ) as api:
        return await deployed_dag_request(
            db,
            session,
            api,
            deployment,
            event_public_key=public.public_bytes(
                serialization.Encoding.Raw, serialization.PublicFormat.Raw
            ).hex(),
        )


@pytest.mark.parametrize("asynchronous_scope", [False, True])
@pytest.mark.parametrize("shape", ["fanout", "linear", "rebalance"])
async def test_public_facade_seal_and_recovered_authoring_keep_exact_metadata(
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
    hosted_api_server: str,
    asynchronous_scope: bool,
    shape: str,
):
    request, headers = await enrolled_parent(db, session, deployment, hosted_api_server)
    assert not any((await topology_counts(db)).values())
    with parent_context(hosted_api_server, request, headers) as (client, run):
        graph = declared_graph(shape=shape)
        assert not any((await topology_counts(db)).values())
        if asynchronous_scope:
            async with async_stream_flow_scope(run):
                sealed = graph.seal()
        else:
            with stream_flow_scope(run):
                sealed = graph.seal()
        assert graph.seal() is sealed
        recovered = declared_graph(shape=shape).seal()
        assert recovered == sealed
        plan = client.read_stream_dag(graph_revision_id=sealed.graph_revision_id)
        for terminal in (Completed(), Failed(), Cancelling()):
            with pytest.raises(httpx.HTTPStatusError) as refused:
                client.set_flow_run_state(run.id, terminal)
            assert refused.value.response.status_code == 409
            assert refused.value.response.json()["detail"]["code"] == "DAG_NOT_STARTED"
        assert client.read_flow_run(run.id).state.is_running()
    assert plan.graph_digest == sealed.graph_digest
    assert plan.execution_ready is False and plan.state == "SEALED"
    coordinates = {
        "fanout": [("A", 1), ("B", 1), ("C", 2)],
        "linear": [("A", 1), ("B", 1)],
        "rebalance": [("A", 2), ("B", 3)],
    }[shape]
    assert [(op.operator_uid, op.parallelism) for op in plan.operators] == coordinates
    physical_count = sum(count for _, count in coordinates)
    edge_count = 2 if shape == "fanout" else 1
    edge_partitions = 1 if shape == "linear" else 3
    assert len({item.manifest.task_run_id for item in plan.subtasks}) == physical_count
    assert all(item.assigned_worker_id is None for item in plan.subtasks)
    counts = await topology_counts(db)
    assert counts["task_run"] == physical_count
    assert counts["stream_graph_port"] == (5 if shape == "fanout" else 3)
    assert counts["stream_graph_edge"] == edge_count
    assert counts["stream_graph_edge_partition"] == edge_partitions
    assert counts["stream_partition"] == physical_count + edge_partitions
    for table in (
        "stream_record",
        "stream_subtask_grant",
        "stream_physical_slot_allocation",
    ):
        assert counts[table] == 0


async def test_lost_client_return_after_http_commit_replays_and_conflicts_safely(
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
    hosted_api_server: str,
):
    request, headers = await enrolled_parent(db, session, deployment, hosted_api_server)
    accepted = []
    original = SyncPrefectClient.seal_stream_dag

    def lose_return(client: SyncPrefectClient, **kwargs: Any):
        plan = original(client, **kwargs)
        accepted.append(plan)
        raise httpx.ReadTimeout("Injected lost return after actual HTTP acceptance")

    with parent_context(hosted_api_server, request, headers) as (client, _):
        first = declared_graph()
        with patch.object(SyncPrefectClient, "seal_stream_dag", lose_return):
            with pytest.raises(httpx.ReadTimeout):
                first.seal()
        assert len(accepted) == 1
        recovered = declared_graph().seal()
        retried = first.seal()
        assert recovered == retried
        assert recovered.graph_revision_id == accepted[0].id
        before = await topology_counts(db)
        with pytest.raises(StreamRequestError) as refused:
            declared_graph(source_port="changed").seal()
        assert refused.value.code == "IDENTITY_CONFLICT"
        assert await topology_counts(db) == before
        assert (
            len(
                client.read_stream_dag(
                    graph_revision_id=recovered.graph_revision_id
                ).subtasks
            )
            == 4
        )


async def test_dag_label_does_not_authorize_code_with_another_flow_profile(
    db: PrefectDBInterface,
    session: AsyncSession,
    deployment: orm_models.Deployment,
    hosted_api_server: str,
):
    request, headers = await enrolled_parent(db, session, deployment, hosted_api_server)
    with parent_context(hosted_api_server, request, headers, wrong_profile=True):
        with pytest.raises(StreamProtocolError, match="Flow authority"):
            declared_graph().seal()
    assert not any((await topology_counts(db)).values())
