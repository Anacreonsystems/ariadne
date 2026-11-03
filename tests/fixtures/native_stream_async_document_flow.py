"""Async Flow and async generators over real document references."""

from pathlib import Path

from tests.fixtures.native_stream_document_flow import anya, petya

from prefect import flow, task
from prefect.streams import StreamSourceConfig, StreamTaskConfig
from prefect.task_runners import ThreadPoolTaskRunner


@task(version="async-documents-v1", retries=2, stream=StreamSourceConfig())
async def async_petya(ctx, dataset, coordination, fault):
    for record in petya.fn(ctx, dataset, coordination, fault):
        yield record


@task(
    version="async-documents-v1", retries=2, stream=StreamTaskConfig(input="document")
)
async def async_anya(ctx, document, coordination, fault):
    for reference in anya.fn(ctx, document, coordination, fault):
        yield reference


@flow(
    version="async-documents-v1",
    retries=2,
    task_runner=ThreadPoolTaskRunner(max_workers=2),
)
async def documents_flow(dataset, coordination, fault):
    source = async_petya.submit_stream(
        dataset, coordination, fault, submission_key="documents.ingest"
    )
    output = async_anya.submit_stream(
        source, coordination, fault, submission_key="documents.sign"
    )
    # Collection seals the graph; normal flow completion must still join the
    # operator's EOF before proposing Flow success.
    values = await output.collect(limit=500).resolve()
    assert len(values) == 500
    assert source.wait().accepted_source_position == 500
    assert source.completion is not None
    Path(coordination, "source-future-state-id").write_text(
        str(source.completion.state.id)
    )
    return "documents-complete"
