"""Finite entrypoint refusals must precede collection or task creation."""

from tests.fixtures.native_stream_document_flow import petya

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner


@task(version="finite-v1")
def finite_work(value):
    raise AssertionError("Finite task user code must never run in this profile")


@flow(
    version="unsupported-v1", retries=1, task_runner=ThreadPoolTaskRunner(max_workers=2)
)
def unsupported_flow(dataset, coordination, kind):
    source = petya.submit_stream(
        dataset, coordination, kind, submission_key="documents.ingest"
    )
    collection = source.collect(limit=1)
    if kind == "call":
        finite_work(collection)
    elif kind == "submit":
        finite_work.submit(collection)
    elif kind == "map":
        finite_work.map(collection)
    elif kind == "apply-async":
        finite_work.apply_async(args=(collection,))
    else:
        raise ValueError("Unknown unsupported control")
