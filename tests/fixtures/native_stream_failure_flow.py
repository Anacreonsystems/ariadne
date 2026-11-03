"""Failure and transport controls through the ordinary flow-engine entrypoint."""

from pathlib import Path

import httpx
from tests.fixtures.native_stream_document_flow import anya, petya

from prefect import flow
from prefect.client.orchestration import SyncPrefectClient, get_client
from prefect.task_runners import ThreadPoolTaskRunner


def observe_terminal_abort(definition, run, state):
    if not run.parameters["kind"].startswith("construction"):
        return
    with get_client(sync_client=True) as client:
        for key in ["documents.ingest", "documents.sign"]:
            manifest = client.read_stream_submission(
                flow_run_id=run.id, submission_key=key
            )
            task_run = client.read_task_run(manifest.task_run_id)
            assert task_run.state.is_failed() and task_run.run_count == 0
            info = client.read_stream(manifest.stream_id)
            assert info.state == "FAILED" and info.record_count == 0
    Path(run.parameters["coordination"], "hook-observed-abort").touch()
    raise RuntimeError("Injected terminal hook failure after observing durable abort")


@flow(
    version="failure-controls-v1",
    retries=2,
    task_runner=ThreadPoolTaskRunner(max_workers=2),
    on_failure=[observe_terminal_abort],
)
def failure_flow(dataset, coordination, kind):
    if kind == "lost-seal-response":
        original = SyncPrefectClient.seal_stream_manifest
        lost = False

        def lose_once(self, **kwargs):
            nonlocal lost
            result = original(self, **kwargs)
            with Path(coordination, "seal-operation-ids").open("a") as log:
                log.write(str(kwargs["operation_id"]) + "\n")
            if not lost:
                lost = True
                for key in ["documents.ingest", "documents.sign"]:
                    manifest = self.read_stream_submission(
                        flow_run_id=kwargs["flow_run_id"], submission_key=key
                    )
                    assert self.read_stream(manifest.stream_id).record_count == 0
                    assert self.read_task_run(manifest.task_run_id).run_count == 0
                raise httpx.ReadError("Injected loss after real graph seal response")
            return result

        SyncPrefectClient.seal_stream_manifest = lose_once
    source = petya.submit_stream(
        dataset, coordination, kind, submission_key="documents.ingest"
    )
    output = anya.submit_stream(
        source, coordination, kind, submission_key="documents.sign"
    )
    if kind.startswith("construction"):
        raise ValueError("Injected failure before graph admission")
    output.wait()
    assert source.wait().accepted_source_position == 500
    if kind == "application-failure":
        raise ValueError("Injected failure after graph admission")
    return "documents-complete"
