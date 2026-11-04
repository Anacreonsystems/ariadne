# ADR-0040: Native integration acceptance and operation

- Status: `progress`
- Date: 2026-11-04
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- Owners: SDK, server, worker and UI maintainers; ETL Architect / HighLoad Principal
- Depends on: [mixed tasks](ADR-0037-mixed-finite-and-streaming-tasks.md), [deployment lifecycle](ADR-0038-native-deployment-worker-lifecycle.md), [progress projections](ADR-0039-native-progress-projections.md)
- Delivery: [tasks](../todo.md), [sprints](../sprint_board.md)

## Problem

A passing standalone durable-channel prototype does not establish an integrated
Prefect feature. Even separate passing SDK, API and worker tests do not prove
that a real deployment preserves its TaskRuns, stream records, retry policy and
concurrency slot across process loss. The release boundary must also account
for delayed TaskRun events, cancelled children and an enrolled worker that dies
before creating its child.

## Theory

The theory is that the existing FlowRun, TaskRun, deployment, ProcessWorker and
executor can own the complete bounded streaming lifecycle. Durable channels and
receipts extend those objects; a separate orchestration service is unnecessary.
Acceptance requires one reproducible source snapshot, actual system boundaries,
independent database observations and an independent architecture review.

## Solution and agreement

N1.10 closes the selected foundation only after the criteria below pass. The
preceding allocation is N1.6b +10% (60%), N1.7 +10% (70%), N1.8 +15% (85%) and
N1.9 +5% (90%); the final acceptance contributes +10% (100%). These are planned
shares of the stated problem. Discovery runs and documentation alone earn no
share. Current earned readiness stays at the previously accepted 50% until
source-bound acceptance is recorded.

The profile is finite, versioned, top-level Python code, exactly one ordinary
preparation task, a single-source linear native stream graph and one ordinary
summary task. Sync and async SDK paths use the existing engines. Storage uses
PostgreSQL 17. Workers use the trusted local Linux pidfd profile on the same
host/PID namespace and an explicit shared local workspace. The immutable resource profile admits at most 128 ACTIVE native FlowRuns per
work pool; excess creation receives `CAPACITY_EXCEEDED` without a new FlowRun.
This includes unreleased terminal snapshots and bounds candidate discovery.
The configured task
runner must have enough concurrent slots for the source and operators.

External writes remain the task author's responsibility under
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md). A retry may repeat
an external write whose yield was not durably accepted. Large objects stay in
application storage; stream values and finite JSON results contain bounded
references and integrity metadata.

A native worker explicitly releases platform replay payloads after the child
has exited and its final task metadata is reconciled. The external objects are
not deleted. Cancellation requires actual child-death evidence before final
state, task cleanup and release. A replacement can also retire an abandoned
terminal launch. An enrolled launcher without a registered child has its own
persisted process identity: a fresh worker must prove it is gone, without
signalling a live worker that may supervise other flows. Losing the launcher
before any work starts produces a clear failed run with no stream DATA.

## Stories

- As a developer, I want to deploy ordinary and streaming Python tasks together
  using the existing SDK and ProcessWorker.
- As a user, I want the next worker to resume the first incomplete document,
  preserve the run's identity and expose progress through the normal FlowRun UI.
- As a system, I want every restart, cancellation and release to obey durable
  authority, actual process evidence, retry budgets and concurrency policy.
- As an operator, I want a reproducible acceptance record and a practical
  procedure for starting, draining, cancelling and inspecting the supported flow.

## Acceptance criteria

- [ ] As a developer, I can execute the declared deployment through actual SDK,
  HTTP server, PostgreSQL and ProcessWorker processes, including sync/async
  500-document flows and a finite preparation/summary.
- [ ] As a user, I got the same TaskRun/channel identities and correct document
  boundary after prefix failure/receipt loss, before/after document-200 yield,
  tail receipt loss and whole-worker SIGKILL.
- [ ] As a system, I can admit one winning replacement after real stale-renewal
  time and verified process death, while rejecting live-launcher takeover,
  stale generations, code drift and timeout-as-death.
- [ ] As a user, I can drain the accepted prefix, cancel through SDK/UI/CLI,
  finalize orphan cancellation and inspect released state without invented
  cursor or receipt history.
- [ ] As a system, I can retire pre-spawn and registered startup failures,
  exhausted retry policy and abandoned final launches without permanent
  Running/Pending states or retained concurrency slots.
- [ ] As a developer, I got bounded resource refusals, paired PostgreSQL/SQLite
  migrations, client-only package compatibility, unchanged ordinary workflows
  and successful public/CI type checks with actual zero exit status.
- [ ] As an operator, I got a source-bound manifest, preserved first failures,
  zero skipped selected native cases, ordinary regression results, a reviewed
  PR and CI results linked from this document.

## Operation of the implemented profile

### Declare and deploy

The executable four-task example is
[the mixed deployment fixture](../../../tests/fixtures/native_stream_mixed_flow.py).
Its public shape is:

```python
from prefect import flow, task
from prefect.streams import NativeStreamFlowConfig, SourceRecord, StreamSourceConfig, StreamTaskConfig
from prefect.task_runners import ThreadPoolTaskRunner

@task(version="documents-v1", retries=2, stream_key="documents.prepare")
def prepare(dataset):
    return {"dataset": dataset}

@task(version="documents-v1", retries=2, stream=StreamSourceConfig())
def ingest(ctx, config):
    for position, reference in read_after(config, ctx.accepted_source_position):
        yield SourceRecord(position, reference)

@task(version="documents-v1", retries=2, stream=StreamTaskConfig(input="document"))
def sign(ctx, document):
    yield idempotent_sign_and_store(document)

@task(version="documents-v1", retries=2, stream_key="documents.summary")
def summarize():
    return inspect_signed_documents()

@flow(version="documents-v1", retries=2,
      streaming=NativeStreamFlowConfig(),
      task_runner=ThreadPoolTaskRunner(max_workers=2))
def pipeline(dataset):
    config = prepare(dataset)
    source = ingest.submit_stream(config, submission_key="documents.ingest")
    output = sign.submit_stream(source, submission_key="documents.sign")
    output.wait()
    return summarize()
```

`read_after`, signing/storage and summary are application functions in this
shape illustration; the linked fixture supplies real file data, hashes and
idempotent writes, and imports `SourceRecord`. Every native task needs an
explicit code version and an adequate retry budget. The tests exercise the
actual complete example, rather than treating this abbreviated illustration as
an independent acceptance artifact.

Configure the API URL/authentication and a PostgreSQL 17 server, apply the
paired migrations with `uv run prefect server database upgrade -y`, then create
a process pool and start an ordinary ProcessWorker:

```bash
uv run prefect work-pool create native-documents --type process
uv run prefect worker start --pool native-documents --type process
```

Create a deployment from the declared Flow with
`RunnerDeployment.from_flow(..., work_pool_name="native-documents",
job_variables={"working_dir": "/absolute/shared/workspace"})` and `apply()`.
The [real worker test](../../../tests/test_native_worker_deployment_http.py)
contains the complete deployment creation and FlowRun invocation. The server
binds the declaration before launch, then the worker enrolls and registers its
blocked child before delivering private credentials through a one-use inherited
file descriptor. Credentials do not enter Flow parameters, deployment variables
or the bootstrap environment.

### Observe and recover

Open the React UI at `/v2` and its FlowRun details page. The native section shows generation, source
accepted position, consumer completed/in-flight positions, pending input, EOF,
finite receipt status and resource limits. Task links open the normal TaskRuns.
The authenticated projection contains no record values or private capabilities.

If document 200 has no accepted yield, the replacement processes 200 again. If
its yield is durable, processing resumes after the completed cursor. A source
uses `ctx.accepted_source_position` to resume publication. Already completed
finite receipts suppress repeat user-code execution. This does not make a
client's external side effects transactional.

A fresh worker in the same marked pool discovers an abandoned launch after
120 seconds without that launch's renewal. Generic worker heartbeats do not
renew another launch. The new worker verifies Linux process identity before a
single generation CAS. Two fresh workers cannot acquire the same generation.
The existing deployment concurrency slot stays assigned during replacement.
The [worker lifecycle tests](../../../tests/test_native_worker_recovery_http.py)
use actual processes and real elapsed renewal time.

### Drain, cancel and release

Use the native FlowRun controls to drain or cancel. Drain closes source
admission: already accepted records reach the tail and the source publishes EOF
at its last accepted position. A cancelled run fences writes, waits for verified
child exit, then becomes Cancelled and releases its platform payloads.

The existing CLI also routes enrolled native runs through this protocol:

```bash
uv run prefect flow-run cancel FLOW_RUN_UUID
```

The paired clients expose `drain_native_stream`, `cancel_native_stream` and the
safe projection. Control requests include a stable operation UUID and expected
generation. Retry an uncertain response with the same operation and payload;
refetch after a generation conflict. Timeout is not process-death evidence.

After release, metadata and published sequence positions remain inspectable;
consumer/receipt history that was removed is labelled unavailable. Released
records are outside the replay horizon. Application object retention is managed
separately.

## Consequences

### Boundaries and later epochs

This foundation does not establish arbitrary DAG/fan-out/join semantics,
partitioned operators, unbounded retention or online GC, cross-host/container
recovery, multi-tenant untrusted workers, multi-sink atomic effects, HA storage or
a throughput/latency SLO. Those require their own ADRs, code and acceptance.
The completed [72-hour prototype campaign](ADR-0030-continuous-arc-soak-and-evidence-collection.md)
remains historical experimental evidence; it is not counted as a 72-hour native
kernel deployment test. This integration uses the user-approved short native
acceptance and does not start another long campaign.

## Evidence and final review

Pending the common source freeze and its final gate. Discovery successes and
failures remain recorded in the linked ADRs and the final evidence manifest;
they do not replace this gate.
