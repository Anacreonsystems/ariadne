# Finite durable streaming with ordinary Prefect Tasks

This guide describes the current Ariadne checkout's `partitioned-dag-v1`
development profile. It follows the real two-worker
[normal-eight-document proof](evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-normal8-v1/receipt.json):
one source partition, one B partition and two C partitions. The native runtime
uses ordinary `@flow` and `@task` definitions; it creates one stable TaskRun per
physical subtask, not one TaskRun per document.

The example below adapts that proven API path to large local objects. Its
application-specific file operations are illustrative client code; this exact
adaptation has not been executed as a native acceptance run. The
[epoch](epochs/epoch-02-native-streaming-completion.md),
[ADR-0043](adr/ADR-0043-physical-graph-and-partitioned-channels.md) and their
criterion/evidence records govern readiness. This guide does not change their
status or establish completion of the whole streaming model.

## The graph and its result

```text
                    forward       B0
                 ┌─────────────── one copy/document
A0: finite source
                 └─────────────── C0 and C1
                    broadcast     one copy/document in EACH partition
```

For eight input documents, A0 publishes eight records, B0 processes eight,
and C0 and C1 each process eight. C's two partitions are replicas of this
broadcast subscription; this example does not divide C's work in half. There
are four physical TaskRuns and four Task processes across two registered
workers. Each edge partition has its own durable order and consumer cursor.
There is no implicit global order or collected result across the branches.

## Prerequisites

Use Linux with `/proc` and the local PID/start-time/verified-stop support used
by the native worker, PostgreSQL **17**, and this repository's prepared Python
environment. SQLite refuses public DAG execution. The two ProcessWorkers,
Flow coordinator and Task processes in this walkthrough run on the **same
host**, with the same checkout, environment and absolute working directory.
All can access the example's object directory. This is not a cross-host setup.

Use a fresh pool/tag name for the one-time registration below. The entire
finite graph needs four physical slots: the current worker cap is two per
worker, shared across native profiles. Its four tagged Tasks also need a tag
limit of four. The worker CLI's `--limit 1` limits Flow submissions; it does not
change that physical-subtask cap.

In a server terminal, configure your own PostgreSQL connection and API login,
upgrade that database with this checkout, and start the server:

```shell
export PREFECT_SERVER_DATABASE_CONNECTION_URL='postgresql+asyncpg://USER:PASSWORD@localhost:5432/ariadne'
export PREFECT_SERVER_API_AUTH_STRING='demo:replace-this-password'
uv run --no-sync prefect server database upgrade --yes
uv run --no-sync prefect server start --host 127.0.0.1 --port 4200
```

In the registration, worker and submission terminals, use the same API URL and
login and change to the same absolute checkout directory:

```shell
export PREFECT_API_URL='http://127.0.0.1:4200/api'
export PREFECT_API_AUTH_STRING='demo:replace-this-password'
```

The login belongs to the trusted client/worker environment. The platform gives
a physical Task only its own scoped grant; do not put administrative credentials
in Task parameters or object references.

## Save the application as `finite_dag.py`

Large bytes live outside the platform stream. Each `SourceRecord` carries a
small JSON reference and an explicit increasing source position. The operator
receives one reference at a time and yields its result immediately.

```python
import hashlib
import json
from pathlib import Path

from prefect import flow, task
from prefect.streams import (
    NativeStreamFlowConfig,
    SourceRecord,
    StreamExchange,
    StreamGraph,
    StreamInput,
    StreamPartitionCall,
    StreamPort,
    StreamSourceConfig,
    StreamTaskConfig,
)

TAG = 'finite-dag-demo'


def put_bytes(destination: Path, content: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + '.tmp')
    temporary.write_bytes(content)
    temporary.replace(destination)


@task(version='finite-dag-v1', retries=2, tags={TAG}, stream=StreamSourceConfig())
def ingest(ctx, object_root: str, count: int):
    root = Path(object_root)
    for position in range(ctx.accepted_source_position + 1, count + 1):
        document_id = f'document-{position}'
        content = (f'document:{document_id}\n' * 16384).encode('utf-8')
        digest = hashlib.sha256(content).hexdigest()
        path = root / 'objects' / f'{digest}.txt'
        put_bytes(path, content)
        yield SourceRecord(position, {
            'document_id': document_id,
            'object_ref': str(path),
            'sha256': digest,
        })


@task(
    version='finite-dag-v1', retries=2, tags={TAG},
    stream=StreamTaskConfig(input='document'),
)
def sign(ctx, document, object_root: str, role: str):
    content = Path(document['object_ref']).read_bytes()
    assert hashlib.sha256(content).hexdigest() == document['sha256']
    receipt = {
        'document_id': document['document_id'],
        'sha256': document['sha256'],
        'signed_by': role,
        'partition': ctx.partition_index,
    }
    path = (Path(object_root) / 'receipts' / role
            / str(ctx.partition_index) / f'{document["document_id"]}.json')
    put_bytes(path, json.dumps(receipt, sort_keys=True).encode('utf-8'))
    yield {'document_id': document['document_id'], 'receipt_ref': str(path)}


@flow(
    version='finite-dag-v1', retries=0, persist_result=False,
    streaming=NativeStreamFlowConfig(
        profile='partitioned-dag-v1', partition_count=None,
    ),
)
def documents(object_root: str, count: int = 8):
    with StreamGraph() as graph:
        a = graph.add(
            ingest, operator_uid='A',
            partitions=(StreamPartitionCall(parameters={
                'object_root': object_root, 'count': count,
            }),),
            output=StreamPort('documents'),
        )
        for role, parallelism, exchange in (
            ('B', 1, 'forward'), ('C', 2, 'broadcast'),
        ):
            graph.add(
                sign, operator_uid=role,
                partitions=tuple(StreamPartitionCall(parameters={
                    'object_root': object_root, 'role': role,
                }) for _ in range(parallelism)),
                input=StreamInput(
                    port=StreamPort('input'), upstream=a.output('documents'),
                    edge_uid=f'A-{role}', subscriber_uid=f'subscriber-{role}',
                    exchange=StreamExchange(kind=exchange),
                ),
                output=StreamPort('result'),
            )
    sealed = graph.seal()
    running = sealed.start(timeout=90)
    finished = running.wait(timeout=150)
    return {'input_documents': count, 'physical_tasks': finished.counts.total}
```

This uses one declared input and output per operator with the frozen
`json-value-v1` port schema. `operator_uid`, edge/subscriber UIDs and partition
positions are explicit identities. Do not change code or parameters in place
while a run is active; deploy a new version instead.

The source publishes only after its own external file write. The platform
accepts source positions durably, so `accepted_source_position + 1` is the
resume position. An operator invocation handles one delivered document. Its
Python `yield` is a hand-off to the platform: it becomes accepted only when the
platform's terminal transaction succeeds.

Do not call `documents(...)` directly for this native profile. Use its enrolled
deployment and workers as below. Flow `retries=0` is deliberate: public seal
and start refuse when the current Flow attempt still has automatic retry
capacity. Task retries are separate; this example configures them explicitly.

## Register the pool and deployment once

Save this as `register_demo.py`, beside `finite_dag.py`, and run it from that
same directory. The names below assume a fresh demo pool/tag. Reusing an active
pool or changing its template is not part of this walkthrough.

```python
import asyncio
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate, WorkPoolCreate
from prefect.deployments.runner import RunnerDeployment
from prefect.workers.process import ProcessWorker

from finite_dag import TAG, documents


async def main():
    template = ProcessWorker.get_default_base_job_template()
    template['prefect_native_streaming'] = {
        'schema_version': 1, 'profile': 'bounded-linear-v1',
    }
    async with get_client() as client:
        await client.create_global_concurrency_limit(
            GlobalConcurrencyLimitCreate(name='tag:' + TAG, limit=4),
        )
        await client.create_work_pool(WorkPoolCreate(
            name='finite-dag-demo', type='process', base_job_template=template,
        ))
    deployment = RunnerDeployment.from_flow(
        documents, name='finite-dag-demo', work_pool_name='finite-dag-demo',
        concurrency_limit=1,
        job_variables={'working_dir': str(Path.cwd()), 'stream_output': True},
    )
    print(await deployment.aapply())


if __name__ == '__main__':
    asyncio.run(main())
```

```shell
uv run --no-sync python register_demo.py
```

Keep the printed deployment UUID. The pool's `bounded-linear-v1` marker is the
existing native-worker discovery opt-in, exactly as in the green HTTP fixture;
it does **not** select the Flow's graph profile. The Flow declaration and its
persisted deployment snapshot select `partitioned-dag-v1`. Do not replace this
marker with an invented pool configuration.

## Start two workers, then submit

Run these in separate terminals with the client environment above:

```shell
uv run --no-sync prefect worker start --pool finite-dag-demo --type process --name demo-left --limit 1
```

```shell
uv run --no-sync prefect worker start --pool finite-dag-demo --type process --name demo-right --limit 1
```

Allow both workers to register and advertise the DAG protocol before starting
the Flow. The server verifies eligible worker identities and reserves the
entire graph before activation. An ONLINE worker alone does not prove that it
has advertised this profile or has free slots; insufficient capacity is a
refusal, not permission to run an incomplete graph.

Save the following as `submit_demo.py`. It takes the deployment UUID printed
by registration. The absolute object directory is shared by this same-host
example's workers and Tasks.

```python
import asyncio
import sys
from pathlib import Path
from uuid import UUID

from prefect.client.orchestration import get_client


async def main():
    async with get_client() as client:
        run = await client.create_flow_run_from_deployment(
            UUID(sys.argv[1]),
            parameters={'object_root': str((Path.cwd() / 'dag-objects').resolve()), 'count': 8},
        )
    print(run.id)


if __name__ == '__main__':
    asyncio.run(main())
```

```shell
uv run --no-sync python submit_demo.py DEPLOYMENT_UUID
```

Inspect the FlowRun in the local UI at `http://127.0.0.1:4200`. For the example,
expect four completed TaskRuns and 24 small receipt files: eight in each of
`dag-objects/receipts/B/0`, `C/0` and `C/1`. The object store contains eight
content-addressed files. The Flow's final Python summary follows `wait()`; it
is not a Task consuming a graph handle as an ordinary result.

`graph.add()` is local authoring. Context exit does not seal or start anything.
`seal()` persists the complete immutable graph. `start()` admits it, lets the
workers prepare/register every Task process, waits for authenticated bootstrap
readiness, and obtains durable activation. Workers then release their local GO
gates. `wait()` observes terminal completion with verified cleanup; normal
completion leaves the Flow Running until its remaining Python code returns.

For an async Flow, the corresponding calls are `await sealed.start(...)` and
`await running.wait(...)`; sync and async native normal-eight paths have real
process evidence. A wait timeout only ends observation. If an exception then
leaves the Flow scope with active work, the Flow's lifecycle cleanup applies.
`running.cancel(reason=...)` waits for verified cleanup and propagates Flow
cancellation; user code must not expect a normal return after cancellation.

## Recovery at document 200

These are **per-subscriber, per-partition input coordinates**, not a global
business-document counter or periodic snapshot position:

| State when an operator process fails | First input delivered to its replacement |
| --- | --- |
| 199 accepted; input 200 began but its `yield`/`return` was not durably accepted | 200 |
| Input 200's terminal transaction was accepted, even if its response was lost | 201 |

Output reference, routed references, managed state, input receipt and cursor
advance in the same terminal acceptance. A branch cannot acknowledge a sibling
branch's cursor. Accepted output is retained durably for the supported finite
run, so a downstream replacement does not ask the source to repeat already
accepted business work merely because an in-memory cache or a periodic
snapshot is behind. There is no application `ctx.barrier()` call in this API.

External effects are different. If a Task writes PostgreSQL or calls an
external API and dies before terminal acceptance, input 200 can execute again.
The **client must make that effect idempotent**, for example with a stable
business key and `INSERT ... ON CONFLICT`, or a receiver's idempotency key. Do
not base that key on a retry attempt or a new random UUID. The example owns its
file-effect policy: deterministic receipt paths and atomic replacement of the
same bytes. Source objects use content-addressed immutable references.

The 200/201 rule is the protocol contract. The linked DAG normal-eight evidence
is not a completed DAG branch-loss/500/5000 acceptance matrix. Preserve that
distinction when testing or deploying this development profile.

## Evidence and remaining profiles

- [ADR-0043](adr/ADR-0043-physical-graph-and-partitioned-channels.md) and
  [epoch 2](epochs/epoch-02-native-streaming-completion.md) define the target
  architecture and its acceptance boundaries.
- [Real sync and async normal eight](evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-normal8-v1/receipt.json)
  proves two workers, four ordinary TaskRuns/processes, eight accepted source
  records, 24 operator turns, four EOFs, and released physical/tag resources.
- [Real preview/failure/cancel controls](evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-lifecycle-v1/receipt.json)
  retains the actual lifecycle evidence and its remaining gates.
- [Real paired SDK HTTP controls](evidence/native-epoch2-dag-execution-sdk-2026-11-05/real-http-v1/receipt.json)
  proves ACK identity, exact retry bodies, current projection and authorization
  over protected TCP/PostgreSQL, plus SQLite refusal. It is not a business-flow
  acceptance run.
- [Accepted fixed-P2 foundation](evidence/native-epoch2-n2-1-principal-acceptance-2026-11-05.json)
  is a separate finite profile; this guide does not replace its evidence.

Multi-input/fan-in joins and their durable state protocol remain under
[ADR-0044](adr/ADR-0044-multi-input-turns-and-state-materialization.md).
Continuous sources, event time and timers remain under
[ADR-0045](adr/ADR-0045-continuous-sources-event-time-and-timers.md), with continuous
retention/pressure under [ADR-0046](adr/ADR-0046-continuous-pressure-and-safe-retention.md).
Verified cross-host replacement remains under
[ADR-0047](adr/ADR-0047-selective-recovery-and-cross-host-authority.md), and rescaling
under [ADR-0048](adr/ADR-0048-rescaling-compatibility-and-native-acceptance.md).
These are not implied by starting two workers on one host.
