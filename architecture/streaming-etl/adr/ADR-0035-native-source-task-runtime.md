# ADR-0035: Native source task runtime

- Status: `accepted`
- Date: 2026-11-02
- Reviewed: 2026-11-02, ETL Architect / HighLoad Principal and SDK/server implementation review
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md),
  Stage 3 / N1.4
- Owners: Prefect task-engine maintainers, streaming runtime maintainers,
  ETL Architect / HighLoad Principal
- Depends on: [ADR-0032](ADR-0032-native-task-stream-contract.md),
  [ADR-0033](ADR-0033-native-durable-stream-storage.md),
  [ADR-0034](ADR-0034-native-stream-http-adapter.md)

## Problem

The accepted operator contract consumes one durable input at a time. A source
has no upstream input and can publish many records while its TaskRun remains
running. Treating every record as another TaskRun would make orchestration
metadata grow with data volume. Treating a long-lived Python generator frame as
the recovery authority would lose its local cursor when the process exits.

The native store already exposes low-level source claim, append and finish
operations. They are not yet a native source-task SDK. The current
`StreamTaskConfig` rejects `source=True`, `Task.submit_stream()` requires an
upstream `Stream`, and the task engine has no source execution loop or accepted
source-position context.

The existing low-level `create_stream` operation also creates an unbound OPEN
header. A native task source needs an immutable TaskRun-to-stream reservation
and exact cleanup paths before it claims an owner. Otherwise failure between
submission and claim can leave an output that no lifecycle authority can safely
finish.

## Theory

A bounded source can recover without restoring a Python frame if every TaskRun
attempt begins from the position already accepted by durable storage. The task
body receives that position and opens its scanner after it. Within the attempt,
one long-lived sync or async generator may yield many explicit positioned
records. The runtime resumes that generator only after storage accepts the
previous yield. Storage acceptance, rather than reaching the Python `yield`,
advances source progress.

If position 200 did not commit, the same TaskRun retry sees accepted position
199 and produces 200 again. If position 200 committed but its acknowledgement
was lost, an exact replay returns the existing acceptance and a fresh attempt
starts from accepted position 200, so the next source turn produces 201. A
newer TaskRun attempt owns a new source epoch and fences the former owner.

This is task retry, not whole-worker reconstruction. Recreating a submission
after the flow process and its in-memory future graph are lost needs a durable
submission identity, lookup and TaskRun/stream reconciliation protocol. That is
a separate stage after N1.4.

## Proposed solution

### Explicit source declaration and submission

Add a distinct `StreamSourceConfig` with source protocol `source-v1`. Keeping
source configuration separate from the operator `StreamTaskConfig` prevents
source multi-emission from weakening the operator's one-input/one-terminal
`yield-v2` contract. The intended SDK shape is an ordinary Prefect task, for
example:

```python
@task(
    stream=StreamSourceConfig(protocol="source-v1")
)
def petya(ctx: SourceContext):
    for position, ref in read_after(ctx.accepted_source_position):
        yield SourceRecord(position=position, value=ref)
```

The exact public type names remain subject to implementation review, but the
semantics do not:

- a source declaration has no stream input port and has one explicit context;
- `Task.submit_stream()` returns a `Stream` for both the operator and source
  configurations, while its typed source form accepts no upstream stream;
- the native backend comes from the active FlowRun/client context rather than a
  storage argument persisted in task parameters;
- ordinary `Task.__call__`, `Task.submit`, finite tasks and undeclared Python
  generator functions retain their existing behavior;
- an unsupported runner, nested source submission, implicit source return,
  unbounded profile or incompatible protocol rejects before source admission.

The submission path allocates one TaskRun id and one stream id, records the
normal Pending TaskRun through the existing Prefect API, reserves the exact
source output, and submits that same TaskRun to the ordinary task runner. It
does not create a TaskRun per record or introduce a second orchestrator.

### One source attempt, many accepted records

Only sync and async generator functions are supported by `source-v1`. The
source task body is invoked once per TaskRun attempt with a `SourceContext`.
`accepted_source_position` is the durable position returned by source claim;
it is not an in-memory count of generator iterations. A retry constructs a new
generator from the new claim result instead of restoring a former frame.

Each yielded `SourceRecord(position, value)` requests one JSON record at the
next contiguous position. The runtime freezes its position and canonical value
before the first transport attempt. It does not request the next yield until
the append is accepted. A transport retry reuses the same frozen record; a
timeout alone does not advance the context. Exact replay of an accepted
position returns its existing record, while changed content fails closed.

After acceptance, the runtime updates its platform-owned context position and
resumes the same generator. This preserves an open scanner or paginated session
within an attempt without making that frame durable authority. Small opaque
JSON references are valid source values under the existing record-size limit;
a future managed ObjectRef or connector service is not required to transport
such references.

Normal generator exhaustion requests successful EOF with
`expected_position=accepted_source_position`. An empty generator therefore
finishes an empty source at position zero. A non-`None` sync-generator return,
a non-generator source function, non-JSON value, non-contiguous position or
changed payload refuses without a new append or successful EOF. If retries are
later exhausted, the separate final TaskRun lifecycle operation may still mark
the source `FAILED`. Async generators use the same yield/exhaustion contract.

### Storage and lifecycle authority

Add a source-specific immutable reservation. It binds the FlowRun, source
TaskRun, stream, protocol version and frozen limits before owner admission.
Low-level unbound `create_stream` is insufficient for native source-task
submission and remains a separate store operation.

Source claim must require that exact reservation and current recorded RUNNING
TaskRun attempt. Its transaction returns the fenced source binding, stream
state and current durable accepted source position. A newer attempt may rotate
owner and epoch for the same TaskRun and source. The former binding cannot
append or finish after takeover. If the stream is already `COMPLETED` after a
lost EOF acknowledgement, the engine reconciles success without invoking user
source code again.

The source lifecycle has three distinct terminal paths:

1. Failure before owner admission may reject only the exact unowned source
   reservation with no records.
2. An intermediate TaskRun failure that will retry leaves the source OPEN. The
   retry claims a newer owner epoch and continues from durable position.
3. Exhausted retry, cancellation or crash may fail the source only through an
   exact final TaskRun lifecycle witness. A stale attempt cannot close work
   owned by a newer attempt.

Successful generator exhaustion causes the platform to verify the expected
accepted position under the source lock and commit the immutable tail before
the TaskRun reports `Completed`. Lost acknowledgement of the same EOF
reconciles idempotently. Downstream EOF handling retains the existing
empty-read, terminal-status, second-read rule so the committed tail preceding
EOF cannot be skipped.

Prefect retains lifecycle authority: FlowRun transitions use the server API,
and the task engine manages and reports TaskRun state through its normal event
path. Stream storage owns source reservation, owner fencing, positions,
records and the tail marker. It does not independently publish a TaskRun or
FlowRun state.

### Bounded first scope

N1.4 supports finite JSON sources within frozen record, byte and per-record
limits. JSON values may be small opaque references. Capacity exhaustion is a
named permanent refusal for this profile; it does not become an infinite HTTP
retry. Online prefix reclamation, durable reader pins for an indefinitely
running source, arbitrary external offsets, managed ObjectRef services,
connector transactions and generalized backpressure need later protocols.

The first scope uses the supported native task runner profile and one source
output. Multiple source outputs, dynamic fan-out, non-generator callables,
source nesting and source-to-source submission are unsupported and reject
before mutation. Multiple sequential yields are valid only for `source-v1`;
they do not change the operator `yield-v2` terminal contract.

### Separate whole-worker recovery stage

N1.4 does not reconstruct a source after losing the whole flow or worker
process. The next runtime stage must define a durable submission identity and
lookup, bind it to deployment/code/graph versions, recover the existing
TaskRun and source stream, and reconcile cases where stream append or EOF is
durable but the corresponding TaskRun event is behind.

That stage must preserve N1.4's TaskRun, stream and source-position identities.
It cannot create a replacement source TaskRun merely because infrastructure
changed, and it cannot infer progress by replaying an arbitrary Python
generator from its beginning.

## Agreement

N1.4 will implement the following bounded native source contract:

1. A source is an explicitly declared ordinary Prefect Task with one TaskRun
   for its stream, not a TaskRun per record and not a separate orchestrator.
2. Source submission has a distinct typed SDK configuration branch and an
   immutable TaskRun-to-source reservation before owner admission.
3. Each TaskRun attempt receives durable `accepted_source_position` and opens
   one sync or async generator that may emit sequential positioned records.
4. Platform append acceptance is the progress boundary. The runtime never
   requests the next yield before acceptance and never relies on a restorable
   Python frame across attempts.
5. Task retry keeps the same TaskRun and source identity, rotates a fenced
   owner epoch, and resumes at the first position after durable acceptance.
6. Pre-owner rejection, final lifecycle failure and exhaustion-driven EOF are
   separate exact operations. Intermediate retry does not fail the source.
7. The initial profile is finite, contiguous, JSON-only and bounded by frozen
   capacity. Unsupported profiles fail before data admission.
8. Durable submission lookup and whole-worker reconstruction are a separate
   follow-up stage and are not N1.4 acceptance claims.

## Stage boundary

N1.4 includes native SDK types, source task declaration and submission, sync and
async generator execution, source reservation/lifecycle APIs, HTTP client
support, exact append/EOF replay, TaskRun retry fencing, packaging and real
PostgreSQL integration evidence.

The short sprint is tracked as NI1-030…034 in [todo](../todo.md) and
[sprint board](../sprint_board.md). Its planned contribution is +10%, reaching
40% of the bounded native foundation only after all acceptance criteria pass.
The implemented source gate has passed and earns that planned +10%, bringing
the bounded native foundation to 40%.

N1.4 does not include automatic recovery after loss of the whole flow process,
deployment replacement, arbitrary connectors, source-side external-effect
transactions, online garbage collection, indefinite streams, multi-source
coordination, production HA or throughput acceptance. Documentation alone does
not satisfy this stage.

## Stories

- As a developer, I want to declare a native source generator as an ordinary
  Prefect task and receive a `Stream` without constructing low-level HTTP
  bindings.
- As a developer, I want source code to receive the durable accepted position
  so it can deterministically choose the next record.
- As a user, I want downstream tasks to receive source records while the source
  TaskRun is still running.
- As a user, I want a retry after accepted position 200 to continue with 201,
  while a failure before acceptance redelivers 200.
- As a system, I want one bounded source TaskRun rather than orchestration rows
  proportional to the record count.
- As a system, I want stale owners, changed position payloads and unsupported
  source profiles refused without mutating durable progress.
- As a system, I want source EOF and final TaskRun state to retain separate,
  reconcilable authorities.

## Acceptance criteria

- [x] As a developer, I can declare and submit real sync and async native source
  generator tasks through the ordinary Prefect flow/task runner and
  authenticated HTTP client.
- [x] As a developer, I can use an explicit context whose
  `accepted_source_position` comes from the committed source authority rather
  than an in-process counter.
- [x] As a user, I got several records from one generator attempt with early
  downstream delivery before the source TaskRun reached `Completed`, followed
  by an exact exhaustion-driven durable EOF tail.
- [x] As a user, I got position 200 again after failure before append acceptance
  and position 201 after position 200 committed but its response was lost.
- [x] As a system, I can replay an exact positioned value and EOF, reject drift,
  and fence the former source owner after the same TaskRun retries.
- [x] As a system, I can reject an unstarted reservation, preserve OPEN state
  across an intermediate retry, and fail the source only from the exact final
  TaskRun lifecycle witness.
- [x] As a system, I got one source TaskRun and one source stream across the
  bounded record sequence, with no TaskRun or orchestration event per record.
- [x] As a system, I can finish an empty generator at EOF position zero and
  reject non-contiguous positions, a non-`None` generator return,
  non-generator functions, unsupported runners and unbounded source profiles
  before unsafe mutation.
- [x] As a developer, I got ordinary finite-task, operator-stream, generator,
  client-package and source-disabled regression coverage.
- [x] As a developer, I got real PostgreSQL 17 and HTTP process-loss tests for
  append-before-commit, commit-before-response, owner takeover and EOF replay,
  with source hashes bound to the evidence.
- [x] As a system, I got independent review that distinguishes TaskRun retry
  recovery from the deferred whole-worker reconstruction protocol.

## Evidence

The initial inspected boundary is retained below; the final implemented gate
following it supersedes these starting observations:

- [`StreamTaskConfig`](../../../src/prefect/streams.py) rejected `source=True`;
- [`Task.submit_stream`](../../../src/prefect/tasks.py) required an upstream
  `Stream` and used the operator output reservation path;
- the [stream engine](../../../src/prefect/_internal/stream_engine.py) implements
  only per-input consumer turns at the starting boundary;
- the [native client backend](../../../src/prefect/client/stream_storage.py)
  initially lacked source-task lifecycle methods;
- the [server store](../../../src/prefect/server/models/streams.py) and
  [HTTP routes](../../../src/prefect/server/api/streams.py) expose low-level
  source claim, contiguous append and finish operations; immutable native
  source-task reservation and pre-owner cleanup were not yet implemented.

A tiny in-process design probe opened one generator at accepted position 198,
accepted positions 199 and 200, discarded that frame, and opened a fresh
generator at durable position 200. The fresh attempt began at 201; scanner-open
inputs were `[198, 200]`, one per attempt rather than one per record. This probe
compares the call model only. It does not exercise Prefect, HTTP, PostgreSQL,
ownership or failure atomicity and is not N1.4 acceptance evidence.

The accepted N1.3 transport evidence in
[ADR-0034](ADR-0034-native-stream-http-adapter.md) proves the underlying HTTP
and store boundary for its declared operator scope. It does not prove this
source SDK/runtime. N1.4 acceptance comes from its own executed gate below.

### Final implemented gate — 2026-11-02

[The acceptance aggregate](../evidence/native-stage4-acceptance-2026-11-02.json)
binds all final source hashes, commands, logs and JUnit results.
[Independent Principal review](../evidence/native-stage4-principal-2026-11-02.json)
verified all eleven criteria and the explicit recovery boundary.

| Gate | Result | Evidence |
| --- | --- | --- |
| Source/operator SDK, actual HTTP/process recovery, built stripped client and wire contracts | 145 passed, no skips, no source drift | [Native gate](../evidence/native-stage4-final-native-http-2026-11-02.json) |
| Retained operator test-adapter contract | 36 passed | [Legacy contract](../evidence/native-stage4-final-legacy-contract-2026-11-02.json) |
| Native store/API, including source reservation/open/close and lifecycle | 61 passed | [Store/API gate](../evidence/native-stage4-final-store-api-2026-11-02.json) |
| PostgreSQL migrations; SQLite capability and migrations | 4 + 7 passed | [PostgreSQL](../evidence/native-stage4-final-postgres-migrations-2026-11-02.json), [SQLite](../evidence/native-stage4-final-sqlite-capability-2026-11-02.json) |
| Ordinary task engine, futures, tasks and runners | 718 passed, 2 existing expected failures | [Kernel](../evidence/native-stage4-final-kernel-2026-11-02.json) |
| Existing client/base/routes | 333 passed, 2 existing skips, 1 existing non-strict xpass | [Client](../evidence/native-stage4-final-client-2026-11-02.json) |
| Required type analysis, public completeness, generated API/UI | 1,290 files, zero errors; 100%; build and 323 API tests passed | [Static/UI](../evidence/native-stage4-static-2026-11-02.json) |

The three process controls use production APIs and PostgreSQL 17. A unique
trigger pauses an INSERT transaction at an observed advisory-lock wait; killing
the API before commit leaves source position/count/prefix unchanged. A child
client accepts append then exits with `os._exit(77)`; after API restart, normal
TaskRun retry transitions claim a newer owner and resume from the durable
position. Durable EOF survives another restart and reconciles as COMPLETED.
The low-level caller retains exact identities; this does not reconstruct a
lost flow's Python graph or submission.

The stripped-client child has no server models/database modules or database
configuration. It executes an actual async native source and operator over
HTTP. Source and operator paths reuse engine-owned clients per record.

[Retained negative trials](../evidence/native-stage4-negative-findings-2026-11-02.json)
record the initial common parameter resolver assuming every stream config had
an input. Its fix permits stream dependencies only for operator configuration;
source side inputs resolve as ordinary finite inputs. Test-only DTO, fault
sender and error-precedence corrections remain visible. The failed overlapping
UI dependency install/build is retained; its sequential successor passed.
Existing multiprocessing resource-tracker diagnostics remain unsuppressed and
retain the unchanged-baseline classification from ADR-0033.

One diagnostic limit remains: an exception in a user generator's cleanup can
replace an earlier exception's message. Both stay on a failed path; neither can
accept an additional record or successful EOF.

The considered per-publication source-call model was not selected: reopening
scanners or paginated sessions for every document weakens source locality.
One generator per attempt preserves that locality while durable positions
govern reconstruction. This choice does not preserve Python frames.

The next [durable submission recovery proposal](ADR-0036-durable-submission-recovery.md)
is `pending`; its open generation/identity questions earn no readiness.

## Failure modes

- Reusing unbound `create_stream` can leave an OPEN source without an exact
  TaskRun cleanup authority after submission failure.
- Advancing an in-memory counter before append commit can skip a record after a
  crash; advancing it on timeout can skip a committed-but-unacknowledged record.
- Restarting a Python generator without passing the durable accepted position
  can duplicate external reads and cannot identify the first unaccepted record.
- Treating an arbitrary function return or non-`None` generator return as EOF
  can silently truncate a source after a coding error.
- Closing the source on an intermediate TaskRun failure prevents native retry;
  allowing a stale binding to close it after takeover corrupts the new owner.
- Marking the TaskRun `Completed` before durable EOF can leave downstream tasks
  waiting on an OPEN tail that no active owner can finish.
- Retrying permanent capacity refusal as transient pressure can loop forever in
  the bounded profile.
- Claiming whole-worker recovery from TaskRun retry evidence can hide the loss
  of submission identity and in-memory future graph.

## Consequences

The source API becomes explicit and somewhat stricter than an ordinary Python
generator. Within one attempt it retains the natural long-lived generator and
scanner model; across attempts it deliberately reconstructs the generator from
durable position instead of preserving its frame. Position, replay and EOF
behavior are therefore deterministic and auditable. Users who read external
systems remain responsible for mapping the durable contiguous position to an
idempotent external read; N1.4 does not make external effects transactional
with the Prefect store.

The finite bounded profile is useful for proving native source semantics but is
not an indefinite streaming runtime. It deliberately exposes capacity limits
until reader pins and online reclamation have their own protocol.

Separating N1.4 from whole-worker reconstruction keeps the first implementation
reviewable and prevents task retry evidence from being misreported as
infrastructure recovery. The follow-up must build on the same identities rather
than replacing them.

## Links

- [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- [ADR-0032: Native task and stream dependency contract](ADR-0032-native-task-stream-contract.md)
- [ADR-0033: Native durable stream storage](ADR-0033-native-durable-stream-storage.md)
- [ADR-0034: Native stream HTTP adapter and runtime admission](ADR-0034-native-stream-http-adapter.md)
- [Native task declarations](../../../src/prefect/tasks.py)
- [Native stream contracts](../../../src/prefect/streams.py)
- [Native stream engine](../../../src/prefect/_internal/stream_engine.py)
- [Native HTTP storage client](../../../src/prefect/client/stream_storage.py)
- [Native server store](../../../src/prefect/server/models/streams.py)
