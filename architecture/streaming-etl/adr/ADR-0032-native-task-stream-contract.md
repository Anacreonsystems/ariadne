# ADR-0032: Native task and stream dependency contract

- Status: `accepted`
- Date: 2026-10-31
- Accepted: 2026-11-01, Stage 1 native SDK/task-engine contract scope
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- Owners: Prefect task-engine maintainers, streaming runtime maintainers,
  ETL Architect / HighLoad Principal
- Depends on: [ADR-0031](ADR-0031-validated-durable-stream-theory.md),
  [ADR-0010](ADR-0010-platform-durable-yield-channel.md),
  [ADR-0011](ADR-0011-yield-completion-and-client-effects.md),
  [ADR-0017](ADR-0017-sync-async-generator-adapter.md)
- Related prototype boundaries:
  [ADR-0024](ADR-0024-control-plane-authority-and-lifecycle.md),
  [ADR-0025](ADR-0025-prefect-adapter-and-operational-projections.md)

## Problem

Prefect tasks can already wrap Python generator and async-generator functions.
Their yielded values do not currently form a platform-owned durable stream
dependency with a stable consumer cursor, fenced retries, per-input completion,
or immediate downstream readiness. Adding another wrapper around `yield` would
leave that distinction unresolved.

Native integration also cannot reinterpret every generator task or every task
dependency as a stream. Existing finite tasks, futures, generator behavior,
retry rules, and completion states must remain compatible. A mixed flow needs
to express a durable streaming edge alongside ordinary finite dependencies
without selecting a global execution mode.

The first stage therefore needs a narrow contract for task identity, stream
dependency readiness, and one durable input turn. It must not silently decide
future multi-output, fan-out-output, filtering, source-publication, storage,
server, worker, or UI protocols.

## Theory

The durable-stream theory is successful within the scope recorded by
[ADR-0031](ADR-0031-validated-durable-stream-theory.md): a platform-owned
ordered edge, stable logical consumer, fenced owner, and atomic terminal
completion can distinguish an incomplete input from one whose platform output,
managed state, completion, and cursor are durable.

That theory can enter Prefect natively by making stream behavior an explicit
property of an output/input edge and its task invocation contract. The ordinary
finite dependency path can remain unchanged. Prefect remains the authority for
TaskRun and FlowRun lifecycle, while the stream progress authority records the
per-input turn under the same native execution model.

This ADR plans the first native slice. It does not claim that documentation, the
standalone prototypes, or the historical external adapter implement this slice.

## Proposed solution

### Native dependency kind

Add an explicit stream dependency kind to Prefect's graph and task invocation
model. A flow may contain ordinary finite edges and stream edges at the same
time. There is no flow-wide streaming mode.

An ordinary finite input keeps its current readiness rule. A task consuming a
stream input may become runnable for a durable input record while its producer
TaskRun is still running. A finite task may consume a stream only through an
explicit bounded collection boundary whose completion condition is known. An
implicit direct stream-to-finite dependency must fail validation rather than
wait forever or observe an arbitrary prefix.

The first implementation slice is expected to touch the native
[task definitions](../../../src/prefect/tasks.py),
[task engine](../../../src/prefect/task_engine.py), and dependency/future
[contracts](../../../src/prefect/futures.py). Later stages will define the
server, persistence, worker, deployment, and UI changes. The existing
[server orchestration policy](../../../src/prefect/server/orchestration/core_policy.py)
remains the FlowRun lifecycle boundary; this stage must not bypass the server
API for FlowRun state changes or replace the task engine's existing ownership
of TaskRun state.

### Task, retry attempt, consumer, and worker identity

Petya and Anna are Prefect tasks represented by Prefect TaskRuns. Petya
publishes stream records; Anna consumes them. If Anna fails on document 200,
Tanya is a retry attempt of **the same TaskRun and the same stable logical
consumer**. Tanya is not a new downstream task, a new consumer, or a worker.

A worker is infrastructure that launches or hosts flow execution. Replacing a
worker or other execution infrastructure may require a separate fenced
infrastructure attempt, but that future protocol must preserve the TaskRun and
consumer identities defined here. This ADR does not equate task retry with
infrastructure replacement.

### One input turn

The first native operator protocol preserves the validated protocol-epoch-2
rule: one durable input permits exactly one terminal `yield` or `return`.

For terminal `YIELD`, reaching a Python `yield` instruction is insufficient.
When the platform accepts that terminal yield, it atomically records:

- the one platform-managed output envelope;
- the managed state required by the next input;
- the terminal completion witness; and
- the consumer cursor advance.

The accepted yield completes the input turn. The runtime must not wait for
generator exhaustion or a later `return` before making the output durable and
ready downstream. The runtime closes the invocation after that terminal event;
it does not resume or inspect otherwise unreachable generator code after the
yield. Generator cleanup cannot publish another output or alter the accepted
value or managed state.

`RETURN` remains the other terminal completion kind from the validated
single-turn protocol. This ADR does not generalize it into a filtering or
arbitrary empty-output feature. Multiple yields, multiple outputs, named output
fan-out, and general zero-output operator behavior require later explicit ADRs
and storage protocols. A task that declares one of those unsupported profiles
must fail closed before input admission. If the runtime or storage boundary
later receives a duplicate or conflicting terminal observation for an already
accepted turn, it may reconcile only the exact existing completion for lost-ack
recovery; otherwise it rejects the observation without a new mutation. The
durable first output, state, completion, and cursor remain unchanged. Stage 1
does not require static inspection of unreachable generator code after the
accepted yield.

If the process fails before durable terminal completion, document 200 is
redelivered to the same stable consumer under a fenced retry owner. If the
completion commits and only its acknowledgement is lost, recovery observes the
durable cursor and starts with document 201. A stale owner cannot publish a
terminal write after takeover.

External PostgreSQL, HTTP, filesystem, and other client effects are not part of
the platform transaction. Their determinism and idempotency remain the client's
responsibility under [ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

### Source boundary

A native source may publish many records without consuming one upstream record.
Its progress therefore requires a separate source cursor, append identity, and
admission contract. Source publication must not be modeled as repeated terminal
yields of the per-input operator contract in this ADR. A later stage must define
and accept that native source protocol before production source execution is
claimed.

### Lifecycle authority

Stage 1 preserves Prefect's existing ownership model. Prefect Server
orchestrates FlowRun state transitions through the server API. The native task
engine manages TaskRun states locally and reports them through the existing
task-run event path. Stream progress storage is authoritative only for record
identity, delivery attempt, owner fence, terminal completion, managed state,
and consumer cursor.

How a future stream fence or recovery result reconciles with native TaskRun
events requires a later lifecycle ADR and failure tests. This stage must neither
bypass the server for FlowRun state transitions nor let stream storage publish
an independent FlowRun or TaskRun terminal state.

No external adapter or second orchestrator is part of the target architecture.
Historical adapter evidence informs the native implementation, but an adapter
row cannot independently declare a Prefect TaskRun completed, failed, retried,
drained, or cancelled.

## Agreement

Native integration epoch 1 will begin with this task and dependency contract:

1. Stream is an explicit edge kind inside a mixed Prefect flow.
2. Existing finite tasks and ordinary Prefect generator functions retain their
   behavior unless they participate in the explicit native stream contract.
3. One input turn has one terminal `yield` or `return`; platform acceptance of
   that terminal event is the durable progress boundary.
4. A retry retains the TaskRun and stable consumer identity and receives a
   fenced execution owner.
5. Existing Prefect ownership remains: the server API orchestrates FlowRun
   state, the task engine manages and reports TaskRun state, and stream storage
   owns per-record progress.
6. Unsupported source, multiple-output, fan-out-output, or empty-output
   profiles fail closed before admission until their own decisions are
   accepted; a later duplicate or conflicting terminal observation cannot
   mutate the accepted completion.
7. Native SDK, server, storage, worker, deployment, and UI integration will be
   delivered in explicit later stages rather than hidden behind a permanent
   external adapter.

## Stage 1 boundary

Stage 1 covers the contract, SDK-facing dependency representation, task-engine
readiness slice, and native TaskRun/consumer identity binding needed to execute
meaningful sync and async tests. It may use a narrowly scoped test storage
fixture while the native persistence stage is still pending.

Atomic output/state/completion/cursor assertions against that fixture prove the
Stage 1 task-engine contract only. They do not prove transaction atomicity,
crash recovery, fencing, or durability in the native product storage. Those
claims require the real persistence implementation and evidence in Stage 2 and
later integrated stages.

Stage 1 does **not** claim completion of Native integration epoch 1. It does not
accept production storage, source admission, barriers, branch/join recovery,
retention, backpressure, server migrations, distributed worker replacement,
deployment upgrades, authorization, UI, or operational scale. Later stages
must implement and accept those boundaries without weakening this contract.

Under [ADR-0000](ADR-0000-decision-lifecycle.md), this ADR may become accepted
only for its explicitly declared Stage 1 scope after code and every criterion
below pass. Such scoped acceptance would not establish production readiness for
the native engine. The broader production criteria remain open in the parent
epoch and its later ADRs, consistent with the review responsibility in
[ADR-0008](ADR-0008-architecture-role-charter.md).

## Stories

- As a developer, I want to connect a Prefect task output to a native stream
  input without changing unrelated finite task dependencies.
- As a developer, I want sync and async task bodies to use one documented
  terminal `yield` or `return` contract per durable input.
- As a user, I want Anna's accepted output for document 200 to become available
  before Petya's TaskRun completes.
- As a user, I want Tanya to retry Anna's same TaskRun and stable consumer from
  the first input without a durable completion.
- As a user, I want ordinary Prefect tasks and generator functions to preserve
  their existing behavior when no stream edge is declared.
- As a system, I want Prefect lifecycle and per-record progress to have distinct,
  non-competing authorities.
- As a system, I want unsupported dependency and output shapes rejected before
  they can mutate stream progress.

## Acceptance criteria

- [x] As a developer, I can declare native stream output and input edges in a
  mixed Prefect flow and inspect their dependency kind without selecting a
  global mode.
- [x] As a developer, I can execute real sync and async native task inputs where
  one accepted terminal `yield` or `return` completes exactly one durable input
  turn.
- [x] As a user, I got the accepted output downstream before the producer
  TaskRun reached `Completed`.
- [x] As a user, I got document 200 after failure before durable completion and
  document 201 after completion committed but its acknowledgement was lost.
- [x] As a user, I got Tanya as a retry attempt of Anna's same TaskRun and stable
  consumer rather than a TaskRun or consumer per document.
- [x] As a system, I got output, managed state, completion, and cursor together
  or none in the Stage 1 contract fixture after every tested terminal boundary,
  without treating that fixture as native storage atomicity evidence.
- [x] As a system, I can reject a stale-owner terminal write, reject a declared
  multi-yield profile before admission, and prevent a duplicate or conflicting
  terminal observation from mutating the already accepted output, state,
  completion, or cursor.
- [x] As a developer, I can run existing finite task, future, retry, and ordinary
  generator regression suites without a stream edge changing their behavior.
- [x] As a developer, I can connect a stream to a finite task only through an
  explicit bounded collection boundary; an unbounded direct dependency fails
  validation.
- [x] As a system, I can reject unsupported source, fan-out-output,
  multiple-output, and generalized empty-output profiles before any unsafe
  write.
- [x] As a system, I got bounded TaskRun metadata: record count increases do not
  create a TaskRun per record.
- [x] As a system, I got TaskRun lifecycle transitions only through Prefect's
  native orchestration boundary.

## Evidence

The successful theory and its limits are recorded by
[ADR-0031](ADR-0031-validated-durable-stream-theory.md). Historical executable
evidence for the turn contract includes
[ADR-0010](ADR-0010-platform-durable-yield-channel.md),
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md), and
[ADR-0017](ADR-0017-sync-async-generator-adapter.md). The control and real-Prefect
adapter experiments in [ADR-0024](ADR-0024-control-plane-authority-and-lifecycle.md)
and [ADR-0025](ADR-0025-prefect-adapter-and-operational-projections.md) expose
integration risks but are not evidence that this native Stage 1 exists.

Native implementation began on 2026-11-01. The selected API is an ordinary
`Task(stream=StreamTaskConfig(input=...))` submitted through its explicitly
typed `submit_stream()` method. `Task.__call__` and `Task.submit` reject this
declared stream profile rather than contradict their finite-result overloads.
Ordinary tasks and generators retain those existing entrypoints.

The [native contracts](../../../src/prefect/streams.py),
[per-input executor](../../../src/prefect/_internal/stream_engine.py),
[Task SDK](../../../src/prefect/tasks.py),
[task engine](../../../src/prefect/task_engine.py),
[runner](../../../src/prefect/task_runners.py), and
[dependency resolver](../../../src/prefect/utilities/engine/__init__.py)
now contain the first implementation. The
[frozen-source acceptance](../evidence/native-stage1-acceptance-2026-11-01.json)
passes all Stage 1 criteria. Real PostgreSQL adapter evidence remains an
integration test of this interface, not proof of native production migrations.

Required Stage 1 acceptance evidence, now supplied by the final gate below:

- reviewed native changes under `src/prefect` for the dependency representation
  and task-engine slice;
- behavior-focused sync and async tests covering early delivery, retry identity,
  pre-commit failure, lost acknowledgement, stale ownership, and unsupported
  profiles;
- regression evidence for finite tasks, futures, retries, and existing generator
  tasks;
- a mixed-flow integration test with an explicit bounded collection boundary;
- measurements proving TaskRun and orchestration metadata do not grow per
  record; and
- an ETL/HighLoad Principal review that records the remaining later-stage
  production gaps.

## Failure modes

- Treating every Python generator yield as a durable stream record changes
  existing Prefect behavior and bypasses explicit dependency typing.
- Waiting for generator exhaustion after an accepted terminal yield delays
  downstream delivery and reintroduces a hidden client commit boundary.
- Resuming or statically interpreting generator code after the accepted
  terminal yield invents behavior outside the one-terminal-event API.
- Creating a TaskRun or consumer for every record makes orchestration metadata
  the data plane and loses stable retry identity.
- Treating Tanya as a new TaskRun or as a worker can skip document 200 or create
  two active consumers.
- Advancing the cursor before output and managed state are durable can lose a
  document; publishing them before a fenced completion can duplicate or drift
  state.
- Letting a direct unbounded stream satisfy a finite dependency can leave a task
  permanently waiting or expose an arbitrary prefix.
- Reusing the per-input turn protocol for a multi-record source loses source
  append identity and admission semantics.
- Admitting a declared multiple-yield, implicit fan-out-output, or generalized
  empty-output profile without a new manifest protocol creates ambiguous
  completion and replay. A duplicate terminal observation after acceptance must
  never rewrite the durable first completion.
- Allowing stream storage or an adapter to publish native TaskRun terminal state
  creates two lifecycle authorities.

## Consequences

The first implementation must change native Prefect dependency and task-engine
contracts rather than promote the historical adapter. Review must therefore
protect existing task and generator behavior with explicit regression tests.

The one-terminal-event rule keeps Stage 1 small and grounded in validated
failure semantics. It also means common filtering, flat-map, and multi-output
patterns remain unavailable until later protocol decisions. Native source,
fan-out, branch/join, persistence, lifecycle, deployment, and UI stages must
reference this ADR and state how they preserve or deliberately supersede its
contract.

The architecture remains one Prefect product: common SDK, server authority,
deployments, workers, storage interfaces, and UI. Internal data-plane modules
may have separate schemas, pools, and capacity controls so per-record traffic
does not turn ordinary orchestration metadata into the stream log.

## Initial planning review record

On 2026-11-01 the ETL Architect / HighLoad Principal reviewed this planning ADR
against the repository's existing FlowRun and TaskRun ownership model and the
completed scoped prototype handoff. The review corrected three boundaries:
FlowRun state remains server-orchestrated while TaskRun state remains managed by
the task engine; the one-terminal-event contract does not require inspection of
unreachable generator code; and fixture atomicity is contract evidence rather
than native storage evidence. The ADR remains `planning`, and no Stage 1
acceptance criterion is earned by this review.

## Implementation agreement (2026-11-01)

One `submit_stream()` call allocates one native TaskRun and output edge. The
existing task engine performs its normal context, retry, event and terminal
state work; the internal executor invokes the user function once per input.
It never submits another TaskRun per record. Configurations survive
`with_options()` and serialization. The context parameter is injected, so it
is omitted from the call arguments.

```python
from prefect import task
from prefect.streams import StreamContext, StreamTaskConfig


@task(stream=StreamTaskConfig(input="document"), retries=2)
def transform(document: int, *, ctx: StreamContext):
    ctx.managed_state["count"] = ctx.managed_state.get("count", 0) + 1
    yield document * 2


# Inside a flow, with an explicitly attached existing Stream:
# output = transform.submit_stream(upstream)
# finite_result = ordinary_task.submit(output.collect(limit=1000))
# output.wait()
```

The native `StreamStorage` interface is the production-code boundary; core code
imports no architecture prototype. The integration tests use a
[real PostgreSQL adapter](../../../tests/_native_stream_postgres.py) over the
retained data-plane implementation. This adapter is test infrastructure, not a
native migration, automatic source connector or deployed storage service.

YIELD persists one output and a frozen managed-state snapshot before invocation
cleanup; code after that yield is not resumed. RETURN persists completion and
managed state without an output envelope, following the validated storage
protocol. An optional returned dictionary supplies final managed state;
otherwise the context's managed state is used. This is not a general filtering
or multi-output API. External effects retain client-owned retry safety.

Supported execution is a top-level flow with a ThreadPoolTaskRunner and enough
slots for its active producer, stream operators and finite work. Nested stream
submission, process/distributed runners, map, deferred execution, implicit
finite collection and undeclared output profiles fail before admission.
An ordinary finite future cannot hide a Stream from dependency validation.

The current executor polls an empty input; high-load notifications, native
storage migrations, source admission, deployment serialization and infrastructure
replacement remain later-stage work. A Pause/control signal preserves an open
stream; this guard does not claim full server suspension/resume integration.

## Review findings and correction history

The implementation review required these additional cases before acceptance:

1. A committed end marker with a lost acknowledgement precedes TaskRun success.
   Retry reconciles only the same existing binding and fully drained immutable
   input tail; it never reopens the output or publishes more DATA.
2. Rejected submission or failure before TaskRun creation closes an allocated
   output with a failure marker while preserving the original exception.
3. Recursive side-input validation rejects nested bare streams before output or
   TaskRun creation. Only the declared input port accepts a Stream.
4. The public method is explicitly `submit_stream()`, keeping finite entrypoint
   overloads truthful.
5. Output failure finalization is limited to terminal Failed/Crashed/Cancelled
   states; Running/Pending and Prefect control signals preserve their open stream.
6. Async runner submission creates a coroutine inside the accepted worker job,
   avoiding an unawaited coroutine when execution is rejected before entry.

The first expanded candidate passed 22 tests before a test setup omitted the
required `create_task_run` arguments. After that correction, a hostile repeated
completion was correctly rejected by PostgreSQL but revealed an adapter
exception-type mismatch; the adapter now translates it without weakening the
transaction rejection. An async startup fault initially targeted the sync-only
creation helper; its corrected test targets `Task.create_local_run`. These are
retained test-harness findings, not passed negative controls. The corrected
focused startup matrix passed all six sync/async cases before final acceptance.

## Stage 1 acceptance evidence (2026-11-01)

The [acceptance record](../evidence/native-stage1-acceptance-2026-11-01.json)
binds all six native core files, two native test files and the retained
PostgreSQL data-plane dependency before and after execution. All nine hashes
match across both native profiles and the complete regression profile, with
no source drift.

| Profile | Actual execution and result |
| --- | --- |
| [SQLite orchestration](../evidence/native-stage1-sqlite-2026-11-01.json) | Real Uvicorn HTTP API, native Prefect SDK/engines and PostgreSQL 17.11 stream storage: 36/36 tests, no skip; 156.23 pytest seconds. |
| [PostgreSQL orchestration](../evidence/native-stage1-postgres-2026-11-01.json) | Real Uvicorn HTTP API, PostgreSQL orchestration metadata and PostgreSQL 17.11 stream storage: 36/36 tests, no skip; 153.42 pytest seconds. |
| [Finite runtime regression](../evidence/native-stage1-regression-2026-11-01.json) | Complete `test_task_engine`, `test_futures`, `test_tasks`, and `test_task_runners`: 718 pass, two existing expected xfails, no failure/error; 156.30 pytest seconds. |
| [Client wheel](../evidence/native-stage1-client-wheel-2026-11-01.json) and [source audit](../evidence/native-stage1-client-wheel-source-audit-2026-11-01.json) | Actual stripped `prefect-client` build and isolated Python 3.11 import pass. All six packaged core modules match current source byte for byte; server database code is absent. |

The tests are in [test_stream_tasks.py](../../../tests/test_stream_tasks.py).
They run a separate HTTP server process and real SQL operations, including
transaction rollback and committed-but-unacknowledged completion. They do not
replace the task engine or storage transaction with an in-memory simulation.
Only named rejection/startup failures are injected to exercise those boundaries.

| Acceptance area | Executed evidence |
| --- | --- |
| Native declaration, copying and typed edges | `test_stream_task_remains_a_task_and_preserves_options`, explicit Stream/collection kinds, and mixed finite dependencies. |
| Sync/async YIELD and RETURN | Both modes in terminal-failure, terminal-return and finish-acknowledgement tests. RETURN has no output envelope. |
| Early delivery | `test_native_early_delivery_and_mixed_finite_dependencies` persists and consumes output while its real producer TaskRun is still running; API dependency linkage is checked. |
| Input 200/201 and same TaskRun retry | `test_document_200_recovery_uses_its_actual_durable_sequence` starts with records 1 through 201, fails at sequence 200, and checks replay at 200 or continuation at 201 with the same TaskRun/consumer. |
| Output/state/completion/cursor and closed tail | Real PostgreSQL failure before commit, lost completion acknowledgement, cleanup-state freezing, and lost finish acknowledgement all converge without extra output. |
| Fencing and conflicting completion | Real storage rejects an old owner, accepts exact completion retry and rejects changed intent without a new mutation. |
| Finite and unsupported profiles | Explicit bounded collection succeeds; bare streams, nested side streams, hidden finite-Future streams, process runners, map/deferred modes and declared unsupported output profiles reject. |
| Bounded native metadata | At 1 and 1000 source documents, two operators and one finite task produce exactly three TaskRuns and at most four state records per TaskRun. |
| Lifecycle and startup | Six sync/async submission/start/executor failures close rejected outputs and preserve errors; Pause safety preserves OPEN rather than declaring native failure. |

The two xfails explicitly cover synchronous sleep inside an async task, which
the existing async timeout cannot interrupt. The full parallel regression also
emits Python multiprocessing `resource_tracker` semaphore diagnostics. The
[baseline comparison](../evidence/native-stage1-baseline-diagnostic-2026-11-01.json)
reproduces them on unchanged code: 49 serial ProcessPool tests pass without the
diagnostic, and 49 parallel baseline tests pass with the same diagnostic class.
It is retained as a separate infrastructure follow-up, not suppressed or
reported fixed by this implementation. Initial baseline setup errors are also
retained and excluded from successful baseline evidence.

This accepts only the implemented Stage 1 native contract. The selected
PostgreSQL adapter remains test infrastructure. Native storage migrations,
default backend, native source admission, whole-flow/infrastructure restart
correlation, full suspension/resume and production load remain later-stage
criteria in [epoch 1](../epochs/epoch-01-native-prefect.md).

The [final Principal acceptance](../evidence/native-stage1-principal-acceptance-2026-11-01.json)
verifies all gate records and frozen sources, accepts this scoped status and
closes sprint N1.1 at its planned 10% foundation share. No Stage 1 blocker
remains; the later-stage scope above remains unaccepted.

## Links

- Parent cycle: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- Successful scoped theory:
  [ADR-0031](ADR-0031-validated-durable-stream-theory.md)
- Durable channel and turn semantics:
  [ADR-0010](ADR-0010-platform-durable-yield-channel.md),
  [ADR-0011](ADR-0011-yield-completion-and-client-effects.md),
  [ADR-0017](ADR-0017-sync-async-generator-adapter.md)
- Historical integration evidence and limits:
  [ADR-0024](ADR-0024-control-plane-authority-and-lifecycle.md),
  [ADR-0025](ADR-0025-prefect-adapter-and-operational-projections.md)
- Native implementation surfaces:
  [tasks](../../../src/prefect/tasks.py),
  [task engine](../../../src/prefect/task_engine.py),
  [futures](../../../src/prefect/futures.py),
  [server orchestration policy](../../../src/prefect/server/orchestration/core_policy.py)
