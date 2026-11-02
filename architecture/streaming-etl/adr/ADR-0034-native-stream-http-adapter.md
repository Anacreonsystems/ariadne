# ADR-0034: Native stream HTTP adapter and runtime admission

- Status: `accepted`
- Date: 2026-11-01
- Reviewed: 2026-11-02, SDK/server implementation with ETL Architect / HighLoad Principal
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md), Stage 2 / N1.3
- Owners: SDK/server maintainers, ETL Architect / HighLoad Principal
- Depends on: [ADR-0032](ADR-0032-native-task-stream-contract.md),
  [ADR-0033](ADR-0033-native-durable-stream-storage.md)
- Delivery: [task registry](../todo.md), [sprint board](../sprint_board.md)

## Problem

Native server storage alone is not an SDK connection. Workers must use the
Prefect API rather than receive database credentials or import server models
from the stripped client package. The TaskRun engine publishes states through
asynchronous events; its local RUNNING attempt can precede the durable server
snapshot required by storage admission.

## Theory

An authenticated API can expose committed store operations and translate frozen
bindings/tokens without moving progress authority into a client process.
Recovery requires an explicit handshake for TaskRun metadata lag and compatible
versions/capabilities.

## Proposed solution

The initial review questions are retained below. The 2026-11-02 agreement
following them governs implementation; acceptance still requires executed code.

- Routes use existing API authentication and native DB dependency injection;
  stream scope and TaskRun identities come from stored rows.
- The adapter carries safe routing/identity data and obtains a client on the
  caller loop. Credentials, an AsyncClient, a pool or server imports must not
  be serialized into task parameters.
- Requests carry immutable bindings/tokens. Responses follow commit. Error
  mapping distinguishes stale ownership, metadata not ready, unsupported
  capability, changed intent, size/capacity and released replay horizon.
- Admission waits for or establishes a verified current RUNNING TaskRun
  snapshot. It cannot invent attempt 999 or overwrite orchestration authority
  to bypass event lag. Cancellation and attempt changes fence record writes.
- Resolve output reservation and rejected-start cleanup before integration:
  an output can exist before its TaskRun is recorded or an owner is admitted.
  A worker failure in that interval needs authenticated lifecycle authority to
  close the reserved output, without forging a RUNNING attempt or a data-owner
  epoch. Otherwise downstream can wait forever on an unclaimed OPEN stream.
  The six Stage 1 startup-failure cases must be repeated against the native
  transport, rather than inherited from its test adapter.
- Native DB protocol version 1 and terminal-event contract `yield-v2` are
  separate axes. Unsupported combinations reject before data admission; there
  is no global flow mode.
- The adapter inherits bounded capacity and whole-flow release. Automatic HTTP
  retries must not loop forever on permanently undersized frozen limits.

Source SDK, whole-flow/infrastructure recovery, branch/join, full deployment
lifecycle, online GC and production HA/throughput retain later decisions.

## Agreement — 2026-11-02

The three prerequisite pull requests were reviewed and merged in order:
[PR #2](https://github.com/Anacreonsystems/ariadne/pull/2),
[PR #3](https://github.com/Anacreonsystems/ariadne/pull/3), and
[PR #4](https://github.com/Anacreonsystems/ariadne/pull/4).
Further implementation uses one working branch, `native-stream-storage`, based
on the merged main tree. Historical source branches remain evidence references;
new work does not form another chain of dependent feature branches.
The exact tested heads and resulting merge commits are retained in
[the consolidation record](../evidence/native-pr-consolidation-2026-11-02.json).

### Identity, reservation and failure

- Allocate a TaskRun UUID and an output UUID before submitting the native task.
  The runner, engine, future, output reservation and logical consumer must use
  that same TaskRun id. Ordinary task submission is unchanged.
- Reserve the output under its FlowRun and exact producer TaskRun UUID before
  a worker can start. Existing partition fields retain the immutable binding;
  there is no owner, attempt or epoch until authoritative admission.
- An exact reservation retry returns the same header. Drift and cross-flow
  reuse refuse without mutation. Rejecting an unstarted output may only fail
  that exact, still-unowned reservation with no records or consumer. Its race
  with owner admission is serialized; rejection cannot replace an owner.
- Final task failure is a separate lifecycle operation, not a fabricated data
  owner. It verifies the stored TaskRun's exact FlowRun, attempt and final
  FAILED/CRASHED/CANCELLED state before closing that producer's output. This
  also handles a newer retry failing before it can replace an older owner;
  stale attempts cannot close newer work. Parent-before-child locking follows
  [ADR-0095](ADR-0095-implicit-foreign-key-lock-order.md).

### Recorded TaskRun admission

Submission constructs one local Pending TaskRun, registers its exact id,
dynamic key, task key and flow through the existing `create_task_run` API,
then reserves the output before passing that same prebuilt TaskRun to the
runner/engine. Require returned identity equality; do not silently adopt a
canonicalized different row or allocate another dynamic key in the engine.
Normal task-engine events still publish Running and retry states. No forced
state transition or invented attempt bridges metadata lag. If submission or
worker entry fails, only a confirmed still-Pending registration may emit the
normal Failed task event; an unverified canonicalized row is never failed.

Consumer admission checks the durable row under its transaction locks. Missing
or behind nonfinal metadata gets a typed `METADATA_NOT_READY` refusal and no
owner/progress mutation. The client may bounded-poll that same request with
the same owner id. Exact RUNNING/run_count admits; server-ahead, final,
cancelled and identity-conflicting rows refuse. A forged far-future attempt
cannot create an epoch while waiting. Event-worker draining alone is not
evidence that the recorder committed.

### HTTP and client boundary

- The authenticated route prefix is `/streams/v1`. POST operations expose
  create; output reserve/reject/final failure; consumer open/read/complete/
  finish; low-level source claim/append/finish; and final-flow release. GET
  operations expose status and a bounded prefix. These source operations wrap
  the existing store; no source-task decorator profile is added.
- DTOs are mirrored in separate client/server schema modules. DB protocol 1
  and terminal contract `yield-v2` are validated independently. Each mutation
  owns one store transaction and responds after commit.
- Errors carry `{code, message, retryable}` in HTTP detail. Metadata lag is
  retryable; stale ownership, identity drift, lifecycle conflicts, unsupported
  capability, size/capacity refusal and expired horizon retain distinct codes.
  Retries reuse identical identities and frozen terminal intent. Frozen
  capacity refusal does not become an infinite retry loop.
- Client methods have paired sync/async implementations in an orchestration
  submodule. The production storage adapter retains only safe routing/config
  data in task parameters. It resolves the existing client context on the
  caller's loop; pools, live clients and credentials are not pickled. Per-record
  calls reuse task-engine client/session ownership.
- Native terminal finish uses the acquired consumer binding explicitly.
  Pre-admission rejection uses the reservation identity; final lifecycle
  failure uses authoritative task identity. A mutable output-id-to-owner cache
  cannot substitute a new owner's token into an old owner's failure request.

### Required execution evidence

Repeat the six existing startup-failure scenarios against native HTTP storage.
Run real Uvicorn and PostgreSQL 17 through sync/async tasks, same-TaskRun retry,
delayed metadata, reserve/reject/open races, lost responses and server restart
around durable acceptance. Verify one output/state/cursor receipt, unchanged
intent on retry, explicit drift refusal, no per-record TaskRun growth, API
authentication, unsupported capability refusal and stripped-client packaging.
N1.3 may earn its planned +10% only after its acceptance criteria pass.

Process-loss acceptance is scoped to a low-level HTTP client whose caller
retains the exact serialized reservation, binding and delivery token. A fresh
client may replay that intent after its predecessor dies and the API restarts.
This does not reconstruct `submit_stream` after losing a flow/worker process.
Such reconstruction needs a durable submission identity and lookup/resume
contract in the later runtime stage. In particular, an output already
COMPLETED before the worker emits its Completed task state is a later
worker-reconciliation case. Current canonicalized-id refusal remains fail-closed.

## Stories

- As a developer, I want native tasks to use durable storage through the normal
  client without database access.
- As a user, I want a lost response or restarted client to recover durable
  progress without another logical consumer or output.
- As a system, I want unsupported backends, stale attempts and cross-flow
  requests refused before mutation.

## Acceptance criteria

- [x] As a developer, I can run sync/async native TaskRuns through an actual
  HTTP adapter with native migrations and no prototype module.
- [x] As a system, I can handle metadata lag without accepting a future or
  cancelled TaskRun generation.
- [x] As a user, I can recover around terminal acceptance after loss of a
  low-level client process, lost HTTP responses and an API restart, when the
  caller retains the exact serialized reservation/binding/delivery token.
- [x] As a system, I can terminate rejected/unstarted output reservations
  without weakening fencing or leaving a downstream dependency OPEN forever.
- [x] As a developer, I got DTO/error parity, API authentication and a working
  stripped-client package.
- [x] As a system, I can reject unsupported backends/versions, cross-flow
  bindings and released references without partial data.
- [x] As a developer, I got independent review and real HTTP/PostgreSQL gates.

## Evidence

The [final acceptance aggregate](../evidence/native-stage3-acceptance-2026-11-02.json)
and [Principal review](../evidence/native-stage3-principal-2026-11-02.json)
bind the executed source and close this scoped N1.3 decision. N1.2 evidence
does not replace the HTTP/process gate. This earns the planned +10%, bringing
the bounded native foundation to 30%; production readiness is not measured.

| Gate | Executed result | Evidence |
| --- | --- | --- |
| Native SDK, real HTTP/process recovery, stripped wheel and client contracts | 78 passed, no skips | [Final transport gate](../evidence/native-stage3-final-sdk-final-2026-11-02.json) |
| Native store and HTTP server | 54 passed, no skips | [Store/API gate](../evidence/native-stage3-native-store-api-2026-11-02.json) |
| Retained Stage 1 test-adapter contract | 36 passed | [Legacy contract](../evidence/native-stage3-final-legacy-contract-2026-11-02.json) |
| PostgreSQL migrations; SQLite migrations/capability refusal | 4 + 7 passed | [PostgreSQL](../evidence/native-stage3-postgres-migrations-2026-11-02.json), [SQLite](../evidence/native-stage3-sqlite-capability-2026-11-02.json) |
| Ordinary engine/future/task/runner | 718 passed, 2 pre-existing expected failures | [Kernel regression](../evidence/native-stage3-final-kernel-2026-11-02.json) |
| Existing client/base/routes | 333 passed, 2 existing skips, 1 existing non-strict xpass | [Client regression](../evidence/native-stage3-final-client-regression-2026-11-02.json) |
| Required configured type analysis | 1,290 files, zero errors | [Type report](../evidence/native-stage3-ci-types-final-2026-11-02.json) |
| Public type completeness; generated API/UI | 100%; build and 323 API tests passed | [Static/UI report](../evidence/native-stage3-static-2026-11-02.json) |

CI now requires the native PostgreSQL 17 HTTP/process/package gate and both
database capability profiles to execute without native skips. Hosted CI for
the published change is separate from these completed local gates.

### Hosted CI follow-up — 2026-11-02

[PR #5](https://github.com/Anacreonsystems/ariadne/pull/5), revision `d4451b8`,
completed 96 successful checks, five conditional skips and one failed test-list
coverage check. Both native jobs passed, including all 78 HTTP/process/package
and client-contract cases on PostgreSQL 17. The
[hosted record and job logs](../evidence/native-stage3-hosted-ci-2026-11-02.json)
retain the results.

The coverage verifier reads only the general test matrix. Three new top-level
files were executed by the dedicated native job but absent from that matrix.
They are now listed in `client-execution` and its matching SQLite exclusion;
the unchanged verifier covers all 482 files. The dedicated PostgreSQL 17 job
still requires the native cases to execute without skips. Unsupported general
database profiles retain their explicit capability skips. This correction
changes no SDK, engine, API, schema or storage bytes and earns no extra share.
The successor hosted result remains pending.

### Executed findings

[The retained negative traces](../evidence/native-stage3-negative-findings-2026-11-02.json)
record three discoveries during actual HTTP/PostgreSQL execution:

1. The shared root test profile disabled TaskRunRecorder. Admission correctly
   refused unrecorded RUNNING metadata; the native fixture now starts the real
   service, rather than forcing stored state to match a local engine.
2. Final operator failure left an admitted output OPEN because lifecycle
   fallback expected `TERMINAL_CONFLICT` while reservation rejection returns
   `STALE_OWNER`. The client now handles the exact typed refusal and verifies
   the final TaskRun before failing that output. Sync/async regressions cover
   this and a newer retry failing before replacing the previous owner.
3. A worker-entry callback can execute after leaving the worker's Context.
   Compensation lost API/settings ownership and left its output OPEN. The
   callback now runs in a copy of the submitting Context. A worker is explicitly
   held until callback registration in the startup regression, so the callback
   must execute outside its former worker Context. Runner shutdown waits for
   cleanup; normal finite-task callbacks are unchanged.

Required static analysis also moved the unchanged structural lifecycle protocol
from public `prefect.streams` into the existing private stream-engine module.
The native, package, legacy and kernel gates were repeated on the final source.
Two existing proxy-inspection tests exposed inherited upper/lower-case proxy
and loopback-bypass settings. Archived main reproduced the issue. Their fixtures
now isolate the fake transport configuration; production routing is unchanged.
The known multiprocessing resource-tracker diagnostics remain visible in kernel
logs and retain their unchanged-baseline classification from ADR-0033.

The source registration helper in the HTTP tests uses the existing low-level
source API. It is not an implemented native source-task decorator. The stripped
client test builds the actual client wheel, removes database configuration from
the child process, and executes an async stream task through the real API.

## Consequences

Acceptance completes Stage 2's transport boundary and enables the later native
source/runtime sprint. Source-task SDK, whole-flow/worker reconstruction,
online GC, deployment HA and production throughput remain separate work.
