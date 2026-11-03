# ADR-0036: Durable submission identity and whole-worker recovery

- Status: `progress`
- Date: 2026-11-02
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md),
  proposed Stage 4 / N1.5
- Owners: Prefect task-engine, server lifecycle and streaming runtime maintainers;
  ETL Architect / HighLoad Principal
- Depends on: [ADR-0032](ADR-0032-native-task-stream-contract.md),
  [ADR-0033](ADR-0033-native-durable-stream-storage.md),
  [ADR-0034](ADR-0034-native-stream-http-adapter.md),
  [ADR-0035](ADR-0035-native-source-task-runtime.md)

## Problem

The native stream stages recover a TaskRun attempt when the caller retains its
TaskRun, stream, binding and position identities. They do not reconstruct a
submission after the flow-launch process and its in-memory future graph are
lost.

The current submission path allocates random identities. `create_local_run`
uses `dynamic_key_for_task_run(..., stable=False)`, while native submission
preallocates fresh TaskRun and stream UUIDs. Re-entering the flow cannot derive
which former TaskRun and output stream correspond to a source or operator call.
Blindly submitting again creates a second logical channel; searching by task
name or call order can attach to the wrong channel after a branch, refactor or
concurrent submission.

Recovery also has two different failure cases:

1. A dead process cannot write, but its TaskRun metadata may still say
   `Running` and its source or consumer may have accepted progress beyond the
   last observed task event.
2. A partitioned or delayed process may still be alive. A replacement that
   merely looks up the old IDs can race that zombie. Existing stream owner
   epochs fence the former writer only after a new owner has claimed the
   particular source or consumer; they do not establish an execution-wide
   takeover before reconstruction begins.

Python frames, local futures, wall-clock guesses and arbitrary event arrival
order cannot be recovery authority. A new process needs a durable identity and
fence before it can decide whether to resume, reconcile a completed operation,
or reject changed code and graph shape.

## Theory

A submission can survive infrastructure replacement if the platform creates
one immutable manifest before execution and addresses it by a stable semantic
slot inside the FlowRun. The manifest binds the slot to its TaskRun, output
stream, role, protocol, code, parameters and upstream identities. Exact replay
returns the same manifest; a changed witness fails before a second TaskRun or
stream is created.

Identity lookup alone is insufficient. A replacement must first acquire a
durable execution generation that every subsequent stream admission, record
write, terminal marker and lifecycle reconciliation checks. A strictly newer
generation fences a zombie independently of clocks and before individual
source or consumer ownership is rotated.

After fencing, the replacement can inspect the immutable manifest and durable
stream state. It retains the same TaskRun UUID across infrastructure attempts,
uses the normal TaskRun retry/run-count protocol, and constructs a fresh Python
generator or operator invocation from durable progress. It does not restore a
Python frame or create a TaskRun per record.

This theory has not been implemented or accepted. In particular, Prefect does
not yet have an agreed server transition for taking over a TaskRun whose last
metadata is still `Running` after its process disappeared.

## Proposed bounded design

### Immutable submission manifest

Introduce a server-owned create-once manifest addressed by
`(flow_run_id, submission_key)`. Its proposed authority fields are:

- role (`SOURCE` or one-input `OPERATOR`) and protocol (`source-v1` or
  `yield-v2`);
- stable TaskRun UUID and output stream UUID;
- task key, deployment and code-version witness;
- canonical parameter digest, excluding injected runtime context and secrets;
- exact upstream stream identity or a no-upstream source marker;
- frozen stream limits and protocol version;
- graph or submission-schema version; and
- creation generation and immutable manifest digest.

The manifest response is the lookup API used by the SDK after a process
restart. Creating the same key with the same witness returns the original
TaskRun and stream. Reusing a key with changed role, code, parameters, upstream
or limits is an identity conflict and performs no mutation.

The first profile should require an explicit stable submission key for each
bounded source and operator in a finite linear flow. Branch, join, dynamic map
and conditional graph reconstruction remain unsupported until a graph manifest
can bind their topology without relying on execution order.

### Execution-generation fence

Before reading manifests for recovery, a replacement claims a new durable
FlowRun execution generation with compare-and-swap against the current
generation and holder. Every native admission and mutating stream request then
carries or resolves that generation in addition to its source or consumer
owner epoch. Once the generation advances, requests and late lifecycle events
from the former generation are rejected even if their individual stream lease
or binding still appears current.

Timeout alone must not silently authorize a takeover. The agreement must choose
an explicit server-observed holder loss, a renewable lease with conservative
expiry and monotonic generation, or a trusted infrastructure death witness.
Clock timestamps may aid diagnostics but cannot override the durable generation
comparison.

### TaskRun and stream reconciliation

After acquiring the generation, the replacement resolves each manifest and
applies a state matrix:

- OPEN stream plus retryable TaskRun resumes the same TaskRun UUID at its next
  normal run count and claims a new stream owner epoch;
- accepted source position or consumer cursor determines the first work item;
- immutable completed stream plus lagging TaskRun reconciles TaskRun success
  without re-entering user code;
- failed stream or terminal TaskRun must agree with the permitted lifecycle
  outcome;
- successful source TaskRun requires exact durable EOF first; and
- a TaskRun reported `Completed` while its required output remains OPEN is a
  conflict, not permission to infer EOF.

No client may use `force=True` to rewrite orchestration state. The server needs
an agreed recovery transition for an old `Running` TaskRun. That transition
must be bound to the new execution generation, preserve the TaskRun UUID,
advance `run_count` through normal retry semantics and make late events from the
former generation ineffective.

### Recovery boundaries

The first executable scope is a finite, bounded, linear source-to-operator
flow. Source recovery creates a fresh generator with the durable accepted
position. Operator recovery redelivers only an input without durable terminal
completion. Exact EOF, output and completion replay remain idempotent.

External reads and effects remain client-owned. A submission manifest can bind
an opaque connector or object reference, but it cannot make an external system
transactional with the Prefect database.

## Alternatives under review

### Explicit stable submission key

The developer or deployment declares a key for each source and operator slot.
This is the preferred first-scope candidate because a refactor must deliberately
preserve or change the key. Its cost is a new SDK concept and validation against
duplicate keys.

### Deterministic automatic ordinal

The flow runtime derives a key from task identity and submission order. This is
convenient for straight-line code but is unsafe under conditional branches,
concurrent submission, dynamic mapping and harmless reordering. It should not
be the sole durable identity unless a compiled graph proves the ordinal stable.

### Current TaskRun dynamic key or task name

This requires no new API, but current stream submission deliberately uses an
unstable dynamic key and random TaskRun/stream UUIDs. Task names are not unique.
This option cannot support exact reconstruction.

### Deployment graph manifest

A predeclared graph could derive every slot and support branches and joins. It
is a stronger long-term model, but requires native graph IR, version migration
and deployment compatibility work beyond the bounded N1.5 candidate.

## Open agreement questions

The questions below record the original 2026-11-02 proposal. On 2026-11-03,
the ETL Architect / HighLoad Principal and implementation owner agreed the
bounded answers in the [design review](../evidence/native-stage5-design-review-2026-11-03.md).
The user requested implementation. The decision is now in progress; neither
the agreement nor an individual implementation slice satisfies this ADR's
acceptance gate.

1. Is an explicit `submission_key` mandatory in the first profile, and which
   SDK call owns it?
2. What canonical parameter representation excludes secrets and unstable
   Python object identity while detecting meaningful drift?
3. Which code-version witness is authoritative for local, packaged and deployed
   flows?
4. What server-observed fact permits execution-generation takeover without a
   clock-only fail-open path?
5. How does the server transition an abandoned `Running` TaskRun into its next
   normal attempt without `force=True` and without fabricating a client event?
6. How are TaskRun state events bound to execution generation so a zombie's late
   `Completed` or `Failed` event cannot overwrite replacement progress?
7. Does a single FlowRun generation fence all streaming submissions, or do
   independent recovery regions need separate generations later?
8. Which completed-manifest and lookup records remain durable after whole-flow
   stream release, and what named horizon expires old recovery requests?
9. What deployment/code changes require a new FlowRun rather than recovery of
   the existing one?

The agreed first sequence is N1.5a immutable identity, N1.5b execution fencing
and lifecycle, then N1.5c reconstruction. Explicit call-site keys and task/flow
versions are mandatory. Parameters use strict finite JSON and durable object
references. Infrastructure replacement consumes ordinary retry policy and
requires confirmed process termination by the existing runner. A live
partition remains unsupported until FlowRun lifecycle writes also carry and
validate execution generation. The original questions and alternatives remain
above for traceability.

## N1.5a implementation boundary

The first slice adds the authenticated `submissions/reserve` and
`submissions/lookup` operations and their paired Python clients. Reservation
creates the manifest, normal Pending TaskRun and reserved output in one native
PostgreSQL transaction. The server chooses the TaskRun and channel IDs.
Canonical task/flow/deployment versions, finite JSON parameters, upstream
identity, protocol, policy, limits, tags and labels bind the call slot.

The profile has one source root and a linear chain. It rejects another root
or a second consumer of an upstream manifest before creating that rejected
submission. Hard limits are 128 manifests, 1 MiB of canonical manifest material
per FlowRun, 64 KiB per manifest and 32 KiB of parameters per call. Payload
release retains a RELEASED identity witness, while execution replay refuses
with `REPLAY_HORIZON_EXPIRED`.

This API establishes identity only. Replaying a reservation whose TaskRun is
already running refuses with `RECOVERY_NOT_READY`; lookup remains diagnostic.
The call-site `submit_stream(submission_key=...)`, generation-bound execution
and automatic whole-process reconstruction belong to the following slices.
Ordinary unkeyed N1.4 source/operator calls keep their existing behavior.

The [N1.5b wire proposal](../evidence/native-stage5b-wire-proposal-2026-11-03.md)
develops the next lifecycle and event-authority boundary. It is design input,
not implementation evidence or an accepted recovery guarantee.

## Stories

- As a developer, I want to give a bounded native source or operator a stable
  submission identity so restarting flow infrastructure finds the same TaskRun
  and stream.
- As a developer, I want changed code, parameters, protocol or upstream wiring
  to fail before duplicate work is admitted.
- As a user, I want a dead worker replacement to continue from the first
  incomplete source position or input while retaining the original TaskRun.
- As a user, I want durable EOF accepted before task success to reconcile after
  a lost response without invoking my source again.
- As a system, I want a replacement generation to fence a partitioned zombie
  before either process can publish more records or lifecycle events.
- As a system, I want normal TaskRun retry semantics and server lifecycle
  authority, without forced state writes or restoration of Python frames.
- As a system, I want unsupported branch, join and dynamic graph profiles to
  refuse before creating a partial recovery manifest.

## Acceptance criteria

- [x] As a developer, I can create or exactly replay an immutable submission
  manifest and receive the same TaskRun and output stream identities.
- [x] As a developer, I can reuse a declared submission key only with the exact
  role, protocol, code, parameter, upstream and limit witness; drift performs no
  write.
- [ ] As a user, I got the same source TaskRun UUID after the flow-launch
  process died, with a fresh generator starting after the durable accepted
  position.
- [ ] As a user, I got the same operator TaskRun UUID after infrastructure loss,
  with only the first input lacking durable terminal completion redelivered.
- [ ] As a user, I got durable EOF and output completion reconciled before
  TaskRun success without re-entering completed user code.
- [ ] As a system, I can claim a strictly newer execution generation and reject
  record, terminal and lifecycle writes from a still-running former process
  before individual stream takeover.
- [ ] As a system, I can advance the retained TaskRun through a normal retry
  attempt without `force=True`, a replacement TaskRun or a fabricated Python
  frame.
- [ ] As a system, I can reject late former-generation `Completed`, `Failed` and
  cancellation observations without using wall-clock order as authority.
- [ ] As a system, I got one TaskRun and one stream per manifest across repeated
  reconstruction attempts, with no TaskRun per record.
- [ ] As a developer, I got named fail-closed refusals for missing manifests,
  expired recovery horizon, code/graph drift and unsupported topology.
- [ ] As a developer, I got real HTTP/PostgreSQL subprocess tests at manifest,
  reservation, append, completion, EOF and TaskRun-state response-loss
  boundaries, including a live zombie writer.
- [ ] As a user, I got ordinary finite-task, accepted N1.2-N1.4 stream and
  client-package regressions on the same source bytes.
- [ ] As a system, I got independent review that distinguishes TaskRun retry,
  whole-worker reconstruction and later branch/join recovery scopes.

## Evidence

N1.5a's [scoped acceptance](../evidence/native-stage5a-acceptance-2026-11-03.json)
and [independent review](../evidence/native-stage5a-principal-2026-11-03.json)
pass on one identical 31-file source manifest. Six profiles execute 377 cases
with zero failures or skips: 167 native HTTP/client/source/process/package
cases, 67 store/API cases, 10 PostgreSQL migration cases, 13 SQLite
migration/capability cases, 36 legacy contract cases and 84 ordinary TaskRun
model/orchestration API cases. The legacy contract deliberately retains its
prototype-backed test adapter; native persistence proof comes from the other
profiles. UI API regression passes 323 cases, CI type analysis reports zero
errors, and public type completeness is 1.0.

The real HTTP controls cover 16 concurrent exact reservations, client death
after creation, API SIGKILL while the manifest INSERT waits inside its
transaction, drift refusals, both hard budgets, oversized parameters and safe
refusal to attach to an already running TaskRun. Direct PostgreSQL counts
independently check all-or-none manifest/TaskRun/channel creation. The
[negative findings](../evidence/native-stage5a-negative-findings-2026-11-03.json)
retain the SQLite reflection assertion defect, the correctly rejected
source-drift trial and the legacy invocation that omitted its required test
DSN. Successful successors remain linked; no rejected evidence was removed.

Only the first two identity criteria are checked. This ADR remains `progress`:
N1.5b/c and the remaining eleven criteria are open. Earned native foundation
readiness remains 40%, with no contribution from this identity slice alone.

At the 2026-11-03 start, no N1.5 acceptance evidence exists. Current source inspection
shows why lookup is not yet possible:

- [`Task.create_local_run`](../../../src/prefect/tasks.py) uses an unstable
  dynamic key for the current stream submission path;
- [`Task._submit_stream`](../../../src/prefect/tasks.py) allocates fresh TaskRun
  and stream UUIDs;
- the [stream engine](../../../src/prefect/_internal/stream_engine.py) allocates
  a fresh owner for each admitted TaskRun attempt but does not own an
  execution-wide generation; and
- [ADR-0035](ADR-0035-native-source-task-runtime.md) proves TaskRun retry from
  retained identities and explicitly defers whole-worker reconstruction.

Those observations support the problem statement only. They do not validate a
manifest schema, takeover rule or lifecycle transition.

## Failure modes

- A random new TaskRun or stream after process loss duplicates a logical
  channel and can replay external reads.
- An automatic call ordinal changes under a branch, refactor or concurrent
  submission and attaches recovery to the wrong output.
- A manifest that omits code, parameters or upstream identity accepts semantic
  drift under an old key.
- Looking up identities before fencing lets a zombie publish between inspection
  and takeover.
- Rotating only a source or consumer owner leaves other submissions and late
  TaskRun events under the former execution authority.
- Clock-only expiry can admit two writers during a pause or network partition.
- Forcing an old `Running` TaskRun into a convenient state bypasses Prefect
  orchestration authority and can lose a legitimate terminal event.
- Reporting TaskRun success before exact EOF or completion strands downstream
  readers on an OPEN stream.
- Restoring a Python generator frame or replaying from position zero makes
  local process memory the progress authority.
- Deleting lookup metadata without a named recovery horizon can let an old key
  alias a different future submission.

## Consequences

A durable submission key becomes part of the user-visible identity contract.
It adds friction to the first bounded profile but makes restarts auditable and
prevents call-order heuristics from becoming hidden authority.

An execution-generation fence adds a server check to native mutation paths and
requires explicit lifecycle design for abandoned `Running` TaskRuns. This is a
larger change than source TaskRun retry, which is why it remains a separate
stage.

The proposed N1.5 contribution is +10%, reaching 50% of the native-foundation
scope only after implementation, all acceptance criteria and independent
review. The [task registry](../todo.md) and [sprint board](../sprint_board.md)
track the three implementation slices. No percentage is earned by starting a
slice or documenting the agreement.

The initial linear-flow restriction leaves branch, join and dynamic graph
recovery open. It does not reduce the later requirement; it gives the identity
and fencing protocol a bounded executable first proof.

## Links

- [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- [ADR-0032: Native task and stream dependency contract](ADR-0032-native-task-stream-contract.md)
- [ADR-0033: Native durable stream storage](ADR-0033-native-durable-stream-storage.md)
- [ADR-0034: Native stream HTTP adapter and runtime admission](ADR-0034-native-stream-http-adapter.md)
- [ADR-0035: Native source task runtime](ADR-0035-native-source-task-runtime.md)
- [Native task submission](../../../src/prefect/tasks.py)
- [Native stream engine](../../../src/prefect/_internal/stream_engine.py)
- [Native server storage](../../../src/prefect/server/models/streams.py)
