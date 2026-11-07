# ADR-0044: Multi-input turns and state materialization without business replay

- Status: `progress`
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, state/runtime maintainers
- Related: [architecture](ADR-0042-flink-informed-native-streaming-architecture.md), [managed state](ADR-0013-managed-state-recovery-classes.md), [historical join](ADR-0022-postgresql-routed-join-and-regional-recovery.md), [channels](ADR-0043-physical-graph-and-partitioned-channels.md), [retention](ADR-0046-continuous-pressure-and-safe-retention.md)

## Problem

The current native transform has one input, one in-flight coordinate and either
one `YIELD` output or an internal state-only `RETURN` terminal outcome. A join
needs several input cursors, durable unmatched
state and a defined input scheduling order. Consuming both ports is not by
itself a correlated join. The historical fan-in/long-run evidence does not prove
an implemented native business-key join.

Copying periodic state snapshots and replaying Python from the last snapshot
would repeat already accepted business work. That violates the required yield
contract even when the client makes external effects idempotent.

## Theory

Every accepted turn can journal its authoritative state mutations together
with its output, receipt and input progress. Periodic snapshots then accelerate
materialization of that state without invoking the task function again.

## Agreed state authority

Two representations preserve the same per-turn durability boundary:

1. `INLINE_TURN_STATE`: persist the bounded state postimage/mutations in the
   terminal transaction, as the accepted foundation already does.
2. `JOURNALED_TURN_STATE_WITH_SNAPSHOTS`: persist a versioned platform mutation
   batch in that same transaction; maintain a materialized cache and optionally
   publish complete, verified snapshots for faster reconstruction.

The batch describes state operations/postimages, not instructions to call
`f(input)` again. Its immutable logical identity binds FlowRun/graph revision,
original assignment, operator/state-owner identity, input port/partition/sequence,
apply ordinal, before/after state versions, mutation digest, terminal outcome
and output identity. Timer registration/removal also belongs in the batch.

The authoring TaskRun, attempt and physical owner epoch are audited **fencing
evidence**, checked when committing; they are not part of exact accepted-outcome
identity. A replacement owner discovers the same prior batch/receipt after a
lost response. Rescale transfers accepted identities and their lookup mappings;
it must not rewrite them with the new TaskRun or assignment's author identity.

Recovery from snapshot 100 with accepted frontier 200 is:

```text
load verified snapshot 100
apply verified state batches 101 … 200 in accepted apply_ordinal order
assert reconstructed state root and cursor vector match the accepted frontier
resume the next incomplete business turn
```

This internal fold must not invoke Python task bodies, repeat external effects,
append outputs, or advance already accepted cursors. Missing/corrupt batch 151
means activation fails closed or selects another complete compatible chain.
It does not authorize cursor rollback and reprocessing 151–200.

Opaque closure/process/library state with no complete durable mutation
representation is unsupported by this contract. A different user-code replay
contract would need a separate explicit decision.

## Proposed multi-input and join protocol

- A mailbox admits one tagged input turn at a time per state owner. The terminal
  transaction atomically binds the selected port's cursor, shared state batch,
  output outcome, receipt and global apply ordinal. Other ports retain their
  previous accepted coordinates. Persisted ordering matters for noncommutative
  updates; grouping mutations by port during restore is incorrect.
- A separate inbox staging position may record durable arrival. It must never
  be presented as business completion. Completion holes cannot be collapsed to
  a maximum observed sequence for retention or recovery.
- A first correlated join should define one-to-one matches by stable business
  key/lineage, duplicate policy, unmatched-input lifetime, late data and end
  conditions. Union, correlated join, interval/window join and barrier alignment
  remain distinct operations.
- An unmatched join input may need zero output. Introduce an explicitly tagged
  `NO_OUTPUT` terminal outcome in a new cardinality-0..1 profile; exact SDK
  spelling remains to be designed. In that **new** language profile, yielded
  JSON `None` and ordinary Python return values map to `OUTPUT(value)`, including
  `OUTPUT(null)`; absence of output requires the explicit tag. The accepted
  epoch-1 internal `RETURN` wire is instead state-only: its optional dictionary
  is managed state and it has no output envelope. Preserve that old meaning.
  Version admission and receipt decoding explicitly; do not reinterpret or
  reuse old `RETURN` rows as the proposed language protocol. Flat-map
  cardinality is separate work.
- Barrier alignment may establish a consistent materialization/rescale cut
  across input partitions. A barrier cannot itself acknowledge a business input
  or publish an incomplete snapshot. Unaligned snapshots are deferred until
  measured pressure and an explicit in-flight-data protocol justify them.

### Ordered barriers and complete cuts

A coordinator-authorized barrier has a stable cut ID, coordinator/assignment
epochs, cut purpose and immutable participant/input manifest. DATA and BARRIER
occupy the same fenced ordered partition log; a barrier cannot overtake a
pre-cut append intent. On each required input, consume and accept all preceding
DATA before recording its barrier position. Align inputs, record cursor vectors,
apply high-water marks and content-verified state/timer roots, and propagate
output barriers only after all pre-cut output routes are durable.

Publish one global `COMPLETE` marker only after every immutable participant
acknowledges its exact manifest/root under the current coordinator fence. The
marker atomically binds the whole participant vector and compatibility digest.
Only a compatible complete cut may authorize published snapshot selection,
state-journal retirement or assignment transfer. `COMPLETE` is global to the
declared cut scope: a local state-materialization cut may contain one state
owner, while graph rescale includes every affected source/operator/edge. A
partial graph cut cannot masquerade as a complete local cut after the fact.
Barriers do not advance business completion. Incomplete preparations retain
their pins or are explicitly aborted; they never become GC authority.

## Discussion and rejected theory

The initial suggestion of a separate checkpoint-managed state class was
rejected during [Principal discussion](../evidence/flink-architecture-2026-11-04/principal-discussion.json).
Keeping cursor 200 while loading state 100 and immediately processing 201
produces wrong results. Rewinding that cursor to 100 instead repeats accepted
business calls. The journaled-state representation resolves both failures.

The [standalone probe](../prototypes/turn_state_materialization_probe.py) is a
small falsifiable model, not a second native engine. Its
[test driver](../prototypes/test_turn_state_materialization_probe.py) checks
state reconstruction, missing/corrupt batches, apply order, real process cuts
and lost acknowledgements. Its evidence is linked below after execution.

## Stories

- As a user, I want a replacement to continue after my last durably accepted yield.
- As a developer, I want stateful multi-input tasks to recover without repeating completed Python calls.
- As a system, I want corrupt or incomplete state reconstruction to fail before business execution.

## Acceptance criteria

- As a system, I got state 200 from snapshot 100 plus batches 101–200 while business-call/effect counters, output identities and accepted cursor remain unchanged; next business input is 201.
- As a system, I can refuse missing, corrupt or incompatible batch 151 without rewinding progress or invoking user code.
- As a developer, I got the persisted noncommutative multi-port order after restore; an intentionally per-port-grouped restore fails the oracle.
- As a user, I got input 200 again after a process dies before terminal acceptance, and input 201 after durable acceptance followed by a lost response.
- As a system, I can execute a true correlated join with explicit zero-output completion, durable unmatched state and concurrent fault recovery through real native SDK/HTTP/PostgreSQL workers.
- As a system, I can distinguish staged arrival, accepted business progress, complete snapshot cuts and event-time watermarks in the API and observability.
- As a system, I can resolve an accepted batch/receipt after owner replacement without a second output, state mutation, business call or cursor movement while still refusing stale-owner writes.
- As a developer, I got distinct native round trips for OUTPUT(null), OUTPUT(value) and explicit NO_OUTPUT while old epoch-1 RETURN receipts retain their state-only meaning.
- As a system, I can interrupt every participant/barrier/snapshot/COMPLETE transition and refuse partial cuts for restore, GC and rescale.

## Evidence and current state

The [source-bound v3 experiment](../evidence/flink-architecture-2026-11-04/turn-state-materialization-probe-v3.json)
passes 9 test methods / 10 scenarios. The
[independent repeat](../evidence/flink-architecture-2026-11-04/independent-probe.json)
also passes all 9 methods against unchanged source. They exercise the first
four criteria at standalone model scope, not native scope:

| Experiment | Observed result |
| --- | --- |
| Snapshot 100, accepted 200, old batches 1–100 removed | Fold exactly 101–200; state/output/cursor unchanged; external audit stays 200; next input 201. |
| Process exits before accepting 200 | Durable cursor 199; fresh process really invokes 200; external effect 200 can repeat. |
| Process exits after accepting 200, response lost | Durable cursor 200; fresh process invokes only 201; accepted output prefix stays unchanged. |
| Missing/corrupt 151 | Fail closed without cursor rewind or business calls. |
| Two-port noncommutative order | Accepted ordinal order yields 220; incorrect per-port grouping yields 400 and is refused. |

The [v1 capture error](../evidence/flink-architecture-2026-11-04/turn-state-materialization-v1-capture-error.json)
and [v2 predecessor](../evidence/flink-architecture-2026-11-04/turn-state-materialization-probe-v2.json)
remain historical records. The capture error is not concealed as a protocol
failure or a successful evidence capture.

The [evidence clarification](../evidence/flink-architecture-2026-11-04/evidence-clarifications.json)
corrects an early discussion/v3 limit sentence that called current native
`RETURN` value-bearing. Native source review shows state-only `RETURN`; the
probe only tests yield-style output turns. Its test results and frozen reports
are retained unchanged, and no native return semantics are proved by them.

Native multi-input/join APIs, tagged zero-output turns and journaled native
state are **not implemented or accepted**. This ADR remains `progress` even
when the standalone experiment passes. See the
[research comparison](../research/flink-2.3-architecture.md) and
[epoch boundary](../epochs/epoch-02-native-streaming-completion.md).

## Consequences and failure modes

Journal retention is part of correctness. State batch GC needs a complete
verified compatible materialization plus all reader/recovery/rescale pins;
snapshot failure eventually causes pressure, not silent journal deletion.
Batch size, materialization cost and state hot keys require measured limits.
Client idempotency still protects effects attempted before an unaccepted yield.

## Work approval and stable criterion IDs

Approved for work on 2026-11-04 as part of the ADR-0042–0048 set at revision
`fae53c90aa3b44dbc45032833b4d193001e5c8bd`; see the
[approval record](../evidence/flink-architecture-2026-11-04/epoch2-work-approval-2026-11-04.json).
Native multi-input/join completion depends on ADR-0043 physical ports, edges and
subtask fencing. The standalone materialization probe remains non-native input.

| Stable ID | Existing acceptance criterion |
| --- | --- |
| A44-01 | Rebuild state 200 from snapshot 100 plus batches without replay. |
| A44-02 | Refuse missing/corrupt/incompatible batch 151 without rewind. |
| A44-03 | Preserve noncommutative multi-port apply order. |
| A44-04 | Recover input 200 or 201 around terminal acceptance. |
| A44-05 | Native correlated join with NO_OUTPUT and durable unmatched state. |
| A44-06 | Distinguish arrival, business progress, complete cuts and watermarks. |
| A44-07 | Resolve accepted batch after owner replacement and fence stale writes. |
| A44-08 | Round-trip OUTPUT(null), OUTPUT(value), NO_OUTPUT and old RETURN. |
| A44-09 | Refuse every partial barrier/snapshot/COMPLETE cut. |


## Planning discussion: native state journal handoff — 2026-11-06

The [canonical planning package](../evidence/native-epoch2-state-journal-planning-2026-11-06/v1/index.json)
retains the original draft, [reviewed v2 implementation proposal](../evidence/native-epoch2-state-journal-planning-2026-11-06/v1/original/implementation-spec-v2.md),
[Principal read-only review](../evidence/native-epoch2-state-journal-planning-2026-11-06/v1/original/principal-review/state-spec-v2-planning-readonly-review-v1.json)
and [internal task refinement](../evidence/native-epoch2-state-journal-planning-2026-11-06/v1/original/proposed-task-refinement-v1.json)
on the exact `d906ceca414baeac825ce472e0b78cd1dcae3363` source basis.
The review closes preparation only: implementation is not ready or authorized
by this handoff. The [NI2-014 dependency gate](../todo.md) still requires its
source-bound runtime/hosted exit before the state implementation handoff.
Concurrent 5000-record work remains separate and supplies no state acceptance.

As a user, I still want continuation after my last accepted yield. As a
developer, I still want reconstruction without repeating accepted Python
business calls. As a system, I still need corrupt or incomplete chains refused
before business execution, while current-owner write fences remain mandatory.
The proposed slices map to the existing criteria, without adding a contract:

| Existing criterion | Proposed implementation refinement | Current evidence boundary |
| --- | --- | --- |
| A44-01 — Rebuild state200 from snapshot100 plus batches without replay | NI2-020b follows a genuine NI2-024a local COMPLETE cut and folds101–200 before business201. | OPEN: no legal native snapshot100/cut or native oracle has been implemented or executed. |
| A44-02 — Refuse missing/corrupt/incompatible151 without rewind | NI2-020a develops journal/resolve/genesis reconstruction; NI2-020b supplies the real snapshot-chain refusal oracle. | Planning only; proposed checks are not native results. |
| A44-07 — Resolve accepted state after replacement and fence stale writes | NI2-020a/b separate accepted logical identity from current physical authorization and restore readiness. | Planning only; no owner-replacement state implementation or independent runtime verification. |

The internal order is NI2-020a journal/resolve/genesis fold, then NI2-024a's
minimum one-owner ordered DATA/BARRIER cut, then NI2-020b's native snapshot100
plus batches101–200 oracle, followed by NI2-021 and the general NI2-024
remainder. This avoids requiring completed NI2-020b before its local-cut
prerequisite. A cached postimage, fixture seed or journal-only marker cannot
stand in for a legal COMPLETE cut. The original NI2-020 parent is not done;
full mailbox, multi-input and graph-wide cut work remain separate.

V2 addresses the review's Pending restore TaskRun-lock and SOURCE identity
findings. Exact profile/API names, composite FKs, the complete lock/attempt
trace, encoded/temporary quotas and the local-cut intent/pin protocol still
need implementation refinement. The [Principal context and clarification](../evidence/native-epoch2-state-journal-planning-2026-11-06/v1/original/principal-handoff/handoff.md)
and immutable source bindings remain preserved. This dated discussion changes
no approved body or acceptance criterion, implements no state capability and
earns no credit. ADR-0044 stays `progress`; epoch-2 earned readiness stays15%.


## Concrete state-contract planning agreement — 2026-11-06

The [closed exact-contract package](../evidence/native-epoch2-state-journal-planning-2026-11-06/contract-v1/index.json)
retains the [revised contract](../evidence/native-epoch2-state-journal-planning-2026-11-06/contract-v1/original/contract-v2.md)
and [Principal planning agreement](../evidence/native-epoch2-state-journal-planning-2026-11-06/contract-v1/original/principal-review/contract-finalization-principal-planning-closure-v1.json).
The original REQUEST_CHANGES review and all v1/v2 payloads remain immutable.
Source bindings cover27/40 immutable d906 paths plus four C26 Git bindings;
the unrelated current logging-test change is not the source authority.

The agreed planning detail includes the explicit profile-v2/API-v3 five-feature
negotiation tuple, eight proposed tables with25 foreign-key/four unique
constraints and the eleven existing profile CHECK inventory. A37-field charged
packet has a static2156-byte metadata bound below4096, with reservation S+4096.
These are proposed wire/schema choices and static arithmetic, not implemented
DDL/DTO behavior or passing runtime tests. The retained falsifiers must exercise
the actual implementations before any acceptance claim.

The mandatory [C26 planning addendum](../evidence/native-epoch2-state-journal-planning-2026-11-06/contract-v1/original/c26-generic-flow-deletion-addendum-v1.md)
requires the new-v2 generic deletion path to validate already-retired lifecycle
state under the ordered Flow/Authority/Graph prefix before any slot/log cleanup
or deletion. It supplies a future guard/falsifier, not an observed current bug,
new old-profile policy or permission to repair retirement during deletion.

NI2-014 remains the preimplementation gate. NI2-020a is still genesis-only;
A44-01 stays OPEN until a genuine aligned024a COMPLETE cut and020b native
snapshot100 plus101–200 fold oracle exist. These ordinary implementation details
stay inside the already approved ADR44 scope, with no new user reapproval
requirement and no implementation GO from this planning record. The entire
prior ADR body/criteria/history remain unchanged; no state code/runtime or
acceptance credit is produced. Earned epoch-2 readiness remains15%.
