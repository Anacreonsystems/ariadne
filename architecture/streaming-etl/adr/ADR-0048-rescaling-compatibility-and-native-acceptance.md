# ADR-0048: Rescaling, compatibility and measured native acceptance

- Status: `planning`
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, release/runtime maintainers
- Related: [architecture](ADR-0042-flink-informed-native-streaming-architecture.md), [graph](ADR-0043-physical-graph-and-partitioned-channels.md), [state](ADR-0044-multi-input-turns-and-state-materialization.md), [pressure](ADR-0046-continuous-pressure-and-safe-retention.md), [recovery](ADR-0047-selective-recovery-and-cross-host-authority.md), [epoch](../epochs/epoch-02-native-streaming-completion.md)

## Problem

Increasing parallelism changes state ownership, input routing and lifecycle
aggregation. A snapshot without compatible code, schema, positions and object
roots cannot safely become a new execution. Passing a small standalone model
or the earlier 72-hour prototype does not prove this native path or its load
capacity.

## Theory

Stable key groups, complete state-transfer cuts and versioned compatibility
manifests can support planned rescale without rolling back accepted turns.
Source-bound real-system acceptance can keep scope and progress auditable.

## Proposed solution and agreement

- Persist key encoding, hash/routing version and maximum key-group count
  independently of current parallelism. A new assignment epoch redistributes
  whole key groups at a complete verified transfer cut.
- Freeze/synchronize input ownership, record per-input cursor vectors, accepted
  apply ordinals, state roots, timers and retained data references. Verify the
  target materialization before publishing the new assignment atomically.
  Reject mixed old/new assignment writes. Failure before activation retains a
  recoverable old assignment; failure after activation uses the new one.
- Manifest compatibility includes operator UID, graph revision, package digest,
  runtime profile, state schema/serializer, routing version and source-position
  codec. Define supported migrations explicitly; refuse incompatible changes
  before business execution. Do not serialize arbitrary Python frames.
- Define TaskRun identity at rescale according to the explicit ADR-0043 choice.
  No number of records creates new TaskRuns. Assignment changes may create
  bounded metadata only under an auditable retirement policy.
- Exercise actual SDK, HTTP API, PostgreSQL, deployment workers and operating
  system processes throughout implementation. Standalone experiments can
  refute theories but cannot close native product criteria.
- Keep separate earned scopes and source-bound reports: foundation, new native
  profile, fault recovery, pressure/load and two-host placement. A feature is
  accepted only after all its declared criteria pass with reviewed evidence.

### Complete transfer manifest

Use ADR-0044's immutable participant cut and ADR-0047's all-or-none activation.
The manifest includes or explicitly resolves every admitted authority component:

- Source enumerator/discovery and split-assignment epochs, positions, in-flight
  read permits and append intents, including committed appends with lost replies.
- Port/partition bindings, persisted exchange routes, arrival and completion
  frontiers/holes, inherited accepted receipts and stable original turn IDs.
- State roots/batches, per-owner apply ordinals, unmatched join values, timers,
  watermarks, idleness/RESUME status and late-data policy.
- Occupied and held capacity by quota class, reservation/refund identities,
  control headroom, GC/recovery/backup/object pins and their current owners.
- Scoped writer/event/timer capabilities, worker placements, task retry budgets
  and lease ownership needed by the target assignment.

For each component declare transfer, resolution before the cut, or unsupported
admission. Refuse rescale if any required state lacks a portable representation.
Move pin/reservation/capability ownership atomically with assignment activation;
the old assignment may neither mutate state nor refund/release transferred
resources. Preserve original receipt lookup across new TaskRun identities so
rescale never makes an accepted logical turn appear new. Router/schema changes
require declared compatibility/migration, not a silent change of parallelism.

## Proposed native acceptance matrix

| Area | Required observations |
| --- | --- |
| Fixed parallelism | Separate source/operator processes, stable identities, per-key order, metadata independent of document count. |
| Branched DAG / join | Independent cursors; correlated match oracle; unmatched/zero-output and end conditions; no cross-branch acknowledgement. |
| Turn faults | Before terminal transaction, during it, after commit/lost response; state/output/cursor atomicity and no accepted-body replay. |
| Authority | Old owner, coordinator and assignment rejection; concurrent cancel; one valid native lifecycle owner. |
| Continuous pressure | Several retained-window turnovers; paused branch; bounded rows/bytes/WAL/index growth; fair resume and no leaked credits. |
| State/rescale | Snapshot lag, missing/corrupt batch refusal, noncommutative apply order, timer identity, complete cut, interrupted transfer. |
| Two-host recovery | Actual separate hosts/nodes, verified-stop capability, portable package and objects, no timeout-as-death substitution. |
| Compatibility | Existing finite flows/client wheel, bounded profile, old supported contracts and explicit unsupported admission. |
| Load | Declared record sizes/rates/key skew, hardware/DB/storage, throughput and latency percentiles, recovery/catch-up time, pressure and resource bounds. |

No numerical service objective is invented here. Establish baseline, budget and
repeatable workload before setting an enforceable target. Record noisy/failed
observations, not just the best run. A new 72-hour campaign is not required for
this architecture cycle; any later long run is a separately declared gate.

## Stories

- As an operator, I want to change physical parallelism without reprocessing accepted documents.
- As a developer, I want incompatible state/code/routing changes to fail clearly.
- As a user, I want completion claims to identify tested behavior and practical limits.

## Acceptance criteria

- As a system, I can rescale a stateful real native graph from two to three subtasks with fixed key groups, correct order/state/output and no accepted-body replay.
- As a system, I can recover faults on both sides of assignment activation while rejecting stale writers and preserving a complete materialization chain.
- As a developer, I got positive compatible upgrade and negative incompatible package/schema/router/source-codec tests before task invocation.
- As an operator, I can reproduce every applicable matrix gate from frozen source, commands, environment and raw observations.
- As a user, I can inspect measured load and recovery limits without confusing standalone prototype success with native integration completion.
- As a developer, I got explicit ADR acceptance only after the native implementation and every corresponding criterion are independently reviewed.
- As a system, I can rescale with an idle input, watermark skew, unmatched join value, completion hole, lost append response, timer and held capacity/retention pins while preserving each component and refusing old-owner mutation or release.

## Evidence and current state

Planning only. No rescale/native continuous-load/two-host completion is earned.
The historical [epoch 1 final gate](../evidence/native-epoch1-final-acceptance-2026-11-04.json)
remains its own scoped proof. The
[new architecture research](../research/flink-2.3-architecture.md) and ADR-0044
experiment inform future native work but do not replace this acceptance matrix.

## Consequences and failure modes

Rescale adds a state-transfer lifecycle and storage pins; partial transfer must
not become active. Schema migration failures, routing mismatch and expired
input horizons are explicit refusal outcomes. If measured PostgreSQL limits
miss the target workload, record the result and revisit the affected ADR before
claiming industrial streaming capacity.

## Work approval and stable criterion IDs

Approved for work on 2026-11-04 as part of the ADR-0042–0048 set at revision
`fae53c90aa3b44dbc45032833b4d193001e5c8bd`; see the
[approval record](../evidence/flink-architecture-2026-11-04/epoch2-work-approval-2026-11-04.json).
It is the final convergence decision and depends on completed native gates for
ADR-0043 through ADR-0047. It does not make umbrella ADR-0042 a prerequisite
cycle.

| Stable ID | Existing acceptance criterion |
| --- | --- |
| A48-01 | Rescale a real stateful graph two → three without accepted replay. |
| A48-02 | Recover faults on both sides of atomic assignment activation. |
| A48-03 | Compatible upgrade and incompatible package/schema/router/codec refusal. |
| A48-04 | Reproduce every matrix gate from frozen source and raw evidence. |
| A48-05 | Publish measured load/recovery limits without prototype confusion. |
| A48-06 | Accept an ADR only after all native criteria are independently reviewed. |
| A48-07 | Preserve full source/control/state/capacity/retention state through rescale. |
