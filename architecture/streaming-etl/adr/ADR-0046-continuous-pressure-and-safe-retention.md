# ADR-0046: Continuous backpressure and reference-safe retention

- Status: `planning`
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, storage/runtime maintainers
- Related: [architecture](ADR-0042-flink-informed-native-streaming-architecture.md), [historical retention](ADR-0023-bounded-retention-and-replay-horizons.md), [channels](ADR-0043-physical-graph-and-partitioned-channels.md), [state](ADR-0044-multi-input-turns-and-state-materialization.md), [sources](ADR-0045-continuous-sources-event-time-and-timers.md)

## Problem

The accepted foundation refuses a lifetime capacity limit and releases storage
after whole-flow termination. Raising that limit does not create continuous
bounded streaming. Fan-out, lost acknowledgements, state journals and recovery
also create different reasons to retain an otherwise consumed record.

## Theory

Downstream capacity credits and reference-aware partition retirement can bound
continuous operation without deleting data needed by an independent subscriber
or recovery plan. Flink's credit-based pressure is a useful execution principle;
the retained PostgreSQL log needs additional durable reclamation rules.

## Proposed solution and agreement

- Bound bytes and records for each admitted profile and shared storage budget.
  Reserve the admitted maximum authoritative turn footprint atomically before
  invoking business code. Define oversize-value refusal
  separately from temporary pressure. Large bodies remain object references.
- Propagate full downstream capacity back to upstream publication/read admission
  through durable accounting and fair notifications, not busy polling. Reclaim
  orphaned reservations only through fenced owner/attempt transitions.
- Coordinate record-capacity credits with physical/task concurrency admission
  from [ADR-0043](ADR-0043-physical-graph-and-partitioned-channels.md). Data-plane
  backpressure cannot fix a region whose downstream tasks never receive slots.
  Admit the required concurrently-live set or apply an explicitly supported
  slot-sharing policy; test insufficient tag/global slots as a negative control.
- GC uses the minimum safe frontier of all registered subscriber completions,
  in-flight claims, state/join references, active recovery plans, verified state
  materializations and rescale/backup pins. Track completion holes explicitly.
  Worker disappearance alone cannot remove a subscriber or retention root.
- Retire immutable segments/receipts only under a declared replay horizon and
  an auditable tombstone/manifest protocol. If an old digest is no longer
  retained, return a named `HORIZON_CLOSED` refusal; do not republish an old
  sequence or pretend unlimited exact-retry deduplication remains possible.
- Object GC follows all durable log/state/reference roots. A watermark or
  processing-time TTL is not sole authority to delete recoverable channel data.
- Expose occupied/reserved/reclaimable bytes, slowest subscriber lag, pinned
  segments, age, blocked publication duration and GC progress.

### Full-turn reservations and progress headroom

The admitted maximum covers producer/output and edge-route envelopes, state
batches/postimages, unmatched join state, inbox staging, timer mutations,
completion receipts, cursor/index updates and reservation/control metadata.
Declare per-class byte/count bounds and charge actual storage amplification
under the selected profile. Profiles unable to bound the turn before invocation
are refused by this first contract; a weaker effect/retry policy needs a
separate decision. Oversize user state/output is an explicit bounded refusal.

A durable reservation binds logical turn ID, quota classes, amounts and current
owner fence. Acquire the entire vector atomically or wait without holding a
partial reservation or DB locks. The terminal transaction converts held budget
to retained usage and refunds only unused amounts, exactly once. Exact accepted
retry resolves its receipt without a second reservation. Refusal/abort refunds
the hold once; lost-owner reclamation fences the old reservation capability
before release. Later GC releases consumed retained usage once through the
segment/reference ledger, never by refunding the original reservation again.

Define one lock order and durable fair wait order for shared budgets; wakeups
recheck atomic availability. Keep bounded dedicated headroom for barriers,
drain/cancel, recovery and required join-input/control progress. Admitted joins
need finite unmatched/window/skew bounds and a wait-for analysis. Refuse an
unsupported resource cycle, or fail with an explicit unmatched/pressure-limit
outcome rather than wait forever. No arbitrary unbounded-state join is admitted
on the assumption that its matching input will eventually free space.

## Discussion and tradeoff

A healthy branch advances independently while its slow sibling's retained
suffix fits the admitted budget. With bounded shared storage, an indefinitely
stalled branch eventually backpressures the shared producer and therefore the
healthy branch. Unbounded branch independence and bounded storage cannot both
be promised. Explicit branch removal or a weaker retention contract is a
separate operator action, never an inference from timeout.

Local queue capacity can also create a cycle of blocked operators during joins
or materialization. Admission/credit ordering needs a wait-for analysis and
fault tests; a hard `CAPACITY` error is not evidence that pressure works.

## Stories

- As an operator, I want continuous flows to use bounded storage and reveal the consumer pinning it.
- As a user, I want a slow branch to retain its accepted restart position.
- As a system, I want reclaimed data and expired retry horizons to be explicit.

## Acceptance criteria

- As a system, I can process several multiples of the admitted retained-byte window with real native PostgreSQL workers while retained storage reaches a measured plateau.
- As a user, I can pause one fan-out branch, advance the other within budget, observe upstream pressure at budget exhaustion, then resume without a gap.
- As a system, I got no quota overshoot, leaked reservation or deletion under concurrent append, completion, snapshot, owner loss and GC.
- As a developer, I can reproduce a lost-response retry before receipt retirement and an explicit horizon refusal after retirement.
- As a system, I can prove each deleted segment/object is unreferenced by all declared durable roots, including join state and in-progress recovery.
- As an operator, I can distinguish idle, busy, backpressured and failed execution without treating temporary fullness as terminal task failure.
- As a system, I can avoid permanent starvation when a streaming region needs more simultaneous task slots than a configured tag/global limit permits.
- As a system, I can exhaust data/state/route/control budgets and lose owners around reservation/commit without partial holds, double refunds, leaked credit or business invocation for a turn whose admitted maximum cannot fit.

## Evidence and current state

Planning only. Historical prototype retirement/pressure evidence is input to
the design; it does not close native continuous-retention criteria. See the
[comparison](../research/flink-2.3-architecture.md) and
[epoch scope](../epochs/epoch-02-native-streaming-completion.md).

## Consequences and failure modes

Retention roots increase metadata and lock complexity. Failed materialization,
expired source retention or a permanently pinned subscriber must have visible
failure/pressure outcomes. Performance acceptance must include PostgreSQL WAL,
vacuum, index size and hot partition contention, not just logical record counts.
