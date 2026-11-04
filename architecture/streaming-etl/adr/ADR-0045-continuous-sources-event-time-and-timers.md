# ADR-0045: Continuous sources, event time and durable timers

- Status: `planning`
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, source/runtime maintainers
- Related: [architecture](ADR-0042-flink-informed-native-streaming-architecture.md), [finite source](ADR-0035-native-source-task-runtime.md), [state](ADR-0044-multi-input-turns-and-state-materialization.md), [pressure](ADR-0046-continuous-pressure-and-safe-retention.md)

## Problem

Native sources are finite and flow completion depends on EOF and a finite tail.
A continuous source additionally needs durable split discovery/assignment,
portable replay positions, idleness, drain semantics and bounded downstream
state. An idle source is not a finished source.

## Theory

Flink's enumerator/reader separation and explicit event-time progress can be
adapted to native Task/subtask lifecycles. Durably recording source positions
and timer outcomes preserves accepted-yield recovery across source replacement.

## Proposed solution and agreement

- A native source coordinator tracks durable versioned split identities and
  assignment epochs; source task processes read assigned splits. Its authority
  belongs to the Prefect server/flow execution, not a second scheduler.
- Each connector declares boundedness, position encoding, rewind semantics,
  source identity, external retention and acknowledgement behavior. Source
  publication atomically binds durable record identity and next position.
  Acknowledge externally only after durable platform admission. If the source
  cannot replay, the platform must first durably land the input before it can
  advertise restart safety. Connector limitations are admission constraints.
- Source progress is a vector over splits, not one global offset. Discovery and
  reassignment must not omit or duplicate a split behind a restarted reader.
- A continuous FlowRun remains running until explicit drain/cancel/failure.
  Drain freezes discovery/admission at a declared frontier, finishes assigned
  input, publishes EOF per partition and then permits finite summaries.
- Add explicit per-input event-time watermarks. A multi-input operator advances
  by the minimum of active inputs. Idleness is an explicit policy; a returning
  idle source needs a late-data policy. Watermarks are neither accepted-record
  cursors nor materialization barriers.
- Watermark alignment may pause fast sources/splits to bound skew and join
  state. Event-time cleanup uses the declared lateness/match policy. A
  processing-time TTL alone is not correctness authority for window cleanup.
- Timer registration/cancellation is journaled state. Each firing has a stable
  durable turn identity; accepting its output/state prevents re-firing the
  completed callback on cache reconstruction. Clock and overdue processing-time
  timer behavior require a versioned policy.

### Ordered source controls and drain frontier

Persist monotonic enumerator, split-assignment and reader-owner epochs. DATA,
WATERMARK, IDLE, RESUME, EOF and DRAIN controls have stable identities in the
same fenced partition order. Controls carry their source position and preceding
DATA frontier; retries must match their payload/position digest. Watermarks are
monotonic and cannot pass unpublished earlier DATA. RESUME after IDLE applies
the declared late-input policy before it can affect joins or timers.

Before an external read, persist a read permit/append intent bound to split,
starting position and owner. Drain atomically freezes discovery and assignment
at one enumerator epoch and freezes new read permits. Its immutable manifest
includes all discovered/assigned splits, in-flight read permits, known positions
and append intents. Resolve or explicitly abort those intents under the fenced
connector rewind contract before publishing each EOF. An unresolved read cannot
be silently omitted from the declared frontier. Reconcile lost responses by
split, source position, record identity and payload digest before republishing.
Drain completes only after all manifest positions and downstream accepted work
are accounted for; temporary source silence never supplies that proof.

## Discussion and limits

The [Flink sources/state comparison](../research/flink-2.3-architecture.md)
motivates these separations. Exact public source/timer API spelling and the
initial connector set remain to be designed. Defining event time does not mean
every operator needs windows; ordinary document ETL can retain simple ordered
per-input processing.

## Stories

- As a user, I want documents to continue arriving while downstream processes them.
- As an operator, I want to stop intake and drain accepted work without treating temporary silence as EOF.
- As a developer, I want timer and late-data behavior to be reproducible after replacement.

## Acceptance criteria

- As a system, I can recover multiple source splits through real native readers without an accepted-record gap or duplicate platform publication.
- As a user, I can leave a source idle, resume it, then explicitly drain all partitions and run the finite tail once.
- As a system, I can reject a connector whose rewind/retention contract cannot satisfy the declared recovery horizon.
- As a developer, I got tested watermark minimum, idleness/reactivation, skew alignment and explicit late-input outcomes in a real keyed join.
- As a system, I can reconstruct timer state without repeating an accepted callback and can recover a firing interrupted before terminal acceptance.
- As an operator, I can observe split assignments, accepted positions, event-time lag, idle status, timers and drain frontiers separately.
- As a system, I can interrupt discovery, assignment, read, append/response, watermark, IDLE/RESUME and drain freeze without a control overtaking DATA or a split disappearing from the frozen frontier.

## Evidence and current state

Design discussion and official-source research only. Existing finite-source
acceptance remains valid within its original scope. No continuous connector,
watermark or timer native acceptance is claimed by this document.

## Consequences and failure modes

Source retention can expire while a consumer is down; recovery must then refuse
the unsatisfied horizon explicitly. Fast inputs can accumulate unmatched state
without watermark alignment or pressure. Timer identity must include the state
owner/assignment and survive replacement without letting old epochs fire.

## Work approval and stable criterion IDs

Approved for work on 2026-11-04 as part of the ADR-0042–0048 set at revision
`fae53c90aa3b44dbc45032833b4d193001e5c8bd`; see the
[approval record](../evidence/flink-architecture-2026-11-04/epoch2-work-approval-2026-11-04.json).
It depends on ADR-0043 physical splits/partitions and ADR-0044 journaled state,
mailbox order and timer identity.

| Stable ID | Existing acceptance criterion |
| --- | --- |
| A45-01 | Recover multiple native source splits without publication gap/duplicate. |
| A45-02 | Idle, resume, drain every partition and run the tail once. |
| A45-03 | Refuse a connector that cannot meet its recovery horizon. |
| A45-04 | Watermark minimum, idleness, alignment and late outcomes in keyed join. |
| A45-05 | Reconstruct timers without repeating accepted callback. |
| A45-06 | Observe split, position, event-time, idle, timer and drain state separately. |
| A45-07 | Fault every source-control boundary without overtaking DATA or losing a split. |
