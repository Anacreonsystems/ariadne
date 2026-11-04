# Native integration epoch 2: complete the streaming execution model

- Status: `progress` — architecture discussion and one standalone experiment
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, native runtime maintainers
- Governing decision: [ADR-0042](../adr/ADR-0042-flink-informed-native-streaming-architecture.md)
- Previous epoch: [accepted bounded foundation](epoch-01-native-prefect.md)

## Problem and objective

The original streaming ETL objective remains unfinished. The previous epoch's
100% is evidence for its explicitly selected finite linear same-host foundation.
It does not include native branches, joins, partitioned execution, continuous
input or cross-host replacement. Preserve that code, evidence and ADR history;
extend it rather than restart from zero.

Complete a native graph execution model in which ordinary Prefect Tasks process
documents incrementally, independently owned physical subtasks recover from
their accepted input coordinates, and continuous operation has bounded storage
and visible pressure. Use Flink as an architectural reference while retaining
the agreed durable-yield protocol and Prefect lifecycle authority.

## Non-negotiable contract

- A task processes many records; no TaskRun per document.
- If input 200 has no durably accepted terminal outcome, replacement receives
  200. If its `yield`/`return` was durably accepted, replacement receives 201.
- Output reference, authoritative managed state, receipt and cursor advance
  atomically. A Python yield that the platform never accepted is incomplete.
- Snapshot lag never rolls back accepted business work. State reconstruction
  may fold journal batches internally without invoking completed user code.
- Client code owns idempotency of external effects; retries before terminal
  acceptance can repeat them. Large objects are published through references.
- Streaming remains part of Prefect's core Task/Flow model. Execution placement
  and scheduling extend existing authorities rather than add a second product.

## Capability and evidence matrix

| Capability | Native today | Direction / required proof |
| --- | --- | --- |
| Finite linear per-record streaming | Accepted within `bounded-linear-v1` | Preserve [epoch 1 evidence](../evidence/native-epoch1-final-acceptance-2026-11-04.json). |
| Separate physical operator processes / partitions | Not implemented | [ADR-0043](../adr/ADR-0043-physical-graph-and-partitioned-channels.md): stable logical operator, TaskRun per physical subtask, independent owners and resource admission. |
| Fan-out, named inputs, correlated join | Historical prototype evidence only | [ADR-0043](../adr/ADR-0043-physical-graph-and-partitioned-channels.md) / [ADR-0044](../adr/ADR-0044-multi-input-turns-and-state-materialization.md): real native DAG and business-key match oracle. |
| Journaled state with derived snapshots | Narrow standalone experiment in this cycle | [ADR-0044](../adr/ADR-0044-multi-input-turns-and-state-materialization.md): no accepted-body replay; then native integration. |
| Continuous split sources / event time / timers | Not implemented | [ADR-0045](../adr/ADR-0045-continuous-sources-event-time-and-timers.md). |
| Continuous bounded retention and credits | Historical prototype evidence only | [ADR-0046](../adr/ADR-0046-continuous-pressure-and-safe-retention.md): real retained-window turnover and fault tests. |
| Same-host whole-process replacement | Accepted for selected local Linux profile | Preserve [ADR-0038](../adr/ADR-0038-native-deployment-worker-lifecycle.md). |
| Selective / cross-host replacement | Not implemented | [ADR-0047](../adr/ADR-0047-selective-recovery-and-cross-host-authority.md): isolated processes and actual two-host verified-stop proof. |
| Rescale, schema evolution, industrial load | Not implemented or sized | [ADR-0048](../adr/ADR-0048-rescaling-compatibility-and-native-acceptance.md). |

## Current architecture cycle

1. Review official release-2.3 Flink documentation and release-2.3.0 source.
2. Compare reference mechanisms with actual native code and retain disagreements.
3. Record ADR-0042–0048 with stories, criteria, failures and evidence boundaries.
4. Test the state-authority counterexample in a small Python/SQLite model using
   real process termination. This tests a theory, not native product completion.
5. Review the resulting documents and validate their graph.

The current cycle earns **zero new native capability completion**. Overall
original-objective progress is not inferred from seven ADR files or a passing
model. Keep the foundation's historical percentage and the new scope separate.

## First proposed native vertical slice

Before a complete distributed DAG, prove physical execution and ownership on
a fixed finite graph:

```text
finite preparation
       ↓
source A: A0, A1       two stable source splits
       ↓
operator B: B0, B1     independent native processes and fenced attempts
       ↓
finite summary        after both partitions drain
```

Use two registered workers with explicit placement and real HTTP/PostgreSQL
storage. For 500 and 5000 inputs, the physical TaskRun count is unchanged.
Kill B0, keep A0/A1/B1 process identities alive, preserve the FlowRun capacity
lease and recover 200/201 according to terminal acceptance. Refuse stale owners
and insufficient region capacity. This closes neither fan-out/join nor
cross-host death-proof criteria; it establishes the execution primitive they
need. Add branched DAG/correlated join, continuous retention and cross-host
profiles through their own gates afterwards.

## Planning and acceptance

Implementation tasks and weighted short sprints are sliced in the next
planning cycle on the user's command, following the established workflow.
The [task registry](../todo.md) and [sprint board](../sprint_board.md) identify
this active design cycle and retain all completed epoch 1 entries. A later
sprint must declare its share of the target problem and earn it through native
criteria; no share is awarded for documentation volume.

The [ADR graph](../adr/README.md), [reference comparison](../research/flink-2.3-architecture.md)
and [Principal discussion](../evidence/flink-architecture-2026-11-04/principal-discussion.json)
provide navigation into the design. Each new ADR stays `planning` or `progress`
until its own implemented scope and every acceptance criterion have evidence.
