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

## Explicit work approval and workflow adaptation

On 2026-11-04 the user directed the team to slice and implement the full
ADR-0042–ADR-0048 set. The approved document version is
`fae53c90aa3b44dbc45032833b4d193001e5c8bd`; the source-bound
[approval record](../evidence/flink-architecture-2026-11-04/epoch2-work-approval-2026-11-04.json)
preserves the seven hashes. Approved solution bodies and acceptance criteria
are immutable. Status, evidence links and appended history/criterion mappings
may change; a semantic change requires a separately approved decision.

This repository uses the following status vocabulary for this approved epoch:

- `pending`: the decision record exists but has not yet been discussed;
- `planning`: the solution and criteria are accepted for planning and
  implementation, but implementation has not started;
- `progress`: approved implementation is in progress and incomplete;
- `accepted`: the independent ETL Architect / HighLoad Principal personally
  ran and reviewed every criterion in the declared scope;
- `rejected`: the theory failed or an approved replacement and retained
  rationale supersede it.

The implementation role writes runtime code and source-bound evidence. The
architect owns the ADR graph and acceptance, reruns the real checks, and is the
only role that may change these ADRs to `accepted`. Routine corrections inside
the approved scope do not require repeated permission; architecture or AC
changes return to a new decision.

## Dependency graph and delivery order

ADR-0042 is an umbrella that **extends** the accepted epoch-1 foundation. It is
not a cyclic prerequisite for the other decisions. The executable graph is:

```text
accepted ADR-0032…ADR-0040 foundation
                 ↓
             ADR-0043
                 ↓
             ADR-0044
                 ↓
             ADR-0045
          ↘      ↓      ↙
             ADR-0046
             ↙     ↘
        ADR-0047   │
             ↘     ↓
             ADR-0048
                 ↓
       ADR-0042 umbrella acceptance
```

ADR-0046 may develop quota primitives beside earlier work, but its complete
gate requires the routed edges, state/join roots and continuous source controls
from ADR-0043–ADR-0045. ADR-0047 requires physical ownership, portable state
frontiers and recovery pins. ADR-0048 converges every preceding native gate.

## Delivery milestones and short-sprint allocation

The milestone percentages below measure the **new epoch-2 profile problem**,
separately from epoch 1's accepted bounded foundation. The
[sprint board](../sprint_board.md) divides them into 24 short sub-sprints with
concrete exits. Planned shares total 100%; earned native readiness is currently
**0%**.

| Sprint | State | Cumulative target | Share | Tasks | Native exit |
| --- | --- | ---: | ---: | --- | --- |
| N2.1 — Fixed physical execution | `progress` | 15% | +15% | NI2-001…006 | Immutable P2 plan, ordinary TaskRun per physical subtask, strict subtask grant, existing PostgreSQL data-plane reuse, separate source/operator processes, input 200/201 recovery, constant metadata at 500/5000, SQLite refusal and independent Principal gate. |
| N2.2 — Branched DAG and exchanges | `planned` | 27% | +12% | NI2-010…014 | Versioned forward/rebalance/broadcast/keyed exchanges, fan-out, independent subscribers and exact route replay pass real SDK/API/PostgreSQL faults. |
| N2.3 — Multi-input state and join | `planned` | 42% | +15% | NI2-020…025 | Journaled turn state, mailbox order, OUTPUT/NO_OUTPUT compatibility, correlated join and complete barrier cuts pass native process faults without accepted-body replay. |
| N2.4 — Continuous source/event time | `planned` | 55% | +13% | NI2-030…035 | Split enumerator/readers, ordered controls, drain, watermarks, idleness and timers pass real continuous native gates. |
| N2.5 — Pressure and online retention | `planned` | 67% | +12% | NI2-040…045 | Full-turn reservations, fair credits, branch pressure, online segment/object GC and bounded PostgreSQL storage plateau pass. |
| N2.6 — Selective and cross-host recovery | `planned` | 80% | +13% | NI2-050…055 | Regional RESTORING/activation, healthy-sibling continuity and real two-host verified-stop replacement pass; timeouts alone remain insufficient. |
| N2.7 — Rescale and compatibility | `planned` | 90% | +10% | NI2-060…065 | Two-to-three subtask rescale transfers the complete manifest atomically; compatible upgrade and incompatible-codec refusals pass. |
| N2.8 — Full profile acceptance | `planned` | 100% | +10% | NI2-070…075 | Source → fan-out → keyed join → sink, continuous pressure, recovery, rescale, load/resource limits, regressions, packaging/UI and independent evidence graph all pass on frozen source. |

No intermediate task earns a fraction of its sprint. A share moves to earned
only when that sprint's exit, every mapped ADR criterion, and independent
source-bound verification pass. Historical or standalone prototype evidence is
never substituted for a native gate.

The [first host preflight](../evidence/native-epoch2-host-preflight-2026-11-04/run-41928669274/receipt.json)
is retained as an `UNPROVEN` infrastructure observation: its two ARC jobs did
not overlap and reported equal salted boot/machine identities. It provides no
cross-host, network or verified-stop acceptance. The fixed-P2 implementation
must first satisfy the separate [two-worker placement agreement](../evidence/flink-architecture-2026-11-04/ni2-006-two-worker-placement-agreement-2026-11-04.json)
on two real registered workers; that same-host gate still does not close
ADR-0047.

The [first physical CI discovery](../evidence/native-epoch2-physical-ci-2026-11-04/discovery.json)
is also retained without readiness credit. Its initial SQLite run exposed an
invalid test event fixture and its successor passed the corrected selector,
but both observations were made while the participating source was changing.
The final N2.1 gate must bind the complete server, client, runtime, worker,
migration, test and workflow source union and rerun the selected profiles.
The current append-only planning tranche has a separate
[document-graph verification](../evidence/flink-architecture-2026-11-04/ni2-006-planning-graph-2026-11-04.json);
it records zero earned native credit.

The [combined one-worker transition snapshot](../evidence/native-epoch2-local-process-discovery-2026-11-04/before-worker-placement/inventory.json)
retains the 46-file WIP source/test inventory before the placement refactor. It
was not executed as one frozen suite and does not retroactively bind the earlier
8/500-document discovery runs or satisfy any acceptance criterion.

The [ADR graph](../adr/README.md), [reference comparison](../research/flink-2.3-architecture.md)
and [Principal discussion](../evidence/flink-architecture-2026-11-04/principal-discussion.json)
provide navigation into the design. Each new ADR stays `planning` or `progress`
until its own implemented scope and every acceptance criterion have evidence.
