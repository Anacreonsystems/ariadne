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
| Separate physical operator processes / partitions | Accepted for the finite fixed-P2 N2.1 slice | [ADR-0043](../adr/ADR-0043-physical-graph-and-partitioned-channels.md): stable logical operator, TaskRun per physical subtask, independent owners and resource admission; branched DAG routing remains open. |
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

Architecture documents and the standalone model earned no capability by
themselves. The later fixed-P2 implementation and independent N2.1 gate now
earn 15%; keep the foundation's historical percentage and the new scope
separate.

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
**15%**.

| Sprint | State | Cumulative target | Share | Tasks | Native exit |
| --- | --- | ---: | ---: | --- | --- |
| N2.1 — Fixed physical execution | `done` | 15% | +15% | NI2-001…006 | Immutable P2 plan, ordinary TaskRun per physical subtask, strict subtask grant, existing PostgreSQL data-plane reuse, separate source/operator processes, input 200/201 recovery, constant metadata at 500/5000, SQLite refusal and independent Principal gate. |
| N2.2 — Branched DAG and exchanges | `progress` | 27% | +12% | NI2-010…014 | Versioned forward/rebalance/broadcast/keyed exchanges, fan-out, independent subscribers and exact route replay pass real SDK/API/PostgreSQL faults. |
| N2.3 — Multi-input state and join | `planned` | 42% | +15% | NI2-020…025 | Journaled turn state, mailbox order, OUTPUT/NO_OUTPUT compatibility, correlated join and complete barrier cuts pass native process faults without accepted-body replay. |
| N2.4 — Continuous source/event time | `planned` | 55% | +13% | NI2-030…035 | Split enumerator/readers, ordered controls, drain, watermarks, idleness and timers pass real continuous native gates. |
| N2.5 — Pressure and online retention | `planned` | 67% | +12% | NI2-040…045 | Full-turn reservations, fair credits, branch pressure, online segment/object GC and bounded PostgreSQL storage plateau pass. |
| N2.6 — Selective and cross-host recovery | `planned` | 80% | +13% | NI2-050…055 | Regional RESTORING/activation, healthy-sibling continuity and real two-host verified-stop replacement pass; timeouts alone remain insufficient. |
| N2.7 — Rescale and compatibility | `planned` | 90% | +10% | NI2-060…065 | Two-to-three subtask rescale transfers the complete manifest atomically; compatible upgrade and incompatible-codec refusals pass. |
| N2.8 — Full profile acceptance | `planned` | 100% | +10% | NI2-070…075 | Source → fan-out → keyed join → sink, continuous pressure, recovery, rescale, load/resource limits, regressions, packaging/UI and independent evidence graph all pass on frozen source. |

No intermediate task earns a fraction of its sprint. A share moves to earned
only when that sprint's declared exit and independent source-bound verification
pass. Partial ADR criteria remain explicitly open for later sprints; historical
or standalone prototype evidence is never substituted for a native gate.

The [first host preflight](../evidence/native-epoch2-host-preflight-2026-11-04/run-41928669274/receipt.json)
is retained as an `UNPROVEN` infrastructure observation: its two ARC jobs did
not overlap and reported equal salted boot/machine identities. It provides no
cross-host, network or verified-stop acceptance. The fixed-P2 implementation
must first satisfy the separate [two-worker placement agreement](../evidence/flink-architecture-2026-11-04/ni2-006-two-worker-placement-agreement-2026-11-04.json)
on two real registered workers; that same-host gate still does not close
ADR-0047.
The bounded [ARC/static follow-up](../evidence/native-epoch2-host-preflight-2026-11-04/run-41931304288/receipt.json)
also remained `UNPROVEN`: the static job was never assigned and executed no
steps, so cancellation after the queue observation provides no second-host
evidence. No further infrastructure probes are planned without a product
criterion that needs a new observation.

The later [local QEMU boot discovery](../evidence/native-epoch2-qemu-readiness-2026-11-05/boot-discovery-v1/index.json)
also remains unproven. Two real guests booted Linux and cloud-init, but the
14 GiB resource guard fired before guest HTTP readiness, 30-second overlap and
distinct salted guest identity witnesses were established. Exact owned-process
cleanup passed without an OOM-kill increment. The result earns zero credit,
does not satisfy NI2-053 or ADR-0047, and will not be retried from the same
resource premise.

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

The first [two-worker eight-document observation](../evidence/native-epoch2-two-worker-discovery-2026-11-04/first-eight-documents/receipt.json)
completed four physical Task processes on two registered ProcessWorkers, but
its combined source snapshot was captured after the run and shared files were
still under implementation. It is a real milestone observation, not the N2.1
gate: both input-200 coordinates, 500/5000, named failure/security controls and
the independent frozen matrix remain required, with zero earned percentage.
The separate [preflight-order successor](../evidence/flink-architecture-2026-11-04/ni2-006-preflight-order-successor-2026-11-04.json)
records the final claim/import/register barrier without rewriting the earlier
agreement or treating application import as a hostile-code sandbox.

The [worker API component archive](../evidence/native-epoch2-worker-api-2026-11-04/index.json)
and [paired SDK archive](../evidence/native-epoch2-sdk-contract-2026-11-04/index.json)
retain their failure histories and current PostgreSQL/SQLite/client successors.
They remain component evidence because the participating runtime, worker and
server union has not yet been frozen and rerun. The
[500-record request inventory](../evidence/native-epoch2-control-overhead-2026-11-04/baseline-500-loss/counts.json)
is likewise an observation of functional control traffic, not a throughput or
industrial-readiness result. None of these records changes the 0% earned value.
Its [two-case successor comparison](../evidence/native-epoch2-control-overhead-2026-11-04/fixture-successor-comparison.json)
also remains discovery-only because its recorded 55-file inventory omitted
participating fixture/control files; the complete source union must be frozen
and rerun independently.
The [worker component archive](../evidence/native-epoch2-worker-2026-11-04/index.json)
likewise retains the supervisor race and exact failed-start cleanup successors,
including the independent component rerun. It does not replace the final
two-worker process matrix and earns no percentage by itself.
The [server component archive](../evidence/native-epoch2-server-2026-11-05/index.json)
retains the paired PostgreSQL/SQLite, migration, legacy-profile and scoped-type
successors with their original failures. It remains component-only and adds no
earned percentage before the whole-union gate.
The [startup-cleanup HTTP adjunct](../evidence/native-epoch2-startup-cleanup-http-2026-11-05/index.json)
adds a real PostgreSQL/TCP/Popen control for the formerly unsafe unknown-wait
path. It is retained as component evidence and does not change readiness.
The [compatibility discovery archive](../evidence/native-epoch2-compatibility-discovery-2026-11-05/index.json)
preserves the strict-type correction and complete ordinary/finite source-bound
successors. It is regression evidence on the live candidate, not the final
owned-checkout acceptance, so earned readiness stays 0%.
The [runtime component archive](../evidence/native-epoch2-runtime-2026-11-05/index.json)
preserves the pre-seal mutation failure and successor, two-worker discovery
cuts and their source qualifications. It does not replace the final frozen
5000-document and full-matrix runs.
The [product checkpoint receipt](../evidence/native-epoch2-product-checkpoint-2026-11-05/receipt.json)
binds the committed implementation and the isolated checkout used by the
Principal. It is a publication/source boundary only; the 13-case process
matrix and complete frozen profiles remain the acceptance authority.
The [checkpoint documentation graph](../evidence/flink-architecture-2026-11-04/ni2-006-product-checkpoint-graph-2026-11-05.json)
binds these append-only status and evidence links with zero earned readiness.

The [ADR graph](../adr/README.md), [reference comparison](../research/flink-2.3-architecture.md)
and [Principal discussion](../evidence/flink-architecture-2026-11-04/principal-discussion.json)
provide navigation into the design. Each new ADR stays `planning` or `progress`
until its own implemented scope and every acceptance criterion have evidence.

## N2.1 scoped acceptance — 2026-11-05

The [Principal N2.1 acceptance](../evidence/native-epoch2-n2-1-principal-acceptance-2026-11-05.json)
closes NI2-001…006 and earns the first 15% share. Its independent frozen matrix
passes all thirteen fixed-P2 cases: sync/async, both input-200 recovery cuts,
500 and 5000 records, authority/import/startup failures and delayed process
retirement. The [native13 archive](../evidence/native-epoch2-final-native13-2026-11-05/index.json)
binds the unchanged 2,232-file input map and the copied generated build module.

Complementary PostgreSQL17 and SQLite controls, PostgreSQL14 backend refusal,
paired migration successors, SDK/package/type regressions and exact startup
cleanup are mapped by hash in the acceptance record. The
[source-relevance bridge](../evidence/native-epoch2-n2-1-source-relevance-2026-11-05.json)
keeps two later test-oracle corrections separate from the frozen product run;
production and DDL bytes did not change. Hosted successor status is tracked as
publication evidence and does not rewrite the independent local results.
The [hosted parent failure archive](../evidence/native-epoch2-ci-parent-1e0ac8d-2026-11-05/index.json)
retains the actual PostgreSQL14 and legacy SQLite job logs that triggered the
two test-only corrections.

This is the fixed finite P2 execution primitive only. ADR-0043 remains
`progress`: branched DAG sealing, independent branch cursors and immutable
multi-exchange routing stay open in N2.2, while the later ADRs retain their
original planning gates.
The [N2.1 documentation graph](../evidence/native-epoch2-n2-1-graph-2026-11-05.json)
validates the resulting statuses and local evidence links.

The [publication closure](../evidence/native-epoch2-n2-1-publication-closure-2026-11-05.json)
records that PR 19 merged the accepted tree without a code-tree delta as
`266609b178fd3361c4588f16a6033d08f40bd0ea`. Hosted run `41950170943`
completed with 71 successful jobs, one declared conditional skip and no
failure. The [hosted archive](../evidence/native-epoch2-n2-1-hosted-ci-2026-11-05/index.json)
preserves the run/job metadata, native PostgreSQL logs and XML count guards.
This publishes the 15% fixed-P2 slice; it does not change the open
ADR-0043 or epoch-02 boundaries above.

## NI2-010 topology-only DAG acceptance — 2026-11-05

The [source-bound Principal acceptance](../evidence/native-epoch2-ni2-010-principal-acceptance-2026-11-05/acceptance.json)
closes NI2-010 as an independently verified metadata component. It covers
protected public seal/read, generic connected single-root topology, paired
PostgreSQL/SQLite migrations, atomic internal region reservation and hostile
fences against existing Task, event and DATA paths. A real two-session
fixed-P2/DAG admission race admits exactly one complete region and leaves no
partial loser allocation.

This result earns 0% by itself. The cumulative epoch value remains 15% and
N2.2 remains `progress`: the public DAG is deliberately unusable until
NI2-011 adds atomic routed records and NI2-012 adds verified start, wait,
failure and cleanup. The [NI2-011 handoff](../evidence/flink-architecture-2026-11-04/ni2-011-implementation-handoff-2026-11-05.json)
defines direct references in the existing data plane, exact replay receipts
and the lock order for that next slice.
