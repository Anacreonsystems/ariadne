# Native integration epoch 2: complete the streaming execution model

- Status: `progress` — native DAG lifecycle/runtime integration and source-bound acceptance gates in progress; 15% earned
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

The 2026-11-05 user direction «Все вливайте, для полных
тестов» releases the publication hold for the current NI2-011 work and
authorizes merging it for the complete repository test matrix. The direction
does not award readiness or close an acceptance criterion. Public DAG
execution remains fenced until the later lifecycle work and its independent
evidence pass.

## NI2-011 finite routed-storage acceptance — 2026-11-05

The [source-bound Principal acceptance](../evidence/native-epoch2-routed-server-2026-11-05/principal-v1/acceptance.json)
closes NI2-011's finite storage component. The final source materializes one
PAYLOAD with bounded direct REFERENCE rows in the terminal transaction,
persists exact replay outcomes, keeps subscriber cursors independent and pins
referenced data across the paired PostgreSQL and SQLite migrations. Legacy
prefix reads now refuse DAG edge partitions before taking their data-row lock.

The independent final cut passed 38 PostgreSQL and one SQLite model controls,
five PostgreSQL and one SQLite prefix controls, the earlier 23/23 migration
gate and strict analysis of all 17 selected modules, with no skips or source
drift. NI2-011 moves to `accepted`, but earns 0% until the N2.2a hosted
publication gate completes. The cumulative epoch value remains 15%. Public
DAG start, Task execution, finish, failure, cancellation and cleanup remain
fenced for NI2-012 through NI2-014.

The initial full main-branch matrix found one test-only historical schema leak:
an expected DAG downgrade refusal left the newer routed migration rolled back
for the next strict database cleanup. The
[independent schema-context acceptance](../evidence/native-epoch2-full-ci-schema-context-2026-11-05/principal-v1/acceptance.json)
records the `finally` restoration and complete 90/90 PostgreSQL/SQLite
migration successors. No product or DDL byte changed, and the epoch remains at
15%.

## NI2-012 native DAG integration progress — 2026-11-05

NI2-012 is now `progress`, rather than planned. The current implementation has
paired SDK/server lifecycle wire, durable worker launch journals, verified
bootstrap/activation, dedicated routed storage methods and public
start/wait/cancel behavior. The source-bound [sync and async normal-eight
gate](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-normal8-v1/receipt.json)
ran four ordinary Task processes across two workers and released every physical
and tag allocation. The [preview, application-failure and cancellation
gate](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-lifecycle-v1/receipt.json)
also reached verified terminal cleanup without treating a timeout as process
death.

The same integration cut passed [sync 500](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-sync500-v1/receipt.json)
and [async 500](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-async500-v1/receipt.json)
with four stable TaskRuns, seven stable channels, exact cursors/EOFs and zero
remaining slots or tags. Both input-200 coordinates passed in sync mode:
[accepted 200 resumes at 201](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-sync-accepted200-v1/receipt.json)
and [incomplete 200 resumes at 200](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-sync-incomplete200-v1/receipt.json).
The [async accepted-200 successor](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-async-accepted200-v1/receipt.json)
passed the same healthy-sibling and exact-replacement checks.

The [async incomplete-200 run](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-async-incomplete200-red-v1/receipt.json)
preserved correct business cursors and final cleanup but exposed a real
terminal-PREPARE race: a stale worker observation created an ungranted candidate
process after a healthy sibling had already committed Task and EOF completion.
The strict no-extra-process oracle remains unchanged while the server adds a
fresh pre-spawn terminal check and the worker converts that typed refusal into
verified retirement. Rebalance/keyed real-process controls, sync/async 5000,
the final frozen union and independent Principal acceptance also remain open.
Therefore NI2-013 and NI2-014 stay planned, N2.2 stays `progress`, and earned
native readiness remains 15%; no approved ADR body or acceptance criterion is
changed by this status update.

## N2.2 retained verification and finalization correction — 2026-11-05

N2.2 remains `progress` with **15% earned** for epoch 2. The evidence below
retains actual earlier executions and their own frozen source bindings; they
are not executions of the corrected candidate. This status update changes no
approved ADR solution or acceptance criterion.

The retained [sync 500](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-sync500-v1/receipt.json)
and [async 500](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-async500-v1/receipt.json)
passed with four stable ordinary TaskRuns, seven channels, exact accepted
cursors and zero remaining allocations. All four 500-document OS-loss cases
also have passing successors: sync/async accepted 200 resume at 201, while
sync/async incomplete 200 resume at 200. The
[async incomplete-200 successor](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-async-incomplete200-green-v2/receipt.json)
closes the previously observed terminal-PREPARE race with the unchanged strict
five-process oracle; its [earlier failure](../evidence/native-epoch2-dag-execution-runtime-2026-11-05/actual-async-incomplete200-red-v1/receipt.json)
is preserved. No healthy sibling is replaced in these passing cases.

The archived [earlier Principal exchange trials](../evidence/native-epoch2-personal-controls-2026-11-05/index.json)
include passing 64-document rebalance and keyed native process controls, with
per-partition order. Their retained observations also passed an independent
post-hoc canonical signed-i64/SHA-256/G16 key check. Therefore terminal-PREPARE and exchange-mode checks are no longer
unexecuted gates. They do not imply that every later source union is accepted.

Both [personal 5000-document trials](../evidence/native-epoch2-finalization-2026-11-05/personal5000-red-v1/receipt.json)
on `d504c7042e72c8d18fb0f63f5ebeeee826a7d650` failed lifecycle acceptance.
The source and three subscriber cursors reached 5000 and all four TaskRuns
were observed Completed, but reference cleanup exceeded the original ten-second
statement budget. The last graph remained ADMITTED, the Flow remained RUNNING
and one physical allocation remained held. A recovery-discovery HTTP 500 then
escaped the worker poll. Later process/database cleanup is operational cleanup,
not successful platform finalization.

The [four-index causal proof and paired migration controls](../evidence/native-epoch2-route-cleanup-indexes-2026-11-05/component-v1/index.json)
address child foreign-key lookup scans without changing constraints, locks,
cleanup order or timeouts. On the same seeded rows the first DELETE timed out
at 10.030 seconds before indexing; the indexed five-statement cleanup and
commit completed in 1.044 seconds. PostgreSQL/SQLite upgrade, downgrade,
retained-row and existing execution-migration checks passed. These are scoped
component measurements, not throughput objectives or corrected native 5000
acceptance. The separately archived narrow worker correction defers only
untyped HTTP 5xx recovery discovery to its next poll and preserves other
refusals. The [scoped successor review](../evidence/native-epoch2-successor-review-2026-11-05/index.json)
retains the final strict 29-file type check and the three constant-table-name
ORM typing corrections. Those corrections preserve both dialects' table/index
metadata and keep earlier execution hashes historical.

The same [archive](../evidence/native-epoch2-finalization-2026-11-05/index.json)
retains a passing real 16-document ordinary Task retry probe: a ValueError on
input 5 before yield repeats only that unfinished input; the B TaskRun keeps
its process and physical epoch 1, and its run count becomes 2. That Task retry
is distinct from replacing a dead OS process.

NI2-012 through NI2-014 remain open. Their final exit requires both corrected
sync/async 5000-document native runs on a fresh frozen source lease, successful
terminal resource release, the required hosted matrix and independent
Principal verification. Neither this archive nor a component PASS earns the
planned 27% target. Joins, managed state, continuous input/retention,
cross-host recovery and rescaling remain later epoch-2 gates.

## N2.2 mid-run liveness hold — 2026-11-05

The [corrected-candidate attempts](../evidence/native-epoch2-midrun-liveness-2026-11-05/cohort/controlled-red-attempt-summary-v1.json)
on `835b8c21c83e4d0a2d95ddc6abe0b06fb9d75db5` were stopped with exit 2 after
an independently observed normal-path liveness/metadata violation. Both sources
accepted 5000 yields; the subscribers had processed only about 2600–2750 inputs.
Final 5000, EOF and lifecycle assertions were not reached. Sync was interrupted
under the paired diagnostic policy. The [exact controlled-stop receipt](../evidence/native-epoch2-midrun-liveness-2026-11-05/cohort/controlled-red-stop-v1.json)
records guarded owned-process shutdown; both source closures retain 1599
unchanged files, zero remaining observed PIDs and zero database sessions.
Operational cleanup is not successful platform resource release.

The [actual C0 retry record](../evidence/native-epoch2-midrun-liveness-2026-11-05/cohort/retry-state-diagnosis-124241500348051.json)
links a consumer-read HTTP 500 to a persisted Retrying event at
18:08:27.397961 UTC. The same physical C0 process and TaskRun executed inputs
1–763 in attempt 1 and 764–2649 in attempt 2, with no repeated accepted-prefix
business sequence. Physical epoch stayed 1 while Task run count became 2.
This is an unexpected ordinary Task retry in the no-fault gate; data corruption
or accepted-body replay was not established.

The [old/new comparison](../evidence/native-epoch2-midrun-liveness-2026-11-05/old-new-comparison/report.json)
shows mid-run renewal timeouts on both the earlier `d504c70…` and corrected
`835b8c2…` code. They occurred before final GC. The four cleanup indexes address
the separately demonstrated GC scan; no causal index regression is claimed.
Both renewal and consumer-read failures retain their own error locations and
raw observations in the [liveness archive](../evidence/native-epoch2-midrun-liveness-2026-11-05/index.json).

The [renewal component](../evidence/native-epoch2-dag-renewal-liveness-2026-11-05/component-v1/index.json)
reproduced a real PostgreSQL lock wait: renewal requested an exclusive FlowRun
prefix while another transaction held the legitimate shared Flow/authority/graph
prefix. The five-second test deadline observed this blocked request; that RED
is not a newly reproduced HTTP 500 or a change to the platform's ten-second
budget. The narrow correction uses a shared parent prefix only for renewal,
retaining worker/capability reads, subtask/grant update locks and all identity,
control and stale-grant fences. Its scoped model/API controls and type check
passed. The [independent personal repeat](../evidence/native-epoch2-dag-renewal-liveness-2026-11-05/principal-v1/index.json)
then passed all 16 real PostgreSQL model/control cases in 62.266 seconds, with
zero skips, failures, errors or source drift and clean owned-database teardown.
This verifies the renewal component; native 5000 acceptance remains open.

The consumer-read timeout reached partition locking after the parent prefix.
Its causal relationship to the renewal queue remains open. The retained
[historical partition window](../evidence/native-epoch2-midrun-liveness-2026-11-05/cohort/partition-read-historical-window-v1.json)
contains actual waiters/blockers but no exact HTTP-request/backend correlation.
A bounded controlled read/append comparison must distinguish a direct partition
conflict from the global renewal queue before the next full 5000 runs. The
forensic databases remain intact; new probes use separate owned databases.

N2.2 stays `progress` at **15% earned**. The 27% target, NI2-012…014 closure and
ADR acceptance remain withheld until both liveness paths, fresh frozen-source
native runs and the required actual hosted matrix pass. No approved solution
or acceptance criterion changes in this update.

## N2.2 bounded read probe and hosted observations — 2026-11-05

The [retained-volume read/append probe](../evidence/native-epoch2-partition-read-probe-2026-11-05/component-v1/index.json)
used separate adapted clones and left the forensic cohort unchanged. Disjoint
append/read completed; a shared-input reader waited on the observed partition
lock and completed after release. Baseline, old-renewal and fixed-renewal
variants each ran 45 seconds without errors. These sequential bounded
measurements establish neither a performance gain nor the historical
consumer-read cause: that timeout remains **not reproduced / unresolved**.

The [diagnostic CI extension](../evidence/native-epoch2-runtime-ci-diagnostics-2026-11-05/component-v2/index.json)
preserves selected API/worker logs, PID/call traces and post-command resources
for native shards and the physical PG batch without changing bodies, oracles
or budgets. The [two actual async recovery CI failures](../evidence/native-epoch2-runtime-ci-diagnostics-2026-11-05/ci-observation-v1/index.json)
and [physical P2 extra-marker observation](../evidence/native-epoch2-runtime-ci-diagnostics-2026-11-05/ci-physical-pg17-red-v1/index.json)
remain open observations with incomplete role/blocker attribution; no data
corruption or common cause is inferred. The next declared native probe is one
normal async 1000-document run with live health checks, crossing the historical
input-764 boundary. Further 5000 runs depend on its actual result. N2.2 remains
`progress` with 15% earned; no ADR criterion is accepted by these measurements.


## N2.2 native 1000 terminal retirement hold — 2026-11-05

As a system, I got an actual async 1000-document execution on frozen
`f416080900b5b15f8d9bc8ac1bc0921221dfe2b1` with the unchanged strict native
helper: **1 PASS in 471.607 seconds**, 1608 leased files with no drift, all
three accepted cursors at 1000, seven terminal channel tails, four ordinary
TaskRuns at run_count/physical_epoch 1, and exact calls and effects. All owned
processes and database sessions stopped. This is a bounded probe, not a 5000
acceptance run.

The additional final log audit is **RED**. The native graph reached RELEASED
with a completed FINALIZE journal and released physical/tag allocations, but
the coordinator's legacy release RPC refused the DAG profile with
`EXECUTION_PROFILE_NOT_READY`. The runner then incorrectly treated that cleanup
exception as a failed start; its crash proposal was fenced with
`RECOVERY_NOT_READY`. Two runner ERROR lines remain in the evidence. The native
coordinator authority was TERMINAL, its deployment snapshot ACTIVE, and its
launch PROCESS_REGISTERED. Graph resource release therefore did not complete
coordinator retirement. Both owned zero-session databases are retained for
forensics; operational cleanup is not counted as platform release.

The 240 live monitor samples observed no HTTP 5xx or unexpected Task retry;
those narrower thresholds did not detect the later typed cleanup refusals.
The final independent audit and the original verifier's failed extra health
assertion are retained separately. DEBUG queue.Empty tracebacks are classified
separately from the two actual runner errors.

At input 764, the source had already accepted its tail 1000 and stopped
appending; EOF occurred only after the final cursor latch. This does not
reproduce the historical concurrent source-1522/consumer-763 read failure,
whose cause remains unresolved. Terminal-path files are byte-identical across
d504, 835, e4e and f416; this source comparison alone does not claim historical
terminal execution or attribute the gap to the renewal correction.

See the [immutable native-1000 package](../evidence/native-epoch2-coordinator-retirement-2026-11-05/native1000-red-v1/index.json),
[qualified result](../evidence/native-epoch2-coordinator-retirement-2026-11-05/native1000-red-v1/cohort/principal-normal1000-qualified-receipt-v2.json),
and [read-only terminal catalog](../evidence/native-epoch2-coordinator-retirement-2026-11-05/native1000-red-v1/cohort/terminal-readonly-catalog-v1.json).
A narrow coordinator-only terminal path is approved for implementation under
the existing integration scope. It must preserve direct DAG data refusals and
legacy linear/fixed-P2 release, require current durable terminal/closure proofs,
and pass real HTTP/PostgreSQL and native worker-health controls before further
5000 runs. N2.2 stays `progress`, with **15% earned**; no ADR acceptance or
additional percentage is claimed.


## N2.2 hosted rebalance runtime RED — 2026-11-05

As a developer, I can inspect the [actual hosted rebalance failure](../evidence/native-epoch2-ci-rebalance-runtime-2026-11-05/b4-red-v1/index.json)
on immutable `b4a5f1ed7abc9cdd35ebb4451c2c1ef628e4c145`,
[run 42099724610, job 127501239557](https://github.com/Anacreonsystems/ariadne/actions/runs/42099724610/job/127501239557).
The single runtime case failed in 229.934 seconds after all 64 source inputs
and the four stable Task bodies completed. At the SDK wait deadline, the
observed graph was still ADMITTED with physical C slots unreleased. The Flow
assertion is FAILED. Graph RELEASED is observed only after timeout and fail
cleanup; it does not demonstrate normal closure before the deadline.

The [observation windows](../evidence/native-epoch2-ci-rebalance-runtime-2026-11-05/b4-red-v1/timeline-observation-windows.json)
retain direct log timestamps separately from estimated monotonic-to-wall
mapping. The later retire409 follows timeout/fail control and is not evidence
that the separate native-1000 post-terminal release refusal caused this case.
The [resource addendum](../evidence/native-epoch2-ci-rebalance-runtime-2026-11-05/b4-red-v1/resource-and-trace-addendum-v1.json)
is an after-command observation, not a controlled CPU experiment. The cause
of release latency remains unresolved; no CPU, renewal or coordinator cause
is assigned, and no timeout or oracle change is justified by this record.

Separately, the current coordinator correction has an
[implementation report](../evidence/native-epoch2-coordinator-retirement-2026-11-05/implementation-report-v1/index.json)
of five native controls passing in 118.14 seconds on its recorded four-file
candidate source set. This is an implementation component result. Independent
Principal verification is pending, and any later fixture successor must retain
its own source binding. It is not the source tested by the b4 hosted case.

Acceptance criteria remain open: as a system, I still need normal graph and
coordinator closure before the existing deadline, clean infrastructure health,
and the remaining source-bound native and hosted matrix. N2.2 stays `progress`;
this record changes no earned readiness or ADR acceptance.


## N2.2 hosted async 500 RED and scoped CI controls — 2026-11-05

As a developer, I can inspect the separate
[b4 async-500 failure](../evidence/native-epoch2-ci-volume-runtime-2026-11-05/b4-async500-red-v1/index.json)
from [job 127501239543](https://github.com/Anacreonsystems/ariadne/actions/runs/42099724610/job/127501239543).
The source accepted all 500 records, but immediately before fail control the
three consumer cursors were only 438/432/435 and all four TaskRuns remained
RUNNING. Worker fenced exceptions occurred after `/fail` returned 200. They
are downstream observations, not the identified initiating error. The async
Flow's original trigger and slow-drain cause remain UNKNOWN. No PG error
trace is retained, and no CPU, renewal, coordinator or common sync-500 cause
is assigned. This differs from rebalance's completed-body release delay.

The separate [generic Python-3.14 job](../evidence/native-epoch2-ci-generic-envelope-2026-11-05/component-v1/index.json)
recorded 4413 PASS and 11 ordinary skips in its 800.25-second test step, but
the required whole job was CANCELLED at the 15-minute envelope. Its reviewed
conditional gives only the runner/worker/settings/input/CLI generic bucket
20 minutes; the other generic buckets retain 15. This changes job setup/body/
cleanup headroom, not native wait budgets or test oracles, and is not a green
rerun. The independent one-line proposal review is bound to that historical
proposal; it does not accept the later combined workflow.

The combined [CI guard component](../evidence/native-epoch2-coordinator-ci-guards-2026-11-05/component-v1/index.json)
has 41 actual passing guard cases on its recorded workflow/test hashes. It
requires both preseal runtime nodes and rejects missing preseal collection.
Those guard cases do not execute the native preseal bodies or close the
runtime matrix. Actionlint passed with external shellcheck and pyflakes
disabled; those external checks were not run.

Acceptance criteria remain open: as a system, I still need the source-bound
native and hosted runtime exits. These archives and scoped CI results change
no earned readiness, task completion or ADR acceptance; N2.2 stays `progress`.


## N2.2 separate hosted sync 500 deadline RED — 2026-11-05

As a developer, I can inspect the separate
[b4 sync-500 record](../evidence/native-epoch2-ci-volume-runtime-2026-11-05/b4-sync500-red-v1/index.json)
from [job 127501239501](https://github.com/Anacreonsystems/ariadne/actions/runs/42099724610/job/127501239501).
This job directly retained the public wait TimeoutError after 752.86 seconds
against the existing 750-second wait. All source positions and three accepted
cursors reached 500, but normal physical closure was not observed before the
deadline. The graph remained ADMITTED with C1 unreleased in a later progress
observation; Graph RELEASED was first observed only after `/fail` cleanup.
The final Flow assertion is FAILED, despite the completed Task bodies.

The first all-Task-completed observation maps to 20:41:58.335 and the direct
timeout log is 20:41:58.230. That estimated 105 ms difference is not a precise
database transaction ordering proof. The
[source-binding erratum](../evidence/native-epoch2-ci-volume-runtime-2026-11-05/b4-sync500-binding-erratum-v1/index.json)
also retains that the reused blob-binding note's missing-initiator wording
belongs to async500; the sync record directly retains its TimeoutError. The
three source hashes themselves remain valid.

Acceptance criteria remain open: as a system, I still need the native runtime
exits within their existing contracts. The latency cause remains UNKNOWN;
this record assigns no CPU, renewal, coordinator or shared async/rebalance
cause. An isolated CI runtime profile remains a proposal, with no implemented
or executed profile claimed here. No readiness or ADR acceptance changes.


## N2.2 hosted accepted-200 recovery remains RED — 2026-11-05

As a developer, I can inspect the
[b4 sync accepted-200 recovery record](../evidence/native-epoch2-ci-recovery-runtime-2026-11-05/b4-sync-accepted200-red-v1/index.json)
from [job 127501239596](https://github.com/Anacreonsystems/ariadne/actions/runs/42099724610/job/127501239596).
The observed replacement retained B's TaskRun, changed its process, and first
processed input 201 after accepted input 200. Fault-to-first-replacement-body
time was 60.123 seconds. The retained B calls cover 1–407 once. These are
partial recovery observations; the complete case still failed its 750.38-second
wait and did not pass the final recovery/effect/signature assertions.

The source accepted all 500 inputs, while the final accepted consumer cursors
were B=407 and the two C partitions=451/448. C body-call counts 452/449 include
in-flight work and must not replace those accepted cursor positions.

Two genuine HTTP 500 errors are retained: replacement launch `prepare` at
20:52:42.903 and `record` at 20:53:06.364. Their traces wait on the Flow row
with `FOR UPDATE`, ending in asyncpg timeout; PostgreSQL records two statement
cancellations while locking `flow_run`. The blocking transaction was not
captured. These callers are replacement prepare/record, not renewal, and no
causal attribution to the separate terminal coordinator correction is made.
Whether these waits dominate the later unfinished drain or only add latency
remains UNKNOWN. Later fenced worker failures follow `/fail` cleanup.

Acceptance criteria remain open: as a system, I still need the entire recovery
case and the source-bound native/hosted exits. This record awards no epoch or
ADR credit, changes no oracle or native wait budget, and supplies no independent
verdict on the current coordinator candidate. N2.2 remains `progress`.


## N2.2 scoped coordinator verification and serial CI configuration — 2026-11-05

As a developer, I can inspect the
[Principal's independent coordinator component proof](../evidence/native-epoch2-coordinator-retirement-2026-11-05/principal-v1/index.json):
**52 actual PASS** comprising 45 PostgreSQL controls, five native lifecycle
scenarios and two native preseal scenarios. The historical 1599-file leases
have no source or import drift. All 50 captured process identities closed,
and the six owned component databases were dropped with no remaining sessions.
These operational checks accompany persisted coordinator authority, snapshot
and launch RELEASED proofs; they do not replace them.

Sync/async normal execution retained four ordinary TaskRuns and exact client
documents. Preview, failure and cancellation preserve their intended outcomes.
Both preseal failures created no TaskRuns, operator processes or streaming
execution metadata. All seven native scenarios reject post-terminal
infrastructure errors. Expected injected-failure errors remain explicitly
classified. The model phase retains 214 harness file-handle ResourceWarnings;
the native/preseal v3 driver corrected context managers without changing warning
filters, product code, budgets or oracles. The original phase-specific driver
and hold hashes, fixture bridge and scoped Principal verdict are preserved.

Separately, the [serial CI component](../evidence/native-epoch2-ci-serial-profile-2026-11-05/component-v1/index.json)
retains 41 passing guard cases, actual collection of 30 selected cases from 31
and a planner mapping all 19 required native cases to jobs, including both
preseal cases. The initial wrong-PATH harness produced 41 setup errors and
zero executed test bodies; its correction uses the existing virtual environment
and installs nothing. After the personal phases stopped, only the workflow
and guard changed; all four runtime/test source hashes stayed equal to the
personal proof. Historical personal manifests are not relabeled as new CI runs.

The configured matrix declares exchanges first and heavy 5000 cases last,
with at most one job from this matrix running concurrently. JSON include order
is not a GitHub FIFO guarantee, and Physical PostgreSQL and other jobs can still
overlap. No host isolation, CPU cause or native budget change is claimed.

Acceptance criteria remain open: as a system, I still need the remaining
source-bound recovery, volume and hosted exits. The b4 recovery prepare/record
500 errors and their Flow-row blocker remain unresolved; corrected full 5000
and actual hosted serial-profile acceptance are unexecuted. This scoped
component result does not complete N2.2 or an ADR. Earned readiness remains
**15%**, with no additional credit.


## N2.2 launch parent-lock correction and scoped sync500 recovery — 2026-11-06

As a developer, I can inspect the [closed parent-reader RED baseline](../evidence/native-epoch2-launch-parent-locks-2026-11-06/baseline-v1/index.json),
[failed first candidate](../evidence/native-epoch2-launch-parent-locks-2026-11-06/failed-candidate-v1/index.json),
[final implementer component](../evidence/native-epoch2-launch-parent-locks-2026-11-06/component-v1/index.json) and
[Principal's independent 50-check verification](../evidence/native-epoch2-launch-parent-locks-2026-11-06/principal-v1/index.json).
The production change selects the existing shared parent prefix only in launch
prepare and record. The helper default, other callers, own-target locks and
lifecycle fences remain unchanged. The RED requests were pending at the same
five-second deadline with an identified synthetic SHARE blocker; each returned
200 only after that holder rolled back. Pending did not mean HTTP 500, and the
historical b4 blocker identity remains unknown. The eight first-candidate
failures were retained incorrect 409 status expectations, corrected to the
existing 403 `SUBTASK_SCOPE_DENIED` contract without another product change.

As a user, I got two personally verified existing native SDK/TCP/PostgreSQL17/OS
sync500 recovery cases: [accepted200](../evidence/native-epoch2-launch-parent-locks-2026-11-06/native-accepted200-v1/index.json)
resumes first at 201, while [incomplete200 and the pair receipt](../evidence/native-epoch2-launch-parent-locks-2026-11-06/native-incomplete200-v1/index.json)
repeat only unfinished input 200. B keeps its TaskRun and changes PID; the
incomplete case has exactly 501 body calls and 500 distinct effects. Both cases
reach all three cursors at 500, retain healthy A/C attempt 1, close all five
births and release graph plus coordinator authority/snapshot/launch states.
Source-body and wider-health PASS are separate: exact API access 5xx,
`TimeoutError`, unhandled ASGI failures and unexpected healthy retries are zero.
The old B exit 77 is the expected injected fault.

As a system, I got exact final-byte source leases and physical cleanup evidence.
The 50 personal checks and each native case retain unchanged 1,600-file holds and zero
import mismatches. Across the native pair, 24 captured PID/start-time identities
closed and four fresh owned databases were dropped without FORCE or remaining
sessions. Cleanup observations accompany the persisted release proofs.

| Approved criterion | Current scoped evidence | Remaining boundary |
| --- | --- | --- |
| A43-05 — Separate source/operator processes and isolated replacement | Personal50 proves shared-reader coexistence and exclusive lifecycle/own-target fences; the native synchronous 500-record pair proves 200/201 continuation with healthy-sibling continuity and exact effects. | Async recovery, corrected 5000 and the complete hosted/frozen-union gates remain open; this does not complete the whole criterion or ADR. |
| A43-07 — Observable slots, pending admission and one FlowRun lease | The [health-artifact collector](../evidence/native-epoch2-runtime-health-artifacts-2026-11-06/component-v1/index.json) retains coordinator retirement and worker health; the pair records released coordinator/graph state and explicit whole-health checks. | Diagnostic retention and these release observations do not establish the complete slot/admission/FlowRun-lease acceptance profile. |

The collector's two actual CLI checks and final static checks pass. Its v1
source reconstruction qualification remains retained; v2 has actual pre-run
source capture. Both workflow event filters include the collector path. The new
retention does not backfill missing coordinator/health reports in older hosted
artifacts. Post-hoc archival resource snapshots are labeled after-case
collection, rather than evidence of either test's resource window. The
[ADR graph README](../adr/README.md) corrects the stale current zero to the existing
15% earned value; that navigation correction grants no new credit.

The current hosted [run 42116718681](https://github.com/Anacreonsystems/ariadne/actions/runs/42116718681) is a separate committed
`7d99d8472c617a966c868c3bd1696a3fe4410c1a` source boundary. At the verified
2026-11-06 02:43:45 Europe/Moscow (2026-11-05 23:43:45 UTC) snapshot, both DAG500 jobs passed their exact no-skip gates:
[sync 1 PASS / 709.65s](https://github.com/Anacreonsystems/ariadne/actions/runs/42116718681/job/127559987264) and
[async 1 PASS / 638.66s](https://github.com/Anacreonsystems/ariadne/actions/runs/42116718681/job/127559987278).
The [hosted sync accepted200 job](https://github.com/Anacreonsystems/ariadne/actions/runs/42116718681/job/127559989778) also completed its
pytest/no-skip/upload/cleanup steps. The whole run was still incomplete; the
remaining recovery/5000 gates were not accepted. These committed-source CI
observations do not test the separately held local parent-lock correction.
Older CI artifacts lack the two new coordinator/health reports, so job success
is not substituted for their missing whole-health evidence.

NI2-012, N2.2b and ADR-0043 remain `progress`. The historical b4 RED and failed
5000 records remain unchanged. Earned readiness stays **15%**; the planned
27% target, full 5000 acceptance and whole-ADR completion remain unearned.


## N2.2 bounded external API profile observation — 2026-11-06

The [closed profile64 observation](../evidence/native-epoch2-api-profile64-2026-11-06/observation-v1/index.json)
retains one control PASS (64.155s) and one instrumented PASS (84.509s) on the
same normal sync64 workload, 150/210/300 budgets and 1,600-file source hold.
The Principal's read-only audit validates the functional pair and selected API
capture while declaring attribution inconclusive. The instrumented API process
lifetime records 64.019836 process CPU seconds across all API threads and
75.97745 wall seconds; cProfile covers its main event-loop thread. Seven entries
have cumulative time below self time. The retained binary and derived tables
therefore do not establish SQL wait, API/DB latency, query counts, CI/b4 cause,
5000 forecasts or throughput/SLO performance. Both phases retain clean recorded
health and owned-process/database closure. This diagnostic observation earns
no new credit; epoch-2 readiness remains **15%**.


## N2.2 async5000, observed-count comparison and worker logging correction — 2026-11-06

The [closed d906 async5000 package](../evidence/native-epoch2-dag-volume5000-2026-11-06/async-v1/index.json)
records one actual native SDK/TCP/PostgreSQL17/OS case: **1 PASS / 2225.473s**,
with original test bytes and 3960/4080/4200 budgets. Source inputs 1–5000 and all three
branches have exactly 5000 calls/effects; four TaskRuns/four births/seven channels,
graph/coordinator release, physical closure and clean wider health are recorded.
The Principal independently audited files, not another native5k execution:
1968 progress records, 15000 calls/effect hashes, 1600 source bindings and 3866 import
records reconcile with no drift/mismatch; 11 captured PID/start identities closed.

The [same-profile async500↔async5000 comparison](../evidence/native-epoch2-dag-volume5000-2026-11-06/async500-comparison-v1/index.json)
now finds **13 observable metadata cardinality/role comparisons MATCH** at d906.
The independent file-only receipt binds hosted async500 PASS / 520.326s and local
async5000: four TaskRuns/subtasks/grants/launches/child PIDs, seven channels,
three consumers, A1/B1/C2 roles, epoch/run_count 1, four physical-slot flags and two
workers. Within-case identities are stable; IDs need not equal across runs.
Former pending entries stay historical. Sanitized CI metadata and its qualified
nonsecret authority_state restoration preserve source hashes. Its zero 5xx report
is an original collector aggregate, not independent HTTP replay; retry count is
unavailable. No CI operator/allocation/assignment SQL or external KV metrics,
throughput/SLO conclusion or forecast is inferred.

The [scoped A43-04/05/07 mapping](../evidence/native-epoch2-dag-volume5000-2026-11-06/async-v1/findings-and-criterion-map.json)
keeps limits explicit: 26 read-only PG captures start at 692/690/694 and stop
preterminal at 4888/4881/4894, not a retroactive500 checkpoint or final allocation
release SQL capture. [Cleanup attribution](../evidence/native-epoch2-dag-volume5000-2026-11-06/async-v1/qualifications/cleanup-attribution-v1/index.json)
proves two fresh scoped names absent afterward and zero sessions at the driver
check. Earlier conftest termination/DROP/CREATE was uninstrumented; driver deletion
of two existing databases or zero sessions before every fixture cleanup is not proved.

The [16-case logging probe](../evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/probe-v1/index.json)
and [real PG baseline](../evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/baseline-v1/index.json)
retain **1 FAIL / 6 PASS / 94.20s** under controlled CLI ordering: effective INFO (20),
handler present, Cloud/API enabled, sink=false and zero shipped DEBUG lines.
Two generated paths lacked before bindings; agreeing runtime/postrun hashes
are not retroactive pre-run proof. The retained fresh baseline DB has zero
observed sessions/no DROP/FORCE. Historical CI gw0 ordering/cause stays unknown.

The [closed corrected full module](../evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/actualpassed-module-v1/index.json)
is **151 PASS / 82.37s**, actual pytest0 and launcher/tool exit 0 persisted, with no
failure/error/skip or harness timeout/signal. The patch is test-only: 24+/6− in
three methods; production/default INFO/Cloud shipping gates are unchanged.
All 1756 source/generated bindings are prebound with zero drift; 1216 imports have
zero mismatch; all five new PID/start identities closed; the fresh DB is retained
with zero observed sessions/no DROP/FORCE. The [Principal peer receipt](../evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/actualpassed-module-v1/peer/closed-v4-file-review-v1.json)
is independent file-only review of author execution, not a personal151 rerun.
The [earlier151 attempt](../evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/failed-candidate-module-v1/index.json)
remains interrupted at 330s/exit143/no JUnit with a dead unreaped PID1 zombie.
The [recovered seven-body attempt](../evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/recovered-selected-v1/index.json)
retains 7 PASS / 12.53s but launcher exit 1/pytest waitUNKNOWN after a postwait wrapper
failure. Neither is rewritten as wrapper acceptance. The old orphan remains a
separate operational artifact, not a failure of successful v4/native5k closure.

The [last old-head CI snapshot](../evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/actualpassed-module-v1/root/old-ci-snapshot.json)
at04:49:28 Europe/Moscow (01:49:28 UTC) reports 13/19 required runtime cases PASS
on d906; 86 jobs success, one active, five queued, one skip and one known General
logging failure. Both sync recovery cases passed; async accepted200 was active,
async incomplete200/four heavy profiles queued. This historical snapshot is not
new-head green CI or acceptance of the unpublished test correction.
Other three local heavy profiles, full 19/four-heavy/NI2-014 gates and full ADR-0043
remain open. NI2-012/N2.2b remain `progress`; planned 27% is unearned and earned
readiness stays **15%**, with zero new credit.
