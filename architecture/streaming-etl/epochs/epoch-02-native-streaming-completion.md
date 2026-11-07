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


## N2.2 personal normal5000 pair and candidate P2 cleanup gates — 2026-11-06

At published 1812460, the [author sync5000 case](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync-v1/index.json)
is **1 PASS / 2090.278s** and the [Principal personal async5000 case](../evidence/native-epoch2-dag-volume5000-2026-11-06/principal-async-v1/index.json)
is **1 PASS / 1989.457s**. Both record actual pytest/tool exit 0 and no failure/
error/skip, unchanged original `[False-5000]`/`[True-5000]` tests and 3960/4080/4200
budgets. Each binds 1600 source files and 3884 imports with zero drift/mismatch,
exact 5000 effects in each of three branches and six wider-health counters 0.
The async result is actual personal SDK/TCP/PostgreSQL17/OS execution, distinct
from the earlier author/file-only results. Author native 11 PID/start identities
plus reader/runner closed; the personal async whole phase closes 14 captured
identities. Fresh databases remain retained with zero sessions at the driver
checks and no terminate/DROP/FORCE. Final readonly connectionclosure is not a
new post-final session-count query or proof about every prior framework action.


The [Principal personal sync5000 case](../evidence/native-epoch2-dag-volume5000-2026-11-06/principal-sync-v1/index.json)
now closes the other normal personal case: **1 PASS / 1974.770s / actual pytest 0/
tool 0**, with source 1600/import 3884 drift/mismatch0, 15000 effects, all six health
counters 0 and 14 phase PID/start identities closed. Its 28 live 19-family readonly
repeatable-read SQL captures begin 211/215/216 and end 4825/4827/4855, followed by
final 19/all 15 literal checks true. Final Consumer 0 rows is not retained 5000 cursor
proof; no exact 500/activation capture is inferred. Source captures remain late,
with the original closed-manifest/time qualification; actual reader/watch sources
and adaptation identify what ran. No active P2 material is included. The normal
personal async/sync5000 pair has two distinct closed actual SDK executions; it
still does not complete P2/four-heavy/current full 19/NI2-014 or earn 27%.


The [independent author-sync metadata](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync-v1/metadata-v1/index.json)
contains 30 live 18-family captures, final 18 and a separate Consumer transaction.
The personal async package contains 24 independently executed live 19-family
repeatable-read readonly captures including Consumer and one final 19-family
capture, all 25 connections/tools closed. Seventeen immutable mapping projections
are stable through the final observations; Consumer owner mapping is stable in
24 async live captures. Final SQL records graph/authority/coordinator RELEASED,
four Tasks COMPLETED/run_count 1, grants RETIRED/births CLOSED, four physical slots
and four tag allocations released, two persisted counters 0 and seven terminal
partitions released/completed at 5000. One persisted historical lease UUID remains;
external KV/backend state is unobserved. Final Consumer queries return 0 rows,
which cannot prove retained literal 5000 Consumer cursors. Separate author-sync
final 18/Consumer transactions do not establish combined atomicity.

Author-sync co-sampled direct-PG labels begin 51/51/51 and end 4718/4727/4714;
personal async actual independent SQL cursors begin 794/795/795 and end 4874/4898/4888.
Later file guards>=4900 are not additional SQL captures at 4900. No activation or
exact 500 snapshot is inferred. Author progress/accepted/final helper snapshots
are direct PostgreSQL ordinary multi-SELECT observations, neither API reads nor
the independent atomic 19-family captures. The [erratum](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync-v1/qualifications/progress-source-provenance-v1/erratum.json)
and Principal adoption preserve that qualification beside old immutable wording.
Personal async binds 1734 author progress frames; the 11 later source copies match
closed source-after and retain capture-time qualification. The old sealed observer
namespace assertion was not changed/executed; separate actual reader/watch sources
and the adaptation receipt identify what ran. Each full history is stored once.

The [current normal sync500 comparison](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-comparison-v1/index.json)
finds **13 normalized observable counts/roles MATCH** between current CI sync500
PASS 539.420s and author sync5000. Principal FILE-ONLY review binds 16 safe CI files
and 1818 author progress frames. CI birth/PID/source/call values are hash-bound
derived aggregates; absent raw CI markers/full history are not independently
replayed. The v1 worker_id_count KeyError and earlier artifact-review grant-state
assumption remain preserved as review/harness errors, not workload failures or
artifact repair. Literal grants are RETIRED. Reported API 5xx0 over 7547 statuslines
is an aggregate, not independent HTTP replay; retry count is unavailable.
No missing CI operator/assignment/allocation SQL or externalKV metrics are inferred.

The [personal85/hosted bridge](../evidence/native-epoch2-dag-volume5000-2026-11-06/routing85-bridge-v1/index.json)
retains 85 personal PASS 17.910s and 85 current hosted PASS 17.482s, same case-set and
codec/test hashes. Four Python minors/four seeds/literal routing goldens support
A43-06/10 at recorded OS/compute-instance scope. Parent sys.modules audit is absent;
PYTHONPATH/child source-SHA assertions qualify imports. Sixteen child invocations
are source-derived, without per-child PID manifests. Different recorded managed-
Cloud/GitHub-hosted instances do not attest physical hosts, ADR47, native process
loss or network behavior. ZIP is digest-only; private inputs/download URLs excluded.

A43-04/05/07 gain sampled identity, scoped process and persisted release support;
these observations do not complete all criteria or all intervening transitions.
The remaining P2 bridge and current
[CI 42147494934](https://github.com/Anacreonsystems/ariadne/actions/runs/42147494934)
full 19/four-heavy/NI2-014 exits remain open; old d906 13/19 is historical. No whole
current CI green/ADR43 result, throughput/SLO/cause/forecast or planned 27% credit.
NI2-012/N2.2b remain `progress`; earned readiness stays **15%**, zero new credit.
All original source/raw inputs/approved ADR body+AC/history and old indices remain
unchanged; no active personal-sync artifacts enter these archives.


### P2 baseline: functional PASS with full cleanup incomplete

The immutable personal fixed-P2 incomplete200 baseline native receipt
`f8c5c21e106f492367590820714dd3f15ed400d28ac2ad9ee1a5266be820aa12`
records **1 PASS / 102.962s / pytest 0/tool 0**, while its actual postterminal SQL
`15f9c83cd98411755a066bfffec8b1517352cccc9257d1f66f17a99ced3bbdf9`
has **9 true / 3 false**, tool 1/connection closed. The sealed STOP
`dae848461ee30e4837d603bca8ad3fa347f3d7d8b61f90ff762a88dd2c22aa74`
retains all actors gone and `full_automatic_cleanup_accepted=false`.
Physical/tag/deployment release and scoped recovery facts remain valid: four
Tasks complete (B0 count 2, others 1), B0 physical epoch 2, five grants/births,
four subtask slots/tag allocations freed and selected active counters 0. The
three full cleanup predicates fail with graph ADMITTED/authority TERMINAL/
launch PROCESS_REGISTERED and partitions.released=false. They are valid fixed-P2
automatic-release requirements, not optional output retention or harmless copied
DAG assertions. This is an OPEN automatic coordinator/payload cleanup result.

Read-only classification `f02d962fbfb13d967ed9dcb09d346bb8c45cd124f5654f534d099236ecf868a1`
compares the accepted lifecycle and explains that the legacy fixture stops workers
before awaiting their separate post-process-wait release phase. About 273ms from
final Flow timestamp to recorded worker stop and no completed access response for
the exact release endpoint support teardown preemption; no exact cancellation/
kill trace or new production regression is established. Retained forensic DBs
are not repaired, and old outcomes/indices remain unchanged.

The first UUID wait proposal remains unexecuted `REQUEST_CHANGES` in its
[separate lineage receipt](../evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/separate-unexecuted-UUID-preflight-request-changes/review.json).
The [closed P2 candidate package](../evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/index.json)
records actual personal native sync incomplete200 **1 PASS / 101.022s**, with
501 body calls/500 effects and replacement first 200; async accepted200 is
**1 PASS / 103.467s**, 500 calls/500 effects and first 201. Each independently
captured 19-family repeatable-read [sync](../evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/sync_incomplete200/independent-SQL/postterminal-capture.json)/
[async](../evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/async_accepted200/independent-SQL/postterminal-capture.json)
SQL snapshot has all 12 literal replacement/physical/full automatic-release
checks true. The new guard observes natural release before worker termination,
within original 840/900 budgets, without manual release or database repair.
The [PG selected review](../evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/pg_selected/principal-PG19-closed-review-v1.json)
records 19 real-PG ASGI/model controls PASS / 49.016s/0 skip; that tier is not a
native TCP pipeline. All three phases have pytest/tool 0, zero failure/error/skip,
1600 candidate source bindings drift 0, native imports 4173 percase/PG imports 1202
mismatch 0 and closed owned actors. Fresh DBs remain retained with driver zero-
session checks; final readonly connections close separately, without a new
post-final session-count query. 17 source captures retain actual owner timing.

The old native baseline/SQL 9 true/3 false remains unchanged in its separate prefix;
new helper assertions do not retroactively attach to that execution. Both named
positive P2 cleanup profiles now have fresh evidence. Normalheavy/stall cleanup
guards and excluded after-activation/import-exit/before-declaration profiles
receive no new execution credit from this pair. The packaging-preparation v1
filename-classification assertion and corrected helper remain distinct from
runtime failures; public testcase selectors are not private DB selectors.

The [selected-file normal hooks](../evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/closed-publication-hooks/hooks-receipt-v1.json)
actually exited 0 at pre-commit and pre-push with candidate source unchanged.
They are selected-file checks, not whole-publication hooks or current/future CI.
The exact version is 1812460 plus test-only patch4550f35d5db3b4ddc74160f55354d33165551d1cd82d33695ee687402621b0b7,
P2 module 61ebfdfaac8e394c08e8156f0ab94a0483701f6932b3bd83020c39d70d1ade71.
The [closed source-relevance bridge](../evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/version-accounting-source-bridge/review.md)
compares 1600 files total:
1599 unchanged and one modified test. Old actual DAG/CI jobs ran 1812460, not this
WIP or a future commit. The DAG module imports execution_api from the modified
P2 module, so full imported-module byte equality is not claimed. Its fixture/
decorator/globals are unchanged and new P2 helper/polling is not invoked by the
DAG fixture; this narrow dependency bridge supports reuse of original version-
attributed evidence without renaming or replaying those executions.

[The existing final exit](#n22-retained-verification-and-finalization-correction--2026-11-05)
keeps NI2-012 through NI2-014 OPEN until corrected native sync/async5000, terminal
release, the required hosted matrix and independent verification are all met.
Bootstrap/retry implementation evidence may be VERIFIED; that does not accept
these tasks while CI is pending. Root's current 1812460 matrix is 15/19, with the
four-heavy/full hosted exits open, not candidate-head CI green. No task-accepted
or zero-credit subscope shortcut is applied. Earned 15%; planned 27% and full
NI2-014/ADR43 completion remain open. No approved ADR body/AC is changed.


## N2.2 current CI sync5000 failure and original-AC block — 2026-11-06

The [closed failed CI case](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/index.json)
binds published 1812460 run 42147494934/job127663650911, original sync `[False-5000]`:
**1 FAIL / 0 ERROR / 0 SKIP**, testcase 4014.625s. Flow FAILED where COMPLETED was
required. The [original 18-file metadata index](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/metadata/original-verify-index.json)
retains `underlying_cause=UNKNOWN` byte-exact; no old receipt or execution is renamed.

The later [bounded diagnostic receipt](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/diagnostics/diagnostic-receipt.json)
records four sanitized excerpts/source hashes: **SDK_DAG_WAIT_TIMEOUT after 3960.31s**
at 05:22:14.207UTC, same-graph `/fail` HTTP200 at 05:22:15.299UTC, then 409
`CANCEL_REQUESTED` Task refusals and three retry-scheduling messages. Actual
control failure_kind/payload is not retained; scheduling does not prove executed
retries/new processes/epochs. The proximate timeout→terminal control→fencing path
is observed. The underlying slow-progress/throughput/resource bottleneck is
still **UNKNOWN**; no CPU/quota/OOM/lock/data-corruption or SLO/forecast cause is
established. The original wait 3960/outer 4080 deadlines remain source-bound.

Source accepted 5000. Last-polled output positions 3178/3206/3179 are separate from
call-record counts 3178/3206/3180; their one-record C1 difference does not prove
loss or retry. Graph RELEASED with FAILED Flow/channels is not successful execution.
Last polling records are not final-progress/accepted/coordinator/worker-health
reports; six dedicated reports are missing and are not backfilled from local
PASS. Actual HTTP retries/operator-allocation SQL and whole worker health remain
unknown. API 5xx0 is a reported aggregate, not independent HTTP replay.

The [later one-shot closed status](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/supplements/ci-status-053618-v1/index.json)
at 05:36:18.437UTC records **15 native PASS / 1 known FAIL / 1 ACTIVE / 2 QUEUED**
on 1812460, with DAG async5000 active since 05:26:07 and P2 sync/async5000 queued.
No additional RED or newer snapshot is inferred; only the exact public closed
index/snapshot digests are archived, without raw API response/ZIP/log export.

The [Principal original-AC BLOCKED map](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/principal/original-AC-component-status-BLOCKED-v1.json)
keeps NI2-012/013 implementation verification scoped and their formal tasks OPEN;
NI2-014 is BLOCKED by actual required hosted RED/fullmatrix unfinished. NI2-020
implementation GO remains false. The existing final exit at
[NI2-012 through NI2-014](#n22-retained-verification-and-finalization-correction--2026-11-05)
requires corrected native/release/hosted/independent evidence together. Actual
local normal5000/P2/PG19 results and the narrow imported-fixture bridge retain
their original clean18124/WIP versions; they do not supply hosted PASS or task
acceptance. No exhaustive case cross-product or zero-subscope workaround is added.
Full 19/four-heavy/NI2-014/wholeADR/planned 27% remain open; earned readiness stays
**15%**, zero new credit. All approved ADR bodies/AC, historical evidence and
existing closed indices remain unchanged.


The later [independent Principal source audit](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/supplements/principal-classification-v1/index.json)
reconciles all 18 metadata copies, four bounded excerpts and four published
source bindings. It independently confirms SDK wait timeout→error cleanup
`/fail`→secondary fencing, while underlying slow-progress/resource cause remains
UNKNOWN. This is read-only source/file classification with no new test, DB,
ZIP/network query or acceptance; original metadata/status/AC-map indices remain
immutable at their capture times.


## N2.2 bounded sync500 CONTROL/INSTRUMENTED observations — 2026-11-06

The [closed diagnostic 500 pair](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-measurement-pair-v1/index.json)
contains two personally executed normal sync500 phases: CONTROL 1 PASS 218.526s
and INSTRUMENTED 1 PASS 250.317s, both actual pytest/tool 0/0 failure/error/skip.
All 1600 runtime and 636 publication files remain identical, 3884 imports have no
mismatch, six wider-health counters are zero, provision hook 1/original body 0,
fresh databases retained with sessions[] at driver checks and 13 owned identities
closed per phase. Original 750/840/900 budgets remain. These are diagnostic 500
runs, not personal 5000, a performance SLO or hosted-gate acceptance.

Selected whole API process/all-thread CPU is 189.345343s control and 222.174119s
instrumented. Instrumented API lifetime is 241.237177977s wall. These observed
counter boundaries are not request/component CPU attribution or calibrated
instrumentation overhead. Local/hosted resources differ; no CI cause or 5000
forecast follows. Process samples are not exit totals; missing CPU is unknown.

The byte-exact [public structured ledger](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-measurement-pair-v1/instrumented/api-measurements.json)
and [derived reconciliation](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-measurement-pair-v1/principal-derived/derived-api-counts-v1.json)
record 181310 balanced selected SQLAlchemy Engine dispatch attempts/5868 requests,
0 errors/0 pending. These are not all PostgreSQL statements or necessarily successful
server queries. ASGI ContextVar grouping can include awaited/inherited background
work until the request row closes; **outside means absent or closed request
context**. This exact definition in the [Principal review](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-measurement-pair-v1/principal-review/principal-pair-closed-review-v1.json)
also clarifies the archive wrapper's compact “no/open request-context” wording.
The synchronous getter 8008 calls use 2.073158677s thread CPU, a small observed share
of whole API CPU. `_dag_plan` 4104 calls total 151.159770104s inclusive **wall**
across awaits, not CPU/critical path. Summed SQL client-wall 596.685020727s exceeds
API wall 241.237s because concurrent spans overlap and include driver/network/server/
scheduling. Overlapping SQL/request/plan spans cannot be added as disjoint latency.

No getter cache/reuse, optimization or proof/lifecycle change was implemented.
The getter returns a fresh mutable DTO. Unexecuted V1 field/range/hook preparation
defects remain preserved separately from successful runtime phases. The required
CI sync5000 RED and underlying slow-progress cause UNKNOWN are not fixed by these
observations. NI2-012…014 remain OPEN/BLOCKED, NI2-020 implementation blocked;
earned readiness **15%**, planned 27% unearned. Existing ADR bodies/AC/history,
all earlier indices and the current publication delta remain unchanged.


## N2.2 closed projection diagnostics, JOIN PG68 and CI03 status — 2026-11-06

The [closed TaskRun projection source package](../evidence/native-epoch2-dag-volume5000-2026-11-06/taskrun-projection-v1/index.json)
preserves rejected/unexecuted V1 and approved V2, Root application/normal selected
hooks, proposed preformat test b016 and actual posthook 27c1 plus immutable final
three-file captures. V2 retains nine mapped TaskRun fields/proofs and explicitly
refreshes mutable project_execution rows; no DTO cache/reuse/lock/validation skip.
The unsafe-V1 activation FAIL and old-product 4 PASS remain source-derived
unexecuted expectations. [Actual PG23](../evidence/native-epoch2-dag-volume5000-2026-11-06/taskrun-projection-pg23-v1/index.json)
is 23 PASS/55.516s/0 skip, including four real stale-identity/freshness cases plus
19 previous scoped cases; source 1601/import 1209 drift/mismatch 0. It is real
PG/model/API-ASGI correctness, not a native 5000/performance result.

The [POSTFIX500 pair/comparison](../evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-measurement-postfix-pair-v1/index.json)
records personal CONTROL 1 PASS 212.262s/API wholeCPU185.085536s and INSTRUMENTED
1 PASS 227.587s/API wholeCPU198.606205s on that exact source 1601, publication 636
unchanged, 3884 imports/no mismatch/six health0/provision1original0, retainedDB
sessions[] at driverchecks and 13 identities closed each. Eight observers are
byte-identical to BEFORE. Old public 50MB metrics are referenced, not copied again.
Matched DATA dispatch histograms are complete47→46, nonemptyread39→38,
emptyread31→30 and sourceappend41→40: **-1 per matched request, 3507 saved**.
Total 181310→175522 saves 5788 but includes requestmix/poll differences; it is not
all attributed to projection. Single-run times/CPU differences are descriptive,
not calibrated causal speedup, CI benefit, SLO or 5000forecast. SQL/request/plan
spans overlap; inclusive wall is not CPU/pure SQL latency or a critical path.

The later [bounded reference-JOIN source](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-bounded-join-v1/index.json)
is a distinct model/test source version: manifest bf5dc647…/review 66348187…/
hooks 0888e041…, model a540…/test ccdb…, source1601 a0f2…. It bundles six LEFTJOINs
with local scalar facts and refreshes three parent ORM aliases; fresh topology
rejection/eager parent refresh on failure are declared strengthening, not blanket
identity-map sideeffect equivalence. Existing contents() equality is a business
projection, not all DBcolumns/wholeDB unchanged. No cache/newlock/proof weakening.
[Actual JOIN PG68](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-bounded-join-pg68-v1/index.json)
is **67 PASS / 1 exact SQLite-only SKIP / 147.025s**, pytest/tool0, 1601 source
no drift, 1209 imports/no mismatch/provision1original0/ECHILD/five identities gone/
retainedDB driver sessions[]. The only skip is
`test_sqlite_discovery_authorization_refuses_before_any_mutation`, reason
`SQLite backend-refusal control` on realPG17. Six new JOIN cases passed. This is
route45+disjointPG23/model/API-ASGI correctness, not native/performance acceptance.
The future JOIN CONTROL/instrumented500 pair is **PENDING**; active trial files
were not read or copied and no PASS/dispatch/CPU benefit is invented for it.
Closed projection/POSTFIX results remain their original earlier source versions.

The [published 18124 async CI5000 failure](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-async5000-failure-v1/index.json)
retains 18 safe files, actual Flow FAILED vs COMPLETED/1 FAIL. Primary cause is
NOT_RETAINED and underlying bottleneck UNKNOWN; the separate sync SDK 3960.31s
timeout cause is not transferred to async. Secondary fencing/aggregate line
counts do not establish primary cause, executed retries or complete health.
[Six safe resource JSONs](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-cpu-wall-pairing-v1/index.json)
retain arithmetic CPU deltas, but genuinely paired same-cgroup CPU/wall is
NOT_OBSERVED: start-wall timestamp and cgroup identity/membership are missing.
No API/PG/host attribution, resource-cause or causal timing conclusion follows.

[CI02 as-of 07:09:39.284](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-status-070939-v1/index.json)
is preserved separately from [CI03 as-of 07:33:36.420UTC](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-status-0733-v1/index.json):
**16 native PASS / 2 known DAG5000 FAIL / physical async5000 ACTIVE**, all on OLD
published 1812460. Physical sync5000 whole job succeeded 07:14:53, exact runtime
06:37:06→07:14:37. This is job/step status only, no artifact download/health replay
or proof of the new cleanup guard. No newer matrix state is inferred. Old CI does
not become projection/JOIN/futurecommit CI. Full 19/four-heavy/NI2-012…014 remains
OPEN/BLOCKED, NI2-020 implementation blocked, 15% earned/no 27% credit; no new gate
or decision is introduced. Original ADR42–48 approved bodies/AC/history and all
closed artifacts/indices remain unchanged.


## N2.2 terminal old18124 CI04 matrix — 2026-11-06

[CI04 exact public ledger and declared snapshot](../evidence/native-epoch2-dag-volume5000-2026-11-06/ci-status-075802-v1/index.json)
at 07:58:02.148UTC closes all 94 jobs (91SUCCESS/2knownFAIL/1SKIP) and all 19 native
cases: **17 PASS / 2 known DAG5000 FAIL**, no active/queued cases. P2async5000
wholejob SUCCESS 07:53:02, runtime 07:18:09→07:52:47/no-skips-validatorSUCCESS;
P2sync wholejob success remains in CI03. These are OLD 1812460 job/step statuses,
not artifacts/health replay, new cleanup-guard execution or candidate/futureheadCI.
CI02/03 and failure/cause/resource history above remain immutable. The terminal
required matrix is **not green**; no further active or queued status is implied.

Source components are verified only in their declared scopes/versions. Later JOIN
PG68 is **67 PASS / 1 exact SQLite-only backend SKIP / 147.025s**. The JOIN500
measurement pair remains **PENDING** until its own closed owner handoff; no
performance result or fixedCI claim is borrowed from prior POSTFIX500. Current
tracker rows are concise; detailed causality/proof/version limits remain in the
appended history and linked packets. NI2-012…014 remain OPEN/BLOCKED, NI2-020
implementation blocked, earned readiness **15%**, planned 27% unearned. Existing
approvedADR42–48 bodies/AC and all earlier evidence/history remain unchanged.


## N2.2 closed JOIN500 diagnostic pair: dispatch reduction without speedup — 2026-11-06

The [closed JOIN500 pair](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-measurement-pair-v1/index.json)
now records actual CONTROL 1 PASS 223.458s/APIwholeCPU196.472938s and INSTRUMENTED
1 PASS 293.313s/APIwholeCPU262.308499s, both pytest/tool 0/all six health0/source1601a0f2/
publication636 unchanged/import3884 no mismatch/provision1original0/retainedDB
driver sessions[]/all 13 phase identities closed each. No active file was copied.
The [exact comparison](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-measurement-pair-v1/principal-derived/POSTFIX-JOIN-comparison-v1.json)
confirms nonempty-read38→32 and complete46→40 for 1500 matched requests each,
**18000 matched dispatches saved**; sourceappend40/emptyread30 are unchanged.
Total 175522→160132 decreases 15390, with requestmix/background differences not all
assigned to JOIN. The new ledger 160132 before/after balances 0 errors/0 pending;
old baseline rawmetrics are linked by SHA rather than recopied.

Observed CONTROL suite/API CPU is **slower by 5.27%/6.15%** than POSTFIX; treated
suite/API CPU is **slower by 28.88%/32.07%**. These are individual descriptive
comparisons, not demonstrated speedup or calibrated causal regression. Reason
for slower work remains UNKNOWN; no SQL compilation/materialization/cache/resource
cause is established. Selected API Engine attempts are not all/successful PG
statements. SQL/request/dag_plan elapsed spans overlap and are not CPU/pure SQL
latency/critical path. Same local resources do not establish hostedCI parity or
5k forecast. Retain actual functional PASS and slower observations together;
no automatic source erasure or whole-ADR acceptance/rejection follows.

The separate new COUNTERS-ONLY diagnostic is source-only preparing/**NOT EXECUTED**;
its source/trial files are not read or archived here. PG68 remains 67 PASS/1 exact
SQLite-only backendSKIP. OLD 18124 CI all 19 terminal17 PASS/2DAG5kFAIL is still not
green; neither dispatch reduction nor local500PASS fixes required hosted gates.
NI2-012…014 OPEN/BLOCKED, NI2-020 implementation blocked, earned **15%**, planned 27%
unearned. No approved ADR body/AC/history or older index was rewritten.


## N2.2 closed counters-only diagnostic: residual API CPU unknown — 2026-11-06

The earlier COUNTERS-ONLY preparation now has one actual [closed diagnostic 500](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-api-sync-counters-v1/index.json):
**1 PASS/224.438s/API wholeCPU196.481766s**, source1601a0f2/publication636 unchanged,
3884 imports/no mismatch/sixhealth0/provision1original0/retainedDB driver sessions[]/
all 13 owned identities gone. No SQL/ASGI ledger/global profiler/cache was used.
Exact [counters](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-api-sync-counters-v1/run/api-sync-cpu-counters.json)
show alias6011/.196471s, Select121231/2.695267s, compiler4762/.560284s,
validated4057/2.180657s, submission16224/1.477574s, pure reference3000/.915598s
(selected inclusive threadCPU). Originalexceptions0/observererrors0/active0.

**Inclusive stages may nest and are not summed.** Compiler constructions are not
engine cache misses; Select covers constructor/column coercion, not generative
JOIN/WHERE/cache-key work. Wholeprocess CPU includes observation overhead; six
selected stages are not a complete profile. Other ORM/result adaptation, JSON/
digests and scheduling remain unattributed. Pureasync helper CPU depends on its
pinned no-suspension/direct-sync-callee proof. These costs do not justify a
shape-cache fix or explain the prior slowdown; **residual CPU remains UNKNOWN**.

The [Principal review](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-api-sync-counters-v1/principal/principal-counters-closed-review-v1.json)
preserves the failed file-audit object/digest assertion as preparation, not an
actual test failure/product drift. A new standard nonblocking statistical profile
is only source-feasibility/**PENDING, NOT EXECUTED**; no source/trial is read here.
All prior JOIN/source/pass/slower-measurement and CI failure histories remain.
No CI fix/SLO/5000forecast, NI2-012…014 acceptance, NI2-020 GO or 27% credit follows;
earned readiness **15%**, old CI17PASS/2DAG5kFAIL/notgreen/NI014blocked. No approved
ADR body/AC/history or older index was rewritten.


## N2.2 closed py-spy500 and JOIN integration rollback — 2026-11-06

The previously pending statistical profile now has one actual [closed personal
py-spy500 diagnostic](../evidence/native-epoch2-dag-volume5000-2026-11-06/api-pyspy500-v1/index.json):
**1 PASS/224.973s/API whole-process CPU197.759607s**, historical JOIN
source1601a0f2/publication636 unchanged/import3884 no mismatch/sixhealth0/all14
owned identities gone/fresh database retained with driver sessions[]. The sampler
selected one verified API owner for fixed120s/49Hz nonblocking recording; actual
`wait4` exit0/resource CPU8.317530s is sampler accounting, not four seconds.
The [raw profile](../evidence/native-epoch2-dag-volume5000-2026-11-06/api-pyspy500-v1/run/api-profile.raw)
is preserved once (2,226,954 bytes). The [descriptive summary](../evidence/native-epoch2-dag-volume5000-2026-11-06/api-pyspy500-v1/principal/descriptive-stack-summary-v1.json)
records **3748 weights = 3609 nonempty + 139 EMPTY**, and **1899 separate
unreadable errors**. Weights are not CPU seconds; the error population cannot
be divided by trace weights to invent a loss rate. Empty observations remain
unknown; high read loss/non-atomic reads and truncated Python/native ancestry
can bias attribution. Inclusive stack counts overlap and are not summed.
Verdict: **DESCRIPTIVE_ONLY / HIGH_LOSS; whole-API CPU attribution INCONCLUSIVE**.
No exact CPU percentage, residual bottleneck, cache justification, latency/SLO,
CI cause or 5000 forecast follows. The [Principal review](../evidence/native-epoch2-dag-volume5000-2026-11-06/api-pyspy500-v1/principal/principal-pyspy500-closed-review-v1.json)
preserves hold487a phase-flag assertion as **UNEXECUTED preparation** before any
trial/DB/Popen, followed by separately authorized holdab245 with only the phase
flag corrected; there was one actual native attempt.

Root rejected the bounded reference JOIN optimization **for current integration**
because no benefit was demonstrated. The [closed exact two-file rollback](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-rollback-v1/index.json)
restored modela4acc3d4/testf7efcd77 and source1601**012a2d9d**, retaining P2 cleanup
and TaskRun projection. The [final owner hooks receipt](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-rollback-v1/owner/hooks-receipt.json)
records normal selected pre-commit0/pre-push0/no source or AST mutation, publication636
unchanged and PRIMARY clean18124. The earlier apply receipt's HOOKS_PENDING status
is preserved as its historical stage. All candidate source/tests, PG68 PASS, JOIN500
slower pair, counters and py-spy evidence remain on their actual **a0f2** source;
they are not relabeled as rollback-source tests. This integration decision is
not rejection of ADR43 or its approved requirements. Restored-source PG62 remains
preparation/**NOT EXECUTED** here; CI-runner DAG500 diagnostics are source-planning/
**NOT RUN**. No post-rollback functional/performance PASS is inferred.

OLD published18124 CI remains all19 terminal **17 PASS/2 DAG5000 FAIL**, not green
and not rollback/candidate CI. NI2-012…014 remain OPEN/BLOCKED, NI2-020 implementation
blocked, earned readiness**15%**, planned27% unearned. Prior requirements/history,
failed/preparation records, approved ADR42–48 bodies/AC and all closed indices
remain immutable. Conflict scan remains incomplete; grill-me/spec-first variants
remain excluded. No repo/source/Git-index/runtime/DB/hooks/network action was
performed by this evidence/draft preparation.


## N2.2 restored-source PG62 component verification — 2026-11-06

The [closed restored-source PG62 packet](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-reverted-pg62-v1/index.json)
now records the Principal's actual model/API-ASGI real-PG17 component execution:
**61 PASS / 1 exact SQLite-only backend SKIP / 136.786s**, pytest/tool exit0.
The [personal review](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-reverted-pg62-v1/principal/principal-PG62-closed-review-v1.json)
and [actual receipt](../evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-reverted-pg62-v1/run/pg62-receipt-v1.json)
bind source1601**012a2d9d** before/after equal, 1209 import audit rows/no mismatch,
provisioning-only hook1/original provisioning body0, all five observed identities
gone/waitpid ECHILD, fresh retained DB with driver sessions[]. The only skip is
`test_sqlite_discovery_authorization_refuses_before_any_mutation`, reason
`SQLite backend-refusal control`. The first file-only allowlist draft's reference
to nonexistent publication-before/after captures is preserved as preparation-only
correction, not a workload failure or invented before/after capture; the actual
publication guard is bound in hold/runner/receipt.

This closes the prior preparation-only status **within restored-source component
verification**, without claiming native SDK/TCP 500/5000, performance or hostedCI
acceptance. JOIN remains excluded from current integration for no demonstrated
benefit; exact a4acc/f7ef rollback, retained P2 cleanup/TaskRun projection and all
historical candidate a0f2 code/tests/PG68/slower pair/counters/high-loss py-spy
records remain distinct and immutable. PG62 is not retrospective validation of
those candidate measurements. CI-runner DAG500 diagnostics remain source-only/
**NOT RUN**. OLD published18124 CI terminal17PASS/2DAG5000FAIL remains not green
and is not current rollback-source CI. NI2-012…014 OPEN/BLOCKED, NI2-020
implementation blocked, earned15%, planned27% unearned; no new readiness credit.
No approved ADR42–48 body/AC/history, old index or prior draft was rewritten.


## N2.2 local DAG500 observer rehearsal and reviewed CI diagnostics — 2026-11-06

The [closed two-file diagnostic implementation](../evidence/native-epoch2-dag-volume5000-2026-11-06/dag500-ci-observer-implementation-v1/index.json)
preserves final workflow499d58a3/plugin a25be925 source, exact patch, bounded
plan/matching Principal reviews, normal selected hooks0/0 and pure transport/
extraction/lifecycle checks. It adds explicit DAG500sync/async choices and a
DAG-profile observer branch while retaining legacy P2 behavior, original500
test nodes/750-840-900 budgets and ordinary CI fixtures. Those source/pure
checks alone are not SQL, native or hostedCI acceptance.

The [actual closed LOCAL rehearsal](../evidence/native-epoch2-dag-volume5000-2026-11-06/dag500-observer-rehearsal-v1/index.json)
now records Principal personal SDK/TCP/OS/PG17 execution: **1 PASS/0 SKIP/226.097s**,
pytest/tool0; source1603 before/after equal = legacy1601**012a2d9d** plus explicit
workflow/plugin extras, 3885 import rows/no mismatch. Local provisioning-only
hook1/original provisioning body0 differs from the future ordinary CI fixture.
B0/C0/C1 each have500 calls/500 effects; all six native health categories empty,
all13 observed owned identities gone/ECHILD, fresh retainedDB driver sessions[].
The [observer audit](../evidence/native-epoch2-dag-volume5000-2026-11-06/dag500-observer-rehearsal-v1/run/DAG-observer-audit.json)
preserves **45 snapshots/41 complete4Task7partition snapshots/all11 query families**,
with one owned Flow/Graph and at least one actual all-three-frontiers500 sample.
Exactly one preGraph/bootstrap **UndefinedTableError** is retained with its
connection confirmed closed; later queries succeeded. **This is not a claim of
zero observer errors.** Final cleanup absence and normal-case control/action/
message UNKNOWN remain literal; no retrospective durable cursor or actual
failed-case cause capture is invented.

Actual checkpoint84.07843ms acknowledged, active0/unconfirmed closes0/threadSTOP;
these observations and cooperative6s-query/1s-close limits are not strict future
real-time guarantees. Snapshots are default-readonly multiSELECT/**NONATOMIC**,
not RR. Ten paired same-PID/start CPU windows are partial observed windows, not
exit/lifetime CPU, SQL/RPC latency or critical path; cgroup totals can include
other processes. Same runner label does not establish physical-host/resource
parity or CI slowdown cause. Exact100 rehearsal inputs are accounted once:88
physical copies+12 hash-bound canonical references to the CI implementation
sibling, keeping shared499d/a25 sources in one copy. The [personal review](../evidence/native-epoch2-dag-volume5000-2026-11-06/dag500-observer-rehearsal-v1/principal/principal-rehearsal-closed-review-v1.json)
and [minimal dispatch guard](../evidence/native-epoch2-dag-volume5000-2026-11-06/dag500-observer-rehearsal-v1/principal/minimal-CI-dispatch-guard-v1.json)
preserve the execution boundary. The static ephemeral PostgreSQL fixture URI
in source YAML is the explicitly classified public literal, not private runtime
DSN/env/config; no unrelated URL exception follows.

**The new diagnostic workflow has NOT RUN ON CI.** Local rehearsal is not oldCI
reproduction, a fixed CI bottleneck, performance/5000 acceptance or permission
for automatic dispatch/async/heavy runs. OLD18124 native matrix remains all19
terminal17PASS/2DAG5000FAIL, distinct from this candidate source. NI2-012…014
OPEN/BLOCKED, NI2-020 implementation blocked, earned15%, planned27% unearned.
TaskRun projection/P2 cleanup retained/JOIN reverted; all historical candidate
code/tests/slower measurements/profiles/failures and prior V1…V6 appendices
remain exact. No approved ADR42–48 body/AC/history or closed index was rewritten.
Conflict scan incomplete; grill-me/spec-first variants excluded.


## N2.2 published candidate and one hosted DAG500 sync diagnostic — 2026-11-06

The [compact closed publication/run bundle](../evidence/native-epoch2-dag-volume5000-2026-11-06/published-dag500-sync-status-v1/index.json)
records normal configured commit/push hooks0 and publication **b0328b395ff997c6ed50f2c73e3eb650af8c48c2**
on native-stream-storage/PR25 draft. Descendant **95fb3e73de66bccfc141fbb9fda22b64cf42e131**
adds only two hostedPG17 selection lines for four projection regressions; test
bodies/budgets/dependencies/lock/archives/docs and diagnostic499d/a25 are unchanged.
The old V7 `NOT RUN ON CI` statements remain exact as-of history.

One manually dispatched [hosted DAG500 sync run42206832141, attempt1/job127855515745](https://github.com/Anacreonsystems/ariadne/actions/runs/42206832141/job/127855515745)
finished SUCCESS on runner `anacreon-runners-runner-kjwvb` at **2026-11-06 14:19:00
Europe/Moscow (11:19:00 UTC)**. It executed **b0328**, not later95fb. The exact
numeric-safe terminal summary records **1 PASS/676.29s** and separate observer
validation **SUCCESS/130 samples/123 complete**. No second dispatch/rerun/cancel
or test-budget change is inferred. CPU/PG/health/artifact diagnosis awaits its
own closed owner handoff; this summary bundle did not download/replay artifacts
or establish zero observer errors, throughput/resource cause, complete PID/DB
closure or a performance result. Snapshot/cleanup-absence limits remain.

This is one hosted500 diagnostic, not fullmatrix/heavy5000/NI2-012…014 acceptance,
95fb Unit/Static outcome or closure of the historical18124 DAG5000 failures.
Latest95fb automatic checks are a separate evidence stream; no newer check state
or green matrix is inferred from this b0328 run. NI2-012…014 OPEN/BLOCKED, NI2-020
implementation blocked, earned**15%**, planned27% unearned. All prior evidence/
source versions, approved ADR42–48 bodies/AC/history and closed indices remain
unchanged; conflict scan incomplete, grill-me/spec-first variants excluded.


## N2.2 95fb CI status, scoped purge correction and readiness diagnostic controls — 2026-11-06

The [finite correction/control packet](../evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-ci-correction-controls-v1/index.json)
keeps actual95fb Unit failures separate from the earlier single b0328 hosted500
PASS and from the current tested-but-unpublished candidate. At **14:56:48 Moscow
(11:56:48 UTC)** the closed94job snapshot was76success/4knownFAIL/1skip/1active/
12queued; at **15:10:06 Moscow (12:10:06 UTC)** it was79success/4sameFAIL/1skip/
1active/9queued, native7success/2sameFAIL/1active/9queued. No new failure or green
matrix is inferred. Root's triage distinguishes hostedAPI20s readiness failures
(physicalSQLite/syncnormal; cause ambiguous), rebalance64 nonterminal partial
observation, and canonical fixtureFK failure. Static's10minute job-timeout
annotation is separate; patchc43f8614/workflow6405101 changes only that job10→20,
not functional/API/watchdog budgets, test commands, hookSKIP or requirements.
The packet preserves source proofs and compact indices, not original failed-job
rawlogs or fullsnapshot replays.

Exact test patch**04019041** extracts existing canonical purge ordering for
fixture/filter setup+teardown/history callers; local utilityDBBase tables are
excluded and production ORM/FKs unchanged. AuthorPG17 baseline **RED1PASS/
1ERROR11.10s/exit1** with named restrictiveFK remains intact. AuthorSQLiteGREEN
is4PASS; [Principal personally executed PG17GREEN](../evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-ci-correction-controls-v1/inputs/native-95fb-purge-principal-verification-v1/personal-pg17-review-v1.json)
is**4PASS/0SKIP14.00s** (JUnit14.35), source1604 no drift/import1201 no mismatch/
provision1original0/ownedprocesses gone/freshretainedDB0sessions. Successful
negative tests catch/assert FK refusal and caller rollback/commit semantics;
printed-log `expected_restrictive_FK_observed=false` is not a failed caught-FK
assertion. The pre-review comparator-name assertion remains preparation-only.
This is scoped regression evidence, not fullmodule/hosted acceptance.

Startup diagnostics preserve proposal1a0bbbcf/bc723627 and actual tested module/
tests**3a32ff38/a44a6f2d** distinctly. Only allowlisted failure metadata is added;
**20s**,200+owned-header predicate, URL/proxy rules and owned cleanup remain.
Author actual6PASS used driver**84d6d46b**, but inherited assets.json lists old
unused**23b46e97**: **it does not prebind executed84d**. The disclosed harness
driver-binding limit is separate from actual module/source/import/closure
checks; author functionalPASS is retained with that qualification.
[Principal independent personal controls](../evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-ci-correction-controls-v1/inputs/native-95fb-readiness-principal-checks-v1/closed-personal-review-v1/review.json)
correctly prebound driver**93c3209f** and passed**6/43.09s/tool0** on the same
tested sources,1604 drift0/import3244 mismatch0/provision1original0/eight owned
identities gone/ECHILD/freshretainedDB0sessions. Actual owned503 was20.021s/
189attempts/header-match=true/cancel=true, child460168 cleanup exit−15. Real
foreign-listener refusal/two owned APIs are distinct from three transport/timing
metadata doubles. This verifies diagnostics/control behavior; the hosted startup
and rebalance underlying causes remain**UNKNOWN**, with no startup fix claim.

The earlier compact b0328 CPU/PG-review-pending note remains history; the now
[closed independent numeric verdict](../evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-ci-correction-controls-v1/inputs/native-hosted-DAG500-principal-b0328-v1/numeric-verdict-v1.json)
and [local comparison](../evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-ci-correction-controls-v1/inputs/native-hosted-DAG500-principal-b0328-v1/local-CI-route-comparison-v1.json)
are preserved as two smallJSONs plus source-index references, without copying
all artifact/history. Observed API535.63CPU/653.633pairedwall is a partial
samePID/start window. PG lock rows/edges/ClientRead are samples, not wait
durations/incidents or proven request/OSPID critical-path attribution. Two
bootstrap observer errors remain; never observer0errors. Matching DATA route
counts do not equal matching overall work/background mix; access-log lifetime
and CPU windows differ. No debugbuild/WAL/cache/lock-removal remedy, resource
causality or5000 forecast follows. Any next profiling is a separate**plan-only/
NOT RUN**, without evidence credit; prior executed historical profiles remain.

Prior V7 and compact four-input b0328→95fb status, all failed/passed/version
histories and approved ADR42–48 bodies/AC remain exact. No candidate hostedCI
result/fullmatrix/heavy5000/NI2-012…014 acceptance; tasks OPEN/BLOCKED, NI2-020
implementation blocked, earned**15%**, planned27% unearned. Conflict scan remains
incomplete; all grill-me/spec-first variants excluded.


## N2.2 post-hook import source bridge and exact lint policy — 2026-11-06

The [tiny closed post-hook supplement](../evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-posthook-source-bridge-v1/index.json)
preserves the failed first normal commit (exit1/1.283s/HEAD95fb). NormalRuffI001
reordered imports only in filters/history/testing-fixtures tests. Author and
independent Principal compare final bytes to actual preformat tested captures:
**same import multiset/all other moduleAST identical**. The seven original
tested captures/receipts remain exact; three sorted final files are bridged by
source analysis, **not relabeled as newly executed**. No runtime/DB rerun.

The preserved machine numeric verdict triggered codespell; exact patch43d0707
adds only its one full path as the third existing codespell skip. Pyproject9fe69183
is otherwise raw/parsed-TOML identical to61b6; sealed verdict ee3ae1ad/archives
and uv.lock305a remain unchanged. No global ignore/hookSKIP/dependency or
functional-budget change. Pyproject is a policy-only eighth source path, not a
newly tested runtime change; final publication must capture the new source1601
digest, not claim all old inventory bytes unchanged. Normal retry/publication
outcomes remain separate. Priorf628/V7/compact history and all qualifications
remain intact;15%earned/NI2-012…014OPEN/BLOCKED/NI2-020blocked/27unearned.


## N2.2 final compact lint-policy qualification — 2026-11-06

The [compact final lint supplement](../evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-final-lint-policy-v1/index.json)
retains second normal-commit failure: exit1/1.281s/HEAD95fb/no mutations. Three
immutable captures of existing lint configuration triggered it; none was edited.
Root authorized only their three exact additional skip paths. Compact JSON and
independent Principal review bind pyproject9fe→63942325, all other raw/parsedTOML
identical, seven code paths/uv.lock305a/prior archive bytes unchanged. The earlier
third-skip note remains historical; this is a further policy-only extension.
No new rawconfig/patchcontext/source copy, recursive capture or runtime/DB test;
normal retry/publication outcomes remain separate. f628/3981/tested captures/
history remain exact;15%earned/NI2-012…014OPEN/BLOCKED/NI2-020blocked/27unearned.


## N2.2 0213 profile consistency and CI as-of status — 2026-11-06

The [compact closed history packet](../evidence/native-epoch2-dag-volume5000-2026-11-06/0213-profile-consistency-v1/index.json)
records publication0213b24fefdd3561b95d8dadf356482c89c67903 with normal Git/hooks/
push0, source160461c665 and exact155path guard; published three-doc bytes match
the prior reviewed snapshots. Closed CI **as-of16:14:04 Europe/Moscow (13:14:04UTC)**
has94Unit jobs75success/0observedFAIL/3active/15queued/1skip; native19 has3success/
1active/15queued. Static2/2SUCCESS is separate (Root-reported precommit12m13
within20m). This is not terminal/current fullgreen. Old95 prepush JIT fiveFAIL,
including True500 **PRIMARY_NOT_RETAINED**, stays on95fb; retained secondary
cleanup/fencing observations do not reconstruct its primary cause.

Principal's one actual0213 False500 native case remains **PASS**: owner-reported
console254.84s/safeJUnit255.178s,1604source drift0/import3884 mismatch0/1500effects/
sixhealthlists empty/release and observedPID closure/retainedDB0sessions. Only
the explicit public functional derivative is copied; raw receipt/header is
excluded. Structural profile capture20.008wall/19.668main-threadCPU is separate
from timing attribution:2757functions/13negative self/14cum<self/negative summed
self and impossible roughly19k-second self entry make attribution **INCONCLUSIVE**.
The independent audit corrects the earlier main-thread-only assumption: this
exact Python3.12 backend uses process-global monitoring/shared profiler context
with per-thread clocks; no product/network/ORM hotspot or regression is inferred.

Author88c audit remains historical; correction25b7 binds actual executablefa6744
and probe7ccf, with installed shared library only corroboration. Four closed
benign scripts/four results and independent111bf/ba578 falsify even a shared
process-clock substitution: nonnegative/conserved totals still misattribute
per-function CPU. No corrected native profile exists. Raw512KBpstats/all21micro
stats remain references, not recopied; reports use exact-byte `.log` captures.
Yappi is separate benign investigation/pending, **not native or product work**
in this packet. No source fix, CI cause, performance/SLO, heavy5000/fullmatrix
or acceptance credit; NI2-012…014OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned.
All earlier ADR42–48 bodies/AC/history/packets remain exact; conflict scan
incomplete and grill-me/spec-first variants excluded.


## N2.2 bounded P2 EOF rendezvous and DAG failure visibility — 2026-11-06

The [closed finite substantive packet](../evidence/native-epoch2-dag-volume5000-2026-11-06/0213-p2-eof-dag-visibility-v1/index.json)
keeps old published0213 CI separate from the uncommitted four-file candidate.
Closedv6 asof17:33:57Moscow had83success/2FAIL/1active/7queued/1skip; v7 asof
**18:12:36Moscow (15:12:36UTC)** has83success/**4CIjobFAIL**/1active/5queued/1skip,
native19=10success/3CIjobFAIL/1active/5queued. Static2PASS is prior; four5000
cases remain queued. Distinct dispositions: physical fixture90s EOF issue has
the local fixedscope below; DAGTrue500838.57 primary remainsNOT_RETAINED,
visibility only; Falseaccepted200893.64 outer terminal assertion has primary
UNKNOWN; Falseincomplete200 is **runner-lost-communication CIjobFAIL**, actual
pytest exit/assertionNOT_OBSERVED/log404, not proof of test/product failure.
No current/fullgreen or underlying infrastructure-cause claim follows.

Confirmed P2 source-fixture90s Timeout led to samePIDattempt2, not productextra
Birth. RawHH:MM trace labels have no embedded date/TZ and are not converted to
proved Moscow time. Proposal f722/actual executed diff4a2 retain lineage; actual
92d395/dd589/sourcee44 bind ONE same-host monotonic finite840/3540 deadline
atomically beforePOST, parentanchor strictly earlier. Original numeric budgets/
assertions stay unchanged. Author three profiles PASS102.066/100.852/114.127,
replacementfirst201/200/201/fivebirths500effects/fullrelease/source+import+PID+
retainedDB closure valid. Principal personally repeats existingasyncaccepted
PASS ownerconsole100.21/JUnit100.569: **3unique profiles/4actual executions**.
The nonexistent fourth selector is0collected/0body preparation, no newcoverage.
ConfiguredRuff0.15.19PASS remains distinct from shared0.16.2's four baseline
findings85c08; no no-qa or broader source waiver.

DAGvisibility proposal00c53/actualcombined76fc/source20c adds one bounded failure
JSON/collectorfilename only: no normalpoll/DB/schema/publicAPI or original
domain-oracle change. Actual pre-DB collection verifies original pytest markers
**300/300/180**: lifecycleFalse-failure testPASS31.700 (intentionalFlowFAILED,
ValueError+ownedgraphUUID); lifecycleTrue-normal PASS35.434/no failurefile;
PrincipalpresealTrue PASS25.84/JUnit26.198 (ValueError/nullgraph/asyncframe,
all14executioncounts0/coordinatorRELEASED/original rejected-health0). Source/
imports/PID/ECHILD/retained0sessions closure passes within each version. Author
13FS+collectorCLI0 and Principal30FS helper checks are separate from those three
native executions; no actualSDKwaittimeout experiment. Mandatory44e0 qualifies
P2e44→20c relevance: DAGmodule absence is audited, but collector is **outside
importauditprefix**; its relevance is command/callsite review, not zero-row
import proof. Earlier P2 runs are not relabeled as execution on20c.

Prior273c/b09d history stays exact, including firstcProfile nativePASS/timing
INVALID; its pendingYappi note is followed by now-closed **YappiFAIL297.43** on
old0213/source61c665: stricthealth/twoAPI500/APIexit−9, noYSTAT/finalmetadata,
restorationUNOBSERVABLE. Partial functional/release facts do not override FAIL.
No nativePASS, CPUattribution, product hotspot or accepted profiler/host cause;
no retry/profile work authorized by this evidence. All earlier versions,
approvedADR42–48 bodies/AC/history and packets remain immutable. NI2-012…014
OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned, no full19/fourheavy/SourceGate
acceptance; conflict scan incomplete/grill-me/spec-first variants excluded.


## N2.2 single-leaf cursor latch and scheduling cap — 2026-11-06

The [small closed fixture/cap proof](../evidence/native-epoch2-dag-volume5000-2026-11-06/ed892-leaf-latch-cap-v1/index.json)
binds publisheded892 plus exactthree-file candidate/e624: DAGtest9f7 holds only
B's final acceptance while SourceA/healthyC reach EOF; CImaxparallel1→3 and its
existing selection guard change, production/DB protocol/matrix/budgets/oracles
unchanged. Authorrebalance64 PASS54.296 and syncincomplete200 PASS219.818;
Principal personalrebalance PASS54.10/JUnit54.451 and asyncaccepted200
PASS216.30/JUnit216.745. Allfour pytest/tool0/no skips, source1604drift0/import
mismatch0/originalhealth+retirement+release checks pass, retainedDB sessions[];
all50knownPID/start identities gone. Captures witness A/Ccomplete/Bheld with
threefrontiers32/32/64 or500/500/500. Syncincomplete retains replay; asyncaccepted
resumes201/no replay/C500once. Original150/210/300 and750/840/900 budgets stay.

Eight real-filesystem/stub-domain latch controls pass; selection9PASS/actionlint0
are direct observed output with no persisted log (external shellcheck/pyflakes
disabled). Olded892CIrebalance FAIL250.39/proximalSDKwaitTimeout and underlying
noncompletionUNKNOWN remain historical. Localfixture-overlap verification is
not CPU/product/host-cause proof, hostedCIgreen, performance/SLO or full19/fourheavy
acceptance. Rawreceipts/ownedheaders/source/config/profiler copies excluded;
prior history remains exact. NI2-012…014OPEN/BLOCKED/NI2-020blocked/15%earned/
27unearned; approvedADR42–48 bodies/AC unchanged, conflict scan incomplete and
grill-me/spec-first variants excluded.


## N2.2 Worker renewal contention and bounded infrastructure corrections — 2026-11-07

[Closed component](../evidence/native-epoch2-worker-renewal-2026-11-07/verification.json)
binds `cd80536` plus patch `94b0408a`. PostgreSQL reproduces Worker SHARE/UPDATE
contention; same-identity renewal now uses SHARE while registration/rotation retain
exclusive fencing. Author RED1/GREEN10 and personal PG10 preserve identity,
occupancy and expiry. Test API startup 60s and named-emitter logging have personal
29 PASS/3 legitimate skips, with the raw wrapper exit1 scope correction explicit.

Frozen native author async normal500 265.73s, sync accepted200 220.41s/start201 and
personal async incomplete200 266.42s/start200 PASS. Only unfinished input 200 replays;
healthy C branches each execute 500 once. Original 750/840/900 budgets, health/
release, 1599-file guards, imports, PID/ECHILD and retained-DB closure pass. Normal
intermediate C1 RUNNING is distinct from verified terminal completion; the earlier
metadata-drift baseline remains diagnostic only.

Old cd80536 CI: 82 SUCCESS/11 FAIL/1 SKIP; native 10 SUCCESS/9 FAIL stays unchanged.
No hosted/whole-timeout/performance credit; current local 5000/full 19/four-heavy gates
remain open. NI2-012…014 OPEN/BLOCKED, NI2-020 blocked, 15% earned/27% unearned.
Approved ADR42–48 bodies/criteria and prior failures remain unchanged.


## N2.2 Signed RUNNING contention and frozen recovery proof — 2026-11-07

[Component proof](../evidence/native-epoch2-running-event-2026-11-07/verification.json)
binds `232a855` plus `b9d387d3`. The retained hosted C0 event timed out on
FlowRun UPDATE before body entry; six births closed before epoch 7. Only canonical
signed singleton RUNNING now shares both global prefixes; TaskRun UPDATE, CAS,
fences and all terminal/default exclusive paths remain. Author PG RED/GREEN,
personal existing PG scope: 63 PASS and actual SQLite control: 1 PASS verify this;
the unfiltered wrong-backend 45 PASS/1 FAIL remains retained.

Frozen native author sync/async normal 500 and personal sync accepted200/async
incomplete200 PASS: JUnit cases 241.132/254.083/232.784/253.370s. Accepted input resumes 201;
only unfinished 200 replays; C branches each run 500 once. Original 750/840/900,
health/release, 1599-file/import guards and PID/ECHILD/retained-DB closure pass.
Old 232 sync/async 5000 also PASS after correcting the owned PG stand 512MiB→2GiB,
with zero new OOM kills; those passes do not execute this RUNNING change. Initial
OOM-affected FAIL/interruption remain retained. Current 232 snapshot 14:38:51 UTC has 83 SUCCESS/6 FAIL/3 RUNNING/1 QUEUED/1 SKIP
(native 9/6/3/1); all six failures and old cd80536 eleven remain. Only the first
three current failures were inspected; no cause is assigned to the other three.
Full19/four-heavy gates open.
NI2-012…014 OPEN/BLOCKED, NI2-020 blocked, 15% earned/27% unearned; ADR bodies intact.
