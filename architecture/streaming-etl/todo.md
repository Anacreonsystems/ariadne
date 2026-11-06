# Streaming ETL task registry

This registry slices the [ADR graph](adr/README.md) into delivery tasks. Task
completion does not make an ADR `accepted`; evidence for every criterion in
the declared ADR scope must be linked back to it.

Current direction: [Native integration epoch 2](epochs/epoch-02-native-streaming-completion.md)
implements the explicitly approved [ADR-0042](adr/ADR-0042-flink-informed-native-streaming-architecture.md)–[ADR-0048](adr/ADR-0048-rescaling-compatibility-and-native-acceptance.md)
set. The user approved version `fae53c90aa3b44dbc45032833b4d193001e5c8bd`
on 2026-11-04; the [approval record](evidence/flink-architecture-2026-11-04/epoch2-work-approval-2026-11-04.json)
binds the decision bytes. The original broader objective remains unfinished.
The independently verified fixed-P2 slice earns **15%** of the epoch-2 target;
branched DAG, state, continuous input, cross-host recovery and rescale remain
open.

Historical foundation: [Native integration epoch 1](epochs/epoch-01-native-prefect.md)
is `accepted`, with **100% earned native foundation readiness** for the selected scope. Its [first-stage contract](adr/ADR-0032-native-task-stream-contract.md)
is accepted for its native Task SDK/engine scope with real HTTP/PostgreSQL
checks. The first sprint earned its planned 10% share after the complete
[Stage 1 gate](evidence/native-stage1-acceptance-2026-11-01.json) passed.
The [CI integration successor](evidence/native-stage1-ci-acceptance-2026-11-01.json)
binds the final code revision, complete runtime reruns, full type check and
client wheel/source build; failed intermediate observations are retained.
The [N1.2 gate](evidence/native-stage2-closure-acceptance-2026-11-01.json) accepts the
native storage-only slice. [ADR-0034](adr/ADR-0034-native-stream-http-adapter.md)
accepts the HTTP/admission slice through the
[final native transport gate](evidence/native-stage3-acceptance-2026-11-02.json).
That earns N1.3's planned +10%. The native source SDK is now accepted under
[ADR-0035](adr/ADR-0035-native-source-task-runtime.md) and its
[source gate](evidence/native-stage4-acceptance-2026-11-02.json), earning N1.4's
next planned +10%. Bounded whole-flow reconstruction is now accepted in
[ADR-0036](adr/ADR-0036-durable-submission-recovery.md) after the
[bounded design agreement](evidence/native-stage5-design-review-2026-11-03.md)
and [final acceptance](evidence/native-stage5c-acceptance-2026-11-03.json),
earning N1.5's planned +10% to reach 50%.

The separate DS-052a follow-up is now `done` under
[ADR-0030](adr/ADR-0030-continuous-arc-soak-and-evidence-collection.md): the
[actual campaign](https://github.com/Anacreonsystems/ariadne/actions/runs/40672223609)
completed on 2026-10-31 with 18 passing segments, original long review, fresh
collector and publication. The [completion record](evidence/ci-arc-72h-completed-40672223609.json)
and [published-artifact audit](evidence/ci-arc-72h-artifact-audit-40672223609.json)
close its criteria. [ADR-0031](adr/ADR-0031-validated-durable-stream-theory.md)
accepts the tested prototype theory. This lane adds no percentage to either the
historical short delivery or the new native implementation scale.

Delivery states are separate from ADR statuses. Sizes assume `S` ≤ 2 engineer
days, `M` = 3–4 days, and `L` ≈ one engineer week. `H` and `M` denote high and
medium delivery risk.

## Native epoch 2 — full implementation registry

ADR-0042 is an extending umbrella, not a blocking dependency. Task dependency
edges start at the completed ADR-0032–ADR-0040 foundation and proceed through
ADR-0043–ADR-0048. Stable `Axx-yy` IDs refer to the unchanged criterion mapping
appended to each approved ADR.

### N2.1 — Fixed physical execution (`+15%`, earned `15%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-001 | `done` | ADR-0032…0040, A43-01 | M/H | Immutable graph revision, logical operator and physical subtask/grant topology with paired migrations. Reuse existing StreamPartition/Record/Consumer/Completion and ordinary TaskRun; no second data plane. |
| NI2-002 | `done` | NI2-001 | M/H | Freeze reserve/seal/read-plan and subtask claim/register/renew/replacement server DTO/API with secret-free projection. A child gets only an exact subtask credential, never Basic/Flow-wide/admin authority. A43-01, A43-05, A43-07. |
| NI2-003 | `done` | NI2-001, NI2-002 | L/H | Extend existing append/read/complete/status, task-event and task-lease guards with graph/assignment/subtask/attempt authority. Scope the former one-source restriction only for `partitioned-fixed-v1`; preserve `bounded-linear-v1`. Reserve the runnable P2 region before any source starts. A43-03, A43-05, A43-08. |
| NI2-004 | `done` | approved ADR-0043 | S/M | Internal `tagged-key-v1` exact codec/group mapping and cross-runtime/hash-seed golden controls; [independent component verification](evidence/flink-architecture-2026-11-04/ni2-004-principal-verification-2026-11-04.json) passed 85/85 without skips. No public wire, admission or readiness claim yet. A43-06, A43-10. |
| NI2-005 | `done` | NI2-002, NI2-003, NI2-004 | L/H | Thin source/operator subtask subprocess bootstrap over existing TaskRun engines and ProcessHandle/pidfd. P2 has A0/A1 source and B0/B1 operator TaskRuns, each with existing output partitions; one Flow lease and ordinary task/tag leases. A43-04, A43-05, A43-07, A43-08. |
| NI2-006 | `done` | NI2-001…005 | M/H | Real SDK/HTTP/PostgreSQL P2 gate: sync/async, 500/5000 inputs, B0 loss, healthy A0/A1/B1, 200/201 recovery, cross-subtask capability and slot-starvation controls, SQLite fail-closed, finite regressions, packaging and [Principal acceptance](evidence/native-epoch2-n2-1-principal-acceptance-2026-11-05.json). A42-01, A43-01, A43-03…08, A43-10. |

### N2.2 — Branched DAG and durable exchanges (`+12%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-010 | `accepted` | NI2-006 | M/H | Seal named typed ports, DAG revision and acyclic topology; reject cycles, duplicate subscribers and unsupported edges before user code. The [Principal acceptance](evidence/native-epoch2-ni2-010-principal-acceptance-2026-11-05/acceptance.json) closes the metadata-only component with zero earned epoch percentage; public execution remains fenced. A43-02 remains partial until NI2-012…014. |
| NI2-011 | `accepted` | NI2-010 | L/H | Materialize bounded forward/rebalance/broadcast/keyed routes in the existing terminal transaction, with independent edge partitions/cursors/pins. The [Principal acceptance](evidence/native-epoch2-routed-server-2026-11-05/principal-v1/acceptance.json) closes the finite storage component with zero earned epoch percentage; public DAG execution remains fenced until NI2-012. A43-03 and A43-09 remain partial. |
| NI2-012 | `progress` | NI2-010, NI2-011 | M/H | SDK/client/worker graph bootstrap and fan-out with ordinary Task definitions and constant fixed-graph metadata. Source components verified within declared versions; local normal5k/P2 retained. JOIN excluded from current integration for no demonstrated benefit; rollback source012a/hooks0/0, P2/TaskRun projection retained; restored-source PG62: 61 PASS/1 exact SQLite SKIP. Historical JOIN measurements/PG68/profile stay on a0f2. OLD18124 CI terminal17 PASS/2DAG5kFAIL; task OPEN. A42-02/A43-02/04 and scoped A43-04/05/07 retained; 15% earned/27% unearned/NI2-020blocked. |
| NI2-013 | `progress` | NI2-011, NI2-012 | M/H | Exact append/read retry and branch replacement with immutable route digest, different partition counts and no cross-branch acknowledgement. A43-03, A43-09. Scoped implementation/native/P2 versions retained; JOIN excluded for no demonstrated benefit, rollback source012a/hooks0/0 retains P2/TaskRun projection; restored-source PG62: 61 PASS/1 exact SQLite SKIP. Historical JOIN PG68/500/counters/py-spy remain distinct; profile attribution INCONCLUSIVE. OLD18124 CI terminal17 PASS/2DAG5kFAIL; task OPEN/required matrix not green; 15% earned/27% unearned/NI2-020blocked. |
| NI2-014 | `progress` | NI2-010…013 | M/H | Real branched native process gate and independent Principal verification on frozen source. A43-02, A43-03, A43-09. Source components verified by declared version; JOIN excluded for no demonstrated benefit, rollback source012a/hooks0/0 with P2/TaskRun projection retained. Historical JOIN PG68/500/profile on a0f2; restored-source PG62: 61 PASS/1 exact SQLite SKIP; CI-runner diagnostics source-only/NOT RUN. OLD18124 CI all19 terminal17 PASS/2DAG5kFAIL; NI2-014 BLOCKED. Earned15%/27%unearned/NI2-020blocked. |

### N2.3 — Multi-input, journaled state and correlated join (`+15%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-020 | `planned` | NI2-014 | L/H | Native state batches/postimages, stable logical receipt identity, derived materialization and fail-closed contiguous fold. A44-01, A44-02, A44-07. |
| NI2-021 | `planned` | NI2-020 | L/H | Tagged multi-input mailbox/inbox, completion holes and one durable global apply order. A44-03, A44-06. |
| NI2-022 | `planned` | NI2-020, NI2-021 | M/H | Versioned OUTPUT(value)/NO_OUTPUT with old epoch-1 state-only RETURN decoding unchanged. A44-08. |
| NI2-023 | `planned` | NI2-021, NI2-022 | L/H | First one-to-one correlated keyed join with duplicate, unmatched, late and end policies plus bounded unmatched state. A42-02, A44-05. |
| NI2-024 | `planned` | NI2-020, NI2-021 | L/H | Ordered barrier participant manifests, state/timer roots, immutable COMPLETE cuts and complete-only restore/GC authority. A44-06, A44-09. |
| NI2-025 | `planned` | NI2-020…024 | M/H | Real SDK/HTTP/PostgreSQL/process join/state matrix and Principal verification, including 200/201 and no accepted-body replay. A42-03, A44-01…09. |

### N2.4 — Continuous sources, event time and timers (`+13%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-030 | `planned` | NI2-025 | L/H | Durable source enumerator, split assignment, connector position/rewind/retention admission and native readers. A45-01, A45-03. |
| NI2-031 | `planned` | NI2-030 | L/H | Ordered DATA/WATERMARK/IDLE/RESUME/EOF/DRAIN entries, read permits and exact append-intent reconciliation. A45-01, A45-07. |
| NI2-032 | `planned` | NI2-030, NI2-031 | M/H | Atomic enumerator-frontier drain, all-split EOF and finite tail exactly once. A45-02, A45-06, A45-07. |
| NI2-033 | `planned` | NI2-025, NI2-031 | M/H | Watermark minimum, idleness/reactivation, skew alignment and explicit late-input outcomes in the keyed join. A45-04, A45-06. |
| NI2-034 | `planned` | NI2-020, NI2-033 | M/H | Journaled timer register/cancel/fire identities, overdue policy and no accepted callback replay. A45-05, A45-06. |
| NI2-035 | `planned` | NI2-030…034 | M/H | Real continuous source/drain/event-time/timer process gate and independent Principal verification. A42-04, A45-01…07. |

### N2.5 — Backpressure and online retention (`+12%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-040 | `planned` | NI2-014, NI2-025, NI2-035 | L/H | Full-turn reservation vectors for data/routes/state/join/timer/control with exact conversion/refund/reclamation and control headroom. A46-03, A46-08. |
| NI2-041 | `planned` | NI2-040 | M/H | Durable fair capacity wait/notification and upstream pressure integrated with region/task/tag/global admission. A46-02, A46-06, A46-07. |
| NI2-042 | `planned` | NI2-040, NI2-041 | L/H | Online segment/receipt retirement, completion holes, subscriber/state/recovery/rescale pins and explicit horizon tombstones. A46-04, A46-05. |
| NI2-043 | `planned` | NI2-042 | M/H | Object-reference GC follows every durable log/state/join/recovery root; timeout/watermark alone cannot delete. A46-05. |
| NI2-044 | `planned` | NI2-041, NI2-042 | M/M | Pressure/lag/reservation/pin/GC projections distinguish idle, busy, backpressured and failed states. A46-06. |
| NI2-045 | `planned` | NI2-040…044 | L/H | PostgreSQL turnover, paused branch, owner loss, WAL/index/vacuum and storage-plateau gate with independent Principal verification. A42-04, A46-01…08. |

### N2.6 — Selective and verified cross-host recovery (`+13%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-050 | `planned` | NI2-025, NI2-045 | M/H | Compute immutable recovery region and pin accepted inputs/state/retention roots without restarting unaffected subtasks. A47-01, A47-02, A47-06. |
| NI2-051 | `planned` | NI2-050 | L/H | RESTORING target plans and one atomic READY activation; stale coordinator/assignment/subtask writes and mixed activation are refused. A47-03, A47-07. |
| NI2-052 | `planned` | NI2-051 | M/H | Same-host selective replacement proves B-only loss with healthy siblings, bounded budgets, cancellation and one Flow lease. A47-01, A47-02, A47-03. |
| NI2-053 | `planned` | NI2-051 | L/H | Real two-host placement with provider/old-host verified stop, portable package/state/object references and strict no-timeout-as-death admission. A47-04, A47-05. |
| NI2-054 | `planned` | NI2-050…053 | M/H | Region-expansion, shared-process loss, unavailable partition and cancellation races with exact explanations. A47-05, A47-06, A47-07. |
| NI2-055 | `planned` | NI2-050…054 | M/H | Source-bound same-host and actual two-host fault matrix plus independent Principal verification. A42-05, A47-01…07. |

The [local QEMU boot discovery](evidence/native-epoch2-qemu-readiness-2026-11-05/boot-discovery-v1/index.json)
is retained as `UNPROVEN_RESOURCE_GUARD`: two guests booted, but the resource
ceiling stopped them before the required overlap and guest identity witnesses.
It earns zero credit and leaves NI2-053 `planned`.

### N2.7 — Rescale and compatibility (`+10%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-060 | `planned` | NI2-035, NI2-045, NI2-055 | L/H | Complete transfer manifest for source/control/mailbox/state/timer/capacity/pins/capabilities and immutable cut. A48-07. |
| NI2-061 | `planned` | NI2-060 | L/H | Two-to-three subtask transfer, whole key-group movement, atomic owner/pin/reservation activation and old-writer fence. A48-01, A48-02. |
| NI2-062 | `planned` | NI2-061 | M/H | Native API/SDK/worker rescale lifecycle and logical operator/old TaskRun retirement projections. A48-01, A48-07. |
| NI2-063 | `planned` | NI2-060, NI2-062 | M/H | Package/state/router/source-codec compatibility and explicit supported migration/refusal before user code. A48-03. |
| NI2-064 | `planned` | NI2-061…063 | M/H | Fault every transfer/activation boundary and prove one complete old or new assignment, never mixed. A48-02, A48-07. |
| NI2-065 | `planned` | NI2-060…064 | M/H | Real rescale/compatibility gate on frozen source with independent Principal verification. A42-03, A48-01…03, A48-07. |

### N2.8 — Full native profile acceptance (`+10%`)

| ID | State | Depends | Size/risk | Deliverable and mapped criteria |
| --- | --- | --- | --- | --- |
| NI2-070 | `planned` | NI2-014, NI2-025, NI2-035, NI2-045, NI2-055, NI2-065 | L/H | End-to-end source → fan-out → keyed join → sink through ordinary SDK/server/worker paths with continuous input and stable identities. A42-02. |
| NI2-071 | `planned` | NI2-070 | L/H | Declared load/skew/record-size workload, storage plateau, throughput/latency/recovery/catch-up measurements and explicit missed limits. A48-05. |
| NI2-072 | `planned` | NI2-070 | M/H | Finite/native foundation regressions, paired migrations, supported Python/client package, mixed-version and unsupported-profile controls. A43-01, A48-03. |
| NI2-073 | `planned` | NI2-070, NI2-071 | M/M | Operator/UI/runbook projections expose logical/physical state, pressure, recovery, rescale, limits and evidence without capabilities or payloads. A42-01, A48-05. |
| NI2-074 | `planned` | NI2-070…073 | L/H | Frozen complete matrix: per-turn faults, drain/cancel, pressure/GC, regional/two-host recovery, rescale, compatibility and bounds. A42-02…05, A48-01…05, A48-07. |
| NI2-075 | `planned` | NI2-074 | M/H | Architect personally reruns every applicable AC, validates source/evidence graph and only then moves individually proven ADRs to `accepted`. A48-04, A48-06. |

The eight sprint shares total 100% of the new epoch-2 profile problem. A task
state never earns readiness by itself; each sprint remains at its prior earned
target until its final Principal gate passes. Cross-host work requires a real
provider/agent stop-witness capability; containers on one kernel are not a
substitute.

## Native epoch 1 — Stage 1 implementation

| ID | State | Depends | Deliverable and acceptance criteria |
| --- | --- | --- | --- |
| NI1-001 | `done` | [ADR-0032](adr/ADR-0032-native-task-stream-contract.md) | Native Task configuration, typed Stream/collection dependencies and `submit_stream()`. As a developer, I can inspect the stream edge while ordinary finite entrypoints and copying retain their contracts. |
| NI1-002 | `done` | NI1-001 | Sync/async per-input execution inside one TaskRun. As a user, I can recover input 200 or 201 on the same TaskRun according to durable terminal acceptance. |
| NI1-003 | `done` | NI1-001, NI1-002 | Real HTTP/PostgreSQL acceptance, negative cases and full finite-task regression. As a system, I got constant TaskRun/state metadata at 1 and 1000 records and no unsupported admission. |
| NI1-004 | `done` | NI1-003 | Independent architecture review, client-wheel smoke and evidence/ADR closure. As a developer, I can reproduce the tested source and distinguish Stage 1 from native persistence/deployment readiness. |

## Native epoch 1 — Stage 2 delivery

| ID | State | Depends | Deliverable and acceptance criteria |
| --- | --- | --- | --- |
| NI1-010 | `done` | NI1-004 | [ADR-0033](adr/ADR-0033-native-durable-stream-storage.md), bounded native storage and retention agreement. As a developer, I can distinguish storage-only acceptance from HTTP/runtime admission. |
| NI1-011 | `done` | NI1-010 | Four native ORM models and paired migrations. As a system, I got actual dialect upgrade/downgrade and round-trip fidelity. |
| NI1-012 | `done` | NI1-011 | Atomic fenced store, quotas and whole-flow release. As a user, I can recover the incomplete input or durable completion without partial output/state/progress. |
| NI1-013 | `done` | NI1-012 | Independent hostile review and frozen real PostgreSQL gates, including implicit FK lock ordering and owned test APIs. As a developer, I can reproduce code hashes, failures and ordinary-flow/database regression. |
| NI1-020 | `done` | NI1-013 | Review [ADR-0034](adr/ADR-0034-native-stream-http-adapter.md). As a developer, I can identify safe HTTP admission and metadata-lag behavior. |
| NI1-021 | `done` | NI1-020 | Native API and client adapter. As a user, I can use platform storage without DB credentials in the worker. |
| NI1-022 | `done` | NI1-021 | [Transport acceptance](evidence/native-stage3-acceptance-2026-11-02.json) and packaging. As a system, I got verified remote recovery with retained identities, not only local model tests; whole-flow/worker reconstruction remains the next runtime stage. |

## Native epoch 1 — Stage 3 source delivery

| ID | State | Depends | Deliverable and acceptance criteria |
| --- | --- | --- | --- |
| NI1-030 | `done` | NI1-022 | Review [ADR-0035](adr/ADR-0035-native-source-task-runtime.md). As a developer, I can distinguish one generator per source attempt from one terminal event per operator input, and task retry from whole-worker recovery. |
| NI1-031 | `done` | NI1-030 | Source reservation, atomic admission-position response, exact EOF and client API. As a system, I can bind a source before a worker starts and fence stale append/finish requests. |
| NI1-032 | `done` | NI1-030, NI1-031 | Native source configuration, submission and sync/async generator loop. As a user, I can receive positioned references while one ordinary source TaskRun stays active and retry from durable source progress. |
| NI1-033 | `done` | NI1-031, NI1-032 | Real PostgreSQL/HTTP source failure and regression gates. As a user, I got input 200 or 201 according to durable acceptance, safe early delivery and no record-sized orchestration history. |
| NI1-034 | `done` | NI1-033 | [Principal review](evidence/native-stage4-principal-2026-11-02.json), frozen source evidence, packaging and required CI selection/gate integration. As a developer, I can reproduce N1.4's bounded source gate and identify the deferred whole-worker/GC scope; hosted successor CI is reported separately. |

## Native epoch 1 — Stage 4 durable submission and recovery

The combined N1.5 gate earned its planned +10%, reaching 50%. Identity and fencing slices
earn no percentage separately. All tasks trace to
[ADR-0036](adr/ADR-0036-durable-submission-recovery.md).

| ID | State | Depends | Deliverable and acceptance criteria |
| --- | --- | --- | --- |
| NI1-040 | `done` | NI1-034 | Bounded architecture agreement. As a developer, I can trace identity, retry, generation and infrastructure ownership to the design review. |
| NI1-041 | `done` | NI1-040 | Atomic manifest, TaskRun and reserved output with paired migrations and HTTP/client DTOs. As a system, I can exactly replay a lost creation response without duplicate identities or partial rows. [N1.5a acceptance](evidence/native-stage5a-acceptance-2026-11-03.json). |
| NI1-042 | `done` | NI1-041 | Identity drift, capacity, topology and release-horizon controls. As a developer, I got named refusals and real HTTP/PostgreSQL tests on the same 31 source files, with [Principal review](evidence/native-stage5a-principal-2026-11-03.json). |
| NI1-043 | `done` | NI1-042 | FlowRun execution generation and verified infrastructure replacement. As a system, I can reject writes from the previous generation. |
| NI1-044 | `done` | NI1-043 | Normal TaskRun retry and durable terminal reconciliation. As a user, I can retain the TaskRun UUID without forced states or Python frame restoration. |
| NI1-045 | `done` | NI1-043, NI1-044 | Late TaskRun event and FlowRun lifecycle guards. As a system, I got old-generation observations rejected before they change current progress. [N1.5b acceptance](evidence/native-stage5b-acceptance-2026-11-03.json) and [Principal review](evidence/native-stage5b-principal-2026-11-03.json), zero independent percentage. |
| NI1-046 | `done` | NI1-045 | SDK reconstruction from explicit stable keys. As a user, I can replace a terminated flow process and continue the original channels. |
| NI1-047 | `done` | NI1-046 | Real process-loss and response-loss matrix plus ordinary runtime/package regressions. As a developer, I got exact first-incomplete continuation on unchanged source bytes. |
| NI1-048 | `done` | NI1-047 | Independent review, evidence graph and ADR closure. As a system, I got all scoped criteria mapped to [frozen acceptance](evidence/native-stage5c-acceptance-2026-11-03.json), [Principal review](evidence/native-stage5c-principal-2026-11-03.json) and the [documentation graph](evidence/native-stage5c-graph-2026-11-03.json), earning 50% foundation readiness. |

## Native epoch 1 — Stage 5 mixed finite/stream delivery

All tasks trace to [ADR-0037](adr/ADR-0037-mixed-finite-and-streaming-tasks.md).
N1.6a was a server/client authority slice (+0%, readiness stayed 50% at its gate).
N1.6b is accepted after complete mixed SDK/process acceptance, earning its +10%.

The [N1.6a hosted closure](evidence/native-stage6a-hosted-successor-2026-11-04.json)
records merged PR 11 and 423 PostgreSQL/88 SQLite selected cases with zero skips.
The [N1.6b review](evidence/native-stage6b-sdk-design-review-2026-11-04.md) and
[real protocol adjunct](evidence/native-stage6b-prefix-recovery-adjunct-2026-11-04.json)
refine NI1-054 without claiming that the mixed SDK is implemented.

| ID | State | Depends | Deliverable and acceptance criteria |
| --- | --- | --- | --- |
| NI1-050 | `done` | NI1-048 | Bounded mixed-flow design and Principal agreement. As a developer, I can trace finite receipts, prefix recovery and tail admission to ADR-0037. |
| NI1-051 | `done` | NI1-050 | Separate finite bindings/receipts, staged execution anchor and paired migrations. As a system, I can preserve TaskRun identity without fake stream IDs or EOF. |
| NI1-052 | `done` | NI1-051 | Fenced reserve/complete/lookup/recovery HTTP API and paired clients. As a user, I can exactly replay finite progress under current generation authority. |
| NI1-053 | `done` | NI1-051, NI1-052 | Real API/PostgreSQL hostile matrix, migration parity and independent A review. As a system, I got [corrected scoped acceptance](evidence/native-stage6a-corrected-acceptance-2026-11-04.json) and [Principal review](evidence/native-stage6a-corrected-principal-2026-11-04.json); earned readiness remains 50%. |
| NI1-054 | `done` | NI1-053 | Direct finite Task invocation around the linear graph, durable-state selection of first seal/recovery, existing-tail witness discovery, sync/async parity and named early refusals. As a developer, I can prepare and summarize the stream in one FlowRun. |
| NI1-055 | `done` | NI1-054 | Real mixed 500-document process-loss matrix. As a user, I got four stable TaskRuns, two channels, receipt-based finite bypass and first-incomplete document recovery. |
| NI1-056 | `done` | NI1-055 | Ordinary-flow regression, evidence graph and independent combined acceptance. As a system, I can close ADR-0037 and earn the planned 60% milestone only after all criteria pass. |

## Native epoch 1 — deployment, worker lifecycle and final acceptance

All remaining tasks trace to
[ADR-0038](adr/ADR-0038-native-deployment-worker-lifecycle.md). They extend the
existing Prefect server, worker and work-pool path. They do not add an external
controller or claim production HA. The final selected-profile gate now closes
N1.6b–N1.10 and earns 100% of the bounded foundation.

| ID | State | Depends | Deliverable and acceptance criteria |
| --- | --- | --- | --- |
| NI1-060 | `done` | NI1-056 | Typed native Flow capability and immutable deployment/FlowRun protocol-profile digest. As a developer, I can declare the bounded profile before launch, and changed or unsupported declarations refuse before DATA. |
| NI1-061 | `done` | NI1-060 | Server worker-enrollment and exact-replay launch operation before Flow `Running`. As a system, I can bind work pool, worker, generation and one concurrency slot without putting credentials in parameters or events. |
| NI1-062 | `done` | NI1-061 | Bounded inherited-FD credential transport and actual work-pool launch. As a user, I can launch the mixed Flow through an ordinary Prefect deployment while ordinary deployments remain unchanged. |
| NI1-063 | `done` | NI1-060…062 | N1.7 real HTTP/PostgreSQL/deployment gate and independent review. As a system, I got declaration, version and secret-leak negative controls before earning 70%. |
| NI1-070 | `done` | NI1-063 | Durable Linux process identity and single-use death-verification operation. As a system, I can distinguish the recorded child from PID reuse and refuse timeout-only takeover. |
| NI1-071 | `done` | NI1-070 | One-winner generation replacement with stale-holder fencing and slot continuity. As a user, I got the same FlowRun, TaskRuns and channels after confirmed process loss. |
| NI1-072 | `done` | NI1-071 | Authenticated cancellation and drain with replayable commands. As a user, I can stop new admission, drain accepted work through EOF/tail, or cancel and reject later writes. |
| NI1-073 | `done` | NI1-070…072 | Worker/server restart, foreign PID, reused PID, live-old-child and retry-budget hostile matrix. As a system, I got no duplicate launch, double slot or timeout-as-death path. |
| NI1-074 | `done` | NI1-073 | N1.8 independent replacement/lifecycle acceptance. As a system, I got the selected Linux local-process profile reviewed before earning 85%. |
| NI1-080 | `done` | NI1-074 | Bounded authenticated progress/lifecycle projections and native UI. As a user, I can inspect generation, owner, positions, lag, EOF and receipts without payloads or capabilities. |
| NI1-081 | `done` | NI1-080 | SDK/server/worker/deployment version compatibility and paired migration gate. As a developer, I can observe a named pre-DATA refusal for mixed versions or code drift. |
| NI1-082 | `done` | NI1-080, NI1-081 | Selected-profile manifest/channel/receipt/projection/log and engine-owned buffer bounds and SQLite fail-closed controls. As a system, I got explicit capacity limits without a throughput or HA claim. |
| NI1-083 | `done` | NI1-082 | N1.9 projection/compatibility/resource acceptance. As a user, I got an operational boundary and independent review before earning 90%. |
| NI1-090 | `done` | NI1-083 | Actual work-pool mixed deployment in sync and async profiles. As a developer, I can run prefix, source, operator and tail without a test launcher or prototype adapter. |
| NI1-091 | `done` | NI1-090 | End-to-end retry, lost-response, confirmed replacement, server restart, drain and cancellation cuts with PostgreSQL oracles. As a user, I got exact first-incomplete continuation and terminal lifecycle. |
| NI1-092 | `done` | NI1-091 | Ordinary Flow/Task/generator, package, migration, client/server, UI and resource regression. As a system, I got no native-lane skips and no ordinary-runtime regression. |
| NI1-092a | `done` | NI1-080, NI1-092 | [Owned UI graph lifecycle](adr/ADR-0041-owned-graph-lifecycle-during-navigation.md). As a user, I can navigate to a TaskRun without queued layout, late data or stale subscriptions mutating a destroyed scene; real Pixi/throttle, stop/start, full UI and post-close packaged-browser gates pass. No extra foundation percentage. |
| NI1-093 | `done` | NI1-092 | Source-bound final evidence, [runbook and independent epoch review](adr/ADR-0040-native-integration-acceptance-and-operations.md). As a user, I got the bounded native-foundation acceptance and its explicit non-HA/non-performance limits before 100%. |

## Protocol epoch 2 — short experimental delivery — target 100%, earned 100%

The previous source/checkpoint/sink contract is superseded. Historical tasks
remain below as evidence but do not count toward the current target.

| Workstream | State | Tasks | Exit |
| --- | --- | --- | --- |
| Contract and focused model | `done` | DS-001, DS-002 | Durable-yield boundary and focused Principal PASS exist. |
| R0-A subprocess durability | `done` | DS-003a…c | Fresh-process crash oracle earned 3%. |
| R0-B state and barriers | `done` | DS-004S, DS-004a…b | State/replay decision, composed barrier model, 34 process crashes, and Principal PASS earned 6%. |
| R0-C branches and pressure | `done` | DS-005a…d | Branch/join plus retention/backpressure passed Principal review and earned 8%. |
| R0-D SDK and hostile gate | `done` | DS-006a…c, DS-007 | Real generators and Principal PASS earned 10%. |
| R1-A production storage safety | `done` | DS-010a…d | PostgreSQL authority, HA, fencing, and ObjectRef visibility passed Principal review and earned 18%. |
| R1-B retention and load | `done` | DS-011a…d | Bounded compaction plus two frozen-hash SLO runs passed Principal review and earned 30%. |
| R2-A production-backed runtime | `done` | DS-020a…d | Sync/async terminal execution and failure semantics passed Principal review and earned 42%. |
| R2-B replacement and state | `done` | DS-021a…d | Takeover, state migration, and repeated Anna/Tanya replacement passed Principal review and earned 55%. |
| R3-A coordinator barriers | `done` | DS-030a…c | Ordered barriers, frozen recovery, hostile traces, and independent Principal PASS earned 64%. |
| R3-B DAG recovery and bounded history | `done` | DS-031a…d | Routed joins, regional recovery/takeover, retained-horizon refusal, bounded retirement and Principal PASS earned 75%. |
| R4-A control-plane authority | `done` | DS-040a…d | Actual HTTP lifecycle/tenant/leader authority, full native activation checks, bounded admissions and Principal PASS earned 83%. |
| R4-B Prefect and operations | `done` | DS-041a…d | Actual SDK/API, fenced replacement, continuous maintenance, operational drill, 139-test gate and Principal PASS earned 90%. |
| R5-A release and schema recovery | `done` | DS-050a…d | Frozen 159-test gate and architecture review earn 95% for selected migration, signed schema recovery and runbook scope. |
| R5-B short release acceptance | `done` | DS-051a…d | 164-test gate, actual capacity/catch-up, selected worker boundary and 7 independent hostile controls close the declared short experimental delivery at 100%. |

Sprint R0 exit: DS-001, DS-002, DS-003a…c, DS-004S/a/b, DS-005a…d,
DS-006a…c, and DS-007 pass; only then does earned readiness become
10%. R0-A process-loss recovery and the DS-004S state/replay spike pass their
focused Principal reviews. Barrier composition also passed its focused review;
branching, reference-safe retention, and bounded live-data pressure passed
their focused reviews. Sync/async generators, durable invocation fencing,
sixteen process-loss boundaries, timeout/cancellation, and the final DS-007
hostile gate then passed. R1 and R2 subsequently passed the current PostgreSQL
HA/load and generator/replacement hostile gate on 2026-10-24. The 55% R2-B
gate was followed by the PostgreSQL coordinator gate on 2026-10-25. R3-A now
earned 64%; R3-B subsequently passed 93/93 frozen-source tests and independent
Principal review, earning 75%. R4-A then passed 110/110 frozen-source tests and
Principal review, earning 83%. R4-B then earned 90%, R5-A earned 95%, and
the final R5-B gate completed the agreed short experimental delivery at 100%.
Evidence: [R4-A report](evidence/postgres-r4a-candidate2-2026-10-25.json) and
[Principal PASS](evidence/postgres-r4a-principal-2026-10-25.json).

## Epoch 2 weekly sprint plan

Percentages are cumulative and earned only by a complete sprint exit. Risk and
size use `H/M` and `S/M/L`. [ADR-0012](adr/ADR-0012-protocol-epoch-2-delivery-gates.md)
defines the gating policy.

### Active task to ADR map

| Tasks | Technical ADRs |
| --- | --- |
| DS-003a…c, DS-050a…d, DS-051a…d | [ADR-0009](adr/ADR-0009-failure-injection-evidence.md), [ADR-0012](adr/ADR-0012-protocol-epoch-2-delivery-gates.md) |
| DS-004S, DS-004a…b, DS-030a…c | [ADR-0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0010](adr/ADR-0010-platform-durable-yield-channel.md), [ADR-0013](adr/ADR-0013-managed-state-recovery-classes.md), [ADR-0014](adr/ADR-0014-ordered-data-barrier-composition.md), [ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md) |
| DS-005a…d, DS-031a…d | [ADR-0003](adr/ADR-0003-branch-and-join-recovery.md), [ADR-0007](adr/ADR-0007-backpressure-and-high-load.md), [ADR-0015](adr/ADR-0015-independent-branches-and-durable-join-buffer.md), [ADR-0022](adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md) |
| DS-006a…c, DS-020a…d, DS-021a…d | [ADR-0011](adr/ADR-0011-yield-completion-and-client-effects.md), [ADR-0006](adr/ADR-0006-durable-channels-and-state.md) |
| DS-010a…d | [ADR-0010](adr/ADR-0010-platform-durable-yield-channel.md), [ADR-0006](adr/ADR-0006-durable-channels-and-state.md) |
| DS-011a…d | [ADR-0006](adr/ADR-0006-durable-channels-and-state.md), [ADR-0007](adr/ADR-0007-backpressure-and-high-load.md) |
| DS-040a…d, DS-041a…d | [ADR-0001](adr/ADR-0001-runtime-boundary.md), [ADR-0010](adr/ADR-0010-platform-durable-yield-channel.md) |
| DS-007 | [ADR-0008](adr/ADR-0008-architecture-role-charter.md), [ADR-0009](adr/ADR-0009-failure-injection-evidence.md) |
| DS-050a…d | [ADR-0026](adr/ADR-0026-release-compatibility-and-disaster-recovery.md) |
| DS-051a…d | [ADR-0027](adr/ADR-0027-short-acceptance-and-separate-long-soak.md), [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md) |
| DS-009 | [ADR-0011](adr/ADR-0011-yield-completion-and-client-effects.md), future multi-output ADR |

### W1–W2 / R0-A — subprocess durability — target 3%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-003a** | `done` | DS-002 | H / M | Real `os._exit` at upload, append, delivery, yield observation, completion, and progress boundaries. As a developer, I can kill a worker at every named durable boundary without timing sleeps. Evidence: 22 abrupt exits, fresh reopen, retry convergence, and Principal PASS in [ADR-0009](adr/ADR-0009-failure-injection-evidence.md). |
| **DS-003b** | `done` | DS-003a | H / M | Fresh-process reopen and independent oracle. As a system, I got output/state/completion/cursor together or none and rejected unsafe controls. Evidence: sequence-200 YIELD/RETURN matrix, 17 unsafe controls, and Principal PASS in [ADR-0009](adr/ADR-0009-failure-injection-evidence.md). |
| **DS-003c** | `done` | DS-003a | M / S | Orphan ObjectRef and lost runtime acknowledgement cases. As a user, I got Tanya redelivering exactly the first incomplete input. Evidence: staging/published orphan detection and lost-ACK recovery to 200 or 201 in [ADR-0009](adr/ADR-0009-failure-injection-evidence.md). |

Exit: fresh storage connections and processes prove both sides of document 200;
mocked exceptions alone do not earn 3%.

### W3–W5 / R0-B — state decision and barrier composition — target 6%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-004S** | `done` | DS-003a…c | H / M | Blocking state/replay spike. As a developer, I can declare partition-local per-turn state or checkpoint-managed shared state. Evidence: independent oracle, document-200 boundary, sealed ordered two-input replay, lifecycle/storage controls, and Principal PASS in [ADR-0013](adr/ADR-0013-managed-state-recovery-classes.md). |
| **DS-004a** | `done` | DS-004S | H / M | Ordered DATA/BARRIER composition. As a system, I can keep per-record progress separate from a coordinator checkpoint cut. Evidence: composed log, independent oracle, document-200 boundary, aligned join, fresh-runtime restore, exact tail replay, stable outputs, post-recovery continuation, direct-SQL unsafe controls, and Principal PASS in [ADR-0014](adr/ADR-0014-ordered-data-barrier-composition.md). |
| **DS-004b** | `done` | DS-004a | H / S | Crash before/after barrier. As a system, I cannot combine cursor 200 with state 100 or create a barrier retroactively after failure. Evidence: 34 named `os._exit(86)` cases, fsynced ACK marker, fresh recovery worker, integrity checks, exact retry, independent oracle, and Principal PASS in [ADR-0014](adr/ADR-0014-ordered-data-barrier-composition.md). |

Exit: executable controls show partition-local state resuming at 201 and shared
or multi-input state restoring a completed barrier then replaying its tail.

### W6–W8 / R0-C — branch, join, retention and pressure — target 8%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-005a** | `done` | DS-004S/a/b | H / M | Two independent consumer cursors. As a user, I can stop Anna while another branch continues. Evidence: one 301-row upstream log, Anna 200→Tanya 201, Boris 300→301, branch-local barrier progress, exact completion/output witnesses, independent oracle, and Principal PASS in [ADR-0015](adr/ADR-0015-independent-branches-and-durable-join-buffer.md). |
| **DS-005b** | `done` | DS-004S/a/b | H / M | Aligned join and durable post-barrier buffer. As a system, I cannot mix cuts at a join. Evidence: bound branch DATA/BARRIER witnesses, exact `{fast:2,slow:2}` snapshot, durable DATA 3/4 buffer, ordered drain, fresh reopen, hostile controls, and Principal PASS in [ADR-0015](adr/ADR-0015-independent-branches-and-durable-join-buffer.md). |
| **DS-005c** | `done` | DS-005a…b | H / M | Retention roots, immutable manifests/lifecycle, tombstone retry, late-root rejection, independent oracle, and Principal PASS. As a system, I cannot delete referenced data and can reclaim unreferenced data. |
| **DS-005d** | `done` | DS-005c | M / S | Atomic record/byte credits, concurrent reservation, block/compact/resume and crash evidence passed Principal review. As a user, Petya continues only to the declared durable limit, then is throttled without silent loss. |

Exit: lagging-branch restart, retention negative control, and full-buffer
block/resume tests pass.

### W9–W12 / R0-D — generator SDK and hostile gate — target 10%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-006a** | `done` | DS-003a…c, DS-005a…d | H / M | Real sync generator, immutable terminal observation, exact YIELD/RETURN completion, and fresh recovery passed. As a developer, I can execute Petya→Anna without a manual commit API. |
| **DS-006b** | `done` | DS-006a | H / M | Async cancellation, timeout, cross-runtime exclusion and recovery passed. As a developer, I get the same progress contract under async cancellation. |
| **DS-006c** | `done` | DS-006a…b | M / S | Durable invocation holder/epoch/attempt fencing, illegal close/return controls and independent oracle passed. As a system, I reject next-input consumption and a second terminal event before completion. |
| **DS-007** | `done` | DS-003a…c, DS-004S/a/b, DS-005a…d, DS-006a…c | H / S | ETL/HighLoad Principal reran 11 tests and hostile races, then passed R0-D on 2026-10-23. As a system, I got PASS for stateless and stateful recovery before claiming 10%. |

Exit: real sync/async generators plus subprocess recovery pass.

### W13–W15 / R1-A — production storage safety — target 18%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-010a** | `done` | DS-007 | H / S | Backend/deployment ADR and explicit consistency/HA assumptions. As a developer, I can identify the authoritative store and failure boundary. Evidence: [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-010b** | `done` | DS-010a | H / L | Ordered partition append plus durable cursor/completion/state transaction. As a system, I preserve exact envelope and progress witnesses across remount/reopen. Evidence: 22/22 backend tests and [HA release 3](evidence/postgres-ha-release3-2026-10-24.json). |
| **DS-010c** | `done` | DS-010b | H / M | Production lease and fencing CAS. As a system, I reject stale writers after process or node reconnect. Evidence: stale-owner controls and promoted-primary recovery in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-010d** | `done` | DS-010b | M / M | ObjectRef visibility and orphan ledger. As a user, I never receive an envelope whose object is not durable. Evidence: staged-orphan and content-address controls in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |

Exit: integration tests run on the actual backend and persistent volume with
two-coordinator races; SQLite-only evidence cannot earn 18%.

### W16–W18 / R1-B — retention, backpressure and load — target 30%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-011a** | `done` | DS-010a…d, DS-004a…b | H / M | Reference graph and compaction. As a system, I reclaim every unreferenced segment and no referenced record/ObjectRef. Evidence: [ADR-0020](adr/ADR-0020-bounded-metadata-and-idempotency-horizon.md). |
| **DS-011b** | `done` | DS-010a…d | H / M | Per-edge byte/record/time/metadata/consumer budgets and credit propagation. As a user, I can see and predict producer throttling. Evidence: atomic capacity controls in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-011c** | `done` | DS-011a…b | H / M | Recovery-horizon and catch-up capacity model. As an operator, I can size storage from ingress, outage horizon, and catch-up target. Evidence: fixed 60-second horizon and 1.25× recovery floor in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-011d** | `done` | DS-011a…c | M / L | Benchmark/failover harness. As a user, I got agreed p50/p95/p99, sustained ingress, stalled consumer, compaction, and restart measurements. Evidence: [trials 29](evidence/postgres-load-release-trial29-2026-10-24.json), [30](evidence/postgres-load-release-trial30-2026-10-24.json), and [HA release 3](evidence/postgres-ha-release3-2026-10-24.json). |

Exit: agreed SLO thresholds and overload policy pass; measurements without an
agreed threshold do not earn 30%.

### W19–W21 / R2-A — Python runtime execution — target 42%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-020a** | `done` | DS-010a…d, DS-011d | H / L | Sync runtime over the production backend. As a developer, I can run documented decorators and generators. Evidence: [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-020b** | `done` | DS-010a…d, DS-011d | H / L | Async runtime. As a developer, I get equivalent delivery semantics for async iteration. Evidence: [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-020c** | `done` | DS-020a…b | H / M | Runtime adapter for output/state/completion transaction. As a system, I do not request N+1 before N terminates. Evidence: atomic terminal transaction and 22-boundary crash matrix in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-020d** | `done` | DS-020c | M / M | Exception, cancellation, and timeout semantics. As a user, I can distinguish retryable failure, cancellation, and completed return. Evidence: cancellation/timeout hostile controls in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |

Exit: actual generators and backend survive process loss around observation and
durable completion.

### W22–W24 / R2-B — worker replacement and managed state — target 55%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-021a** | `done` | DS-020a…d | H / M | Heartbeats, lease renewal, and takeover. As a system, I maintain one effective owner. Evidence: epoch/holder fencing in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-021b** | `done` | DS-020a…d, DS-004S | H / L | Per-turn managed state API. As a user, Tanya gets the state matching document 200 or 201. Evidence: PostgreSQL state projection and replacement tests in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-021c** | `done` | DS-021b | H / M | State schema/version compatibility. As a developer, I get deterministic migration or rejection. Evidence: double-run canonical migration and hostile controls in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-021d** | `done` | DS-021a…c | H / M | Repeated replacement-worker matrix. As a system, I reject stale completion and corrupted/incompatible state. Evidence: twenty-turn Anna/Tanya matrix and Principal PASS on 2026-10-24. |

Exit: repeated Anna/Tanya loss, takeover races, and state corruption controls
pass before claiming 55%.

### W25–W27 / R3-A — coordinator barriers — target 64%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-030a** | `done` | DS-021a…d | H / M | Periodic barrier injection shares the partition lock and ordered log with DATA and compaction. As a system, I never synthesize a caller-selected or post-failure cut. Evidence: concurrent ordering and retention-race tests in [ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md). |
| **DS-030b** | `done` | DS-030a | H / L | Immutable intent, source/participant manifests, content-addressed snapshots, create-only completion, hard budgets, and frozen recovery plan. As a system, I select only a complete compatible checkpoint. Evidence: independent oracle and unsafe status-only control in [ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md). |
| **DS-030c** | `done` | DS-030b | H / M | Coordinator takeover resumes the same intent, fences the old epoch, and binds graph/code/state identities. As a developer, I can inspect exact cuts and snapshot versions. Evidence: 14 real-process crash boundaries, 10/10 focused tests, 20/20 takeover repetitions, 39/39 regression tests, and Principal PASS on 2026-10-25. |

Exit: coordinator process loss at every transition and an unsafe incomplete-
checkpoint control pass.

### W28–W30 / R3-B — branches, joins and selective recovery — target 75%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-031a** | `done` | DS-030a…c | H / L | Per-input join alignment and buffering. As a system, I got one coherent cut through routed barriers, durable mailbox/terminal transactions, abort release and async generators. Twelve process-loss boundaries, independent oracles and final Principal PASS close [ADR-0022](adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md)'s R3-B scope. |
| **DS-031b** | `done` | DS-031a | H / M | Branch-local cursors and recovery regions. As a user, I got B/J/D fencing/replay/atomic activation while C stayed live. Append-only owner takeover, four recovery-process crashes, fresh-process convergence and final Principal PASS complete the task. |
| **DS-031c** | `done` | DS-031b | H / M | Upstream expansion inside the retained durable horizon. As a system, I can explain and execute policy-driven expansion, or refuse unsupported source/invalid physical replay authority before publishing a fence. Planner, B/C/J/D execution and internal-output integrity controls pass. External source rewind and missing-byte reconstruction remain explicitly unsupported in [ADR-0093](adr/ADR-0093-implicit-regeneration-of-missing-durable-output.md). |
| **DS-031d** | `done` | DS-031a…b | M / M | Backpressure through a branched DAG. As a user, I got hard record/byte limits, returned credits, DAG pressure and live regional catch-up. One hundred cycles of all four retirement layers retain exactly 64 logical rows within the declared byte budget; final Principal PASS closes [ADR-0023](adr/ADR-0023-bounded-retention-and-replay-horizons.md)'s R3-B scope. |

Exit: canonical branch/join DAG, skewed barriers, branch loss, unavailable cuts,
and mixed-cut/stale-fence model checks pass.

### W31–W33 / R4-A — orchestration server — target 83%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-040a** | `done` | DS-031a…d | H / L | Server persistence/internal APIs as projections of data-plane authority. As a developer, I can query actual run, partition, owner, input cursor, barrier and recovery authority through HTTP; an independent oracle rejects fabricated projections. Full 110-test gate and Principal PASS in [ADR-0024](adr/ADR-0024-control-plane-authority-and-lifecycle.md). |
| **DS-040b** | `done` | DS-040a | H / M | Coordinator leadership and fencing. As a system, I can reject stale leaders through a locked live epoch for lifecycle, reconcile and admission retirement; takeover cannot overtake command commit. Full gate and Principal PASS complete. |
| **DS-040c** | `done` | DS-040a…b | M / M | Deploy/run/cancel/drain. As a user, I got closed admission, atomic DATA publication, an exact full-graph drain and cancellation including regional recovery. Partial native graphs/missing guards fail before activation. HTTP process-loss resume, bounded 100-cycle history and Principal PASS complete. |
| **DS-040d** | `done` | DS-040a | M / S | Authorization and tenant identity. As a system, I reject forged/revoked/insufficient capabilities and cross-tenant controls; source and output partitions have exclusive deployment ownership. Concurrent collision and final Principal PASS complete. |

Exit: HA server failover, duplicate command, stale leader, and permission-negative
tests pass without control/data dual writes.

### W34–W36 / R4-B — Prefect integration and operability — target 90%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-041a** | `done` | DS-040a…d | H / L | Feature-flagged Prefect flow/task integration. As a user, I can deploy the production-shaped streaming flow. Actual SDK/API adapter and independent gate pass in [ADR-0025](adr/ADR-0025-prefect-adapter-and-operational-projections.md). The selected profile atomically rejects regional recovery while a Prefect attempt is active; composing both owner models remains explicit future work while standalone R3 regional recovery stays supported. |
| **DS-041b** | `done` | DS-041a | H / M | Deployment reconnect/resume. As a user, control-plane restart does not lose data-plane progress. |
| **DS-041c** | `done` | DS-041a | M / M | Metrics, traces, and log correlation. As an operator, I can identify lag, in-flight owner, last completion, barrier age, and replay cause. |
| **DS-041d** | `done` | DS-041b…c | M / M | API/UI projections, alerts, and runbook drill. As an operator, I can diagnose backpressure and stalled recovery. |

Exit: real Prefect server/worker E2E, server restart, and alert drill pass.

### W37–W39 / R5-A — upgrade and chaos acceptance — target 95%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-050a** | `done` | DS-041a…d | H / M | Rolling and mixed-version compatibility. As a system, I keep monotonic cursors or reject the rollout. |
| **DS-050b** | `done` | DS-050a | H / M | Schema migration and rollback policy. As a developer, I can migrate or receive a deterministic incompatibility error. |
| **DS-050c** | `done` | DS-050a | H / L | Process, node, network, and object-store fault campaign. As a user, I got the published recovery behavior under infrastructure faults. |
| **DS-050d** | `done` | DS-050b…c | M / M | Backup/restore, drain, and tested runbook. As an operator, I can recover without cursor regression or stale writes. |

Exit: old/new workers, coordinator failover, storage remount, and missing/corrupt
artifact controls pass.

Evidence: [R5-A full candidate](evidence/postgres-r5a-candidate-2026-10-26.json),
[architecture review](evidence/postgres-r5a-architecture-review-2026-10-26.json),
and [recovery runbook](recovery_runbook.md). This selected schema-recovery gate
earns 95%; it does not claim whole-cluster DR, zero RPO or general worker isolation.

### W40–W42 / R5-B — soak, capacity and release — target 100%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-051a** | `done` | DS-050a…d | H / L | Short declared acceptance profile with injected failures and stalled branches under [ADR-0027](adr/ADR-0027-short-acceptance-and-separate-long-soak.md). As a system, I got no unexplained loss, cursor regression, mixed cut, or stale write in the measured run. |
| **DS-051b** | `done` | DS-051a, DS-011a…d | H / M | Capacity/catch-up validation under [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md). As an operator, I can compare independent calibration with later recovery and inspect conditional headroom estimates, distinguishing logical admission credits from physical disk/WAL behavior. |
| **DS-051c** | `done` | DS-051a | M / M | Security and credential-boundary review. As a system, workers cannot forge storage authority. |
| **DS-051d** | `done` | DS-051a…c | H / S | Final Principal acceptance and ADR evidence graph. As a user, I got the published guarantee and measured limits with no missing evidence link. |

Separate nonblocking follow-up: **DS-052a** — `done` — the real 72-hour campaign, original reviewer, fresh collector, publication and downloaded-artifact audit passed under [ADR-0030](adr/ADR-0030-continuous-arc-soak-and-evidence-collection.md). The [completion record](evidence/ci-arc-72h-completed-40672223609.json) and [artifact audit](evidence/ci-arc-72h-artifact-audit-40672223609.json) bind the unchanged profile and exact source. It carries no additional delivery percentage under [ADR-0027](adr/ADR-0027-short-acceptance-and-separate-long-soak.md).

### Historical DS-052a execution observations

The dated observations below retain their original running/queued statuses.
The current final result appears above and in the [operator guide](soak_ci.md).

2026-10-27 follow-up: the repository transfer is verified and the
[static-analysis repair](evidence/ci-static-analysis-repair-2026-10-27.json)
passes hosted repository hooks and type checking. The replacement smoke is
queued without an ARC host. Source-hygiene re-acceptance preserves the failed
full trial and requires a passing serial frozen-source successor; DS-052a and
its long-duration criteria remain open.

The [serial full successor](evidence/postgres-ci-style-regression-serial-2026-10-27.json)
now passes 164/164 with no drift and the same source hashes as the preserved
failed trial. Together with 197 fast regressions and hosted static analysis,
it validates the source-hygiene change without changing the short-delivery
percentage or closing long-duration work.

The [Principal review](evidence/ci-source-hygiene-principal-review-2026-10-27.json)
accepts this source-hygiene successor with no blockers.

Later ARC acceptance on 2026-10-27: the
[actual one-job smoke](evidence/ci-arc-smoke-observed-40578404646.json) passes
with recovery, four oracles, public export and owned cleanup, and its
[Principal acceptance](evidence/ci-arc-smoke-principal-review-2026-10-27.json)
closes the smoke repair. The 72-hour run and DS-052a remain open/unstarted;
this follow-up contributes no additional short-delivery percentage.

Post-merge dispatch on 2026-10-27: the
[72-hour campaign](https://github.com/Anacreonsystems/ariadne/actions/runs/40601620025)
now exists on `main`; the [dispatch record](evidence/ci-long-soak-dispatch-40601620025.json)
shows its prepare job queued without an assigned persistent runner. DS-052a
remains `progress`; startup and actual long-duration acceptance are still open.

2026-10-28 retrigger: the user confirmed ARC availability and requested a fresh
trigger. The [unstarted static-profile run was cancelled](evidence/ci-long-soak-cancelled-40601620025.json),
and the [fresh merged-source ARC smoke](evidence/ci-arc-smoke-observed-40650444560.json)
passes recovery, four oracles, review/export and cleanup. DS-052a remains
`progress`: no 72-hour workload ran, and an ARC long profile requires the
separate state/lifetime/evidence validation recorded in
[ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md).

Exit: workload, configuration, raw results, and invariant report are archived;
only this complete gate earns 100% and permits eligible ADRs to become
`accepted`.

### Epoch 2 critical path and parallel lanes

```text
DS-003a→b/c
→ DS-004S → DS-004a→b
→ DS-005a/b→c→d
→ DS-006a→b/c → DS-007                         10%
→ DS-010a→b→c/d → DS-011a/b→c/d                30%
→ DS-020a/b→c→d → DS-021a/b→c/d                55%
→ DS-030a→b→c → DS-031a→b→c/d                  75%
→ DS-040a→b/c/d → DS-041a→b/c→d                90%
→ DS-050a→b/c→d → DS-051a→b/c/d               100%
```

Benchmark harness,
observability schema, and deployment fixtures may start early in parallel, but
they cannot satisfy or bypass a dependency gate.

### Parallel research lane — no readiness percentage

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-009** | `blocked` | DS-006a…c | H / S | Compare numbered `emit()+complete()` with generator multi-yield and create a planning or rejected ADR. As a developer, I can reason about crash after output N without expanding epoch-2 implementation scope. |

DS-009 may run after the SDK contract exists. It does not belong to any epoch-2
gate and cannot delay 100% unless it disproves the single-terminal-event
contract itself.

Each sprint exit requires linked artifacts, reproducible commands, raw failure
or load evidence, resolved blocking review findings, and updated ADR/task/board
status. A green test suite without the sprint-specific environment and evidence
does not earn the percentage.

## Historical protocol epoch 1 — frozen superseded planning

Everything below records the old plan and evidence. Checkboxes, critical paths,
and WIP instructions below are historical and do not direct protocol epoch 2.

- [x] **ST-001** Establish ADR lifecycle and dependency graph
  ([ADR-0000](adr/ADR-0000-decision-lifecycle.md)).
- [x] **ST-002** Define runtime boundary and guarantee vocabulary
  ([ADR-0001](adr/ADR-0001-runtime-boundary.md)).
- [x] **ST-003** Prototype incomplete-checkpoint rejection
  ([ADR-0002](adr/ADR-0002-checkpoints-and-recovery-domains.md)).
- [x] **ST-004** Prototype selective branch recovery
  ([ADR-0003](adr/ADR-0003-branch-and-join-recovery.md)).
- [x] **ST-005** Prototype recovery after a crash between sink commits
  ([ADR-0004](adr/ADR-0004-multi-sink-commit.md)).
- [x] **ST-006** Complete ETL/HighLoad Principal review and counterexamples
  ([ADR-0008](adr/ADR-0008-architecture-role-charter.md),
  [ADR-0009](adr/ADR-0009-failure-injection-evidence.md)).
- [x] **ST-007** Preserve rejected transaction and idempotency theories
  ([ADR-0090](adr/ADR-0090-prefect-transactions-as-coordinator.md),
  [ADR-0091](adr/ADR-0091-idempotency-alone-is-atomic.md)).

## Sprint 1A/1B — safety contracts — cumulative targets 20% and 27%

Sprint 1 was split after Principal review found that its original H-risk chain
did not fit one short sprint. Sprint 1A isolates the end-to-end safety invariant;
Sprint 1B freezes the complete contracts before barrier work begins.

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-010** | ST-001…006 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0005](adr/ADR-0005-operator-contract.md) | Canonical job, graph-version, operator, edge, partition, record, checkpoint, and generation IDs. | As a developer, I can derive the same stable identity after restart. As a system, I can reject reuse with different lineage or payload. |
| **ST-011** | ST-010 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0004](adr/ADR-0004-multi-sink-commit.md) | Checkpoint state machine with start, alignment, prepare, commit-decision, completion, and abort paths. | As a system, I can validate every transition and reject rollback after `COMMIT_DECIDED`. |
| **ST-012** | ST-010 | H / M | [0001](adr/ADR-0001-runtime-boundary.md), [0005](adr/ADR-0005-operator-contract.md) | Versioned Source, Operator, Channel, StateBackend, and Sink contracts with capability negotiation. | As a developer, I can implement a participant against a versioned contract. As a user, I got deployment rejection when requested guarantees exceed capabilities. |
| **ST-013** | ST-011, ST-012 | H / M | [0009](adr/ADR-0009-failure-injection-evidence.md) | Named failure matrix and deterministic fault-injection API without timing sleeps. | As a developer, I can inject a crash before and after every durable boundary. As a system, I got a named invariant result for every point. |
| **ST-014** | ST-010, ST-011 | M / S | [0009](adr/ADR-0009-failure-injection-evidence.md) | Independent safety oracle for recovery checkpoint, visible records, decisions, and allowed duplicates. | As a system, I can compare recovered state with an independent oracle. |

Sprint exit: identities, transitions, and guarantees are explicit protocol
artifacts rather than implicit prototype behavior.

### Execution evidence

- [x] **Sprint 1A / 20% gate:** stable identity derivation, immutable lineage claims,
  combined checkpoint/decision/source-ACK record, recovery on both sides of the
  decision, independent oracle, and unsafe control are implemented; ETL/HighLoad
  Principal review passed on 2026-10-20.
- [x] **ST-010 implementation:** all eight canonical identity classes reproduce
  after restart; the integrated durable registry rejects lineage or payload drift
  for the same record across checkpoint transactions by storing an explicit
  lineage digest.
- [x] **ST-011 implementation:** the checkpoint lifecycle persists across reopen,
  rejects illegal transitions, and forbids abort after `COMMIT_DECIDED`.
- [x] **ST-012 implementation:** versioned participant contracts negotiate tier
  `T` and reject unsupported source or sink capabilities.
- [x] **ST-013 implementation:** nine durable boundaries expose deterministic
  before/write-before-ACK/after-ACK failure points without sleeps; every guarded
  predecision phase edge is exercised at all three offsets.
- [x] **ST-014 implementation:** an independent oracle names recovery, decision,
  payload, duplicate, expected-effect completeness, and transactional-visibility
  invariant results, including visible effects before a commit decision, effects
  missing from an externally committed sink, and external participant IDs absent
  from the immutable manifest. Missing required local rows produce failed named
  invariants rather than an oracle exception.
- [x] **Sprint 1B / 27% gate:** ETL/HighLoad Principal review found no remaining
  material defect within the Sprint 1 prototype scope on 2026-10-20; ADR/task/
  board links pass validation.

Evidence lives in
[integrated_protocol.py](prototypes/integrated_protocol.py),
[test_integrated_protocol.py](prototypes/test_integrated_protocol.py),
[ADR-0002](adr/ADR-0002-checkpoints-and-recovery-domains.md),
[ADR-0005](adr/ADR-0005-operator-contract.md), and
[ADR-0009](adr/ADR-0009-failure-injection-evidence.md). The first split-state
iteration is retained as rejected evidence in
[ADR-0092](adr/ADR-0092-split-safety-state-machines.md).

## Sprint 2 — barriers, recovery regions, and fencing — cumulative target 43%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-020** | ST-010…014 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Partitioned Python channel with real data and barrier records. | As a system, I can preserve per-partition order across restart and reject gaps or wrong epochs. |
| **ST-021** | ST-020 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0003](adr/ADR-0003-branch-and-join-recovery.md) | Aligned barrier state machine for multi-input joins. | As a system, I can snapshot a join without mixing pre-barrier and post-barrier records. |
| **ST-022** | ST-010, ST-020 | H / M | [0003](adr/ADR-0003-branch-and-join-recovery.md) | Recovery-region planner for descendants, replay cuts, join inputs, and upstream expansion. | As a user, I can see why each node entered recovery. As a system, I can replay every boundary input from one completed checkpoint. |
| **ST-023** | ST-010…012 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Generation leases and fencing on channel, snapshot, and decision writes. | As a system, I can reject every write from an obsolete worker or coordinator epoch. |
| **ST-024** | ST-013, ST-020 | H / M | [0009](adr/ADR-0009-failure-injection-evidence.md) | Subprocess kill/reopen harness with independently reopened stores. | As a developer, I can terminate a process at named boundaries and reopen every durable component in a new process. |
| **ST-025** | ST-011, ST-021, ST-023 | H / L | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | TLA+ checkpoint, alignment, and fencing model, including unsafe variants. | As a system, I got bounded model evidence for checkpoint consistency and stale-writer exclusion. |

Sprint exit: selective recovery uses actual barriers and process loss, and obsolete
workers cannot mutate the new generation.

### Execution evidence

- [x] **ST-020:** ordered partition records include barriers; reopen, stable
  record claims, contiguous sequence, and lease/generation fencing pass Principal
  review.
- [x] **ST-021:** an early join input is durably blocked after its barrier; the
  snapshot is recorded only after every input reaches the same checkpoint, then
  buffered post-barrier data is applied. Restore retains immutable historical
  consumed identities, rejects a divergent same-result suffix, and permits exact
  replay. Principal review passed.
- [x] **ST-022:** the planner binds one completed checkpoint to per-partition
  replay cuts, returns failed descendants and reasons, and expands upstream when
  the checkpoint is unavailable at a boundary. Principal review passed; binding
  the manifest digest to a production graph version remains an integration task.
- [x] **ST-023:** channel and join leases use a trusted durable monotonic store
  clock, non-shortening renewal, expiry-gated generation takeover, and atomic
  owner/generation/expiry guards on mutation. Principal review passed.
- [x] **ST-024:** 21 real `os._exit` cases cover seven boundaries at three
  offsets, followed by fresh channel, join, or coordinator reopen and independent
  oracle evaluation where applicable. Principal review passed.
- [x] **ST-025:** the two-input/two-checkpoint/two-epoch TLA+ model generates
  9,471 states and finds 2,132 distinct safe states to depth 17. Independent
  mixed-checkpoint and missing-fence mutations produce the expected
  `SameCheckpointCut` and `FencingSafety` counterexamples. Principal review
  passed within bounded safety scope; liveness remains outside the model.
- [x] **Sprint 2 / 43% gate:** all task-level prototype/formal criteria passed
  ETL/HighLoad Principal review on 2026-10-20. This is not production acceptance.

Evidence lives in [barrier_protocol.py](prototypes/barrier_protocol.py) and
[test_barrier_protocol.py](prototypes/test_barrier_protocol.py); process-loss
evidence is in
[test_subprocess_recovery.py](prototypes/test_subprocess_recovery.py), and formal
artifacts live in [models/README.md](models/README.md).

## Sprint 3A/3B — multi-sink and source acknowledgement — cumulative targets 49% and 58%

Sprint 3A is a one-week safety slice on the serialized critical path. It closes
the cooperative sink contract and authoritative decision creation before source
and weaker-tier execution expand the state space.

- [x] **ST-030a:** define `CooperativeSinkV1` prepare/commit/abort/recover
  semantics, immutable prepare-token identity, and coordinator plus participant
  fencing; pass a durable reference-sink contract suite.
- [x] **ST-030b:** abort every prepared participant after predecision failure,
  retry a lost abort acknowledgement, and forbid abort after durable `COMMIT`.
- [x] **ST-031a:** enforce COMMIT creation in storage from `PREPARED` only and
  only when every immutable sink participant is prepared; preserve a forged raw
  SQL decision as a rejected control.
- [x] **ST-032a:** continue recovery past an unavailable committed participant
  and expose durable attempt count, last error, and blocked participant state.

Sprint 3A exit: ST-030 and ST-031 pass Principal review and ST-032 has explicit
blocked-participant evidence. Only then is cumulative 49% claimed. ST-033,
behavioral tiers I/B, and ST-035 remain in Sprint 3B toward 58%.

### Sprint 3A execution evidence

- [x] `CooperativeSinkV1` has durable prepare tokens, transaction-scoped
  coordinator fences, independent participant-generation fences, recoverable
  terminal state, and lost-ACK contract tests.
- [x] The authority creates `COMMIT` only from the exact immutable prepared
  manifest. `ABORT` first advances the coordinator fence, is irreversible, and
  continues cleanup past unavailable participants.
- [x] Recovery attempts are registered durably before external calls and finish
  as `IN_PROGRESS`, `SUCCEEDED`, or `BLOCKED`. Owner/fence and attempt-number
  CAS guards reject late results; restart reconciles terminal participant state.
- [x] Operator, sink, and source-partition identities are disjoint, so one
  participant cannot overwrite another role's diagnostics.
- [x] Crash, lost-ACK, unavailable-participant, takeover, forged-decision, and
  terminal-reconciliation regressions pass in the 92-test prototype suite.
- [x] ETL/HighLoad Principal hostile review passed on 2026-10-20. Sprint 3A's
  cumulative 49% gate is claimed as prototype evidence; ADRs remain `progress`.

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-030** | ST-011, ST-012, ST-023 | H / M | [0004](adr/ADR-0004-multi-sink-commit.md) | Versioned cooperative sink protocol with prepare, commit, abort, recover, fencing, and immutable intent digest. | As a developer, I can implement a cooperative sink and run its contract suite. |
| **ST-031** | ST-030 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0004](adr/ADR-0004-multi-sink-commit.md) | One durable decision record linking checkpoint and multi-sink transaction. | As a system, I cannot publish `COMPLETED` or acknowledge source progress before durable `COMMIT_DECIDED`. |
| **ST-032** | ST-024, ST-031 | H / L | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tests for prepare failure, lost ACK, coordinator crash, partitions, repeated recovery, and unavailable sinks. | As a system, I can abort before the decision and only commit after it. As a user, I can see transactions blocked in `COMMITTING`. |
| **ST-033** | ST-020, ST-031 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0005](adr/ADR-0005-operator-contract.md) | Source acceptance, acknowledgement, and replay contract. | As a user, I got no acknowledged-input loss after any enumerated crash. As a system, I can replay unacknowledged input with stable IDs. |
| **ST-034** | ST-012, ST-030 | M / M | [0004](adr/ADR-0004-multi-sink-commit.md) | Conformance suites for sink tiers `T`, `I`, and `B`. | As a user, I can see the enforced tier and transient-visibility semantics. |
| **ST-035** | ST-025, ST-031 | H / L | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | TLA+ multi-sink decision model with missing-decision and missing-fence variants. | As a system, I got one irreversible decision per immutable manifest within documented liveness assumptions. |

Sprint exit: the bounded model demonstrates end-to-end safety from accepted input
to multiple sinks; temporary partial visibility remains explicit.

### Sprint 3B1 — source acceptance and replay — cumulative target 53%

The ETL/HighLoad Principal split Sprint 3B after the 49% gate. ST-033 freezes the
source side before tier behavior and the multi-sink formal model encode it.

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-033a** ✓ | ST-020, ST-031 | H / S | [0005](adr/ADR-0005-operator-contract.md) | `ReplayableSourceV1` contract for stable record identity, opaque position, runtime ordinal, replay, acknowledgement, and participant fencing. | As a developer, I can implement read/replay/ack without comparing opaque positions. As a system, I got an immutable per-partition cut. |
| **ST-033b** ✓ | ST-033a | H / M | [0005](adr/ADR-0005-operator-contract.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Atomic accepted-input ledger and durable channel handoff. | As a system, I cannot report acceptance unless identity, lineage, payload, ordinal, position, and channel record are durable. As a developer, I can retry an identical handoff and drift is rejected. |
| **ST-033c** ✓ | ST-033b | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0005](adr/ADR-0005-operator-contract.md) | Exact checkpoint vector, decision-bound ACK, and replay planner. | As a user, I got no acknowledged position without exact `COMMIT` and covered cut. As a system, I can replay every unacknowledged record without selecting a partial vector. |
| **ST-033d** ✓ | ST-024, ST-033c | H / M | [0009](adr/ADR-0009-failure-injection-evidence.md) | Deterministic and subprocess crash matrix with an independent source oracle. | As a developer, I can reproduce every source boundary after fresh reopen. As a user, I can inspect accepted, committed, externally acknowledged, and replay-start cuts plus blocking gaps. |

Sprint 3B1 exit: ST-033a…d pass ETL/HighLoad Principal hostile review. Only
then is cumulative 53% claimed. ST-034 contributes the next 2% and ST-035 the
final 3% toward Sprint 3B's 58% target.

Sprint 3B1 passed ETL/HighLoad Principal hostile review on 2026-10-21. The
cumulative 53% prototype gate is claimed. The source suite has 18 focused cases,
including real-process crashes, a real coordinator decision authority, takeover
after an unknown external result, repeated-cut epochs, and independent-oracle
negative controls.

### Sprint 3B2A — sink-tier conformance — cumulative target 55%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-034a** ✓ | ST-012, ST-030 | H / S | [0004](adr/ADR-0004-multi-sink-commit.md) | `TierGuaranteeV1`, immutable execution intent and common visibility/outcome vocabulary. | As a developer, I can map each tier to required capabilities and terminal states. As a user, I can see guarantees, partial visibility, duplicates, uncertainty, and gaps. |
| **ST-034b** | ST-034a | M / S | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tier `T` conformance suite over `CooperativeSinkV1`. | As a system, I got no visible `T` effect before `COMMIT` and one logical effect per sink after recovery. As a user, I can observe permitted `PARTIAL` visibility between commits. |
| **ST-034c** | ST-034a | H / M | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tier `I` reference adapter, durable delivery intent, and crash/recovery suite. | As a developer, I can retry an unknown write with the same effect key. As a system, I converge without logical duplicates and retain input while a delivery is unresolved. |
| **ST-034d** | ST-034a | H / M | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tier `B` reference adapter and persisted retry/exhaustion policy. | As a user, I got explicit duplicate, split, `UNKNOWN`, `EXHAUSTED`, and `GAPPED` evidence. As a system, I advance across a gap only with frozen lossy opt-in. |
| **ST-034e** | ST-034b, ST-034c, ST-034d | H / S | [0001](adr/ADR-0001-runtime-boundary.md), [0004](adr/ADR-0004-multi-sink-commit.md) | Mixed-tier restart matrix and independent tier oracle. | As a system, I select the weakest tier, reject a stronger request, and reject tier/capability/policy drift. As a user, I can inspect the reason for deployment or recovery rejection. |

Sprint 3B2A exit: ST-034a…e pass ETL/HighLoad Principal hostile review. Only
then is cumulative 55% claimed. Production adapters remain ST-060.

### Sprint 3B2B — formal tier-T decision model — cumulative target 58%

ST-035 models only the tier `T` decision core behind the barrier/checkpoint
assume-guarantee boundary. The safe model covers two checkpoints, two sinks,
immutable manifests, prepare tokens, coordinator and participant fencing,
irreversible `COMMIT`/`ABORT`, permitted partial visibility, completion, and
source ACK. Missing-decision and missing-fence mutations must produce named TLC
counterexamples. Conditional convergence assumes weak fairness and eventual
participant availability; simultaneous visibility remains outside the claim.

## Sprint 4 — durable storage, backpressure, and load — cumulative target 72%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-040** | ST-020, ST-033 | H / L | [0006](adr/ADR-0006-durable-channels-and-state.md) | Go segmented durable-channel prototype without an external broker. | As a system, I can reopen committed segments after process and storage-remount simulations without record loss. |
| **ST-041** | ST-011, ST-012 | H / M | [0005](adr/ADR-0005-operator-contract.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Full/incremental state-snapshot prototypes and compatibility metadata. | As a developer, I can restore a compatible schema and I get deterministic rejection for an incompatible schema. |
| **ST-042** | ST-022, ST-033, ST-040 | H / M | [0003](adr/ADR-0003-branch-and-join-recovery.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Retention watermark from checkpoints, consumer cursors, and pending sink decisions. | As a system, I can prove no referenced segment is deleted and reclaim every unreferenced segment. |
| **ST-043** | ST-040 | H / M | [0007](adr/ADR-0007-backpressure-and-high-load.md) | Byte/record credits and bounded memory/durable backlog. | As a system, I can propagate sink pressure to the source without unbounded growth. |
| **ST-044** | ST-041…043 | H / L | [0007](adr/ADR-0007-backpressure-and-high-load.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Slow-sink, disk-full, skew, and recovery-catch-up benchmarks. | As a user, I got measured throughput, p95/p99 latency, checkpoint time, replay rate, and overload behavior. |
| **ST-045** | ST-040…044 | H / M | [0006](adr/ADR-0006-durable-channels-and-state.md), [0007](adr/ADR-0007-backpressure-and-high-load.md) | Storage decision, rejected alternatives, and capacity model. | As a developer, I can size memory and disk from ingress, recovery horizon, and catch-up target. |

Sprint exit: production integration has a selected backend and measured operating
envelope rather than an abstract high-load claim.

## Sprint 5 — experimental production integration — cumulative target 88%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-050** | Sprint 1–4 exits | H / L | [0001](adr/ADR-0001-runtime-boundary.md), [0005](adr/ADR-0005-operator-contract.md) | Experimental sync/async `yield` runtime behind a feature flag. | As a developer, I can run source → transform → branch → join → sink through the documented API. |
| **ST-051** | ST-025, ST-035, ST-045 | H / L | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Production checkpoint/recovery coordinator and durable decision store. | As a system, I can recover coordinator state without selecting an incomplete checkpoint. |
| **ST-052** | ST-023, ST-051 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Worker ownership, generation leases, and stale-writer enforcement. | As a system, I got one effective writer generation per partition after replacement. |
| **ST-053** | ST-011, ST-031, ST-045 | H / L | [0001](adr/ADR-0001-runtime-boundary.md), [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Server persistence schemas and internal APIs. | As a developer, I can inspect checkpoint, participant, recovery, and commit state through stable internal APIs. |
| **ST-054** | ST-050…053 | M / M | [0007](adr/ADR-0007-backpressure-and-high-load.md) | Events, metrics, and recovery-graph observability. | As a user, I can see lag, replay, alignment, recovery region, sink tier, and blocked commits. |
| **ST-055** | ST-050…054 | H / L | [0009](adr/ADR-0009-failure-injection-evidence.md) | Production contract tests mapped to TLA+ actions and prototype invariants. | As a system, I got production evidence for every mapped safety transition. |

Sprint exit: an experimental production-shaped runtime exists, while ADRs remain
short of `accepted` until external and load acceptance passes.

## Sprint 6 — external acceptance and release gate — cumulative target 100%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-060** | ST-034, ST-050 | H / L | [0004](adr/ADR-0004-multi-sink-commit.md) | Reference replayable source and tier `T`/`I` sinks without an external broker. | As a developer, I can validate real adapters with the prototype conformance suite. |
| **ST-061** | ST-055, ST-060 | H / L | [0003](adr/ADR-0003-branch-and-join-recovery.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Branched-DAG end-to-end crash matrix through multiple sinks. | As a user, I got outputs equal to the failure-free oracle for every supported tier. |
| **ST-062** | ST-044, ST-061 | H / L | [0007](adr/ADR-0007-backpressure-and-high-load.md) | Long soak, host loss, slow/unavailable sink, and catch-up tests. | As a user, I got documented capacity and recovery objectives sustained under load. |
| **ST-063** | ST-041, ST-051, ST-061 | H / L | [0005](adr/ADR-0005-operator-contract.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Cancellation, drain, upgrade, graph-version, and state-migration tests. | As a system, I can reject or migrate incompatible recovery state without silent corruption. |
| **ST-064** | ST-054, ST-061…063 | M / M | [0000](adr/ADR-0000-decision-lifecycle.md) | Operational runbook and user guarantee documentation. | As a user, I can diagnose stalled checkpoints and understand possible duplicates or partial visibility. |
| **ST-065** | All previous | H / M | [ADR graph](adr/README.md) | Acceptance-evidence index and ADR status review. | As a developer, I can trace every accepted criterion to production evidence. As a user, I got no guarantee based only on prototype results. |

Sprint exit: only ADRs with production code and complete evidence move to
`accepted`; the rest remain `progress`/`planning` or become `rejected`.

## Critical path and parallelism

```text
ST-010
→ ST-011 / ST-012
→ ST-020
→ ST-021 / ST-022 / ST-023
→ ST-024 / ST-025
→ ST-030 → ST-031
→ ST-032 / ST-033 / ST-035
→ ST-040 / ST-041
→ ST-042 / ST-043 → ST-044 → ST-045
→ ST-050 / ST-051 / ST-053 → ST-052 / ST-054 / ST-055
→ ST-060 → ST-061 → ST-062 / ST-063 → ST-065
```

Do not start barriers before identities and the checkpoint state machine; do not
start multi-sink coordination before sink contracts and fencing; do not advance
source acknowledgement before the checkpoint/decision link; do not implement
retention before recovery cuts and pending-commit references; and do not select
production storage before crash/load evidence.

After their contracts freeze, barrier/join, fencing, and subprocess work can run
in parallel. Executable and TLA+ models can also run in parallel, as can channel
and state-backend experiments. SDK and server work can overlap only after the
serialized protocol is stable.

## Final short delivery evidence

The [164-test final gate](evidence/postgres-r5b-final2-2026-10-26.json) and
[7 independent controls](evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
close DS-051a…d. [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md)
publishes the workload, conditional forecasts, fixes, rejected trials and
limits. This is 100% of the agreed short experimental implementation, not a
claim of production completeness. DS-052a, hosted runner availability and
broader technical-ADR production criteria remain separate open work.

Final sign-off: [architecture review](evidence/postgres-r5b-architecture-review-2026-10-26.json)
and [documentation graph](evidence/adr-graph-final-2026-10-26.json).

## Final native foundation acceptance

The [final source-bound acceptance](evidence/native-epoch1-final-acceptance-2026-11-04.json)
and its linked independent review, hosted CI and retained first failures
close the criteria of this decision. All implementation and acceptance
tasks for this selected scope are complete. Earlier planning and local-only
observations remain historical records.

The accepted profile is finite versioned top-level Python code, a finite
prefix and tail around a single-source linear stream graph, PostgreSQL 17
and trusted same-host Linux/PID-namespace process recovery. There are at
most 128 ACTIVE native runs per work pool. Application code owns external
effect idempotency. General DAG/partition support, cross-host recovery,
production HA, performance SLOs and a new 72-hour native campaign are
outside this accepted foundation.

## N2.2 finalization gate update — 2026-11-05

The [current epoch evidence note](epochs/epoch-02-native-streaming-completion.md#n22-retained-verification-and-finalization-correction--2026-11-05)
records the historical terminal-PREPARE and exchange-mode PASS successors,
both personal 5000 lifecycle failures and the separate ordinary Task retry.
The [retained finalization archive](evidence/native-epoch2-finalization-2026-11-05/index.json)
and [four-index causal proof](evidence/native-epoch2-route-cleanup-indexes-2026-11-05/component-v1/index.json)
do not replace corrected native 5000 execution or full frozen-union acceptance.
N2.2 remains `progress`, with 15% earned; the 27% target is unearned.


## NI2-012 current liveness evidence — 2026-11-05

The [latest epoch note](epochs/epoch-02-native-streaming-completion.md#n22-bounded-read-probe-and-hosted-observations--2026-11-05)
links the scoped renew proof, retained-volume read probe and actual hosted
failures. Read non-reproduction is not root-cause closure; the next native 1000
probe remains unexecuted until its exact candidate is published. No task/ADR
acceptance or readiness credit changes here.


## N2.2 native 1000 coordinator hold — 2026-11-05

The [f416 native-1000 evidence](evidence/native-epoch2-coordinator-retirement-2026-11-05/native1000-red-v1/index.json) retains strict
async helper **1 PASS / 471.607 seconds**, no source drift, no Task retry or
HTTP 5xx, and complete graph data/physical closure. The broader final audit is
RED on two coordinator cleanup errors: legacy DAG release was refused, leaving
the native authority TERMINAL, snapshot ACTIVE and coordinator launch
PROCESS_REGISTERED. A narrow terminal-path correction and real native
worker-health controls are required before corrected 5000 runs. The historical
partition-read cause remains unresolved; this 1000 probe stopped source append
before crossing consumer input 764. N2.2 remains `progress` at **15% earned**,
with zero additional acceptance credit.


## N2.2 hosted rebalance runtime RED — 2026-11-05

As a developer, I can inspect the [actual hosted rebalance failure](evidence/native-epoch2-ci-rebalance-runtime-2026-11-05/b4-red-v1/index.json)
on immutable `b4a5f1ed7abc9cdd35ebb4451c2c1ef628e4c145`,
[run 42099724610, job 127501239557](https://github.com/Anacreonsystems/ariadne/actions/runs/42099724610/job/127501239557).
The single runtime case failed in 229.934 seconds after all 64 source inputs
and the four stable Task bodies completed. At the SDK wait deadline, the
observed graph was still ADMITTED with physical C slots unreleased. The Flow
assertion is FAILED. Graph RELEASED is observed only after timeout and fail
cleanup; it does not demonstrate normal closure before the deadline.

The [observation windows](evidence/native-epoch2-ci-rebalance-runtime-2026-11-05/b4-red-v1/timeline-observation-windows.json)
retain direct log timestamps separately from estimated monotonic-to-wall
mapping. The later retire409 follows timeout/fail control and is not evidence
that the separate native-1000 post-terminal release refusal caused this case.
The [resource addendum](evidence/native-epoch2-ci-rebalance-runtime-2026-11-05/b4-red-v1/resource-and-trace-addendum-v1.json)
is an after-command observation, not a controlled CPU experiment. The cause
of release latency remains unresolved; no CPU, renewal or coordinator cause
is assigned, and no timeout or oracle change is justified by this record.

Separately, the current coordinator correction has an
[implementation report](evidence/native-epoch2-coordinator-retirement-2026-11-05/implementation-report-v1/index.json)
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
[b4 async-500 failure](evidence/native-epoch2-ci-volume-runtime-2026-11-05/b4-async500-red-v1/index.json)
from [job 127501239543](https://github.com/Anacreonsystems/ariadne/actions/runs/42099724610/job/127501239543).
The source accepted all 500 records, but immediately before fail control the
three consumer cursors were only 438/432/435 and all four TaskRuns remained
RUNNING. Worker fenced exceptions occurred after `/fail` returned 200. They
are downstream observations, not the identified initiating error. The async
Flow's original trigger and slow-drain cause remain UNKNOWN. No PG error
trace is retained, and no CPU, renewal, coordinator or common sync-500 cause
is assigned. This differs from rebalance's completed-body release delay.

The separate [generic Python-3.14 job](evidence/native-epoch2-ci-generic-envelope-2026-11-05/component-v1/index.json)
recorded 4413 PASS and 11 ordinary skips in its 800.25-second test step, but
the required whole job was CANCELLED at the 15-minute envelope. Its reviewed
conditional gives only the runner/worker/settings/input/CLI generic bucket
20 minutes; the other generic buckets retain 15. This changes job setup/body/
cleanup headroom, not native wait budgets or test oracles, and is not a green
rerun. The independent one-line proposal review is bound to that historical
proposal; it does not accept the later combined workflow.

The combined [CI guard component](evidence/native-epoch2-coordinator-ci-guards-2026-11-05/component-v1/index.json)
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
[b4 sync-500 record](evidence/native-epoch2-ci-volume-runtime-2026-11-05/b4-sync500-red-v1/index.json)
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
[source-binding erratum](evidence/native-epoch2-ci-volume-runtime-2026-11-05/b4-sync500-binding-erratum-v1/index.json)
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
[b4 sync accepted-200 recovery record](evidence/native-epoch2-ci-recovery-runtime-2026-11-05/b4-sync-accepted200-red-v1/index.json)
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
[Principal's independent coordinator component proof](evidence/native-epoch2-coordinator-retirement-2026-11-05/principal-v1/index.json):
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

Separately, the [serial CI component](evidence/native-epoch2-ci-serial-profile-2026-11-05/component-v1/index.json)
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

As a developer, I can inspect the [baseline RED](evidence/native-epoch2-launch-parent-locks-2026-11-06/baseline-v1/index.json),
[retained failed candidate](evidence/native-epoch2-launch-parent-locks-2026-11-06/failed-candidate-v1/index.json),
[implementer component](evidence/native-epoch2-launch-parent-locks-2026-11-06/component-v1/index.json) and
[separate Principal 50 checks PASS](evidence/native-epoch2-launch-parent-locks-2026-11-06/principal-v1/index.json).
Only launch prepare/record select the shared parent prefix; default/other
callers, own-target locks and lifecycle fences remain unchanged. The baseline
identified a synthetic SHARE blocker and pending Flow `FOR UPDATE`, followed
by the same request's 200 after rollback. It does not identify the historical
b4 blocker or turn pending into HTTP 500.

As a user, I got personally verified native SDK/TCP/PostgreSQL17/OS sync500
[accepted200](evidence/native-epoch2-launch-parent-locks-2026-11-06/native-accepted200-v1/index.json) and
[incomplete200 pair PASS](evidence/native-epoch2-launch-parent-locks-2026-11-06/native-incomplete200-v1/index.json).
Accepted200 resumes at 201; incomplete200 repeats only unfinished 200,
with 501 B body calls and 500 distinct effects. B retains its TaskRun, healthy
A/C stay at attempt 1, all cursors reach 500 and all five births plus graph and
coordinator state close/release. Both source-body and wider-health checks pass;
exact API 5xx / TimeoutError / unhandled ASGI / unexpected healthy retry counts are zero.
As a system, I got 1,600-file holds with zero drift/import mismatches, 24 closed
native PID/start-time identities and four dropped fresh databases without
FORCE or remaining sessions.

A43-05 gains scoped process/replacement and parent-fence evidence. A43-07 gains
[coordinator/worker-health retention](evidence/native-epoch2-runtime-health-artifacts-2026-11-06/component-v1/index.json) and explicit
release/health observations, without claiming its complete slot/admission
profile. The collector's two CLI checks and final static checks pass; v1's
reconstructed-source qualification remains preserved. Existing hosted missing
reports are not backfilled, and post-hoc resource snapshots are labeled as
archival collection. The [ADR graph README](adr/README.md) retains the corrected
current **15%**, with no new earned credit. The [epoch note](epochs/epoch-02-native-streaming-completion.md#n22-launch-parent-lock-correction-and-scoped-sync500-recovery--2026-11-06)
contains the full story/criterion mapping and immutable evidence links.

The separate current [hosted 7d99 run](https://github.com/Anacreonsystems/ariadne/actions/runs/42116718681) had both DAG500 exact no-skip
jobs successful at the 2026-11-06 02:43:45 Europe/Moscow (2026-11-05 23:43:45 UTC) snapshot:
[sync 1 PASS / 709.65s](https://github.com/Anacreonsystems/ariadne/actions/runs/42116718681/job/127559987264) and [async 1 PASS / 638.66s](https://github.com/Anacreonsystems/ariadne/actions/runs/42116718681/job/127559987278).
Hosted sync accepted200 also completed its exact step/gates, while the whole
run and remaining recovery/5000 gates were unfinished. These CI observations
bind committed 7d99, not the local held correction; missing old coordinator/health
reports limit whole-health attribution.

NI2-012/N2.2b and ADR-0043 remain `progress`, with **15% earned**. Async recovery,
corrected 5000 and full hosted/frozen-union exits remain open. The planned 27%
and whole-ADR completion are not earned; historical failures remain intact.


## N2.2 async5000, observed-count comparison and worker logging correction — 2026-11-06

The [first native async5000 case](evidence/native-epoch2-dag-volume5000-2026-11-06/async-v1/index.json)
passed 1 case / 2225.473s with exact three 5000-call/effect branches and fixed
4 TaskRuns / 4 births / 7 channels. Independent Principal file-only review is not a
second native5k run. The [normal async500 comparison](evidence/native-epoch2-dag-volume5000-2026-11-06/async500-comparison-v1/index.json)
now records 13 observed cardinality/role comparisons MATCH at d906. This supports
A43-04 counts only; A43-04/05/07 remain scoped. No final allocation SQL/external
KV observation or independent CI HTTP/retry replay is inferred. PG samples begin
692/690/694 and end preterminal 4888/4881/4894. [Cleanup limits](evidence/native-epoch2-dag-volume5000-2026-11-06/async-v1/qualifications/cleanup-attribution-v1/index.json)
retain zero sessions at driver check/two fresh names absent afterward, with earlier
framework cleanup uninstrumented.

[Controlled logging baseline](evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/baseline-v1/index.json)
remains1 FAIL / 6 PASS / 94.20s at INFO20; its two generated before-binding gaps remain
qualified. The [test-only corrected module](evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/actualpassed-module-v1/index.json)
now has 151 PASS / 82.37s/actual pytest exit 0/tool exit 0, 1756 prebound source files with no drift,
1216 imports with no mismatch, five new PIDs closed and retained fresh DB sessions0.
Its Principal review is file-only, not personal151 execution. Production code
is unchanged. The [interrupted151 wrapper](evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/failed-candidate-module-v1/index.json)
and [recovered7PASS wrapperexit1/waitUNKNOWN](evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/recovered-selected-v1/index.json)
retain their qualifications; the old dead unreaped zombie is separate from
successful v4/native5k closure. Historical CI gw0 ordering/cause remains unknown.

The [old d906 CI snapshot](evidence/native-epoch2-worker-log-ci-isolation-2026-11-06/actualpassed-module-v1/root/old-ci-snapshot.json)
(04:49:28Europe/Moscow/01:49:28 UTC) reports 13/19 runtime cases PASS and the known
General logging failure. It proves neither new-head green CI nor the unpublished
correction. See the [epoch note](epochs/epoch-02-native-streaming-completion.md#n22-async5000-observed-count-comparison-and-worker-logging-correction--2026-11-06)
for source/audit limits. Other three local heavy profiles and full 19/four-heavy/
NI2-014 remain open. NI2-012/N2.2b stay `progress`, **15% earned**, zero new credit;
planned 27% and whole ADR-0043 acceptance remain unearned.


## N2.2 personal normal5000 pair and candidate P2 cleanup gates — 2026-11-06

Published 1812460 now has a closed [author sync5000](evidence/native-epoch2-dag-volume5000-2026-11-06/sync-v1/index.json)
**1 PASS/2090.278s** and [Principal PERSONAL async5000](evidence/native-epoch2-dag-volume5000-2026-11-06/principal-async-v1/index.json)
**1 PASS/1989.457s**, pytest/tool0, no failure/error/skip, original tests/budgets.
Each source 1600/import 3884 lease has zero drift/mismatch and exact effects/clean
wider health. Whole async phase 14 PID/start identities closed; retained fresh DB
sessions 0 at driver check/no terminate/DROP/FORCE, final SQL connections closed.
Principal async SQL has 24 live 19-family captures plus final 19/all15 literal checks
true; Consumer 0 rows is not retained 5000 Consumer-cursor proof. Author-sync metadata
has 30 live 18-family captures plus final 18/separateConsumer. Actual first/last SQL
frontiers and author direct-PG co-sample labels are distinct; file guard 4900 is not
a SQL 4900/activation/exact 500 capture. Seventeen immutable mappings remain stable;
persisted release/counters are observed, externalKV/HTTP retry counters unknown.


The [personal sync5000 package](evidence/native-epoch2-dag-volume5000-2026-11-06/principal-sync-v1/index.json)
adds 1 PASS/1974.770s, pytest 0/tool 0, 1600 source/3884 imports drift0/mismatch0,
15000 effects, 14 phase identities closed and 28 live 19-family SQL captures plus
final 19/all 15 literal checks. The normal personal async/sync pair is closed;
Consumer 0 rows/no exact 500/source-capture-time qualifications remain explicit.

The [current sync500 comparison](evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-comparison-v1/index.json)
records 13 normalized observable-count matches at current head, not missing CI SQL
or raw-marker/full-history reconstruction. Failed v1 preparatory KeyError and
literal grant RETIRED correction stay preserved. The [personal85/hosted bridge](evidence/native-epoch2-dag-volume5000-2026-11-06/routing85-bridge-v1/index.json)
supports A43-06/10 goldens at recorded-instance scope, not physical-host/ADR47 or
native process-loss/network proof; parent import audit/16 source-derived child limits
remain explicit. See the [epoch note](epochs/epoch-02-native-streaming-completion.md#n22-personal-normal5000-pair-and-candidate-p2-cleanup-gates--2026-11-06)
for A43-04/05/07 release/provenance and late source-capture qualifications.
The remaining P2 bridge and current CI full 19/four-heavy/
NI2-014 remain open; old d906 13/19 is historical. NI2-012/N2.2b stay `progress`,
earned **15%**, zero new credit; planned 27%/whole-ADR acceptance remain unearned.


The separate immutable fixed-P2 incomplete200 baseline has native 1 PASS 102.962s
but actual postterminal SQL **9 true/3 false** (f8c5c21e…/15f9c83c…); sealed STOP
dae84846… and classification f02d962f… preserve full automatic cleanup OPEN.
Graph ADMITTED/authority TERMINAL/launch PROCESS_REGISTERED/partitions not released
remain literal failures despite valid physical/tag/counter/recovery facts.
Fixed-P2 automatic post-wait cleanup is required. Legacy fixture worker-stop
boundary/timing supports preemption, without exact kill trace or a new production
regression claim. Prior UUID-V1 remains unexecuted `REQUEST_CHANGES`. The [closed P2 candidate](evidence/native-epoch2-dag-volume5000-2026-11-06/p2-candidate-v1/index.json)
now records actual personal native sync incomplete200 1 PASS/101.022s and async
accepted200 1 PASS/103.467s, both independent 19-family SQL ALL 12 true; selected
real-PG ASGI/model 19 PASS 49.016s, all pytest/tool 0/0 skip/actors STOP. The old baseline
9 true/3 false remains unchanged, not reclassified benign. Selected-file normal
pre-commit/pre-push 0 preserve candidate test61eb. The exact f9143d59…/b0d879f3…
source-relevance bridge keeps old DAG/CI actual 1812460 attribution: imported P2
module bytes change but execution_api fixture/globals remain unchanged. There is
no WIP DAG rerun/full import-byte-equality or future-commit CI claim. Named new
P2 positive cleanup profiles close their scoped observations; other source-
covered heavy/stall/excluded profiles receive no new execution credit.
[The existing exit](epochs/epoch-02-native-streaming-completion.md#n22-retained-verification-and-finalization-correction--2026-11-05)
keeps NI2-012 through NI2-014 OPEN until native/release/hosted/independent gates
all close. Implementation VERIFIED is not task acceptance. The latest Root
1812460 CI snapshot is 15/19; four-heavy/fullmatrix/NI2-014 remain OPEN, earned 15%,
planned 27% unearned. No approved ADR change, predicate weakening or forensic DB repair.


## N2.2 current CI sync5000 failure and original-AC block — 2026-11-06

The [closed current CI sync5000 failure](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/index.json)
is actual 1812460 `[False-5000]` 1 FAIL, Flow FAILED versus expected COMPLETED.
Original metadata `underlying_cause=UNKNOWN` remains exact. A later bounded
receipt directly observes SDK wait 3960.31s timeout→same-graph `/fail` HTTP200→
Task `CANCEL_REQUESTED` fencing. Three retry-scheduling messages do not prove
executed retry processes/epochs; underlying slow-progress/resource bottleneck
remains UNKNOWN. Last polling is not a final/coordinator/worker-health report;
missing reports/HTTP retry/SQL metrics are not invented or backfilled.

The [closed 05:36:18.437UTC snapshot](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/supplements/ci-status-053618-v1/index.json)
has **15 PASS / 1 known FAIL / 1 ACTIVE / 2 QUEUED** at 1812460; DAG async5000 is
active, P2 sync/async5000 queued. No newer status or additional RED is inferred.
The [Principal original-AC map](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/principal/original-AC-component-status-BLOCKED-v1.json)
keeps NI2-012/013 implementation verification separate from formal OPEN tasks;
NI2-014 is BLOCKED, NI2-020 implementation GO false. See the [epoch failure note](epochs/epoch-02-native-streaming-completion.md#n22-current-ci-sync5000-failure-and-original-ac-block--2026-11-06).
Existing deliverables/criteria, all prior local/failed/version evidence and the
required hosted exit stay unchanged. Earned 15%, planned 27% unearned; no full 19/
four-heavy/NI2-014/wholeADR or localPASS-for-hosted acceptance shortcut.


The [independent Principal classification](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-sync5000-failure-v1/supplements/principal-classification-v1/index.json)
verifies 18 metadata files/excerpts/four source bindings and confirms only the
SDK wait timeout→error cleanup `/fail`→secondary fencing path. Underlying
bottleneck remains UNKNOWN; no test/DB/ZIP/network rerun or acceptance follows.


## N2.2 bounded sync500 BEFORE measurement — 2026-11-06

The [closed BEFORE diagnostic pair](evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-measurement-pair-v1/index.json)
retains CONTROL 1 PASS 218.526s and INSTRUMENTED 1 PASS 250.317s on source 1600/publication 636,
not the later projection/JOIN source. Raw/derived counts and getter2.073 threadCPU/
dag_plan151.160 inclusivewall/SQL596.685 overlap limits remain in the epoch note;
no cache/optimization or CI fix was claimed by that measurement.


## N2.2 closed projection diagnostics, JOIN PG68 and CI03 status — 2026-11-06

[Projection V2/source lineage](evidence/native-epoch2-dag-volume5000-2026-11-06/taskrun-projection-v1/index.json)
keeps rejected V1/unexecuted expectations separate from actual27c1 source and
[PG23 actual 23 PASS 55.516s](evidence/native-epoch2-dag-volume5000-2026-11-06/taskrun-projection-pg23-v1/index.json).
[POSTFIX500/comparison](evidence/native-epoch2-dag-volume5000-2026-11-06/sync500-measurement-postfix-pair-v1/index.json)
has actual CONTROL 212.262s/instrumented 227.587s, one fewer dispatch per matched
DATA request/3507 saved; total 5788 and observed times include requestmix/overhead,
not a causal speedup or CI fix. BEFORE/raw50MB is reused by reference only.
The later [JOIN source](evidence/native-epoch2-dag-volume5000-2026-11-06/reference-bounded-join-v1/index.json)
and [closed PG68](evidence/native-epoch2-dag-volume5000-2026-11-06/reference-bounded-join-pg68-v1/index.json)
are distinct: **67 PASS/1 exact SQLite-only SKIP 147.025s**, 1601 source/import
no drift/mismatch, five identities closed. JOIN500 benchmark remains PENDING;
active trial is unread/unarchived, no borrowed oldsource result.

[Async CI18safe](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-async5000-failure-v1/index.json)
primarycause NOT_RETAINED/UNKNOWN is not assigned the sync timeout cause.
[CPU/wall pairing](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-cpu-wall-pairing-v1/index.json)
is NOT_OBSERVED, without genuine cgroup/wall matching. [Latest closed CI03](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-status-0733-v1/index.json)
at 07:33:36.420UTC has 16 PASS/2 DAG5k FAIL/physical async ACTIVE on OLD 1812460;
physicalsync wholejobSUCCESS is not a health replay/newguard result. CI02 remains
immutable. See the [epoch note](epochs/epoch-02-native-streaming-completion.md#n22-closed-projection-diagnostics-join-pg68-and-ci03-status--2026-11-06).
NI2-012…014 remain OPEN/BLOCKED, NI2-020 implementation blocked, 15% earned,
planned 27% unearned. All previous history/requirements/approved ADR bodies remain.


## N2.2 terminal old18124 CI04 matrix — 2026-11-06

[Terminal CI04](evidence/native-epoch2-dag-volume5000-2026-11-06/ci-status-075802-v1/index.json)
at 07:58:02.148UTC has all 94 jobs terminal and all 19 native cases terminal:
**17 PASS / 2 known DAG5000 FAIL** on OLD 1812460, no active/queued case. Both
physical5000 wholejobs succeeded, without artifact download/health replay or
credit to new WIP cleanup/query guards. CI03 remains historical. Source components
retain their actual versions; JOIN PG68 is 67 PASS/1 exact SQLite-only SKIP, JOIN500
pair PENDING. See the [epoch terminal note](epochs/epoch-02-native-streaming-completion.md#n22-terminal-old18124-ci04-matrix--2026-11-06)
and prior appended cause/resource/version links. NI2-012…014 OPEN/BLOCKED,
NI2-020 implementation blocked; earned 15%, planned 27% unearned/fullmatrixnotgreen.


## N2.2 closed JOIN500 diagnostic pair: dispatch reduction without speedup — 2026-11-06

[JOIN500 now CLOSED](evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-measurement-pair-v1/index.json):
CONTROL 1 PASS 223.458s/API196.472938CPU and treated 1 PASS 293.313s/API262.308499CPU,
source1601a0f2/health/import/ownedclosure checks pass. Matched read38→32/complete46→40
confirms 18000 dispatches saved; total−15390 also reflects requestmix. Observed
suite/CPU are slower (+5.27%/+6.15% control,+28.88%/+32.07% treated), no demonstrated
speedup, calibrated causal regression or CI-cause fix. [Details](epochs/epoch-02-native-streaming-completion.md#n22-closed-join500-diagnostic-pair-dispatch-reduction-without-speedup--2026-11-06)
retain overlap/API-attempt/version limits. COUNTERS-ONLY prep NOT EXECUTED.
PG68 remains 67 PASS/1 SQLite backend SKIP, oldCI17PASS/2DAG5kFAIL/notgreen, NI2-012…014
OPEN/BLOCKED/NI2-020blocked/earned15%/27unearned. All prior pass/failure/source and
measurement history remains intact; no whole-ADR acceptance/rejection is inferred.


## N2.2 closed counters-only diagnostic: residual API CPU unknown — 2026-11-06

[COUNTERS-ONLY now CLOSED](evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-api-sync-counters-v1/index.json):
1 PASS 224.438s/API196.481766CPU, source1601a0f2/import/health/retainedDB/13identity
closure valid. Six selected inclusive CPU stages do not form an additive full
profile; compiler constructors are not cache misses, residual CPU UNKNOWN and no
shape-cache fix is justified. [Epoch limits](epochs/epoch-02-native-streaming-completion.md#n22-closed-counters-only-diagnostic-residual-api-cpu-unknown--2026-11-06)
retain all counts/exceptions/failure-preparation lineage. New standard statistical
profile is source-feasibility/PENDING, NOT EXECUTED. OldCI17PASS/2DAG5kFAIL remains
notgreen; NI2-012…014OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned. Prior versions,
functionalPASS and slower JOIN measurements remain immutable, no whole-ADR change.


## N2.2 closed py-spy500 and JOIN integration rollback — 2026-11-06

[py-spy500 now CLOSED](evidence/native-epoch2-dag-volume5000-2026-11-06/api-pyspy500-v1/index.json):
1 PASS224.973s/API197.759607CPU on historical a0f2; sampler exit0/CPU8.317530s,
3748 weights=3609 nonempty+139 EMPTY/1899 separate unreadable errors. High loss
limits the profile to **DESCRIPTIVE_ONLY**, whole-API CPU attribution INCONCLUSIVE;
no exact CPU share/cache/cause/CI fix is inferred. Original hold487a assertion is
unexecuted preparation, corrected holdab245 authorized one actual native case.

Root rejected JOIN for current integration due no demonstrated benefit.
[Exact two-file rollback](evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-rollback-v1/index.json)
restored a4acc/f7ef/source012a; final normal selected hooks0/0. P2 cleanup and
TaskRun projection retained. All candidate code/tests/PG68/slower pair/counters/
py-spy history remains on a0f2, not rollback-source tests; no whole-ADR43 rejection.
PG62 preparation NOT EXECUTED here; CI-runner DAG500 diagnostics source-planning/
NOT RUN. [Epoch details](epochs/epoch-02-native-streaming-completion.md#n22-closed-py-spy500-and-join-integration-rollback--2026-11-06)
retain capture/loss/source and lifecycle limits. OLD18124CI17PASS/2DAG5kFAIL remains
not green; NI2-012…014OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned.


## N2.2 restored-source PG62 component verification — 2026-11-06

[Restored-source PG62 now CLOSED](evidence/native-epoch2-dag-volume5000-2026-11-06/reference-join-reverted-pg62-v1/index.json):
Principal actual **61 PASS/1 exact SQLite backend SKIP/136.786s**, pytest/tool0,
source1601012a before/after equal/1209 imports0mismatch/provision1original0/
five identities gone/ECHILD/fresh retainedDB driver sessions[]. Missing
publication-before/after references in the first file-only allowlist draft remain
a preparation correction; no captures are invented. [Epoch scope](epochs/epoch-02-native-streaming-completion.md#n22-restored-source-pg62-component-verification--2026-11-06)
distinguishes model/API-ASGI PG component verification from native SDK/TCP,
performance and CI acceptance. JOIN excluded from current integration; P2 and
TaskRun projection retained; all a0f2 candidate experiments remain historical.
CI-runner DAG500 diagnostics source-only/NOT RUN; OLD18124CI17PASS/2DAG5kFAIL
not green/NI2-012…014OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned.


## N2.2 local DAG500 observer rehearsal and reviewed CI diagnostics — 2026-11-06

[Reviewed CI diagnostic implementation](evidence/native-epoch2-dag-volume5000-2026-11-06/dag500-ci-observer-implementation-v1/index.json)
has exact499d/a25 captures/patch, plan/source reviews, normal hooks0/0 and pure
checks. [Actual LOCAL observer rehearsal](evidence/native-epoch2-dag-volume5000-2026-11-06/dag500-observer-rehearsal-v1/index.json):
Principal personal SDK/TCP/OS/PG17 **1 PASS/226.097s**, source1603=legacy012a+
CI extras/import3885 no mismatch/native6health0/provision1original0/13ownedgone/
ECHILD/fresh retainedDB driver sessions[]. Coverage45 snapshots/41complete4Task7part/
all11families; one bootstrap UndefinedTableError is preserved, not observer0errors.
Checkpoint84.07843ms active0/unconfirmed0/threadSTOP and ten pairedPIDCPU partial
windows retain [scope/causality limits](epochs/epoch-02-native-streaming-completion.md#n22-local-dag500-observer-rehearsal-and-reviewed-ci-diagnostics--2026-11-06).
New diagnostic CI **NOT RUN**; old18124CI17PASS/2DAG5kFAIL remains not green.
NI2-012…014OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned; no native5k/CI/perf
acceptance or automatic nextcase. V6 and all prior histories remain unchanged.


## N2.2 published candidate and one hosted DAG500 sync diagnostic — 2026-11-06

[Closed publication/run summary](evidence/native-epoch2-dag-volume5000-2026-11-06/published-dag500-sync-status-v1/index.json):
b0328 published with normal configured hooks0; descendant95fb only adds two
hostedPG17 selection lines, docs/test bodies/budgets unchanged. One manual
run42206832141/attempt1/job127855515745 on **b0328** finished SUCCESS at
14:19:00 Europe/Moscow (11:19:00 UTC): **1 PASS676.29s**, separate observer
SUCCESS/130 samples123 complete. CPU/PG/fullartifact diagnosis remains pending;
no health/lifecycle/performance/CI-cause replay claim. [Scope/source trail](epochs/epoch-02-native-streaming-completion.md#n22-published-candidate-and-one-hosted-dag500-sync-diagnostic--2026-11-06)
keeps earlier NOTRUN history and separate95fb Unit/Static checks. No fullmatrix/
heavy5k/NI2-012…014 acceptance: OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned.


## N2.2 95fb CI status, scoped purge correction and readiness diagnostic controls — 2026-11-06

[Closed finite correction/control packet](evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-ci-correction-controls-v1/index.json):
95fb Unit at15:10:06Moscow (12:10:06UTC) has79success/4sameFAIL/1skip/1active/
9queued; native7success/2sameFAIL/1active/9queued, not green.14:56 snapshot remains
historical.04019041 preserves authorPGRED1PASS1ERROR11.10, authorSQLite4PASS and
PrincipalPERSONALPG4PASS14.00 (JUnit14.35), test-fixture purge only/prodFKs unchanged.
Staticc43f8614 changes only job10→20minutes, no functional budget/hook bypass.

Readiness actual3a32/a44a adds safe failure metadata while retaining20s/200+owned
header/URL/proxy/cleanup. Author6PASS executed84d, **inherited assets23b does not
prebind84d**; actual module/import/source proofs are distinct. Independent
Principal93c correctly prebound/personal6PASS43.09, realowned50320.021s189attempts/
header-match/cancel/child−15, source/import/ownedclosure valid. This verifies
diagnostics, not startup cause/fix; hosted startup/rebalance cause UNKNOWN.
Compact b0328 CPU/PG verdict/local comparison is descriptive, two observer
bootstrap errors retained/noCPUcriticalpath or remedy claim; next profiling
plan-only NOT RUN. [Detailed scope and provenance](epochs/epoch-02-native-streaming-completion.md#n22-95fb-ci-status-scoped-purge-correction-and-readiness-diagnostic-controls--2026-11-06)
preserves V7/compact72line history. No candidateCI/fullmatrix/heavy5k acceptance;
NI2-012…014OPEN/BLOCKED/NI2-020blocked/15%earned/27unearned.


## N2.2 post-hook import source bridge and exact lint policy — 2026-11-06

[Tiny post-hook supplement](evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-posthook-source-bridge-v1/index.json)
retains failed first normal commit and independently bridges only three Ruff
import reorders: same import multiset/all otherAST versus actual tested
preformat captures. Seven tested captures stay exact; sorted final bytes are
not claimed executed. Exact third-path codespell skip/pyproject9fe is lint
policy only; sealed numeric verdict/archives/uv.lock unchanged, no hookSKIP.
[Scope](epochs/epoch-02-native-streaming-completion.md#n22-post-hook-import-source-bridge-and-exact-lint-policy--2026-11-06)
keeps final8paths separate from runtime evidence; no new tests/DB/credit.
Priorf628/V7/compact history remains; NI2-012…014OPEN/BLOCKED/15%earned/27unearned.


## N2.2 final compact lint-policy qualification — 2026-11-06

[Final compact lint supplement](evidence/native-epoch2-dag-volume5000-2026-11-06/95fb-final-lint-policy-v1/index.json)
retains failed retry2/no mutations and only three additional exact archive skip
paths, pyproject9fe→63942325. Independent raw/parsedTOML/lock/code/archive proof;
no rawconfig/context recursion, new tests or runtime credit. Earlier third-skip
note/f628/3981 histories stay exact; NI014blocked/15%earned/27unearned.
