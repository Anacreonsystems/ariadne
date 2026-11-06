# Streaming ETL sprint board

Current product cycle: [Native integration epoch 2](epochs/epoch-02-native-streaming-completion.md),
approved implementation of [ADR-0042](adr/ADR-0042-flink-informed-native-streaming-architecture.md)–[ADR-0048](adr/ADR-0048-rescaling-compatibility-and-native-acceptance.md)
at version `fae53c90aa3b44dbc45032833b4d193001e5c8bd`. ADR-0042–ADR-0044 are
`progress`; ADR-0045–ADR-0048 are approved and remain `planning` until their
implementation sprints start. The fixed-P2 N2.1 gate earns **15% native
capability completion**. Task states show activity, while readiness moves only
at independently verified sprint gates.

Historical product cycle: [Native integration epoch 1](epochs/epoch-01-native-prefect.md),
`accepted`, **100% earned native foundation readiness** for the selected scope. The
[first-stage ADR](adr/ADR-0032-native-task-stream-contract.md) is accepted for
its scoped contract gate. Implementation was requested on 2026-11-01 with frequent tests
against the actual runtime and storage.

## Native epoch 2 board

| Sprint | State | Planned cumulative target | Share | Tasks | Exit |
| --- | --- | ---: | ---: | --- | --- |
| N2.1a — Topology and wire | `done` | 5% | +5% | NI2-001, NI2-002 | Paired topology migrations and immutable plan/subtask DTOs pass real PostgreSQL/SQLite/API controls; no data-plane or runtime claim. |
| N2.1b — Scoped authority and store | `done` | 10% | +5% | NI2-003, NI2-004 | Strict subtask-only authentication, existing-store fences, whole-region admission and canonical keys pass cross-resource negatives; codec alone earns 0%. |
| N2.1c — Physical runtime gate | `done` | 15% | +5% | NI2-005, NI2-006 | Four ordinary physical TaskRuns run in separate processes, recover 200/201 and keep metadata constant at 500/5000 under the complete real gate. |
| N2.2a — DAG and routes | `progress` | 21% | +6% | NI2-010, NI2-011 | Typed acyclic graph and four append-materialized exchanges preserve independent edge identities. |
| N2.2b — Fan-out runtime gate | `progress` | 27% | +6% | NI2-012…014 | Source components VERIFIED in declared scopes. JOIN not retained for current integration: no demonstrated benefit; exact two-file rollback to source012a and normal selected hooks0/0, P2/TaskRun projection retained; restored-source PG62: 61 PASS/1 exact SQLite SKIP. Historical JOIN PG68/500/counters/py-spy stay on a0f2; high-loss profile attribution INCONCLUSIVE. OLD 1812460 CI terminal 17 PASS/2 DAG5000 FAIL, not green. NI2-012…014 OPEN/BLOCKED; NI2-020 blocked; earned 15%, 27% unearned. Detailed versions/limits retained below. |
| N2.3a — State and mailbox | `planned` | 32% | +5% | NI2-020, NI2-021 | Journaled state and deterministic multi-input apply order recover without accepted-body replay. |
| N2.3b — Join, outcomes and cuts | `planned` | 37% | +5% | NI2-022…024 | OUTPUT/NO_OUTPUT, bounded correlated join and complete barrier cuts compose atomically. |
| N2.3c — Stateful join gate | `planned` | 42% | +5% | NI2-025 | Native process-loss matrix closes every ADR-0044 criterion. |
| N2.4a — Split source controls | `planned` | 47% | +5% | NI2-030…032 | Enumerator, readers, ordered controls and drain pass source process faults. |
| N2.4b — Event time and timers | `planned` | 51% | +4% | NI2-033, NI2-034 | Watermark/idleness/late-data and timer recovery pass keyed-join controls. |
| N2.4c — Continuous gate | `planned` | 55% | +4% | NI2-035 | Real continuous native matrix closes ADR-0045. |
| N2.5a — Reservations and credits | `planned` | 59% | +4% | NI2-040, NI2-041 | Full-turn reservations and fair pressure avoid overshoot and slot starvation. |
| N2.5b — Retention and observability | `planned` | 63% | +4% | NI2-042…044 | Segments/objects retire only beyond every durable root and expose pressure/pins. |
| N2.5c — Bounded-storage gate | `planned` | 67% | +4% | NI2-045 | Multiple retained-window turnovers reach a measured PostgreSQL plateau. |
| N2.6a — Region planning | `planned` | 71% | +4% | NI2-050, NI2-051 | Immutable RESTORING plans activate one complete region or none. |
| N2.6b — Selective local recovery | `planned` | 74% | +3% | NI2-052 | B-only process replacement keeps healthy siblings and one Flow lease. |
| N2.6c — Cross-host authority | `planned` | 77% | +3% | NI2-053 | A real second host with verified-stop authority and portable references passes; no timeout substitution. |
| N2.6d — Recovery gate | `planned` | 80% | +3% | NI2-054, NI2-055 | Expansion/cancel/activation faults and both placement profiles close ADR-0047. |
| N2.7a — Transfer and assignment | `planned` | 84% | +4% | NI2-060, NI2-061 | Complete transfer manifest and two-to-three key-group assignment activate atomically. |
| N2.7b — Rescale compatibility | `planned` | 87% | +3% | NI2-062, NI2-063 | SDK lifecycle and compatible/incompatible migrations are explicit before user code. |
| N2.7c — Rescale gate | `planned` | 90% | +3% | NI2-064, NI2-065 | Every transfer cut observes one old or new assignment and closes ADR-0048 rescale criteria. |
| N2.8a — End-to-end and load | `planned` | 94% | +4% | NI2-070, NI2-071 | Full topology and declared workload publish honest throughput/latency/recovery/resource limits. |
| N2.8b — Compatibility and operations | `planned` | 97% | +3% | NI2-072, NI2-073 | Regressions, packaging, mixed versions and operator projections pass. |
| N2.8c — Final acceptance | `planned` | 100% | +3% | NI2-074, NI2-075 | Frozen complete matrix and independent Principal evidence graph close all A42–A48 criteria. |

The shares total 100% of the new profile problem and are separate from epoch
1's historical 100% bounded-foundation measure. Current epoch-2 earned target:
**15%**. The [scoped N2.1 Principal acceptance](evidence/native-epoch2-n2-1-principal-acceptance-2026-11-05.json)
earns the fixed-P2 share. The standalone materialization experiment and
documentation approval do not earn a sprint share.

## Native epoch 1 board

| Sprint | State | Planned cumulative target | Share | Tasks | Exit |
| --- | --- | ---: | ---: | --- | --- |
| N1.1 — Native task/stream contract | `done` | 10% | +10% | NI1-001…004 | All ADR-0032 criteria, frozen real HTTP/PostgreSQL gates, finite task/runner regressions, client-wheel smoke and Principal review pass. |
| N1.2 — Native storage transactions | `done` | 20% | +10% | NI1-010…013 | ADR-0033 storage-only criteria: migrations, atomic fenced operations, quotas, final-flow release and real PostgreSQL hostile gate. |
| N1.3 — Native storage transport | `done` | 30% | +10% | NI1-020…022 | ADR-0034 accepted: actual HTTP/client/API restart with retained tokens, lifecycle cleanup, DTO/error parity, stripped-client packaging and Principal gate pass. |
| N1.4 — Native source task runtime | `done` | 40% | +10% | NI1-030…034 | ADR-0035 accepted: sync/async generators publish positioned references; exact reservation/EOF, same-TaskRun retry, process loss, stripped package and Principal gates pass. |
| N1.5a — Durable submission identity | `done` | 40% | +0% | NI1-040…042 | [377-case scoped gate](evidence/native-stage5a-acceptance-2026-11-03.json) and independent review pass; exact HTTP replay and drift/limit refusals. No whole-worker claim. |
| N1.5b — Execution fencing and lifecycle | `done` | 40% | +0% | NI1-043…045 | [1,724-case scoped acceptance](evidence/native-stage5b-acceptance-2026-11-03.json) and independent review: generation fence, signed events and normal retry preparation; no SDK reconstruction. |
| N1.5c — Whole-worker reconstruction gate | `done` | 50% | +10% | NI1-046…048 | [2,457-case frozen acceptance](evidence/native-stage5c-acceptance-2026-11-03.json) and [Principal review](evidence/native-stage5c-principal-2026-11-03.json) pass; five 500-document whole-process cuts retain the same TaskRuns/channels. Bounded selected-launcher profile, not production worker automation. |
| N1.6a — Finite authority and receipts | `done` | 50% | +0% | NI1-050…053 | [corrected scoped acceptance](evidence/native-stage6a-corrected-acceptance-2026-11-04.json), SQLite HTTP adjunct and [Principal review](evidence/native-stage6a-corrected-principal-2026-11-04.json) pass. Server/client authority only; mixed SDK remains N1.6b. |
| N1.6b — Mixed finite/stream runtime | `done` | 60% | +10% | NI1-054…056 | All [ADR-0037](adr/ADR-0037-mixed-finite-and-streaming-tasks.md) criteria, real mixed process matrix and independent review pass. Accepted in the final selected-profile gate. |
| N1.7 — Native declaration and deployment launch | `done` | 70% | +10% | NI1-060…063 | [ADR-0038](adr/ADR-0038-native-deployment-worker-lifecycle.md): immutable Flow/deployment capability, worker enrollment before `Running`, secret-safe launch and actual work-pool gate. |
| N1.8 — Confirmed replacement and lifecycle | `done` | 85% | +15% | NI1-070…074 | Verified Linux process death, one-winner replacement, slot/retry invariants, cancellation and drain pass real process controls. Lease expiry alone cannot authorize takeover. |
| N1.9 — Projections, compatibility and bounds | `done` | 90% | +5% | NI1-080…083 | Authenticated native projections/UI, mixed-version refusals, paired migration compatibility and selected-profile resource bounds pass. |
| N1.10 — Real mixed deployment acceptance | `done` | 100% | +10% | NI1-090…093 | [ADR-0040](adr/ADR-0040-native-integration-acceptance-and-operations.md): actual SDK/server/work-pool/worker deployment, fault/lifecycle matrix, ordinary regressions and independent final review pass for the bounded foundation. |

The 100% records delivery of the agreed bounded native foundation.
It is not a measurement of general production readiness. The [complete Stage 1 gate](evidence/native-stage1-acceptance-2026-11-01.json)
and the [N1.2 storage gate](evidence/native-stage2-closure-acceptance-2026-11-01.json)
now pass. The [N1.3 transport gate](evidence/native-stage3-acceptance-2026-11-02.json)
and [Principal review](evidence/native-stage3-principal-2026-11-02.json)
earned N1.3's planned +10%, reaching 30%.
[N1.4's source gate](evidence/native-stage4-acceptance-2026-11-02.json) and
[Principal review](evidence/native-stage4-principal-2026-11-02.json) now earn the
next planned +10%, reaching 40%. Whole-flow reconstruction is now accepted in
[ADR-0036](adr/ADR-0036-durable-submission-recovery.md) after the
[architecture agreement](evidence/native-stage5-design-review-2026-11-03.md),
implementation and final acceptance, earning N1.5's planned +10% to reach 50%.
N1.5a and N1.5b earn no percentage independently. [PR 6 hosted closure](evidence/native-stage4-hosted-successor-2026-11-03.json)
records N1.4's merge after 97 successful checks and five conditional skips.

The [N1.5 hosted closure](evidence/native-stage5c-hosted-successor-2026-11-03.json)
records PR 9 merged after 97 successful checks/five conditional skips, with
identical tested/merge source. The corrected Python 3.14 and native PostgreSQL
jobs pass; this documentation follow-up earns +0%.

The [N1.6a hosted closure](evidence/native-stage6a-hosted-successor-2026-11-04.json)
records PR 11 merged after 96 successful checks/five conditional skips. Its
corrected 76 source paths survive merge; 423 PostgreSQL and 88 SQLite selected
cases pass without skips. The [N1.6b planning review](evidence/native-stage6b-sdk-design-review-2026-11-04.md)
includes a passing low-level first-seal-in-generation-2 control. Direct mixed SDK
execution and its real process acceptance now pass in the final epoch gate.

N1.6b and the remaining deployment work are now planned as one continuous
route in [ADR-0038](adr/ADR-0038-native-deployment-worker-lifecycle.md).
N1.7 binds a public native capability before worker launch; N1.8 adds verified
local-process replacement plus cancellation/drain; N1.9 adds projections,
compatibility and resource bounds; N1.10 performs the real mixed deployment
acceptance. These planned shares are now earned after final scoped acceptance. Cross-host recovery,
production HA and throughput SLOs remain outside this bounded epoch objective.

N1.9/N1.10 also includes NI1-092a under
[ADR-0041](adr/ADR-0041-owned-graph-lifecycle-during-navigation.md): the actual
TaskRun navigation gate exposed upstream graph callbacks outliving their scene
and an early browser-error snapshot. Owned teardown and the corrected browser
gate must pass before final acceptance. This quality closure has +0% beyond
the existing allocation and does not alter the streaming protocol.

N1.1 follow-up: [final code/CI acceptance](evidence/native-stage1-ci-acceptance-2026-11-01.json)
records both real PostgreSQL-stream profiles, full finite regression, type
checking and wheel/source builds after hosted findings. This earns no extra
percentage beyond the same Stage 1 scope.

## Experimental handoff

The separate DS-052a lane is `done` under
[ADR-0030](adr/ADR-0030-continuous-arc-soak-and-evidence-collection.md). The
[real campaign](https://github.com/Anacreonsystems/ariadne/actions/runs/40672223609)
completed on 2026-10-31; [final evidence](evidence/ci-arc-72h-completed-40672223609.json)
and the [artifact audit](evidence/ci-arc-72h-artifact-audit-40672223609.json)
confirm original review, 18 segments, fresh collection and retrievable upload.
This lane adds no percentage. [ADR-0031](adr/ADR-0031-validated-durable-stream-theory.md)
records successful experimental theory without transferring readiness to native
core integration.

Default cadence is one week with parallel owners. A task sized `L` consumes one
owner for most of the sprint. If staffing cannot satisfy the exit criteria, work
rolls forward and the cumulative target is not claimed.

The historical percentage measures readiness toward protocol epoch 2: yielded
ObjectRef records flow immediately through a platform-owned durable channel,
replacement workers redeliver the first input without a durable downstream
`yield`/`return`, and periodic barriers preserve graph state. Client code owns
determinism and external-effect idempotency.

Historical experimental earned readiness: **100% of the agreed short experimental delivery**. The earlier 53% measured a different protocol
that coupled source acknowledgement, global checkpoint decisions, and external
sink outcomes. Its evidence is retained below but is not counted toward the new
target.

## Protocol epoch 2 board

| Week | State | Planned target | Share | Tasks | Weekly exit |
| --- | --- | ---: | ---: | --- | --- |
| W1 | `done` | 1% | +1% | DS-003a | 22 named real-process crash cases pass without sleeps; Principal PASS recorded. |
| W2 / R0-A gate | `done` | 3% | +2% | DS-003b, DS-003c | Independent oracle and fresh recovery prove document-200 redelivery and all-or-none progress; Principal PASS recorded. |
| W3 | `done` | 4% | +1% | DS-004S | State classes and recovery rule are selected by executable spike and Principal PASS. |
| W4 | `done` | 5% | +1% | DS-004a | Ordered DATA/BARRIER composition, exact runtime replay, unsafe SQL controls, and Principal PASS complete. |
| W5 / R0-B gate | `done` | 6% | +1% | DS-004b | The 34-case real-process matrix, fresh-process convergence, and Principal PASS close R0-B. |
| W6 | `done` | 7% | +1% | DS-005a, DS-005b | Independent branch cursors, bound DATA/BARRIER witnesses, durable aligned-join buffer, and Principal PASS complete. |
| W7 | `done` | 7.5% | +0.5% | DS-005c | Retention roots, full tombstones, lifecycle authority, late-root bounds, and independent oracle pass. |
| W8 / R0-C gate | `done` | 8% | +0.5% | DS-005d | Atomic record/byte credits and block/compact/resume passed Principal review. |
| W9 | `done` | 9% | +1% | DS-006a | Sync generator executes the durable completion contract. |
| W10 | `done` | 9.5% | +0.5% | DS-006b | Async cancellation/timeout and cross-runtime exclusion pass. |
| W11 | `done` | 9.75% | +0.25% | DS-006c | Durable invocation fencing and illegal protocol controls pass. |
| W12 / R0-D gate | `done` | 10% | +0.25% | DS-007 | Principal hostile review passed all R0 evidence. |
| W13 | `done` | 11% | +1% | DS-010a | Production backend and HA assumptions are selected. |
| W14 | `done` | 15% | +4% | DS-010b | Ordered durable append/progress works on the actual backend. |
| W15 / R1-A gate | `done` | 18% | +3% | DS-010c, DS-010d | Fencing and ObjectRef visibility survive races and remount; Principal PASS recorded. |
| W16 | `done` | 21% | +3% | DS-011a, DS-011b | Compaction references and capacity budgets are enforced. |
| W17 | `done` | 24% | +3% | DS-011c | Recovery horizon and catch-up capacity model is reviewable. |
| W18 / R1-B gate | `done` | 30% | +6% | DS-011d | Agreed load SLO passes benchmark, failover, stalled consumer, and Principal review. |
| W19 | `done` | 35% | +5% | DS-020a, DS-020b | Sync and async runtimes execute on production storage. |
| W20 | `done` | 39% | +4% | DS-020c | Runtime atomically connects terminal output/state/progress. |
| W21 / R2-A gate | `done` | 42% | +3% | DS-020d | Exception, cancellation, timeout, and process-loss semantics pass Principal review. |
| W22 | `done` | 47% | +5% | DS-021a, DS-021b | Lease takeover and per-turn managed state compose. |
| W23 | `done` | 50% | +3% | DS-021c | State version migration/rejection is deterministic. |
| W24 / R2-B gate | `done` | 55% | +5% | DS-021d | Repeated Anna/Tanya replacement matrix and Principal review pass. |
| W25 | `done` | 57% | +2% | DS-030a | PostgreSQL coordinator injects BARRIER through the same partition order as DATA and compaction. |
| W26 | `done` | 61% | +4% | DS-030b | Immutable manifests, participant snapshots, create-only completion, and frozen recovery selection pass the independent oracle. |
| W27 / R3-A gate | `done` | 64% | +3% | DS-030c | Fourteen coordinator crashes, 20/20 deterministic takeover repetitions, exact graph/state authority, and Principal PASS close R3-A. |
| W28 | `done` | 68% | +4% | DS-031a | Routed alignment, durable mailbox/terminal turns, abort release, twelve real process-loss boundaries and Principal PASS complete. |
| W29 | `done` | 71% | +3% | DS-031b | B/J/D regional fencing/replay/activation leaves C live; owner takeover, four recovery-process crashes and Principal PASS complete. |
| W30 / R3-B gate | `done` | 75% | +4% | DS-031c, DS-031d | Retained-horizon expansion/refusal, four retirement layers, full 100-cycle census and live catch-up pass 93/93 frozen-source tests and independent Principal review. |
| W31 | `done` | 78% | +3% | DS-040a | Exact authenticated HTTP projections and independent authority oracle passed [ADR-0024](adr/ADR-0024-control-plane-authority-and-lifecycle.md)'s gate. |
| W32 | `done` | 80% | +2% | DS-040b, DS-040d | Serialized leadership fencing, revocable capabilities and exclusive tenant partition ownership pass Principal review. |
| W33 / R4-A gate | `done` | 83% | +3% | DS-040c | Full native graph/bootstrap validation, atomic admission/publication, cancel/drain and restart pass 110/110 tests and independent Principal review. |
| W34 | `done` | 86% | +3% | DS-041a | Actual feature-flagged Prefect SDK/API streaming and artifact identity pass. |
| W35 | `done` | 88% | +2% | DS-041b, DS-041c | Fenced resume, maintenance recovery and correlated telemetry pass. |
| W36 / R4-B gate | `done` | 90% | +2% | DS-041d | 139/139 frozen tests, real HTTP/Chromium runbook drill and Principal PASS close R4-B. |
| W37 | `done` | 91.5% | +1.5% | DS-050a | Durable release/session fencing and mixed-version acceptance in [ADR-0026](adr/ADR-0026-release-compatibility-and-disaster-recovery.md). |
| W38 | `done` | 93.5% | +2% | DS-050b, DS-050c | Migration and infrastructure chaos campaign pass. |
| W39 / R5-A gate | `done` | 95% | +1.5% | DS-050d | Backup/restore, drain, and recovery runbook pass. |
| W40 | `done` | 97% | +2% | DS-051a | Short declared acceptance profile and fault/resource checks pass; DS-052a tracks the separate 72-hour follow-up. |
| W41 | `done` | 98% | +1% | DS-051b, DS-051c | Capacity predictions and credential boundary are verified. |
| W42 / R5-B gate | `done` | 100% | +2% | DS-051d | Final Principal review closes the evidence graph. |

Every row is a one-week sprint assuming at least three parallel owners. The
planned target shows contribution to the target problem; earned readiness moves
only at rows marked `gate`. With fewer owners the dates move and dependent tasks
are never compressed. R5-A has earned 95% for the declared schema-recovery
scope. R5-B has now passed the successor complete short gate and closes the
selected experimental delivery at 100%.
These percentages measure the agreed experimental delivery, not general
production readiness or elapsed calendar weeks.

R5-B evidence: [164-test final report](evidence/postgres-r5b-final2-2026-10-26.json)
and [7 independent controls](evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
close W40–W42. All 126 tested Python/HTML hashes match, source drift is empty,
the remote_apply/sync profile passes, and no unclosed-socket warning occurs.
The actual capacity scenario recovers under live ingress and passes six
independent oracles. [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md)
records the exact limits and retained failed trials.

The [final architecture review](evidence/postgres-r5b-architecture-review-2026-10-26.json)
records `PASS_SHORT_EXPERIMENTAL_100`; the
[final graph validation](evidence/adr-graph-final-2026-10-26.json) closes the
documentation/status check. ADR-0026, ADR-0027 and ADR-0029 are accepted within
their declared scopes; broader production criteria remain open.

R5-A evidence: [159-test report](evidence/postgres-r5a-candidate-2026-10-26.json)
and [architecture review](evidence/postgres-r5a-architecture-review-2026-10-26.json)
close W37–W39 at 95%. The report passes in 736.183 seconds without source
drift on PostgreSQL 17.11 remote_apply/sync; the
[recovery runbook](recovery_runbook.md) records the exact selected procedure
and limits. The successor short capacity and integration changes are covered
by the final R5-B gate above.

R4-B evidence: [139-test report](evidence/postgres-r4b-candidate3-2026-10-25.json)
and [Principal PASS](evidence/postgres-r4b-principal-2026-10-25.json). All 108
Python/HTML hashes match with no drift, synchronous `remote_apply` is intact,
and seven hostile controls passed independent rerun. Earlier interrupted and
failed candidates remain linked in [ADR-0025](adr/ADR-0025-prefect-adapter-and-operational-projections.md).
DS-009 runs in a separate no-percentage research lane. Detailed task acceptance
is in [todo.md](todo.md), governed by
[ADR-0012](adr/ADR-0012-protocol-epoch-2-delivery-gates.md).

DS-052a is a separate `done` lane with no percentage: the continuous
72-hour campaign and publication passed under
[ADR-0030](adr/ADR-0030-continuous-arc-soak-and-evidence-collection.md). Its
[CI status and run instructions](soak_ci.md) distinguish local checks, published
workflows, GitHub smoke evidence and actual long-duration acceptance.

## Historical DS-052a execution observations

The dated entries below preserve their original statuses. The current
completed result and native successor cycle are recorded at the top.

CI follow-up on 2026-10-27: repository transfer to `Anacreonsystems` is verified.
The [static-analysis repair](evidence/ci-static-analysis-repair-2026-10-27.json)
passed hosted pre-commit and type checks. The replacement streaming smoke still
awaits an ARC host; DS-052a remains `progress` with no additional percentage.
The source-hygiene successor gate separately records every failed and passing
trial without rewriting historical acceptance evidence.

The [serial source-hygiene successor](evidence/postgres-ci-style-regression-serial-2026-10-27.json)
passes 164/164 on identical source bytes after the retained first-trial lease
expiry. The selected short-delivery percentage remains 100%; hosted runner and
72-hour acceptance remain separate open criteria.

[Principal acceptance](evidence/ci-source-hygiene-principal-review-2026-10-27.json)
confirms the repair with no blockers and retains the current scoped percentage.

Later ARC acceptance on 2026-10-27: the
[hosted smoke](evidence/ci-arc-smoke-observed-40578404646.json) and
[Principal review](evidence/ci-arc-smoke-principal-review-2026-10-27.json)
pass after the shared-directory and short-resource-policy repairs. Existing
runner access and the one-job smoke are verified. DS-052a remains `progress`
for the unstarted static-runner 72-hour campaign; no percentage is added.

Post-merge dispatch on 2026-10-27: the
[long-profile run](https://github.com/Anacreonsystems/ariadne/actions/runs/40601620025)
is queued on the merged `main` source. Its
[dispatch observation](evidence/ci-long-soak-dispatch-40601620025.json)
does not yet verify runner assignment or workload startup. DS-052a remains
`progress` with no percentage added.

2026-10-28 retrigger: the [unassigned static campaign was cancelled](evidence/ci-long-soak-cancelled-40601620025.json)
at the user's retrigger request; the
[fresh ARC smoke](evidence/ci-arc-smoke-observed-40650444560.json) passes on
merged `main`. DS-052a remains `progress` without additional percentage;
actual 72-hour execution and any ARC long-profile validation remain open.

R1/R2 gate evidence is recorded in
[ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md),
[ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md), and
[ADR-0020](adr/ADR-0020-bounded-metadata-and-idempotency-horizon.md). The final
candidate passed 22/22 backend and 7/7 runtime tests. Two consecutive frozen-hash
load runs passed the agreed 510 records/s workload, latency, catch-up,
compaction, capacity, and independent-oracle checks; release-3 HA preserved all
acknowledged records and recovered a contiguous 1…200 suffix. The ETL/HighLoad
Principal independently reran the suites and recorded PASS with no blockers on
2026-10-24. As a developer, I can run sync and async streaming tasks on the
selected authority; as a user, I got first-incomplete redelivery through
replacement; as a system, I got bounded durable progress and stale-owner
fencing under the measured profile.

R3-A evidence is recorded in
[ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md) and the
[frozen report](evidence/postgres-barrier-r3a-2026-10-25.json). The gate passes
10/10 focused tests, 14 abrupt coordinator boundaries, 20/20 isolated takeover
repetitions, and the 39/39 PostgreSQL regression suite. ETL/HighLoad Principal
closed seven hostile findings and recorded PASS on 2026-10-25.

R3-B evidence is recorded in
[ADR-0022](adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md),
[ADR-0023](adr/ADR-0023-bounded-retention-and-replay-horizons.md), the
[final frozen report](evidence/postgres-r3b-final2-2026-10-25.json), and the
[Principal PASS](evidence/postgres-r3b-principal-2026-10-25.json). All 93 tests
pass in 336.425 seconds with 39 matching source hashes and no drift. Four
independent targeted review cases also pass. One hundred integrated
data/checkpoint/regional-recovery/retirement cycles retain exactly 64 logical
rows and at most 30,452 JSON bytes under the declared 512-row/256-KiB profile.
Source rewind and reconstruction of absent durable bytes are unsupported;
invalid physical authority receives a named atomic refusal. As a user, I got
healthy-sibling continuity and first-incomplete continuation through regional
replacement; as a system, I got bounded retained history with exact recovery
authority. This earns 75% and does not constitute production acceptance.

## Historical protocol epoch 1 portfolio

| Sprint | State | Target | Increment | Tasks | Exit decision |
| --- | --- | ---: | ---: | --- | --- |
| 0 — Architecture foundation | `done` | 15% | +15% | ST-001…007 | ADR graph, prototypes, counterexamples, and Principal PASS exist. |
| 1A — Atomic progress foundation | `done` | 20% | +5% | ST-010, ST-011, ST-014 foundation | Source ACK cannot advance beyond one immutable combined checkpoint/sink decision; Principal PASS recorded. |
| 1B — Safety contract freeze | `done` | 27% | +7% | ST-010…014 completion | Identities, state machines, participant contracts, failure vocabulary, and oracle passed Principal review. |
| 2 — Barriers and fencing | `done` | 43% | +16% | ST-020…025 | Actual barriers survive process loss; recovery regions are explainable; stale owners are fenced. |
| 3A — Cooperative sink safety | `done` | 49% | +6% | ST-030, ST-031, ST-032 slice | Versioned cooperative sinks abort before and converge after an authoritative decision; Principal PASS recorded. |
| 3B1 — Source acceptance and replay | `done` | 53% | +4% | ST-033a…d | Source acknowledgement is tied to exact durable input coverage and replay cuts; Principal PASS recorded. |
| 3B2 — Tiers and formal decision model | `progress` | 58% | +5% | ST-034…035 | Tier behavior is explicit and the end-to-end decision has bounded formal evidence. |
| 4 — Storage and high load | `blocked` | 72% | +14% | ST-040…045 | Backend and operating envelope are selected from crash/load evidence. |
| 5 — Experimental integration | `blocked` | 88% | +16% | ST-050…055 | Production-shaped experimental runtime and server control state pass contract tests. |
| 6 — External acceptance | `blocked` | 100% | +12% | ST-060…065 | Real adapters, crash/soak/upgrade evidence, runbook, and ADR status review pass. |

## Sprint 1A — Atomic progress foundation — `done`

Goal: establish the minimum end-to-end invariant before freezing all participant
contracts.

- **Identity lane:** canonical graph, checkpoint, generation, and record lineage.
- **Decision lane:** one immutable record binds checkpoint state, sink decision,
  and source position.
- **Evidence lane:** independent oracle and unsafe split-write control.

Exit criteria:

- As a developer, I can derive identical checkpoint and transaction identities
  after restart and enumerate their durable transitions.
- As a user, I got no source position reported as acknowledged while its required
  sink outcome is undecided.
- As a system, I can prove in the modeled crash points that acknowledged progress
  never exceeds the highest immutable combined decision.

Evidence: 50 standard-library prototype tests pass. The integrated suite runs the
actual protocol through 27 boundary/offset crash points plus a dedicated 27-case
legal phase-edge matrix, store reopen, external-effect retry,
recovery, and an independent oracle. It also checks exhaustive transition rules,
CAS conflicts and epoch takeover, capability tiers, opaque source-position
vectors, restorable-artifact manifests, canonical external transaction IDs,
owner-aware artifact identity, a global stable-record payload registry, foreign
keys and schema triggers that preserve decision/ACK ordering and immutable
intent, complete expected sink effects, fencing, and unsafe controls. The 20%
target passed ETL/HighLoad Principal review on 2026-10-20 and is now claimed.
This remains local prototype evidence rather than a production guarantee.

## Sprint 1B — Safety contract freeze — `done`

Goal: remove implicit safety assumptions before expanding the prototypes.

- **Protocol lane:** ST-010 stable identities → ST-011 checkpoint state machine.
- **Participant lane:** ST-010 → ST-012 versioned capability contracts.
- **Evidence lane:** ST-011 + ST-012 → ST-013 deterministic fault API;
  ST-010 + ST-011 → ST-014 safety oracle.

Exit criteria:

- As a developer, I can derive stable identities and implement a versioned
  participant contract.
- As a user, I got an explicit validation error for an unsupported guarantee.
- As a system, I can reject illegal state transitions and compare recovery with
  an independent oracle.

Implementation and tests exist for ST-010…014. ETL/HighLoad Principal review
passed on 2026-10-20, so the cumulative 27% target is claimed. Production claims
remain gated by later sprints.

## Sprint 2 — Barriers, recovery regions, and fencing — `done`

Goal: replace simulated snapshots with ordered barriers, real process loss, and
ownership protection.

- **Channel lane:** ST-020 barrier records → ST-021 aligned joins.
- **Recovery lane:** ST-020 → ST-022 recovery planner.
- **Ownership lane:** ST-023 generation leases and fencing.
- **Evidence lane:** ST-024 subprocess harness and ST-025 TLA+ model.

Exit criteria:

- As a system, I can checkpoint a join without mixing barrier sides.
- As a user, I can see the selected recovery region and its replay cuts.
- As a system, I got stale-writer exclusion in executable and bounded formal
  models.

Current evidence: `barrier_protocol.py` models ordered `DATA` and `BARRIER`
records, aligned joins with durable post-barrier buffers, immutable historical
replay identity, recovery cuts, trusted-clock leases, and generation fencing.
The full suite passes 66 tests. Its subprocess matrix executes 21 abrupt exits
across data/barrier append, partial alignment and snapshot, post-barrier buffer,
takeover, restore, and combined decision; fresh channel, join, and coordinator
instances verify recovery. The bounded TLA+ model generates 9,471 states and
finds 2,132 distinct safe states through depth 17; mixed-checkpoint and missing-
fence mutations produce the expected named counterexamples. ETL/HighLoad
Principal review passed on 2026-10-20, so the cumulative 43% target is claimed.
This remains prototype and bounded formal evidence rather than a production
guarantee.

## Sprint 3A — Cooperative sink safety — `done`

Goal: join accepted input, checkpoint completion, and external outcomes into one
recoverable protocol.

Hostile review of the Sprint 1 model found missing abort/recover contracts,
participant-side fencing, storage decision guards, and blocked-participant
diagnostics. Sprint 3A closes those gaps through the serialized path sink state
machine → decision constraint → recovery failure evidence. ETL/HighLoad
Principal review passed on 2026-10-20, so the cumulative 49% target is claimed.

- **Sink lane:** ST-030 protocol → ST-031 combined decision → ST-032 failures.
- **Source lane:** ST-020 + ST-031 → ST-033 acknowledgement/replay contract.
- **Conformance lane:** ST-034 tier suites.
- **Formal lane:** ST-035 multi-sink decision model.

Sprint 3B continues ST-032 and implements ST-033…035 after the 3A contracts
freeze. It remains blocked by the Sprint 3A gate.

Exit criteria:

- As a system, I can abort before `COMMIT_DECIDED` and only commit afterward.
- As a developer, I can run the durable cooperative-sink contract suite.
- As a user, I can see blocked participants, durable retry count, last error,
  owner, fence, and recovery outcome.

Evidence: the 92-test standard-library suite covers durable cooperative-sink
tokens and transaction fences, immutable manifests, authoritative COMMIT and
ABORT decisions, cleanup past an unavailable participant, lost acknowledgements,
owner takeover, durable attempt starts, terminal-state reconciliation, and
rejection of cross-role identity collisions. Recovery diagnostics expose owner,
fence, attempt count, outcome, blocked state, and last error. This is prototype
evidence; production acceptance and ST-033…035 remain open.

## Sprint 3B — Source, tiers, and formal model — `progress`

Goal: bind acknowledged input to durable channel coverage, define weaker sink
tier behavior, and model the irreversible multi-sink decision.

- **Source lane:** ST-033 acceptance, durable handoff, replay, and ACK contract.
- **Conformance lane:** ST-034 behavioral suites for tiers `T`, `I`, and `B`.
- **Formal lane:** ST-035 safe multi-sink model plus missing-decision and
  missing-fence controls.

The cumulative 58% target remains unclaimed until all three lanes pass hostile
review.

The first gate is **Sprint 3B1 / 53%**: ST-033a contract and cuts → ST-033b
atomic acceptance/handoff → ST-033c ACK and replay recovery → ST-033d crash and
oracle evidence. ST-034 and ST-035 remain in Sprint 3B2 for the last 5%.

Sprint 3B1 passed on 2026-10-21. Sprint 3B2 now splits into **3B2A / 55%** for
ST-034 tier conformance and **3B2B / 58%** for the tier `T` formal decision
model. ST-034 runs 034a contract → 034b/034c/034d tier evidence → 034e mixed-tier
oracle. ST-035 starts only after the tier `T` model boundary and invariant names
are frozen.

ST-034a passed Principal review on 2026-10-21. The immutable tier intent,
`BPolicy`, effect-authority field, and observation vocabulary are frozen for the
prototype. ST-034b…e remain open, so earned readiness stays at 53%.

Exit criteria:

- As a user, I got no acknowledged-input loss for enumerated source failures.
- As a user, I can see the enforced sink tier and partial-visibility semantics.
- As a system, I got bounded model evidence for one irreversible decision per
  immutable sink manifest and named counterexamples for missing guards.

## Sprint 4 — Durable storage, backpressure, and load — `blocked by Sprint 3`

Goal: select storage and capacity limits from measurements.

- **Transport lane:** ST-040 Go segmented log → ST-043 bounded credits.
- **State lane:** ST-041 snapshot formats.
- **Retention lane:** ST-042 reference-safe garbage collection.
- **Evidence lane:** ST-044 benchmarks → ST-045 storage/capacity decision.

Exit criteria:

- As a system, I can recover durable segments and never delete referenced data.
- As a system, I can bound memory and storage under slow-sink and catch-up load.
- As a user, I got measured throughput, latency, checkpoint, replay, and overload
  limits.

## Sprint 5 — Experimental production integration — `blocked by Sprint 4`

Goal: implement the validated protocol behind an experimental feature flag.

- **Runtime lane:** ST-050 sync/async `yield` runtime.
- **Coordinator lane:** ST-051 durable coordinator → ST-052 worker fencing.
- **Server lane:** ST-053 schemas and internal APIs.
- **Operations lane:** ST-054 observability.
- **Evidence lane:** ST-055 production contracts mapped to prototype and TLA+.

Exit criteria:

- As a developer, I can run the canonical branched `yield` DAG through Prefect.
- As a system, I can restart coordinator and workers without selecting incomplete
  state or accepting stale writes.
- As a user, I can observe lag, alignment, replay, recovery, and sink decisions.

## Sprint 6 — External acceptance and release gate — `blocked by Sprint 5`

Goal: establish production evidence and decide ADR statuses.

- **Adapters:** ST-060 replayable source and reference tier `T`/`I` sinks.
- **Correctness:** ST-061 branched-DAG crash matrix.
- **Load:** ST-062 soak, host loss, sink outage, and catch-up.
- **Lifecycle:** ST-063 drain, cancellation, upgrade, and state migration.
- **Operations:** ST-064 runbook and guarantee documentation.
- **Governance:** ST-065 acceptance index and ADR review.

Exit criteria:

- As a user, I got failure-free-equivalent logical output for every supported
  guarantee tier and documented capacity objective.
- As a system, I can safely reject or migrate incompatible recovery state.
- As a developer, I can trace every `accepted` criterion to production evidence.

## WIP and sequencing rules

- Maximum three active high-risk tasks across the portfolio.
- A sprint may begin preparatory work early, but its target percentage is earned
  only after all exit criteria pass.
- Production storage selection waits for ST-044 measurements.
- Source acknowledgement waits for ST-031 combined decision semantics.
- Production coordinator implementation waits for ST-025 and ST-035 formal
  evidence.
- ADR status changes occur in ST-065 and never follow prototype success alone.

Detailed dependencies, deliverables, and task-level acceptance criteria are in
[todo.md](todo.md).

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

## N2.2a metadata component acceptance — 2026-11-05

The [NI2-010 Principal acceptance](evidence/native-epoch2-ni2-010-principal-acceptance-2026-11-05/acceptance.json)
accepts the topology-only DAG component. Protected PostgreSQL seal/read,
generic fixed-parallelism topology, paired migrations, whole-region metadata
admission and explicit execution fences passed on the frozen candidate. The
public surface still exposes only seal/read; DAG start, wait, DATA routing and
terminal cleanup remain unavailable.

NI2-010 therefore moves to `accepted` with zero earned epoch percentage.
N2.2a stays `progress` at the previously earned 15% cumulative value because
its +6% exit also requires NI2-011 routed exchanges. The
[NI2-011 implementation handoff](evidence/flink-architecture-2026-11-04/ni2-011-implementation-handoff-2026-11-05.json)
starts that dependency without changing ADR-0043 from `progress`.

## N2.2a finite route component acceptance — 2026-11-05

The [NI2-011 Principal acceptance](evidence/native-epoch2-routed-server-2026-11-05/principal-v1/acceptance.json)
accepts the bounded routed-storage component: deterministic route planning,
atomic PAYLOAD/REFERENCE persistence, exact replay receipts, independent edge
cursors, paired migration pins and a legacy-prefix fence. The final independent
PostgreSQL/SQLite and strict-type gates passed without skips or source drift.

NI2-011 therefore moves to `accepted`. N2.2a and ADR-0043 remain `progress`,
and earned readiness remains 15% while the complete hosted publication matrix
is pending. Public DAG execution and lifecycle stay fenced for NI2-012 through
NI2-014.

## N2.2 finalization gate update — 2026-11-05

The [current epoch evidence note](epochs/epoch-02-native-streaming-completion.md#n22-retained-verification-and-finalization-correction--2026-11-05)
records the historical terminal-PREPARE and exchange-mode PASS successors,
both personal 5000 lifecycle failures and the separate ordinary Task retry.
The [retained finalization archive](evidence/native-epoch2-finalization-2026-11-05/index.json)
and [four-index causal proof](evidence/native-epoch2-route-cleanup-indexes-2026-11-05/component-v1/index.json)
do not replace corrected native 5000 execution or full frozen-union acceptance.
N2.2 remains `progress`, with 15% earned; the 27% target is unearned.

## N2.2 mid-run liveness hold — 2026-11-05

The [current epoch note](epochs/epoch-02-native-streaming-completion.md#n22-mid-run-liveness-hold--2026-11-05)
records the interrupted corrected-candidate 5000 attempts. The
[liveness archive](evidence/native-epoch2-midrun-liveness-2026-11-05/index.json)
preserves actual renewal/read HTTP failures, the same-PID C0 Task retry without
accepted-prefix replay, exact source closure and controlled stop. Final 5000
assertions were not reached. The [renewal component](evidence/native-epoch2-dag-renewal-liveness-2026-11-05/component-v1/index.json)
is a scoped lock correction; the separate partition-read cause and subsequent
native/hosted gates remain open. N2.2 is `progress`, with **15% earned** and the
27% target unearned. No ADR status is accepted by this note.


## N2.2 latest probe/CI status — 2026-11-05

The [current epoch observations](epochs/epoch-02-native-streaming-completion.md#n22-bounded-read-probe-and-hosted-observations--2026-11-05)
separate the historical d504 GC failure, latest `835b8c2` controlled interruption,
scoped renewal correction, bounded read non-reproduction and hosted failures.
The planned next normal 1000 probe earns no completion credit by being planned.
N2.2 stays `progress` at 15% earned; native and hosted exits remain open.


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
