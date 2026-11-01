# Native integration epoch 1: Prefect core streaming foundation

- Status: `progress`
- Date: 2026-10-31
- Owners: ETL Architect / HighLoad Principal, SDK/server/runtime maintainers
- Earned native foundation implementation readiness: **10%**
- Target: **100% of the native foundation scope defined here**
- Basis: [validated prototype theory](../adr/ADR-0031-validated-durable-stream-theory.md)
- First stage: [ADR-0032](../adr/ADR-0032-native-task-stream-contract.md)
- Navigation: [ADR graph](../adr/README.md), [task registry](../todo.md), [sprint board](../sprint_board.md)

## Problem

The durable-stream prototype and its separate Prefect adapter have passed their
selected experimental gates, including the real 72-hour campaign. At this
epoch's definition, Prefect core lacked native durable stream dependencies and
per-input recovery. Stage 1 now implements the selected SDK/task-engine
contract; native persistence and deployment integration remain open. An
adapter connecting two execution models is not the final product requested by
the user.

The target is Ariadne as a native extension of Prefect. Streaming must be rooted
in SDK contracts, graph dependencies, task execution, server lifecycle, storage
and observability, while ordinary flows and tasks preserve their behavior.

## Theory and agreement

The [validated theory](../adr/ADR-0031-validated-durable-stream-theory.md)
supports beginning native integration. Its durability protocol can become a
core capability through explicit stream dependencies and task execution
contracts. Internal module and resource boundaries support this integration;
they do not create another user-facing product or a global flow mode.

This is **native integration epoch 1**, a product-delivery sequence distinct
from historical **protocol epoch 2**. The existing one-terminal-event protocol
continues to apply. Epoch numbering does not change that protocol, rename its
history or transfer its completion percentage to native implementation.

On 2026-10-31 the user directed creation of this epoch and its first-stage ADR.
On 2026-11-01 the user then directed implementation with frequent real-system
tests. The first short sprint now has its own code and evidence below;
documentation alone does not earn implementation readiness.

## Terminology and invariants

| Example or term | Native meaning |
| --- | --- |
| Petya / Петя | Source or upstream Prefect Task, represented by a TaskRun in a FlowRun. |
| Anna / Аня | Downstream Prefect Task/TaskRun consuming the durable stream. |
| Tanya / Таня | A new retry attempt of Anna's same TaskRun and logical consumer. |
| Prefect worker | Deployment execution infrastructure; it is not another name for Anna or Petya. |
| Stream dependency | A native graph edge that makes accepted records available before the producer TaskRun completes. |
| Terminal event per input | The single supported `yield` or `return` observed and durably accepted by the platform for that input. |

The native contract preserves these rules:

1. A TaskRun processes many records; there is no new TaskRun for every document.
2. With `completed_through=199` and input 200 in flight, failure before durable
   terminal acceptance redelivers 200. Durable acceptance followed by a lost
   acknowledgement resumes at 201.
3. Accepted output references, managed state, completion and consumer progress
   commit atomically. Merely reaching Python `yield` is insufficient. Generator
   exhaustion after that terminal yield is not an extra completion condition.
4. A retry retains logical channel/consumer identities, changes fenced execution
   ownership, and rejects stale platform writes. Whole-flow restart and
   infrastructure replacement require separate identity mappings and tests.
5. Source publication has a separate stable source-record/position contract.
   Multiple source emissions are not multiple terminal events for one
   transform input. Python frames or arbitrary local variables are not restored.
6. Large payloads use durable object references before envelope publication.
   Client code retains responsibility for external-effect idempotency and
   determinism under the selected state/recovery contract.
7. Ordinary tasks, futures, generator tasks, result caching and dependency
   behavior keep their existing semantics. Stream edges explicitly carry their
   additional semantics inside the same flow.

## Epoch objective and boundary

Deliver a bounded, native, end-to-end foundation: a normal Prefect deployment
starts a mixed flow containing ordinary finite work, a replayable source Task,
an incremental transform Task and an explicitly bounded terminal consumer.
The source continues emitting while downstream processes earlier records.
The transform recovers its first incomplete input through a real Prefect retry.
The flow can drain, complete, or cancel through supported native lifecycle
paths, with queryable progress and bounded channel retention.

The initial scope may use one ordered partition and partition-local state on
PostgreSQL. Its supported capability profile must be explicit before input is
admitted. SQLite behavior, source connectors, authentication and deployment
constraints must be stated and unsupported stream profiles rejected. Ordinary
SQLite-backed workflows retain their existing support.

Branch/join composition, regional shared-state recovery, arbitrary output
cardinality, parallel partitions, high-load sizing, richer UI and production
rollouts require later scoped decisions. Their prototype evidence is retained
as a reference, not silently inherited by this native foundation.

## Stages

| Stage | Goal and exit | State / governing decision |
| --- | --- | --- |
| 1. Native task and dependency contract | Define stream ports, supported per-input execution, finite dependency boundaries and stable TaskRun retry binding. Native SDK/task-engine tests execute against a real HTTP API and an explicit PostgreSQL test adapter; native migrations and deployment proof follow. | `accepted` for Stage 1 — [ADR-0032](../adr/ADR-0032-native-task-stream-contract.md). |
| 2. Native durable storage | Add versioned platform models, migrations and an internal storage interface for append, terminal completion, fencing, retention and quotas. Prove atomicity and compatibility on the selected PostgreSQL profile. | Subsequent ADR required; [ADR-0018](../adr/ADR-0018-postgresql-authoritative-data-plane.md) is prototype input. |
| 3. Native runtime and source | Connect task execution to real durable storage, persist managed state and source coordinates, and implement supported sync/async execution and same-TaskRun retry. Prove failures before/after terminal acceptance. | Subsequent ADR required; [ADR-0017](../adr/ADR-0017-sync-async-generator-adapter.md) and [ADR-0019](../adr/ADR-0019-production-generator-state-and-replacement.md) are prototype input. |
| 4. Mixed-flow execution | Schedule downstream while producers run, support finite side inputs and explicit bounded collection/end-of-stream, and prevent dependency/capacity deadlocks. Register required consumers before source admission. | Subsequent ADR required; Stage 1 defines the contracts. |
| 5. Server, deployment and operations | Bind native deployment/FlowRun/TaskRun identities, authorization, lifecycle commands, cancellation/drain, worker launch and bounded progress projections. Reject incompatible code/state versions before data admission. | Subsequent ADR required; [ADR-0024](../adr/ADR-0024-control-plane-authority-and-lifecycle.md) and [ADR-0025](../adr/ADR-0025-prefect-adapter-and-operational-projections.md) are transition references. |
| 6. Native foundation acceptance | Run the selected graph through actual SDK/server/worker deployment, task retry and process-loss cases; prove ordinary-flow regression, resource isolation, completion/cancellation and documented compatibility limits. | Subsequent acceptance ADR required; first native gate cannot be replaced by standalone prototype results. |

Stage 1 tests use real Prefect task/dependency code, a hosted HTTP API process
and an explicit real PostgreSQL test adapter. They do not substitute this
adapter for Stage 2 native production-storage atomicity/migrations or Stage 6
real deployment acceptance. Stage exits earn readiness
only after code and their acceptance criteria pass; documentation work alone
does not earn readiness; the current 10% comes from Stage 1 code and evidence.

The initial document cycle did not allocate tasks or sprint shares. On
2026-11-01 the user directed implementation and frequent real-system testing.
The first short implementation sprint is now tracked in [todo](../todo.md) and
[sprint_board](../sprint_board.md), with a planned 10% contribution to this
epoch's foundation objective. That is a planning estimate of problem coverage,
not a throughput or production-readiness measurement. Its Stage 1 criteria and
frozen-source evidence gate have now passed,
earning 10% of this bounded foundation. Later sprint allocation remains open.

## Native ownership and storage boundary

The existing Prefect ownership model is the starting point: the server
orchestrates FlowRun state transitions; the task engine manages TaskRun states
and reports them through native events. New stream lifecycle operations must
fit that model rather than create an independent external run controller.
The server's public API remains the authenticated entry for supported control
operations. Exact task-event/stream-fence reconciliation belongs to the native
lifecycle ADR and its failure tests.

Stream records, cursors, barriers, mailboxes and compaction use an internal
storage interface with appropriate pools, quotas and hot tables. Orchestration
history and UI projections remain bounded instead of recording a state
transition or allocating a TaskRun per document. A control database outage,
stale task event or late owner write cannot authorize stream progress.

## Stories

- As a developer, I want to express ordinary and streaming dependencies inside
  a Prefect flow with explicit, compatible SDK contracts.
- As a user, I want Anna to consume Petya's documents immediately and Tanya to
  resume Anna's unfinished input through a native retry.
- As a system, I want one consistent Prefect lifecycle with atomic stream
  progress, fenced execution and bounded storage.

## Acceptance criteria for the epoch

These criteria apply to the complete foundation with native persistence,
source admission and deployment binding. Stage 1 tests against an explicit
storage adapter establish their scoped contract, not completion of this gate.

- [ ] As a developer, I can run the selected mixed flow through the native SDK,
  server and worker deployment without the experimental adapter coordinating
  an external streaming run.
- [ ] As a user, I can observe downstream processing while the producer TaskRun
  is still running, and terminal completion only after its durable tail drains.
- [ ] As a user, I got input 200 again after pre-acceptance failure and input
  201 after a lost acknowledgement of durable completion of 200.
- [ ] As a system, I can keep the same downstream TaskRun and consumer binding
  across task retry, reject the former execution owner and advance each input
  completion only once.
- [ ] As a developer, I can reproduce the declared source-position, state,
  cancellation, migration and failure behavior with independent invariant
  checks on actual PostgreSQL storage.
- [ ] As a system, I got ordinary finite-flow and generator-task regression
  coverage, bounded metadata and resource checks, and fail-closed refusal of
  unsupported stream capabilities before input admission.
- [ ] As a user, I can inspect progress, in-flight ownership, lag and pressure
  through authenticated native projections and follow a tested recovery guide.

## Evidence and observability graph

The [prototype acceptance](../adr/ADR-0031-validated-durable-stream-theory.md)
links to the original short gates, 72-hour source, reviewer, collector and
public evidence. [Stage 1](../adr/ADR-0032-native-task-stream-contract.md) links
to the implemented native contract and its completed scoped criteria. Each
later stage must add its own ADR, implementation links, executed commands,
configuration and independent evidence before its status advances.

Stage 1 source and real-system tests now exist and are linked from
[ADR-0032](../adr/ADR-0032-native-task-stream-contract.md). Its
[final gate](../evidence/native-stage1-acceptance-2026-11-01.json) has
passed. Native migrations, source admission and deployment evidence remain
future stages. No full-epoch acceptance criterion is checked by this document.

The [initial Principal review](../evidence/native-epoch-1-principal-review-2026-11-01.json)
passed the planning scope and Stage 1 boundary at 0% earned implementation
readiness. Its observation remains unchanged; the implementation gate above
supports the current 10% foundation share.

The [final Stage 1 Principal acceptance](../evidence/native-stage1-principal-acceptance-2026-11-01.json)
reviews both real-system profiles, finite regressions, wheel, source hashes and
scope limits. It accepts N1.1 as done with the planned 10% foundation share.
The [CI integration successor](../evidence/native-stage1-ci-acceptance-2026-11-01.json)
retains initial hosted/static failures and verifies the final committed source
with both real-storage profiles, finite regression, type checks and client
wheel/source builds. It does not extend Stage 1 acceptance to later stages.

## Failure modes and consequences

Implicitly treating a stream as a completed future would block downstream or
change existing result behavior. Recreating channel identity on retry would
lose recovery continuity. Unfenced task events could report a terminal state
while old execution can still publish. Unbounded per-record orchestration
metadata could overwhelm the server. These are explicit design and acceptance
risks for the stages above.

The successful prototype justifies proceeding, while the native foundation
requires its own implementation and proof. Historical ADRs and evidence remain
available; this epoch supersedes only the external-adapter-as-final-product
direction in [ADR-0001](../adr/ADR-0001-runtime-boundary.md) and
[ADR-0025](../adr/ADR-0025-prefect-adapter-and-operational-projections.md).
