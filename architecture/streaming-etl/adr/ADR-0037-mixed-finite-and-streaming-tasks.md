# ADR-0037: Finite tasks around a native streaming graph

- Status: `accepted`
- Date: 2026-11-04
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- Owners: Task/Flow SDK, native lifecycle and storage maintainers; ETL Architect / HighLoad Principal
- Depends on: [ADR-0036](ADR-0036-durable-submission-recovery.md), [ADR-0035](ADR-0035-native-source-task-runtime.md)
- Delivery: [task registry](../todo.md), [sprint board](../sprint_board.md)

## Problem

N1.5 reconstructs a finite linear stream graph after a whole flow-engine process
is lost. Its enrolled execution rejects ordinary finite Task calls. A developer
cannot yet prepare configuration with an ordinary task, stream documents, then
run an ordinary final summary task in that same FlowRun.

The restriction is enforced in both SDK and server. Task creation, lifecycle
writes and the existing TaskRun recorder require a sealed stream manifest and
accepted EOF before task success. Merely relaxing an SDK guard would run user
code whose state events the server must discard, or would let old-generation
finite events escape the execution fence.

## Theory

Finite tasks can share the same execution authority if they have their own
immutable submission binding and durable completion receipt. They do not need
a stream or a fictitious EOF. Their normal TaskRun identity, retry policy and
state events remain part of Prefect orchestration. The receipt is persisted
before TaskRun success and provides the recovery result after process loss.

A finite prefix also introduces a failure boundary before any stream graph
exists. Its complete immutable binding must be a recoverable plan anchor before
its user code starts. Takeover and preparation must understand that anchor;
otherwise a prefix crash would strand the FlowRun behind N1.5's intentional
unsealed-graph refusal.

## Proposed solution and agreement

The ETL Architect / HighLoad Principal and implementation owner agree to split
this decision into two short delivery slices. N1.6a implements and accepts the
server/client authority foundation, earning +0%. N1.6b integrates ordinary sync/
async Task invocation and the complete mixed-flow process matrix; only the
combined accepted decision earns the planned +10%, reaching 60% of the bounded
native foundation. N1.6b is now accepted within the final epoch gate.

The first profile is exactly one finite prefix, one finite linear source/operator
stream graph, and one finite tail. It uses existing `@flow` and `@task` concepts,
stable call-site keys and explicit code versions. It requires top-level functions,
strict finite JSON parameters/results and durable references for large objects.
Finite results are bounded to 64 KiB. All submission material shares the existing
1 MiB FlowRun manifest budget.

### Separate finite authority

A finite submission binds its phase (`PREFIX` or `TAIL`), stable key, TaskRun,
code/parameter/policy witness and creation generation. Reservation atomically
creates its normal Pending TaskRun. Exact replay returns the same identity;
conflicting witnesses perform no mutation. Separate finite tables preserve the
meaning of existing stream manifests, stream IDs and EOF.

Completion atomically persists canonical JSON, its digest, operation identity
and consumed attempt under current execution authority. Lost acknowledgements
replay the same receipt. Changed output, attempt or operation conflicts. A finite
TaskRun can report success only when its matching receipt already exists.
External effects and their idempotence remain client responsibilities under
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

### Staged plan and recovery

Prefix reservation seals its complete immutable plan anchor before user code
can start. Confirmed process death can authorize a newer generation at that
anchor even with zero stream submissions. Preparation retains TaskRun IDs,
reconciles completed receipts without re-entering user code, and applies normal
retry budget/delay to unfinished finite work with `force=False`.

Native stream declarations require the exact completed prefix witness. Graph
seal binds that witness along with the ordered stream manifest. The mixed
profile requires full leaf completion before tail admission. The server checks
all stream EOFs and absence of in-flight work. Flow success requires the tail
receipt and those same durable stream outcomes.

Finite state events use the current generation, holder, binding digest and
existing signed TaskRun-event protocol. The recorder and HTTP lifecycle guard
validate the appropriate finite receipt or stream EOF. A former generation
cannot create, complete or report state for finite work after takeover. Current
late success can reconcile metadata only when covered by durable completion.

### Compatibility and limits

The existing stream-only v1 request behavior and accepted N1.5 recovery remain
unchanged. Mixed requests carry explicit finite witnesses. Unsupported SQLite
mutations refuse before writes; paired schema migrations still round-trip.
Whole-flow release retires finite payloads and retains the named replay-horizon
witness. No online GC or unbounded retention guarantee is added.

N1.6b initially supports direct finite calls only. Finite submit/map/deferred
execution, nested tasks, arbitrary finite chains, mid-graph finite calls,
TAIL admission based only on partial collection, branch/join and production worker automation remain later
scopes. Unsupported calls before graph seal must refuse before admission and
DATA. A TAIL call after bounded collection must refuse while any leaf remains
OPEN, before its TaskRun/body; DATA may already exist when collection returns.
If all EOFs become durable before TAIL reservation, that EOF condition permits
admission. Collection size alone never proves EOF.

## Stories

- As a developer, I want ordinary preparation and summary tasks around the
  streaming portion of one Prefect flow.
- As a user, I want a replacement process to reuse the prefix result and resume
  the first incomplete document before running the tail.
- As a system, I want finite task completion and events to obey the same durable
  execution-generation authority as stream writes.
- As a developer, I want bounded, named refusals for changed witnesses and
  unsupported mixed dependency patterns before user work starts.

## Acceptance criteria

### N1.6a: server/client foundation, +0%

- [x] As a developer, I can reserve or exactly replay a finite binding with one
  TaskRun and no allocated stream, while code/parameter/policy/phase drift
  performs no mutation.
- [x] As a system, I can recover from the sealed prefix anchor before any stream
  exists, preserving task identity and normal retry limits.
- [x] As a user, I got the exact JSON completion result after a lost receipt
  response; conflicting completion cannot replace it.
- [x] As a system, I can reject finite TaskRun success without its receipt and
  reject old-generation HTTP writes or signed events through the real recorder.
- [x] As a system, I can refuse stream admission without the prefix witness,
  tail admission before all EOFs, and Flow success before the tail receipt.
- [x] As a developer, I got bounded manifest/result quotas, release/horizon
  behavior, paired dialect migrations and fail-closed SQLite controls.
- [x] As a developer, I got paired sync/async HTTP clients and client-only DTOs
  verified against the actual API and PostgreSQL with independent row checks.
- [x] As a system, I got unchanged stream-only and ordinary orchestration
  regressions plus independently reviewed source-bound evidence.

### N1.6b: mixed SDK/runtime, planned +10%

- [x] As a developer, I can run sync and async finite preparation, incremental
  native processing and finite summary using ordinary Flow/Task engines.
- [x] As a user, I got the same four TaskRun identities and two channels after
  prefix receipt loss, document-200 process loss, and tail receipt loss.
- [x] As a user, I got the completed prefix skipped, first-incomplete document
  replayed, and the tail run or skipped according to its durable receipt.
- [x] As a system, I can refuse a TAIL after partial collection while any leaf
  remains OPEN, before creating its TaskRun or entering its body, even when
  prior DATA exists; durable EOF before reservation permits admission.
- [x] As a system, I can refuse mid-graph finite calls and unsupported finite
  entrypoints before creating that call's TaskRun, while the unsealed graph
  still admits no stream DATA.
- [x] As a developer, I got real 500-document HTTP/PostgreSQL/process evidence,
  ordinary finite-flow regression and independent final acceptance before 60%.

## Evidence

The [N1.5 hosted closure](../evidence/native-stage5c-hosted-successor-2026-11-03.json)
records the accepted predecessor. The current source boundaries are
[Task entrypoint guards](../../../src/prefect/tasks.py),
[execution authority](../../../src/prefect/server/models/stream_executions.py),
[TaskRun recorder](../../../src/prefect/server/services/task_run_recorder.py) and
[graph reconstruction](../../../src/prefect/_internal/stream_reconstruction.py).
They establish the problem and compatibility baseline, not N1.6 acceptance.
New experiments, disagreements, commands and outcomes will be linked here.

The [rejected recursive null filter](ADR-0097-recursive-null-omission-in-recovery-responses.md)
records why finite result null must remain explicit in recovery responses.

## N1.6a scoped acceptance

The [scoped acceptance](../evidence/native-stage6a-acceptance-2026-11-04.json)
and [Principal review](../evidence/native-stage6a-principal-2026-11-04.json)
pass sixteen profiles on an identical 76-file source manifest: 2,519 cases,
2,499 passed, seven existing expected failures and 13 existing skips, zero
failures/errors or source drift. The client pass count includes one known
non-strict XPASS; native profiles have no skips. The
[captured compiler](../evidence/native-stage6a-acceptance-compiler-2026-11-04.txt)
checks every selected ledger, raw log, JUnit, marker and source hash.

The finite HTTP profile passes 17 cases: eight concurrent exact reservations
create one TaskRun and no channels; sync/async completion survives API restart;
object/null receipts survive actual client process loss and prefix-only takeover;
normal and signed late failure cannot advance an accepted result; tail/Flow
success require their durable outcomes. Independent JSON/SHA computations
verify returned receipt/result witnesses. The
[additional SQLite HTTP probe](../evidence/native-stage6a-sqlite-api-adjunct-2026-11-04.json)
exercises both new mutation routes against a fresh migrated server, returns typed
`UNSUPPORTED_BACKEND`, and independently observes zero TaskRun/finite/channel
rows. Its captured probe and unchanged before/after 76 hashes are separate
from the common profile manifest.

The 209-case prior native HTTP/client/process/package profile and 17 whole-flow
cases also pass, including the existing five 500-document recovery cuts. These
remain stream-only SDK regression; they do not claim the new mixed SDK runtime.
The 36 legacy contract cases intentionally retain their prototype-backed test
adapter. Normal task/flow/client/API/recorder and mapped lease-retention
regressions pass. The kernel retains 21 known CPython resource-tracker diagnostics,
matching Stage 4's count (versus 18 in N1.5c); no clean-teardown claim is made.

The [original static gate](../evidence/native-stage6a-static-2026-11-04.json)
records whole CI type analysis (1,308 files, zero errors, 138 baseline warnings),
required hooks and Node 24.20 UI check/lint/build/323 API tests. Its public-type
PASS was incorrect: the retained log contains a missing attribute annotation
error even though the displayed completeness score rounds to 100%. The original
static, Principal and acceptance records remain historical; their public-type
qualification is superseded by the correction below.
Paired revisions are PostgreSQL `e41c7b6d2a90` and SQLite `a12d8e4f6c30` under
the [migration notes](../../../src/prefect/server/database/_migrations/MIGRATION-NOTES.md).
The [finite store](../../../src/prefect/server/models/finite_submissions.py),
[receipt witness](../../../src/prefect/server/models/_stream_receipts.py) and
[paired clients](../../../src/prefect/client/orchestration/_streams/client.py)
are implemented in the existing platform.

[Negative findings and retention](../evidence/native-stage6a-negative-findings-2026-11-04.json)
keep root discovery failures and distinguish reported exploratory server
assertion mistakes without saved raw files. Those observations are not rebuilt
or promoted as acceptance. Review also corrected timestamp/self-digest coupling,
recursive null omission and receipt poisoning by late failure. The final recorded
profiles test the corrected code.

This closes N1.6a only: NI1-050…053 are done, parent ADR remains `progress`, and
earned foundation stays 50% (+0%). Direct mixed finite Task execution and the
four-TaskRun/two-channel 500-document matrix remain N1.6b. Existing SDK finite
entrypoint refusals are intentionally retained until that integration passes.

## Static acceptance correction

The [hosted failure](../evidence/native-stage6a-hosted-typecheck-initial-2026-11-04.log)
exposed the missing explicit type of `NativeFiniteStore.streams`. Reading the
original local raw log confirmed the same error had already been present.
Rounded type completeness alone is therefore rejected as an acceptance test:
the process must exit zero and report no error diagnostics.

The [annotation bridge](../evidence/native-stage6a-test-typing-bridge-v2-2026-11-04.json)
records three annotation-only source changes, unchanged normalized executable
bodies, 73 identical frozen paths, and 92 passing focused cases: six PostgreSQL
model cases plus 43 migration cases on each dialect. Its conditional verdict
does not grant static acceptance. Test parameter and helper return annotations
also satisfy the test-suite Standards. No SDK admission rule changes.

The [corrected static check](../evidence/native-stage6a-static-correction-2026-11-04.json)
requires actual exit status and diagnostics. The
[corrected acceptance](../evidence/native-stage6a-corrected-acceptance-2026-11-04.json)
qualifies the original functional evidence through this explicit source bridge
and supersedes the erroneous static approval. The original 2,519-case run is
not relabelled as a full rerun of the successor source. Earned readiness stays
50%, and this ADR stays `progress` until N1.6b passes.

## Hosted closure and next runtime review

[PR 11](https://github.com/Anacreonsystems/ariadne/pull/11) merged after 96
successful checks and five conditional skips. The
[hosted closure](../evidence/native-stage6a-hosted-successor-2026-11-04.json)
binds all 76 corrected source hashes to the identical tested and merge trees.
The PostgreSQL 17 job passes 423 cases: 36 retained adapter contracts, 76 native
storage cases, 250 SDK/HTTP/process/package cases, 18 API cases and 43 migrations.
The SQLite job passes 88 cases. Both selected jobs have zero skipped cases.
Hosted public-type verification and whole CI type analysis pass; ordinary
Python 3.14 execution passes 1,617 cases with its declared skips/expected failures.
The post-merge main workflow is a separate run, not part of this closure.

The [N1.6b planning review](../evidence/native-stage6b-sdk-design-review-2026-11-04.md)
and [publication record](../evidence/native-stage6b-planning-evidence-2026-11-04.json)
retain a withdrawn hypothesis and its correction. A staged server recovery
protocol was initially proposed for an unfinished PREFIX. Source review showed
that the existing routes permit prefix-only preparation followed by the first
stream seal in a recovered generation. The
[actual HTTP/PostgreSQL control](../evidence/native-stage6b-prefix-recovery-adjunct-2026-11-04.json)
then passed that path: confirmed process death, same-prefix retry/receipt, first
generation-2 seal, one DATA record, EOF, TAIL receipt and Flow success. The probe
and raw output are separately bound; all 76 accepted source paths remain equal.

N1.6b must select first seal versus recovery from durable graph state, discover
existing tail witnesses before preparing the immutable recovery plan, and
persist each finite receipt inside the ordinary task engine before success.
These are planned SDK integration requirements. The additional protocol control
does not implement direct finite Task calls, the full mixed 500-document matrix,
or any new readiness percentage. NI1-054…056 remain planned.

Final review also separates refusal guarantees by phase: partial collection
already implies DATA, so its refusal cannot promise zero earlier stream DATA.
Its acceptance criterion now checks no TAIL TaskRun/body; pre-seal unsupported
calls cannot create their own TaskRun or admit DATA. Earlier reserved graph
TaskRuns may already exist behind the seal gate. If every EOF becomes durable
before TAIL reservation, admission is allowed: an earlier bounded collection
neither proves EOF nor permanently prohibits a later TAIL.

## Consequences

Finite tasks keep their domain meaning, while a durable receipt makes their
completion recoverable within the enrolled flow. This extends lifecycle and
storage contracts before relaxing SDK admission. It requires explicit phases,
finite result bounds and additional signed-event validation. Acceptance of the
server slice alone leaves the parent ADR in progress and earns no percentage.

## N1.6b implemented runtime and local acceptance

The mixed runtime now uses ordinary Task/Flow engines for finite prefix and
tail receipts, with stable `stream_key` bindings, bounded JSON results and
canonical parameter witnesses. Recovery looks up the immutable plan and skips
completed finite bodies; incomplete attempts retain their normal retry budget.
Sync and async entrypoints implement the same contract.

The [local epoch ledger](../evidence/native-epoch1-local-acceptance-2026-11-04.json)
records all 15 mixed HTTP cases passing, including 500-document prefix,
before/after document-200 yield and tail process cuts, partial-collection TAIL
refusal and unsupported finite-entrypoint refusals. The 22 actual worker cases
also exercise the mixed flow through deployment infrastructure. The
[Principal review](../evidence/native-epoch1-principal-local-2026-11-04.json)
confirms all N1.6b criteria locally. The [final acceptance and runbook](ADR-0040-native-integration-acceptance-and-operations.md)
owns hosted CI and final status closure; the preceding planning observations
remain historical evidence rather than the current implementation state.

## Final native foundation acceptance

The [final source-bound acceptance](../evidence/native-epoch1-final-acceptance-2026-11-04.json)
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
