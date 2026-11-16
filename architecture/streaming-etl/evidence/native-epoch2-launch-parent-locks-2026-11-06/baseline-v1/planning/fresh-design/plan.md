# Launch parent-lock correction: source-bound execution plan

DESIGN ONLY. No production/test source edits or DB/runtime checks have run.
Checkout: `/tmp/ariadne-native-5000-diagnosis-v1`, clean detached
`7d99d8472c617a966c868c3bd1696a3fe4410c1a`.
Current hashes and read sources are in `source-bindings.json`. All four held
coordinator paths match the supplied hashes. Approved ADR42–48 original bodies
match `fae53c90aa3b44dbc45032833b4d193001e5c8bd`.

The known grill-me/spec-first variants remain excluded. The cloud-only skill
conflict/generator scripts are unavailable; no exhaustive scan or generated ADR
operation is claimed. The existing approved ADR scope governs this correction.
Principal review and Root GO are required before the steps below execute.

## Candidate and exact boundaries

Change only two `lock_worker_subtask` calls in
`src/prefect/server/models/stream_dag_launches.py`:

- Prepare at265–267: add `graph_update=False`, retaining `all_workers=True`.
- Record at435–437: add `graph_update=False`.

Keep helper default `graph_update=True` at84, every other caller, all lower lock
queries, validation, transaction ordering and DML unchanged. Prepare's business
DML is one Attempt INSERT at397–421. Record changes only its Attempt process
identity/record receipt/state at456–464. `lock_dag_graph` at187–270 takes Flow,
Authority, Graph in that order using `read=not update`; false therefore selects
FOR SHARE. Own Worker/Capability SHARE, own Subtask UPDATE, Attempt UPDATE and
replacement TaskRun/output locks remain intact. No cache or new lock mode API.

The b4 real prepare500/record500 both reached Flow FOR UPDATE. Their historical
blocker identity and their causal share of the volume timeout remain UNKNOWN.
The controlled holder below is a new synthetic, identified blocker. A passing
component result does not clear the b4 full-case failure or earn new readiness.
Earned epoch2 is15%; the separately accepted coordinator result earns0% here.

## Proposed repository test destination

Add unheld adjacent module
`tests/server/models/test_stream_dag_launch_locks.py` after GO. Keep existing
`test_stream_dag_launches.py`, held model test and native HTTP fixture unchanged.
The new module imports the existing `execution_api` fixture and committed setup
helpers without editing them. Tests carry existing `clear_db` and database
service markers. Full annotations and existing real server/database fixtures
follow tests/AGENTS.md; no full SDK fixture enters server tests.

Use a separately owned `httpx.AsyncClient` derived from `execution_api`'s ASGI
app/auth/base URL, with `ASGITransport(..., raise_app_exceptions=False)`.
Never mutate the shared fixture transport. Requests exercise real protected
routes, real transaction contexts, real PostgreSQL17 and real storage DML.
This surface is explicitly HTTP-via-ASGI, separate from TCP and native OS tests.

Source anchors:

| Purpose | Current source |
| --- | --- |
| Real protected prepare/record route transactions | api/stream_dag_launches.py55–87 |
| Existing protected ASGI fixture | tests/server/api/test_stream_dag_executions.py33–41 |
| Admission without birth | tests/server/models/test_stream_dag_executions.py65–151 |
| Registered/active replacement setup | same module163–315 |
| Parent SHARE holder | tests/server/models/test_stream_dag_launches.py57–70 |
| Real blocker polling precedent | tests/server/api/test_native_deployments.py1724–1746 |
| Terminal replacement and accepted replay | tests/server/models/test_stream_dag_launches.py338–462 |
| Delayed record after control STOP_REQUIRED | same module587–694 |
| All births close before final stop | same module698 onward |
| Parent scope/current Flow run_count | models/stream_dag_executions.py104–114 |
| Exact old Attempt worker/parent generation | models/stream_dag_launches.py223–255 |

## Four positive request cases and setup

Each case owns a fresh graph. All setup commits before a holder opens.

1. `initial-prepare`: `start_deployed_region`, poll assignment, deterministic
   prepare UUID `uuid5(subtask, 'prefect-stream-dag-launch-v1/'+str(start))`.
   Assert no prior Attempt/current Grant for the target.
2. `initial-record`: same admission; prepare through the protected HTTP route,
   commit it, use its accepted Attempt ID, deterministic record UUID and stable
   valid fixture PID/ticks/journal digest. Assert PREPARED before holding locks.
3. `replacement-prepare`: `ready_deployed_region`, choose B, authorize exact
   current Grant/epoch by normal replacement lifecycle, close its old birth by
   normal close with an exact fixture WAITPID witness, then commit. Keep the
   TaskRun nonterminal/output OPEN. Build deterministic prepare UUID from the
   replacement operation. A fixture WAITPID witness proves setup protocol only,
   never actual OS process death.
4. `replacement-record`: same replacement setup; protected HTTP prepare commits
   the candidate first, then record its exact accepted ID. Old Grant and old
   closed birth remain unchanged.

Do not mutate TaskRun/Flow state by SQL. Signed Task event/orchestration paths
remain authoritative. Setup model helpers are real operations, not mocks.

## Snapshot and mutation oracle

Build an owned graph/flow/worker/target ID set before contention. Snapshot via
fresh independent read sessions, not ORM identity-map objects. Capture all row
columns and compare exact values, except the explicitly permitted Attempt
record fields plus ORM `updated` for record. Scope each query to owned IDs.

Capture FlowRun, FlowRunState, ExecutionAuthority, GraphRevision,
PhysicalSubtask, TaskRun, TaskRunState, Partition, Worker, WorkerCapability,
PhysicalSlotAllocation, SubtaskAllocation, SubtaskGrant, SubtaskReplacement,
DAGLifecycleOperation, DAGControlTarget and the graph's Attempt rows. Resolve
SubtaskAllocation limit IDs and scoped Task tags to snapshot matching ordinary
ConcurrencyLimit and ConcurrencyLimitV2 rows. Setup may request one unique
normal tag limit to ensure capacity snapshots have an actual occupied row.
No workers/services continue writing these owned resources during the case.

Prepare permits one new PREPARED Attempt with the exact expected index,
planned epoch, start/replacement identity and current parent generation tuple;
all previous Attempts and every other scoped row must be identical. Record
permits only PID/ticks/journal, record operation/digest/receipt, state and updated
on its existing Attempt. All other rows remain identical. Replays must return
the original receipt and no extra mutation/birth. Independent committed-row
reads happen while the holder remains in its open transaction.

## Baseline RED before touching production

Use the exact positive regression selector on unchanged product bytes; no xfail
or inverted expectation. Keep each attempt's private raw log, sanitized log,
JUnit, source-before/after manifests, command and witness. Expected baseline
result is failing the requirement200+commit while a parent SHARE holder stays
open. A pending request is not relabeled HTTP500.

Open holder H in an explicit independent transaction and capture its backend
PID. Acquire only Flow SHARE → Authority SHARE → Graph SHARE in correct order.
Start one Q HTTP request in an owned shielded asyncio task. Fixed local
functional deadline:5s, unchanged for baseline and candidate. No modification
of server statement/lock timeouts, SDK backoff or native750s budget.

While Q is pending, observer O clears its stats snapshot and finds candidates
in `current_database()`, excluding H and O; require exactly one actual lock
waiter whose recursive `pg_blocking_pids` chain contains H and whose query
prefix names the expected Flow relation. Store safe backend PID, backend/xact
timestamps and transaction identity, application name, wait event/type, query
text and every relevant relation/tuple/transactionid lock from pg_locks. Poll
within the same fixed5s functional window; no timing-only block assertion.
Dedicated owned DB plus one in-flight target HTTP request provides correlation.
Ambiguity/missing witness is harness INCONCLUSIVE, not a product RED.

For unchanged baseline require H still open, the actual query ends/contains
FOR UPDATE where visible and selects FlowRun, and H is the real blocker. Save
Attempt absence/PREPARED unchanged plus no parent/slot/grant delta. At5s capture
pending or the actual HTTP response status; the200-before-release requirement
fails. In finally rollback H, then await Q ordinarily (5s cleanup bound) and
record whether this same valid request returns200 after release. Cancel/await
only Q if cleanup cannot finish. Never kill unrelated backend/processes.
H release is strictly after the under-holder independent row observation.

## Candidate GREEN and fence controls

After baseline RED is recorded, apply the two-caller correction. Run the same
four tests with the same5s local deadline and unchanged oracle. Q must return200
and the required mutation must be independently committed while H is still
open. Verify all snapshot invariants, stable operation receipt and replay.

For each of prepare/record × Flow/Authority/Graph, open a fresh H taking SHARE
on predecessors and UPDATE on only the selected parent, then stop acquiring
locks. Example Graph probe: Flow SHARE → Authority SHARE → Graph UPDATE.
This is essential: UPDATE on Flow for every probe would never test the later
parents. Require the actual Q blocker/expected selected table, pending Q and
no Attempt delta before releasing H, then200 after release. These are real
exclusive-writer fence checks through HTTP, without weakening lifecycle locks.

For each of prepare/record use another fresh target-reader H with the common
parent SHARE prefix plus a DIFFERENT target Subtask SHARE; the request target
must differ. The request must commit200 before H releases. A SAME-target
Subtask SHARE intentionally blocks own Subtask UPDATE and is a separate lower
serialization positive control.

## Control and unique-birth guards

- Control-first with FAIL, CANCEL and STARTUP_ABORT through normal lifecycle
  APIs: reject fresh birth with no new Attempt; accepted prepare replay keeps
  its receipt but returns STOP_REQUIRED; a delayed record journals the exact
  PID and returns STOP_REQUIRED; differing record material conflicts with no
  mutation. Retain existing587–694 test unchanged and add HTTP route coverage.
- Launch-first overlap: direct-model prepare in an explicitly held request-like
  transaction L obtains parent SHARE and flushes an uncommitted birth. Launch
  real control HTTP Q while L stays open; observe Q blocked at Flow UPDATE by
  L via pg_blocking_pids. Commit L; Q must now include the target/birth in its
  durable closure obligations. No final Flow failure/last slot release until
  every prepared/recorded birth closes. Label L as a direct-model concurrency
  adjunct; the control waiter itself remains real HTTP. Do not present a
  sequential prepare-then-control check as proof of this overlap.
- Concurrent identical prepare: fresh target, two real HTTP tasks released
  together, expect same receipt/ID, one index/epoch and exactly one inserted
  Attempt. Keep real own-Subtask UPDATE serialization. A deliberately held
  own-Subtask lock can prove both waiting requests' lower-lock behavior before
  release; request outcome/row oracle remains necessary after release.
- Concurrent identical record: two real HTTP tasks return one stable receipt;
  fixed PID/ticks/journal is immutable. Concurrent changed PID/ticks/digest
  must yield exactly one accepted body and reject the other, never mix fields.
- Changed prepare digest (e.g. wrong current start/replacement identity), stale
  worker/principal and wrong assigned Worker reject without Attempt delta.
- Record parent generation/holder/run_count guard and old worker generation
  are tested against exact prepared births. Where public lifecycle intentionally
  disallows a takeover, use explicitly labeled isolated corruption injection
  under the normal parent lock order in a fixture, not a claimed user flow.
  Do not alter Flow or Task state directly. A parent scope run_count mismatch
  must remain IDENTITY_CONFLICT; an old Attempt generation tuple remains
  SUBTASK_SCOPE_DENIED. Exact error expected depends on which earlier guard is
  intentionally broken; bind it to223–255 or scope104–114 rather than guessing.
- Missing/exact slot mismatch, stale/offline capability, admission mismatch,
  unauthorized/wrong current Replacement/Grant/epoch, unclosed previous birth,
  terminal TaskRun/output and64/retry budget retain their current source guards.
  Use existing real lifecycle tests where available; any corruption fixture is
  separately named guard coverage and snapshots reject incidental writes.

## Runner, privacy and serial execution

After GO create a NEW `/tmp/native-launch-parent-lock-recovery2-runs-v1/run.py`.
Do not reuse/overwrite any old evidence. Existing coordinator runner is a
read-only pattern. Programmatically load
`/tmp/native-ni012-runtime-test-env.json`; never print selector contents, DSN,
auth payloads or environment. Validate PG major17 through asyncpg, generate a
unique new owned database under the configured prefix, and preserve old DBs.
The fixture also owns `<new-name>_tests_master`; cleanup checks only these two
fresh names. If owned sessions remain after pytest exits, record cleanup block
and report it; do not terminate unrelated sessions.

Use uv only: `uv run --no-sync --project <checkout> python -m pytest ...` with
`UV_PROJECT_ENVIRONMENT=/workspace/ariadne/.venv`, and prepend that venv's bin
to PATH for every child. Set PYTHONPATH to checkout/src and checkout, preserve
the runtime's inherited settings except the explicit owned DB selector. Save
version/import path proof from the actual child interpreter. Raw logs mode0600;
sanitize connection strings before public output/artifact summaries. Inventory
all src/prefect Python, selected test/helper/fixture files, conftests,
pyproject.toml and uv.lock before/after. Stop on any held-source drift.

Run baseline body, candidate body, focused launch/lifecycle/API regressions and
any approved native follow-up strictly sequentially, no xdist, no hooks during
test bodies. Root owns docs/commit/push only after actors stop. Principal must
personally rerun the final held candidate tests; source review and this
implementer run do not replace independent execution. Broader native
accepted200/500/5000 TCP+OS gates remain separately chosen follow-up and keep
whole-case effects, signatures, terminal assertions and original budgets.

## Stop points

Stop for Root/Principal on ambiguous RED witness, unexpected guard behavior,
source drift, cleanup leak or a proposed change beyond the two callers. Keep
all actual failed attempts. No budget increase, oracle relaxation, blocker
attribution, ADR body edit or new72h soak is part of this plan.
