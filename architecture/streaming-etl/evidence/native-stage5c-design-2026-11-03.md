# N1.5c agreement: native graph reconstruction and unsealed abort

Status: `planning`. The ETL Architect / HighLoad Principal reviewed the bounded design.
This is an agreement and test plan, not implementation or acceptance evidence.
It assumes the frozen N1.5b execution authority and preserves the 40% earned scope.

## Existing declaration replay

`NativeStreamStore.reserve_submission` must keep the current Flow -> authority ->
manifest -> Task lock order. For an existing ACTIVE manifest with the exact
request digest and current execution credentials, return the immutable manifest
for any retained Task state, including Running, AwaitingRetry, Completed and
Failed. This is a declaration lookup only. It must not create a TaskRun, reopen a
stream, change its owner, or admit DATA. A RELEASED manifest remains
`REPLAY_HORIZON_EXPIRED`; request drift remains `IDENTITY_CONFLICT`.

New declarations remain allowed only while the authority is unsealed and in the
initial graph-building generation. A sealed manifest set refuses later keys.
RECOVERING cannot add keys.

## Same-generation Flow retry guard

In `guard_flow_lifecycle`, before orchestration:

- CLAIMED + unsealed -> Running is the initial start.
- ACTIVE + unsealed -> Running is an in-process graph-construction retry.
- RECOVERING + sealed -> Running is the explicit newer-generation takeover.
- ACTIVE + sealed + Flow already Running -> Running is an idempotent proposal.
- ACTIVE + sealed + current Flow Scheduled/AwaitingRetry -> Running is refused
  with `RECOVERY_NOT_READY`; post-seal Flow application retry requires a new
  FlowRun. Task-local retries remain supported.

The rule does not use wall time and does not change ordinary unenrolled flows.

## Atomic abort of an unsealed graph

Add `POST /streams/v1/executions/abort-unsealed`.

Request:

- `flow_run_id: UUID`
- `operation_id: UUID`
- `manifests: list[StreamManifestWitness]`, 1..128, exact ordered set
- `error: str`, 1..4096 UTF-8 bytes

Response:

- `flow_run_id`, `generation`, `operation_id`
- `items`: ordered `{submission_key, task_run_id, stream_id}`
- `state: "TERMINAL"`
- `outcome_digest`

The request uses current holder headers. It is legal only after normal
CoreFlowPolicy has made the FlowRun final and `apply_flow_lifecycle_result` has
made the authority TERMINAL. It is not a force transition.

Lock order in one transaction:

1. FlowRun FOR UPDATE;
2. StreamExecutionAuthority FOR UPDATE;
3. current StreamExecutionAttempt FOR UPDATE;
4. all named StreamSubmission rows ordered by task_run_id FOR UPDATE;
5. all TaskRun rows ordered by id FOR UPDATE;
6. all StreamPartition rows ordered by id FOR UPDATE;
7. absence check for StreamConsumer and StreamCompletion rows.

Preconditions before mutation:

- current exact execution capability, terminal FlowRun and TERMINAL authority;
- `manifest_set_digest`, seal operation and prepared generation are absent/zero;
- the request names every ACTIVE manifest exactly once and every digest matches;
- all Tasks are Pending with run_count 0;
- all streams are OPEN, unowned, attempt/position/count/bytes/last_sequence zero;
- no consumer or completion row exists.

Mutation:

- propose Failed for every Pending Task through CoreTaskPolicy with force=False;
  Pending->Failed is terminal because RetryFailedTasks applies only from
  Running;
- mark every output/source stream FAILED with the bounded abort error;
- retain immutable manifests and the authority TERMINAL witness;
- store request and outcome digests atomically.

Exact replay returns the stored byte-equivalent outcome. Changed operation
material is `IDENTITY_CONFLICT`. Any partial activity is `TERMINAL_CONFLICT` and
rolls back all writes.

### Durable abort receipt

Do not overload `recovery_*` columns: recovery and abort decode different DTOs.
Add nullable `abort_operation_id`, `abort_request_digest`, `abort_outcome`, and
`abort_outcome_digest` to `StreamExecutionAttempt`, with an all-null/all-present
CHECK and `UNIQUE(flow_run_id, abort_operation_id)`. Add a new paired PostgreSQL
and SQLite migration after the N1.5b heads and record it in
`[migration notes](../../../src/prefect/server/database/_migrations/MIGRATION-NOTES.md)`. No backfill;
downgrade removes only abort receipts and therefore removes exact abort replay.

## Required real PostgreSQL tests

1. Exact declaration replay in Running, AwaitingRetry and Completed states
   returns the same IDs/digest and changes no counts/state/stream fields.
2. Request drift and a new key after seal are read-only refusals.
3. Initial unsealed and RECOVERING-new-generation Running are admitted, while
   sealed ACTIVE AwaitingRetry -> Running is refused without state change.
4. Terminal Flow + exact full zero-data graph aborts all Tasks/streams in one
   commit; exact operation replay is identical.
5. Missing/reordered/drifted manifests, a Running Task, owned stream, one DATA
   record, or existing consumer/completion all reject with zero mutation.
6. Lost response after abort commit replays the exact receipt from a fresh API
   process.
7. API death before commit leaves all Tasks Pending and streams OPEN.
8. Ordinary unenrolled finite flows and N1.4 unkeyed streams remain unchanged.

## Reconstruction boundaries that must remain explicit

- Re-entering the Flow body repeats declaration code. The first profile requires
  straight-line, side-effect-free or client-idempotent graph declaration. It
  rejects result-dependent branching, mapping, fan-out and late declarations.
- Every native Task engine is held before `begin_run`; no stream DATA is admitted
  until the complete ordered graph is sealed or the newer generation is
  prepared.
- The first wait/collect boundary seals the graph. A declaration after that
  boundary is a named refusal.
- A process loss while unsealed cannot use takeover/prepare. It follows the
  terminal abort path and a new FlowRun. Process loss after seal uses a newer
  generation and prepare.
- Completed+EOF manifests produce a synthetic stream result without invoking
  user code. AwaitingRetry honors durable scheduled time once and starts with
  the consumed retry count.
- UI/admin cancellation without execution credentials is outside this slice;
  enrolled lifecycle writes remain holder-fenced.
- Exact seal replay in a newer generation must never be interpreted as recovery
  preparation; the reconstruction path calls `prepare-recovery` and verifies
  `prepared_generation == generation` before releasing workers.

## Trusted FD bootstrap review

A selected launcher may pass only `{flow_run_id, generation, holder_id, api_url,
holder_capability, event_private_key}` to the child. The supervisor capability
stays in the launcher. Use a one-shot inherited FD/pipe on POSIX, `CLOEXEC` by
default plus explicit `pass_fds`, a strict exact-key JSON parser and a small
bounded frame (4 KiB). Read once, close before user code, reject
trailing bytes, duplicate/unknown keys, invalid UUID/hex/generation, credentialed
or cross-origin API URLs, and never place this object in argv, environment,
Task/Flow parameters, logs or serialized context. A platform without equivalent
handle inheritance fails closed in this first profile. This is process launch
plumbing, not a global Flow mode or a second controller.

## Stories

- As a developer, I want ordinary versioned Flow/Task declarations with explicit
  `submit_stream(submission_key=...)` slots to reconstruct the same identities.
- As a user, I want document 200 redelivered if its yield was not accepted and
  document 201 after its yield was accepted, even if the complete flow process died.
- As a system, I want completed EOF to bypass the user function and want every
  active task held until the exact graph has been reconciled.

## Planned acceptance

- [ ] As a user, I got early consumption of a real document while its source
  TaskRun remained Running, using only small URI/hash references in the channel.
- [ ] As a user, I got all 500 ordered documents after whole-process death before
  and after the document-200 yield, without retaining TaskRun/channel IDs in the
  replacement launch payload.
- [ ] As a user, I got source position 201 after death at accepted position 200,
  and no source user-code invocation after accepted EOF and lost task success.
- [ ] As a system, I got two logical TaskRuns and channels throughout replacement,
  bounded orchestration history, and final cursor/managed-state/output agreement.
- [ ] As a system, I got replay-safe seal/prepare operation UUIDs, bounded private
  descriptor bootstrap, safe declaration failure and unsupported-graph refusals.

## Review resolutions

Seal and prepare use a deterministic operation UUID derived from FlowRun,
execution generation and operation schema. A response lost after server commit
must replay the same operation, including after a fresh process starts. A failed
local submission can leave a durable reservation without a future; abort/retry
waits only for futures actually created. Every Task snapshot is refreshed before
workers are released. Completed outputs resolve a typed bounded result before
entering either task engine. The flow finish hook runs inside the existing task
runner lifetime and joins every native task before proposing Flow success.

The process-local registry is admission plumbing. It does not replace durable
per-yield progress with a periodic snapshot. Flow declaration code repeats and
must have no effects or implement its own idempotency. External file writes in
the 500-document test deliberately implement client idempotency; the platform
claim covers channel progress and identity, not external exactly-once effects.

A selected trusted launcher supplies the private descriptor to the ordinary
`python -m prefect.flow_engine` entrypoint. Automatic integration of every
production deployment worker, non-POSIX handle transport, branch/join graphs,
production HA and throughput objectives remain outside this bounded gate.

## Flow application retry policy

Task-local retries retain their configured budget. After graph admission, a
Flow application failure proposes an ordinary Failed state with the existing
`StateDetails.retriable=False`. `RetryFailedFlows` must honor that explicit
value just as task retry policy does; default/True flow retries remain unchanged.
The server's same-generation sealed Running guard remains defense in depth.
Infrastructure takeover still uses its server-owned normal retry transition.

Terminal unsealed cleanup runs after the accepted Flow state is known and before
user terminal hooks, so hook failure cannot strand the zero-work graph. An
AwaitingRetry graph remains reserved, with workers released locally and held
again by exact declaration replay on its next construction attempt.

## Links

- [ADR-0036](../adr/ADR-0036-durable-submission-recovery.md)
- [Native epoch](../epochs/epoch-01-native-prefect.md)
- [Task registry](../todo.md)
- [Sprint board](../sprint_board.md)
