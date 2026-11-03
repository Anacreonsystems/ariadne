# Native stage 5b wire proposal: execution generation and lifecycle fence

- Date: 2026-11-03
- Status: proposal pending implementation and tests
- Depends on: frozen N1.5a immutable submission identity
- Runtime changes made by this document: none

## Decision boundary

`FlowRun.run_count` remains an orchestration attempt witness, not the execution
capability. A former process can read a newer shared FlowRun projection and
copy that run count into a late TaskRun event. N1.5b therefore introduces a
separate server-owned generation and an unforgeable holder capability captured
before worker threads start. The binding is immutable for that infrastructure
process and is never refreshed from `FlowRunContext` during a Task retry.

An execution-authority row enrolls only a FlowRun that uses keyed N1.5
submissions. Ordinary finite tasks and unkeyed N1.4 stream calls retain their
existing behavior. This is an execution context and fence, not a product-wide
mode or another scheduler.

## Storage authority

Use a current authority row plus bounded append-only generation attempts.

### Current authority

`StreamExecutionAuthority`:

- `flow_run_id`, unique and cascade-bound to the FlowRun;
- `generation`, positive and monotonically increasing;
- `holder_id`, `holder_capability_digest` and the holder's Ed25519 event public
  key;
- `supervisor_id` and `supervisor_capability_digest`;
- `state`: `CLAIMED`, `ACTIVE`, `RECOVERING`, `TERMINAL`, or `RELEASED`;
- the FlowRun run count expected or accepted for this generation;
- the sealed manifest-set digest, count and timestamp, once N1.5c seals it;
- `last_operation_id` and `last_request_digest` for lost-response replay; and
- a hard generation limit, initially at most 64.

### Generation attempt

`StreamExecutionAttempt` records one bounded row per generation:

- FlowRun, generation, operation ID, holder and request digest;
- previous generation and holder for takeover attempts;
- bounded infrastructure-death witness digest and non-secret diagnostic shape;
- creation and retirement timestamps; and
- immutable outcome digest.

The unique keys are `(flow_run_id, generation)` and
`(flow_run_id, operation_id)`. Reusing an operation ID with drift fails. Exact
historical retry can return its immutable result, but its old binding remains
fenced by the current authority row. Generation rows are bounded by the hard
limit and become a rolling release witness when the FlowRun is released.

Capabilities are caller-generated 256-bit random values. The server stores only
SHA-256 digests. They are never Task/Flow parameters, labels, event attributes,
logs, or response fields. The existing runner retains the supervisor
capability; the launched flow process receives only its holder capability via
the internal execution context.

The holder also generates an Ed25519 key pair. The authority row stores the
public key. The private key stays in the same internal execution context as the
holder capability. It authenticates asynchronous TaskRun events; neither
private value is persisted in task or flow parameters. This is bounded event
authentication, not a general worker or administrator credential boundary.

## HTTP contract

### Initial claim

`POST /streams/v1/executions/claim`

```text
flow_run_id
operation_id
holder_id
holder_capability
holder_event_public_key
supervisor_id
supervisor_capability
expected_generation = 0
protocol_version = 1
```

The trusted launcher calls this endpoint before it starts the child process.
The server locks FlowRun then authority, requires a nonterminal retry-capable
FlowRun with no execution authority, and creates generation 1 in `CLAIMED`.
It does not require an existing manifest: requiring user code to create one
before the execution fence would make the first task thread an unfenced
writer. Exact retry returns the same public result. A conflicting existing
authority refuses.

Response `StreamExecutionBinding` contains only:

```text
flow_run_id
generation
holder_id
operation_id
state
flow_run_run_count
created_at
```

The caller already owns the capability it generated.

### Trusted takeover

`POST /streams/v1/executions/takeover`

```text
flow_run_id
operation_id
expected_generation
expected_holder_id
supervisor_id
supervisor_capability
new_holder_id
new_holder_capability
new_holder_event_public_key
death_witness {
  launcher_instance_id,
  child_process_id,
  exit_code,
  observed_at,
  kind = WAITPID | OWNED_SIGKILL
}
```

Only the supervisor capability may advance generation. The bounded first scope
requires an observed child exit or confirmed SIGKILL from that existing
launcher. A timeout or lease timestamp is diagnostic and cannot authorize the
CAS. In the same FlowRun/authority transaction the server advances generation,
proposes `Failed` through `CoreFlowPolicy`, and requires its governed result to
be `AwaitingRetry`. It does not propose `Crashed`, which does not exercise the
normal FlowRun retry rule. The committed binding is `RECOVERING`; the next
generation-bound normal `Running` transition moves it to `ACTIVE`.

The same operation ID and request returns the committed binding after a lost
response. Drift, wrong supervisor, wrong old holder, an exhausted generation
limit, or a terminal/released authority fails without mutation.

### Runtime request headers

Every mutation for an enrolled FlowRun carries:

```text
X-Prefect-Stream-Execution-Generation: positive integer
X-Prefect-Stream-Execution-Holder: UUID
X-Prefect-Stream-Execution-Capability: opaque base64url value
```

The HTTP client adds these from an internal execution context. Backends and
Task parameters persist no live client or capability. The server hashes the
capability and validates all three coordinates under shared authority lock.

For an enrolled FlowRun, missing or stale headers fail before TaskRun or stream
locks. Unenrolled N1.4 calls keep their version-1 behavior. Recoverable
bindings use an explicit protocol extension so an old client cannot silently
omit the fence.

Initial claim happens before the child has built any manifest. Once enrolled,
manifest reservation itself requires the same headers. A recoverable flow
cannot be retrofitted after user code or task threads have begun.

### Manifest seal

N1.5c adds `POST /streams/v1/executions/seal-manifest`. The request carries the
current execution headers, an operation ID, and the exact ordered list of
`(submission_key, manifest_digest)`. The server stores an immutable set digest
and count and rejects later registration. Whole-worker recovery requires this
seal. A death before sealing returns `MANIFEST_NOT_SEALED`; the server never
infers a complete graph from whichever rows happened to commit before a crash.

Keyed task threads may be scheduled while the bounded linear graph registers,
but enrolled source and consumer admission waits for the seal. The first wait
or bounded collection boundary seals the registered graph, releases admission,
and prevents a later task declaration. This keeps accepted data at zero while
the graph is partial without introducing a global flow mode.

### Recovery preparation

`POST /streams/v1/executions/prepare-recovery`

The request carries the current execution headers and an exact ordered list of
`(submission_key, manifest_digest)`. The server requires the list to match the
sealed active manifest set. Under one transaction it inspects stream outcomes and prepares
each abandoned `Running` TaskRun through
`set_task_run_state(..., force=False, task_policy=CoreTaskPolicy)`. OPEN work
must yield exact `AwaitingRetry`; committed EOF/completion may reconcile a
terminal state. A policy refusal rolls back the whole bounded linear set.

The response is a state matrix with TaskRun UUID, stream UUID, durable
position/cursor, stream outcome, TaskRun state and next expected run count. A
prepared TaskRun remains at its old run count in `AwaitingRetry`. The normal
sync or async TaskRun engine hydrates that row, increments exactly once while
entering `Running` or `Retrying`, and emits a signed current-generation event.
There is no second server-side `Running` admission. Reconstruction initializes
local retry accounting from the retained TaskRun attempt instead of resetting
an already-consumed retry budget. The response contains no capability and is
immutable for the operation ID.

## FlowRun lifecycle fence

Data-plane fencing alone is insufficient. Once a FlowRun is enrolled, its
ordinary state-proposal API must require the same execution binding. Otherwise
the old flow engine could submit a late `Completed` or `Failed` state and close
the FlowRun after the data plane had moved to a new generation.

The bounded profile has two paths:

1. the current holder proposes normal FlowRun states with its holder headers;
2. the existing supervisor uses a dedicated infrastructure-loss endpoint with
   its supervisor capability after child death.

The supervisor endpoint applies CoreFlowPolicy; it does not call
`force=True`. An exact governed `Failed` to `AwaitingRetry` result is required. The next
normal `Running` increments `FlowRun.run_count`; server logic records its
association with the already-issued execution generation. The two counters are
checked for consistency but are not interchangeable authorities.

Missing generation on an enrolled FlowRun fails closed. Cancellation must use
the existing runner cancellation path plus a generation-aware server proposal;
it cannot bypass the fence through a legacy endpoint.

## TaskRun event fence

Events for a keyed manifest include non-secret attributes:

- FlowRun execution generation;
- holder ID;
- manifest digest; and
- TaskRun run count already present in the event payload;
- the event-key identifier and an Ed25519 signature.

The signature covers a canonical versioned witness containing the event ID,
name and occurrence time; FlowRun and TaskRun IDs; generation, holder,
manifest digest and run count; and digests of the complete `task_run` and
`validated_state` event payloads. The recorder verifies it with the public key
stored for the current generation. A signature is safe to retain in event
transport; the private event key and holder capability never enter the event.

The recorder processes ordinary and enrolled TaskRuns separately. For enrolled
events it acquires shared FlowRun and authority locks, verifies current
generation/holder, signature and manifest binding, then applies a
generation-local state matrix. Old, unsigned or missing-generation events are excluded before TaskRun state
insert and current-state upsert. Their event IDs may be reported through
bounded metrics/logging, but no unbounded rejection ledger is added.

Timestamp remains only an ordering aid inside one accepted generation. It is
never allowed to override generation. A replacement `Running` event must match
the server-prepared expected run count; later terminal events must keep that
count.

## Lock order

All enrolled operations use this order:

1. FlowRun row: shared for normal mutation, exclusive for takeover/release;
2. execution-authority row: shared for normal mutation, exclusive for CAS;
3. submission manifests ordered by submission key;
4. TaskRuns ordered by UUID;
5. stream partitions ordered by UUID;
6. consumers, records and completion witnesses.

Recorder batches sort FlowRun IDs and follow the same prefix. Takeover takes
FlowRun exclusive before authority exclusive. This preserves the N1.2 parent
lock correction and avoids TaskRun/stream-to-FlowRun foreign-key cycles.

The TaskRun state endpoint first resolves the immutable FlowRun ID without
claiming TaskRun authority, then acquires FlowRun and execution-authority locks
and rechecks the TaskRun binding before its existing TaskRun lock and policy.
Recorder batches acquire sorted FlowRun/authority prefixes before retaining
their existing conflict-key ordering.

## Failure and retry semantics

- A holder request from an old generation returns `STALE_EXECUTION` without a
  TaskRun or stream mutation.
- Missing headers for an enrolled flow return `EXECUTION_BINDING_REQUIRED`.
- Wrong capability returns a generic authorization conflict and does not reveal
  which coordinate matched.
- Exact claim/takeover/prepare replay returns the prior result.
- An operation-ID drift returns `IDENTITY_CONFLICT`.
- An exhausted retry budget returns `RECOVERY_POLICY_EXHAUSTED`; no forced state
  write is attempted.
- An unavailable or unverifiable death witness returns
  `TAKEOVER_NOT_AUTHORIZED`.
- Released authority returns `RECOVERY_HORIZON_EXPIRED`.

## Required controls before reconstruction

Real HTTP/PostgreSQL tests must cover:

1. claim and takeover response loss with exact replay;
2. concurrent takeover where exactly one operation wins;
3. old source, consumer and output bindings rejected after CAS;
4. late old `Running`, `Completed`, `Failed` and cancellation TaskRun events
   rejected before recorder mutation;
5. late old FlowRun `Completed`/`Failed` proposals rejected;
6. current-generation TaskRun event ordering and run-count validation;
7. `Running` to exact `AwaitingRetry` through CoreTaskPolicy without force;
8. retry-budget exhaustion with no partial graph preparation;
9. API death before CAS commit and after commit; and
10. parent/authority/TaskRun/stream lock schedules with no deadlock.

The controls also require initial claim before process launch, refusal of
unsealed admission and reconstruction, signature tamper and
current-generation-copy attacks, and proof that recovery consumes exactly one
normal TaskRun retry.

N1.5c reconstruction must not begin until these controls pass on frozen source
bytes. N1.5b by itself earns no percentage and does not broaden accepted N1.4.

## Rejected earlier shapes

An earlier draft required an active keyed manifest before initial claim. That
ordering is rejected because the child would create its first TaskRun and
stream before it possessed an execution-wide fence. Claim is launcher-owned
and precedes child start; graph completeness is a later immutable seal.

An earlier draft placed only generation, holder and manifest coordinates on
TaskRun events. Those fields are identifiers, not proof: an old or unrelated
client could copy the current public coordinates. The current proposal binds
each generation to an event public key and requires a signature over the exact
event witness.

An earlier draft described infrastructure loss as a `Crashed` transition. The
existing `CoreFlowPolicy` normal retry path governs `Running` to `Failed` into
`AwaitingRetry`; the proposal uses that path and rejects forceful or fabricated
state changes.
