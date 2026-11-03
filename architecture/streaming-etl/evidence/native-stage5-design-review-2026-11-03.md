# Native stage 5 design review: durable submission and worker recovery

- Date: 2026-11-03
- Scope: pending N1.5 / ADR-0036
- Reviewed source: `408076c95ca932e0ef2fd68988ee01ce40835217`
- Runtime changes in this review: none

## Verdict

N1.4 remains accepted for bounded native source TaskRun retry from retained
durable identities. N1.5 remains pending: N1.4 does not reconstruct a
submission after the flow process and its futures disappear, and it does not
fence lifecycle observations emitted by a former flow-process generation.

The nine ADR-0036 questions have a bounded agreement below. Implementation
should be split into identity, fencing/lifecycle, and reconstruction slices.
No individual slice earns the proposed N1.5 increment.

The first reconstruction profile is a finite linear native source to
one-input operator flow on PostgreSQL. It requires explicit stable submission
keys and code versions, canonical JSON parameters, configured TaskRun and
FlowRun retries, and a trusted existing runner that proves the former process
has exited. It excludes timeout-only takeover, an unkillable partitioned flow
process, branch/join/map reconstruction, online prefix GC, and Python-frame
restoration.

## Current authority and gaps

- [`Task._submit_stream`](../../../src/prefect/tasks.py) creates random TaskRun
  and stream UUIDs, then registers the TaskRun and reserves output in separate
  calls. Re-entering flow code cannot recover the intended call slot.
- [`TaskRunEngine.begin_run`](../../../src/prefect/task_engine.py) copies
  `FlowRun.run_count` to `TaskRun.flow_run_run_count`, increments TaskRun
  `run_count` locally, and emits a client event.
- [`emit_task_run_state_change_event`](../../../src/prefect/utilities/engine/__init__.py)
  serializes that client-side value into an asynchronous event.
- The [TaskRun recorder](../../../src/prefect/server/services/task_run_recorder.py)
  chooses a newer `state_timestamp` on conflict. It does not compare a
  server-owned execution generation, so a delayed old terminal event can
  overwrite replacement progress.
- `CoreTaskPolicy` already supplies non-forced retry transitions and run-count
  increments. It is reusable only after a recovery fence exists and only when
  the exact orchestration result is checked.

`flow_run_run_count` is useful evidence, but is not a fence: it comes from the
event payload and advances during lifecycle execution rather than in an atomic
pre-reconstruction revocation.

## Agreement on the nine questions

### 1. Submission key

The first recoverable profile requires a keyword-only call-site identity:

```python
task.submit_stream(..., submission_key="source.orders")
```

The server enforces uniqueness of `(flow_run_id, submission_key)`. Exact replay
returns the same manifest, TaskRun and stream. A key belongs to a call slot,
not a Task definition; putting it only in `StreamSourceConfig` or
`StreamTaskConfig` would merge multiple calls of one Task. Automatic call
ordinals and task names are not recovery identities. Presence of the key opts
that call into the recoverable contract; there is no global mode.

### 2. Canonical parameters

The SDK binds the Task signature before manifest creation. It then creates a
strict canonical envelope:

- the declared stream input is its upstream manifest key, stream UUID and
  protocol, not a Python object representation;
- injected `SourceContext`, `_StreamRun` and execution context are omitted;
- remaining values must be JSON primitives, arrays, string-keyed objects, or
  explicit durable-reference DTOs;
- non-finite numbers, cloudpickle identity, fallback type-name placeholders
  and arbitrary objects fail before any write; and
- secret values are excluded; only a stable block/document reference and
  explicit version witness may participate.

Canonical UTF-8 JSON uses sorted keys and fixed number/string encoding. Store a
SHA-256 digest and a bounded non-secret canonical envelope for diagnostics.
`wait_for` is unsupported in the first profile because its finite dependency
identities are not yet part of the manifest.

### 3. Code witness

Require explicit non-empty `Task.version` and `Flow.version`. A deployed flow
also binds deployment ID and deployment version or immutable artifact digest.
Names, source inspection and automatic bytecode hashes remain diagnostic.
Local execution without explicit versions remains valid for N1.4 but refuses a
recoverable keyed submission.

### 4. Takeover fact

Create one server-owned FlowRun execution-authority row with monotonic
generation, holder ID, holder-capability hash, and status. Initial execution is
generation 1. Recovery compare-and-swaps the expected generation and holder.

For the first profile, the existing runner/worker may advance generation only
after it observes termination of the process it launched. Tests use real child
exit or SIGKILL confirmation. Clock expiry alone is not authority. This is the
existing infrastructure lifecycle actor, not a second controller.

If process death cannot be established, recovery fails closed. A future live
partition takeover requires an external hard revocation plus generation checks
on FlowRun state requests; stream and TaskRun-event fencing alone is
insufficient.

### 5. Abandoned `Running` TaskRun

Under execution-authority, FlowRun, manifest, TaskRun and stream locks in that
order, the server:

1. validates manifest identity and durable stream state;
2. proposes infrastructure loss using
   `set_task_run_state(..., force=False, task_policy=CoreTaskPolicy)`;
3. requires the exact `AwaitingRetry` response from `RetryFailedTasks`; and
4. admits replacement `Running` through normal policy, incrementing the same
   TaskRun's run count.

The bounded profile requires sufficient configured FlowRun and TaskRun retry
budgets; infrastructure loss consumes them. Exhausted policy is a named refusal
and never falls back to `force=True`. A later decision may introduce a separate
infrastructure-recovery budget.

Durable EOF or operator completion is reconciled to terminal TaskRun state
without user-code entry. An OPEN output with a successful terminal TaskRun is a
conflict. Failed stream and TaskRun outcomes must agree. Lifecycle preparation
for the bounded manifest set should commit atomically; any policy refusal rolls
back the attempt.

### 6. Late lifecycle events

Every TaskRun event for a recoverable manifest carries execution generation and
manifest digest. The recorder locks the execution-authority row in shared mode,
then verifies generation, manifest-to-TaskRun identity, and run count before
upsert. The recovery CAS takes the exclusive counterpart. A missing generation
on a manifest-bound TaskRun fails closed. Stale events create bounded audit
diagnostics but do not update TaskRun or insert a current-state pointer.

Timestamp ordering remains an ordering rule only within one accepted
generation. Rejected events must be filtered before TaskRun-state insertion;
adding a comparison against event-supplied `flow_run_run_count` alone is not
sufficient. Ordinary finite TaskRuns and non-recoverable N1.4 calls retain the
existing event path.

FlowRun lifecycle must obey the same boundary. In the first, verified-dead
profile, the existing supervisor exclusively performs recovery lifecycle after
process termination. Before live-partition recovery can be claimed, generation
must also be carried by and checked on enrolled FlowRun state proposals. An old
flow engine otherwise could still propose `Completed` or `Failed` and close the
flow despite a fenced data plane.

### 7. Fence granularity

Use one generation for the whole FlowRun in N1.5. Replacement covers every
recoverable stream submission and refuses an unsupported or inconsistent
manifest before reconstruction. Branch-local recovery regions remain a later
graph-manifest decision.

### 8. Retention horizon

Whole-flow release may remove payloads and hot receipts, but retains one
bounded witness per submission: key, TaskRun UUID, stream UUID, role/protocol,
manifest digest, terminal outcome and last generation. Enforce hard manifest
row and byte limits before creation.

After the configured FlowRun recovery horizon, lookup raises
`RECOVERY_HORIZON_EXPIRED`; the old key never aliases another manifest in that
FlowRun. Normal FlowRun deletion may cascade the witness. Online retirement for
indefinitely running flows is outside this scope.

### 9. Deployment and code change

Any task/flow/deployment version, canonical parameter, upstream, protocol,
limit or manifest-schema drift refuses recovery of the existing FlowRun. A new
FlowRun is required. Exact infrastructure replacement keeps the same FlowRun,
TaskRun and stream identities.

## Implementation slices

### N1.5a: immutable identity

Add paired ORM/migrations and one server transaction that creates or exactly
returns the manifest, TaskRun, and reserved output together. Replace the
current client composition only for keyed submissions. Add hard manifest
budgets and drift controls. This slice cannot execute recovery and earns no
N1.5 percentage alone.

Primary code points are [`tasks.py`](../../../src/prefect/tasks.py), paired
database models/migrations, [`server/models/streams.py`](../../../src/prefect/server/models/streams.py),
stream DTO/API, and client transport.

### N1.5b: generation and lifecycle

Add execution authority, generation-bound native bindings, non-forced lifecycle
preparation, and recorder filtering. Every source append/EOF, consumer
read/complete/finish, output failure and release validates generation under the
common lock order. Use a versioned recoverable binding so an old client cannot
silently omit the fence.

### N1.5c: reconstruction

On normal FlowRun retry, straight-line flow code replays explicit keys and gets
the same TaskRun/stream. Fresh source generators begin after accepted position;
operators begin at the first input without durable terminal completion.
Completed EOF/output is reconciled before user code. Unsupported topology fails
before creating another manifest.

Only combined N1.5a-c evidence can satisfy ADR-0036 and earn the proposed 50%
native-foundation milestone.

## Required real controls

Use real Prefect HTTP and PostgreSQL subprocess tests on frozen source bytes:

1. lose manifest-create response and prove one exact manifest, TaskRun and
   stream after restart;
2. reject code/parameter/upstream/protocol/limit drift with zero mutation;
3. SIGKILL the owning flow process, observe its exit in the existing
   supervisor, advance generation by CAS, and re-enter the same FlowRun;
4. resume source after accepted 200 at 201 and redeliver only the first operator
   input without durable terminal completion;
5. lose responses after CAS, lifecycle preparation, Running admission, append,
   completion and EOF, and prove exact convergence;
6. hold old bindings across generation advance and reject every old mutation;
7. deliver old `Running`, `Completed`, `Failed` and cancellation TaskRun events
   after advance and prove replacement state unchanged;
8. reconcile already committed EOF/completion without user-code entry;
9. exhaust retry policy and prove named refusal without `force=True`, duplicate
   identity or partial preparation;
10. exercise manifest budgets, release witnesses and named horizon expiry; and
11. run ordinary finite-task, N1.2-N1.4 and client-package regressions on the
    same source hashes.

A test that leaves the complete former FlowRun process alive is not a valid
first-profile recovery test. It becomes valid only after FlowRun state requests
also enforce execution generation.

## Risks and scope boundary

- The current timestamp-authoritative event recorder blocks safe recovery.
- Manifest, TaskRun and output reservation must be one server transaction.
- CoreTaskPolicy recovery consumes configured retry budget in this first scope.
- Fencing native writes without fencing FlowRun state requests is not a live
  partition takeover.
- Arbitrary Python/cloudpickle hashes are not semantic parameter identities.
- One lock order must cover generation, flow, TaskRun, stream, release and
  recorder paths to avoid another implicit-FK lock cycle.

Accepted N1.4 remains the finite source/operator TaskRun-retry contract with
retained identities. This document does not accept N1.5 and does not claim
branch/join reconstruction, live partition takeover, online GC, external-effect
atomicity, production HA, or feature parity with a general distributed stream
processor.
