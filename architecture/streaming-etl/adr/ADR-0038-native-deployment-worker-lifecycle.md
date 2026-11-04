# ADR-0038: Native deployment, worker enrollment and lifecycle

- Status: `progress`
- Date: 2026-11-04
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- Owners: deployment, worker, lifecycle, API and operations maintainers
- Depends on: [ADR-0036](ADR-0036-durable-submission-recovery.md), [ADR-0037](ADR-0037-mixed-finite-and-streaming-tasks.md)
- Delivery: [task registry](../todo.md), [sprint board](../sprint_board.md)

## Problem

The accepted native foundation can reconstruct a bounded graph through a
selected test launcher. It does not yet make that launcher a supported Prefect
deployment and worker contract. The current holder capability and signing key
live only in the launching process. If that supervisor dies, a new worker
cannot use `waitpid` to prove that another supervisor's child is gone. A lease
timeout proves only that no renewal was observed. Treating it as proof of death
could start a replacement while the former process is still running.

Deployment also needs to know that a Flow is a native streaming flow before it
launches user code. Inferring the capability from a Task call is too late to
enrol the worker, reserve one concurrency slot and install lifecycle fencing.
Passing the holder secret through Flow parameters, Task parameters or deployment
variables would expose an authority credential through ordinary orchestration
metadata.

The remaining epoch criteria also require cancellation, drain, progress
projections, version refusal and an actual work-pool deployment gate. These
concerns must extend Prefect's existing worker and server authority rather than
introduce a second global run controller.

## Theory

A native deployment can remain an ordinary Prefect deployment if it carries a
typed capability declaration that is immutable for one FlowRun. The declaration
lets the server and worker establish execution authority before the child Flow
engine reports `Running`. Ordinary deployments omit it and retain their current
behavior.

Replacement is safe only when the selected infrastructure profile can identify
the exact former process and verify that it is no longer live. The first bounded
profile can do this on one trusted Linux host: persist the host boot identity,
PID, process start ticks, launch nonce digest and execution generation; inspect
the same identity through `/proc`; and use `pidfd` to terminate and observe an
exact matching live process. PID alone is never an identity. A missing PID, a
changed boot identity or a different process start identity proves that the
recorded process is no longer live in this bounded local profile; it does not
authorize killing the different process that now owns a reused PID.

An authenticated trusted worker generates a single-use replacement challenge;
the server stores only its digest and authorizes the operation against the
locked launch identity. The server records the later death observation and
mints the next holder only after that operation succeeds. Lease expiry can
request this check but cannot complete it. If the replacement cannot inspect
the recorded host/process or cannot obtain the required `pidfd` observation,
takeover fails closed. A live former child remains fenced by the server
generation even after verified replacement, but generation fencing is not
itself the death proof.

This is a trusted-runtime boundary, not a hostile-host security claim. It uses
the existing Prefect server, worker and work-pool path and does not add an
external control platform. Cross-host replacement, general container or
Kubernetes termination attestations, regional failover and high availability
need later profiles.

## Proposed solution and agreement

### Public capability declaration

Add a typed public Flow capability declaration with protocol and profile fields,
provisionally `streaming=NativeStreamFlowConfig(protocol="native-v1",
profile="bounded-linear-v1")`. Its exact Python spelling is finalized with the
SDK change, but these wire semantics are fixed:

- the declaration is visible to deployment construction and scheduling before
  a FlowRun child starts;
- the deployment stores a canonical declaration and code/version digest, and
  the FlowRun binds an immutable copy;
- absence preserves ordinary Flow behavior; an unknown protocol/profile or a
  deployment/Flow mismatch refuses before worker launch or DATA admission;
- holder capabilities, signing keys and supervisor secrets are never fields of
  this declaration, Flow/Task parameters, labels, events, logs or projections.

The first profile is the bounded graph from ADR-0037: one finite prefix, one
source, one linear operator and one finite tail, with strict JSON side values,
durable references for large objects and explicit code versions. The declaration
does not create a product-wide streaming mode.

### Worker enrollment and launch

An authenticated worker claims a named launch operation before starting the
Flow engine. The server binds FlowRun, deployment, work pool, worker identity,
protocol/profile digest, generation and a single concurrency-slot lease. The
worker starts the child only after that claim. It transfers the holder capability
and private event key through a bounded inherited file descriptor or equivalent
one-use local pipe. They are not placed in environment variables or serialized
Flow parameters. The child proves the bound generation before native Task or
stream admission.

An exact launch replay returns the existing operation. A different deployment,
code digest, worker or profile conflicts without starting user code. A worker
crash before child start leaves an auditable unstarted operation that can be
retired without consuming a Flow retry. A child crash uses normal FlowRun retry
policy and keeps the same native manifests and TaskRun identities.

### Confirmed replacement

For the selected Linux local-process profile, enrollment first persists the
launcher host, boot ID, PID and process start ticks. Child registration then
persists a separate identity:

1. worker/launcher instance and host identity;
2. Linux boot ID, PID and `/proc/<pid>/stat` start ticks;
3. launch nonce digest, FlowRun, generation and operation ID;
4. concurrency-slot identity and the supported process-control profile.

The server can delegate one replacement check after the old worker stops
renewing or an operator requests recovery. The new trusted worker must run on
the recorded host. It reports one of these verified outcomes under the operation
lock:

- the exact process is live, so it terminates that identity and waits on its
  `pidfd` for exit;
- the PID is absent, the host rebooted, or the PID belongs to a process with a
  different start identity, so the recorded identity is no longer live;
- identity or host access is unavailable, so replacement is refused.

Only the first two outcomes allow the server to consume the operation, advance
the execution generation and issue a new holder capability. Concurrent checks
have one winner. The former holder, signed events and stream writes are rejected.
The original concurrency slot remains attached to the FlowRun across this
infrastructure replacement; it is neither released early nor acquired twice.

An enrolled launcher with no registered child is a distinct recovery candidate.
The verifier never signals a live launcher because it may host other FlowRuns.
It can retire the launch only after the launcher identity is absent, belongs to
a reused PID or belongs to a previous boot. A concurrent child registration
wins the launch-row lock and changes the required proof to `CHILD`; a stale
launcher observation cannot retire the registered child. The same rule applies
to a replacement generation that fails before its child is created.

A registered child can also exit after Flow `Running` but before the first
manifest. That cut is a startup failure only when the server proves the graph
is still empty: no TaskRun, stream partition, stream/finite submission or sealed
manifest exists. It final-fails and releases that empty run. Any durable graph
witness keeps the normal retry/takeover rules; the worker cannot label it empty.

### Cancellation and drain

Cancellation and drain are authenticated server lifecycle commands bound to
the current generation and worker operation.

- Drain closes new source admission, permits accepted inputs and Task retries to
  reach durable completion, commits EOF, runs the finite tail and then reports
  Flow completion. A bounded deadline may escalate to cancellation, never to an
  unrecorded success.
- Cancellation closes admissions, requests the current child to stop, confirms
  its exit through the selected process profile, records cancelled Task/Flow
  lifecycle and rejects all later holder writes.
- A worker or server restart replays the same command/operation. Conflicting
  commands are serialized and produce a named refusal.

Cancellation is two-phase. The admin command first moves the FlowRun to
`Cancelling` and fences writes. The current holder, or a replacement worker with
an authorized exact death challenge, then supplies the recorded process death
witness. Only this finalization can move Flow and nonfinal Tasks to `Cancelled`
and release the snapshot, launch, channels and retained payload. A verified dead
child with an exhausted retry policy uses a distinct retirement operation; it
asks normal orchestration policy first and refuses retirement while an
`AwaitingRetry` transition remains available.

If the child and its supervisor are lost after the Flow is already final, a
stale registered-child candidate may perform verified terminal cleanup without
rewriting the final Flow state. Release remains blocked while any bound TaskRun
terminal event is not yet recorded; the worker retries the named metadata-lag
refusal and retained payload stays available if recorder recovery takes longer.
An unclaimed native snapshot can be released after an accepted final Flow state
without death proof because no launch or execution authority ever existed.

The deployment binding and work-pool update paths lock the same WorkPool row
and refresh its ORM identity before merging the reserved capability marker.
This preserves a concurrent user template update in either lock order and
prevents a stale identity-map value from restoring an older template.

Flow retries, Task retries and infrastructure replacement remain separate
counters. Infrastructure replacement does not reset either retry budget.
In-process Flow retry cannot replace a sealed native graph; it retains the
existing ADR-0036 refusal and requires a supported replacement or new FlowRun.

### Native projections

Authenticated server projections expose bounded control data: declaration and
code digests, generation, holder/worker identity, launch/death-operation state,
source accepted position, consumer completed/in-flight position, EOF, finite
receipt status, lag/pressure, drain/cancel state and observation time. They never
return payload values, holder capabilities, private keys or parameter secrets.
UI views consume these projections instead of scanning hot record tables.

Projection rows are observations, not execution authority. Missing telemetry
cannot authorize state or progress. Resource limits bound result counts, prefix
reads, manifest bytes, channel bytes, retained receipts and projection history.
The `bounded-default-v1` profile admits at most 128 active native FlowRuns per
work pool under the same locked pool row used for marker updates. Exact snapshot
replay precedes this capacity check, a released snapshot frees one place, and
the recovery-candidate limit therefore covers the entire admitted pool without
starving a later loss behind an unbounded stale set.
The concrete UI and projection contract is tracked by
[ADR-0039](ADR-0039-native-progress-projections.md).

## Delivery slices

### N1.6b: bounded mixed SDK/runtime, +10% to 60%

ADR-0037 remains the current implementation slice. NI1-054 is `progress`; its
completion needs the real mixed SDK/process gate. This ADR does not claim that
share and current earned readiness remains 50%.

### N1.7: declaration and deployment launch, +10% to 70%

Implement the public Flow capability, immutable deployment/FlowRun binding,
worker enrollment, one-use secret transport and an actual work-pool launch.
Unknown or changed profiles fail before child start and DATA.

### N1.8: confirmed replacement, cancellation and drain, +15% to 85%

Implement the Linux identity/death operation, one-winner replacement, slot
continuity, generation fencing, cancellation and drain. Lease expiry alone has
an explicit negative control.

### N1.9: projections, compatibility and resource bounds, +5% to 90%

Add authenticated native projections and UI, mixed-version refusals, migration
compatibility and selected-profile resource bounds. This slice does not claim a
throughput SLO, online GC, high availability or cross-host recovery.

### N1.10: real mixed deployment acceptance, +10% to 100%

Run the complete selected profile through actual SDK, server, work pool and
worker processes with independent PostgreSQL oracles, lifecycle faults and
ordinary-flow regressions. Reaching 100% means the bounded native-foundation
objective in the epoch, not production HA or feature parity with a general
distributed stream processor.

## Stories

- As a developer, I want to declare a native streaming capability on a Flow so
  the deployment and worker can reject incompatible profiles before launch.
- As a user, I want one ordinary Prefect deployment to run finite and streaming
  tasks, survive a confirmed process loss, drain and cancel through supported
  lifecycle commands.
- As a system, I want verifiable process identity and single-winner generation
  replacement without treating timeout as proof of death.
- As a user, I want bounded native progress and recovery projections without
  exposing records, parameters or authority credentials.
- As a system, I want Flow, Task and infrastructure retry counters plus one
  concurrency slot to remain consistent across replacement.

## Acceptance criteria

### N1.7

- [ ] As a developer, I can declare the supported native protocol/profile on a
  Flow and persist the same immutable digest in its deployment and FlowRun.
- [ ] As a system, I can reject a missing, changed or unsupported declaration
  before child launch and before DATA while ordinary deployments are unchanged.
- [ ] As a system, I can enrol an authenticated worker and claim generation one
  before Flow `Running`, with an exact-replay launch operation.
- [ ] As a system, I got holder/event credentials delivered only through a
  bounded inherited channel and absent from parameters, events, logs and API/UI
  projections.
- [ ] As a user, I got the selected mixed Flow launched by an actual Prefect
  work pool and worker with one concurrency slot.

### N1.8

- [ ] As a system, I can identify the exact Linux child using boot ID, PID and
  process start ticks, terminate a live match through `pidfd`, and avoid a reused
  PID or foreign process.
- [ ] As a system, I can refuse replacement when only a lease expired or when
  the recorded host/process identity cannot be inspected.
- [ ] As a user, I got one replacement winner after confirmed child or worker
  loss, the same FlowRun/TaskRuns/channels, the first incomplete input and stale
  former-owner writes/events rejected.
- [ ] As a system, I can preserve the one concurrency slot and existing Flow and
  Task retry budgets across infrastructure replacement.
- [ ] As a user, I can drain accepted work through EOF and tail completion, or
  cancel the current generation and observe later writes rejected.
- [ ] As a system, I got server/worker restart and command-replay controls with
  no double release, duplicate launch or unrecorded success.

### N1.9

- [ ] As a user, I can inspect authenticated generation, worker, progress,
  in-flight, lag, EOF, receipt and lifecycle projections without payloads or
  secrets.
- [ ] As a developer, I can upgrade and downgrade paired schema revisions and
  get a named pre-DATA refusal for incompatible SDK/server/worker/deployment
  protocol or code versions.
- [ ] As a system, I got bounded manifest, channel, receipt, prefix, projection,
  log and memory behavior for the selected profile, with SQLite mutations still
  fail-closed.
- [ ] As a user, I got documented operational limits without a production HA,
  cross-host recovery or throughput claim.

### N1.10

- [ ] As a developer, I can deploy the versioned mixed Flow through a real work
  pool and run sync/async finite prefix, native source/operator and finite tail
  without a test launcher or prototype storage adapter.
- [ ] As a user, I got early downstream delivery, exact document replay after
  pre-commit failure, continuation after lost acknowledgement, confirmed worker
  replacement, drain and cancellation on actual PostgreSQL storage.
- [ ] As a system, I got stale-worker, PID-reuse, timeout-only, mixed-version,
  server-restart and resource-bound hostile controls with independent durable
  state/progress oracles.
- [ ] As a system, I got ordinary Flow/Task/generator, package, migration,
  client/server and UI regression gates with no native-lane skips.
- [ ] As a user, I got an independently reviewed, source-bound acceptance and
  runbook that states the bounded foundation and all remaining production
  limits.

## Consequences

- A lease expiry marks the execution suspect and schedules verification. It
  cannot advance generation.
- A live exact old child must be terminated and observed exited. A different
  process at the same PID is not touched.
- A successor without access to the recorded Linux host fails closed. The first
  profile makes no cross-host recovery promise.
- Losing a supervisor capability does not authorize reconstructing it from
  Flow parameters or database rows. The server issues a new capability only
  after the single-use death operation.
- A projection outage or stale observation cannot change lifecycle or stream
  state.
- A deployment declaration mismatch, code drift or version mismatch refuses
  before DATA; it does not silently fork channels or TaskRuns.
- Reconstruction in the first profile still requires the bound Deployment and
  its local workspace to be readable. Removal or loss of that workspace fails
  closed and may require operator repair; the profile does not package code or
  claim remote artifact recovery.
- More than 128 concurrently active native FlowRuns in one work pool are
  refused by the named resource profile. This bound keeps recovery discovery
  complete without an unbounded response.
- Cancellation may discard unaccepted client work. External effects remain the
  client's idempotence responsibility.

The design adds durable launch and lifecycle records plus operational surface
area. In return, the final native profile no longer depends on an in-memory test
supervisor for its safety argument. General hostile-worker isolation, remote
attestation, Kubernetes pod replacement, regional failover, unbounded streams,
online GC and performance SLOs remain separate decisions.

## Evidence

No N1.7-N1.10 implementation evidence exists at decision creation. The accepted
[whole-flow reconstruction](../evidence/native-stage5c-acceptance-2026-11-03.json),
[finite authority gate](../evidence/native-stage6a-corrected-acceptance-2026-11-04.json)
and ADR-0037 mixed-runtime work are prerequisites, not proof of deployment or
worker acceptance. Each slice must link real HTTP/PostgreSQL/process records and
an independent review before its percentage is earned.

## Implemented slices and local acceptance

The declaration, deployment binding, enrollment, inherited-FD launch,
ProcessWorker discovery/replacement and native lifecycle paths are implemented
in commit `35dc8d5`. The preceding delivery-slice paragraphs describe the
original plan; implementation is no longer pending.

The [local epoch ledger](../evidence/native-epoch1-local-acceptance-2026-11-04.json)
and [independent Principal review](../evidence/native-epoch1-principal-local-2026-11-04.json)
cover the selected N1.7–N1.10 criteria:

- `worker-http`: 22 real deployment/process cases, including sync/async flows,
  five document/receipt cuts, real 120-second orphan recovery with competing
  workers, drain, SDK/CLI and orphan cancellation, live launcher refusal,
  pre-spawn failure, failure before any TaskRun and exhausted retry retirement.
- `kernel`: exact Linux process identity, PID-reuse/foreign-scope controls and
  unchanged ordinary runner/task behavior.
- `deployment-api`: declaration/authority/version/replay/capacity controls,
  worker registration and actual concurrent work-pool row-lock races.
- `native-http`, `finite-http`, `store-api`, `recorder`, `lease` and lifecycle
  profiles: fenced durable progress, signed events, metadata reconciliation,
  concurrency-slot continuity and release.
- Paired migration/client/SQLite profiles, strict type checks and the packaged
  UI gate preserve compatibility and explicit operational bounds.

[ADR-0040](ADR-0040-native-integration-acceptance-and-operations.md) records the
source bridge, corrected first failures, exact scope and remaining hosted CI
condition. These observations do not add a general HA or cross-host guarantee.

## Related documents

- [ADR-0024: Control-plane authority and lifecycle](ADR-0024-control-plane-authority-and-lifecycle.md)
- [ADR-0025: Prefect adapter and operational projections](ADR-0025-prefect-adapter-and-operational-projections.md)
- [ADR-0036: Durable submission recovery](ADR-0036-durable-submission-recovery.md)
- [ADR-0037: Finite tasks around a streaming graph](ADR-0037-mixed-finite-and-streaming-tasks.md)
- [ADR-0039: Native progress projections](ADR-0039-native-progress-projections.md)
- [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- [Task registry](../todo.md)
- [Sprint board](../sprint_board.md)
