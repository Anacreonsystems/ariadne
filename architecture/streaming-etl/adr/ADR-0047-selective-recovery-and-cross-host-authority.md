# ADR-0047: Selective recovery and verified cross-host authority

- Status: `planning`
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, worker/server maintainers
- Related: [architecture](ADR-0042-flink-informed-native-streaming-architecture.md), [local lifecycle](ADR-0038-native-deployment-worker-lifecycle.md), [graph](ADR-0043-physical-graph-and-partitioned-channels.md), [state](ADR-0044-multi-input-turns-and-state-materialization.md), [acceptance](ADR-0048-rescaling-compatibility-and-native-acceptance.md)

## Problem

The accepted launcher owns a whole FlowRun process and verifies same-host Linux
process death with access to its PID namespace. Discovery selects that host.
Neither a heartbeat timeout nor changing a host field proves that the old
process has stopped. Local file references and deployment paths are also not
automatically accessible on a replacement machine.

## Theory

Independently fenced physical attempts plus durable channel boundaries can
make recovery smaller than a whole FlowRun. Cross-host replacement can preserve
strict no-overlap execution only with a trusted stop witness and portable code,
state and object references.

## Proposed solution and agreement

- Separate FlowRun/coordinator authority from physical subtask attempt epochs.
  A failed subtask can retry without placing healthy peers or the entire flow
  into retry. All platform mutations validate graph/assignment/attempt identity
  under an explicit lock order and the FlowRun cancellation fence.
- Compute the affected region from lost ownership, non-durable data, shared
  state and placement. Durable accepted partitions are recovery boundaries.
  If B fails, C can continue consuming B's already accepted records and later
  wait for B; restarting C is not inherently required. If B and C share a
  failed process, both must be replaced. Unavailable required durable data is
  a recovery failure, not permission to regenerate accepted upstream outputs.
- Preserve stable subscriber/partition identities and accepted frontiers;
  materialize state as in ADR-0044. Freeze recovery inputs and pins before
  reassignment. Reject the old physical epoch after activation.
- Initial cross-host capability requires provider-verified termination of the
  old workload, or a trusted old-host agent's verifiable exit witness. Bind it
  to the exact execution identity, host, incarnation and authority generation.
  Timeout, lease expiry and lost network access are eligibility signals only.
- Package identity, compatible runtime/state schema and durable object
  references must be resolvable on the target host before placement activates.
  Two containers on one kernel do not establish two-host acceptance.
- Keep bounded recovery budgets, durable cancellation and one FlowRun capacity
  lease. A cancelled old coordinator must not resurrect subtasks after takeover.

### All-or-none regional activation

Persist one immutable recovery plan binding graph/assignment/coordinator
generations, participant set, target attempts/placements, package/state
compatibility, accepted frontier, stop witnesses and input/state/retention pins.
Targets remain `RESTORING`: they can verify/materialize that plan but cannot
invoke business code, claim live inputs, fire timers or publish data.

All targets must verify the same plan digest and report ready. One atomic server
transaction checks those attestations, cancellation, stop proofs and capacity,
publishes the new active region generation and activates all scoped grants.
Before it, the old assignment remains the recoverable authority (failed/stopped
processes need not be live); after it, all old region writers are fenced.
Unaffected regions retain their existing owners. Do not activate individual
targets independently or invalidate healthy peers through a global owner swap.

Coordinator/target loss before activation leaves the immutable plan resumable
or explicitly aborted with its pins retained until safe release. A changed
target attempt requires a superseding plan, not mutation of a ready plan's
participant identity. Lost activation responses resolve by plan ID/generation;
never activate again. Failure after activation uses the new generation and
cannot roll back to the old writer. Cancellation wins any competing activation.

## Discussion and alternatives

Flink region recovery is not a universal promise to restart only the failed
streaming task. Pipelined streaming edges can make a large region; failed or
unavailable partitions can expand recovery. Ariadne's durable edge boundaries
provide a different tradeoff: more persistence work in exchange for narrower
logical recovery. Physical placement remains an independent boundary.

A platform-fenced replacement without verified physical death could preserve
platform-write authority while old code continues external effects. That is a
different overlap/availability contract and is **not admitted** by this ADR.
Client idempotency remains required even under the strict profile; platform
fences cannot intercept arbitrary writes to an external PostgreSQL database.

Strict verified-stop replacement can sacrifice availability during a network
partition. Fail closed until a valid stop witness exists; do not call an
unreachable host dead. Infrastructure-provider support is a capability gate.

## Stories

- As a user, I want Anna's replacement to resume her durable cursor while healthy Petya tasks continue.
- As an operator, I want to know which physical processes a failure will restart and why.
- As a system, I want stale owners to be unable to mutate native state after a replacement activates.

## Acceptance criteria

- As a system, I can kill one real operator process, retain healthy source/sibling process identities, and recover input 200 or 201 according to terminal acceptance.
- As a user, I can continue consuming an accepted downstream suffix while its producer is being replaced, without deleting or regenerating that suffix.
- As a system, I can reject old coordinator, assignment and subtask epochs under concurrent append, completion, cancellation and recovery.
- As an operator, I got real two-host replacement with a bound verified-stop witness, portable package/state/object references and unchanged durable data identities.
- As a system, I can refuse an unreachable-host replacement without a witness and report the precise capability/availability limitation.
- As a developer, I can reproduce recovery-region expansion for a shared-process failure and distinguish it from one isolated subtask failure.
- As a system, I can lose the coordinator or any target around plan persistence, restore, readiness and activation/response while observing one complete old or new active region, never a mixed assignment.

## Evidence and current state

Planning and audit only. Existing same-host pidfd acceptance is retained in
ADR-0038; it is not cross-host evidence. No distributed placement provider or
cross-host replacement test is claimed in this cycle.

## Consequences and failure modes

The scheduler must account for placement and correlated failure, not just graph
edges. A valid fence protects platform state but does not undo effects made
before a failed turn. Missing durable data, invalid stop proofs and incompatible
runtime/state versions all block activation explicitly.

## Work approval and stable criterion IDs

Approved for work on 2026-11-04 as part of the ADR-0042–0048 set at revision
`fae53c90aa3b44dbc45032833b4d193001e5c8bd`; see the
[approval record](../evidence/flink-architecture-2026-11-04/epoch2-work-approval-2026-11-04.json).
It depends on ADR-0043 physical ownership, ADR-0044 portable state/frontiers and
ADR-0046 recovery pins/capacity. The current environment has no verified
cross-host provider/agent capability, so A47-04 remains an explicit later gate.

| Stable ID | Existing acceptance criterion |
| --- | --- |
| A47-01 | Replace one operator process while healthy identities remain. |
| A47-02 | Consume an accepted suffix during producer replacement without regeneration. |
| A47-03 | Reject stale coordinator, assignment and subtask epochs under races. |
| A47-04 | Real two-host replacement with verified stop and portable artifacts. |
| A47-05 | Refuse unreachable-host replacement without a valid witness. |
| A47-06 | Reproduce recovery-region expansion versus isolated failure. |
| A47-07 | Fault regional plan/restore/activation without mixed assignment. |
