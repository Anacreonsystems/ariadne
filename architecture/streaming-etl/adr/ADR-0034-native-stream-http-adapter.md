# ADR-0034: Native stream HTTP adapter and runtime admission

- Status: `pending`
- Date: 2026-11-01
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md), Stage 2 / N1.3
- Owners: SDK/server maintainers, ETL Architect / HighLoad Principal
- Depends on: [ADR-0032](ADR-0032-native-task-stream-contract.md),
  [ADR-0033](ADR-0033-native-durable-stream-storage.md)
- Delivery: [task registry](../todo.md), [sprint board](../sprint_board.md)

## Problem

Native server storage alone is not an SDK connection. Workers must use the
Prefect API rather than receive database credentials or import server models
from the stripped client package. The TaskRun engine publishes states through
asynchronous events; its local RUNNING attempt can precede the durable server
snapshot required by storage admission.

## Theory

An authenticated API can expose committed store operations and translate frozen
bindings/tokens without moving progress authority into a client process.
Recovery requires an explicit handshake for TaskRun metadata lag and compatible
versions/capabilities.

## Proposed solution

This is a pending handoff, not a finalized HTTP contract. Review must settle:

- Routes use existing API authentication and native DB dependency injection;
  stream scope and TaskRun identities come from stored rows.
- The adapter carries safe routing/identity data and obtains a client on the
  caller loop. Credentials, an AsyncClient, a pool or server imports must not
  be serialized into task parameters.
- Requests carry immutable bindings/tokens. Responses follow commit. Error
  mapping distinguishes stale ownership, metadata not ready, unsupported
  capability, changed intent, size/capacity and released replay horizon.
- Admission waits for or establishes a verified current RUNNING TaskRun
  snapshot. It cannot invent attempt 999 or overwrite orchestration authority
  to bypass event lag. Cancellation and attempt changes fence record writes.
- Resolve output reservation and rejected-start cleanup before integration:
  an output can exist before its TaskRun is recorded or an owner is admitted.
  A worker failure in that interval needs authenticated lifecycle authority to
  close the reserved output, without forging a RUNNING attempt or a data-owner
  epoch. Otherwise downstream can wait forever on an unclaimed OPEN stream.
  The six Stage 1 startup-failure cases must be repeated against the native
  transport, rather than inherited from its test adapter.
- Native DB protocol version 1 and terminal-event contract `yield-v2` are
  separate axes. Unsupported combinations reject before data admission; there
  is no global flow mode.
- The adapter inherits bounded capacity and whole-flow release. Automatic HTTP
  retries must not loop forever on permanently undersized frozen limits.

Source SDK, whole-flow/infrastructure recovery, branch/join, full deployment
lifecycle, online GC and production HA/throughput retain later decisions.

## Stories

- As a developer, I want native tasks to use durable storage through the normal
  client without database access.
- As a user, I want a lost response or restarted client to recover durable
  progress without another logical consumer or output.
- As a system, I want unsupported backends, stale attempts and cross-flow
  requests refused before mutation.

## Acceptance criteria

- [ ] As a developer, I can run sync/async native TaskRuns through an actual
  HTTP adapter with native migrations and no prototype module.
- [ ] As a system, I can handle metadata lag without accepting a future or
  cancelled TaskRun generation.
- [ ] As a user, I can recover around terminal acceptance after client process
  loss, lost HTTP responses and a server restart.
- [ ] As a system, I can terminate rejected/unstarted output reservations
  without weakening fencing or leaving a downstream dependency OPEN forever.
- [ ] As a developer, I got DTO/error parity, API authentication and a working
  stripped-client package.
- [ ] As a system, I can reject unsupported backends/versions, cross-flow
  bindings and released references without partial data.
- [ ] As a developer, I got independent review and real HTTP/PostgreSQL gates.

## Evidence

No implementation or acceptance is claimed. N1.2 evidence in ADR-0033 does not
replace this HTTP/process gate.

## Consequences

Acceptance would complete Stage 2's transport boundary and enable the later
native source/runtime sprint. It would not establish production capacity or
complete the native foundation.
