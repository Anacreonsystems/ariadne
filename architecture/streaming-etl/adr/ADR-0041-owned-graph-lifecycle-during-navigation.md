# ADR-0041: Own graph callbacks across FlowRun navigation

- Status: `progress`
- Date: 2026-11-04
- Owners: UI maintainers; native foundation acceptance owners
- Depends on: [native projections](ADR-0039-native-progress-projections.md)
- Blocks: [final native acceptance](ADR-0040-native-integration-acceptance-and-operations.md)
- Delivery: [task registry](../todo.md), [sprint board](../sprint_board.md)

## Problem

The actual packaged-UI acceptance navigates from a completed native FlowRun to
one of its existing TaskRuns. An initial browser probe recorded its error
snapshot before closing the context and then received a late uncaught
`Cannot read properties of null (reading 'x')`. That trial cannot establish
the zero-page-error criterion. Its raw output is retained as a failed discovery,
even though its prematurely written result contained an empty error array.

Twenty-four bounded replays initially did not reproduce the error. A corrected
probe, with complete event timing, then reproduced it before browser teardown:
TaskRun visible at 2,893 ms, page error at 2,896 ms and context close at 3,431 ms.
The stack maps to a queued 250 ms `lodash.throttle` layout callback reading a
destroyed Pixi element's position. Independent source comparison found the
same event/artifact factory bytes in upstream `87b5243` and the native
implementation's preceding commits. This is an existing scene-lifecycle defect
exposed by the new real navigation gate.

Further tracing exposed the same lifetime problem in an outstanding event-data
response: it can call a root render after `application.destroy()` cleared the
stage. Factories completing after the global emitter was cleared can also
register new listeners before their parent rejects their late result. Merely
cancelling one timer does not release those subscriptions or fence that work.

The [initial boundary review](../evidence/native-epoch1-ui-late-pageerror-principal-boundary-2026-11-04.json)
and [confirmed diagnosis](../evidence/native-epoch1-ui-late-pageerror-principal-diagnosis-2026-11-04.json)
retain the distinction between an unconfirmed observation, its later
reproduction and the resulting acceptance blocker.

## Theory

The scene that owns a callback must also own its cancellation and subscriptions.
Destroying that scene ends its right to mutate layout, attach children, publish
fetched data or schedule another poll. An asynchronous operation may finish
after destruction; its result must then be discarded and its newly allocated
resources destroyed without touching a newer scene.

Pixi's existing container `destroyed` lifecycle provides the local ownership
boundary. Captured stage identity and a local start/stop scope distinguish an
old initialization from a later graph. This can fix the observed defects
without changing the streaming protocol, server lifecycle or public graph API.

## Solution and agreement

1. Event/artifact containers cancel their queued throttles and remove named
   emitter subscriptions when destroyed. Layout work checks owner liveness
   after asynchronous boundaries before reading or rendering Pixi elements.
2. Late leaf/cluster results are destroyed instead of attached to a dead
   container. Their wrapper and low-level factories remove their own global
   subscriptions on destruction, including listeners registered after a prior
   global emitter clear.
3. Root event/artifact renderers capture their stage before awaiting children
   or data. A stopped initialization cannot overwrite a later graph's stop
   handle or mutate its scene. Event-data stop suppresses late callbacks and
   polling rescheduling, including when a request was already outstanding.
   Application initialization also owns a local instance and generation: only
   an initialized current instance may be published or appended. Stopping an
   unfinished initialization fences it; a late completion destroys its own
   instance and cannot replace a newer application or register a resize
   listener. Stale rejected event requests cannot report errors for a later
   graph.
4. The live browser probe records stack/timeline information, observes settled
   TaskRun navigation through context/browser closure and only then writes its
   final error snapshot and verdict. An injected late browser error must make
   it exit unsuccessfully and retain a failed result.

A separate global `graphStopping` signal was considered. It was not selected:
the existing owned-container destruction event and local start/stop scope
express the required boundary without another global event protocol.

These changes close an operational UI criterion within N1.9/N1.10 and earn no
additional percentage beyond the original native foundation allocation.
Earlier backend and process evidence remains valid only through an explicit
source-relevance bridge proving those bytes unchanged. Hosted acceptance of
the native runtime and of this UI successor are recorded separately when their
source commits differ. No old failed result is rewritten as a passing run.

## Stories

- As a user, I want to follow a native FlowRun's TaskRun links while layout and
  data requests are pending without the old page continuing to mutate graphics.
- As a developer, I want callbacks, listeners, timers and late resources to
  have an explicit owner that survives asynchronous control flow correctly.
- As a system, I want a stopped graph to reject old work while a new graph
  continues to receive its own updates.
- As an operator, I want the real browser gate to fail for an error occurring
  after navigation rather than publish a premature successful snapshot.

## Acceptance criteria

- [ ] As a developer, I can reproduce the original queued-layout/late-data
  failure with actual Pixi containers, the real throttle and controlled delayed
  boundaries; the corrected implementation passes meaningful disposal tests.
- [ ] As a system, I can cancel queued work, fence in-flight layout, destroy
  late leaves/clusters and release wrapper/low-level emitter subscriptions.
- [ ] As a system, I can stop during initialization or an outstanding request,
  start a new graph and retain only the new owner's callbacks and polling.
- [ ] As an operator, I got a nonzero result and retained error/timeline from
  the corrected probe when a real late browser error is deliberately injected.
- [ ] As a user, I can navigate a fresh actual eight-document packaged
  PostgreSQL/ProcessWorker flow through TaskRun links with a clean final
  post-close browser error snapshot and exact served bundle-byte proof.
- [ ] As a developer, I got the complete UI regression suite, selected native
  and lifecycle regressions, 33 browser/accessibility cases, types, lint,
  formatting and production build on the successor source.
- [ ] As a system, I got reviewed source relevance, unchanged Python/backend
  evidence and successful hosted UI/static/CodeQL checks on the actual final
  source before this decision is accepted.

## Consequences

The same FlowRun and TaskRun pages remain the public navigation surface.
Scene lifetime becomes an explicit internal boundary rather than an assumption
that clearing a global emitter also cancels asynchronous work. A completed
network response can legitimately be ignored after its scene ends.

This decision does not alter stream ordering, durable records, cursors,
generation fencing, retry budgets or effect idempotency. It establishes no
general UI performance or memory SLO. The broader foundation remains bounded
by [ADR-0040](ADR-0040-native-integration-acceptance-and-operations.md).
It does not promise that every historical prerequisite-wait Promise settles
after stop: clearing its event listener prevents it from receiving a newer
graph's events. No global rejection/cancellation API is introduced for those
void startup operations. The claim is bounded to scene mutations,
subscriptions, polling and resource ownership.

## Evidence and implementation

The observed paths include [event collections](../../../ui-v2/src/graphs/factories/runEvents.ts),
[artifact collections](../../../ui-v2/src/graphs/factories/runArtifacts.ts),
[collision layout](../../../ui-v2/src/graphs/utilities/detectHorizontalCollisions.ts),
[event data](../../../ui-v2/src/graphs/factories/eventData.ts),
[root event ownership](../../../ui-v2/src/graphs/objects/flowRunEvents.ts) and
[root artifact ownership](../../../ui-v2/src/graphs/objects/flowRunArtifacts.ts).
The [real-container regression](../../../ui-v2/tests/graphs/run-graph-teardown.test.tsx)
and [packaged live probe](../../../ui-v2/tests/browser/native-stream-live.mjs)
are the executable acceptance boundaries. Final successor reports, first
failures, source hashes and hosted results are pending; implementation alone
does not close this decision.
