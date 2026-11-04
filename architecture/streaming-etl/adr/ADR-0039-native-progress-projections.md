# ADR-0039: Native streaming progress projections

- Status: `progress`
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, server and UI maintainers
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md), N1.9
- Depends on: [ADR-0038](ADR-0038-native-deployment-worker-lifecycle.md), [ADR-0037](ADR-0037-mixed-finite-and-streaming-tasks.md)

## Problem

An ordinary TaskRun state does not describe incremental stream progress. A
running producer can have durable records available while its consumer is
retrying one unfinished input. The FlowRun page must expose that distinction
without displaying payloads or fetching the record log. Execution credentials,
private event keys and process identities must not become browser data.

Native streaming is an explicit capability of an ordinary Prefect Flow. Adding
its operational view must not cause ordinary flows to query new endpoints on
older servers or replace the existing FlowRun/TaskRun views.

## Theory

A bounded read-only projection can join durable control rows, stream headers,
consumer positions and finite receipt existence. Its cost depends on the
bounded graph size, not on the number or size of records. A single observation
shows the generation, launch phase, source admission, durable positions and
consumer lag. It cannot authorize a retry, completion or takeover.

The user can follow a streaming task to the same TaskRun used by the normal
Prefect UI. A distinct observation time and explicit loading/error state keep
missing telemetry from appearing as completed work. Progress coordinates refer
to durable platform acceptance; an in-flight position is separate from a
completed-through position.

## Proposed solution and agreement

The authenticated API exposes
`GET /streams/v1/deployments/{flow_run_id}/projection`. Its frozen public DTO
contains bounded task and finite-work lists, without stream envelopes, parameter
values, finite result values, holder capabilities, event keys, PID, host boot
identity or process-control credentials. The server selects fields explicitly;
it does not serialize ORM objects into the response.

The existing FlowRun details page includes a streaming progress card for runs
whose server label declares the native capability. Ordinary runs make no new
projection requests. The card uses the same API client, authentication, CSRF
and unauthorized-response behavior as the rest of the application. Types are
generated from the actual server OpenAPI schema.

The card displays generation and lifecycle state, worker identity, durable
source/output counters, completed-through and in-flight input positions, EOF,
finite receipt status and observation time. Stream/task rows link to their
ordinary TaskRun detail pages. Lag comes from the consumer's input channel,
never from its own output count. Pressure uses persisted stream limits and
header counters rather than counting log rows.

The shared API transport keeps ordinary string error details unchanged and
formats a native `{code, message}` detail as a named refusal. Deployment-run
forms can therefore display a native capacity error instead of an object
placeholder. Malformed objects, validation arrays and non-JSON upstream errors
use a status-only fallback without serializing client input.

A native projection failure is visible with a retry action. The application
does not turn a 404, authorization failure, network outage or server error into
an empty successful stream. Loading does not suspend the ordinary FlowRun
page. Polling refreshes observations and never mutates execution state.

After final execution release, the retention contract deletes consumer rows and
finite receipt payloads. The projection then reports missing consumer positions
and receipt absence. The card explicitly labels those fields as unavailable or
no longer retained; it does not label them as zero progress or awaiting a future
receipt. Published positions remain available from retained stream headers.
This view describes current retained data, not an archival consumer history.

The card includes a confirmed drain action through ADR-0038's native lifecycle
API. The existing cancel dialog uses the native generation-bound cancellation
command after enrollment; before enrollment it keeps ordinary orchestration's
cancellation path. Unsupported generic pause, retry and forced state changes
are not offered for native runs. The same mutation operation ID is retained
when a response is unconfirmed; a generation change is observed before another
explicit user action can submit against the new generation. Neither action
puts holder credentials into the browser. General dashboards,
throughput SLOs, unbounded history, cross-host recovery and legacy Vue parity
are outside this bounded delivery slice.

## Resource bounds

The selected `bounded-default-v1` resource profile caps ACTIVE native deployment
snapshots at 128 per work pool under the existing pool lock. A 129th FlowRun is
refused and its creation rolls back. Exact operation replay is checked before
capacity admission. Releasing an unclaimed snapshot frees capacity for a new
run. The recovery-candidate response's 128-row bound can therefore cover the
entire eligible active pool instead of indefinitely hiding an eligible run
behind an unbounded backlog.

The public projection caps stream-task rows at 128 and finite-task rows at two;
channel limits and persisted header counters bound each displayed pressure
value. These are selected-profile bounds, not a throughput SLO or archival
history/online-GC guarantee. [ADR-0038](ADR-0038-native-deployment-worker-lifecycle.md)
and its real PostgreSQL controls govern server-side enforcement.

## Stories

- As a user, I want to see durable source and consumer progress while a
  streaming FlowRun is running, and open its normal TaskRun pages.
- As a developer, I want the browser to use generated API types and the shared
  authenticated transport while ordinary workflows keep their existing UI.
- As a system, I want bounded read-only observations without exposing payloads
  or execution authority.

## Acceptance criteria

- [ ] As a user, I can inspect generation, worker, launch/lifecycle state,
  completed-through, in-flight input, lag, EOF and finite receipt status with
  an explicit observation time.
- [ ] As a user, I can request native drain/cancellation with a generation-bound
  command, safely retry an unconfirmed response and cancel a run before worker
  enrollment through ordinary orchestration.
- [ ] As a user, I can follow each streaming or finite task to its existing
  TaskRun and distinguish pending work from accepted completion.
- [ ] As a system, I can return the projection from bounded control/header
  reads without payloads, results, parameters or private/process credentials.
- [ ] As a user, I got named native capacity/lifecycle errors while ordinary
  string errors remain unchanged and malformed errors do not echo client input.
- [ ] As a developer, I got actual-server OpenAPI types, API/component tests,
  accessible loading/error states, Storybook cases and a passing UI build.
- [ ] As a user, I got ordinary FlowRun UI regression coverage and no new
  projection calls for runs without the native capability.

## Implementation

- [Public server projection DTOs](../../../src/prefect/server/schemas/native_deployments.py)
  and [bounded projection reads](../../../src/prefect/server/models/native_deployments.py).
- [Shared native/ordinary error transport](../../../ui-v2/src/api/service.ts)
  and [real client wire tests](../../../ui-v2/src/api/service.test.ts).
- [Generated-type UI query and lifecycle mutations](../../../ui-v2/src/api/native-streams/index.ts).
- [FlowRun progress card](../../../ui-v2/src/components/flow-runs/native-stream-progress/index.tsx)
  and [generation-aware lifecycle dialog](../../../ui-v2/src/components/flow-runs/native-stream-progress/native-stream-control-dialog.tsx).
- [Existing cancel dialog integration](../../../ui-v2/src/components/flow-runs/flow-run-actions/cancel-flow-run-dialog.tsx)
  and [FlowRun page](../../../ui-v2/src/components/flow-runs/flow-run-details-page/index.tsx).
- [API/operation replay tests](../../../ui-v2/src/api/native-streams/native-streams.test.ts),
  [progress/control tests](../../../ui-v2/src/components/flow-runs/native-stream-progress/native-stream-progress.test.tsx)
  and [browser/WCAG story probe](../../../ui-v2/tests/browser/native-stream-progress.mjs)
  and [real served-UI probe](../../../ui-v2/tests/browser/native-stream-live.mjs).

The React UI exposes the view on the existing FlowRun details page, under the
server's configured v2 UI base path. Browser checks cover light/dark desktop and
mobile layouts. The story probe uses MSW fixtures and is not a substitute for
ADR-0038's real HTTP/PostgreSQL/deployment acceptance.

To reproduce the browser checks, start Storybook on the same checkout and then
run the probe with the installed Playwright browser (or an explicit Chromium
executable):

```bash
cd ui-v2
npm run storybook -- --host 127.0.0.1 --port 6008 --ci --no-open
# In another shell:
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/browser/native-stream-progress.mjs
```

`NATIVE_STORYBOOK_URL` and `NATIVE_BROWSER_OUTPUT` override the local URL and
artifact directory. The probe checks the card and lifecycle dialog, takes
screenshots and records browser errors, WCAG violations and page overflow.

## Evidence and status

The [complete UI gate](../evidence/native-stage9-ui-complete-2026-11-04/acceptance.json)
passes against 20 unchanged UI source paths: 97 real-client-wire/component/
ordinary-view regressions with zero failures or skips, full TypeScript analysis,
full ESLint and a production build. The earlier
[17-file UI gate](../evidence/native-stage9-ui-2026-11-04/acceptance.json) remains
a preserved predecessor before the shared native-error transport and live probe. ESLint retains 62 existing warnings; format/build
warnings remain in the captured output. The gate adds no foundation readiness
percentage by itself.

The [actual Chromium story results](../evidence/native-stage9-ui-complete-2026-11-04/browser.json)
pass 33 light/dark/desktop/mobile cases with zero page errors or selected
WCAG 2 A/AA violations. All 21 progress-card layout cases also have no page
overflow. These checks use Storybook/MSW fixtures. Sample captures show
[light](../evidence/native-stage9-ui-2026-11-04/desktop-light-running.png),
[dark](../evidence/native-stage9-ui-2026-11-04/desktop-dark-running.png),
[mobile](../evidence/native-stage9-ui-2026-11-04/mobile-light-running.png),
[released history](../evidence/native-stage9-ui-2026-11-04/desktop-light-released.png)
and [cancellation](../evidence/native-stage9-ui-2026-11-04/desktop-dark-control-cancel.png).

The evidence retains failed discovery controls and their corrections: initial
tests before implementation, test-fixture corrections, a real dark-link contrast
failure, the browser harness correction and released-history regression. An
actual browser also exposed an outdated generated MSW worker; official
regeneration and normal project tooling corrected it before the final gate.

The [actual packaged-UI smoke](../evidence/native-stage9-ui-complete-2026-11-04/live.json)
also passes against a real authenticated API, PostgreSQL and ProcessWorker.
A fresh eight-document mixed deployment completes in generation one and releases
its retained data. Chromium reads the actual `/v2` FlowRun page and native
projection, observes both published positions at eight, checks all four existing
TaskRun links and opens the operator's real TaskRun page. There are zero page
errors and zero service workers; this check uses no MSW or response mocks. The
[served capture](../evidence/native-stage9-ui-complete-2026-11-04/live-flow-run.png)
and [exact HTTP asset check](../evidence/native-stage9-ui-complete-2026-11-04/live-bundle.json)
prove that entry references and the new native-error transport bytes match the
[fresh package build](../evidence/native-stage9-ui-complete-2026-11-04/package.json).

The normal build command packages both UI bundles. The final React build is
copied into its standard package data directory; the legacy bundle is unchanged
in that final copy. The server copies assets into its configured static cache
using a version/base-path reference. Rebuilding the same development version
therefore requires a fresh owned static directory or explicit cache invalidation
for an accurate live check. The final smoke uses a fresh owned static directory.

The live probe additionally retains its first URL assertion correction: the
normal TaskRun `tab=Logs` search parameter is accepted by checking the pathname.
The raw HTTP asset probe declares `Accept: text/html` to match real browser
navigation instead of assuming a non-HTML request gets SPA fallback.

The common [local epoch acceptance](../evidence/native-epoch1-local-acceptance-2026-11-04.json)
and [independent review](../evidence/native-epoch1-principal-local-2026-11-04.json)
now pass. This ADR remains `progress` until hosted CI and the final graph/status
closure in [ADR-0040](ADR-0040-native-integration-acceptance-and-operations.md)
pass.
The [task registry](../todo.md), [sprint board](../sprint_board.md) and
[epoch criteria](../epochs/epoch-01-native-prefect.md) remain authoritative for
delivery readiness.

## Consequences

The browser gains a bounded streaming observation alongside ordinary run
metadata. This adds one read endpoint for explicitly native runs and a compact
operational card. Projection availability remains independent of progress
acceptance: a failed or stale observation grants no execution authority.

## CI security successor: private artifacts and worker message origins

The hosted CodeQL review identified two predictable temporary report paths and
an unchecked message origin in the generated MSW worker. The
[local security successor](../evidence/native-stage9-ui-codeql-successor-2026-11-04/acceptance.json)
changes exactly three UI/test-support files and preserves the earlier
[complete UI gate](../evidence/native-stage9-ui-complete-2026-11-04/acceptance.json)
and [initial UI gate](../evidence/native-stage9-ui-2026-11-04/acceptance.json).
The other seventeen UI source paths remain byte-identical. This correction
adds no native-foundation readiness percentage or streaming capability.

Both browser probes now create an exclusive random default output directory
with `mkdtemp` under the operating-system temporary directory. The
[filesystem checks](../evidence/native-stage9-ui-codeql-successor-2026-11-04/permissions.json)
verify mode `0700`, current-user ownership and absence of a symlink for both
defaults. An explicit `NATIVE_BROWSER_OUTPUT` remains an operator-selected path;
the actual served-UI check also verifies that this override is preserved.
The probes print the chosen artifact directory in their console reports.

The worker rejects a message whose origin differs from its own origin before
looking up the source client. It also checks the resolved client's URL origin
before handling protocol data. The real Chromium worker accepts a native
`KEEPALIVE_REQUEST` with the expected same origin. A synthetic foreign-origin
`ExtendableMessageEvent` dispatched through that actual handler reached client
lookup before the correction and is rejected before lookup afterward. The
[expected failing regression](../evidence/native-stage9-ui-codeql-successor-2026-11-04/discovery-origin-before.log)
is retained alongside the passing browser result; the temporary lookup observer
is restored after the check. The local origin guard must be preserved when the
upstream MSW worker is regenerated.

The successor passes 97 UI/client-wire regressions, 33 Chromium story cases,
TypeScript analysis, full lint and a production build. It introduces no unused
warnings; the 62 existing lint warnings remain recorded. A fresh real
PostgreSQL/ProcessWorker deployment processes eight documents, and both the
default-output and explicit-output browser probes read its packaged UI without
MSW or mocked responses. The
[actual HTTP asset comparison](../evidence/native-stage9-ui-codeql-successor-2026-11-04/live-bundle.json)
includes the served `mockServiceWorker.js` bytes and proves delivery of the
origin guard in the current package.

These local checks do not stand in for hosted CodeQL. A new hosted analysis
after publication remains required; the findings have not been dismissed or
suppressed. Its result belongs to the common epoch's hosted successor record.
