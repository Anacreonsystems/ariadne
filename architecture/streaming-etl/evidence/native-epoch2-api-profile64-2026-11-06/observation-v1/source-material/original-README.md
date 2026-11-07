# Prepared API-only normal64 probe — NOT EXECUTED

This directory is an external diagnostic candidate, not repository production or
test source. Root requested preparation only while the Principal runs the two
native500 recovery cases. No Python test body, DB connection, API process,
worker, profile, dependency installation or hook has been executed here.

The governing proposal is
`/tmp/native-launch-parent-lock-principal-recovery2-v1/profiling-plan-v1.md`,
SHA256 `c665f48aaffd07d0605687f62d44063c2ce6179ca3259942e8a07fdf01e105ac`.
Principal agreed that a labelled Popen observer may delegate the unchanged
original call and write only allowlisted identity. A pre-enable manifest
missing/mismatch is INCONCLUSIVE without waiting or profiling another process.

Assets:

- `test_native_api_profile64.py` calls the existing original execution fixture
  generator and unchanged `_run_dag_lifecycle`. Sync, normal, broadcast,64;
  wait150/observe210/pytest300. All helper oracles, startup/stop budgets,
  infrastructure health, source/operator delays and lifecycle remain intact.
- `api_selector.py` accepts only the fixture's exact11 argv items: interpreter,
  `-m uvicorn --factory prefect.server.api.server:create_app --fd <fd> --header
  X-Native-Recovery-ID:<32hex> --log-level warning`.
- `sitecustomize_profile.py` rejects other original interpreter argv before any
  cProfile import. Selected API PID/startticks/header/fd/argv must match the
  external Popen manifest at `_serve` entry. The profile wraps only that API's
  `Server._serve`, preserves arguments/return/exceptions, and dumps in finally
  before installed uvicorn capture_signals restores handlers/re-raises SIGTERM.
  It installs no signal handlers, timeout changes, threads, atexit dependency or
  cache. Other API threads, DB execution, Worker/Flow/Task are not profiled.
- `profile_support.py` retains source-snapshot/fresh-DB/process helper functions
  from the Principal's source-bound independent runner, with provenance in the
  preparation bindings. `run_profile64.py` requires a NEW explicit Root hold
  before it reads the private selector or connects to PG17. Do not give it the
  launch/native hold as a substitute.

Execution remains gated until both Principal native cases STOP and Root gives a
separate profile GO. A new authorization document must contain exactly:

```
status: ROOT_AUTHORIZED_PROFILE64_SOURCE_HOLD
runtime_authorized_by: /root
principal_native_status: STOPPED
authorized_phases: [control, profile]
source_root: /tmp/ariadne-native-5000-diagnosis-v1
head: <actual held HEAD>
profile_plan_sha256: <the SHA above>
files: <fresh profile_support.source_snapshot() mapping after native STOP>
assets: <name:SHA256 for the five prepared .py files>
```

The current preparation binding is read-only review evidence, not authorization.
Root/Principal should review the exact wrapper and shim before issuing that
hold. No runtime validation, pytest collection or fake uvicorn run is claimed.
Only static AST parsing of these external source texts is performed.

After authorization, with PATH prefixed by `/workspace/ariadne/.venv/bin` and
`UV_PROJECT_ENVIRONMENT=/workspace/ariadne/.venv`, run via uv:

```
uv run --no-sync python <this-dir>/run_profile64.py /ABS/NEW_ROOT_HOLD.json control
uv run --no-sync python <this-dir>/run_profile64.py /ABS/NEW_ROOT_HOLD.json profile /ABS/CLOSED_CONTROL/receipt.json
```

Run serially, never while another runtime actor is active. The profile command
requires a successful CLOSED control on identical held source/assets with all
owned cleanup complete. Each creates a distinct fresh owned PG17 database and
its fixture `_tests_master` database. An exclusive external driver lock prevents
overlapping invocations of these two phases, but Root remains responsible for
the separate Principal/native actor stop condition.

The driver keeps source before/after, imported Prefect byte observations, exact
owned process PID/startticks,1Hz API /proc CPU/RSS/context-switch samples,
versions/cgroup/affinity, private raw logs/XML0600 and sanitized siblings, raw
pstats, atomic completion marker and sorted self/cumulative callers/callees.
The normal control has no `_serve` wrapper and only sampling coverage: its last
sample is not a complete CPU total at process exit. Profile completion adds
independent process RUSAGE CPU totals and process elapsed time from Linux boot
clock/startticks, plus separately measured `_serve` wall/CPU. CPU totals cover
all API threads; cProfile covers the selected main event-loop thread only.

The existing source-bound collector preserves available worker-health and
coordinator-retirement JSON. A separate whole-prefix health audit rejects
observed HTTP access/client5xx mentions, TimeoutError, ASGI unhandled errors,
ERROR log lines and normal-task run_count/epoch above1 in helper progress.
Typed409 is not5xx; missing health files remain missing/incomplete. A stored
Popen.returncode snapshot, read only after the fixture teardown, rejects an
unknown or abnormal/forced API exit even when a dump marker exists. Existing
fixture teardown owns process waits; the driver does not signal API/workers or
extend stop budgets. It checks exact identity closure and zero owned sessions,
and drops only validated successful trials' newly owned databases. Failed or
inconclusive fresh databases are retained for diagnosis, with zero-session
verification and explicit cleanup receipt; they require separate later cleanup.
Operational cleanup is not evidence of the platform release/terminal protocol.
Old soak/forensic databases are untouched. Private selector contents, DSNs,
auth and environment dumps are never copied into the evidence.

A missing or failed profile dump/completion marker, forced kill, ambiguous API
identity, source/import drift, missing cleanup or any functional assertion
failure remains RED/INCONCLUSIVE. Do not extend counts or limits to obtain a
profile.64 observations earn no readiness, throughput/SLO/5000 acceptance or
cause attribution for CI duration or b4 blockers. Async pstats counts include
coroutine resumptions; cumulative time is not direct SQL wait/HTTP latency and
function calls are not SQL query counts. Any optimization is separate future
evidence-backed work.128/async probes require Root selection after64.
