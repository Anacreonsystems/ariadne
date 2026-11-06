Bounded API profiling plan — proposed, not executed

Purpose: investigate where time is spent in the observed native functional run. This is an instrumented functional probe, not throughput/SLO/5000 acceptance, a 5000 forecast, or evidence of the cause of previous failures. No source optimization, prefix cache, timeout increase, query-plan change or production setting change is proposed.

Evidence boundary

The current7d99 CI sync500 observation has ready-to-terminal673.126090185s and B first-to-last638.52954556s; async500 has602.635967378s and573.873618224s. Those are same-host monotonic functional timings. Artifact resource snapshots show4CPU/8GiB and installed dependencies, but do not establish the cause of duration. The full dynamic API argv was not captured in those CI artifacts; only source argv template and runtime API PID/socket observations are retained. This diagnostic will capture its own actual allowlisted process identity.

Execution prerequisite and priority

Do not execute this plan until the launch50 Principal actor has explicitly STOPPED and Root gives a separate GO for the profiling probe. All runtime actors remain serial on this4CPU/16GiB environment. Read-only plan preparation may overlap; profile bodies, API processes and database connections may not. Bind the final source union and external diagnostic assets before each actual trial; all four held paths and approved ADR bodies/AC remain unchanged.

Smallest useful sequence

1. Run one normal synchronous64-document broadcast graph through an external test wrapper calling the unchanged `_run_dag_lifecycle` helper and its ordinary SDK/deployment/ProcessWorker/API path. Keep its original short helper limits: wait_timeout150s, observation_timeout210s, pytest timeout300s. Keep every whole-case data/effect/signature, metadata-count, retry, terminal/coordinator and infrastructure-health assertion. No direct SQL state installation or altered source/operator delays.
2. Run the identical64 workload with API-only cProfile. This is a fresh owned PG17 database and fresh workers/server, serial after the control. Same source, limits, payloads and assertions. The control allows the instrumented observation to be interpreted with an explicit overhead qualification; neither result is a performance acceptance or controlled CI4CPU/8GiB comparison.
3. Stop and inspect the retained evidence.128 documents and/or asynchronous mode are optional later probes, requiring Root's selection after64. Never grow count or timeouts automatically to obtain a profile. Any timeout is honestly retained as functional RED/profile inconclusive; the unchanged existing assertion is not weakened.

Narrow profiling surface

An external sitecustomize, isolated to the diagnostic PYTHONPATH, first inspects only sys.orig_argv. Activate profiling only for an exact `-m uvicorn` invocation with `--factory prefect.server.api.server:create_app`, the inherited `--fd`, and the fixture's X-Native-Recovery-ID header. Require the owned server PID/startticks and header identity to match the wrapper's manifest. No worker/Flow/Task/pytest process is profiled. Nonmatching processes perform no cProfile import, thread creation, signal handling or profiling. Retain actual selected PID/startticks/executable/cwd and allowlisted argv; never dump environment/auth/DSN.

cProfile covers the selected API process's main event-loop thread, including startup, routes and ordinary background work on that thread. It does not cover PostgreSQL server execution, worker/client CPU, other API threads, or the complete distributed wait path. Use standard cProfile elapsed timer and retain raw pstats; summarize self time, cumulative time, callers/callees and exact function counts with explicit coroutine-resumption qualification. An async cumulative function time is not a direct SQL-wait or HTTP-latency measurement; profiler call counts are not SQL query counts. Independently retain process wall/CPU totals so event-loop polling is not reported as CPU work.

Reliable dump without changing signals

Atexit alone is insufficient here: installed uvicorn0.52.0 Server.capture_signals restores handlers and re-raises captured SIGTERM after graceful shutdown (server.py323–340). The fixture normally terminates its API child and waits10s before its existing kill fallback. Do not replace signal handlers or modify that budget.

In this explicitly instrumented trial only, the external shim wraps `uvicorn.Server._serve` with an async try/finally: enable the profile before awaiting the original coroutine; in finally disable and dump raw pstats plus an atomic completion marker before capture_signals re-raises SIGTERM. Preserve arguments, return value and exceptions. Atexit may be an additional fallback, never the sole dump guarantee. This is runtime instrumentation, not a production library edit. Dump failure, forced kill or missing completion marker means profile INCONCLUSIVE, even if business assertions pass. Do not infer normal API cleanup from the existence of a partial profile.

Lower-impact first option

If Root prefers no runtime method wrapper initially, use the same small normal64 control with a1Hz read-only `/proc/<exact-api-pid>` CPU/RSS/context-switch sampler and the helper's existing phases/call/source/progress logs. That can distinguish low versus sustained API CPU without modifying runtime functions. It cannot name expensive Python call paths. Add cProfile only when that next question remains useful; py-spy is not currently installed, and no dependency installation is proposed.

Artifacts and interpretation

Retain source before/after hashes, external wrapper/shim hashes, exact invocation, tool/dependency versions, effective cgroup/affinity observations, API PID/startticks, all existing functional artifacts, cProfile raw file plus dump-complete marker, sorted pstats reports and process wall/CPU/RSS samples. Preserve private raw logs/XML mode0600 and publish only sanitized siblings. Use the newly fixed collector's healthJSON entries where present; missing health observations stay missing rather than being treated as clean.

Each trial owns only its newly created PG17 base/test databases, API server and worker/process identities. Require zero source/import drift and exact process/session closure. Operational fresh-DB cleanup is not product terminal/release proof. Do not touch previous soak/forensic databases, reset statistics, install extensions, enable auto_explain, add PG query logging or alter caches.

Report the result as a bounded observation: which API call paths dominated this instrumented trial, how much API CPU/wall time was observed, and what remains unmeasured. No causal conclusion about b4 blockers or CI duration follows from these tables alone. Source optimization remains a later evidence-backed proposal.
