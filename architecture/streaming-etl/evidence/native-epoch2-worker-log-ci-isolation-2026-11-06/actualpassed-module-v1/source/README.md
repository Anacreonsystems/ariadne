# Final full logging-module acceptance v4

Root authorized one new full-module attempt: exact CLI predecessor first,
then all 150 existing logging cases, exactly 151 collected tests. Source HEAD
is d906 and the sole authorized test change remains b8fb7f17… . No production
code, hooks, stage, commit, publication, or native ETL phase is changed.

Earlier attempts remain immutable and qualified: v2 full module was abandoned
at its diagnostic deadline, and v3 had seven passing bodies followed by a
metadata exception and unrecorded pytest exit. No old receipt is promoted.
The prior dead PID1 orphan remains an unresolved operational artifact.

This runner binds all git files plus every src/prefect Python source, including
generated paths. The v3 optimized callback checks event/function-name before
precomputed filename; trace is limited to three AST-bound worker API methods.
The CLI safe owned TOML, real test bodies/migrations/API/client/session fixtures,
and exact provisioning-only hook with invocation1/originalbody0/stable-callback
proof remain unchanged.

Before DB/Popen, the runner sets and verifies its own future-child subreaper and
requires waitpid(-1,WNOHANG) to report ECHILD. Its old PID1 orphan cannot be its
child. After direct Popen.wait, the ACTUAL exit is persisted immediately to
pytest-exit before metadata/reaping. Adopted future children are reaped with
waitpid(-1,WNOHANG), bounded alive-child polling, and ECHILD success. No proc
task children interface is used. Direct wait, actual reaps, and PID-start
closure are recorded separately.

try/finally captures source, import, JUnit, PID, DB, and provisioning evidence
independently if any metadata helper fails. A genuine pytest exit remains
separate from harness errors. The fresh unique PG17 DB is retained on every
outcome, with an owned zero-session query and no drop/force/old-DB operation.
The private selector is read only in memory, never printed or copied. Runtime
proxy/CA and HOME/CODEX_HOME are preserved; inherited Prefect secrets/settings
are replaced by controlled local child settings. Caches and all trial output
stay outside the repository.

Budgets are unchanged: per test 120s, whole pytest process guard 330s, outer
360s. Timeout starts with exact owned pytest Python PID/start SIGINT and grace,
then bounded validated-own-group TERM/KILL if needed. No retries, skips, xfails,
assertion weakening, or another phase follow a failure.

Authorized one-shot command:

```sh
uv --directory /workspace/ariadne run --no-sync --offline python -B /tmp/worker-logging-ci-diagnosis-d906-v1/pg17-module-acceptance-v4/runner.py
```

Only sanitized pytest.log and pytest.xml are public captures. Never print the
private logs/XML, selector, argv values, or environment. Receipt and result
hashes accompany source/import/ownership/provisioning/reaping metadata.
