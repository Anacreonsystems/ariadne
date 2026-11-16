# One candidate plus whole existing logging module on fresh PG17

Root authorized the exact candidate SHA 89e45f… applied only to
`tests/test_logging.py`. Before SHA f52c13f7…; after SHA
`b8fb7f17a015147cf4032e74065308027af3a6393cab55cc9653cff1ca02d246`.
The 24 additions and 6 deletions add scoped DEBUG at three emissions and
captured-record assertions for the two negative cases. Production behavior,
Cloud gating, default INFO, and exact one-send/worker-ID assertions stay intact.
No stage, commit, or push is performed.

One existing-test execution: the exact CLI predecessor first, then the entire
existing `tests/test_logging.py` module, with `-n0 -q -o addopts=` and a 120s
per-test limit. The controller outer limit is 360s; the pytest process guard
reserves shutdown/evidence time by stopping a still-running process at 330s.
No retry, skip, xfail, or assertion reduction is introduced.

The v1 baseline and database remain untouched, including its qualified missing
before-bindings for two generated files. This new driver hashes the union of
git-tracked test/source/config assets and **all** `src/prefect/**/*.py` before
and after execution, explicitly including those generated files. The plugin
derives the three real DEBUG emission lines from the patched AST and captures
sanitized state without replacing any logger/worker/test body. The selected
collection must start with the CLI predecessor, include all six worker cases,
and otherwise contain only the full logging module.

The ownership-only provisioning hook is retained: exact fixture identity,
session scope/master/cache key, and the newly created PG17 URI allowlist are
validated; original fixture SHA and override SHA are bound; hook invocations
must be one and original destructive body calls zero under a fail-closed
profile guard. Real migrations, API server, clients, sessions, and existing
test transport mocks are unchanged. The fresh owned database is retained on
both pass and failure after checking zero sessions; no drop/force/old database
access is allowed.

Authorized one-shot command:

```sh
uv --directory /workspace/ariadne run --no-sync --offline python -B /tmp/worker-logging-ci-diagnosis-d906-v1/pg17-candidate-module-v2/runner.py
```

The private selector is read only in process. Inherited Prefect settings are
removed in the child; runtime proxy/CA context and HOME/CODEX_HOME are retained
without inspection/output or reassignment. The shared venv prefixes PATH.
Caches, temporary files, imports/PID audit, sanitized JUnit/stdout, and receipts
are written only to a new trial directory here. No additional runtime phase is
authorized by this receipt.
