# Selected seven-test acceptance v3

Root authorized one new actor: the exact CLI predecessor followed by the six
existing `TestWorkerLogging` methods, with candidate source SHA b8fb7f17… .
No further source edits, stage, commit, hook, retry, full-module expansion,
or native ETL phase are authorized.

The previous 151-test attempt remains abandoned and INCONCLUSIVE. Its dead,
unreaped PID384983/start14778279 under PID1 remains an operational artifact.
This runner never signals, adopts, or tries to reap that prior child. It does
not change v2 closure claims or the native 5k PID oracle.

Before fresh database creation or Popen, this runner sets PR_SET_CHILD_SUBREAPER
(36,1) on itself and verifies PR_GET_CHILD_SUBREAPER (37). Failure stops before
those resources. Runner/parent PID-start identities, successful verification,
and the runner's source SHA are bound in subreaper-proof.json. This affects
only its future descendants. The direct Popen child is waited first; any new
owned adopted descendants are validated by parent, group, and birth/start
identity before waitpid(WNOHANG). Actual reaps and remaining children are saved.

The seven tests retain real repository bodies, migrations, API, clients, and
sessions. Only fresh database provisioning is overridden with the existing
exact fixture/module/scope/master/cache-key/PG17 URI allowlist checks, original
fixture SHA binding, and original-body guard. Hook count must be one and
original-body call count zero.

The profile guard now checks event, function name, then a precomputed filename;
it allocates no Path per function call. sys.settrace is enabled only during
the three worker API methods. CLI and other tests have before/after state
capture. Emission lines are AST-derived and source-bound. Collection must be
exactly seven nodes in the stated order.

Source snapshots cover git-tracked files plus all src/prefect/**/*.py, including
the two generated paths, before and after. The fresh owned PG17 database is
retained on every outcome after an owned-session check; no force/drop is used.
Proxy/CA context is preserved, HOME/CODEX_HOME are untouched, and no inherited
PREFECT endpoint/profile/credential values enter the child.

Limits are 120s per test and 180s for the whole actor. At the 140s process guard,
the exact owned pytest Python PID/start is selected from in-process owned argv
and gets SIGINT first, allowing up to 20s for pytest teardown/sessionfinish.
Only a validated own group may then receive bounded TERM/KILL. All signals,
direct wait, adopted child reaps, and closure state are recorded.

Authorized one-shot command:

```sh
uv --directory /workspace/ariadne run --no-sync --offline python -B /tmp/worker-logging-ci-diagnosis-d906-v1/pg17-selected-acceptance-v3/runner.py
```

Private selector, argv, environment, and raw private captures are never printed.
Public evidence consists of sanitized pytest.log/pytest.xml, metadata, state
captures, source/import hashes, provisioning proof, reaping proof, and receipt.
