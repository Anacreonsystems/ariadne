# Existing native DAG5000 gates — PREPARED, NOT EXECUTED

Root is publishing final `d906ceca414baeac825ce472e0b78cd1dcae3363`.
This directory prepares only the two existing DAG5000 selectors. It does not
run or authorize either phase. P2 gates and their different budgets are outside
this runner. Known grill-me/spec-first variants remain excluded; the conflict
scan remains incomplete. This continues approved ADR42–48, not a new ADR.

First phase, only after a separate Root GO and a new authorized hold:

```
PATH=/workspace/ariadne/.venv/bin:$PATH UV_PROJECT_ENVIRONMENT=/workspace/ariadne/.venv uv run --no-sync python /tmp/native-n2-2-volume5000-preparation-recovery2-v1/native_5000_runner_v1.py /ABS/NEW_ROOT_ASYNC_HOLD.json async5000
```

The driver executes exactly this existing node, once:

```
uv run --no-sync python -m pytest 'tests/test_native_stream_dag_execution_http.py::test_public_dag_bounded_volume_postgres[True-5000]' -q -o addopts= --junitxml=<fresh-trial>/pytest.private.xml --basetemp=<fresh-trial>/pytest
```

It uses the unmodified test/helper `f1fc5195...` and original
wait3960/observe4080/pytest4200. It changes no production/factory/test/budget,
signal handler, worker stop behavior or data-plane cache, and imports no
cProfile. There is no wrapper test and no substitute workload.

After the first actor STOP and another explicit Root GO, a separate invocation
may select `sync5000`, executing the same function's `[False-5000]` node. The
sync hold must contain `sync_separate_root_go:true` and the exact closed async
receipt path/hash; it checks that the async actor has no same-identity live or
zombie processes. The driver never starts a second phase automatically. A
filesystem lock prevents overlapping invocations of this runner; Root still
coordinates all other runtime actors and hooks.

`PREPARED_SOURCE_HOLD.json` records the current1600-file source union and five
external assets with a non-authorizing status. A new post-hooks runtime hold
must use `ROOT_AUTHORIZED_NATIVE_5000_SOURCE_HOLD`, `runtime_authorized_by:/root`,
`publication_hooks_status:STOPPED_PASS`, final HEAD/current byte mappings and
only the phase explicitly selected by Root in `authorized_phases`. The pending
hold cannot pass the runtime gate. Source and external asset mismatch stops
before selector reading or DB creation.

The support functions and startup import audit reuse the personally passed
Principal native500 approach. The external observer is derived from its v4
artifact reader, with all normal subtasks limited to attempt1 rather than
recovery's B allowance2. It opens no DB and controls no process. During an
authorized run use:

```
uv run --no-sync python /tmp/native-n2-2-volume5000-preparation-recovery2-v1/observe_native_5000_v1.py /ABS/FRESH_TRIAL
```

The controller itself records `/tmp` and `/workspace` disk observations every
10s while the one pytest child runs. This passive observation does not kill
processes, grow budgets or change the existing lifecycle. It records source
before/after, imported Prefect code/source hashes and exact PID/startticks,
process closure, actual command and private selector path. It never copies
private config values or environment and uses uv/shared venv only.

Each phase creates one unique owned PG17 base DB; pytest owns its generated
`_tests_master` DB. The read-only post-run audit reconciles exact5000 source,
branch calls, unique effects and normal physical metadata, original whole-case
test result and broader5xx/timeout/ASGI/worker-error/healthy-retry observations.
Only a successful source/body/health/import/process validation permits drop,
and only after an actual zero-session check on those two newly owned names.
Failures/inconclusive runs retain their fresh databases and raw artifacts for
diagnosis; live sessions prevent drop and are recorded, never force-terminated.
Old soak/forensic databases are untouched. Operational DB deletion is separate
from the unchanged product terminal/release proof.

All real attempts, including failure, get distinct trial directories. Raw
log/XML are mode0600; public siblings redact DSNs. Do not publish raw API/worker
logs or selector/config/env. No automatic correction, retry or next-node launch
occurs after a failed body/health/harness check. STOP and report first, then let
Root choose the next action. A test pass is a functional gate, not throughput,
causal evidence or independent ADR acceptance; no readiness credit is added by
the implementation actor's receipt alone.
