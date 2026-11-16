# Worker logging CI diagnosis — prepared, not executed

Bound source: `/workspace/ariadne`, revision
`d906ceca414baeac825ce472e0b78cd1dcae3363`.
Observed failure: run `42134420475`, job `127620964363`, canonical group
12/18, Python 3.12/Postgres 14, on `gw0`:
`tests/test_logging.py::TestWorkerLogging::test_worker_logger_sends_log_to_api_worker_when_connected_to_cloud`.
The filtered mocked `APILogWorker.instance().send` calls were empty.

No tests, probe execution, Prefect import, database, dependency, hook, or
repository mutation was performed for this preparation. Execute only after
the root actor confirms the runtime STOP and grants GO. These assets do not
grant that GO.

## Source evidence

- `tests/test_logging.py:1063`: `worker_handler` yields a new handler but does
  not attach it. The failing test requests this fixture through `usefixtures`
  and emits `DEBUG` through `worker._logger`, relying on the session logging
  graph rather than the class's separate `logger` fixture.
- `src/prefect/logging/logging.yml`: `prefect.workers` inherits its level from
  `prefect`, and owns the configured `WorkerAPILogHandler`.
- `src/prefect/logging/handlers.py:299`: the handler requires both a worker ID
  and `determine_server_type() == CLOUD`; `APILogHandler.emit` then checks the
  API logging setting and `send_to_api`, prepares the record, and calls
  `emit_api_log` synchronously. A configured process sink substitutes for the
  shipping worker. Sleeps and flushes do not solve pre-emit suppression.
- `src/prefect/client/base.py:819`: Cloud recognition is a prefix test against
  the configured Cloud API base. A custom base is a separate candidate; it
  must be captured as a Boolean/enum, without exporting any URL.
- `tests/fixtures/logging.py`: autouse reset drains/resets shipping workers;
  it does not restore logger levels, disabled flags, handlers, or the sink.
- `tests/test_logging.py:310`: the alias settings test really sets the global
  `prefect` logger level to INFO without restoring it. However, that node is
  absent from the clean group-12 duration artifact, so it is not an evidenced
  predecessor for this CI failure.
- A relevant node IS in that artifact:
  `tests/cli/test_config.py::test_view_prefers_canonical_dot_env_value_over_alias`.
  It removes `PREFECT_*` environment values, then uses an empty profile with
  `include_current_context=False`, and invokes the CLI. This obtains default
  INFO logging and `testing.test_mode=False`. `_setup_and_run` in
  `src/prefect/cli/_app.py:83` calls real `setup_logging()` in that mode.
  `temporary_settings`/`use_profile` restore settings context, not the process
  logging graph. This is a concrete order-dependent candidate, not a proven
  attribution to the original gw0 failure.
- The original log has dots and the failing gw0 identity, but no per-worker
  node order or effective logger state. The duration artifact establishes
  membership, not execution order.
- The source has only two test-local sink mutations in `TestAPILogHandler`;
  the override uses `finally: set_api_log_sink(None)`. Neither sink test is in
  the clean group-12 artifact. Process-pool initializer sets a sink inside
  subprocess workers; do not infer a parent-process leak without evidence.

## Bounded reproduction

1. Run the prepared DB-free probe once in an isolated process. It invokes
   the unchanged CLI predecessor function body extracted from its source AST,
   with real MonkeyPatch/CLI helper and an owned tmp_path. An owned TOML only
   supplies safe home/profiles paths and disabled plugin/analytics settings;
   it leaves the predecessor's logging and test-mode defaults intact. The
   probe uses real
   logger factories and real `WorkerAPILogHandler`, replacing only the
   shipping transport. It checks baseline Cloud shipping, CLI INFO
   reconfiguration, scoped leaf DEBUG, its restoration, disabled/logger/handler suppression, sink
   routing, API toggle, missing ID, and self-hosted workers with durable IDs.
   It disables plugins and telemetry, blocks socket connections and SQLite
   connections, and writes only sanitized state and controlled marker data.
2. If the probe shows the expected pre-emit mechanism, run the exact existing
   predecessor and target, in that explicit order with `-n0`, on a NEW owned
   database/environment approved by the root actor. Use an external pytest
   plugin to capture sanitized per-node logger state. Do not use any old
   soak/forensic database. The exact predecessor deletes the testing env
   switches; an isolated disposable home/container is required to avoid
   fallback reads of a real user's default profile.
3. Compare a fresh target-only pass with the pair's target failure; then
   reverse the pair. This attributes a controlled failure to the action.
   The historical CI gw0 ordering remains unproven unless a retained worker
   trace supplies it.

Prepared probe invocation (not yet authorized or executed):

```sh
uv --directory /workspace/ariadne run --no-sync --offline python -I /tmp/worker-logging-ci-diagnosis-d906-v1/probe.py --execute-after-root-go --repo /workspace/ariadne --out /tmp/worker-logging-ci-diagnosis-d906-v1/probe-run-01
```

The script clears the child process environment before importing Prefect and
uses explicit local settings. It checks the source revision and records
source hashes. Execution has a 30-second process deadline.

## Minimal fix candidates, contingent on evidence

1. If exact CLI predecessor INFO suppresses DEBUG and scoped leaf DEBUG
   restores one send, use `caplog.at_level(DEBUG, logger=worker._logger.name)`
   around all three worker log emissions. This makes the intended emission
   explicit and restores the previous level/cache. Add matching captured
   record assertions in both negative cases so they cannot pass vacuously.
   The external `candidate-scoped-debug.patch` prepares this narrow change.
2. Restore the CLI predecessor's logging graph only if further evidence
   warrants changing that test's responsibility. If a missing/disabled
   handler is observed, make the worker fixture own its intended handler
   and restore prior state. Do not preemptively add that complexity. Preserve
   the CLI's legitimate production logging setup and INFO default.
3. If evidence instead identifies the Cloud-base gate or sink routing, fix
   that controlled test context or its leaking owner. Do not remove the
   production Cloud gate or infer Cloud support from a native worker ID.

Regression coverage must include Cloud plus worker ID (one ship), Cloud
without ID (zero ship), self-hosted without ID (zero ship), and self-hosted
with a native enrolled worker ID (console marker retained, zero ship).
For the last case, add an actual console/caplog assertion to the existing
native-ID regression, which currently asserts only zero shipping.
