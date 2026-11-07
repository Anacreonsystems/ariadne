# One unchanged existing-test PG17 baseline

Root authorized one execution only: the exact CLI predecessor followed by
all six existing `TestWorkerLogging` methods, in explicit order, using
`-n0 -q -o addopts=`. Source HEAD is d906, logging test SHA is f52c13f7…,
and the candidate patch is not applied.

The private selector is read in-process only; no values are printed or copied
to evidence. A fresh unique database is created on the allowlisted local
PG17 service. The external `pytest_fixture_setup(tryfirst=True)` hook replaces
only destructive database provisioning, after checking exact fixture identity,
module/path/scope, worker master, cache key, and precreated database URI.
Its invocation must be exactly one. A session profile guard makes any call
to the original provisioning body fail before that body executes. Original
fixture source SHA and override SHA are recorded. Real migrations, hosted API,
clients, sessions, workers, handlers, and existing test bodies remain intact.

The diagnostic plugin seeds only safe owned home/profiles/plugin/analytics/
ephemeral TOML settings in the CLI test's owned tmp_path before CALL. It logs
only node/time and sanitized logger/handler/gating state. The three real DEBUG
source lines receive state snapshots without replacing the log methods.

The launcher preserves runtime proxy/CA context, prefixes the shared venv in
PATH, uses uv without sync/network dependency resolution, directs Python cache
and pytest temporary files outside the repository, and captures import byte
hashes plus PID/start identities. The whole pytest process budget is 120s;
the controller outer deadline is 180s. No retry, force/drop, or source patch is
performed. The owned database remains after a zero-session check.

Authorized one-shot command:

```sh
uv --directory /workspace/ariadne run --no-sync --offline python -B /tmp/worker-logging-ci-diagnosis-d906-v1/pg17-baseline-v1/runner.py
```

The launcher immediately prints trial and pytest PID/start identity, then a
closed-result summary. Raw captures are mode 0600. Only `pytest.log` and
`pytest.xml` are sanitized public artifacts; never print private captures.
An unexpected baseline pass is reported honestly and does not trigger another
execution or patch.
