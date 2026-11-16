# Worker logging probe v2 — prepared, not executed

Source binding: `/workspace/ariadne`, HEAD
`d906ceca414baeac825ce472e0b78cd1dcae3363`.
The only current primary worktree change is the approved ADR-0044 planning
document; source and tests are unchanged.

The v1 assets remain immutable. `probe-v2.py` differs from `probe.py` only in
child-process environment handling: it preserves inherited proxy/CA settings
before clearing settings and restores that whitelist before adding the
controlled local Prefect configuration. The whitelist is `HTTP_PROXY`,
`HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`, their lowercase variants,
`SSL_CERT_FILE`, `SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, and `CURL_CA_BUNDLE`.
Values are never logged, serialized, or included in the hash bindings.
No inherited endpoint, profile, API key, database setting, or other
`PREFECT_*` value is retained. No HOME/CODEX_HOME setting is introduced.

The 30-second deadline, explicit execution gate, source check, 16 cases,
socket/SQLite audit deny, exact CLI predecessor function body, and proposed
test-only patch are unchanged. The v1 README describes the diagnostic scope
and limitations; historical gw0 execution order remains unproven.

No runtime test/probe, Prefect/pytest import, database, dependency, or hook
was executed while preparing v2. Root GO after the active 5k runtime STOP is
still required. Do not apply the candidate patch before actual probe evidence.

Exact one-shot invocation, only after that GO:

```sh
uv --directory /workspace/ariadne run --no-sync --offline python -I /tmp/worker-logging-ci-diagnosis-d906-v1/probe-v2.py --execute-after-root-go --repo /workspace/ariadne --out /tmp/worker-logging-ci-diagnosis-d906-v1/probe-v2-run-01
```

`probe-v2-run-01` must not already exist. Output is sanitized `result.json`
inside that new directory. The command does not authorize follow-on runs or
source changes. `brief-diff-v2.patch`, `bindings-v2.json`, and
`assets-v2.sha256` bind this prepared revision and its unchanged v1 inputs.
