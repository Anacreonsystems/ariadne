# Closed JOIN rollback for current integration — 2026-11-06

Root rejected the bounded reference JOIN optimization for current integration
because the closed JOIN500 pair did not demonstrate benefit. The owner restored
exactly two files from immutable original captures: model
[a4acc](source/stream_routes.before.py.log) and route test
[f7ef](source/test_stream_routes.before.py.log). P2 cleanup and TaskRun projection
remain. This is not rejection of ADR43 or its approved contract. All JOIN
candidate code/tests, PG68, functional PASS, slower pair, counters and py-spy
profile evidence remain historical on source1601a0f2 and must not be relabeled
as tests of rollback source1601**012a2d9d**.

The exact [owner apply receipt](owner/rollback-receipt.json) was written at the
`HOOKS_PENDING` stage; that historical status is preserved. The later
[final selected hooks receipt](owner/hooks-receipt.json) records actual normal
pre-commit0/pre-push0, no byte/AST mutation, hook children reaped, restored source
1601 digest012a2d9d446ab42ab737e68ba5dfd503adca6a395f79b02e201d767c67c30e7c,
publication636 unchanged and PRIMARY clean18124. These hooks are the author's
closed execution; the archivist did not run hooks, code, DB or collection.

This packet copies exactly the 11-file owner public allowlist plus the allowlist
itself, with [source→copy hashes](provenance.json). All copied bytes remain
unchanged; code assets use `.py.log` filenames. No live/current checkout or
future PG62 trial was read. At this closure, restored-source PG62 is only
preparation/**NOT EXECUTED** and CI-runner DAG500 diagnostics are source-planning/
**NOT RUN**. No post-rollback functional/performance PASS is inferred.

The old published18124 CI remains terminal17nativePASS/2DAG5000FAIL, not green
and not rollback-source CI. NI2-012…014 OPEN/BLOCKED, NI2-020 implementation
blocked, earned readiness15%, planned27% unearned. Existing approved ADR42–48
bodies/AC/history and all old evidence indices remain immutable. No whole-ADR
acceptance/rejection, resource-cause finding or new readiness credit follows.
Private logs/config/env/selectors/credentials/unlisted files are excluded;
conflict scan incomplete and grill-me/spec-first variants excluded.
