# SYNC5000 metadata observation — prepared, not executed

Source18124603882fe9196a6eec6063a4619e7087f48f. Only outside-repository
assets are prepared. No project/pytest/asyncpg imports, database connection,
probe, hook or live/final query has run during preparation.

The exact18 prior metadata SELECT families are reused byte-for-byte. If no
unique Flow UUID is yet present in the owned worker View-at log, one additional
SELECT discovers Flow only WHERE id equals the already captured Graph UUID,
inside the same read-only repeatable-read transaction. Graph-to-Flow and all4
TaskRun IDs must then match the owned progress. An ambiguous worker Flow ID,
wrong trial/command/hash/database/source/PID identity or unsupported scope fails
closed. Never query old soak/forensic DBs or use an unscoped Flow discovery.

The driver uses ONE fresh actual database, never a derived _tests_master DB.
Root must provide the exact trial path, command hash, actual database and PID
start identity in a fresh scope JSON within this directory. Its preparation
hash binds these frozen assets and18124 ORM/core bytes. The private selector
is read programmatically only after validation; DSN/environment/credentials,
business payloads and capability fields are never serialized.

Live start requires statusROOT_AUTHORIZED_LIVE_READONLY_SYNC5000 and
authorized_by/root plus explicit --execute-root-go. controller.py waits for
owned graph progress, captures immediately, then at least65seconds apart.
Transactions have default/read-only on, PG17 validation, repeatable-read,
2s command/statement timeout,100ms lock timeout,3s idle transaction timeout,
6s whole capture envelope and immediate close. No explicit row locks, DDL/DML,
backend cancellation/termination, app actor, subprocess or signals are used.
Only ordinary PostgreSQL relation AccessShare from SELECT is implicit.

The controller checks artifacts/PID every2seconds and stops before terminal at
all3cursors>=4900, all-cursors-observed, pytest exit/gone/change, Root stop marker
or first inconclusive capture. Capture checks the guard before and immediately
after connection. Each session closes before success output. Record controller
PID/startticks and STOP; external parent waits exit and seals reap proof without
using /proc/task/children or signaling PID1. Starting before500 is an aim;
only actual observed positions and activation state earn an observation label.

Postterminal capture is a different entry point and Root scope status:
ROOT_AUTHORIZED_POSTTERMINAL_READONLY_SYNC5000. It requires prior live STOP,
the exact closed native driver STOP receipt with retained DB/zero sessions,
and the bound resolved Flow/Graph/task identity. It performs ONE18-family
snapshot and closes immediately. It is never scheduled by the live controller.
Actual physical/tag allocation released flags or absence, counters, graph and
lease pointers must be reported as observed, including failures. Persisted
Flow lease pointer/counters do not inspect the external KV lease backend.

Prepared live command, only after exact Root runtime/observer GO:

    uv --directory /workspace/ariadne run --no-sync --offline python -B /tmp/native-sync5000-metadata-observer-18124-v1/controller.py --scope /tmp/native-sync5000-metadata-observer-18124-v1/ROOT_AUTHORIZED_LIVE_SCOPE.json --execute-root-go

Prepared final command, only after a later separate Root final-capture GO:

    uv --directory /workspace/ariadne run --no-sync --offline python -B /tmp/native-sync5000-metadata-observer-18124-v1/capture_final.py --scope /tmp/native-sync5000-metadata-observer-18124-v1/ROOT_AUTHORIZED_FINAL_SCOPE.json --execute-root-go

Retain failure/inconclusive observations. No retry, database drop, signal,
source/test/timeout change, old async-index edit, A43/NI014 readiness credit,
throughput/SLO claim or architecture approval follows from preparation.
All grill-me/spec-first variants are excluded; conflict scanning is incomplete.
