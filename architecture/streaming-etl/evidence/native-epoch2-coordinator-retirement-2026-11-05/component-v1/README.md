# Native DAG coordinator retirement: implementation evidence

A final native DAG Flow previously entered the bounded-linear/fixed-P2 release path. Baseline HTTP returned `409 EXECUTION_PROFILE_NOT_READY`; the actual native small run kept authority `TERMINAL`, snapshot `ACTIVE` and coordinator launch `PROCESS_REGISTERED`. Skipping that call would retain metadata; reusing legacy mutation could rewrite durable DAG grant proofs.

The candidate dispatches a DAG snapshot or graph marker to a dedicated terminal-proof validator, then releases only coordinator authority/snapshot/native-launch metadata. It returns zero newly released payload streams. Public RPC, legacy data guards, physical launch/retirement journals and platform request budgets remain unchanged.

| Implementation gate | Actual result | Fixture binding |
| --- | --- | --- |
| PostgreSQL model module | 45 passed, 1 SQLite case deselected; 230.80 s | `c171…` fixture predecessor in source lease |
| Relevant DAG API and legacy release | 28 passed, 1 SQLite case deselected; 33.32 s | predecessor source lease |
| Real TCP/PG/OS native profiles | 5 passed; 118.14 s | native fixture `c1713085…` |
| Real sync/async pre-seal failures | 2 passed; 35.74 s | fixture successor `f1fc5195…` |
| Strict native type check | 29 files, 0 errors, 0 warnings | unchanged final product hashes |

Both normal native modes and preview have genuine worker ERROR count zero. Failure/cancellation and pre-seal failure retain expected business errors; rejected post-terminal infrastructure errors are zero. Every native case observed coordinator authority, snapshot and current launch `RELEASED`. The two pre-seal cases additionally observed all 14 scoped graph/Task/submission/grant/stream/allocation/data/journal counts zero.

All failed draft, invalid fixture, mistaken selection and interrupted attempts are retained separately. An `exit 5` with zero collected tests is a harness failure, not a green gate. Original RED test source bytes were reconstructed deterministically from the baseline plus the retained edit; their hashes match both actual pre-run leases. That provenance is distinguished from runtime-captured source copies.

The old five executions remain bound to their original fixture. The independently reviewed AST bridge permits a fixture-only successor without relabelling those executions or repeating the PostgreSQL module solely for the fixture hash. The bridge review is not independent runtime acceptance.

This package reports implementation evidence only. Independent Principal runtime repetition is pending. There is no native5000, epoch/sprint acceptance, readiness or long-soak credit. It does not resolve the historical C0 read timeout or the separate b4 rebalance deadline-before-release finding.

Related evidence: [actual native1000 finding](../native1000-red-v1/index.json), [previous implementation report](../implementation-report-v1/index.json), [b4 rebalance finding](../../native-epoch2-ci-rebalance-runtime-2026-11-05/b4-red-v1/index.json). Architecture: [ADR-0042](../../../adr/ADR-0042-flink-informed-native-streaming-architecture.md), [ADR-0043](../../../adr/ADR-0043-physical-graph-and-partitioned-channels.md), [ADR-0048](../../../adr/ADR-0048-rescaling-compatibility-and-native-acceptance.md), [sprint board](../../../sprint_board.md), [todo](../../../todo.md).
