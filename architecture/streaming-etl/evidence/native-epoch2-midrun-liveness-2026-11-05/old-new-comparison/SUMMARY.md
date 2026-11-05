# Midrun comparison: d504 and 835

This read-only diagnostic compares exact byte snapshots of archived d504 logs and current 835 logs. It starts no workloads, issues no database queries, reads no private pytest logs and changes no repository files.

As a developer, I want to distinguish an existing renewal-lock timeout from the terminal cleanup defect and from a newly observed consumer-read failure.

The observation criteria are satisfied by the linked request-response counts, original timestamps, unchanged source-frame chains and separate progress snapshots. This diagnostic does not accept an ADR or a runtime gate.

| Source and mode | Renewal HTTP 500 responses | Renewal wall-clock window | Other HTTP 500 responses | Maximum Task run counts |
| --- | ---: | --- | --- | --- |
| d504 sync | 16 | 16:30:06–16:47:31 | recovery candidates: 1, at 17:09:07 | All 1 |
| d504 async | 7 | 16:27:09–16:47:44 | replacement retirement: 3; DAG worker poll: 1; recovery candidates: 1, from 17:08:14 | All 1 |
| 835 sync snapshot | 32 | 17:59:15–18:28:31 | ordinary stream worker poll: 1, at 18:18:36 | All 1 |
| 835 async snapshot | 36 | 17:58:53–18:28:39 | DAG consumer read: 1, at 18:08:27 | C-0: 2; others: 1 |

The first renewal timeout in each of the four API logs follows the same project stack, including source line numbers: API `stream_workers.py:100`, model `stream_workers.py:450`, `stream_dag_launches.py:926`, `lock_worker_subtask` at line 93, and `lock_dag_graph` in `stream_dag_executions.py:205`. All four end with `TimeoutError`. The renewal problem therefore predates the four cleanup indexes. A matching stack does not identify the historical blocking transaction.

The old bodies reached source 5000 and all three consumer cursors 5000; every Task's maximum run count remained 1. The first terminal-finalization trace occurs only at 17:09:07 for sync and 17:08:14 for async, well after the early renewal errors. Their later cleanup failure remains a separate retained RED result.

The new snapshots have source 5000 but consumers around 2600–2700. Neither shows all consumers at 5000 or a terminal-finalization trace. Their errors cannot be described as a repeated terminal-GC timeout. Async's consumer-read failure follows `read_stream_dag_consumer:1777` → `read_consumer:787` → `lock_partitions:394`. Its first observed C-0 run count 2 occurs 612.676 seconds after the first progress snapshot, at source 1522 with consumer cursors B=767, C-0=763 and C-1=766; physical epoch remains 1.

The API traceback class-line totals are d504 sync 72, d504 async 48, 835 sync 134 and 835 async 151 `TimeoutError` occurrences. Framework wrappers repeat exceptions, so these totals are not request counts. Worker `ReadError` trace lines occur in both generations. Old worker logs additionally contain one `PrefectHTTPStatusError` and one `StreamRequestError` in each body's fatal recovery-poll sequence. The new bodies have not reached that sequence; its absence does not prove finalization or recovery acceptance.

The evidence supports investigating the existing graph/partition locking path and the new read failure. It does not establish that the indexes caused a regression, prove a particular blocker or rule out every effect of the successor changes.

The complete route counts, original monotonic progress readings, redacted snippets and per-input hashes are in [report.json](report.json). The source-frame extracts are in [trace-context.json](trace-context.json). New files may have appended after these snapshots; each result is bound to the bytes actually read. Wall-clock logs and monotonic progress windows are deliberately retained as separate clocks.
