# Rejected fixed-P2 transaction-proof optimization

Status: `Rejected`. Native credit: **0**. **No port; no further iteration.**

This archive records an isolated attempt to avoid repeatedly acquiring the same
physical authority prefix inside one NativeStreamStore operation. The
[ETL Architect / HighLoad Principal](receipt.json) rejected the corrected attempt:
555 added production lines reduced SQL count but increased observed CPU and wall
time. Product code, tests, configuration and the body/status of
[ADR-0043](../../adr/ADR-0043-physical-graph-and-partitioned-channels.md) were not
changed by publication. The originating
[CPU diagnosis](../native-p2-progress-observation-2026-11-05/cpu-diagnosis-v1/index.json)
remains separate evidence.

| One local 500-document run | Baseline | Corrected candidate |
| --- | ---: | ---: |
| API SQL statements | 72,149 | 46,684 |
| API CPU seconds | 72.676 | 97.798 |
| API wall seconds | 107.138 | 128.127 |
| Full pytest seconds, XML | 119.129 | 137.200 |

The corrected source passed 55 real PostgreSQL controls and real 8/500-document
runs, including four terminal Tasks, EOF and physical-slot/Flow-lease release.
This did not meet the performance objective. Shared load was uncontrolled; these
observations establish neither a causal SLO nor 5000-document/CI acceptance.
The earlier unsafe candidate's apparent speedup is not evidence for the final
source.

Two predecessor holes are retained: raw `END`/`ABORT`/keyword-comment transaction
control released PostgreSQL locks while SQLAlchemy objects looked active; and a
boundary during the fresh SELECT await could be followed by proof reconstruction.
The corrected successor closed the named controls, but its performance regression
caused rejection. No optimization code was ported.

As a developer, I want rejected performance experiments to remain reviewable
without confusing their functional controls with production acceptance.
Acceptance criteria: as a developer, I can verify the 125-entry original ledger
and all published payload hashes, resolve the 14 redacted log counterparts,
reproduce the decision and confirm that
the attempt earns zero native credit.

- [Decision and bounded results](receipt.json).
- [ZIP archive](evidence.zip): 125 published payloads plus the byte-identical
  original index; 111 payloads remain byte-identical, 14 logs have only database
  URL authentication removed. Original private logs remain outside the repository.
- [Immutable original index](original-index.json) and
  [publication mapping](publication-map.json) bind original and published hashes.
- [Redaction receipt](redaction-receipt.json) records 42 credential redactions.
- [Publication index](index.json) binds every file in this directory.

Inside the ZIP, `successor-v2/final-receipt.json`, `successor-v2/comparison.json`,
`successor-v2/corrected-full-model.xml`, `successor-v2/source-before.json` and
`successor-v2/source-after.json` identify the rejected final source. The source,
commands, initial failures and both security REDs are preserved. The cleanup
receipt proves zero owned test databases/sessions, five gone exact API identities,
30 absent worker/Task PID observations and five closed API ports. Existing
unrelated services and processes were left alone.
