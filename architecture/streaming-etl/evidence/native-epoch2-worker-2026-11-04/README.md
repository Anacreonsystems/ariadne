# Assigned-worker component evidence

This archive preserves the worker successor tested on 2026-11-04. Its three
source hashes match the independent ETL Architect / HighLoad Principal receipt.
The complete worker regression passed 39 cases; the independent focused run
passed 14 cases. These are component checks and earn **0% native readiness**.
The complete frozen native matrix remains a separate gate.

- [Original implementation receipt](receipt.json): commands, results, exact
  source hashes, retained failures and evidence limits.
- [Relative artifact index](index.json): every relocated path, byte count and
  SHA-256. Raw Python snapshots and the raw patch use a `.log` suffix to preserve
  their original bytes under repository hooks.
- [Independent component receipt](native-epoch2-principal-worker-controls-receipt.json).
- [Raw source patch](source.patch.log).

The earlier failures remain evidence: an incomplete migration window, an import
observed during a source edit, initial HTTP fixture assumptions, and the
reproduced duplicate-supervisor initialization race. They are not overwritten
by later passing observations. `controls-03.xml` was never emitted because
collection failed; its raw log is retained.

This bundle supports [ADR-0043](../../adr/ADR-0043-physical-graph-and-partitioned-channels.md)
and the [epoch-2 task registry](../../todo.md). It does not accept the ADR or
close NI2-006. The [initial two-worker discovery](../native-epoch2-two-worker-discovery-2026-11-04/first-eight-documents/receipt.json)
is predecessor evidence; later native runs must bind their own complete source
union. The implementation and acceptance boundaries remain in the
[epoch document](../../epochs/epoch-02-native-streaming-completion.md).

## Metadata observation experiment

The [metadata cadence successor](metadata-cadence-successor/receipt.json) records
the independently approved one-variable metadata polling experiment and its
exact source delta. Its native A/B result is pending; the original 39/14-case
evidence above remains bound to the original source. DATA acceptance, receipt
retries, child input polling and lease/freshness deadlines are unchanged.
