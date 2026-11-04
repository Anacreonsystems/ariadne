# Flink 2.3 architecture review for Ariadne native streaming

- Date: 2026-11-04
- Scope: architecture research and comparison, not runtime certification
- Decisions: [ADR-0042](../adr/ADR-0042-flink-informed-native-streaming-architecture.md)–[ADR-0048](../adr/ADR-0048-rescaling-compatibility-and-native-acceptance.md)
- Source inventory: [official references and response hashes](../evidence/flink-architecture-2026-11-04/official-sources.json)
- Discussion: [ETL Principal](../evidence/flink-architecture-2026-11-04/principal-discussion.json), [native worker review](../evidence/flink-architecture-2026-11-04/worker-architecture-review.json)

## Reference versions and method

Reviewed official Apache Flink release-2.3 documentation and seven implementation
files pinned to the dereferenced `release-2.3.0` commit
`c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`. The annotated tag object is
`f6d0471ac428242643463b05b6ae58ee77c9e603`. This is a bounded design review,
not an exhaustive source audit or a performance comparison with a running Flink
cluster.

Documentation on a release branch can change. The inventory records fetch
times, URLs, response sizes/statuses and raw-response hashes. Full third-party
HTML/source snapshots are not vendored. Initial navigation-only source/sink
redirect pages and one HTTP 404 were excluded from substantive evidence;
the final inventory uses the actual internals/source and sink pages. Earlier
`master` exploration in the discussion record is non-normative; the source
links below use the pinned release commit.

## Mechanisms and adaptation

| Mechanism in Flink | Verified behavior | Ariadne decision |
| --- | --- | --- |
| JobManager / TaskManager | Dispatcher submits jobs; JobMaster owns one JobGraph; ResourceManager allocates slots; TaskManagers execute subtasks. Slots reserve managed memory, not CPU isolation. Operator chaining shares a task thread. | Keep Prefect server/FlowRun authority and workers; add physical planning, placement and subtask processes under it. Do not create another JobManager service. [ADR-0042](../adr/ADR-0042-flink-informed-native-streaming-architecture.md) / [ADR-0043](../adr/ADR-0043-physical-graph-and-partitioned-channels.md). |
| Key groups | Stable redistribution units relate maximum parallelism to current subtask ownership. | Version canonical key encoding/hash and retain fixed key groups across rescale; do not copy Java object hashing into Python. [ADR-0043](../adr/ADR-0043-physical-graph-and-partitioned-channels.md) / [ADR-0048](../adr/ADR-0048-rescaling-compatibility-and-native-acceptance.md). |
| Checkpoints | Consistent operator state and source positions support restore and replay of the suffix after the selected checkpoint. | Preserve per-yield accepted frontiers; journal state in terminal transactions. Snapshots accelerate internal materialization without repeating accepted Python calls. [ADR-0044](../adr/ADR-0044-multi-input-turns-and-state-materialization.md). |
| Barrier alignment | Aligned checkpoints wait for input channels; unaligned checkpoints include in-flight buffers to reduce alignment delay at additional storage/I/O cost. | Use ordered barriers for complete state-transfer/materialization cuts. Do not import unaligned recovery before a measured need and a durable in-flight-data protocol. |
| Pipelined-region failover | Failed region restarts; missing input partitions involve producer regions; involved regions involve their consumers. Streaming pipelined topology can form a large region. | Durable accepted edges can narrow logical recovery, but shared process/state still expands the failure boundary. Flink does not guarantee restart of B alone. [ADR-0047](../adr/ADR-0047-selective-recovery-and-cross-host-authority.md). |
| Backpressure | Downstream output availability blocks upstream progress; runtime metrics distinguish busy, idle and backpressured time. Receiver credits are a network-flow-control mechanism. | Adapt capacity propagation to a persistent PostgreSQL log with durable quotas, retention roots and fair admission. [ADR-0046](../adr/ADR-0046-continuous-pressure-and-safe-retention.md). |
| Source enumerator / reader | Enumerator discovers and assigns splits; reader exposes split state, asynchronous availability and completion; assignments can be returned after reader failure. | Native source tasks need durable split identities/positions/epochs, connector replay contracts and acknowledgement only after platform admission. [ADR-0045](../adr/ADR-0045-continuous-sources-event-time-and-timers.md). |
| Event time / joins | Watermark progress is the minimum over active inputs; idleness and source alignment control stalls/skew. Window joins match keyed records within defined windows. | Separate union, correlated join, barrier alignment and watermark progress. Define late-input and unmatched-state policy before cleanup. [ADR-0044](../adr/ADR-0044-multi-input-turns-and-state-materialization.md) / [ADR-0045](../adr/ADR-0045-continuous-sources-event-time-and-timers.md). |
| State TTL | The reviewed keyed-state guide states TTL is processing-time based; it is not an event-time correctness proof. | Do not use elapsed TTL alone to reclaim join/recovery data. [ADR-0046](../adr/ADR-0046-continuous-pressure-and-safe-retention.md). |
| Sink commit protocol | External delivery guarantees depend on the sink implementation and its commit/transaction protocol. | Arbitrary client PostgreSQL/HTTP effects remain client-idempotent. Multi-destination atomic visibility is a separate unsolved contract, not a consequence of a yield or snapshot. |

## Official documentation

- [Architecture, components, slots and chaining](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/)
- [Stateful processing and checkpoint recovery](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/stateful-stream-processing/)
- [Checkpoint configuration and guarantees](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Checkpointing under backpressure](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpointing_under_backpressure/)
- [Task failure and region recovery](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/task_failure_recovery/)
- [Keyed state and TTL](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/)
- [Source internals](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/sources/)
- [Sink internals](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/internals/sinks/)
- [Window joins](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/joining/)
- [Watermarks, idleness and alignment](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/generating_watermarks/)
- [Backpressure monitoring](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/monitoring/back_pressure/)

## Pinned implementation observations

- [CheckpointCoordinator.java](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointCoordinator.java#L567): checkpoint triggering, acknowledgement handling and restoration of checkpointed subtask state (`restoreLatestCheckpointedStateToSubtasks`, line 1661).
- [SingleCheckpointBarrierHandler.java](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/SingleCheckpointBarrierHandler.java#L234): channel barrier handling and checkpoint triggering through a state machine.
- [RestartPipelinedRegionFailoverStrategy.java](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/failover/RestartPipelinedRegionFailoverStrategy.java#L151): `getRegionsToRestart` expands to unavailable producers and involved consumers. This directly contradicts a blanket B-only-restart interpretation.
- [KeyGroupRangeAssignment.java](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-runtime/src/main/java/org/apache/flink/runtime/state/KeyGroupRangeAssignment.java#L124): owner calculation `keyGroupId * parallelism / maxParallelism`; Java `hashCode()` participates in the upstream key-group calculation, which Ariadne must replace with its own stable versioned key encoding/hash.
- [SourceReader.java](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-core/src/main/java/org/apache/flink/api/connector/source/SourceReader.java#L80): reader split-state snapshots, split assignment and pause/resume capability for alignment.
- [SplitEnumerator.java](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumerator.java#L55): failed-reader split return and enumerator state snapshots.
- [CreditBasedPartitionRequestClientHandler.java](https://github.com/apache/flink/blob/c0f8d1a1e09f209885a88f9c19ceb9d9e9870283/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/CreditBasedPartitionRequestClientHandler.java): receiver credit notifications and `AddCredit` messages; the persistence/reclamation protocol for Ariadne's database log remains a separate design.

## Actual native boundary found in review

At base revision `52a981e997f049abe58ebd962b6c32a8d2f1076d`, the code paths
`_internal/stream_engine.py`, `_internal/stream_reconstruction.py`,
`workers/_native.py`, `server/models/streams.py`,
`server/models/stream_executions.py` and `server/database/orm_models.py` show:

1. One long-lived TaskRun invokes the transform repeatedly; atomic terminal
   completion is already a valuable native foundation.
2. The whole graph executes in a single FlowRun process; local thread submission
   is not physical distributed execution.
3. Native graph admission enforces one source/linear downstream chain and one
   input/output consumer binding. A schema uniqueness rule also forbids fan-out.
4. Current capacity is lifetime refusal with final-flow release, not online
   continuous reclamation and backpressure.
5. Whole-flow authority and same-host verified death do not provide subtask
   ownership or cross-host takeover.

Detailed paths/line references and corrected lock reasoning are retained in
the worker review. These are observed constraints, not regressions introduced
by this design cycle.

## Review conclusions and open work

The architect and worker reviewer initially disagreed about TaskRun ownership.
Their shared recommendation is now a logical operator aggregation identity plus
ordinary TaskRuns per physical subtask. The alternative logical TaskRun with
child lifecycles is retained with its costs. New profile/API compatibility and
native evidence are still required; discussion alone cannot close them.

The most dangerous rejected state theory was periodic snapshot rollback as
recovery authority. The standalone experiment in
[ADR-0044](../adr/ADR-0044-multi-input-turns-and-state-materialization.md) tests
the replacement: internal state-batch materialization without accepted business
replay. The broader new native execution model remains to be implemented and
validated under [epoch 2](../epochs/epoch-02-native-streaming-completion.md).
