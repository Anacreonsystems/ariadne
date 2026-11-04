# ADR-0043: Physical graph and partitioned durable channels

- Status: `planning`
- Date: 2026-11-04
- Owners: ETL Architect / HighLoad Principal, SDK/server/worker maintainers
- Related: [architecture](ADR-0042-flink-informed-native-streaming-architecture.md), [mixed tasks](ADR-0037-mixed-finite-and-streaming-tasks.md), [recovery](ADR-0047-selective-recovery-and-cross-host-authority.md), [rescaling](ADR-0048-rescaling-compatibility-and-native-acceptance.md)

## Problem

The native manifest is a chain. A consumer ID is a TaskRun ID, bindings have one
input and output, a unique upstream constraint forbids fan-out, and completion
uses one scalar cursor. All tasks share one FlowRun process. These assumptions
cannot represent independent partitions, branches or isolated operator failure.

The existing strict queue order also must not silently become an unspecified
parallel order. Partitioning introduces a new explicit ordering contract.

## Theory

A versioned logical DAG can expand into stable physical subtasks and durable
partitions. Independently owned execution processes and subscriber cursors
provide meaningful recovery boundaries without a TaskRun per document.

## Proposed solution and agreement

Agreed foundations:

- Logical graph: stable operator UID, named typed input/output ports, immutable
  graph revision, validated acyclic edges and finite/stream dependency types.
- Physical graph: fixed-parallelism subtasks, partition identities, key-group
  assignments, worker slots and fenced attempts. A logical definition remains
  an ordinary Prefect Task; stream processing repeatedly invokes its body.
- Durable coordinate: `(channel_id, partition_id, sequence)`, separate from
  business `document_id`. Consumer identity also binds subscriber, input port
  and assignment. Retry preserves all logical data identities.
- Fan-out: one immutable producer log can serve several independently retained
  subscribers. Completing one branch cannot acknowledge another branch's input.
- Ordering: preserve total order in the existing single-partition profile.
  New partitioned profiles guarantee order within a partition and routed key;
  no implicit total order across partitions. A global merge/reorder operator
  requires an explicit ordering key, missing-input policy and bounded buffer.
- Routing: canonical key encoding, hash algorithm, routing version and fixed
  key-group count are persisted. Python `hash()` is not a portable partitioner.
  Use `subtask = floor(key_group * parallelism / key_group_count)` with an
  explicitly versioned hash-to-group mapping, not an accidental runtime hash.

### Physical exchanges and durable routing

An edge identity binds graph revision, producer operator/output port, subscriber
operator/input port, exchange kind and routing version. Producer partitions
and edge partitions are distinct identities. The proposed first implementation
materializes routing **at terminal append**, not independently at reader retry:

| Exchange | Persisted mapping |
| --- | --- |
| `forward-v1` | Explicit one-to-one producer/edge partition map; refuse incompatible parallelism. |
| `rebalance-v1` | Deterministic target from the persisted producer partition/sequence and routing version; never a process-local random/round-robin counter. |
| `broadcast-v1` | One reference entry for every partition in the sealed edge target manifest. |
| `keyed-v1` | Canonical key → persisted key group → target partition under the sealed assignment. |

The terminal transaction stores the immutable producer envelope and all bounded
edge-routing entries, along with state, receipt and input completion. Each edge
partition has its own ordered sequence. A route identity binds original turn,
edge, target partition and output ordinal; its digest includes the sealed
mapping. Exact retry resolves that prior route set, rather than rerouting using
the replacement's current parallelism. Readers consume persisted edge entries.
Rescale transfers their retained identities/cursors through ADR-0048.

Different branches may have different exchanges and partition counts. Their
subscriber cursors and route-retirement roots are independent; the shared
producer envelope remains until all edge references and other pins release it.
No claim of zero-cost fan-out is made: every routed reference consumes the
admitted terminal-write budget. FIFO is the persisted edge-partition order;
independent producer partitions provide no implicit cross-source causal order.

### Canonical key domain for the first partitioned profile

Propose `tagged-key-v1`, with exact Python types and a maximum **4096 encoded
bytes**, including tags/lengths. This deliberately bounded domain must be
implemented and proved before profile admission:

| Key type | Canonical bytes |
| --- | --- |
| `None` | ASCII `N`. |
| `bool` | ASCII `B`, then one byte 0 or 1. Test exact type before integer. |
| signed 64-bit `int` | ASCII `I`, then eight big-endian two's-complement bytes. |
| `str` | ASCII `S`, four-byte unsigned big-endian UTF-8 byte length, then strict UTF-8. No Unicode normalization; distinct code-point sequences remain distinct. Surrogates are refused. |
| `bytes` | ASCII `Y`, four-byte unsigned big-endian length, then the raw bytes. |

All floats, including negative zero, infinities and NaNs, structured keys and
custom objects are refused before DATA admission. No implicit string/number
coercion is allowed. The proposed hash is SHA-256 over these bytes;
`key_group = unsigned_big_endian(full_digest) % key_group_count`. Persist the
encoding/hash/routing versions and key-group count in the manifest.

SDK/JSON transport uses explicit type tags: integer values are canonical decimal
strings within the signed-64-bit range; bytes are canonical padded base64;
strings and booleans retain their exact types; null has its own tag. Decode and
re-encode must produce the same canonical bytes. Unsupported tags, noncanonical
integer/base64 forms and oversize encodings fail closed. These are proposed wire
constraints, not a new implemented SDK serializer.

### TaskRun identity: discussed recommendation

The first review produced two alternatives. The direct architect/worker review
then agreed on the first as the planning recommendation:

| Alternative | Benefit | Cost / proof needed |
| --- | --- | --- |
| One TaskRun per physical subtask; stable on retry | One Prefect task lifecycle owner per independently executing process; ordinary retries and logs remain attributable. | Refines epoch 1's one-TaskRun-per-logical-node meaning; logical grouping/UI and rescale-created bounded TaskRun sets need explicit treatment. |
| One TaskRun per logical operator plus physical child vertices/attempts | Preserves logical TaskRun identity independently of parallelism and rescale. | Requires a new physical lifecycle/state aggregator, independent retries/concurrency and lock ownership; children cannot independently propose success for the shared TaskRun. |

Use a stable logical `StreamOperatorRun`/operator UID for topology, aggregate
status, UI grouping and rescale history. Each physical subtask is an ordinary
Prefect TaskRun; retry/replacement retains its TaskRun ID and increments the
attempt. A planned assignment epoch creates a bounded new subtask TaskRun set.

The [Principal's initial preference](../evidence/flink-architecture-2026-11-04/principal-discussion.json)
and the
[worker architecture review](../evidence/flink-architecture-2026-11-04/worker-architecture-review.json)
retain the original disagreement. A logical TaskRun with child-owned physical
attempts is a coherent deferred alternative, but would add another complete
physical state/retry/concurrency lifecycle and a single parent-state aggregator.
There is no measured metadata constraint yet to justify that cost.

This explicitly refines epoch 1's one-TaskRun-per-logical-node contract for a
new profile. It is not a claim of implemented compatibility. Metadata remains
proportional to operators, subtasks and assignment changes, never documents.
`planning` means the discussed direction is ready to plan; it does not mean the
schema, SDK or native criteria are accepted.

### Native integration constraints

Current `_task` takes a TaskRun `FOR UPDATE` and `_bound` locks input/output
partition rows. Hiding concurrent subtasks under one current TaskRun would
serialize their hot path and obscure ownership. The FlowRun shared lock is
not itself an exclusive global serialization point. A new design must specify
lock order, subtask/partition fences and cancellation checks without discarding
the proven atomic completion transaction.

Physical slots and the work-pool FlowRun concurrency lease are separate
resources. Scheduling must reserve a runnable set or support safe staged
admission; indefinitely running source tasks cannot monopolize all capacity
while downstream consumers wait for slots.

Ordinary tag/global task concurrency also needs an explicit streaming policy:
atomic region admission or a declared compatible slot-sharing/resource rule.
TaskRun identity alone does not remove the existing flow-wide holder/signature
guard; independent physical owners require subtask-scoped data-plane fencing.

### Contracts to close before the native implementation gate

- Define aggregate operator status for partial retry, terminal subtask failure,
  drain and planned rescale handoff. Old assignment TaskRuns need an explicit
  terminal reason; do not report an unfinished stream as successful work.
- Define one hierarchy for task retry policy, infrastructure restart budget,
  region escalation and terminal FlowRun failure. Replacement must not reset
  retry counts or bypass normal task-event ownership.
- Define long-lived task timeout versus per-input timeout and cancellation
  behavior. Existing finite-task semantics remain the baseline outside the new
  declared profile.
- Keep logical business/record identity stable across retry and rescale.
  A client idempotency key cannot rely solely on a TaskRun ID or attempt number
  that changes with a new assignment. Expose the stable input/lineage identity.

## Stories

- As a developer, I want fan-out and parallel operators to retain clear Prefect task identities.
- As a user, I want document order and restart coordinates to be explicit.
- As a system, I want each physical process to have one fenced lifecycle owner.

## Acceptance criteria

- As a developer, I can read an agreed TaskRun/subtask identity contract and its finite-task compatibility rules before native implementation begins.
- As a system, I can seal a real branched DAG and reject cycles, incompatible ports, duplicate subscribers and unsupported profiles before side effects.
- As a system, I got stable partition/subscriber identities through retry, with completion on one branch leaving the other branch's cursor unchanged.
- As a user, I can process 500 and 5000 records with unchanged execution metadata counts for a fixed graph and assignment.
- As a system, I can execute source and operator subtasks in distinct native processes and replace one without falsely completing the logical operator or restarting healthy processes.
- As a developer, I got identical key-group mapping across supported Python versions, hash seeds and hosts, and a documented absence of total cross-partition order.
- As an operator, I can observe physical slot ownership, pending admission and the retained FlowRun concurrency lease without duplicate allocation.
- As a system, I can refuse or safely queue a region whose simultaneously-live subtasks exceed available task/tag/global slots, without starting a source that starves all its downstream tasks.
- As a system, I can route one producer into branches with different exchanges/partition counts, retry append/read and replace one branch without a changed route digest, duplicate, omission or cross-branch acknowledgement.
- As a developer, I got canonical-key golden vectors across runtimes, hosts, hash seeds and SDK/JSON round trips, including boolean/integer/string/bytes collisions, Unicode forms and refused floats/noncanonical keys.

## Evidence and current state

Code audit and architecture discussion only; no native DAG/parallelism gate has
run. [Flink research](../research/flink-2.3-architecture.md) supplies reference
mechanisms, not implementation evidence. The first real slice is proposed in
the [epoch document](../epochs/epoch-02-native-streaming-completion.md).

## Consequences and failure modes

Global order can reduce concurrency through head-of-line blocking. Subtask
representation changes API/UI expectations and cannot be concealed as a worker
detail. Process isolation and durable edges are both needed for selective
recovery; changing one without the other leaves an incorrect failure boundary.
