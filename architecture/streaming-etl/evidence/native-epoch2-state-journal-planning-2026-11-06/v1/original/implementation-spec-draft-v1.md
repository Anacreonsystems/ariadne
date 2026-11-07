# NI2-020: native state journal and materialization — implementation preparation

Status: **planning / external draft for Principal review**. This is an implementation
specification under approved ADR-0044, not a new ADR, implementation, evidence of
native capability, or permission to bypass NI2-014. Base source:
`d906ceca414baeac825ce472e0b78cd1dcae3363`. Epoch-2 readiness remains **15%**.
Only source reads and this external document were authorized. No repository,
runtime, database, hook, dependency, worker, or running 5000-case asset is changed.
The skill conflict scan remains incomplete; all grill-me/spec-first variants
remain excluded. Existing approved ADR bodies and acceptance criteria stay intact.

Authority: [ADR-0044](https://github.com/Anacreonsystems/ariadne/blob/d906ceca414baeac825ce472e0b78cd1dcae3363/architecture/streaming-etl/adr/ADR-0044-multi-input-turns-and-state-materialization.md),
[task registry](https://github.com/Anacreonsystems/ariadne/blob/d906ceca414baeac825ce472e0b78cd1dcae3363/architecture/streaming-etl/todo.md),
and the Principal's source-bound handoff at
`/tmp/ni2-020-implementation-handoff-d906-v1/handoff.md`.
All new type/table/method names below are proposed implementation names. They
do not exist at this revision unless identified as an existing source seam.

## Problem and unchanged acceptance boundary

`StreamConsumer.managed_state` stores the current inline postimage. A routed
receipt preserves its digest, but not a historical state chain. A replacement
can continue the inline profile, but cannot prove a fold of state batches after
a snapshot. A source-code replay would repeat accepted business calls and is
excluded by ADR-0044.

- As a user, I want the replacement to start after my last durably accepted
  yield, including when the old response was lost.
- As a developer, I want state restoration to execute platform data operations
  without calling my task function, emitting another output, or attempting an
  external effect again.
- As a system, I want a missing, corrupt, wrong-owner or incompatible state batch
  to stop restoration before business execution while leaving accepted progress
  untouched.

Acceptance criteria, with scope kept visible:

- As a system, I can atomically accept a state postimage, route receipt, output,
  input progress and journal ordinal once, and can resolve that outcome after
  physical owner replacement. This is the NI2-020a contribution to A44-07.
- As a system, I can fold the retained genesis-to-frontier journal without task
  calls and refuse a corrupt or missing accepted batch. This is scoped
  NI2-020a evidence for A44-02; it is not the snapshot-100 test in A44-01.
- As a system, I got state 200 from a **legitimate COMPLETE snapshot 100** and
  batches 101–200, with unchanged calls/effects/output IDs/cursors, then invoke
  only 201. A44-01 remains OPEN until NI2-024a and NI2-020b actually prove it.

## 1. Internal work refinement and the legal snapshot dependency

The current task graph must not be interpreted as permitting a cache row or
fixture-seeded state to become snapshot authority. The proposed internal order
is:

```text
NI2-014 source-bound native DAG gate
  -> NI2-020a: journal, frontier, current-owner resolution, genesis fold
  -> NI2-024a: minimum one-state-owner aligned local cut
  -> NI2-020b: real native snapshot100 + batches101..200 acceptance
  -> NI2-021: full multi-input mailbox
  -> remaining NI2-024: general participant/global cuts
```

If the local cut keeps the NI2-024a label, its dependency is **NI2-020a**, not
completed NI2-020b or the full mailbox. NI2-020 stays `progress` until its native
acceptance is met; the refinement neither marks it complete early nor weakens
A44-01. NI2-024a is a prerequisite implementation slice within already approved
ADR-0044. The later multi-input/global cut remains separate work.

NI2-020a admits **genesis-only restoration**. It cannot publish, select, GC from,
or transfer an assignment using a purported snapshot100. The genesis `{}` and
zero frontier are immutable initial state defined by the sealed state contract,
not a fabricated COMPLETE barrier snapshot.

The same immutable participant/input manifest and ordered DATA/BARRIER proof
required by ADR-0044 still apply to a one-owner local cut. Completing a cached
postimage digest is insufficient. Journal rows are not themselves an input-log
barrier. No first-stage retention deletion or rescale is authorized by this work.

## 2. Source facts that constrain the design

Paths below are relative to `src/prefect/` and source-bound in `source-bindings.json`.

| Existing seam | Consequence |
| --- | --- |
| `server/schemas/stream_dags.py:184`, `:271`; `server/models/stream_dags.py:57` | Seal and plan digests use the entire strict v1 `model_dump(mode="json")`. Adding an optional/default state field to that model changes old canonical bytes. |
| `streams.py:56`, `:498`; `_internal/stream_dag.py:245`, `:381` | Flow enrollment, local authoring witness, deployment snapshot and public seal must all agree on the admitted profile and code identity. |
| `server/database/orm_models.py:1670`, `:1783`, `:2024`, `:2555` | Graph, subtask, capability, deployment and related topology checks explicitly fence the existing profiles. A new state protocol requires paired schema/profile admission changes. |
| `server/models/stream_routes.py:941` | `_commit_route` already owns the atomic routed terminal transaction and exact receipt lookup. Reuse that boundary, rather than creating a second terminal engine. |
| `server/models/stream_routes.py:1046`, `:1079` | The existing semantic digest excludes physical owner/attempt; the state postimage digest is already in the immutable routed intent. Existing receipts must keep their interpretation. |
| `server/models/stream_routes.py:362`, `:396` | Own TaskRun UPDATE precedes sorted partition locks and Consumer UPDATE. A new state-owner lock must fit this order without a parent lock upgrade. |
| `server/models/stream_dag_executions.py:1603`, `:1877`, `:1999`, `:2278` | Current-child authorization can operate before readiness, but the routed data path requires `ready=True, active=True`. Calling that path to make the child ready would be circular. |
| `server/models/stream_dag_executions.py:1811` | Bootstrap ready verifies a stored immutable witness, not a newly generated live-state bootstrap. A restore witness can be frozen per grant without becoming stale at business turn 201. |
| `_internal/stream_routed_storage.py:296`, `:411`; `_internal/stream_engine.py:282`, `:370`, `:442` | The engine already uses `ctx.managed_state` and a frozen current-turn intent. Old RETURN is state-only; YIELD(null) is an actual output. |
| `server/database/orm_models.py:732`, `:836`; `server/models/stream_routes.py:785` | Existing record sequence and consumer completion are business coordinates. Inserting a barrier into that sequence would change document-201 lineage. |
| `server/models/stream_dag_executions.py:2763`, `:3015` | Both finalizers delete Completion, RouteReceipt and Consumer. New roots/foreign keys require an explicit dependency-safe terminal teardown. |

## 3. Version and canonical-byte recommendation

**Recommendation for Principal review: an explicit new `partitioned-dag-v2`
profile with separate strict v2 DTOs and an explicit profile parser.** Reuse the
same physical/lifecycle/task engine and routed terminal transaction. This is a
protocol boundary inside native Prefect execution, not a second engine.

Proposed v2 declaration fields, all present and required in its canonical form:

```json
{
  "profile": "partitioned-dag-v2",
  "assignment_epoch": 1,
  "terminal_profile": "yield-state-return-v1",
  "transport_protocol": "ordered-channel-v1",
  "state_protocol": "journaled-postimage-v1",
  "cut_protocol": "none",
  "operators": ["the existing operator fields plus explicit v2 state contracts"],
  "edges": ["the existing edge fields"]
}
```

The abbreviated arrays describe structure, not executable requests or golden
vectors. The implementation must publish exact byte fixtures with valid IDs.
NI2-020a accepts only `cut_protocol="none"`; unsupported cut values fail before
allocating task/data metadata. NI2-024a may admit its independently implemented
`aligned-local-v1` value through the same explicit v2 feature negotiation. It
must not reinterpret a v2 graph already sealed with `none`.

- Keep the existing v1 classes, defaults, `model_dump`, domain separators,
  receipt decoding and exact old JSON unchanged. Do not serialize a new
  `state: null`, protocol field, wrapper, or reordered collection into v1.
- V2 gets its own seal/plan/bootstrap/read/complete/outcome models. Its graph
  digest covers state contracts, transport/cut versions, limits, initial state,
  owner UIDs, ports, task/code/version and all existing immutable graph fields.
- Domain-separate the v2 declaration, state-contract, batch, chain, snapshot,
  cut-completion and restore-witness hashes. Use the existing strict finite JSON
  canonical encoding and `stream_value_JSON` storage, not generic JSONB for
  postimages. Never include auth capabilities or physical epochs in accepted
  logical outcome identity.
- Flow `NativeStreamFlowConfig(profile="partitioned-dag-v2", partition_count=None)`
  enrollment must copy that exact profile into the native deployment snapshot.
  An old v1 snapshot cannot seal a v2 graph. Workers advertise a separate exact
  v2 capability; a v1 advertisement does not satisfy it.
- Extend profile-family dispatch and database checks explicitly. Do not relax
  them to arbitrary strings or use `startswith` as protocol authorization.
- Proposed API namespace: `/streams/v3/dags`, with parallel narrow parent/child
  routers and DTOs delegating to shared native services. Existing
  `/streams/v2/dags` routes continue to require their old profile. The exact
  namespace is a review choice; it is not yet a public API.
- SDK authoring can add an explicit state-contract argument to `StreamGraph.add`.
  The old argument-absent path must emit its identical witness. New-profile
  authoring emits v2 fields; ordinary `@task`, `ctx.managed_state`, sync/async
  engines and old state-only RETURN semantics remain the execution language.

Required negative downgrade traces:

1. V1 client/server sees the new profile: strict rejection before enrollment,
   seal, assignment or business code; never fall back to INLINE.
2. A v2 child sends an old read/complete/bootstrap/activation request: the server
   checks profile before mutation, returns a typed refusal, and leaves every
   cursor, state root, journal, output and ready witness unchanged.
3. V1 deployment snapshot + v2 graph, v1 worker + v2 assignment, or altered
   state-contract digest: reject before a child receives business input.
4. Stored old RETURN receipt remains state-only and byte-identical; new
   journaled RETURN still emits no output. New OUTPUT/NO_OUTPUT semantics are
   NI2-022, not a side effect of this profile.
5. Existing v1 graph/receipt golden vectors, live native cases and strict client
   round trips remain exact on the final union source.

### Smaller-looking side-contract alternative

Alternative B preserves the v1 base graph and stores a separate immutable
state-contract digest. Its required transitive binding is:

```text
deployment snapshot -> graph revision + state-contract digest
 -> worker capability + assignment/grant scope
 -> immutable bootstrap + ready/activation witness
 -> route authorization + state batch + accepted resolution
```

Each link and every old endpoint needs a mandatory no-downgrade fence. Without
the deployment link an old server can admit an INLINE graph; without the child
link an old bootstrap can activate; without a terminal link an old complete can
advance the cursor without a batch. Merely placing a row beside an unchanged v1
graph digest proves none of these links. This alternative is **not selected in
this draft**: it adds independent authority and still needs most admission
changes. A smaller diff is insufficient reason to accept that proof burden.

## 4. Proposed logical state rows and immutable identities

Names are concrete implementation candidates, not existing database objects.
Paired PostgreSQL/SQLite migrations provide schema parity; physical execution
and state mutations remain PG17-only, with SQLite refusal before mutation.

| Family / key | Required fields and constraints |
| --- | --- |
| `StreamStateOwner`, UUID PK | Graph/Flow FK, operator-run FK, immutable owner UID, original assignment epoch and original partition index, state-contract/compatibility digest; UNIQUE(graph, original assignment, operator, owner UID, original partition). Accepted apply ordinal/version, state-value digest, chain root and accepted cursor vector are authoritative. Physical TaskRun, worker and grant are not the logical key. |
| `StreamStateOwnerInput`, UUID PK | Immutable owner/input-port/subscriber/edge-partition/input-stream lineage; FK to the existing exact stable edge-partition tuple. UNIQUE(owner, input port, original partition). It can name the sealed subscriber before a Consumer row exists. Batches and later cursor vectors use this mapping. |
| `StreamStateOwnerBinding`, owner + assignment key | Maps a logical owner to the current physical subtask and assignment, with exact graph/subtask FKs. Initial mapping is one owner per subtask; replacement keeps it. Future assignment transfer must preserve historical owner and batch identities, not rewrite them with a new TaskRun. Rescale itself is not implemented here. |
| `StreamStateBatch`, deterministic UUID | UNIQUE(owner, apply ordinal), UNIQUE(owner input, business sequence) and UNIQUE(route receipt). Logical batch ID derives from owner + original input lineage/record + sequence; attempt/physical epoch are audit columns. Stores before/after versions, before/after state digests, previous/result chain roots, bounded postimage, payload digest/size, terminal kind, exact route-receipt/output linkage, original data/log coordinates and mutation/batch digest. |
| `StreamStateMaterialization`, owner/restore-attempt key | Derived postimage/cache, verified-through ordinal, state digest/chain root/cursor vector, target frontier, selected legal anchor, current grant/physical fence and phase MATERIALIZING/READY. Partial materialization is never authority. Restore attempt identity binds the current grant and immutable target frontier. |
| Later local-cut/snapshot family | Immutable cut manifest and snapshot payload/root; per-participant input/output barrier acknowledgements; immutable COMPLETE witness binding the whole declared vector. PREPARED/ABORTED rows cannot authorize restore. No bare boolean `complete` is sufficient. These rows are activated only with the actual NI2-024a protocol. |

Keep a direct receipt FK with RESTRICT semantics. The batch must bind its owner
input lineage and the existing receipt's consumer/sequence/kind/semantic digest;
use the existing completion-witness unique tuple or add an explicit composite
unique tuple where necessary. A lone unrelated receipt ID is insufficient.
The implementation must spell the final ORM constraint tuples before migration
code; the family table above is not a claim that those constraints exist.

State postimages may be the first batch representation. They eliminate the need
for a user mutation DSL, but do not permit skipping an intermediate batch. A
fold verifies every ordinal, before/version/root link, postimage digest, receipt
link and final accepted cursor vector. Applying postimage200 directly while
batch151 is missing is forbidden.

Recommended digest structure:

```text
state_value_digest = H(canonical(postimage))
batch_digest = H(domain, owner/original input identity, apply ordinal,
                 before/after versions and roots, canonical postimage digest,
                 terminal kind, route receipt + output identity, compatibility)
chain_root[n] = H(domain, chain_root[n-1], batch_digest[n])
```

Apply ordinal is a global state-owner order, not a permanent synonym for a port
sequence. They coincide in the admitted single-input slice; NI2-021 must not
depend on that coincidence. Initial state/version/root are sealed, with `{}` as
the first compatible state. Opaque Python closure/process memory is unsupported.

## 5. Atomic terminal path, quotas and lock order

Recommended transaction order, after the existing current-child scope proof:

```text
existing Flow/Authority/Graph SHARE, Worker/Subtask/Grant/launch fences
 -> own TaskRun UPDATE
 -> current logical owner binding SHARE; StateOwner UPDATE
 -> complete UUID-sorted StreamPartition UPDATE union
 -> Consumer UPDATE
 -> exact accepted receipt/batch check, or validate fresh delivery + capacity
 -> payload + references + route receipt + Completion + StateBatch
 -> accepted owner frontier + Consumer business cursor + derived cache
 -> one outer commit
```

The precise new binding/FK acquisitions require a full lock trace review.
No late parent SHARE-to-UPDATE upgrade is allowed. Every state operation follows
the same task/owner/partition/consumer order; control/finalization takes the
existing exclusive graph fence first and uses sorted own-resource locks.

The accepted-lookup branch precedes allocating an ordinal, output ID, new
payload, batch, reservation or progress update. It validates the immutable
state batch and receipt and returns the exact accepted outcome. A changed
postimage or route intent conflicts. Before-current-version checks apply only
to a new turn; otherwise a lost ACK would turn exact replay into a false conflict.

For a fresh turn, the v2 frozen intent includes current owner ID, compatibility
digest, expected before-version/root, current delivery, postimage and existing
terminal value/key. StateBatch, route receipt, output, completion, accepted
cursor and owner frontier are one transaction. Injecting failure at any flush
must leave the entire accepted set absent.

Proposed first finite quota policy copies the existing sealed operator limits
into an explicit v2 state quota: postimage <= `max_state_bytes`, retained batch
count <= `max_records`, retained encoded journal bytes <= `max_bytes`. These are
separate usage counters and must not consume the same bytes twice implicitly.
Account for encoded metadata as well as the postimage; choose a fixed bounded
batch-overhead cap only after calculating the maximum DTO encoding. It is a
storage bound, not an invented latency or throughput SLO.

Reserve sufficient bounded journal credit before exposing a fresh input body.
Only one in-flight logical turn is admitted per state owner. A replacement
reuses the logical reservation for that incomplete coordinate; an accepted
turn converts it once. With no online GC in this slice, exhausted finite state
quota must cause a clear typed capacity refusal before another body invocation,
not an endless wait for a reclaimer that does not exist. Full-turn vectors,
continuous pressure and online reclamation remain ADR-0046. Snapshot and
materialization temporary storage need separately bounded sealed quotas before
NI2-024a is enabled; they cannot be silently unbounded.

## 6. Pre-ready restore and current-owner accepted resolution

Proposed narrow v2 child endpoints, with paired sync/async client methods:

| Endpoint family | Request / result / authorization |
| --- | --- |
| `POST /streams/v3/dags/state/resolve` | Current v2 context + logical owner/input coordinate or batch ID; returns immutable batch/receipt/output IDs and roots. It accepts no old private capability or claimed output body. Current child scope with `ready=False, active=False`, exact owner mapping and graph/control fences; no business mutation. |
| `POST .../state/materializations/start` | Current owner, immutable target frontier and exact compatibility; selects only the admitted legal anchor and creates an idempotent restore attempt. Initial NI2-020a selects genesis only. |
| `POST .../state/materializations/step` | Restore attempt ID + expected verified ordinal/root + bounded page intent; returns verified progress or final READY witness. Performs only platform state fold, never Task.fn or route append. |
| `POST .../subtasks/{id}/bootstrap-ready` | V2 signed request binds base bootstrap digest and completed restore-witness digest. Refuses partial/missing/wrong-grant materialization. |
| V2 data `open/read/complete/finish` | Existing current ready/active child proof plus state-cache/frontier checks where applicable. No v1 method may operate on a v2 subtask. |

These names are proposed; exact DTO classes are separate server/client models.
The allowlists in `_internal/stream_execution.py` and routed storage dispatch
must enumerate methods and paths explicitly. Child scope never becomes an
arbitrary state-owner/admin SQL API. Resolve cannot read another graph, owner,
port, assignment, or released horizon. Stale owner credentials are still refused
even when the requested batch is accepted.

The restore lifecycle avoids the existing ready/active circular dependency:

1. Registered current child reads a **v2 immutable bootstrap** with owner,
   compatibility, and frozen target accepted frontier. No task body runs.
2. Dedicated restore authorization reuses `lock_dag_child` with readiness and
   activation false, plus current process/slot/owner mapping/control checks.
   It does not call `authorize_dag_child_routing`, `NativeDAGRouteStore` or
   `lock_current_task` requiring RUNNING. The task is permitted to remain Pending
   under this narrow platform operation.
3. Start/step folds server-side in bounded pages. Use the existing sealed prefix
   record/byte bounds as page caps. Intermediate cache is MATERIALIZING. A lost
   page response is idempotently resolved from its exact target/page roots, not
   rerun as a business turn. A new physical grant gets a new restore attempt;
   another grant's incomplete cache never becomes its anchor.
4. The final step verifies all accepted ordinals and the target state/chain root
   and cursor vector, then stores a current-grant READY witness. It changes no
   accepted ordinal, cursor, business audit, output or receipt.
5. V2 bootstrap-ready signs that immutable restore witness. First activation or
   isolated replacement activation requires it. Healthy sibling grants remain
   active and do not repeat a global activation.
6. The witness attests initial restoration, e.g. frontier200. Business turn201
   atomically moves the accepted/cache frontier to201; it must not invalidate
   the frozen bootstrap-ready witness. Data reads separately require cache
   version/root/current grant to match the latest accepted owner frontier.

For current-owner resolution before ready, authorizing a pending task is a
deliberately narrow read/fold seam. It does **not** grant that task permission to
read fresh business input, complete a delivery, emit an event that starts user
execution, or bypass activation. Header/auth guard tests must prove this.

## 7. Ordered local cut prerequisite — proposed transport design

This section is **NI2-024a planning**, not an implemented snapshot protocol or
claim that NI2-020a meets A44-01. It needs Principal review before code.

Recommended opt-in table: `StreamPartitionLog(stream_id, log_position,
entry_kind DATA|BARRIER, data_sequence/data_record_id?, cut_id/barrier_id?)`.
DATA and BARRIER share this one ordered log. DATA points to the existing
immutable StreamRecord; no v1 record is renumbered. Atomic DATA append writes
both its payload/reference and its log entry under the partition lock.

V2 progress distinguishes:

- `business_completed_through` / existing data lineage and
  `ctx.input_sequence`: 200 then201;
- `accepted_log_position`: transport ordering, including internal barriers;
- staged/prefetched position: arrival only;
- snapshot cut frontier and event-time watermark: separate concepts.

For example, DATA200 may be log200, BARRIER cut-X log201, and DATA201 log202.
Accepting the barrier advances only internal transport alignment, never business
completion from200 to201. DATA201 remains business input201. New v2 delivery and
cursor DTOs bind both coordinates; old DeliveryToken and v1 JSON stay exact.

### Pre-cut intents and source fencing

Current source append atomically commits DATA/references/receipt. The SDK frozen
intent is private memory, not an existing durable PREPARED-intent ledger. The
new log table alone does not establish the required no-overtake proof.

The concrete candidate is a bounded v2 `StreamRouteAppendIntent` protocol:
producer TaskRun/stream and logical producer-position key, canonical frozen
value/key digest and bounded value/reference, request digest, and PREPARED /
COMMITTED / explicitly fenced-abort outcome. One pending intent per producer,
with its logical identity independent from the authoring physical attempt.
Preparing an intent and preparing a cut use the same producer serialization
lock. A cut captures all already registered pre-cut intents and prevents new
preparations crossing its boundary until the ordered marker is placed.

The source engine drains/commits each captured pre-cut intent before it may
append its exact cut barrier. An old direct-append endpoint cannot bypass this
v2 gate. If a producer dies with a pending durable pre-cut intent, the cut may
wait or be explicitly aborted under the current coordinator fence; it cannot
skip that intent and publish COMPLETE. No automatic prepared-intent takeover or
external-effect guarantee is claimed here. Any later recovery/adoption needs
its own exact current-owner validation and retained logical identity.

The initial cut slice may admit one pre-armed cut at a declared safe source
frontier, for one stateful **leaf** owner. Pre-arming before execution lets the
source engine place the barrier after the preceding append ACK and before
freezing the next DATA intent, without assuming that it can interrupt arbitrary
blocked user code. The manifest is immutable and includes the required source
producer/edge proof as well as the single state participant. Dynamic cuts of
blocked generators, general multi-owner alignment and graph-wide cuts remain
unimplemented until their actual protocol is supplied. This restricted first
profile is a proposal, not an accepted new guarantee.

### Legitimate snapshot100 anchor

The local cut has a stable ID/purpose, graph/coordinator/assignment versions,
compatibility digest, exact participant/required-input/output manifest and
pre-cut intent frontier. A BARRIER entry is fenced into the required input log
after all pre-cut DATA. The owner accepts all preceding DATA before observing
that barrier and pauses post-cut business inputs while aligning.

Only then can its acknowledgement bind business/log cursor vectors,
accepted apply/version high water, state value and chain roots, and evidence
that every pre-cut output route is durable. Output barriers follow pre-cut
routes. The first leaf profile still records its own output barrier; it cannot
pretend to prove an arbitrary downstream participant set.

Snapshot payload, verified content root and participant acknowledgement remain
PREPARED until a current coordinator publishes the immutable COMPLETE witness
for the **whole declared manifest**, including producer/barrier placement proof.
Any interrupted preparation retains pins or is explicitly aborted. A compatible
COMPLETE local cut may authorize local state restore only. It does not by itself
authorize journal GC, retention reclamation, graph rescale or atomic external
sink writes. A historical COMPLETE witness stays immutable; current write/auth
fences are separate from the logical identity of an already accepted cut.

If this closed protocol is not implemented, NI2-020b must remain blocked.
Creating snapshot100 from `Consumer.managed_state`, a digest, a fixture seed,
an internal journal-only marker or an unaligned transport arrival is forbidden.

## 8. Implementation commits and real acceptance cutpoints

All commits below require Root's post-NI2-014 implementation GO. Current work is
only this document. Proposed tests do not exist and are not test results.

1. **Version/admission/schema foundation.** Separate v2 DTO/canonical/parser and
   explicit capability/deployment/profile fences; paired DB migrations and
   state owner/input/binding/batch/materialization/ordered-log DATA foundation.
   Real PG/SQLite schema refusal and old v1 golden/paired-client regression.
2. **One terminal state transaction.** Reuse the native routed acceptance seam;
   add bounded pre-body reservation, exact duplicate/lost-ACK response, batch
   root chain and composite constraints. Real PG transaction rollback at each
   flush, concurrent duplicate, changed-intent refusal and independent-owner
   contention. Old v1 route bytes remain exact.
3. **Least-privilege resolve and paged fold.** Dedicated pre-ready current-child
   operations, no route-ready cycle; every missing/corrupt/incompatible ordinal
   refuses readiness. Test stale/current/foreign owner plus cancel/takeover
   interleavings and idempotent lost page responses on real PG/API boundaries.
4. **Native SDK/worker plumbing and NI2-020a gate.** Paired clients, profile-aware
   bootstrap/private gates/allowlists/storage, same sync/async ordinary task
   engine. Real TCP/PG17/ProcessWorker test folds genesis1..200 after actual B
   death; unchanged business counters before201; RETURN state-only regression;
   strict process/slot/coordinator/API health. Principal independently verifies.
   This leaves snapshot100/A44-01 OPEN.
5. **NI2-024a local cut prerequisite.** Add durable append-intent/cut fencing,
   ordered BARRIER transport, participant/output proofs and genuine COMPLETE.
   Interrupt prepare, pending intent, DATA commit, input barrier, owner ACK,
   snapshot payload, output barrier and COMPLETE publication. No partial cut
   can be selected for restore/GC/rescale. Source/barrier failures are actual
   process or transaction cuts, not sleep-based simulated outcomes.
6. **NI2-020b full native oracle.** Reach legal COMPLETE snapshot100 with
   state sum5050; accept200 with sum20100; kill B while A/C stay healthy. A new B
   materializes exactly101..200 and starts201 with sum20301. At the pre-201 gate,
   body/effect audit stays200 and all accepted output/receipt/cursor IDs match.
   Remove/corrupt/incompatibly tag151 in separate owned adversarial cases:
   activation is refused, cursor remains200 and no business201 runs. Lost ACK200
   and pre-accept200 death separately demonstrate201 versus200; old owner is
   fenced. Sync/async parity, final lifecycle closure and independent verdict.

The real native test should extend the existing process fixture approach in a
dedicated `tests/test_native_stream_state_journal_http.py` module. Both old
INLINE/RETURN and the new opt-in profile run on the final union source. A
standalone prototype, DB fixture seed, mocked Python call or mere model-table
assertion cannot close native A44-01/02/07.

Terminal teardown must first hold the verified graph/process-closure fence,
then remove new dependent restore/cut/snapshot/journal rows in the actual FK
dependency order before the existing receipt/consumer/data cleanup. No live
accepted chain is deleted to make a finalizer pass. Completed graph-release
semantics must explicitly state whether post-release accepted lookup has ended;
`ROUTE_HORIZON_EXPIRED` is not authorization to rerun old business code.

## 9. Review decisions still required before implementation

1. Confirm new-profile/API naming and exact v2 byte fixtures; explicit version
   admission is recommended, not yet a merged protocol.
2. Finalize composite FK tuples and full lock trace across complete/read,
   pre-ready materialization/resolve, replacement, control and terminal cleanup.
3. Set an encoded batch-overhead bound and separately bounded temporary
   materialization/snapshot/append-intent quotas. Preserve no invented SLO.
4. Review the minimal local-cut leaf/pre-armed restriction, source-intent
   registration/fencing, failure/abort/pin authority, and whether that slice is
   sufficient before enabling the snapshot100 native oracle.
5. Define deterministic code-to-test hooks for lost ACK and corrupted owned
   durable fixtures without altering Flow/Task orchestration state directly.
6. Add source-bound runtime evidence and an independent Principal verdict.
   No acceptance percentage or `accepted` ADR status follows from this plan.

The parent task remains in progress. Real contract changes return to architecture
review; ordinary detail refinement inside the already approved ADR-0044 scope
does not ask the user to approve the same work again.
