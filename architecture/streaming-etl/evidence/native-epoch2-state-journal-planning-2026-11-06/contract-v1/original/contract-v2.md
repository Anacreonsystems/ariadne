# NI2-020a implementation contract — exact planning choices

Status: **planning, successor revision for Principal agreement**; no code or native
acceptance. Source authority is immutable Git revision
`d906ceca414baeac825ce472e0b78cd1dcae3363`, not the currently edited primary
logging test. This package supersedes no closed planning artifact. It refines
approved ADR44 A44-02/07 at component/native-genesis scope; **A44-01 remains
OPEN**. NI2-014 and a separate implementation handoff remain prerequisites.
No state runtime, DB, hook, prototype, install or repository mutation is run.

## Developer/user/system stories and acceptance

- As a user, I want replacement to resume after an accepted yield without
  repeating that task body or effect.
- As a developer, I want the platform to restore declared managed state and
  resolve an accepted outcome under the new current process authority.
- As a system, I want corrupt/incompatible/missing state history, old physical
  credentials and wrong task attempts to fail before business invocation.
- As a system, I can accept one batch/output/cursor once; exact replay at full
  quota does not reserve again. This remains a future executable criterion.
- As a system, I can fold genesis through accepted200 without business calls
  and then invoke201; missing151 blocks readiness without cursor rewind.
- As a system, I cannot select a cached postimage or fixture seed as COMPLETE
  snapshot100. That acceptance waits for authentic024a and020b.

## 1. Chosen version, API and capability tuple

Choose `partitioned-dag-v2` as the new explicit graph/deployment/worker profile.
Outer Flow enrollment and worker capability `protocol_version` remain integer1;
the profile string selects a distinct immutable protocol. Exact first feature
tuple is:

```json
{"terminal":"yield-state-return-v1","state":"postimage-journal-v1","transport":"ordered-channel-v1","cuts":"none","retention":"finite-release-v1"}
```

These five keys are required, with no omitted/default/null feature values. A
feature registry admits only this complete tuple in020a. Parsing recognizes
`cuts=aligned-local-v1` solely to return `UNSUPPORTED_FEATURE_COMBINATION` before
TaskRun/stream/state metadata. It is not an enabled cut protocol. Worker v2
capability implies this exact tuple for020a; no arbitrary feature-list column
is added to old capability advertisements. A future tuple requires explicit
worker negotiation and server support before metadata admission.

Flow config is `profile="partitioned-dag-v2", partition_count=None`. Its v2-only
enrollment encoder injects this fixed tuple and `features_digest`; it uses a new
strict `NativeStreamDeclarationV2`, not extra defaults on the v1 declaration.
The deployment snapshot freezes both. Feature digest is
`H("prefect-stream-dag-features-v1", features)`. The immutable mapping
(capability protocol=`partitioned-dag-v2`, capability version=1) means precisely
this tuple, forever; a later local-cut tuple cannot reuse that capability claim.
V2 seal must equal the frozen deployment tuple/digest. Admission records the
same digest in its worker assignment witness; v2 grant scope, immutable private/
server bootstrap, signed ready/activation and every data/complete context bind it
again, directly or through the digest-verified graph. A mutable heartbeat feature
array is never authority. Old v2 Flow config requests only the none-cut tuple;
a future explicit tuple needs its own enrollment/worker support and fails here.
The deployment snapshot must name the exact v2 profile. New API prefix is
`/streams/v3/dags`; old `/streams/v2/dags` handles only its existing v1 profile.
New strict server/client types are named `StreamDAGV2SealRequest`, `StreamDAGV2Plan`,
`StreamDAGV2SubtaskBootstrap`, `StreamDAGV2RouteContext`, `StreamDAGV2Delivery`,
`StreamDAGV2ConsumerCompleteRequest`, `StreamDAGV2RouteOutcome` and
`StreamStateAccessV1`. These are agreed planning names if the Principal accepts
this revision, not classes already implemented.

The v2 seal has all current seal fields except profile literal v2, plus required
`features` and one required discriminated `state` contract per operator:
SOURCE=`{"representation":"NONE"}`; INLINE OPERATOR=
`{"representation":"INLINE_TURN_STATE"}`; JOURNALED OPERATOR has
`representation="JOURNALED_TURN_STATE_WITH_SNAPSHOTS"`, `owner_uid`,
`codec="strict-json-postimage-v1"`, `initial_state={}` and the derived sealed
quota object in section4. At least one journaled OPERATOR is required. SOURCE
cannot get an invented input, owner or batch. INLINE cannot be selected by
omitting/changing a journaled contract. Opaque JSON state rescaling is not
admitted; a future binding transfer preserves owner/batch IDs but needs its own
proved state-transfer profile and migration.

V1 classes and all old dumps/defaults remain untouched. V2 parser dispatches on
the exact profile before validating its own class; no conversion via a v1 dump,
generic `exclude_none`, prefix matching, or silent INLINE fallback. V1 routes
reject a stored v2 graph before mutation. New source/consumer/lifecycle/worker
routes delegate to existing native services through explicit version guards;
no second task or terminal engine is introduced. Worker v2 routes use
`/streams/v3/workers/{worker_id}/dags/...`; occupancy is shared across all exact
known physical profiles. New private child context is explicit v2; every current
exact-type dispatch, credential allowlist and event selector must enumerate it.
Signed Task events may retain their existing transport, bound to the v2 grant
scope/graph digest; they never gain unscoped platform credentials.

Canonical bytes are existing strict finite JSON: sorted keys, UTF-8,
`ensure_ascii=False`, `allow_nan=False`, compact separators. Canonical hashes
use `SHA256(domain + NUL + canonical(object))`. Exact new domains are
`prefect-stream-dag-v2`, `prefect-stream-dag-features-v1`,
`prefect-stream-state-contract-v1`, `prefect-stream-state-compatibility-v1`,
`prefect-stream-state-batch-v1`, `prefect-stream-state-chain-v1`,
`prefect-stream-state-restore-target-v1`, `prefect-stream-state-restore-ready-v1`.
For the batch, hash all canonical charged-packet fields except its result batch/chain hashes and the five physical author fields. Hash packet definitions are explicit in `quota-packet-v2.json`; physical author data affects quota accounting only. V1 domains/receipts/null/RETURN retain their old bytes. V2 still uses state-only
RETURN and output-bearing YIELD(null); OUTPUT/NO_OUTPUT is not introduced here.

## 2. Accepted logical identity and schema

The complete proposed PK/FK/Unique tuples are in `constraint-tuples-v2.json`.
All graph/task/stream/grant IDs are UUID; digests are lowercase SHA256 hex64;
progress/capacity counters are nonnegative signed BIGINT with overflow refused
before writes. `stream_value_JSON` stores postimages. Existing migration files
are immutable; add paired PostgreSQL/SQLite migrations, with PG17-only runtime
refusal before any state mutation on SQLite.

Owner ID is UUID7 allocated once at seal, uniquely bound to graph, original
assignment1, operator, `owner_uid`, original partition and original subtask.
OwnerInput ID is UUID5(owner, `state-input-v1/` + input-port UUID + `/` + sealed
edge-partition UUID). Batch ID is UUID5(owner, `state-batch-v1/` + OwnerInput UUID
+ `/` + original input-reference UUID + `/` + decimal business sequence).
Apply ordinal is allocated under owner lock and included immutably in the batch
digest; it is not inferred from an input sequence when multi-input arrives.
Physical grant/attempt/task author fields are audit evidence, not that logical
ID or logical batch hash. The original route receipt/output IDs remain authority.

The public wire `owner_uid` maps to stored `state_uid` by exact ASCII Identifier
bytes: no normalization or alias lookup. Digest construction is acyclic:
features -> pure state-contract -> raw graph -> allocate owner/input IDs ->
compatibility -> genesis. The raw seal contains no allocated owner IDs or
compatibility digest. `quota-packet-v2.json` spells each hash packet/order;
state-value digest remains the existing unprefixed SHA256(canonical(postimage)).
Accepted verification explicitly requires batch.after_state_digest to equal both
the recomputed postimage digest and receipt.managed_state_digest, plus
batch.route_outcome_digest==receipt.outcome_digest and the verified stored
outcome digest. These two equalities are checked even though not columns in the
new receipt FK. Missing one half of an accepted batch/receipt fails closed.

Only one journal owner maps to a subtask in020a. Its binding has assignment1 and
current subtask=original subtask. New physical grants do not change that row.
The separate binding is not permission for current opaque-state rescale.

Each accepted terminal writes one batch, owner frontier, OwnerInput accepted
business/log cursor, existing Consumer cursor, receipt/payload/references and
derived cache in **one outer routed transaction**. Direct postimage200 cannot
replace verification of batches1..200. Accepted route lookup and its exact batch
verification occur before new-turn version/reservation/capacity checks. Changed
accepted intent conflicts; old actor credentials still fail before lookup.

## 3. Current task phase, locks and frozen restoration target

Source trace: seal creates Pending (`stream_dags:843`), admission requires count0
and Pending/Scheduled (`:1403`). Grant creation increments physical epoch only
(`stream_subtasks:1421`). Confirmed Running process loss is proposed through
normal policy to AwaitingRetry/Scheduled with retained count n (`:1750`), or
Pending/Scheduled stays unchanged. Ordinary engine increments the counter when
entering RUNNING (`task_engine:695,1387`); the recorder admits c+1 only RUNNING
(`task_run_recorder:706`). Immediate local Retrying and scheduled local retry
reuse the same process/grant. They can increase task count without a new epoch.

V2 first bootstrap and replay take own TaskRun **UPDATE initially**, following
the existing child prefix; do not take SHARE and later upgrade. Before ready,
the exact stored bootstrap freezes `(task_id, flow_run_run_count, run_count=c,
state_id, state_type=PENDING|SCHEDULED, policy/task/code identity)` and the owner
frontier target. Pre-ready restore/ready require that tuple still current under
UPDATE, plus current grant/subtask/process/slot/control fences. Initial c0 with
physical epoch1 is valid; repeated pre-body replacements can have epoch>1,c0.
Never derive c from epoch.

Lock traces (all reloaded under their locks):

| Operation | Exact acquisition after locator reads |
| --- | --- |
| Bootstrap V2 | Flow/Authority/Graph SHARE -> assigned Worker SHARE -> own Subtask SHARE -> current Grant UPDATE -> own Slot SHARE -> unique Launch SHARE -> TaskRun UPDATE -> Binding SHARE -> Owner UPDATE -> OwnerInput UPDATE -> Cache UPDATE -> create/freeze bootstrap target. |
| Restore start/step or pre-ready resolve | Same existing child prefix with Grant SHARE, `ready=False, active=False` -> TaskRun UPDATE -> Binding SHARE -> Owner UPDATE -> OwnerInput UPDATE -> Cache UPDATE -> own RestoreAttempt UPDATE where applicable. Own Pending/Scheduled tuple must match frozen bootstrap. |
| Business open/read/complete | Child prefix Grant SHARE, ready/active checks -> TaskRun UPDATE (RUNNING,current request count) -> Binding SHARE -> Owner UPDATE -> OwnerInput UPDATE -> complete UUID-sorted partition UPDATE union -> Consumer UPDATE -> Reservation UPDATE -> Cache UPDATE. |
| Accepted resolve after ready | Child prefix/ready/active -> TaskRun UPDATE RUNNING -> request count equals current DB count -> Binding SHARE -> Owner UPDATE -> OwnerInput UPDATE -> immutable batch/receipt verification; no reservation/cache/data mutation. |

OwnerInput/Cache/Reservation are private state-owner rows. No restore/resolve
partition UPDATE is needed: accepted batches/receipts/references are immutable,
finite-profile online GC is absent, graph SHARE excludes terminal deletion and
own TaskRun UPDATE excludes another owner-turn cursor change. Restoring changes
only derived Cache; Consumer state mirror is filled by business open/read under
the complete business lock trace. No Consumer/partition row is mutated by a
restore step. Before business, compare accepted OwnerInput cursor with any
existing Consumer; mismatch fails closed. This premise must be tested against
all actual release paths; a future online GC changes this lock proof.

The bootstrap target is immutable `(owner,compatibility,ordinal,version,
state_digest,chain_root,cursor_vector)` hashed with the target domain. A current
grant gets one RestoreAttempt, target and genesis anchor. Each page verifies
contiguous batches and folds only platform JSON, recording derived verified
ordinal/root. READY verifies the full target and freezes a signed current-grant
restore-ready witness. Bootstrap-ready binds that digest. It does not call a
RUNNING/ready-required routed helper to become ready.

After ready, local retries use the **current** RUNNING TaskRun count and current
ConsumerBinding.attempt. Neither count==epoch nor count==bootstrap c+1 is a
business invariant. The frozen initial restore witness stays valid when the
cache/accepted frontier advances. A new restore target cannot start after ready
in020a; cache corruption is a typed refusal before new input, not an implicit
rerun. A separate current-owner restore lookup returns the frozen Ready witness
without rolling the latest cache back to that initial target.

State API POSTs, paired sync/async, all under `/streams/v3/dags/state/`:

| Suffix/type | Exact body fields beyond schema tag |
| --- | --- |
| `resolve`, `state.resolve.v1` | access(context,bootstrap_digest,current_task_run_count), owner_id,input_id,input_sequence,input_reference_id. Return accepted batch+route outcome; no caller-supplied old output or old capability. |
| `restores/start`, `state.restore.start.v1` | access, owner_id,target_digest,operation_id=UUID5(grant,`state-restore-v1/`+bootstrap_digest). Only pre-ready; returns the immutable target/genesis anchor/restore_id. |
| `restores/step`, `state.restore.step.v1` | access,restore_id,from_ordinal,expected_previous_chain_root,operation_id=UUID5(restore,`state-page-v1/`+decimal from). Sealed page caps; response verified_from/through,range_digest,state_digest,chain_root,ready_witness or explicit null. |
| `restores/resolve`, `state.restore.resolve.v1` | access,restore_id. Current pre-ready or RUNNING phase; returns frozen target/Ready outcome only, never restores again. |

Pre-ready access counter equals frozen c; post-ready access equals current DB
RUNNING count. Every context/owner/input is server-matched; SOURCE, INLINE,
foreign graph/owner and released horizon fail before mutation. Partial old pages
can be checked idempotently against immutable history while pre-ready; no page
may skip an ordinal, move verified progress back, or allocate per-page history.
Here READY has two explicit boundaries: materialization phase READY, and later
grant bootstrap-ready. Once the final step commits materialization READY,
step/start return `STATE_RESTORE_FINISHED` (409, retryable=false); the SDK resolves
the frozen Ready witness using `restores/resolve` while still Pending/current
before it signs bootstrap-ready. This is the recovery path for a lost final-step
HTTP ACK. Nonfinal lost-page ACK replays only while MATERIALIZING. After grant
bootstrap-ready, restore lookup uses the current RUNNING request count; it never
starts a new target or resets a cache already advanced by business turns.

## 4. Exact finite state quota and conversion

From each journaled operator's already sealed limits derive required v2 quota:
`max_batches=max_records`, `max_retained_bytes=max_bytes`,
`max_postimage_bytes=max_state_bytes`, `max_batch_metadata_bytes=4096`.
The canonical charged packet field list is in `quota-packet-v2.json`.
It contains no unbounded UID/key/value outside the postimage: IDs36, digests64,
ASCII UID64, protocol/kind literals and signed counters19 digits. Its metadata
bound is independently calculated there and must be falsified by maximum-shape
encoding before code is enabled. Charge exactly the canonical packet length;
the stored `charged_bytes` field is outside that packet to avoid self-length
recursion. These are logical encoded quotas, not exact PostgreSQL disk-size or
latency/throughput promises.

Let S=max_postimage_bytes, M=4096, R=S+M. Seal rejects before task metadata if
max_retained_bytes<R. State page caps are
`min(max_prefix_records,1000)` and `min(max_prefix_bytes,1048576)`; the latter
must be >=R at seal. Thus even a maximum batch fits one bounded page; there is
no impossible-progress restore configuration hidden behind endless polling.

Before exposing a fresh input to the body, owner/partition/consumer locks bind
its exact input coordinate and persist one HELD Reservation with R bytes and
one batch credit. Require retained_count+reserved_count<=max_batches and
retained_bytes+reserved_bytes<=max_retained_bytes. Reservation UUID5(owner,
`state-reserve-v1/`+input_id+`/`+input_reference_id+`/`+sequence) excludes physical
attempt. Duplicate read and local/physical replacement of incomplete200 reuse
that same held coordinate and charge. New delivery/attempt audit may change,
but its before-version/root and logical charge do not.

Successful terminal acceptance first checks for a known batch/outcome, then for
a fresh turn converts HELD once: retained_count+=1, retained_bytes+=actual L,
reserved_count-=1, reserved_bytes-=R; `unused=R-L` is refunded exactly once by
clearing HELD in the same transaction. Batch stores the reservation ID UNIQUE,
without a foreign key to the mutable current-reservation slot. Rollback restores
all counters and HELD. No release/re-reserve cycle occurs merely because a grant
or task attempt changed. Body failure retains it for the same incomplete input;
verified terminal graph teardown removes it under the normal lifecycle fence.
There is no eviction/online GC in020a. Exhaustion rejects a fresh delivery with
non-retryable `STATE_JOURNAL_CAPACITY_EXHAUSTED` before body invocation; it does
not wait for a nonexistent reclaimer. Oversize user postimage is a failed
unaccepted turn, not a partial state acceptance.

Separate derived storage is bounded: one OwnerCache postimage<=S plus an
optional Consumer mirror<=S; one Reservation metadata row<=4096; at most64
RestoreAttempt metadata rows<=8192 each (one per physical grant), no per-page
row growth. A page folds sequentially with one <=S postimage and <=page-byte-cap
input. Authoritative batches remain in retained quota. The bounded metadata
rows are fixed/control metadata, not a concealed per-business-turn log. No
snapshot/cache file/object store or extra snapshot quota exists in020a.

## 5. Paired CHECK migration and all known release surfaces

`profile-check-matrix-v1.json` records the exact11 current ORM CHECKs and their
v2-only added branches. Existing lifecycle identity, graph control/profile,
operator, subtask, capability, port, edge, edge partition, slot and deployment
bounds stay identical for old profiles. Four additive unique tuples alone are
not a complete v2 migration. The new paired migration replaces the named CHECKs;
historical migration files remain unchanged.

`known-dispatch-inventory-v1.json` records82 literal occurrences across27 source
files, not an exhaustive semantic/conflict scan. Every applicable branch must
choose exactv2 services or refuse. In particular `NativeStreamStore.release`
currently recognizes literalv1 DAG then falls into legacy cleanup: v2 must
explicitly use versioned verified DAG retirement for both normal and
`verified_server` paths. Generic legacy data mutators, fixed/linear release,
Flow/Task/event dispatch, deployment/runner bootstrap and shared occupancy need
negative no-bypass traces, not only old DAG-router refusal.

Under existing verified Graph UPDATE and process-closure fences, v2 normal,
startup-abort, cancel/failure and zero-process-preview cleanup removes:
Cache -> RestoreAttempt -> Reservation -> StateBatch -> PartitionLog ->
OwnerInput -> Binding -> Owner, **before any existing REFERENCE record delete**.
Then preserve the current reference/completion/receipt/consumer/payload order.
PartitionLog's immediate RESTRICT record FK would block deletion if omitted;
StateBatch must be removed before its pinned log rows. A real negative fixture
must demonstrate that omission fails, then all real terminal paths close using
proper order. Restore's no-partition-lock proof is invalid unless every actual
delete path has this graph fence and020a continues to forbid online GC.

## 6. Pre-code falsification gates and scope boundary

`falsification-plan-v2.json` enumerates exact future tests and expected failure
oracles. None has been executed by this preparation. Mandatory blockers before
enabling v2 are: wire/golden/feature rejection; paired migration/negative
composite-FK round trips; real PG lock order/interleavings; exact pre-start and
same-grant retry traces; maximum metadata packet calculation; reserve/convert/
rollback/replay-at-full-quota; and native current-owner genesis fold with
missing151/accepted200 lost ACK. Any failure returns its specific choice to
review rather than increasing time budgets or weakening an old profile.

NI2-024a still owns authentic ordered DATA/BARRIER and durable pre-cut intent
fencing/abort/pins plus immutable participant/input manifest. It is a separate
prerequisite for020b, not implemented or approved as complete by this package.
Cached state, journal markers and test fixture seeds cannot be its anchor.
No full global cuts, multi-input mailbox, continuous GC, source managed state,
opaque-state rescale or external multi-sink atomicity is promised here.
