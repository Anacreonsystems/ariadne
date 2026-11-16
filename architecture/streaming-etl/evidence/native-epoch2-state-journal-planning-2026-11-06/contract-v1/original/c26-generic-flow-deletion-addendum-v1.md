# C26: generic Flow deletion and the new v2 state horizon

Status: **planning addendum / future implementation falsifier**. This is within
the already approved ADR44 state/lifecycle scope. It is not an observed runtime
bypass, a reproduced HTTP500, code, a new guarantee for old profiles, or an
accepted criterion. The frozen eight-artifact contract-v2 review is unchanged.

## Source facts

Immutable d906 `server/models/flow_runs.py:531` and `:562` read selected runs,
clean deployment concurrency slots, then issue DELETE FlowRun. Those functions
do not explicitly acquire the native Graph/process-closure fence. Generic API
`server/api/flow_runs.py:662` and `:703` call them without a NativeStreamError
mapping; log-cleanup jobs are queued only after a successful delete transaction.
Existing cascade/RESTRICT behavior may already block deletion; its actual
runtime result was not tested by this preparation. These source facts therefore
identify a seam that the new state/log RESTRICT roots must cover, not proof of a
current native defect.

## Exact v2-only guard choice

Before **any** concurrency cleanup, log cleanup, DELETE or state mutation:

1. Lock all selected existing FlowRun rows UPDATE in UUID order, then determine
   native profile markers under those locks. This avoids a locator-to-delete
   race. The ordinary non-v2 policy remains the same; earlier serialization does
   not impose a new native state rule on an old profile.
2. For selected v2 candidates lock Authority rows UPDATE in Flow UUID order,
   Graph rows UPDATE in UUID order where present, then the immutable deployment
   snapshot under a shared/refreshed read. A run is v2 if **either** its snapshot
   or graph names `partitioned-dag-v2`; conflicting markers fail closed.
3. Call a **pure validation** seam for already released native retirement.
   Do not call mutating `release_flow(verified_server=True)` to manufacture
   readiness, infer death, discard state, or repair a graph during admin delete.
   No execution capability is synthesized from persisted author data.
4. If a graph exists, require RELEASED, its exact stored terminal proof, released
   coordinator authority/snapshot/launch identity and verified closed physical
   journals, plus zero StateOwner/Cache/Restore/Reservation/Batch/PartitionLog
   roots. Use the same retirement proof facts as the versioned native service,
   under the current locked Flow attempt, rather than a bare state string.
5. If the graph does not exist, accept only the existing verified **pre-seal**
   retirement case: snapshot/authority/coordinator launch are RELEASED under the
   exact stored zero-DAG-graph/process proof and no DAG physical births/state/log
   roots exist. Requiring an impossible Graph.RELEASED row is not the policy.
6. A live, corrupt, unverified or still pinned candidate returns
   `NATIVE_STREAM_DELETE_NOT_READY`, HTTP409, `retryable=false`, with no side
   effect. A mismatched identity uses the existing typed identity conflict.
   Both DELETE `/flow_runs/{id}` and POST `/flow_runs/bulk_delete` explicitly
   catch/map these model errors. Leaving an uncaught model exception would not
   establish this API contract.
7. The bulk route validates **all selected runs before any cleanup**. A mixed
   batch containing one denied v2 run is atomically refused; it must not delete
   an ordinary sibling first or free any selected run's deployment slot.
   Once all guards pass, preserve the existing deletion and post-commit log job
   behavior. Released, verified, already-clean v2 runs remain deletable.

These guards do not cancel live workers implicitly. They preserve the existing
native cancel/wait/verified-release path before deleting its durable horizon.
The new state finalizer must first follow Batch -> PartitionLog -> existing
record cleanup order from contract-v2. No admin deletion substitutes for that
verified terminal path.

## Concrete C26 falsification plan — not executed

- On an owned real PG17 v2 graph with live accepted state and physical children,
  call each actual generic HTTP delete route. Require typed409 and exact
  unchanged Flow/task/state/log/cursor/output/slot sets; no log-cleanup job and
  no integrity exception/HTTP500 is an acceptable substitute.
- Select live v2 plus ordinary/released siblings through actual bulk filters.
  Require atomic refusal before any selected concurrency cleanup or deletion.
- Hold a real restore transaction's Graph SHARE while delete requests the
  ordered UPDATE prefix; capture the actual wait and prove no state/log/data
  deletion can pass that transaction. Then require denial while still live.
- Repeat with an absent graph in legitimate pre-seal-released and unreleased
  coordinator cases: only the fully verified retired case is deletable.
- After actual graph/process/coordinator release and the correct state/log
  teardown, single and bulk delete succeed through the normal API. Old profile
  ordinary deletion regressions preserve their previous policy/statuses.
- Refuse forged/missing retirement witness or snapshot/graph marker conflict;
  do not obtain success by bypassing the state FK, freeing slots early, or
  marking an incomplete graph released.

The precise existing validator extraction and all new DDL/API code are later
implementation work after NI2-014. No code GO, DB action, accepted ADR status,
readiness increase or old-profile policy change follows from this addendum.
