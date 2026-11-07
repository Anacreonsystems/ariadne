# NI2-020a contract finalization: Principal review probes

Planning only on immutable `d906ceca414baeac825ce472e0b78cd1dcae3363`.
No code authorization, runtime/DB operation, accepted criterion or readiness credit.
This supplements the closed v2 handoff; it does not replace it or change ADR44.
Exact wire/FK/quota choices belong in the peer's new finalization artifact.

The source-derived attempt contract is concrete:

* `stream_dag_executions.py:1911` currently reads the own TaskRun under SHARE
  while constructing and retaining its complete immutable bootstrap witness.
  A new v2 path requiring UPDATE must acquire UPDATE initially; replaying a
  stored bootstrap must not create a SHARE-to-UPDATE upgrade later.
* `streams.py:291-350` takes TaskRun UPDATE but its current routing validator
  requires RUNNING/current attempt and rejects Pending/Scheduled. A distinct
  narrow pre-ready predicate must preserve TaskRun UPDATE and the child fences.
* Freeze the actual pre-start TaskRun counter, state ID/type and restore target
  under that lock. Pending counter0 is legal. Scheduled retry counter n is
  distinct from a process epoch. A failed startup may replace a process before
  any business RUNNING event; epoch therefore need not equal counter+1.
* `task_engine.py:695,1387` increments counter for every new Running state.
  `handle_retry:770-811` uses an immediate Retrying state or AwaitingRetry;
  `wait_until_ready:1035,1733` changes Scheduled to Retrying in the same engine.
  These paths do not acquire a new physical grant. The recorder at
  `server/services/task_run_recorder.py:706` admits counter+1 only for RUNNING,
  within the retry-policy bound. Frozen restore readiness cannot be validated
  by permanently requiring current counter=bootstrap counter+1.
* Pre-ready restore can require the frozen counter/state plus exact current
  grant. Post-ready business and accepted-result lookup instead require the
  current DB TaskRun counter and current ConsumerBinding/owner fences. The
  frozen initial-ready witness and advancing accepted/cache frontier are
  separate authorities.

Mandatory pre-code falsifiers for that trace:

1. Pending counter0 with a replacement physical epoch>1 restores before any
   body and does not fail a guessed epoch/counter equality.
2. Scheduled counter n restores before GO; ordinary engine Running n+1 then
   passes current business authorization. A stale counter n request fails.
3. A same-grant immediate local Retrying transition increases the DB counter;
   the old frozen restore-ready witness remains valid while stale business
   binding fails. Repeat with a delayed Scheduled retry.
4. First bootstrap and stored-bootstrap replay, restore start/step/ready,
   business complete, replacement/control and finalizers have one documented
   lock order. An interleaving must not need a late parent/TaskRun upgrade.

Quota accounting needs an exact table/formula, then these concrete probes:

* Grant capacity before exposing a fresh body. Re-reading the same incomplete
  logical coordinate and replacing its physical owner retain one reservation;
  neither path charges twice or advances an accepted ordinal.
* A fresh terminal transaction converts exactly that reservation to immutable
  batch count/bytes and current materialization usage, together with the
  existing receipt/completion/output/cursor transaction. Fault each flush and
  require complete rollback of the accepted set and accounting change.
* At the exact retained quota boundary, resolve/replay a prior accepted turn
  before fresh capacity checks: identical batch/receipt/output is returned,
  with no extra bytes, ordinal, body or cursor movement. The next fresh body
  must be refused with a typed finite-capacity outcome before invocation.
* Refund only on the declared confirmed abort/finalization path. A process
  death or lost ACK alone must not refund a reusable incomplete reservation.
* Bound canonical encoded metadata, postimage bytes, active reservations,
  restore attempts, temporary materialization/cache bytes, page record count
  and page bytes. Distinguish separately counted storage from existing channel
  data quotas; no silent assumption that one max_bytes counter covers both.
* There is no online journal reclamation in020a. Exhaustion cannot wait for a
  nonexistent reclaimer or delete genesis-to-frontier history. No COMPLETE,
  snapshot anchor or barrier capability is admitted before genuine024a.

Other exact contract checks remain required in the peer artifact:

* Separate required v2 profile/DTO/canonical bytes and explicit capability
  refusal before metadata creation. Old v1 graph/request/receipt byte fixtures
  remain exact; the old state-only RETURN and yielded null remain distinct.
* SOURCE explicitly has no state owner, owner input, batches or state APIs.
  Only declared journaled OPERATORs have those rows; INLINE is explicit and
  cannot be an omitted-field fallback for journaled state.
* Composite constraints bind owner, original assignment/input lineage,
  exact stable subscriber, input sequence, kind and semantic digest to the
  existing immutable receipt witness. A valid but unrelated receipt ID must
  fail. Physical author/grant/count are audit/fencing columns, not the logical
  accepted-outcome key. Future rescale is not accepted by an opaque JSON map.
* Fold verifies every ordinal, state/root/digest transition and accepted
  receipt link, including RETURN turns. Removing/corrupting151 fails before
  activation/body; a final postimage200 cannot hide the gap.
*020a's genesis-only evidence is not snapshot100+101…200 acceptance.020b still
  needs a legal024a local cut and the unchanged native A44-01 oracle.

All grill-me/spec-first variants are excluded. Conflict scanning remains known
incomplete. These are planning falsifiers, not tests that have been executed.
