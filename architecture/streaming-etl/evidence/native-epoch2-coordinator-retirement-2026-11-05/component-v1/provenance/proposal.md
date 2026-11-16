# Terminal DAG coordinator retirement: narrow integration correction

Current source f416: NativeProcessStarter.start observes final Flow after its owned waitpid, records terminal evidence, then calls the unchanged release_stream_flow RPC. NativeStreamStore._flow correctly refuses direct legacy DAG data access. Relaxing that guard or reusing the legacy graph mutation block would corrupt canonical RETIRED grant history.

## Owned scope

- src/prefect/server/models/streams.py: explicit immutable-snapshot DAG dispatch at release_flow entry; legacy branch and _flow data guard remain unchanged.
- src/prefect/server/models/stream_dag_executions.py: dedicated coordinator-only release helper, returning0 newly released payload streams.
- tests/server/models/test_stream_dag_executions.py: real FastAPI/PG release controls with existing ready_deployed_region/production retirement helpers and ordinary Flow state transitions.
- tests/test_native_stream_dag_execution_http.py: permanent real SDK/two-ProcessWorker/OS helper additionally waits for coordinator release and inspects terminal worker health.

## Required proof and mutation boundary

Use current persisted native snapshot profile, not mutable user code/job variables. Acquire canonical Flow/authority/graph fences. Validate current execution generation/holder/run_count, immutable declaration digest and matching native launch/snapshot bindings. Current Flow must be final. Existing graph requires RELEASED plus exact current FINALIZE/stateCOMPLETED journal, digest/owner bindings, compatible control receipt and all admitted physical process closure/own tag allocations retired. Preview with exact PREVIEW_ABORT/FINALIZE and zero births is valid. Successful graph followed by Failed/Cancelled ordinary Python tail remains valid; do not equate graph outcome with parent outcome.

For no graph, allow only the separate authenticated pre-seal case with zero native submissions, subtasks, grants, partitions/data, allocations/history and no sealed-manifest witness. This is not fallback for a missing/corrupt proof of an existing graph. Preserve existing pre-enrollment and accepted unstarted-retirement semantics; actual red controls will document them rather than invent blanket missing-metadata refusal.

Atomically close only current coordinator authority/snapshot/native-launch metadata; do not update DAG graph, grants, launch history, receipts, task states, payloads or concurrency counters. Scope own tag allocations; other active flows sharing a tag are permitted. Replay revalidates the same durable proof and returns0 without rewriting history.

Internal verified_server=True cleanup (native verified retirement/cancellation) must use the same DAG-only path and validate an accepted matching-current-generation native control proof; a bare boolean is not terminal authority. Ordinary holder-authenticated release uses existing execution credentials. Missing, stale or corrupt proofs fail before mutation.

DAG execution imports streams, so the release entry may perform the narrowly documented deferred import needed to break this existing cycle. No public endpoint, request/response shape, runner or SDK changes are needed.

## Verification sequence

1. Baseline real HTTP/PG terminal graph+Flow release returns the current409 and yields actual RED. Also run the strengthened eight-document native normal profile to witness the actual NativeStarter terminal cleanup failure, before product edits.
2. Apply the narrow helper/dispatch correction. Run real HTTP/PG normal/failed/cancelled/preview/empty pre-seal/replay plus active parent, corrupt/missing journal, stale holder/generation/control and disjoint active-allocation negatives.
3. Run real native normal sync/async, failure, cancellation, preview and pre-seal failure with strict coordinator metadata/worker health. Preserve legacy bounded-linear/fixed-P2 release and direct DAG data refusals.
4. Native strict typing and applicable format/commit checks. No full5000 or complete epoch acceptance is claimed by this component.
