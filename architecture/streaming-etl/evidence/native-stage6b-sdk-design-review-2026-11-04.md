# N1.6b SDK design review: bounded mixed finite and streaming flow

Status: planning review only. N1.6a supplies durable finite authority and receipts; N1.6b is not implemented or accepted by this document.

## Decision

The first runtime slice can safely support exactly one direct ordinary finite PREFIX task, one linear native source/operator graph, and one direct ordinary finite TAIL task in the same flow. It should add a private finite-task execution path to the existing Prefect task engine. It must not relax `submit`, `map`, deferred calls, nesting, arbitrary finite chains, branching, or production worker-launch automation.

The finite body remains an ordinary sync or async `Task` body. The native layer supplies a stable TaskRun, admission and receipt around that body; it does not introduce another executor or per-record TaskRuns.

## Existing integration points

- `src/prefect/tasks.py::_require_unenrolled_task_entrypoint` currently rejects every ordinary task entrypoint in an enrolled flow. Relax only `Task.__call__` through a finite-aware dispatcher. Keep `submit`, `map`, `delay` / `apply_async`, nested calls, and unsupported runners fail-closed.
- `src/prefect/_internal/stream_reconstruction.py::StreamReconstruction` currently registers only stream manifests. Extend it with immutable finite witnesses and phases; do not infer finite identity from call order alone.
- `src/prefect/task_runners.py::ThreadPoolTaskRunner.submit` already holds reconstructed stream engines behind the graph barrier. A finite TaskRun should use the same immutable execution context and registered manifest digest, but PREFIX must be admitted before the stream graph is sealed and TAIL only after durable EOF.
- `src/prefect/task_engine.py::{TaskRunEngine,AsyncTaskRunEngine}.call_task_fn` is the required receipt boundary. The engine must commit the finite receipt after obtaining and validating the result and before `handle_success` proposes TaskRun Completed. An outer future callback is too late.
- `src/prefect/streams.py::Stream.wait` seals and waits for terminal producer state. `StreamCollection.resolve` may return after `limit` records while the stream is still OPEN; collection completion is not an EOF witness.
- `src/prefect/server/models/finite_submissions.py` and the `/streams/v1/finite/*` API remain the authority for TaskRun identity, receipt, retry reconciliation, tail eligibility, quotas, and replay.

## Bounded runtime contract

1. A finite call in an enrolled flow requires an explicit `submission_key`, explicit task and flow versions, supported retry policy, top-level deterministic code identity, and strict finite JSON parameters.
2. PREFIX is admitted only before the first stream declaration. TAIL is admitted only after the graph has been sealed and every leaf stream is durably COMPLETED with no in-flight delivery. The server check is authoritative.
3. Reservation returns the canonical TaskRun ID and manifest digest. The SDK reads that exact Pending or recovered TaskRun and registers its digest in `StreamExecutionContext`; it never creates a random local TaskRun.
4. The ordinary sync or async task body runs through the normal TaskRun engine. Its result is converted to strict finite JSON and bounded to 64 KiB, then `complete_stream_finite_task` commits the immutable receipt. Only after that commit may `handle_success` publish TaskRun Completed.
5. If a receipt already exists, the body is skipped and its result is returned from the receipt. The same TaskRun ID, run-count authority, manifest, and result are retained. A durable receipt wins over late Failed, Scheduled, or new Running state proposals.
6. Prefix failure before a receipt uses normal retries on the same TaskRun. Exhaustion fails the flow before stream declarations and before DATA. Tail failure retries the same TaskRun after EOF and must not rerun the stream graph.
7. Cached ordinary results, if retained in the slice, still pass through JSON validation and receipt commit. Otherwise cache/result persistence must be explicitly rejected before admission; it cannot bypass the receipt.
8. `return_state=True` needs an explicit contract. Prefer returning the exact receipt-backed Completed state. If that is not implemented in the first slice, reject it before reservation rather than silently changing ordinary-task behavior.

## Process-loss cuts

The existing server model supports these cuts without restoring Python frames:

- receipt committed, response lost: replay the same operation and receive the same receipt;
- process exits after receipt commit but before TaskRun Completed: recovery reconciles the same TaskRun to Completed and skips the user body;
- source/operator process loss after document 200: prefix receipt is reused, the first incomplete document is 201, and stream TaskRun/channel identities remain unchanged;
- tail receipt committed before metadata completion: recovery skips all completed stream bodies and the tail body, then reconciles the same tail TaskRun.

The recovery path has one confirmed SDK gap and one plan-discovery question. It does **not** currently prove a server need for staged recovery:

1. **Prefix dies before receipt: server path is viable, SDK path is missing.** `takeover_execution` accepts the durable finite-prefix anchor even when no stream manifest exists. `prepare_recovery` can then reconcile only that finite PREFIX in generation 2 and set `prepared_generation`. A normal FlowRun Running transition moves authority from RECOVERING to ACTIVE through `apply_flow_lifecycle_result`. After the retried PREFIX commits its receipt, stream reservations can be created and the first `seal_manifest` remains legal because it accepts ACTIVE authority with `manifest_set_digest is None`; it verifies the durable prefix receipt and Pending stream TaskRuns. Thus the one-shot recovery operation covers the old prefix, while the later seal is a distinct operation for the newly declared graph. `tests/server/models/test_finite_submissions.py::test_prefix_only_takeover_prepares_same_task_retry` already proves the finite-only prepare half. N1.6b still needs a real end-to-end control proving prepare -> Running -> same-prefix retry/receipt -> new stream declarations -> first generation-2 seal. Current `StreamReconstruction.seal` chooses prepare solely from `generation > 1`, so it cannot drive this valid protocol today; it must choose from durable sealed/unsealed authority state instead.
2. **Existing tail manifest: confirmed SDK plan-discovery gap, not yet a server-protocol defect.** If the previous generation already sealed the stream graph and reserved TAIL, `prepare_recovery` correctly requires every stored finite witness in its immutable request. Recovery code reaches graph preparation before flow code reaches the tail call, so the current in-memory registry does not know that witness. Supply it from a durable supervisor/SDK registry or add a read-only finite-plan lookup, then require the later tail declaration to match code, parameters, and policy exactly. An existing TAIL implies durable EOF, so drift refusal cannot admit new DATA.

Do not weaken immutable plan comparison. Add server protocol only if a real control demonstrates that durable plan discovery and the existing prepare/seal transitions are insufficient.

## Tail and collection rules

- Resolving `collect(limit=N)` does not authorize TAIL while the stream remains OPEN. TAIL reservation must fail with a named error before creating a tail TaskRun or running its body.
- The bounded supported graph must establish terminal leaf completion with `Stream.wait()` or an equivalent durable EOF check before TAIL reservation.
- A finite call after the first stream declaration but before seal is an unsupported mid-graph call. Reject it locally before reservation while workers are still held, which also guarantees zero DATA for this failure.
- A tail attempt after some DATA but before EOF can only promise “no tail TaskRun and no tail body”; it cannot truthfully promise zero stream DATA.

## Identity, parameters, and fencing

- Prefix and tail use the server-issued TaskRun ID on every retry and generation. Existing engine retry initialization from retained `run_count` prevents retry-budget reset.
- Current generation/holder credentials and the registered manifest digest cover TaskRun events. Old-generation RPCs and signed events must remain fenced after takeover.
- Resolve a `StreamCollection` before finite parameter canonicalization. Reject unresolved Futures, Stream handles, arbitrary objects, non-finite numbers, oversized values, and unsupported nesting before finite reservation.
- `SourceContext` and stream cursors remain independent of finite results. No finite task may mutate per-record cursor authority.
- External side-effect idempotency remains the task author's responsibility; a receipt prevents platform re-execution only after its durable commit.

## Acceptance cuts

All cuts use real Prefect HTTP APIs and PostgreSQL stream storage unless a control is expressly a pure SDK test.

1. Sync flow with sync PREFIX/TAIL and async flow with async PREFIX/TAIL; ordinary finite tasks outside an enrolled flow retain current behavior.
2. Prefix receipt commit followed by lost HTTP response and API restart: exact replay, same TaskRun ID/result, body count one.
3. `os._exit` after prefix receipt and before TaskRun Completed: a new process uses the same TaskRun, skips the prefix body, and starts the graph.
4. Process loss after source position 200: resume at 201 with unchanged prefix/source/operator/tail IDs and two unchanged channels; no TaskRun per record.
5. Tail receipt commit followed by process loss before TaskRun Completed: graph bodies remain skipped, tail result is replayed, body count one.
6. Prefix user failure retries on the same TaskRun; exhausted retries yield zero stream DATA. Tail user failure retries on the same TaskRun without rerunning streams.
7. `collect(limit)` while the stream is OPEN followed by TAIL: named refusal, no tail TaskRun/body. After durable EOF, the exact tail succeeds.
8. Mid-graph finite call, second PREFIX/TAIL, nesting, `submit`, `map`, deferred calls, missing key/version, manifest drift, non-JSON result, and oversize result all fail at the declared boundary with no unsafe mutation.
9. Current-holder and live old-holder attempts race takeover and receipt completion: only the current generation mutates metadata; stale event rows are absent.
10. Flow Completed arriving before the final TaskRun Completed event is reconciled from EOF/receipt; a current-generation late Completed event is accepted and stale terminal events are dropped.
11. Actual API restart around reserve and complete, immutable operation replay, concurrent exact reservations, and conflicting replay refusal.
12. A bounded 500-document profile proves one prefix TaskRun, one source TaskRun, one operator TaskRun, one tail TaskRun, two channels, exact counts/checksums, and bounded receipts. It is not a throughput or production HA claim.
13. Client-package and sync/async DTO parity; execution credentials and live clients are absent from task parameters and persisted manifests.
14. A pre-receipt PREFIX crash is exercised through finite-only generation-2 prepare, normal retry on the same TaskRun, durable receipt, new stream declarations, and the first generation-2 seal. The acceptance must show that the SDK selected prepare versus seal from durable graph state rather than generation number alone.

## Scope boundary

A passing N1.6b proves a bounded mixed-flow runtime slice over the already accepted native storage and recovery authority. It does not prove arbitrary mixed DAGs, branching/fan-out, online retention, unbounded sources, cross-deployment recovery, production launcher automation, or recovery of Python frames. N1.6a remains accepted independently; this planning review does not change its evidence or claim that N1.6b exists.

## Review provenance and delivery state

This is planning evidence for [ADR-0037](../adr/ADR-0037-mixed-finite-and-streaming-tasks.md), [NI1-054…056](../todo.md) and [N1.6b](../sprint_board.md). It adds no implemented scope or earned percentage. The server-state trace was subsequently tested by the [real HTTP/PostgreSQL adjunct](native-stage6b-prefix-recovery-adjunct-2026-11-04.json): prefix-only recovery, same-task retry, first generation-2 seal, one DATA record, EOF, TAIL and Flow success all passed. This low-level protocol control does not replace the required full mixed SDK process matrix.

The [initial review](native-stage6b-sdk-design-review-initial-2026-11-04.txt) is retained verbatim. Its claim that staged server recovery was required was withdrawn after implementation review: the existing first-seal route accepts an active recovered generation with no sealed graph. The corrected SDK selection and stored-tail discovery must be proved on the actual HTTP/PostgreSQL implementation before N1.6b acceptance.

The [traced review snapshot](native-stage6b-sdk-design-review-traced-2026-11-04.txt) is preserved byte-for-byte. [Planning evidence publication](native-stage6b-planning-evidence-2026-11-04.json) maps the adjunct’s temporary review paths to their durable archived copies.
