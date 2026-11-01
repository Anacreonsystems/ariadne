# ADR-0033: Native durable stream storage

- Status: `accepted`
- Date: 2026-11-01
- Accepted: 2026-11-01, N1.2 bounded storage-only scope
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md), Stage 2
- Owners: ETL Architect / HighLoad Principal, server/database maintainers
- Depends on: [ADR-0032](ADR-0032-native-task-stream-contract.md),
  [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)
- Prototype input: [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md),
  [ADR-0020](ADR-0020-bounded-metadata-and-idempotency-horizon.md)
- Delivery: [todo](../todo.md), [sprint board](../sprint_board.md)

## Problem

The native Task/TaskRun engine exists, but its accepted Stage 1 storage fixture
loads a retained prototype outside the product. Native progress needs platform
tables, migrations and transaction ownership before the SDK can use a server
backend. A process-local queue or an ORM `create_all` test cannot establish
that contract.

The current `read_prefix` contract does not register durable reader pins.
Deleting an active prefix could therefore invalidate an ordinary finite
dependency. An indefinitely running stream with online reclamation is a
different capability from a bounded durable store with explicit capacity
refusal. They must not be reported as equivalent.

## Theory

One PostgreSQL transaction can accept a terminal event by storing its output
and immutable receipt, saving managed state, advancing the logical consumer
cursor and clearing the in-flight delivery together. Row locks on stream
headers serialize append against EOF and owner takeover against completion.
The durable receipt resolves an accepted request whose response was lost;
an incomplete request leaves the cursor at the same input.

This is a native implementation hypothesis, independently tested below. The
successful prototype justifies it but does not substitute for these migrations
or implementation tests.

## Proposed solution and agreement

### Delivery boundary

N1.2 implements this storage-only ADR: native ORM and paired migrations,
server-owned storage transactions, typed internal bindings/deliveries,
capacity refusal, and whole-flow release. N1.3 will define the HTTP/client
adapter and cross-process HTTP acceptance in [ADR-0034](ADR-0034-native-stream-http-adapter.md). Stage 2 remains
in progress until both slices pass. Source SDK, automatic runtime selection,
worker replacement, HA, Cloud tenant isolation, online prefix GC and deployment
acceptance remain outside this ADR.
The internal store assumes trusted server database access. Its identity checks
are not remote authentication; HTTP/tenant authorization and rejected-start
reservation cleanup belong to the pending transport/lifecycle decisions.

No TaskRun or FlowRun state is changed by per-record storage operations.
Petya and Anna remain ordinary TaskRuns; a retry of Anna uses the same logical
consumer. Existing orchestration remains the lifecycle authority.

### Models and versioned protocol

Four native tables belong to the Prefect database:

- `stream_partition`: FlowRun scope, immutable capacity limits, producer
  identity/attempt/epoch, ordered tail and counters, terminal marker and
  persistent release witness.
- `stream_record`: dense output sequence, producer position, canonical JSON
  value, digest and byte count. Producer position and output sequence are
  separately unique within a stream; RETURN can advance the former alone.
- `stream_consumer`: exact input/output/TaskRun binding, owner attempt/epoch,
  completed prefix, managed state and at most one durable in-flight delivery.
- `stream_completion`: one immutable terminal intent digest and output
  coordinate per consumer/input. RETURN has no output envelope.

Bindings and delivery tokens are frozen, wire-safe server DTOs. Owner epochs
are issued by the store. Owner IDs are fencing coordinates, not credentials.
Claims validate an existing TaskRun belonging to the stream's FlowRun.
The producer UUID is retained even if asynchronously recorded TaskRun metadata
is later removed; absence never permits rebinding it to another task.

The store accepts protocol version 1 and canonical JSON only. Record values
and managed state use PostgreSQL JSON, preserving signed zero and JSON null;
JSONB normalization is unsuitable for this round-trip contract. It rejects
unsupported versions, non-JSON/non-finite values and oversized records/state
before mutation. Large values must be durable references represented in JSON.
No server-side pickle deserialization is introduced.
The rejected [JSONB alternative](ADR-0094-jsonb-canonical-stream-values.md)
records the executed fidelity failure. Canonical JSON denotes this versioned
application codec, not RFC 8785 number normalization.

### Transactions, ordering and recovery

All public store methods own their transaction and return after commit.
Claims and record operations validate the current durable TaskRun attempt and
state while holding its row lock before acquiring stream headers. RUNNING is
required for append/read/complete and successful finish; the current failed,
cancelled or crashed attempt can only finish with a failure marker. A forged
future attempt cannot poison ownership; a previous attempt is fenced even
before its replacement claims the stream. This may contend with state events;
no high-load acceptance is inferred.
Every operation needing multiple stream headers locks them in UUID order,
then locks the consumer. No path reverses that order. Flow release first locks
the FlowRun, then its ordered stream headers.

An initial claim binds the exact producer or consumer identity. An identical
owner/attempt request is idempotent; only a strictly newer task attempt can
replace an owner and obtain a higher epoch. Old bindings cannot write, even
when an old completion intent happens to match a stored receipt.

Read durably records the next input and a delivery UUID. Repeating a read for
the same owner returns that delivery. A newer owner receives the same
incomplete input with its own fencing coordinates. Completion checks the exact
token and canonical intent. An identical completion retry returns its receipt;
different intent for the same accepted input is rejected.

YIELD atomically inserts one output and receipt, updates counters and state,
advances `completed_through` and clears delivery. RETURN performs the same
transaction without an output record. A quota or SQL failure leaves all of
those changes rolled back. A lost response after commit leaves them all
durable and the next owner starts at the following input.

Closing a source freezes its last sequence under the append lock. Successful
operator finish additionally requires its exact input to be COMPLETED, fully
drained and without an in-flight record. Exact terminal retries succeed;
conflicting markers and appends after EOF reject. No extra TaskRun/event is
created per document.

### Capacity and retention

Each stream freezes limits for records, canonical UTF-8 JSON bytes, individual
values, managed state, consumer count and prefix record/byte bounds. Prefix
size is checked in SQL before payload materialization. These are logical
admission budgets, not physical database/index/WAL disk measurements.
A FlowRun has a fixed upper
bound on stream headers. Capacity refusal is explicit; this slice does not
claim rolling backpressure/resume after online compaction.

Physical reclamation is **whole-flow release** after the authoritative
FlowRun is final. One transaction locks all its headers, marks them released,
removes records, consumer state and receipts, and resets live counters.
Headers retain identities, tails and a release witness. Only native row
payloads are reclaimed; external objects named by references are not deleted.
Old mutations are refused. TaskRun identity/attempt/lifecycle admission precedes
stream inspection, so a terminal actor can receive `ATTEMPT_NOT_CURRENT`
before the release witness is read. Direct prefix access and otherwise admitted
references receive `REPLAY_HORIZON_EXPIRED`; status still exposes the witness.
Exact release retry is idempotent. An active FlowRun cannot be
released. Ordinary FlowRun deletion retains its existing cascade lifecycle.

This keeps finite readers safe while the flow runs, but a long-running flow
can exhaust frozen capacity. Durable reader pins and online retention need a
later ADR and acceptance before an unbounded-running capability is admitted.

### Database capability

PostgreSQL 17 is the selected data plane. SQLite receives matching schema
migrations for repository compatibility; native stream store operations reject
SQLite before writes. Ordinary SQLite workflows retain their support. No
SQLite stream concurrency or PostgreSQL HA/throughput claim is made.

## Stories

- As a developer, I want a platform-owned storage boundary that survives
  recreating the store object and a client process.
- As a user, I want failure before durable terminal acceptance of document 200
  to retain 200, and failure after acceptance to retain progress through 200.
- As a system, I want stale owners, changed completion intent and EOF races
  to leave the database consistent.
- As an operator, I want bounded admission and explicit reclamation of a final
  FlowRun without silently dropping active reader data.

## Acceptance criteria

- [x] As a developer, I can upgrade from each prior dialect head and downgrade
  the new revision, with native tables/constraints matching ORM metadata.
- [x] As a system, I can reject malformed terminal/delivery/receipt NULL
  combinations by direct SQL in both dialects, not only through API guards.
- [x] As a system, I can reject SQLite operations and incompatible versions
  before mutation while ordinary SQLite database/flow checks still pass.
- [x] As a developer, I can use the native store without loading any module
  from `architecture/` or the Stage 1 PostgreSQL test adapter.
- [x] As a user, I got all-or-none output, receipt, state, cursor and delivery
  changes under a real PostgreSQL transaction failure.
- [x] As a user, I can recover document 200 after an incomplete delivery and
  continue at 201 after accepted completion, including a new store/process.
- [x] As a system, I can reject stale owners, changed bindings, changed
  completion intent, cross-flow TaskRuns and incompatible terminal markers.
- [x] As a system, I got a dense ordered immutable output prefix and correct
  YIELD/RETURN/EOF behavior under concurrent completion/takeover/close.
- [x] As a system, I can refuse record/state/capacity limits without partial
  progress or additional output.
- [x] As an operator, I can release a final FlowRun idempotently, observe zero
  live payload/state/receipt rows, and get explicit lifecycle or horizon refusal
  on old references, with an observable released witness.
- [x] As an operator, I cannot release an active FlowRun or mistake this
  bounded profile for online GC or indefinitely running ETL acceptance.
- [x] As a developer, I got real PostgreSQL tests and ordinary-flow/database
  regression with frozen source hashes and independent Principal review.

## Evidence

The [acceptance record](../evidence/native-stage2-acceptance-2026-11-01.json)
checks all ten implementation/test/workflow hashes before and after the final
runs and against committed revision `a0a81d6`. No source drift occurred.

| Gate | Actual result |
| --- | --- |
| [Native store](../evidence/native-stage2-final2-store-2026-11-01.json) | PostgreSQL 17.11; 40/40 pass, zero skip/error; 69.77 pytest seconds. |
| [SQLite capability](../evidence/native-stage2-final2-sqlite-capability-2026-11-01.json) | 1/1 pass; unsupported data plane refuses before native writes. |
| [PostgreSQL DB suite](../evidence/native-stage2-final2-postgres-database-2026-11-01.json) | 125 pass, one pre-existing intentionally skipped Alembic command test; 100.82 pytest seconds. |
| [SQLite DB suite](../evidence/native-stage2-final2-sqlite-database-2026-11-01.json) | 117 pass, nine pre-existing dialect/slow-test skips; 133.43 pytest seconds. |
| [Ordinary kernel](../evidence/native-stage2-final2-kernel-2026-11-01.json) | 718 pass, two known async/synchronous-sleep timeout xfails; 146.71 pytest seconds. |
| [Static gate](../evidence/native-stage2-final2-static-gate-2026-11-01.json) | Full Pyright 1285 files/zero errors; focused storage 4 files/zero errors; public type completeness 100%. |
| [Migration fidelity](../evidence/native-stage2-migrations-json-successor-2026-11-01.json) | Actual prior-head downgrade/new-head upgrade, reflected ORM parity, JSON null and signed-zero fidelity: 3/3 in each dialect. |

The [Principal acceptance](../evidence/native-stage2-principal-acceptance-2026-11-01.json)
verifies all source bindings and protocol boundaries, accepting N1.2 and its
planned +10% contribution (20% cumulative bounded native foundation). Hosted CI
after publication is a separate observation. The HTTP adapter and full Stage 2
remain unaccepted by this storage-only document.

Implementation: [store](../../../src/prefect/server/models/streams.py),
[server DTOs](../../../src/prefect/server/schemas/streams.py),
[ORM](../../../src/prefect/server/database/orm_models.py),
[DB interface](../../../src/prefect/server/database/interface.py),
[PostgreSQL migration](../../../src/prefect/server/database/_migrations/versions/postgresql/2026_11_01_000000_7f3c2a9e4b11_add_native_stream_storage.py),
[SQLite migration](../../../src/prefect/server/database/_migrations/versions/sqlite/2026_11_01_000000_9b6d4e1c8a20_add_native_stream_storage.py).

The [native regressions](../../../tests/server/models/test_streams.py) exercise
actual SQL operations and independent row/counter checks. Deferred PostgreSQL
constraint triggers fail the real commit for YIELD, RETURN and release, proving
all-or-none durable state. A [separate process](../../../tests/server/models/stream_storage_process.py)
exits immediately after a committed read or completion; its successor observes
the incomplete input or next cursor. This proves the internal storage boundary,
not HTTP-response-loss or deployed worker recovery. The new
[migration regressions](../../../tests/server/database/test_stream_storage_migrations.py)
execute in both dialects without skips.

Parallel kernel execution retains the same Python multiprocessing
`resource_tracker` diagnostics already reproduced on the unchanged
[baseline](../evidence/native-stage1-baseline-diagnostic-2026-11-01.json).
They are neither suppressed nor reported fixed by this change.

The [retained initial findings](../evidence/native-stage2-initial-findings-2026-11-01.json)
separate the executed JSONB failure, source-review attempt-poisoning finding,
strict/public typing fixes and lifecycle/horizon error precedence. Exploratory
runtime passes are not treated as the final source-bound acceptance gate.

### Pre-publication SQL shape hardening

The [direct SQL hardening record](../evidence/native-stage2-shape-null-hardening-2026-11-01.json)
preserves three malformed rows that the initial CHECK expressions accepted:
a completed stream without a tail, a delivery UUID without an in-flight
sequence, and a YIELD receipt without an output coordinate. SQL CHECK accepts
UNKNOWN as well as TRUE; comparisons with NULL did not enforce the intended
nonnull branches. Explicit `IS NOT NULL` guards now make them FALSE. ORM and
both still-unpublished migrations were updated together. The native store and
wire API did not change. Actual successor migration tests reject all three
inserts on PostgreSQL and SQLite; valid JSON null remains supported.

The initial acceptance at `a0a81d6` above is retained as history. The refreshed
source-bound gate after commit `f9e0171` is recorded separately below.

The [successor acceptance](../evidence/native-stage2-acceptance-successor-2026-11-01.json)
binds all ten source files to `f9e0171`: 40/40 native store, 1/1 SQLite
capability, 718 kernel passes plus two known xfails, 126 PostgreSQL DB passes
plus one existing skip, and 118 SQLite DB passes plus nine existing skips.
All source-drift lists are empty. Full/focused Pyright have zero errors and
public type completeness remains 100%. Native migration tests now execute
4/4 cases in each dialect, including all three malformed SQL inserts.

A [contended PostgreSQL trial](../evidence/native-stage2-schema-final-postgres-database-2026-11-01.json)
is retained as failed: one `hosted_api_server` readiness setup exceeded the
existing 90-second timeout on `gw0`, then 41 cases reused that cached fixture
exception. The [diagnosis](../evidence/native-stage2-postgres-fixture-timeout-2026-11-01.json)
finds no SQL lock/connection errors and does not assert an unproved root cause.
The [isolated full rerun](../evidence/native-stage2-schema-isolated-postgres-database-2026-11-01.json)
passes on the same bytes and timeout, with no concurrent suite. The failed
trial is not counted as acceptance; code and thresholds were not changed.
The [successor Principal verdict](../evidence/native-stage2-principal-acceptance-successor-2026-11-01.json)
checks all ten current/Git hashes, the isolated PostgreSQL rerun, the complete
set of runtime/static records and the NULL-shape hardening. It accepts
`f9e0171` for N1.2 with no remaining scoped blocker. N1.3 remains pending.

## Consequences

Storage becomes native server code with native migrations and transaction
ownership. The next sprint can expose it through authenticated HTTP without
depending on a prototype module. The bounded retention choice avoids deleting
active prefixes but deliberately does not solve indefinite-stream capacity.
External effects still require client determinism/idempotency; this transaction
does not atomically commit arbitrary external sinks.
