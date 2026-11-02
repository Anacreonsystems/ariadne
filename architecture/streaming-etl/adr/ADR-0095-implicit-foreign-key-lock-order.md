# ADR-0095: Ignore implicit foreign-key locks in stream lock ordering

- Status: `rejected`
- Date: 2026-11-01
- Owners: ETL Architect / HighLoad Principal, native storage maintainers
- Refines: [ADR-0033](ADR-0033-native-durable-stream-storage.md)
- Related: [ADR-0034](ADR-0034-native-stream-http-adapter.md)

## Problem

Native stream transactions declare an explicit row-lock order. The original
consumer-open path locked the current TaskRun and ordered stream headers before
inserting `stream_consumer`. Flow release locked the parent FlowRun before the
same headers. Reading only the explicit `SELECT ... FOR UPDATE` statements made
these paths appear compatible.

PostgreSQL also acquires locks while checking foreign keys. Inserting a new
consumer whose `flow_run_id` references `flow_run` requests an implicit
`FOR KEY SHARE` lock on that parent. This turns the apparent
TaskRun-to-header order into TaskRun to headers to FlowRun and reverses the
release order.

## Theory

The rejected theory was that ordering only the explicit TaskRun, stream and
consumer locks was sufficient. Under that theory, the foreign-key check was
treated as a schema validation detail with no effect on concurrency.

An initial correction considered TaskRun to FlowRun to headers. That removes
the observed release cycle, but it still reverses parent-first FlowRun deletion:
a delete may hold the FlowRun and wait for its cascaded TaskRun while consumer
open holds that TaskRun and waits for the FlowRun.

## Proposed solution and agreement

Reject both incomplete orders. A first consumer open acquires locks in this
order:

1. FlowRun `FOR SHARE`;
2. current TaskRun `FOR UPDATE` and attempt/state validation;
3. terminal FlowRun validation;
4. stream headers in UUID order `FOR UPDATE`;
5. existing consumer row, or insertion of the new consumer.

The parent share lock is acquired before child locks. It conflicts with
FlowRun state updates, release's `FOR UPDATE`, and parent deletion, while
allowing independent opens to share the parent lock. The later foreign-key
check therefore requests no lock that reverses the declared order. TaskRun
validation remains before the terminal-state error, preserving
`ATTEMPT_NOT_CURRENT` precedence for an already terminal TaskRun. A still
RUNNING TaskRun under a final FlowRun receives `TERMINAL_CONFLICT` before any
stream or consumer mutation.

Per-record read and completion paths retain TaskRun to ordered-header locking.
They do not insert a new FlowRun foreign key or acquire the parent afterward,
so they do not form this cycle. [ADR-0034](ADR-0034-native-stream-http-adapter.md)
must preserve the storage transaction rather than reconstruct this order in an
HTTP client.

## Stories

- As a developer, I want the documented lock graph to include locks acquired
  by PostgreSQL constraints as well as explicit queries.
- As a user, I want consumer admission racing final release to return a named
  lifecycle result rather than an untyped database deadlock.
- As a system, I want parent, child and header locks acquired in an order that
  remains safe during FlowRun release and deletion.

## Acceptance criteria

- [ ] As a system, I can run the rejected TaskRun to headers to implicit
  FlowRun order concurrently with release without PostgreSQL detecting a
  deadlock. **Failed.**
- [x] As a developer, I can reproduce the implicit FlowRun `FOR KEY SHARE`
  wait and identify both sides of the cycle from PostgreSQL evidence.
- [x] As a system, I can block first-open on the FlowRun before it owns the
  TaskRun or headers when release wins.
- [x] As a system, I can let first-open commit before a waiting finalization
  and release, after which release removes the consumer and payloads.
- [x] As a user, I get `TERMINAL_CONFLICT` with no consumer mutation when a
  RUNNING TaskRun attempts its first open under a completed, failed or
  cancelled FlowRun.

## Evidence

The [negative probe](../evidence/native-stage2-fk-lock-cycle-negative-2026-11-01.json)
runs two real PostgreSQL 17 transactions. Release holds the FlowRun and waits
for stream headers; first-open holds the TaskRun and headers, then its consumer
insert waits for the FlowRun. PostgreSQL reports `deadlock detected` and names
the referential-integrity query `SELECT ... FROM flow_run ... FOR KEY SHARE`.
The retained raw log is linked from that evidence record.

The replacement behavior is implemented and tested under
[ADR-0033](ADR-0033-native-durable-stream-storage.md). Its real PostgreSQL
regressions inspect `pg_blocking_pids`, use independent `NOWAIT` child-lock
probes, exercise both lock schedules, and cover completed, failed and cancelled
FlowRuns. The ADR-0033 successor gate remains the authority for final source
hashes and the complete N1.2 result.

## Failure modes

- Omitting the parent lock leaves the foreign-key lock invisible until insert.
- Taking FlowRun only after TaskRun avoids one observed cycle but can conflict
  with parent-first cascade deletion.
- Using `FOR KEY SHARE` alone does not fence an ordinary non-key FlowRun state
  update; `FOR SHARE` is required for stable terminal admission.
- Treating PostgreSQL deadlock victim selection as retry logic exposes an
  untyped storage failure and does not prove deterministic lifecycle behavior.

## Consequences

Consumer admission takes a shared FlowRun row lock and may wait behind state
transition, release or deletion. That cost occurs at consumer open, not on each
record. The declared lock graph now includes constraint-induced parent locks.
The rejected ordering remains recorded so later schema or HTTP changes cannot
silently reintroduce it.
