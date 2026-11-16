# Fixed-P2 postterminal state classification

**Verdict: processing and physical/tag/deployment release are proved; full
coordinator/payload cleanup is not proved and was not completed in this capture.**

This is a strictly read-only source/closed-file review on published
`18124603882fe9196a6eec6063a4619e7087f48f`, compared with accepted N2.1
`e0725b0673d9c8015634e93bdd236d43614958a1`. No query, project import, test,
network call, process signal, source edit, or raw-evidence mutation occurred.
The original helper, SQL, command, raw attempt and its status remain unchanged.

## What the raw result proves

The capture at SHA256
`15f9c83cd98411755a066bfffec8b1517352cccc9257d1f66f17a99ced3bbdf9`
is a genuine PG17 read-only snapshot with its connection closed. Nine of its
12 literal checks are true: four completed TaskRuns (B0 count2, others1), stable
assignment epoch1 and B0 physical epoch2, five correctly bound grants/process
births, four freed physical subtask slots, four released tag allocations and
zero actual tag/deployment active-slot counters. Both LEGACY consumers have
completed_through250 with no in-flight delivery. The normal business run/replay
and owned-process closure facts remain valid within their actual receipt scope.

These are strong fixed-P2 processing/recovery and resource facts. An empty DAG
physical-slot/birth table is expected for this profile; it is not a missing P2
allocation. The persisted final Flow lease UUID is historical state detail,
not proof that an external KV lease is still held. External KV state was not
queried.

The other three checks are false because graph=ADMITTED,
authority=TERMINAL, launch=PROCESS_REGISTERED and partitions.released=false.
They must not be rewritten as true or dismissed as a generic DAG assertion bug.
The helper does not demand a non-null DAG terminal operation. Its three full
release predicates are also valid after fixed-P2's actual cleanup transaction.

## The fixed-P2 caller contract is automatic cleanup

At `src/prefect/runner/_starter_native.py:426`, `NativeProcessStarter.start`
awaits `_start_attempt`, reads the observed final Flow state and then calls
`release_stream_flow` at line445. `_start_attempt` owns `process.wait()` at
line604 before returning. The normal fixed-P2 release is therefore automatic
worker-owned post-wait cleanup, not an optional client request to discard
retained outputs.

At `src/prefect/server/models/streams.py:1809`, generic `release_flow`:

- requires a final Flow and TERMINAL/RELEASED execution authority;
- refuses release while any assigned physical child lacks its stop receipt or
  a bound TaskRun lacks terminal metadata;
- sets graph/grants RELEASED (lines1957–1970);
- deletes completion/consumer/record/finite-receipt payload state;
- marks all partitions released, zeroing record/byte accounting (line2057);
- releases manifests, execution authority, deployment snapshot, coordinator
  launches and replacement records (lines2060–2080).

The fixed-P2 release body, after removing only the three new DAG-dispatch prefix
statements, is AST-identical to accepted e072. `start`, `_start_attempt`, process
registration, recovery candidate discovery and verified launch retirement are
also AST-identical. The old P2 test's `finally` block is AST-identical. Exact
facts are in [source-equivalence.json](source-equivalence.json). This establishes
an existing lifecycle phase and an existing test boundary; it does not establish
a new 18124 production regression.

`apply_flow_lifecycle_result` at `stream_executions.py:1058` intentionally sets
authority TERMINAL when the Flow becomes final. This fences ordinary DATA
writes, while still allowing authenticated terminal cleanup. It does not itself
perform release. A historical PROCESS_REGISTERED row can likewise survive until
the later release transaction; the row alone does not imply that its PID is
still alive. The supplied process proof says that exact PID is gone.

## Why this test can finish before cleanup

The P2 testcase sets `passed=True` at
`tests/test_native_partitioned_stream_http.py:645` after observing final Flow,
Task states, physical placement and counters. Its `finally` immediately calls
`worker.terminate()` at649. It never awaits coordinator launch RELEASED or
payload release.

The closed logs show:

- final Flow state timestamp `04:20:15.656480+00:00`;
- both worker-stopped events at approximately `04:20:15.929`;
- four successful subtask lease-release responses;
- **zero recorded access responses for the exact automatic Flow release endpoint
  `/api/streams/v1/release`**.

[Closed log extraction](closed-log-extraction.json) binds the original logs and
line numbers without copying private payloads. About273ms separates the recorded
Flow completion and worker stop. This and the missing awaited gate strongly
support teardown preemption of normal coordinator completion/cleanup. The logs
are not a syscall trace proving which exact cancellation/kill step ran or that
no release request could have started without a completed access line.

The source explains a compatible path: `_start_attempt`'s cancellation cleanup
kills and waits a still-live child (lines611–616). Once `handed_off=True`, it does
not use the unstarted-child recovery branch. Cancellation can therefore unwind
before `start` reaches its normal release call. A natural-quiescence check is
still needed to distinguish test preemption from an independently stalled
coordinator/cleanup failure. No budget extension or intentional-retention claim
is justified by this snapshot.

## Two different observable phases

| Fact | After processing / before generic release | After successful generic release |
| --- | --- | --- |
| Flow and TaskRuns | Completed, correct stable identities/counts | Remain terminal with same identities/history |
| Graph | ADMITTED is valid here | RELEASED |
| Fixed-P2 terminal_operation_id | NULL | Still NULL; no DAG lifecycle journal is required |
| Execution authority | TERMINAL | RELEASED |
| Coordinator launch | PROCESS_REGISTERED can remain historical | RELEASED after worker-owned coordinator wait |
| Physical subtask slots / tag allocations | All released; selected active counters0 | Remain released/counters0 |
| Grants | Five RETIRED identities in this actual run | All become RELEASED; identities/history retained |
| Partitions | COMPLETED, tail/last/producer250, released=false | released=true, counters0; retained tail/position metadata |
| Consumers/completions/records | Actual completed250 cursors can be inspected | Deleted in the release transaction; do not demand retained250 consumer rows afterward |
| Deployment snapshot | Not selected in the raw capture; do not invent its state | RELEASED when actually observed |

The first column is the phase captured by this attempt. Its nine true checks are
sufficient for those selected physical/recovery/counter facts, and support the
original scoped N2.1/A43-07 resource evidence. They are **not** sufficient for a
claim that the complete platform launch/payload lifecycle has been released.
The existing N2.1 accepted record only claimed its actual scoped physical/tag/
Flow-capacity result; do not rewrite that historical record to add or remove
unobserved cleanup guarantees.

## Residual risk and next verification

Unreclaimed platform records are not established intentional output retention.
If left this way permanently, they retain platform data and potentially active
snapshot admission entries: `native_deployments.py:180` counts ACTIVE snapshots
against the128-run work-pool bound. Snapshot state itself was not selected here,
so current capacity consumption is an inference, not a literal SQL finding.
Fixed-P2 is deliberately excluded from the bounded-linear recovery candidate
path at `native_deployments.py:795`; do not assume that merely waiting for a
fresh worker repairs it or bypass that profile fence.

The concrete verified acceptance gap is that the existing testcase reports PASS
and shuts down its workers without observing the final automatic cleanup phase.
For full cleanup acceptance, perform a bounded real verification that lets the
normal worker-owned coordinator wait/release finish **before** workers are
terminated, within the original testcase's budget. Observe profile-specific
released states, and preserve accepted cursor/recovery proof separately from
post-reclamation consumer absence. If natural quiescence still fails to release,
report the lifecycle failure and diagnose it; do not weaken the predicates,
mutate this forensic DB, call release manually as if automatic, or declare the
retained rows benign.

The current body PASS and original post-SQL incomplete result remain immutable.
This report adds classification and no new acceptance, readiness credit or
runtime authorization. All grill-me/spec-first variants remain excluded;
conflict discovery remains incomplete as recorded by the coordinating session.
