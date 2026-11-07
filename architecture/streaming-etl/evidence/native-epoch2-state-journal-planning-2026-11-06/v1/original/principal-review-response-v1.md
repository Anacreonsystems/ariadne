# Response to focused Principal review

This records planning feedback, not code verification or native acceptance.
The original draft v1 remains unchanged. The revised `implementation-spec-v2.md`
addresses both concrete findings:

1. Pre-ready restore keeps **own TaskRun UPDATE** in the established order. It
   uses a narrow Pending-tolerant current-task validator, including the actual
   stored bootstrap/attempt relation. It does not omit the task lock or copy the
   RUNNING helper's `run_count>=1` condition into initial Pending startup.
2. Only explicitly journaled **OPERATORs with sealed input lineage** get state
   owners/batches in this slice. SOURCE retains its accepted source-position
   authority and has no invented input or state-owner identity. Explicit INLINE
   operators, if admitted in v2, keep their old inline authority; omission cannot
   downgrade a journaled operator.

The Principal found the explicit v2/no-downgrade, internal
020a -> local-cut prerequisite -> 020b order, genuine COMPLETE anchor dependency
and frozen-ready-target separation coherent as planning. Exact composite FKs,
the full lock/attempt trace, encoded quota overhead, and the local-cut
intent/pin/failure protocol still require implementation review before code.

No state implementation, runtime, DB operation, acceptance credit or change to
approved ADR acceptance criteria follows from this response.
