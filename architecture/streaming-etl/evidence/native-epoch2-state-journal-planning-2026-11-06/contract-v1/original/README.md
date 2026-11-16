# NI2-020a exact contract — planning handoff closed

The Principal agreed the concrete **planning** choices in
[contract-v2.md](contract-v2.md) and the mandatory
[C26 addendum](c26-generic-flow-deletion-addendum-v1.md). Read
[principal-review/contract-finalization-principal-planning-closure-v1.json](principal-review/contract-finalization-principal-planning-closure-v1.json)
for the scope and limits of that agreement.

This package contains no implementation or passing test claim. It uses immutable
Git d906 source, not the unrelated current logging-test change. The first review
requested changes; its receipt and all original v1 payloads are retained. V2
addresses log-row RESTRICT teardown, all11 old profile CHECKs, generic legacy
release dispatch, acyclic hashes, accepted receipt comparisons, ownership naming
and lost final restoration ACK handling. C26 adds the explicit generic Flow
delete gate before slot/log cleanup; it does not claim an observed current
runtime deletion defect.

The implementation choices include explicit v2 profile/API/feature negotiation,
eight proposed state/log tables with exact constraint tuples, a static
37-field charged-packet bound of2156 bytes under the4096 cap, source-derived
Pending/Scheduled/Running and same-grant retry guards, and reserve/convert/refund
rules. Static bounds, AST checks and read-only FK-target comparisons do not
replace actual DTO, DDL, lock-interleaving or native tests.

NI2-014 remains the implementation dependency. The future falsification plan
must be executed after a separate implementation handoff. NI2-020a is
genesis-only; A44-01 waits for a genuine aligned local cut024a and the real
snapshot100 + batches101..200 oracle020b. No cached state or fixture seed becomes
COMPLETE. SOURCE has no fabricated managed-state owner; opaque-state rescale,
full global cuts, online GC and new terminal-output language remain outside
this slice. Existing v1 bytes, JSON null and state-only RETURN stay exact.

All state preparation runtime/DB/repository/hook/prototype actions are zero.
The concurrent test actors belong to other tasks and were untouched. Readiness
remains15%, with zero credit from this package. All external writes stop after
the index is sealed. The parent may append/link this evidence in existing ADR
documents; this actor performs no repository write or code change.
