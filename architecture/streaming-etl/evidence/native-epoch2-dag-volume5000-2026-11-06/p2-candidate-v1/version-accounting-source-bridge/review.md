# Evidence bridge: clean18124 → one test-only cleanup gate

This is version accounting and a read-only source-relevance review, not a new
architecture decision, runtime execution, acceptance or readiness award.

The exact candidate is **HEAD18124603882fe9196a6eec6063a4619e7087f48f plus
patch4550f35d5db3b4ddc74160f55354d33165551d1cd82d33695ee687402621b0b7**, with
`tests/test_native_partitioned_stream_http.py` at
`61ebfdfaac8e394c08e8156f0ab94a0483701f6932b3bd83020c39d70d1ade71`.
It is not clean18124 and has no newly published commit identity here.

[Source bridge](source-bridge.json) verifies 1600 bound files in total: **1599
unchanged and one changed test**. All bound production/native DAG test/fixture
bytes are unchanged. The primary checkout is clean18124; the detached runtime
checkout has exactly the known one-test modification. This is not a claim that
1600 files *besides* the test were compared.

The prior [matching source review](../native-p2-full-cleanup-candidate-principal-review-v2/review.json)
was Approve at SHA06b77ed6b3f508a2be59a4c95d9086e67ae76e4cae6310fbc46695f768ca1061.
It preserved the V1 RequestChanges and corrected only the new snapshot's UUID
serialization. Removing the exact new asyncio import, private helper and
successful-profile polling block recovers the whole original module AST,
including every old business/cut/ordering/state assertion and original budget.

## What can be reused, and how to describe it

| Evidence | Reusable scope | Required version wording / limitation |
| --- | --- | --- |
| Closed personal async DAG5000, receiptb5cb19c6872858277056a6a4953b27af5c51e241cf7294240d38d32e410e4a3e | Actual `[True-5000]` result, production execution, body/effect/cardinality/health/release facts within its original receipt and later closed SQL evidence. | Actual execution on clean18124, not on the WIP or a future commit. Cite this narrow source/dependency bridge if using it for candidate acceptance. No new personal DAG run occurred here. |
| Closed personal sync DAG5000, receipt1e6eb5ee4057c30bef75c70379459167b2f907402799e0f694036ff8cb327979 | Actual `[False-5000]` result under the same qualification. | Same distinction: retain actual command/source/phase/date/import bindings and zero drift. |
| Actual completed current CI42147494934 jobs on18124 | Their baseline regression, package, migrations, old business/cut/cardinality assertions, and only the exact profiles they actually executed. Production bytes and the existing assertions were not changed by this candidate. | Call these **CI results on18124**, not candidate CI or a future commit's CI. Queued/running jobs receive no success credit. This review performs no API query and makes no new whole-run conclusion. |
| Baseline P2 native sync fault PASS and raw postterminal15f9c83c… | Accepted repeated-input200, stable identities/physical2, exact effects and the nine true physical/tag/deployment/cursor facts actually observed. | Retain the original full-cleanup incomplete result. New helper assertions cannot retroactively be attached to that run or turn its three false cleanup predicates true. |
| Existing selected PG/component receipts at the same runtime source hashes | Their actual targeted capability/recorder/heartbeat/legacy criteria, provided their real source and command are cited. | One test change does not rename or repeat those executions. No blanket statement that the whole candidate suite ran is warranted. |

[Closed DAG references](closed-dag-references.json) binds both real native receipts,
source-before maps and all-actors STOP adapters. Both native receipts actually
have pytest exit0, source1600/drift0, 3884 imports and zero import mismatches.
Their actual nodes are the original sync/async DAG5000 nodes on18124. Those
fields are retained facts, not a new replay of either test.

## Important imported-module qualification

The DAG test itself is unchanged, but it imports `execution_api` from the
**modified P2 test module** at native DAG test line35. Therefore do not say that
all DAG imported-module bytes are identical between baseline and candidate.
The old actual runs imported P2 module38a574eb…; the candidate imports61ebfdfa….

The `execution_api` function, its fixture decorator, signature, body and global
bindings to `RestartableAPI`/`native_backend` are unchanged. Its normalized AST
SHA is fc9a04a2b8bef160245ce122f18ed0041df624bc565c83a2774cb1ec402fa14e.
The added `_terminal_resources` and polling block belong to the P2 testcase and
are not invoked by the DAG fixture. The only extra module import is asyncio,
already imported by the DAG module before that fixture import. This is the
specific source-relevance reason the old DAG results remain applicable to
unchanged production/fixture behavior; it is not full import-byte equality or a
candidate DAG rerun.

## What requires fresh positive candidate evidence

The new cleanup wait itself has no old execution evidence. The minimum newly
required real candidate cases remain:

- `tests/test_native_partitioned_stream_http.py::test_actual_process_worker_runs_four_partition_tasks[500-operator-before-yield-200-False]`;
- `tests/test_native_partitioned_stream_http.py::test_actual_process_worker_runs_four_partition_tasks[500-operator-loss-200-True]`.

Each must have its **own** completed candidate command/node, source-before/after,
imports, actual pytest exit/JUnit, old200/201 and business-effect assertions,
positive `automatic_terminal_release`, and owned process/observer closure. The
original observation840/pytest900 budget remains in force. Expected deliberate
replacement is not a normal-case all-attempts1 failure.

The positive release snapshot must occur automatically before workers are
terminated: exact graph/authority/snapshot/launch RELEASED, four freed physical
subtasks, expected five released grants for these fault cases, four released tag
allocations/partitions, zero accounting/consumer/capacity counters. No manual
release, backend repair or substituted fixture may be represented as automatic
completion. B0's per-partition input200/201 is distinct from a global business
document number. The unchanged client owns repeated external-effect idempotence.

At delegation, Root reported that candidate sync execution was active. This
review reads **no active V3 runtime files** and awards no candidate runtime
result. The eventual state must come from a real closed receipt, even if the
actor finishes while this report is being written.

The new gate intentionally excludes after-activation, import-exit and
before-declaration profiles, preserving their old failure/retry assertions.
Do not claim newly verified full cleanup for those profiles. Terminal-exit-stall
and the two normal heavy P2 profiles are covered by the added guard in source,
but no new candidate execution of them is claimed here. Baseline current CI
heavy results still prove their original cardinality/business assertions, not
this new positive cleanup wait.

## Publication and final accounting

Use a composite evidence statement: original actual results at their original
version + this exact source-relevance bridge + actual new candidate P2 receipts.
Do not say the original full suite ran with the WIP, that CI18124 tested the new
wait, or that a future commit was executed before it existed.

When a future commit is created, compare its runtime/test bytes with the tested
candidate and record the resulting publication-equivalence receipt. That can
show the committed files represent the tested candidate. It cannot change the
head on old GitHub Actions jobs. Any required CI run on the new published head
must be described separately and must actually execute before being called green.
Any further production, fixture, imported-global, helper or budget change
requires a new relevance assessment; this bridge authorizes no such change.

All baseline failures, postterminal facts, source-review attempts and approved
ADR/acceptance content remain immutable. This review ran no project imports,
pytest/collection, database queries, network calls, hooks, process control or
application actor. It changes no source and grants no permission or readiness.
All grill-me/spec-first variants remain excluded; conflict discovery remains
incomplete as previously recorded.
