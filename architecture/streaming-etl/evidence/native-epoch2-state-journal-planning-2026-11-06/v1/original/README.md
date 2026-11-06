# NI2-020 external planning handoff

**Planning handoff closed; implementation and acceptance remain open.**

Read [implementation-spec-v2.md](implementation-spec-v2.md) for the proposed
implementation, [source-bindings.json](source-bindings.json) for the exact
28-file d906 source basis, and
[principal-review/state-spec-v2-planning-readonly-review-v1.json](principal-review/state-spec-v2-planning-readonly-review-v1.json)
for the independent read-only Principal review.

The original draft is retained. The revised document fixes both review
findings: pre-ready restore keeps the own TaskRun UPDATE lock through a narrow
Pending-tolerant validator, and SOURCE has no fabricated managed-state input
identity. The review closes preparation only. Exact wire names, complete FK and
lock trace, bounded overhead/temporary quotas, and local-cut intent/pin protocol
remain implementation prerequisites.

The internal work refinement is 020a journal/resolve/genesis fold -> legitimate
024a local ordered cut -> 020b native snapshot100 + batches101..200 oracle. It
changes no approved ADR criterion. A44-01 remains OPEN until the real COMPLETE
cut and native oracle exist. A cached postimage, fixture seed or journal-only
marker cannot substitute for that cut. New profile/table/route names are
provisional. Existing v1 canonical JSON, null and state-only RETURN semantics
must remain exact.

The already approved ADR44 scope needs no repeated user approval for ordinary
implementation details. This actor currently has only read-only/external-doc
authorization; no code starts until the NI2-014 dependency gate and a Root
implementation handoff. No state runtime, DB connection, hook, repository
mutation, prototype or acceptance test was executed for this package. The
concurrent 5000-document actor is separate and untouched. Epoch-2 readiness
remains15%; this package earns zero credit.

The parent can archive this package and link it from existing ADR44/task/epoch
documents in the primary checkout after review. This actor performs no such
repository write. All index payloads are exact bytes; the index excludes itself.
