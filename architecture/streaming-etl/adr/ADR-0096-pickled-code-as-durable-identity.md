# ADR-0096: Ordinary pickled task code as durable recovery identity

- Status: `rejected`
- Date: 2026-11-03
- Epoch: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md)
- Owners: Task SDK/runtime maintainers; ETL Architect / HighLoad Principal
- Related: [ADR-0036](ADR-0036-durable-submission-recovery.md)

## Problem

A restarted flow must bind its stable submission key to the same task definition.
Ordinary Prefect computes `Task.task_key` from a pickled code object. Treating
that value as a durable witness assumes identical code produces identical keys
across processes.

## Theory

The candidate was to reuse the existing ordinary task key alongside explicit
task/flow versions and immutable parameters. That would avoid a separate code
identity algorithm for native recovery.

The theory is false: a frozenset code constant can serialize in hash-dependent
order. The unchanged document-signing task produced `anya-64bd2e0c` under seed 1
and `anya-c49d6607` under seeds 2 and 7919. Actual whole-flow recovery refused
its second declaration with `IDENTITY_CONFLICT`. Refusal protected data identity,
but could not recover the otherwise unchanged workflow.

## Proposed solution and agreement

Reject ordinary pickled keys as the native durable code witness. Keep ordinary
`Task.task_key` and finite-task cache behavior unchanged. The keyed native
profile uses the structural code identity agreed in
[ADR-0036](ADR-0036-durable-submission-recovery.md), with a full artifact digest
bound into the manifest and explicit task versions for global dependencies.

Pinning the process hash seed would make deployment environment configuration
part of recovery authority and would retain path/debug-table sensitivity. It
is not the chosen protocol.

## Stories

- As a developer, I want unchanged task code to recover across fresh processes.
- As a system, I want a code witness independent of hash seed and debug location.
- As a user, I want a false identity assumption to fail before attaching to a
  different channel or repeating completed source work.

## Acceptance criteria

- [x] As a system, I got a fresh-process counterexample: unchanged code produced
  two ordinary task keys under three controlled hash seeds.
- [x] As a developer, I can verify one identical native key and full digest in
  those same processes without changing the ordinary task-key algorithm.
- [x] As a user, I got the rejected theory and its replacement linked to the
  active recovery ADR; no source or decision history was deleted.

## Evidence

The [independent counterexample](../evidence/native-stage5c-code-identity-counterexample-2026-11-03.json)
records both keys, seeds, canonical digest and exact source hashes. The
[code controls](../../../tests/test_native_stream_code.py) additionally test
semantic changes, path/line invariance, unsupported shapes and runtime
specialization. [Discovery retention](../evidence/native-stage5c-discovery-retention-2026-11-03.json)
retains the failed whole-process replay and its successful successors.

## Consequences

Native TaskRun manifests have an explicit structural witness; their identity
is found through the FlowRun/submission key. A new algorithm/schema or changed
Python runtime can refuse an old witness and require a new FlowRun. This does
not claim deterministic global dependencies or idempotent external effects;
those remain client responsibilities.

This rejected alternative earns no percentage. The combined native recovery
milestone remains governed by [ADR-0036](ADR-0036-durable-submission-recovery.md),
the [task registry](../todo.md) and [sprint board](../sprint_board.md).
