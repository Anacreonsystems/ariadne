# ADR-0094: Use JSONB for canonical stream values

- Status: `rejected`
- Date: 2026-11-01
- Owners: ETL Architect / HighLoad Principal, native storage maintainers
- Related: [ADR-0033](ADR-0033-native-durable-stream-storage.md),
  [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)

## Problem

Native records and managed state need safe JSON persistence and exact
round-trip values. The ordinary server JSON type uses PostgreSQL JSONB;
reusing it appears consistent with existing models.

## Theory

JSONB with explicit JSON-null handling might retain every accepted Python
JSON value while providing the server's usual binary JSON representation.

## Proposed solution and agreement

Use JSONB for record values and managed state, including top-level JSON null.
This alternative was implemented and then tested against actual PostgreSQL.
It is rejected: JSONB normalizes `-0.0` to `+0.0`. Python workflows can observe
that change with `math.copysign`, so a later reader receives a different value.

ADR-0033 instead uses PostgreSQL JSON for values/state, preserving the codec's
serialized spelling, while its quota configuration may use ordinary JSONB.
The term canonical JSON here denotes the versioned application encoding; it
does not claim RFC 8785 normalization, which has different number rules.

## Stories

- As a developer, I want replayed inputs and restored state to retain the
  accepted Python JSON scalar values.
- As a system, I want storage optimization to preserve the durable-value
  contract rather than silently change numeric input.

## Acceptance criteria

- [ ] As a user, I got the same signed-zero value after durable storage and a
  fresh read when using this JSONB alternative. **Failed.**
- [x] As a developer, I can reproduce the failed alternative and locate the
  chosen replacement and its positive round-trip tests.

## Evidence

The [actual failure log](../evidence/native-stage2-json-codec-failure-2026-11-01.log)
shows `math.copysign(1, item.value["negative_zero"])` returning `+1` after
publication of `-0.0`. The
[retained findings](../evidence/native-stage2-initial-findings-2026-11-01.json)
distinguish this executed failure from source-review findings.

The [replacement migration gate](../evidence/native-stage2-migrations-json-successor-2026-11-01.json)
checks actual JSON column types, JSON null and signed zero for records/state.
The native store [regressions](../../../tests/server/models/test_streams.py)
also cover accepted outputs, restored state, escaped NUL and large integers.

## Consequences

The rejected option remains recorded. Native storage gives up JSONB-specific
indexing for these opaque payload/state columns; this profile does not query
their internals. External-effect idempotency remains the client's obligation,
but storage itself must preserve accepted data.
