# ADR-0097: Recursive null omission in recovery responses

- Status: `rejected`
- Date: 2026-11-04
- Owners: Native lifecycle and client-schema maintainers; ETL Architect / HighLoad Principal
- Related: [ADR-0037](ADR-0037-mixed-finite-and-streaming-tasks.md), [ADR-0036](ADR-0036-durable-submission-recovery.md)

## Problem

Adding optional finite recovery items must preserve the existing strict
stream-only response shape. A recursive `exclude_none=True` filter removes
absent optional fields, but finite results also allow the actual JSON value
`null`. Their `result` field is required even when its value is null.

## Theory

The candidate was to apply recursive null exclusion to the complete recovery
response. This theory fails: it removes `receipt.result` when the accepted
result is null. A valid durable completion then becomes an invalid client
response with a missing required field.

## Proposed solution and agreement

Reject recursive null exclusion. Use unset-field omission and construct legacy
responses without setting the new optional finite-items field. Explicitly set
finite receipt results, including null, stay in the serialized response.
Preserve the existing durable result and its digest rather than inferring a
missing receipt from its JSON value.

## Stories

- As a developer, I want old strict clients to receive their existing response
  shape while new finite results retain every valid JSON value.
- As a system, I want a completed null result to differ from absent completion.

## Acceptance criteria

- [x] As a system, I got a counterexample where recursive omission removes the
  required result and client validation fails.
- [x] As a developer, I can round-trip the exact same null receipt with unset
  omission while preserving its explicit result field.
- [x] As a user, I got a real HTTP/PostgreSQL recovery result containing null
  after actual client process loss under the corrected response policy.

## Evidence

The [executed counterexample](../evidence/native-stage6a-null-serialization-counterexample-2026-11-04.json)
and [captured source](../evidence/native-stage6a-null-serialization-counterexample-2026-11-04.txt)
show the rejected and correct serializations against actual client models.
The [finite HTTP matrix](../../../tests/test_native_finite_http.py) includes
actual process loss with object and null results. The [frozen finite HTTP profile](../evidence/native-stage6a-finite-http-final-2026-11-04.json)
passes all 17 cases, including the explicit null result across process loss.

## Consequences

Optional protocol extensions require field-presence rules rather than blanket
value filtering. This is an implementation finding inside ADR-0037 and earns
no independent readiness share. Both the rejected candidate and successful
replacement remain in the documentation graph.
