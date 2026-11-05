# Server component evidence: 5 November 2026

As a developer, I want to trace each server result to the exact tested files and to retain unsuccessful observations alongside their successors.

Acceptance criteria for this archive: as a developer, I can resolve every artifact through a relative index, verify its original SHA-256 and inspect both a failure and its correction. These archive criteria do not accept ADR43 or the complete native implementation.

The [relative index](index.json) records all copied bytes and their provenance. Original receipts retain their original contents, including historical temporary paths; the index translates their bundle-relative paths into permanent archive paths. Raw Python, JSON, patch and configuration snapshots use a `.log` suffix so formatting and spelling hooks cannot rewrite bound history. The suffix changes the archive name only.

The [v1 receipt](v1/receipt.json.log) contains 70 PostgreSQL and 37 SQLite component passes before the genuine type check was corrected. The actual nonignored check found 37 errors and 14 warnings. An earlier absolute include configuration was ignored by the checker and establishes no type proof. The [v2 receipt](v2/receipt.json.log) records 90 PostgreSQL passes, including 20 legacy native deployment API cases, and 37 SQLite passes after type and shared-interface corrections. JSON schemas for all 33 DTOs remained unchanged.

The [v3 receipt](v3/receipt.json.log) records the final 70 PostgreSQL and 37 SQLite component passes. Its 27 owned source files remained unchanged. An independently owned worker model changed a type cast during this replay; the receipt preserves that dependency drift. The final combined type check analyzed eight actual files with zero errors and warnings after the worker API files were frozen. This is component evidence, with no claim that the entire native test matrix used one unchanged source tree.

Failures remain accessible through the index: the interrupted controls run has an unknown exit and no JUnit result; the unsupported-profile test originally expected HTTP 409 instead of the existing 422; a migration test referenced a missing fixture; a SQLite control constructed an incomplete event; the restrictive assignment foreign key exposed cleanup order; and the capability foreign key needed cascading removal of liveness metadata. The assignment foreign key remains restrictive. Only the shared test cleaner removes graph metadata before its early WorkPool deletion.

The underlying graph decision is [ADR43](../../adr/ADR-0043-physical-graph-and-partitioned-channels.md), within [epoch 02](../../epochs/epoch-02-native-streaming-completion.md). Adjacent [worker API evidence](../native-epoch2-worker-api-2026-11-04/index.json) preserves that component's own and independent checks. Full independent native acceptance, the complete matrix and later architecture stages remain separate requirements. Earned acceptance credit from this archive is zero.
