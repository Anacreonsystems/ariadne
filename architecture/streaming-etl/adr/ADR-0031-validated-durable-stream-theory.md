# ADR-0031: Validated durable-stream theory and native integration handoff

- Status: `accepted`
- Date: 2026-10-31
- Accepted: 2026-11-01, selected prototype theory scope
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Scope: selected Python/PostgreSQL prototype and completed continuous-soak profile
- Depends on: [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0011](ADR-0011-yield-completion-and-client-effects.md), [ADR-0027](ADR-0027-short-acceptance-and-separate-long-soak.md), [ADR-0030](ADR-0030-continuous-arc-soak-and-evidence-collection.md)
- Leads to: [Native integration epoch 1](../epochs/epoch-01-native-prefect.md), [Stage 1 contract](ADR-0032-native-task-stream-contract.md)

## Problem

A successful short prototype alone cannot establish sustained recovery and
bounded retention. Conversely, the completed experimental delivery must not be
mistaken for native implementation in Prefect. The project needs an explicit
decision that the tested theory worked, its supporting evidence, and a separate
starting point for implementation in the core.

## Theory

Platform-owned durable channels, stable input identities, and atomic platform
acceptance of one terminal `yield` or `return` per input can preserve ordered
stream progress and managed state across retries. Periodic coordinated barriers
can provide consistent graph cuts and safe retirement while per-record
durability remains continuous between barriers. Client code owns determinism and
idempotency of external effects.

## Decision and agreement

The theory is successful within the implemented experimental profile. On
2026-10-31 the user requested that this result be recorded and that native
integration epoch 1 begin with its first-stage ADR.

This ADR accepts the implemented prototype theory under the scoped acceptance
rule in [ADR-0000](ADR-0000-decision-lifecycle.md). It does not change the broader
production criteria of earlier ADRs. The successor product direction is a
native extension of Prefect: stream dependencies, task execution, server,
storage and observability belong to one platform. The experimental adapter is
transition evidence, not the final architecture.

The native program starts at 0% earned implementation readiness. The historical
100% short experimental delivery and the now-completed DS-052a duration check
retain their own denominators. No percentage transfers automatically.

## Implemented and measured profile

- Exact source: `ff4972fb2c6df313056e828f72a3cad41dce9258`.
- Campaign: `gh-40672223609-1`, on existing `anacreon-runners` infrastructure.
- One Python workload process with logical graph `source -> A -> {B,C} -> J -> D`.
- Real PostgreSQL 17 primary and synchronous standby with `remote_apply`.
- Small synthetic `document-N` byte payloads and requested ingress of 1 record/s.
- Source documents traverse the whole graph sequentially. J and D observe two
  records per source document, one from each branch; this is not a business-key
  join or two independent external sink commits.
- Barriers and retirement run every 20 source documents. Completion of an input
  remains a per-record durable operation.
- Owner loss before terminal persistence occurs every 101 source documents;
  service-process `SIGKILL` is scheduled in segments 5 and 11.

The original reviewer measured 259298.161822 seconds, 188659 completed source
documents, 1867 owner-loss cases, three process boots and all 18 segments. It
reported `long_soak_passed=true` and no violations. All four invariant-check
families passed in every segment. The fresh collector and final upload passed.

## Stories

- As a developer, I want a durable-stream theory supported by executable code
  and failure evidence before changing Prefect core contracts.
- As a user, I want the project to record that the prototype succeeded while
  showing exactly which behavior and deployment were exercised.
- As a system, I want native implementation readiness to remain separate from
  historical experimental readiness.

## Acceptance criteria

- [x] As a developer, I can identify the committed implementation, pinned source,
  declared graph, payload, fault schedule and resource policy in the archived
  manifest and [completion record](../evidence/ci-arc-72h-completed-40672223609.json).
- [x] As a user, I got at least 259200 measured seconds and all 18 linked
  four-hour observations, with successful owner and service-process recovery.
- [x] As a system, I got matching final counts and rolling checksums, the exact
  declared fault counts, synchronous-replication checks, live storage authority,
  and no reported invariant or cursor violation in the selected profile.
- [x] As a system, I got enforced absolute and sustained-growth budgets for
  service memory, schema relations and primary storage throughout the measured
  samples, with no violation under the declared reviewer policy.
- [x] As a user, I can trace a successful original reviewer and fresh collector
  to the same run, attempt, source SHA and published artifact. The local
  reconstruction audit matches the hosted collector receipt, and the downloaded
  published archive matches its GitHub digest and all 22 reconstructed files.
- [x] As a developer, I can inspect the remaining scope and navigate to a
  separately unimplemented native integration epoch and first-stage ADR.

## Evidence

The [completion observation](../evidence/ci-arc-72h-completed-40672223609.json)
contains authenticated run/job/artifact metadata, the original manifest and
review, the hosted collector receipt, per-segment census and local transport
audit. It records 4248 samples across the 18 segments. The
[public evidence reconstruction](../evidence/ci-arc-72h-public-40672223609.zip)
preserves the exact whitelisted manifest, segment and review file bytes decoded
from source-job frames, plus the matching local transport report.

This local ZIP is a reconstruction with its own digest, separate from the
published artifact ZIP. The initial completion observation predates download
of that artifact and retains its original `artifact_archive_download_verified=false`
field. The subsequent [artifact audit](../evidence/ci-arc-72h-artifact-audit-40672223609.json)
records successful download through the connected GitHub tool: all 565526 bytes
match GitHub's SHA-256, and all 22 files match the source-frame reconstruction
byte for byte. The [original artifact ZIP](../evidence/ci-arc-72h-artifact-40672223609.zip)
is also retained. Raw job logs and credentials are excluded from both archives.

- [Completed Actions run](https://github.com/Anacreonsystems/ariadne/actions/runs/40672223609)
- [Original workload/reviewer job](https://github.com/Anacreonsystems/ariadne/actions/runs/40672223609/job/122761609939)
- [Fresh collector/publication job](https://github.com/Anacreonsystems/ariadne/actions/runs/40672223609/job/124513476015)
- [Published artifact](https://github.com/Anacreonsystems/ariadne/actions/runs/40672223609/artifacts/13039886942)
- [Workload implementation](../prototypes/ci_soak/service.py),
  [independent reviewer](../prototypes/ci_soak/review.py) and
  [transport validator](../prototypes/ci_soak/transport.py)
- [Completed short gate](../evidence/postgres-r5b-final2-2026-10-26.json) and
  [independent hostile controls](../evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
  retain their separate profiles and earlier acceptance.

## Failure modes and limits

The 72-hour data path did not execute native Prefect tasks. It did not measure
distributed concurrent operators, saturation throughput, large object payloads,
business database writes, or recovery after replacing the ARC pod or losing its
local database volumes. The two hard process losses retained the supervisor,
container and PostgreSQL state. Owner-loss cases were deterministic injected
failures before terminal persistence, not 1867 independently killed processes.

Resource measurements and growth calculations are those implemented by the
reviewer and sampled by the harness; they do not prove absence of all leaks or
establish production sizing. Ordinary client code can still repeat an external
effect after failure before platform completion. Production security,
multi-tenant isolation and compatibility need their own native acceptance.

## Consequences

The prototype is a validated protocol reference and can supply reusable
implementation components. Native integration proceeds through explicit core
contracts and new evidence. Earlier ADRs, rejected alternatives, failed trials
and historical running-status observations remain in the graph. Success here
provides no automatic `accepted` status to a production ADR.

## Architecture review

The [ETL Architect / HighLoad Principal review](../evidence/native-epoch-1-principal-review-2026-11-01.json)
accepts the scoped handoff on 2026-11-01: a successful original reviewer, exact source/run/attempt binding,
all 18 segments and a successful fresh collector/publication are required.
The [completion record](../evidence/ci-arc-72h-completed-40672223609.json)
satisfies those conditions. Native integration and high-load claims remain
outside this acceptance.
