# Worker renewal correction — 2026-11-07

[Verification](verification.json) binds `cd80536` plus five-file patch `94b0408a`.
Same-identity renewal shares the Worker lock with DATA; registration, missing
protocols and rotation retain the exclusive fence. Author RED1/GREEN10 and
personal PG10 cover contention, identity, admission, occupancy and expiry.

| Native case | Owner | Console seconds | Replacement |
| --- | --- | ---: | --- |
| Async normal500 | Implementation | 265.73 | None |
| Sync accepted200 | Implementation | 220.41 | Starts at 201; accepted input 200 stays complete |
| Async incomplete200 | Independent Principal | 266.42 | Starts at 200; only unfinished input 200 replays |

All three pass without skips under original 750/840/900 budgets, with source/
import, health, graph/channel/slot/coordinator release, PID and retained-DB closure.
Normal C1 is still RUNNING at the intermediate cursor capture; final completion
is verified separately. Personal infrastructure checks have 29 PASS/3 legitimate
skips, retaining the raw wrapper exit 1 and explicit source-scope correction:
535 repository imports match; 1922 virtualenv paths were outside capture.

[Old hosted CI](https://github.com/Anacreonsystems/ariadne/actions/runs/42255204186)
remains 82 SUCCESS/11 FAIL/1 SKIP, native 10 SUCCESS/9 FAIL. No hosted candidate pass,
whole-timeout cause or performance gain is claimed. The metadata-drift baseline
stays diagnostic. NI2-012…014 OPEN/BLOCKED, NI2-020 blocked, 15% earned/27% unearned;
current local 5000/full 19/four-heavy exits remain open. Approved ADR bodies/criteria
are unchanged. Raw logs, credentials, source copies and inventory archives are excluded.
