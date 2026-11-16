The unchanged existing-test baseline reproduced the Cloud logging assertion:
**1 failed, 6 passed in 94.20s**, no errors and no timeout.

The actual Cloud DEBUG source line ran with effective INFO20, API logging
enabled, Cloud classification true, no sink, and the worker API handler attached.
Both unchanged self-hosted negatives also ran at INFO20.

The provisioning hook ran exactly once; the original destructive provisioning
body ran zero times. Real repository migrations, hosted API, client, sessions,
and worker tests were used. All three observed PID/start identities closed;
the fresh owned database remains with zero sessions and no drop/force operation.

All 1754 tracked source bindings remained unchanged. Import coverage has a
qualified gap: four flagged rows refer to two generated untracked files absent
from the before snapshot. Both real processes imported matching hashes, which
also match after completion. The original receipt is retained; the supplement
does not claim before-binding for those two paths.

No candidate patch or retry was performed. This proves the controlled ordering
mechanism; it does not establish the historical CI gw0 order.
