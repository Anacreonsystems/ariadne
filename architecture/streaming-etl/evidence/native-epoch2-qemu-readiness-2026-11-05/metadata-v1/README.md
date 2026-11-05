# QEMU metadata readiness, 2026-11-05

Status: **PROPOSED_NOT_RUN**. Native readiness credit: **0**.

`metadata-readiness-v2.json` is the current proposed boot-only envelope: two512MiB/1vCPU guests, at most1GiB new allocated tmpfs bytes, a14GiB cgroup ceiling and2GiB reserve. It supersedes only the earlier resource proposal in immutable `metadata-readiness-v1.json`; the original readiness and budget receipts remain unchanged in the parent evidence directory.

Authenticated package metadata resolves20 missing packages for QEMU, qemu-utils and OVMF relative to this host:23,938,256 compressed bytes and133,800,960 advertised unpacked bytes. This is a rootless extraction proposal borrowing installed host libraries, not a portable standalone binary bundle. No package body was downloaded or extracted.

The inspected Debian cloud indexes publish plain SHA512SUMS without an advertised signature. The signed alternate is Ubuntu Minimal24.04 release20261103. Official Ubuntu documentation publishes signing fingerprint `D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81`; `sqv` verifies the detached signature over its SHA256SUMS. The expected image is260,112,384 bytes, SHA256 `bc9237bfdfa42b5f9a4404b6c6cab595d3e57599c6498f6664ad00e6192f9595`. Only HEAD and metadata were fetched: image hashing and boot remain untested.

Total downloaded metadata bodies:10,071,563 bytes, below the10MiB limit. `downloads.jsonl` preserves successful fetches and failed404 lookups. All transfers retained the inherited proxy and CA trust. APT ran in simulation mode against an owned configuration and the authenticated index; no system APT state was written. Its harmless absent disabled-config-directory warning remains in the raw stderr logs.

At the v2 resource snapshot the conservative full-envelope calculation fits with only205,819,904 bytes headroom. This is not reserved capacity. Root must resolve fresh capacity immediately before any binary/VM step; the same rule applies before guest2. No VM, container, daemon or service was started or changed. No repository source or ADR was edited during this metadata task.

The future gate is limited to two overlapping guest kernel/boot/machine identities, a restricted owned HTTP artifact round trip, exact host QEMU PID/startticks/pidfd stop and wait, and owned-file cleanup. It earns no Prefect runtime, NI2-053, hardware HA, zone independence or performance credit.

Offline verification commands, using this directory as the working directory:

```sh
/usr/bin/sqv --keyring /usr/share/keyrings/debian-archive-keyring.gpg --cleartext --output /tmp/owned-Release-check InRelease
/usr/bin/sqv --keyring ubuntu-cloud-pubkey.asc ubuntu-dated-SHA256SUMS.gpg ubuntu-dated-SHA256SUMS
```

Check the Release SHA256 entry against Packages.xz, then compare selected package records with `uefi-missing-package-closure.json`. The downloaded Ubuntu package manifest hash is also present in the signed SHA256SUMS. The raw APT configuration binds the original owned `/tmp` path and must be recreated against a new owned path when reproducing the simulation; do not write host APT configuration.
