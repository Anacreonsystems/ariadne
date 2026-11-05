"""Count real access-log requests without retaining credentials or payloads."""

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    raw = args.log.read_bytes()
    pattern = re.compile(r'"(GET|POST|PATCH|DELETE|PUT) ([^ ]+) HTTP/[\d.]+" (\d{3})')
    uuid = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
    counts: Counter[tuple[str, str, int]] = Counter()
    normalized = []
    for line in raw.decode("utf-8").splitlines():
        if "uvicorn.access" not in line:
            continue
        match = pattern.search(line)
        if match is None:
            continue
        method, path, status = match.groups()
        path = uuid.sub(":id", path.split("?", 1)[0])
        path = re.sub(r"/work_pools/[^/]+/", "/work_pools/:pool/", path)
        counts[(method, path, int(status))] += 1
        normalized.append(f"{line.split()[0]} {method} {path} {status}\n")
    assert counts, "No actual access-log requests were found"
    retained = "".join(normalized).encode()
    (args.output / "normalized-access.log").write_bytes(retained)
    record_paths = {
        "/api/streams/v1/consumers/read",
        "/api/streams/v1/consumers/complete",
        "/api/streams/v1/sources/append",
    }
    record_count = sum(
        n
        for (method, path, _), n in counts.items()
        if method == "POST" and path in record_paths
    )
    total = sum(counts.values())
    report = {
        "schema": "native-http-access-counts-v1",
        "source_log_sha256": hashlib.sha256(raw).hexdigest(),
        "normalized_access_sha256": hashlib.sha256(retained).hexdigest(),
        "requests": total,
        "record_read_append_complete_requests": record_count,
        "other_requests": total - record_count,
        "other_fraction": (total - record_count) / total,
        "routes": [
            {"method": method, "route": path, "status": status, "requests": count}
            for (method, path, status), count in counts.most_common()
        ],
        "native_readiness_earned_percent": 0,
        "limits": [
            "Request counts do not establish latency or the cause of wall-clock duration.",
            "Other requests include fixture observation, scheduling, metadata, lifecycle and health traffic.",
            "This is a functional test observation, not a production throughput or resource acceptance.",
            "Only normalized access records are retained; query values, headers and payloads are excluded.",
        ],
    }
    (args.output / "counts.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print(
        json.dumps(
            {
                "requests": total,
                "record_requests": record_count,
                "other_requests": total - record_count,
            }
        )
    )


if __name__ == "__main__":
    main()
