"""Validate reading receipts and union line ranges; never infer reading from exposure."""

import collections
import datetime
import hashlib
import json
from pathlib import Path


def merge_ranges(ranges):
    result = []
    for start, end in sorted(ranges):
        if result and start <= result[-1][1] + 1:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([start, end])
    return result


def main():
    study = Path(__file__).resolve().parents[1]
    corpus = json.loads((study / "corpus.json").read_text())
    raw = (study / "inventory.jsonl").read_bytes()
    if hashlib.sha256(raw).hexdigest() != corpus["inventory_sha256"]:
        raise ValueError("Inventory changed: preserve the fixed reading obligation")
    inventory = {row["path"]: row for row in map(json.loads, raw.splitlines())}
    ranges = collections.defaultdict(list)
    readers = collections.defaultdict(set)
    receipts = 0
    for ledger in sorted((study / "readers").glob("*.confirmed.jsonl")):
        for number, line in enumerate(ledger.read_text().splitlines(), 1):
            try:
                row = json.loads(line)
                item = inventory[row["path"]]
                assert item["kind"] == "text"
                assert row["file_sha256"] == item["sha256"]
                start, end = row["start"], row["end"]
                assert type(start) is int and type(end) is int
                assert (1 <= start <= end <= item["lines"]) or (
                    item["lines"] == 0 and start == 1 and end == 0
                )
                assert row.get("note", "").strip()
                if "commit" in row:
                    assert row["commit"] == corpus["commit"]
                if end:
                    ranges[row["path"]].append((start, end))
                readers[row["path"]].add(ledger.name.removesuffix(".confirmed.jsonl"))
                receipts += 1
            except (KeyError, ValueError, AssertionError) as error:
                raise ValueError(
                    f"Invalid receipt {ledger.name}:{number}: {line}"
                ) from error
    totals = collections.defaultdict(collections.Counter)
    statuses = []
    for path, item in inventory.items():
        covered = merge_ranges(ranges[path])
        count = sum(end - start + 1 for start, end in covered)
        full = item["kind"] == "text" and path in readers and count == item["lines"]
        status = "fully_read" if full else "partially_read" if count else "unread"
        totals[item["owner"]][status + "_files"] += 1
        totals[item["owner"]]["confirmed_text_lines"] += count
        statuses.append(
            {
                "path": path,
                "status": status,
                "confirmed_ranges": covered,
                "confirmed_text_lines": count,
                "readers": sorted(readers[path]),
            }
        )
    count = sum(item["confirmed_text_lines"] for item in statuses)
    summary = {
        "commit": corpus["commit"],
        "inventory_sha256": corpus["inventory_sha256"],
        "generated_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "confirmed_receipts": receipts,
        "confirmed_text_lines": count,
        "total_text_lines": corpus["text_lines"],
        "text_line_reading_percent": round(count * 100 / corpus["text_lines"], 4),
        "fully_read_text_files": sum(row["status"] == "fully_read" for row in statuses),
        "total_text_files": corpus["text_files"],
        "by_inventory_owner": dict(totals),
        "meaning": "Self-attested consumed line ranges, deduplicated across readers. This is reading coverage, not implementation readiness, correctness proof, or executed-test coverage. Binary resources remain inventoried and unreviewed.",
    }
    (study / "reading-status.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in statuses)
    )
    (study / "coverage.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
