"""Print complete numbered source lines; exposures do not certify reading."""

import argparse
import datetime
import hashlib
import json
import subprocess
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("path")
p.add_argument("--start", type=int, default=1)
p.add_argument("--lines", type=int, default=250)
p.add_argument("--reader", required=True)
a = p.parse_args()
study = Path(__file__).resolve().parents[1]
root = Path("/workspace/flink")
meta = json.loads((study / "corpus.json").read_text())
head = subprocess.check_output(
    ["git", "rev-parse", "HEAD"], cwd=root, text=True
).strip()
assert head == meta["commit"]
f = (root / a.path).resolve()
assert f.is_relative_to(root) and f.is_file()
assert 1 <= a.lines <= 500 and a.start >= 1
raw = f.read_bytes()
text = raw.decode("utf-8")
assert "\0" not in text
lines = text.splitlines()
end = min(len(lines), a.start + a.lines - 1)
assert a.start <= len(lines) or not lines
fragment = "\n".join(lines[a.start - 1 : end])
receipt = {
    "path": a.path,
    "reader": a.reader,
    "commit": head,
    "start": a.start,
    "end": end,
    "total_lines": len(lines),
    "file_sha256": hashlib.sha256(raw).hexdigest(),
    "slice_sha256": hashlib.sha256(fragment.encode()).hexdigest(),
    "utc": datetime.datetime.now(datetime.UTC).isoformat(),
    "kind": "exposure_only_not_read_confirmation",
}
assert a.reader.replace("_", "").isalnum()
log = study / "readers" / f"{a.reader}.exposures.jsonl"
with log.open("a") as out:
    out.write(json.dumps(receipt) + "\n")
print(json.dumps(receipt))
print("BEGIN COMPLETE SOURCE LINES")
for i, line in enumerate(lines[a.start - 1 : end], a.start):
    print(f"{i:6d} {line}")
print("END COMPLETE SOURCE LINES")
