"""Verify real served entry and transport bytes against the fresh package build."""

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx

root = Path("/workspace/ariadne")
handoff = json.loads(Path("/tmp/native-ui-live/handoff.json").read_text())
url = handoff["flow_url"]
parsed = urlsplit(url)
base_path = parsed.path.split("/runs/flow-run/", 1)[0]
username, _, password = handoff["api_auth"].partition(":")
package = root / "src/prefect/server/ui-v2"
local_index = (
    (package / "index.html")
    .read_text()
    .replace("/PREFECT_UI_SERVE_BASE_REPLACE_PLACEHOLDER", base_path)
)
entry_pattern = r'<script[^>]+src="([^"]+)"'
expected_entries = re.findall(entry_pattern, local_index)
assert len(expected_entries) == 1
markers = [
    path
    for path in (package / "assets").glob("*.js")
    if b"API request failed with status" in path.read_bytes()
]
assert len(markers) == 1
with httpx.Client(auth=(username, password), timeout=30) as client:
    page = client.get(url, headers={"Accept": "text/html"})
    assert page.status_code == 200
    assert re.findall(entry_pattern, page.text) == expected_entries
    paths = [
        expected_entries[0],
        base_path + "/assets/" + markers[0].name,
        base_path + "/mockServiceWorker.js",
    ]
    rows = []
    for path in paths:
        asset_url = urljoin(url, path)
        assert urlsplit(asset_url).netloc == parsed.netloc
        response = client.get(asset_url)
        assert response.status_code == 200
        relative = urlsplit(asset_url).path.removeprefix(base_path + "/")
        expected = (
            (package / relative)
            .read_bytes()
            .replace(b"/PREFECT_UI_SERVE_BASE_REPLACE_PLACEHOLDER", base_path.encode())
        )
        assert response.content == expected, relative
        rows.append(
            {
                "path": path,
                "status": response.status_code,
                "content_sha256": hashlib.sha256(response.content).hexdigest(),
                "matches_fresh_package": True,
            }
        )
record = {
    "scope": "Actual HTTP static assets; no mocked responses",
    "flow_run_id": parsed.path.rsplit("/", 1)[1],
    "entry_references_match_fresh_package": True,
    "new_native_error_transport_marker": True,
    "worker_origin_guard_present": "event.origin !== self.location.origin"
    in (package / "mockServiceWorker.js").read_text(),
    "assets": rows,
}
Path("/tmp/native-stage9-ui-codeql-live-bundle.json").write_text(
    json.dumps(record, indent=2) + "\n"
)
print(json.dumps(record))
