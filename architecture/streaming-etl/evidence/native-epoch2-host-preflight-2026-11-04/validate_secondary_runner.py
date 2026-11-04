"""Run the exact manual discovery scripts against bounded classification controls."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import yaml


def python_body(step: dict) -> str:
    prefix = "python3 - <<'PY'\n"
    command = step["run"]
    assert command.startswith(prefix)
    body, suffix = command[len(prefix) :].rsplit("\nPY", 1)
    assert not suffix.strip()
    return body


def main() -> None:
    source_path = Path(".github/workflows/native-stream-host-preflight.yml")
    workflow_path = source_path.resolve()
    workflow = yaml.safe_load(workflow_path.read_text())
    dispatch = workflow.get("on", workflow.get(True))["workflow_dispatch"]
    assert dispatch["inputs"]["secondary_runner"]["options"] == ["arc", "static"]
    observe = workflow["jobs"]["observe"]
    assert observe["timeout-minutes"] == 5
    assert observe["strategy"]["max-parallel"] == 2
    assert observe["strategy"]["matrix"] == {"probe": ["a", "b"]}
    assert observe["runs-on"] == (
        "${{ matrix.probe == 'b' && inputs.secondary_runner == 'static' && "
        'fromJSON(\'["self-hosted","Linux","X64"]\') || \'anacreon-runners\' }}'
    )
    probe = python_body(observe["steps"][0])
    classifier = python_body(workflow["jobs"]["compare"]["steps"][1])
    repository, run_id, attempt = "owner/repo", "12345", "1"
    salt = f"{repository}/{run_id}/{attempt}"
    salt_digest = hashlib.sha256(salt.encode()).hexdigest()
    results: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="native-host-classifier-") as directory:
        with contextlib.chdir(directory):
            environment = {
                "PROBE_SALT": salt,
                "PROBE_ID": "a",
                "GITHUB_REPOSITORY": repository,
                "GITHUB_RUN_ID": run_id,
                "GITHUB_RUN_ATTEMPT": attempt,
                "GITHUB_STEP_SUMMARY": str(Path(directory) / "summary.md"),
            }
            with patch.dict(os.environ, environment):
                waits: list[int] = []
                with patch("time.sleep", side_effect=waits.append):
                    with contextlib.redirect_stdout(io.StringIO()):
                        exec(compile(probe, str(workflow_path), "exec"), {})
                assert waits == [45]
                observed = json.loads(Path("native-preflight-a.json").read_text())
                assert observed["scope"] == "NO_NATIVE_ACCEPTANCE"
                assert observed["salt_sha256"] == salt_digest
                for path, domain, field in (
                    (
                        "/proc/sys/kernel/random/boot_id",
                        b"kernel-boot-v1",
                        "boot_sha256",
                    ),
                    ("/etc/machine-id", b"machine-v1", "machine_sha256"),
                ):
                    raw = Path(path).read_bytes().strip()
                    expected = hashlib.sha256(
                        domain + b"\0" + salt.encode() + b"\0" + raw
                    ).hexdigest()
                    assert observed[field] == expected
                    assert raw.decode() not in json.dumps(observed)
                results.append(
                    {"case": "probe_hash_domains_and_45_second_bound", "passed": True}
                )

                start = datetime(2026, 11, 4, tzinfo=timezone.utc)
                first = {
                    "scope": "NO_NATIVE_ACCEPTANCE",
                    "salt_sha256": salt_digest,
                    "boot_sha256": "1" * 64,
                    "machine_sha256": "2" * 64,
                    "started_at_utc": start.isoformat(),
                    "completed_at_utc": (start + timedelta(seconds=45)).isoformat(),
                }
                second = {**first, "boot_sha256": "3" * 64, "machine_sha256": "4" * 64}
                scenarios = [
                    (
                        "distinct_overlapping",
                        {},
                        "INFRASTRUCTURE_PREREQUISITE_OBSERVED",
                        "DISTINCT_SALTED_IDENTITIES_WITH_UTC_OVERLAP",
                    ),
                    (
                        "shared_boot",
                        {"boot_sha256": first["boot_sha256"]},
                        "UNPROVEN",
                        "SHARED_OBSERVED_IDENTITY",
                    ),
                    (
                        "shared_machine",
                        {"machine_sha256": first["machine_sha256"]},
                        "UNPROVEN",
                        "SHARED_OBSERVED_IDENTITY",
                    ),
                    (
                        "missing_identity",
                        {"boot_sha256": None},
                        "UNPROVEN",
                        "MISSING_IDENTITY",
                    ),
                    (
                        "cold_queue",
                        {
                            "started_at_utc": (
                                start + timedelta(seconds=45)
                            ).isoformat(),
                            "completed_at_utc": (
                                start + timedelta(seconds=90)
                            ).isoformat(),
                        },
                        "UNPROVEN",
                        "INSUFFICIENT_OBSERVED_OVERLAP_OR_COLD_QUEUE",
                    ),
                    (
                        "overlap_below_30_seconds",
                        {
                            "started_at_utc": (
                                start + timedelta(seconds=15, microseconds=1)
                            ).isoformat()
                        },
                        "UNPROVEN",
                        "INSUFFICIENT_OBSERVED_OVERLAP_OR_COLD_QUEUE",
                    ),
                    (
                        "overlap_exactly_30_seconds",
                        {"started_at_utc": (start + timedelta(seconds=15)).isoformat()},
                        "INFRASTRUCTURE_PREREQUISITE_OBSERVED",
                        "DISTINCT_SALTED_IDENTITIES_WITH_UTC_OVERLAP",
                    ),
                    (
                        "wrong_salt",
                        {"salt_sha256": "f" * 64},
                        "UNPROVEN",
                        "MISSING_OR_INVALID_OBSERVATION",
                    ),
                    (
                        "unbounded_interval",
                        {
                            "completed_at_utc": (
                                start + timedelta(seconds=61)
                            ).isoformat()
                        },
                        "UNPROVEN",
                        "MISSING_OR_INVALID_OBSERVATION",
                    ),
                    (
                        "invalid_timestamp",
                        {"started_at_utc": "invalid"},
                        "UNPROVEN",
                        "MISSING_OR_INVALID_OBSERVATION",
                    ),
                    (
                        "unaware_timestamp",
                        {"started_at_utc": start.replace(tzinfo=None).isoformat()},
                        "UNPROVEN",
                        "MISSING_OR_INVALID_OBSERVATION",
                    ),
                    (
                        "extra_field",
                        {"unexpected": True},
                        "UNPROVEN",
                        "MISSING_OR_INVALID_OBSERVATION",
                    ),
                    (
                        "invalid_hash_type",
                        {"boot_sha256": 12},
                        "UNPROVEN",
                        "MISSING_OR_INVALID_OBSERVATION",
                    ),
                    (
                        "missing_artifact",
                        None,
                        "UNPROVEN",
                        "MISSING_OR_INVALID_OBSERVATION",
                    ),
                ]
                folder = Path("native-host-observations")
                folder.mkdir()
                for name, changes, expected_class, expected_reason in scenarios:
                    (folder / "native-preflight-a.json").write_text(json.dumps(first))
                    candidate = folder / "native-preflight-b.json"
                    candidate.unlink(missing_ok=True)
                    if changes is not None:
                        candidate.write_text(
                            json.dumps({**copy.deepcopy(second), **changes})
                        )
                    with contextlib.redirect_stdout(io.StringIO()):
                        exec(compile(classifier, str(workflow_path), "exec"), {})
                    result = json.loads(
                        Path("native-host-preflight-result.json").read_text()
                    )
                    assert result["classification"] == expected_class, (name, result)
                    assert result["reason"] == expected_reason, (name, result)
                    assert result["scope"] == "NO_NATIVE_ACCEPTANCE"
                    results.append({"case": name, "passed": True})
    print(
        json.dumps(
            {
                "schema": "native-host-secondary-runner-validation-v1",
                "scope": "NO_NATIVE_ACCEPTANCE",
                "source_path": str(source_path),
                "source_sha256": hashlib.sha256(workflow_path.read_bytes()).hexdigest(),
                "scenario_count": len(results),
                "scenarios": results,
                "native_readiness_earned_percent": 0,
                "limits": "Synthetic classifier and local salted probe controls only; no actual remote host, network, child launch or verified stop exercised.",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
