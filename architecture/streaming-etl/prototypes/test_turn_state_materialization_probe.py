"""Protocol-only checks. Invoke with stdlib unittest, not the SDK test harness."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import turn_state_materialization_probe as probe

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "turn_state_materialization_probe.py"


class TurnStateMaterializationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.database = self.directory / "turns.sqlite"
        self.audit = self.directory / "external-effects.log"

    def worker(self, fault: str | None = None) -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(SCRIPT),
            "worker",
            "--database",
            str(self.database),
            "--audit",
            str(self.audit),
        ]
        if fault:
            command.extend(["--fault", fault])
        return subprocess.run(command, capture_output=True, text=True, timeout=20)

    def fresh_restore(self, expected_status: int = 0) -> dict:
        child = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "restore",
                "--database",
                str(self.database),
                "--audit",
                str(self.audit),
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        self.assertEqual(child.returncode, expected_status, child.stdout + child.stderr)
        return json.loads(child.stdout)

    def fresh_resume_one_step(self) -> dict:
        code = textwrap.dedent(
            """
            import json
            import sys
            from contextlib import closing
            from pathlib import Path
            sys.path.insert(0, sys.argv[1])
            import turn_state_materialization_probe as probe
            database, audit = Path(sys.argv[2]), Path(sys.argv[3])
            audit_before = probe.audit_count(audit)
            with closing(probe.open_database(database)) as db:
                before = probe.restore(db)
                audit_after_restore = probe.audit_count(audit)
                next_input = before.completed_through + 1
                reference, state = probe.business(next_input, before.state, audit)
                probe.commit_yield(db, next_input, reference, state)
                after = probe.restore(db)
            print(json.dumps({
                "restored": before.describe(),
                "resumed": after.describe(),
                "business_input_executed": next_input,
                "business_audit_before_restore": audit_before,
                "business_audit_after_restore": audit_after_restore,
                "business_audit_after_resume": probe.audit_count(audit),
                "external_effect_tail": audit.read_text().splitlines()[-3:],
                "accepted_output_prefix_unchanged": after.ledger[:len(before.ledger)] == before.ledger,
            }, sort_keys=True))
            """
        )
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                str(HERE),
                str(self.database),
                str(self.audit),
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        self.assertEqual(child.returncode, 0, child.stdout + child.stderr)
        return json.loads(child.stdout)

    def observe(self, **observation: object) -> None:
        print(
            "PROBE_OBSERVATION "
            + json.dumps({"test": self._testMethodName, **observation}, sort_keys=True),
            flush=True,
        )

    def assert_platform(self, through: int) -> probe.Restored:
        with closing(probe.open_database(self.database)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                db.execute("SELECT completed_through FROM head").fetchone()[0], through
            )
            for table in ("input_completions", "published_outputs", "state_batches"):
                self.assertEqual(
                    db.execute(f"SELECT count(*) FROM {table}").fetchone()[0], through
                )
            restored = probe.restore(db)
            self.assertEqual(
                restored.state,
                {"handled": through, "total": through * (through + 1) // 2},
            )
            self.assertEqual(
                [row[1] for row in restored.ledger],
                [f"input-{i:04}" for i in range(1, through + 1)],
            )
            self.assertEqual(
                [row[2] for row in restored.ledger],
                [f"output-{i:04}" for i in range(1, through + 1)],
            )
            return restored

    def test_snapshot100_and_batches_restore200_without_business_callable(self) -> None:
        child = self.worker()
        self.assertEqual(child.returncode, 0, child.stderr)
        original = self.assert_platform(200)
        cache = dict(original.state)
        cache.clear()
        # A complete snapshot makes older state batches unnecessary. Keep the
        # published output ledger; remove only state materialization1..100.
        with closing(probe.open_database(self.database)) as db:
            db.execute("DELETE FROM state_batches WHERE sequence<=100")
            self.assertEqual(
                db.execute("SELECT count(*) FROM state_batches").fetchone()[0], 100
            )
        with patch.object(
            probe,
            "business",
            side_effect=AssertionError(
                "Business code cannot run during materialization"
            ),
        ):
            with closing(probe.open_database(self.database)) as db:
                restored = probe.restore(db)
        self.assertEqual(restored.state, original.state)
        self.assertEqual(restored.ledger, original.ledger)
        self.assertEqual(restored.applied_ordinals, tuple(range(101, 201)))
        report = self.fresh_restore()
        self.assertEqual(report["snapshot_version"], 100)
        self.assertEqual(report["next_input"], 201)
        self.assertEqual(report["business_audit_before"], 200)
        self.assertEqual(report["business_audit_after"], 200)
        self.assertEqual(
            report["published_ledger_digest"], probe.digest(original.ledger)
        )
        self.observe(
            **report,
            business_calls_during_restore=0,
            process_boundary=True,
            compacted_pre_snapshot_batches=100,
        )

    def test_process_exit_before_commit200_redelivers200_and_rolls_back_all_platform_writes(
        self,
    ) -> None:
        child = self.worker("before_commit")
        self.assertEqual(child.returncode, probe.BEFORE_COMMIT_EXIT, child.stderr)
        restored = self.assert_platform(199)
        self.assertEqual(restored.applied_ordinals, tuple(range(101, 200)))
        report = self.fresh_restore()
        self.assertEqual(report["next_input"], 200)
        # Business effect200 happened outside the transaction. Recovery does not
        # erase it or promise external exactly-once behavior for redelivery200.
        self.assertEqual(
            self.audit.read_text().splitlines(), [str(i) for i in range(1, 201)]
        )
        self.assertEqual(report["business_audit_before"], 200)
        self.assertEqual(report["business_audit_after"], 200)
        self.observe(
            **report,
            exit_code=child.returncode,
            all_platform_writes_rolled_back_at200=True,
        )

    def test_process_exit_after_commit200_lost_ack_restores201_with_identical_outputs(
        self,
    ) -> None:
        child = self.worker("after_commit")
        self.assertEqual(child.returncode, probe.AFTER_COMMIT_EXIT, child.stderr)
        original = self.assert_platform(200)
        report = self.fresh_restore()
        self.assertEqual(report["next_input"], 201)
        self.assertEqual(report["business_audit_before"], 200)
        self.assertEqual(report["business_audit_after"], 200)
        self.assertEqual(
            report["published_ledger_digest"], probe.digest(original.ledger)
        )
        self.observe(**report, exit_code=child.returncode, accepted_but_ack_lost=True)

    def test_fresh_process_really_resumes200_or201_and_exposes_external_effect_boundary(
        self,
    ) -> None:
        for fault, accepted, delivered in (
            ("before_commit", 199, 200),
            ("after_commit", 200, 201),
        ):
            with self.subTest(fault=fault):
                self.database = self.directory / f"{fault}.sqlite"
                self.audit = self.directory / f"{fault}-effects.log"
                child = self.worker(fault)
                expected_exit = (
                    probe.BEFORE_COMMIT_EXIT
                    if fault == "before_commit"
                    else probe.AFTER_COMMIT_EXIT
                )
                self.assertEqual(child.returncode, expected_exit, child.stderr)
                report = self.fresh_resume_one_step()
                self.assertEqual(report["restored"]["completed_through"], accepted)
                self.assertEqual(report["business_audit_before_restore"], 200)
                self.assertEqual(report["business_audit_after_restore"], 200)
                self.assertEqual(report["business_input_executed"], delivered)
                self.assertEqual(report["resumed"]["completed_through"], delivered)
                self.assertEqual(report["resumed"]["next_input"], delivered + 1)
                self.assertEqual(report["business_audit_after_resume"], 201)
                self.assertTrue(report["accepted_output_prefix_unchanged"])
                self.assertEqual(
                    report["external_effect_tail"], ["199", "200", str(delivered)]
                )
                effects = self.audit.read_text().splitlines()
                self.assertEqual(
                    effects.count("200"), 2 if fault == "before_commit" else 1
                )
                self.assert_platform(delivered)
                self.observe(
                    **report,
                    fault=fault,
                    exit_code=child.returncode,
                    process_boundary=True,
                    duplicate_external_effect200=fault == "before_commit",
                )

    def test_exact_accepted_retry_does_not_repeat_business_or_publish_another_output(
        self,
    ) -> None:
        self.assertEqual(
            self.worker("after_commit").returncode, probe.AFTER_COMMIT_EXIT
        )
        before = self.assert_platform(200)
        with closing(probe.open_database(self.database)) as db:
            accepted = json.loads(
                db.execute(
                    "SELECT payload FROM state_batches WHERE sequence=200"
                ).fetchone()[0]
            )
            self.assertFalse(
                probe.commit_yield(
                    db, 200, accepted["reference"], accepted["postimage"]
                )
            )
            with self.assertRaisesRegex(probe.MaterializationError, "identity drift"):
                probe.commit_yield(db, 200, "object://different", accepted["postimage"])
        after = self.assert_platform(200)
        self.assertEqual(after.ledger, before.ledger)
        self.assertEqual(probe.audit_count(self.audit), 200)
        self.observe(
            next_input=201,
            published_outputs=200,
            business_audit=200,
            exact_retry_added_outputs=0,
        )

    def test_missing_batch151_fails_closed_without_rewind_or_business(self) -> None:
        self.assertEqual(self.worker().returncode, 0)
        original = self.assert_platform(200)
        with closing(probe.open_database(self.database)) as db:
            db.execute("DELETE FROM state_batches WHERE sequence=151")
        report = self.fresh_restore(expected_status=2)
        self.assertFalse(report["ok"])
        self.assertIn("Missing materialization batch", report["reason"])
        self.assert_refusal_did_not_mutate(original, report)

    def test_corrupt_batch151_fails_closed_without_rewind_or_business(self) -> None:
        self.assertEqual(self.worker().returncode, 0)
        original = self.assert_platform(200)
        with closing(probe.open_database(self.database)) as db:
            payload = json.loads(
                db.execute(
                    "SELECT payload FROM state_batches WHERE sequence=151"
                ).fetchone()[0]
            )
            payload["postimage"]["total"] = -1
            db.execute(
                "UPDATE state_batches SET payload=? WHERE sequence=151",
                (probe.canonical(payload),),
            )
        report = self.fresh_restore(expected_status=2)
        self.assertFalse(report["ok"])
        self.assertIn("checksum mismatch", report["reason"])
        self.assert_refusal_did_not_mutate(original, report)

    def assert_refusal_did_not_mutate(
        self, original: probe.Restored, report: dict
    ) -> None:
        with closing(probe.open_database(self.database)) as db:
            self.assertEqual(
                db.execute("SELECT completed_through FROM head").fetchone()[0], 200
            )
            self.assertEqual(
                probe.digest(probe._ledger(db)), probe.digest(original.ledger)
            )
        self.assertEqual(report["business_audit_before"], 200)
        self.assertEqual(report["business_audit_after"], 200)
        self.observe(
            **report, authoritative_cursor=200, no_rewind=True, no_business_replay=True
        )

    def test_schema_and_code_version_drift_are_refused(self) -> None:
        self.assertEqual(self.worker().returncode, 0)
        with closing(probe.open_database(self.database)) as db:
            for arguments in (
                {"state_schema": "signed-counter/v2"},
                {"code_version": "business/v2"},
            ):
                with self.subTest(arguments=arguments):
                    with self.assertRaisesRegex(
                        probe.MaterializationError, "schema/code version"
                    ):
                        probe.restore(db, **arguments)
            self.assertEqual(
                db.execute("SELECT completed_through FROM head").fetchone()[0], 200
            )
        self.assertEqual(probe.audit_count(self.audit), 200)
        self.observe(
            schema_mismatch_refused=True,
            code_mismatch_refused=True,
            authoritative_cursor=200,
            business_audit=200,
        )

    def test_global_apply_ordinal_is_required_for_noncommutative_two_port_mutations(
        self,
    ) -> None:
        result = probe.ordering_counterexample(self.directory / "ordered.sqlite")
        self.assertEqual(result["global_order_result"], 220)
        self.assertEqual(result["naive_port_grouped_result"], 400)
        self.assertTrue(result["strict_grouping_refused"])
        self.assertEqual(result["port_cursors"], {"left": 2, "right": 2})
        self.assertFalse(result["native_join_implemented"])
        self.observe(**result)


if __name__ == "__main__":
    unittest.main()
