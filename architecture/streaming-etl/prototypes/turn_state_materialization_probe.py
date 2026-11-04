"""Standalone protocol probe: materialize accepted turns without business replay.

This is SQLite exploration, not an SDK/backend implementation. Only explicitly
platform-managed JSON state is recoverable; Python frames and arbitrary locals
are not captured. External effects before an unaccepted turn remain client-owned.
The small ordered-mutation example is not an implementation of a native join.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

STATE_SCHEMA = "signed-counter/v1"
CODE_VERSION = "business/v1"
GENESIS = "0" * 64
INITIAL_STATE = {"handled": 0, "total": 0}
BEFORE_COMMIT_EXIT = 77
AFTER_COMMIT_EXIT = 78


class MaterializationError(RuntimeError):
    """Authoritative history cannot be used; never repair by rewinding."""


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def open_database(path: Path, *, create: bool = False) -> sqlite3.Connection:
    mode = "rwc" if create else "rw"
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode={mode}", uri=True, isolation_level=None
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


def initialize(path: Path) -> None:
    if path.exists():
        raise MaterializationError("Refuse to replace an existing database")
    with closing(open_database(path, create=True)) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript(
            """
            CREATE TABLE head (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                completed_through INTEGER NOT NULL,
                batch_digest TEXT NOT NULL
            );
            CREATE TABLE input_completions (
                sequence INTEGER PRIMARY KEY,
                input_id TEXT NOT NULL UNIQUE
            );
            CREATE TABLE published_outputs (
                sequence INTEGER PRIMARY KEY REFERENCES input_completions(sequence),
                output_id TEXT NOT NULL UNIQUE,
                reference TEXT NOT NULL
            );
            CREATE TABLE state_batches (
                sequence INTEGER PRIMARY KEY REFERENCES input_completions(sequence),
                payload TEXT NOT NULL,
                digest TEXT NOT NULL
            );
            CREATE TABLE snapshots (
                version INTEGER PRIMARY KEY,
                payload TEXT NOT NULL,
                digest TEXT NOT NULL
            );
            """
        )
        snapshot = {
            "state_version": 0,
            "state_schema": STATE_SCHEMA,
            "code_version": CODE_VERSION,
            "postimage": INITIAL_STATE,
            "batch_digest": GENESIS,
            "published_prefix_digest": digest([]),
        }
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO head VALUES (1,0,?)", (GENESIS,))
        db.execute(
            "INSERT INTO snapshots VALUES (0,?,?)",
            (canonical(snapshot), digest(snapshot)),
        )
        db.commit()


def _payload(row: sqlite3.Row) -> dict[str, Any]:
    value = json.loads(row["payload"])
    if not isinstance(value, dict) or digest(value) != row["digest"]:
        raise MaterializationError("Snapshot/batch checksum mismatch")
    return value


def _state(value: Any) -> dict[str, int]:
    if (
        not isinstance(value, dict)
        or set(value) != {"handled", "total"}
        or any(type(item) is not int for item in value.values())
    ):
        raise MaterializationError("Unsupported state postimage")
    return dict(value)


def _ledger(db: sqlite3.Connection) -> list[list[Any]]:
    return [
        list(row)
        for row in db.execute(
            "SELECT i.sequence,i.input_id,o.output_id,o.reference "
            "FROM input_completions i JOIN published_outputs o USING(sequence) "
            "ORDER BY i.sequence"
        )
    ]


def commit_yield(
    db: sqlite3.Connection,
    sequence: int,
    reference: str,
    postimage: dict[str, int],
    *,
    fault: str | None = None,
) -> bool:
    """One authoritative transaction; return False for an exact accepted retry."""
    if type(sequence) is not int or sequence < 1 or not reference:
        raise MaterializationError("Invalid turn identity")
    state = _state(postimage)
    db.execute("BEGIN IMMEDIATE")
    try:
        head = db.execute("SELECT * FROM head").fetchone()
        if head is None:
            raise MaterializationError("Missing authoritative head")
        if sequence <= head["completed_through"]:
            row = db.execute(
                "SELECT * FROM state_batches WHERE sequence=?", (sequence,)
            ).fetchone()
            if row is None:
                raise MaterializationError("Missing accepted batch")
            accepted = _payload(row)
            if accepted["postimage"] != state or accepted["reference"] != reference:
                raise MaterializationError("Accepted turn identity drift")
            db.commit()
            return False
        if sequence != head["completed_through"] + 1:
            raise MaterializationError("Input gap; refuse to move cursor")
        batch = {
            "apply_ordinal": sequence,
            "prior_version": sequence - 1,
            "state_version": sequence,
            "state_schema": STATE_SCHEMA,
            "code_version": CODE_VERSION,
            "previous_digest": head["batch_digest"],
            "input_id": f"input-{sequence:04}",
            "output_id": f"output-{sequence:04}",
            "reference": reference,
            "postimage": state,
        }
        db.execute(
            "INSERT INTO input_completions VALUES (?,?)", (sequence, batch["input_id"])
        )
        db.execute(
            "INSERT INTO published_outputs VALUES (?,?,?)",
            (sequence, batch["output_id"], reference),
        )
        db.execute(
            "INSERT INTO state_batches VALUES (?,?,?)",
            (sequence, canonical(batch), digest(batch)),
        )
        db.execute(
            "UPDATE head SET completed_through=?,batch_digest=? WHERE singleton=1",
            (sequence, digest(batch)),
        )
        if fault == "before_commit":
            os._exit(BEFORE_COMMIT_EXIT)
        db.commit()
        if fault == "after_commit":
            os._exit(AFTER_COMMIT_EXIT)
        return True
    except BaseException:
        db.rollback()
        raise


@dataclass(frozen=True)
class Restored:
    snapshot_version: int
    completed_through: int
    state: dict[str, int]
    applied_ordinals: tuple[int, ...]
    batch_digest: str
    ledger: list[list[Any]]

    def describe(self) -> dict[str, Any]:
        return {
            "snapshot_version": self.snapshot_version,
            "completed_through": self.completed_through,
            "next_input": self.completed_through + 1,
            "state": self.state,
            "materialized_batch_count": len(self.applied_ordinals),
            "first_materialized_ordinal": self.applied_ordinals[0]
            if self.applied_ordinals
            else None,
            "last_materialized_ordinal": self.applied_ordinals[-1]
            if self.applied_ordinals
            else None,
            "output_count": len(self.ledger),
            "input_ids_digest": digest([row[1] for row in self.ledger]),
            "output_ids_digest": digest([row[2] for row in self.ledger]),
            "published_ledger_digest": digest(self.ledger),
        }


def restore(
    db: sqlite3.Connection,
    *,
    state_schema: str = STATE_SCHEMA,
    code_version: str = CODE_VERSION,
) -> Restored:
    """Fold durable postimages, never invoke or accept a business callable."""
    db.execute("BEGIN")
    try:
        head = db.execute("SELECT * FROM head").fetchone()
        snapshot_row = db.execute(
            "SELECT * FROM snapshots ORDER BY version DESC LIMIT 1"
        ).fetchone()
        if head is None or snapshot_row is None:
            raise MaterializationError("Missing authoritative head/snapshot")
        snapshot = _payload(snapshot_row)
        version = snapshot["state_version"]
        through = head["completed_through"]
        if version != snapshot_row["version"] or not 0 <= version <= through:
            raise MaterializationError("Invalid snapshot version")
        rows = _ledger(db)
        if [row[0] for row in rows] != list(range(1, through + 1)):
            raise MaterializationError("Input/output ledger is incomplete")
        if digest(rows[:version]) != snapshot["published_prefix_digest"]:
            raise MaterializationError("Published snapshot prefix changed")
        if (
            snapshot["state_schema"] != state_schema
            or snapshot["code_version"] != code_version
        ):
            raise MaterializationError("Snapshot schema/code version mismatch")
        state = _state(snapshot["postimage"])
        previous_digest = snapshot["batch_digest"]
        applied = []
        batches = db.execute(
            "SELECT * FROM state_batches WHERE sequence>? AND sequence<=? ORDER BY sequence",
            (version, through),
        ).fetchall()
        if len(batches) != through - version:
            raise MaterializationError(
                "Missing materialization batch; no rewind allowed"
            )
        for expected, row in enumerate(batches, version + 1):
            batch = _payload(row)
            if (
                row["sequence"] != expected
                or batch["apply_ordinal"] != expected
                or batch["prior_version"] != expected - 1
                or batch["state_version"] != expected
                or batch["previous_digest"] != previous_digest
            ):
                raise MaterializationError("Batch order/version/hash-chain mismatch")
            if (
                batch["state_schema"] != state_schema
                or batch["code_version"] != code_version
            ):
                raise MaterializationError("Batch schema/code version mismatch")
            if [
                expected,
                batch["input_id"],
                batch["output_id"],
                batch["reference"],
            ] != rows[expected - 1]:
                raise MaterializationError("Accepted output/input binding changed")
            state = _state(batch["postimage"])
            previous_digest = row["digest"]
            applied.append(expected)
        if previous_digest != head["batch_digest"]:
            raise MaterializationError("Final batch does not match authoritative head")
        return Restored(version, through, state, tuple(applied), previous_digest, rows)
    except (sqlite3.Error, ValueError, TypeError, KeyError) as error:
        raise MaterializationError(
            "Unreadable authoritative materialization"
        ) from error
    finally:
        db.rollback()


def save_snapshot(db: sqlite3.Connection) -> None:
    state = restore(db)
    payload = {
        "state_version": state.completed_through,
        "state_schema": STATE_SCHEMA,
        "code_version": CODE_VERSION,
        "postimage": state.state,
        "batch_digest": state.batch_digest,
        "published_prefix_digest": digest(state.ledger),
    }
    db.execute("BEGIN IMMEDIATE")
    try:
        head = db.execute("SELECT * FROM head").fetchone()
        if (
            head["completed_through"] != state.completed_through
            or head["batch_digest"] != state.batch_digest
        ):
            raise MaterializationError("Head moved while snapshot was being prepared")
        db.execute(
            "INSERT INTO snapshots VALUES (?,?,?)",
            (state.completed_through, canonical(payload), digest(payload)),
        )
        db.commit()
    except BaseException:
        db.rollback()
        raise


def business(
    sequence: int, state: dict[str, int], audit: Path
) -> tuple[str, dict[str, int]]:
    """An external effect, deliberately outside the platform transaction."""
    descriptor = os.open(audit, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, f"{sequence}\n".encode())
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return f"object://signed/{sequence:04}.json", {
        "handled": state["handled"] + 1,
        "total": state["total"] + sequence,
    }


def audit_count(path: Path) -> int:
    return len(path.read_text().splitlines()) if path.exists() else 0


def run_worker(path: Path, audit: Path, fault: str | None) -> None:
    initialize(path)
    state = dict(INITIAL_STATE)
    with closing(open_database(path)) as db:
        for sequence in range(1, 201):
            reference, state = business(sequence, state, audit)
            commit_yield(
                db, sequence, reference, state, fault=fault if sequence == 200 else None
            )
            if sequence == 100:
                save_snapshot(db)


def ordering_counterexample(path: Path) -> dict[str, Any]:
    """Persist a global order for four platform mutations from two input ports."""
    with closing(open_database(path, create=True)) as db:
        db.execute(
            "CREATE TABLE order_probe (apply_ordinal INTEGER PRIMARY KEY,port TEXT,port_sequence INTEGER,operation TEXT,operand INTEGER,UNIQUE(port,port_sequence))"
        )
        db.execute("BEGIN IMMEDIATE")
        db.executemany(
            "INSERT INTO order_probe VALUES (?,?,?,?,?)",
            [
                (1, "left", 1, "add", 1),
                (2, "right", 1, "multiply", 10),
                (3, "left", 2, "add", 2),
                (4, "right", 2, "multiply", 10),
            ],
        )
        db.commit()
    with closing(open_database(path)) as db:
        global_rows = db.execute(
            "SELECT * FROM order_probe ORDER BY apply_ordinal"
        ).fetchall()
        grouped_rows = db.execute(
            "SELECT * FROM order_probe ORDER BY port,port_sequence"
        ).fetchall()

    def apply(rows: list[sqlite3.Row], *, enforce_ordinal: bool) -> int:
        value = 1
        for ordinal, row in enumerate(rows, 1):
            if enforce_ordinal and row["apply_ordinal"] != ordinal:
                raise MaterializationError(
                    "Per-port grouping broke global apply_ordinal"
                )
            value = (
                value + row["operand"]
                if row["operation"] == "add"
                else value * row["operand"]
            )
        return value

    try:
        apply(grouped_rows, enforce_ordinal=True)
    except MaterializationError:
        grouping_refused = True
    else:
        grouping_refused = False
    return {
        "global_order_result": apply(global_rows, enforce_ordinal=True),
        "naive_port_grouped_result": apply(grouped_rows, enforce_ordinal=False),
        "strict_grouping_refused": grouping_refused,
        "port_cursors": {"left": 2, "right": 2},
        "native_join_implemented": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("worker", "restore"))
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--audit", required=True, type=Path)
    parser.add_argument("--fault", choices=("before_commit", "after_commit"))
    arguments = parser.parse_args()
    if arguments.mode == "worker":
        run_worker(arguments.database, arguments.audit, arguments.fault)
        return 0
    before = audit_count(arguments.audit)
    try:
        with closing(open_database(arguments.database)) as db:
            report = restore(db).describe()
    except (MaterializationError, sqlite3.Error) as error:
        print(
            canonical(
                {
                    "ok": False,
                    "reason": str(error),
                    "business_audit_before": before,
                    "business_audit_after": audit_count(arguments.audit),
                }
            )
        )
        return 2
    print(
        canonical(
            {
                "ok": True,
                **report,
                "business_audit_before": before,
                "business_audit_after": audit_count(arguments.audit),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
