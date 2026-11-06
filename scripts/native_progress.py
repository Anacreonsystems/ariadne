"""Read-only, five-second P2 diagnostic telemetry; no execution semantics."""

import asyncio
import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest

OUTPUT = Path(os.environ["NATIVE_PROGRESS_DIR"])
SOURCE = Path(os.environ["NATIVE_PROGRESS_SOURCE_ROOT"]).resolve()
BASETEMP = Path(os.environ["NATIVE_PROGRESS_BASETEMP"]).resolve()
STARTED = time.monotonic()
ROOT_PID = os.getpid()
STOP = threading.Event()
REQUEST = threading.Event()
DONE = threading.Event()
PAUSED = threading.Event()
SEEN = {}
LAST_VECTOR = None
LAST_ADVANCED = STARTED
SAMPLES = 0
PROFILE = os.environ.get("NATIVE_PROGRESS_PROFILE", "partitioned-fixed-v1")
if PROFILE not in {"partitioned-fixed-v1", "partitioned-dag-v1"}:
    raise ValueError("Unknown native progress profile")

SQL = {
    "flows": """SELECT f.id,f.name,f.state_type::text,f.state_name,f.run_count,f.updated,
   (f.parameters->>'count') AS expected_count,g.id AS graph_id,g.state AS graph_state,
   g.startup_completed_at,g.activation_operation_id
   FROM flow_run f JOIN stream_graph_revision g ON g.flow_run_id=f.id
   WHERE g.profile='partitioned-fixed-v1' ORDER BY f.created""",
    "tasks": """SELECT s.id,s.graph_revision_id,o.operator_uid,s.subtask_index,s.task_run_id,
   t.state_type::text AS task_state,t.state_name,t.run_count,t.updated AS task_updated,
   p.id AS output_stream_id,p.state AS output_state,p.producer_position,p.last_sequence,
   p.tail_sequence,p.record_count,p.byte_count,p.released AS output_released,
   s.physical_slot_released,s.physical_epoch,s.assigned_worker_id,
   g.state AS grant_state,g.child_process_id,g.process_start_ticks,g.last_renewed_at
   FROM stream_physical_subtask s JOIN stream_graph_revision r ON r.id=s.graph_revision_id
   JOIN stream_operator_run o ON o.id=s.operator_run_id
   JOIN task_run t ON t.id=s.task_run_id JOIN stream_partition p ON p.id=s.output_stream_id
   LEFT JOIN stream_subtask_grant g ON g.id=s.current_grant_id
   WHERE r.profile='partitioned-fixed-v1' ORDER BY s.graph_revision_id,o.operator_uid,s.subtask_index""",
    "consumers": """SELECT c.id,c.flow_run_id,c.input_stream_id,c.output_stream_id,
   c.completed_through,c.inflight_sequence,c.attempt,c.owner_epoch
   FROM stream_consumer c JOIN stream_graph_revision g ON g.flow_run_id=c.flow_run_id
   WHERE g.profile='partitioned-fixed-v1' ORDER BY c.id""",
    "records": """SELECT r.stream_id,r.record_kind,COUNT(*) AS rows
   FROM stream_record r JOIN stream_partition p ON p.id=r.stream_id
   JOIN stream_graph_revision g ON g.flow_run_id=p.flow_run_id
   WHERE g.profile='partitioned-fixed-v1' GROUP BY r.stream_id,r.record_kind ORDER BY r.stream_id,r.record_kind""",
    "capacity": """SELECT name,active_slots,"limit" FROM concurrency_limit_v2
   WHERE name='tag:partitioned-fixed-capacity' ORDER BY name""",
    "activities": """SELECT pid,client_port,state,wait_event_type,wait_event,xact_start,query_start,
   EXTRACT(EPOCH FROM clock_timestamp()-xact_start)::float AS xact_age_seconds,
   EXTRACT(EPOCH FROM clock_timestamp()-query_start)::float AS query_age_seconds,
   pg_blocking_pids(pid) AS blockers,LEFT(query,6000) AS query
   FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() ORDER BY pid""",
    "locks": """SELECT pid,locktype,relation,page,tuple,transactionid::text,mode,granted,waitstart
   FROM pg_locks WHERE pid IN (SELECT pid FROM pg_stat_activity
   WHERE datname=current_database() AND pid<>pg_backend_pid()) ORDER BY pid,granted,locktype""",
}


def save(name, value):
    (OUTPUT / name).write_text(json.dumps(value, indent=2, default=str) + "\n")


def processes():
    current = {}
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            raw = (path / "stat").read_text()
            v = raw[raw.rfind(")") + 2 :].split()
            io = {}
            try:
                io = {
                    k: int(n)
                    for k, n in (
                        line.split(":", 1)
                        for line in (path / "io").read_text().splitlines()
                    )
                }
            except (PermissionError, FileNotFoundError):
                pass
            current[int(path.name)] = {
                "pid": int(path.name),
                "ppid": int(v[1]),
                "state": v[0],
                "start_ticks": int(v[19]),
                "cpu_ticks": int(v[11]) + int(v[12]),
                "rss_bytes": int(v[21]) * os.sysconf("SC_PAGE_SIZE"),
                "io": io,
                "command": (path / "cmdline")
                .read_bytes()
                .replace(b"\0", b" ")
                .decode(errors="replace")[:600],
            }
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    selected = {ROOT_PID} | {
        pid
        for (pid, ticks) in SEEN
        if pid in current and current[pid]["start_ticks"] == ticks
    }
    while True:
        new = {pid for pid, p in current.items() if p["ppid"] in selected}
        if new <= selected:
            break
        selected |= new
    result = []
    for pid in selected:
        if pid not in current:
            continue
        row = current[pid]
        SEEN.setdefault((pid, row["start_ticks"]), row.copy())
        result.append(row)
    return sorted(result, key=lambda r: r["pid"])


def effects():
    result = []
    if not BASETEMP.exists():
        return result
    for path in BASETEMP.iterdir():
        if not path.is_dir() or path.is_symlink():
            continue
        names = [p.name for p in path.iterdir() if p.is_file()]
        result.append(
            {
                "directory": str(path),
                "signed_effect_files": sum(
                    n.startswith("signed-") and n.endswith(".json") for n in names
                ),
                "document_files": sum(
                    n.startswith("document-") and n.endswith(".json") for n in names
                ),
                "pid_markers": {
                    n: (path / n).read_text()[:80]
                    for n in names
                    if n.endswith((".pid", ".ppid"))
                },
            }
        )
    return result


def summarize(sample):
    global LAST_VECTOR, LAST_ADVANCED
    tasks = sample.get("tasks", [])
    consumers = sample.get("consumers", [])
    flows = sample.get("flows", [])
    source = sum(
        t["producer_position"] for t in tasks if t["operator_uid"] == "source.documents"
    )
    accepted = sum(c["completed_through"] for c in consumers)
    vector = (
        source,
        accepted,
        tuple(
            (
                str(t["id"]),
                t["task_state"],
                t["output_state"],
                t["physical_slot_released"],
            )
            for t in tasks
        ),
    )
    now = time.monotonic()
    if vector != LAST_VECTOR:
        LAST_VECTOR = vector
        LAST_ADVANCED = now
    expected = sum(int(f["expected_count"] or 0) for f in flows)
    complete = (
        len(tasks) == 4 * len(flows)
        and bool(tasks)
        and all(
            t["output_state"] == "COMPLETED" and t["task_state"] == "COMPLETED"
            for t in tasks
        )
    )
    all_data = expected > 0 and source == accepted == expected
    slots = sum(not t["physical_slot_released"] for t in tasks)
    terminal = bool(flows) and all(
        f["state_type"] in ("COMPLETED", "FAILED", "CANCELLED", "CRASHED")
        for f in flows
    )
    label = "NO_GRAPH_YET" if not flows else "DATA_OR_LIFECYCLE_ADVANCING"
    if flows and now - LAST_ADVANCED >= 30:
        label = "NO_VECTOR_CHANGE_30S_CANDIDATE"
    if all_data and not complete:
        label = "DATA_ACCEPTED_EOF_OR_TASKS_PENDING"
    if all_data and complete and not terminal:
        label = (
            "ALL_DATA_EOF_TASKS_DONE_SLOTS_HELD_CANDIDATE"
            if slots
            else "ALL_DATA_EOF_TASKS_SLOTS_DONE_FLOW_PENDING_CANDIDATE"
        )
    if terminal:
        label = "FLOW_TERMINAL"
    return {
        "elapsed": round(now - STARTED, 3),
        "phase_candidate": label,
        "expected_documents": expected,
        "source_accepted_positions": source,
        "consumer_accepted_sequences": accepted,
        "signed_effect_files": sum(
            x["signed_effect_files"] for x in sample.get("effects", [])
        ),
        "task_states": [t["task_state"] for t in tasks],
        "output_states": [t["output_state"] for t in tasks],
        "physical_slots_remaining": slots,
        "seconds_since_vector_change": round(now - LAST_ADVANCED, 3),
        "blocked_pids": [
            a["pid"] for a in sample.get("activities", []) if a["blockers"]
        ],
        "snapshot_is_nonatomic": True,
    }


async def sample(reason):
    global SAMPLES
    if PROFILE == "partitioned-dag-v1":
        return await sample_dag(reason)
    result = {
        "elapsed": time.monotonic() - STARTED,
        "reason": reason,
        "processes": processes(),
        "effects": effects(),
    }
    raw = os.environ.get("PREFECT_SERVER_DATABASE_CONNECTION_URL") or os.environ.get(
        "PREFECT_API_DATABASE_CONNECTION_URL"
    )
    connection = None
    try:
        if not raw:
            raise RuntimeError("No configured PostgreSQL connection URL")
        parts = urlsplit(raw)
        database = parts.path.strip("/") + os.environ.get(
            "NATIVE_PROGRESS_TEST_DB_SUFFIX", "_tests_master"
        )
        url = urlunsplit(
            ("postgresql", parts.netloc, "/" + database, parts.query, parts.fragment)
        )
        connection = await asyncpg.connect(
            url,
            timeout=2,
            server_settings={
                "application_name": "native-progress-observer",
                "default_transaction_read_only": "on",
                "statement_timeout": "2000",
            },
        )
        for name, query in SQL.items():
            result[name] = [dict(row) for row in await connection.fetch(query)]
    except Exception as error:
        result["observer_error"] = {
            "type": type(error).__name__,
            "message": str(error)[:240],
        }
    finally:
        if connection is not None:
            await connection.close()
    result["summary"] = summarize(result)
    SAMPLES += 1
    with (OUTPUT / "timeline.jsonl").open("a") as stream:
        stream.write(json.dumps(result, default=str) + "\n")
    save("latest.json", result)
    print(
        "NATIVE_PROGRESS " + json.dumps(result["summary"], separators=(",", ":")),
        flush=True,
    )


def collect():
    if PROFILE == "partitioned-dag-v1":
        collect_dag()
        return
    while not STOP.is_set():
        if not PAUSED.is_set():
            try:
                asyncio.run(sample("checkpoint" if REQUEST.is_set() else "periodic"))
            except Exception as error:
                save(
                    "collector-error.json",
                    {"type": type(error).__name__, "message": str(error)[:240]},
                )
        REQUEST.clear()
        DONE.set()
        STOP.wait(5)
        if REQUEST.is_set():
            continue


def import_map(name):
    modules = {
        module: getattr(value, "__file__", None)
        for module, value in list(sys.modules.items())
        if module == "prefect" or module.startswith("prefect.")
    }
    foreign = {
        module: path
        for module, path in modules.items()
        if path and not Path(path).resolve().is_relative_to(SOURCE / "src" / "prefect")
    }
    save(
        name,
        {
            "modules": modules,
            "foreign_prefect_files": foreign,
            "clean_source_root": str(SOURCE),
        },
    )


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    save(
        "collector-configuration.json",
        {
            "cadence_seconds": 5,
            "profile": PROFILE,
            "snapshot_is_nonatomic": True,
            **(
                {
                    "sample_budget_seconds": 6,
                    "close_budget_seconds": 1,
                    "first_cause_encoding": "bounded_json_text_2000",
                    "scope_lookup_sha256": hashlib.sha256(
                        DAG_LOCATE.encode()
                    ).hexdigest(),
                }
                if PROFILE == "partitioned-dag-v1"
                else {}
            ),
            "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
            "collector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "pid": ROOT_PID,
            "source_root": str(SOURCE),
            "basetemp": str(BASETEMP),
            "argv": sys.argv,
            "query_sha256": {
                name: hashlib.sha256(query.encode()).hexdigest()
                for name, query in (
                    DAG_SQL if PROFILE == "partitioned-dag-v1" else SQL
                ).items()
            },
            "observation_only": True,
            "observer_modifies_test_or_database_timeouts": False,
        },
    )
    import_map("imports-start.json")
    session.config._native_progress_thread = threading.Thread(
        target=collect, name="native-progress-observer", daemon=True
    )
    session.config._native_progress_thread.start()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    PAUSED.clear()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_teardown(item, nextitem):
    checkpoint_started = time.monotonic_ns()
    # Snapshot while fixtures and test DB still exist, then close the observer
    # connection before fixture DROP DATABASE. No client/server state mutation.
    DONE.clear()
    REQUEST.set()
    deadline = time.monotonic() + 8
    while not DONE.wait(0.1) and time.monotonic() < deadline:
        pass
    PAUSED.set()
    if PROFILE == "partitioned-dag-v1":
        DAG_CHECKPOINTS.append(
            {
                "requested_monotonic_ns": checkpoint_started,
                "finished_monotonic_ns": time.monotonic_ns(),
                "acknowledged": DONE.is_set(),
                "active_observer_connections": DAG_ACTIVE_CONNECTIONS,
                "unconfirmed_observer_closes": DAG_UNCONFIRMED_CLOSES,
            }
        )


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    STOP.set()
    if PROFILE == "partitioned-dag-v1":
        REQUEST.set()  # Wake the DAG cadence wait before joining.
    thread = getattr(session.config, "_native_progress_thread", None)
    if thread is not None:
        thread.join(timeout=10)
    import_map("imports-finish.json")
    current = processes()
    live = [row for row in current if row["pid"] != ROOT_PID and row["state"] != "Z"]
    save(
        "session-receipt.json",
        {
            "exitstatus": exitstatus,
            "samples": SAMPLES,
            "observer_stopped": thread is None or not thread.is_alive(),
            "remaining_observed_descendants": live,
            "cleanup": "Observed fixture cleanup only; no signal or death proof is fabricated",
            **(
                {
                    "active_observer_connections": DAG_ACTIVE_CONNECTIONS,
                    "unconfirmed_observer_closes": DAG_UNCONFIRMED_CLOSES,
                    "owned_pid_cpu_windows": list(DAG_CPU_WINDOWS.values()),
                    "teardown_checkpoints": DAG_CHECKPOINTS,
                }
                if PROFILE == "partitioned-dag-v1"
                else {}
            ),
            "elapsed_seconds": time.monotonic() - STARTED,
        },
    )


DAG_SQL = {
    "flows": """SELECT f.id,f.name,f.state_type::text,f.state_name,f.run_count,f.updated,
 (f.parameters->>'count') AS expected_count,g.id AS graph_id,g.state AS graph_state,
 g.startup_completed_at,g.activation_operation_id,g.current_control_operation_id,g.profile
 FROM flow_run f JOIN stream_graph_revision g ON g.flow_run_id=f.id
 WHERE g.id=$1 AND f.id=$2 AND g.profile='partitioned-dag-v1'""",
    "tasks": SQL["tasks"].replace(
        "WHERE r.profile='partitioned-fixed-v1'",
        "WHERE r.profile='partitioned-dag-v1' AND r.id=$1 AND r.flow_run_id=$2",
    ),
    "consumers": """SELECT c.id,c.flow_run_id,c.subtask_id,c.task_run_id,
 c.input_stream_id,c.output_stream_id,c.completed_through,c.inflight_sequence,c.attempt,c.owner_epoch
 FROM stream_consumer c WHERE c.graph_revision_id=$1 AND c.flow_run_id=$2 ORDER BY c.id""",
    "partitions": """SELECT p.id,p.producer_kind,p.producer_task_run_id,p.state,
 p.producer_position,p.last_sequence,p.tail_sequence,p.record_count,p.byte_count,p.released
 FROM stream_partition p WHERE p.flow_run_id=$2 AND p.id IN (
 SELECT s.output_stream_id FROM stream_physical_subtask s WHERE s.graph_revision_id=$1
 UNION SELECT e.stream_id FROM stream_graph_edge_partition e WHERE e.graph_revision_id=$1)
 ORDER BY p.id""",
    "records": """SELECT r.stream_id,r.record_kind,COUNT(*) AS rows FROM stream_record r
 WHERE r.graph_revision_id=$1 AND r.flow_run_id=$2
 GROUP BY r.stream_id,r.record_kind ORDER BY r.stream_id,r.record_kind""",
    "capacity": """SELECT name,active_slots,"limit" FROM concurrency_limit_v2
 WHERE name='tag:native-dag-runtime-capacity' AND EXISTS(
 SELECT 1 FROM stream_graph_revision WHERE id=$1 AND flow_run_id=$2) ORDER BY name""",
    "control": """SELECT o.id,o.operation_id,o.kind,o.state,o.effective_control_kind,
 LEFT(o.first_cause::text,2000) AS first_cause,o.first_cause->>'kind' AS failure_kind
 FROM stream_graph_revision g JOIN stream_dag_lifecycle_operation o
 ON o.id=g.current_control_operation_id AND o.graph_revision_id=g.id AND o.flow_run_id=g.flow_run_id
 WHERE g.id=$1 AND g.flow_run_id=$2""",
    "actions": """SELECT o.id,o.operation_id,o.kind,o.state,o.control_operation_id
 FROM stream_dag_lifecycle_operation o JOIN stream_graph_revision g ON g.id=o.graph_revision_id
 WHERE g.id=$1 AND o.flow_run_id=$2 AND o.kind IN ('FAIL','CANCEL','ABORT_PREVIEW')
 AND (o.control_operation_id=g.current_control_operation_id OR
 (g.current_control_operation_id IS NULL AND o.kind='ABORT_PREVIEW'))
 ORDER BY o.created DESC LIMIT 8""",
    "flow_state": """SELECT s.id,s.type::text,s.name,LEFT(s.message,2000) AS message
 FROM flow_run f LEFT JOIN flow_run_state s ON s.id=f.state_id
 WHERE f.id=$2 AND EXISTS(SELECT 1 FROM stream_graph_revision WHERE id=$1 AND flow_run_id=f.id)""",
    "activities": SQL["activities"].replace(
        "ORDER BY pid",
        "AND EXISTS(SELECT 1 FROM stream_graph_revision WHERE id=$1 AND flow_run_id=$2) ORDER BY pid",
    ),
    "locks": SQL["locks"].replace(
        "ORDER BY pid,granted,locktype",
        "AND EXISTS(SELECT 1 FROM stream_graph_revision WHERE id=$1 AND flow_run_id=$2) ORDER BY pid,granted,locktype",
    ),
}
DAG_LOCATE = """SELECT g.id,g.flow_run_id FROM stream_graph_revision g
 WHERE g.profile='partitioned-dag-v1' ORDER BY g.created LIMIT 2"""
DAG_SCOPE = None
DAG_ACTIVE_CONNECTIONS = 0
DAG_UNCONFIRMED_CLOSES = 0
DAG_CPU_WINDOWS = {}
DAG_CHECKPOINTS = []


def dag_effects():
    result = []
    for fixture in effects():
        directory = Path(fixture["directory"])
        partitions = {}
        for role in ("B-0", "C-0", "C-1"):
            folder = directory / role
            partitions[role] = (
                len(list(folder.glob("document-*.json"))) if folder.is_dir() else 0
            )
        result.append({**fixture, "dag_document_files_by_partition": partitions})
    return result


def summarize_dag(sample):
    global LAST_VECTOR, LAST_ADVANCED
    tasks, consumers, flows = (
        sample.get(key, []) for key in ("tasks", "consumers", "flows")
    )
    source = sum(
        row["producer_position"] for row in tasks if row["operator_uid"] == "A"
    )
    cursors = {str(row["subtask_id"]): row["completed_through"] for row in consumers}
    expected = int(flows[0]["expected_count"] or 0) if len(flows) == 1 else 0
    complete = len(tasks) == 4 and all(
        row["output_state"] == row["task_state"] == "COMPLETED" for row in tasks
    )
    all_data = (
        expected > 0
        and source == expected
        and len(cursors) == 3
        and all(value == expected for value in cursors.values())
    )
    slots = sum(not row["physical_slot_released"] for row in tasks)
    terminal = len(flows) == 1 and flows[0]["state_type"] in (
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "CRASHED",
    )
    vector = (
        source,
        tuple(sorted(cursors.items())),
        tuple(
            (
                str(row["id"]),
                row["task_state"],
                row["output_state"],
                row["physical_slot_released"],
            )
            for row in tasks
        ),
    )
    current = time.monotonic()
    if vector != LAST_VECTOR:
        LAST_VECTOR, LAST_ADVANCED = vector, current
    label = "NO_OWNED_DAG_YET" if not flows else "DATA_OR_LIFECYCLE_ADVANCING"
    if flows and current - LAST_ADVANCED >= 30:
        label = "NO_VECTOR_CHANGE_30S_CANDIDATE"
    if all_data and not complete:
        label = "DATA_ACCEPTED_EOF_OR_TASKS_PENDING"
    if all_data and complete and not terminal:
        label = (
            "ALL_DATA_EOF_TASKS_DONE_SLOTS_HELD_CANDIDATE"
            if slots
            else "ALL_DATA_EOF_TASKS_SLOTS_DONE_FLOW_PENDING_CANDIDATE"
        )
    if terminal:
        label = "FLOW_TERMINAL"
    control = sample.get("control", [])
    messages = sample.get("flow_state", [])
    return {
        "elapsed": round(current - STARTED, 3),
        "profile": "partitioned-dag-v1",
        "phase_candidate": label,
        "owned_flow_id": sample.get("owned_flow_id"),
        "owned_graph_id": sample.get("owned_graph_id"),
        "expected_documents": expected,
        "source_accepted_positions": source,
        "consumer_accepted_sequences": sum(cursors.values()),
        "consumer_completed_through": cursors,
        "expected_consumer_count": 3,
        "expected_consumer_total": 3 * expected,
        "all_data_observed": all_data,
        "physical_slots_remaining": slots,
        "partition_count": len(sample.get("partitions", [])),
        "task_states": [row["task_state"] for row in tasks],
        "output_states": [row["output_state"] for row in tasks],
        "worker_ids": sorted(
            {
                str(row["assigned_worker_id"])
                for row in tasks
                if row["assigned_worker_id"] is not None
            }
        ),
        "failure_kind": control[0].get("failure_kind") or "UNKNOWN"
        if control
        else "UNKNOWN",
        "control_operation": control[0] if control else "UNKNOWN",
        "requested_actions": sample.get("actions") or "UNKNOWN",
        "flow_state_message": messages[0].get("message") or "UNKNOWN"
        if messages
        else "UNKNOWN",
        "seconds_since_vector_change": round(current - LAST_ADVANCED, 3),
        "blocked_pids": [
            row["pid"] for row in sample.get("activities", []) if row["blockers"]
        ],
        "snapshot_is_nonatomic": True,
        "cleanup_can_remove_consumers_and_records": True,
    }


def record_dag_cpu_windows(rows, observed):
    for row in rows:
        key = (row["pid"], row["start_ticks"])
        point = {"monotonic_ns": observed, "cpu_ticks": row["cpu_ticks"]}
        window = DAG_CPU_WINDOWS.setdefault(
            key,
            {
                "pid": row["pid"],
                "start_ticks": row["start_ticks"],
                "command": row["command"],
                "first": point,
            },
        )
        window["last"] = point
        window["observed_cpu_delta_seconds"] = (
            point["cpu_ticks"] - window["first"]["cpu_ticks"]
        ) / os.sysconf("SC_CLK_TCK")
        window["qualification"] = (
            "Only paired same-PID/start first/last observed window, not complete lifetime or exit CPU"
        )


async def sample_dag(reason):
    global SAMPLES, DAG_SCOPE, DAG_ACTIVE_CONNECTIONS, DAG_UNCONFIRMED_CLOSES
    result = {
        "elapsed": time.monotonic() - STARTED,
        "reason": reason,
        "profile": PROFILE,
        "sample_monotonic_ns": time.monotonic_ns(),
        "processes": processes(),
        "effects": dag_effects(),
        "completed_query_families": [],
        **{name: [] for name in DAG_SQL},
    }
    record_dag_cpu_windows(result["processes"], result["sample_monotonic_ns"])
    raw = os.environ.get("PREFECT_SERVER_DATABASE_CONNECTION_URL") or os.environ.get(
        "PREFECT_API_DATABASE_CONNECTION_URL"
    )
    connection = None
    try:
        async with asyncio.timeout(6):
            if not raw:
                raise RuntimeError("No configured PostgreSQL connection URL")
            parts = urlsplit(raw)
            database = parts.path.strip("/") + os.environ.get(
                "NATIVE_PROGRESS_TEST_DB_SUFFIX", "_tests_master"
            )
            url = urlunsplit(
                (
                    "postgresql",
                    parts.netloc,
                    "/" + database,
                    parts.query,
                    parts.fragment,
                )
            )
            connection = await asyncpg.connect(
                url,
                timeout=2,
                server_settings={
                    "application_name": "native-dag-progress-observer",
                    "default_transaction_read_only": "on",
                    "statement_timeout": "2000",
                },
            )
            DAG_ACTIVE_CONNECTIONS += 1
            result["server_major"] = connection.get_server_version().major
            if result["server_major"] != 17:
                raise RuntimeError("DAG progress requires PostgreSQL 17")
            result["observer_backend_pid"] = connection.get_server_pid()
            if DAG_SCOPE is None:
                candidates = await connection.fetch(DAG_LOCATE)
                result["scope_candidates"] = len(candidates)
                if len(candidates) > 1:
                    raise RuntimeError(
                        "DAG progress requires one owned graph in this isolated test DB"
                    )
                if candidates:
                    DAG_SCOPE = (candidates[0]["id"], candidates[0]["flow_run_id"])
            if DAG_SCOPE is not None:
                result["owned_graph_id"], result["owned_flow_id"] = DAG_SCOPE
                for name, query in DAG_SQL.items():
                    result[name] = [
                        dict(row) for row in await connection.fetch(query, *DAG_SCOPE)
                    ]
                    result["completed_query_families"].append(name)
    except Exception as error:
        result["observer_error"] = {
            "type": type(error).__name__,
            "message": str(error)[:240],
        }
    finally:
        if connection is not None:
            try:
                async with asyncio.timeout(1):
                    await connection.close()
                result["observer_connection_closed"] = connection.is_closed()
            except Exception as error:
                result["observer_connection_closed"] = False
                result["observer_close_error"] = {"type": type(error).__name__}
            if result["observer_connection_closed"]:
                DAG_ACTIVE_CONNECTIONS -= 1
            else:
                DAG_UNCONFIRMED_CLOSES += 1
                STOP.set()
    flows = result["flows"]
    result["complete_owned_sample"] = (
        set(result["completed_query_families"]) == set(DAG_SQL)
        and len(flows) == 1
        and str(flows[0]["id"]) == str(result.get("owned_flow_id"))
        and str(flows[0]["graph_id"]) == str(result.get("owned_graph_id"))
        and flows[0]["profile"] == "partitioned-dag-v1"
        and result.get("observer_connection_closed") is True
        and "observer_error" not in result
        and "observer_close_error" not in result
    )
    result["sample_finished_monotonic_ns"] = time.monotonic_ns()
    result["summary"] = summarize_dag(result)
    SAMPLES += 1
    with (OUTPUT / "timeline.jsonl").open("a") as stream:
        stream.write(json.dumps(result, default=str) + "\n")
    save("latest.json", result)
    print(
        "NATIVE_PROGRESS "
        + json.dumps(result["summary"], separators=(",", ":"), default=str),
        flush=True,
    )


def collect_dag():
    while not STOP.is_set():
        if not PAUSED.is_set():
            try:
                asyncio.run(
                    sample_dag("checkpoint" if REQUEST.is_set() else "periodic")
                )
            except Exception as error:
                save(
                    "collector-error.json",
                    {"type": type(error).__name__, "message": str(error)[:240]},
                )
        if REQUEST.is_set():
            # Pause before acknowledging teardown. No next sample can race
            # fixture cleanup; setup explicitly clears PAUSED for the next case.
            PAUSED.set()
            REQUEST.clear()
            DONE.set()
        if not STOP.is_set():
            REQUEST.wait(5)  # Teardown wakes immediately, rather than after cadence.
