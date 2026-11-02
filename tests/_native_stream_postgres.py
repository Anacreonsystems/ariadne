"""Real PostgreSQL storage adapter for native-stream integration tests.

This module is test infrastructure.  Production Prefect code depends only on
`prefect.streams.StreamStorage` and never imports the retained architecture
prototype used here.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
import sys
import threading
from collections.abc import Coroutine
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Literal, Self, TypeVar
from uuid import uuid4

import asyncpg

from prefect.streams import (
    StreamProtocolError,
    StreamRecord,
    StreamStatus,
)

T = TypeVar("T")

_PROTOTYPE_PATH = (
    Path(__file__).resolve().parents[1]
    / "architecture"
    / "streaming-etl"
    / "prototypes"
    / "postgres_stream_v2.py"
)
_PROTOTYPE_MODULE_NAME = "_prefect_test_postgres_stream_v2"
_PROTOTYPE_LOCK = threading.Lock()


class NativeStreamAdapterError(StreamProtocolError):
    """A test adapter contract or storage-authority violation."""


class StaleNativeStreamOwner(NativeStreamAdapterError):
    """The binding was replaced by a later TaskRun attempt."""


class InjectedNativeStreamFault(NativeStreamAdapterError):
    """A deterministic fault raised before the completion transaction commits."""


class LostNativeStreamAcknowledgement(NativeStreamAdapterError):
    """The completion committed but its caller did not receive the acknowledgement."""


@dataclass(frozen=True)
class NativeStreamBinding:
    input_id: str
    output_id: str
    consumer_id: str
    owner_id: str
    owner_epoch: int
    attempt: int
    input_channel: str
    output_channel: str
    partition: str = "p0"


def _load_prototype() -> ModuleType:
    """Load the retained backend without adding its directory to `sys.path`."""

    with _PROTOTYPE_LOCK:
        existing = sys.modules.get(_PROTOTYPE_MODULE_NAME)
        if existing is not None:
            return existing
        spec = importlib.util.spec_from_file_location(
            _PROTOTYPE_MODULE_NAME, _PROTOTYPE_PATH
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load PostgreSQL prototype at {_PROTOTYPE_PATH}")
        module = importlib.util.module_from_spec(spec)
        # Dataclasses resolve their defining module while the file executes.
        sys.modules[_PROTOTYPE_MODULE_NAME] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(_PROTOTYPE_MODULE_NAME, None)
            raise
        return module


def _encode(value: Any) -> bytes:
    if isinstance(value, bytes):
        material = {
            "codec": "bytes/base64-v1",
            "value": base64.b64encode(value).decode("ascii"),
        }
    else:
        material = {"codec": "json/v1", "value": value}
    try:
        return json.dumps(
            material, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    except (TypeError, ValueError) as exc:
        raise NativeStreamAdapterError(
            "native stream test values must be bytes or JSON values"
        ) from exc


def _decode(payload: bytes) -> Any:
    try:
        material = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NativeStreamAdapterError(
            "stored stream payload is not canonical JSON"
        ) from exc
    if not isinstance(material, dict) or set(material) != {"codec", "value"}:
        raise NativeStreamAdapterError("stored stream payload has an invalid envelope")
    if material["codec"] == "json/v1":
        return material["value"]
    if material["codec"] == "bytes/base64-v1" and isinstance(material["value"], str):
        try:
            return base64.b64decode(material["value"], validate=True)
        except ValueError as exc:
            raise NativeStreamAdapterError("stored bytes payload is invalid") from exc
    raise NativeStreamAdapterError("stored stream payload uses an unknown codec")


def _channel(stream_id: str) -> str:
    if not stream_id:
        raise NativeStreamAdapterError("stream identity must be non-empty")
    return "native_" + hashlib.sha256(stream_id.encode()).hexdigest()


class PostgresNativeStreamStorage:
    """Thread/loop-safe test implementation of `StreamStorage`.

    `asyncpg` pools are owned by a dedicated event loop.  Every public async
    method proxies to that loop, allowing sync and async Prefect task engines on
    different threads to share one real storage authority.
    """

    schema_id = "prefect-native-test-json/v1"
    lease_seconds = 300.0

    def __init__(self, dsn: str, schema: str | None = None) -> None:
        self.dsn = dsn
        self.schema = schema or f"native_stage1_{uuid4().hex}"
        if not self.schema.startswith("native_stage1_"):
            raise ValueError("test schemas must use the native_stage1_ prefix")
        self._prototype = _load_prototype()
        self._loop = asyncio.new_event_loop()
        self._loop_ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"native-stream-postgres-{self.schema}",
            daemon=True,
        )
        self._closed = False
        self._fault_lock = threading.Lock()
        self._armed_faults = {
            "before_commit": 0,
            "lost_ack": 0,
            "finish_lost_ack": 0,
        }
        self._observed_faults = {
            "before_commit": 0,
            "lost_ack": 0,
            "finish_lost_ack": 0,
        }
        self._store: Any = None
        self._thread.start()
        if not self._loop_ready.wait(timeout=10):
            raise RuntimeError("native stream storage event loop did not start")

    @classmethod
    async def open(
        cls, dsn: str, schema: str | None = None
    ) -> PostgresNativeStreamStorage:
        storage = cls(dsn, schema)
        try:
            await storage._proxy(storage._initialize())
        except BaseException:
            await storage.close()
            raise
        return storage

    create = open

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        self._loop.run_forever()
        pending = asyncio.all_tasks(self._loop)
        for task in pending:
            task.cancel()
        if pending:
            self._loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        self._loop.close()

    async def _proxy(self, coroutine: Coroutine[Any, Any, T]) -> T:
        if self._closed:
            coroutine.close()
            raise NativeStreamAdapterError("native stream storage is closed")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            future.cancel()
            raise

    async def _initialize(self) -> None:
        self._store = await self._prototype.PostgresStream.open(self.dsn, self.schema)
        ddl = """
        CREATE TABLE native_streams_v1(
            stream_id text PRIMARY KEY,
            channel text NOT NULL UNIQUE,
            partition_id text NOT NULL CHECK(partition_id='p0'),
            state text NOT NULL CHECK(state IN ('OPEN','COMPLETED','FAILED')),
            error text,
            next_publish_ordinal bigint NOT NULL DEFAULT 1
                CHECK(next_publish_ordinal>0),
            closed_through bigint,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            finished_at timestamptz,
            CHECK((state='OPEN' AND error IS NULL AND finished_at IS NULL
                   AND closed_through IS NULL)
               OR (state='COMPLETED' AND error IS NULL AND finished_at IS NOT NULL
                   AND closed_through IS NOT NULL)
               OR (state='FAILED' AND error IS NOT NULL AND finished_at IS NOT NULL
                   AND closed_through IS NOT NULL))
        );
        CREATE TABLE native_stream_bindings_v1(
            consumer_id text PRIMARY KEY,
            input_stream_id text NOT NULL REFERENCES native_streams_v1(stream_id),
            output_stream_id text NOT NULL REFERENCES native_streams_v1(stream_id),
            owner_id text NOT NULL,
            owner_epoch bigint NOT NULL DEFAULT 0 CHECK(owner_epoch>=0),
            task_attempt bigint NOT NULL CHECK(task_attempt>0),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            CHECK(input_stream_id<>output_stream_id)
        );
        CREATE TABLE native_delivery_observations_v1(
            consumer_id text NOT NULL REFERENCES native_stream_bindings_v1,
            input_sequence bigint NOT NULL CHECK(input_sequence>0),
            delivery_count bigint NOT NULL CHECK(delivery_count>0),
            PRIMARY KEY(consumer_id,input_sequence)
        );
        CREATE FUNCTION native_stream_record_is_open() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE stream_state text;
        BEGIN
            SELECT state INTO stream_state FROM native_streams_v1
             WHERE channel=NEW.channel FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'record channel is not a native test stream';
            END IF;
            IF stream_state <> 'OPEN' THEN
                RAISE EXCEPTION 'native test stream is closed';
            END IF;
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER native_stream_record_is_open
        BEFORE INSERT ON records FOR EACH ROW
        EXECUTE FUNCTION native_stream_record_is_open();
        """
        async with self._store.pool.acquire() as conn:
            await conn.execute(ddl)

    def arm_fault(
        self,
        fault: Literal["before_commit", "lost_ack", "finish_lost_ack"],
        count: int = 1,
    ) -> None:
        if fault not in self._armed_faults or isinstance(count, bool) or count <= 0:
            raise ValueError("unknown fault or non-positive fault count")
        with self._fault_lock:
            self._armed_faults[fault] += count

    def fail_before_commit(self, count: int = 1) -> None:
        self.arm_fault("before_commit", count)

    def lose_completion_ack(self, count: int = 1) -> None:
        self.arm_fault("lost_ack", count)

    def lose_finish_ack(self, count: int = 1) -> None:
        self.arm_fault("finish_lost_ack", count)

    @property
    def fault_counts(self) -> dict[str, int]:
        with self._fault_lock:
            return dict(self._observed_faults)

    def _consume_fault(self, fault: str) -> bool:
        with self._fault_lock:
            if self._armed_faults[fault] <= 0:
                return False
            self._armed_faults[fault] -= 1
            self._observed_faults[fault] += 1
            return True

    async def create_stream(self, stream_id: str) -> None:
        await self._proxy(self._create_stream(stream_id))

    async def _create_stream(self, stream_id: str) -> None:
        channel = _channel(stream_id)
        await self._store.create_partition(
            channel,
            "p0",
            max_records=10_000,
            max_bytes=64 * 1024 * 1024,
            max_metadata_records=50_000,
            max_producers=128,
            max_consumers=128,
        )
        async with self._store.pool.acquire() as conn, conn.transaction():
            result = await conn.execute(
                """INSERT INTO native_streams_v1(
                       stream_id,channel,partition_id,state)
                   VALUES($1,$2,'p0','OPEN') ON CONFLICT DO NOTHING""",
                stream_id,
                channel,
            )
            if result == "INSERT 0 0":
                row = await conn.fetchrow(
                    "SELECT channel,partition_id FROM native_streams_v1 "
                    "WHERE stream_id=$1",
                    stream_id,
                )
                if row is None or tuple(row) != (channel, "p0"):
                    raise NativeStreamAdapterError("stream identity drift")

    async def open_consumer(
        self,
        input_id: str,
        output_id: str,
        consumer_id: str,
        owner_id: str,
        attempt: int,
    ) -> NativeStreamBinding:
        return await self._proxy(
            self._open_consumer(input_id, output_id, consumer_id, owner_id, attempt)
        )

    async def _open_consumer(
        self,
        input_id: str,
        output_id: str,
        consumer_id: str,
        owner_id: str,
        attempt: int,
    ) -> NativeStreamBinding:
        if (
            not consumer_id
            or not owner_id
            or isinstance(attempt, bool)
            or not isinstance(attempt, int)
            or attempt <= 0
        ):
            raise NativeStreamAdapterError("invalid consumer execution identity")
        async with self._store.pool.acquire() as conn:
            streams = await conn.fetch(
                "SELECT stream_id,channel,state,closed_through "
                "FROM native_streams_v1 "
                "WHERE stream_id=ANY($1::text[]) ORDER BY stream_id",
                [input_id, output_id],
            )
        by_id = {row["stream_id"]: row for row in streams}
        if set(by_id) != {input_id, output_id}:
            raise NativeStreamAdapterError("input and output streams must exist")
        output_state = by_id[output_id]["state"]
        if output_state == "FAILED":
            raise NativeStreamAdapterError("failed consumer output cannot be reopened")
        if output_state == "COMPLETED":
            await self._assert_completed_output_reconcile(
                input_id, output_id, consumer_id, by_id[input_id]
            )

        await self._store.create_consumer(consumer_id, by_id[input_id]["channel"], "p0")
        needs_claim = False
        expected_epoch = 0
        async with self._store.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                """INSERT INTO native_stream_bindings_v1(
                       consumer_id,input_stream_id,output_stream_id,
                       owner_id,task_attempt)
                   VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING""",
                consumer_id,
                input_id,
                output_id,
                owner_id,
                attempt,
            )
            binding = await conn.fetchrow(
                "SELECT * FROM native_stream_bindings_v1 "
                "WHERE consumer_id=$1 FOR UPDATE",
                consumer_id,
            )
            assert binding is not None
            if (
                binding["input_stream_id"],
                binding["output_stream_id"],
            ) != (input_id, output_id):
                raise NativeStreamAdapterError("consumer stream binding drift")
            if attempt < binding["task_attempt"]:
                raise StaleNativeStreamOwner("TaskRun attempt is fenced")
            if attempt == binding["task_attempt"]:
                if binding["owner_id"] != owner_id:
                    raise NativeStreamAdapterError(
                        "one TaskRun attempt cannot change owner identity"
                    )
                expected_epoch = binding["owner_epoch"]
                needs_claim = expected_epoch == 0
            else:
                if attempt != binding["task_attempt"] + 1:
                    raise NativeStreamAdapterError("TaskRun attempt sequence gap")
                # Fencing is durable before the replacement claim.  A process
                # loss between these statements and claim is resumed by the
                # same attempt/owner with owner_epoch=0.
                await conn.execute(
                    """UPDATE consumers SET lease_expires_at=clock_timestamp()
                       WHERE consumer_id=$1""",
                    consumer_id,
                )
                await conn.execute(
                    """UPDATE runtime_invocations
                       SET expires_at=clock_timestamp() WHERE consumer_id=$1""",
                    consumer_id,
                )
                await conn.execute(
                    """UPDATE native_stream_bindings_v1
                       SET owner_id=$2,owner_epoch=0,task_attempt=$3,
                           updated_at=clock_timestamp()
                       WHERE consumer_id=$1""",
                    consumer_id,
                    owner_id,
                    attempt,
                )
                needs_claim = True

        if not needs_claim:
            try:
                await self._store.renew(
                    consumer_id,
                    owner_id,
                    expected_epoch,
                    lease_seconds=self.lease_seconds,
                )
                owner_epoch = expected_epoch
            except self._prototype.StaleOwner:
                owner_epoch = await self._store.claim_consumer(
                    consumer_id, owner_id, lease_seconds=self.lease_seconds
                )
        else:
            owner_epoch = await self._store.claim_consumer(
                consumer_id, owner_id, lease_seconds=self.lease_seconds
            )

        async with self._store.pool.acquire() as conn, conn.transaction():
            updated = await conn.fetchval(
                """UPDATE native_stream_bindings_v1 SET owner_epoch=$4,
                       updated_at=clock_timestamp()
                   WHERE consumer_id=$1 AND owner_id=$2 AND task_attempt=$3
                   RETURNING owner_epoch""",
                consumer_id,
                owner_id,
                attempt,
                owner_epoch,
            )
            if updated is None:
                raise StaleNativeStreamOwner("consumer changed during claim")
        return NativeStreamBinding(
            input_id,
            output_id,
            consumer_id,
            owner_id,
            owner_epoch,
            attempt,
            by_id[input_id]["channel"],
            by_id[output_id]["channel"],
        )

    async def _assert_completed_output_reconcile(
        self,
        input_id: str,
        output_id: str,
        consumer_id: str,
        input_stream: asyncpg.Record,
    ) -> None:
        if input_stream["state"] != "COMPLETED":
            raise NativeStreamAdapterError(
                "completed output requires a completed input tail"
            )
        async with self._store.pool.acquire() as conn:
            witness = await conn.fetchrow(
                """SELECT b.input_stream_id,b.output_stream_id,
                          c.channel,c.partition_id,c.completed_through,
                          c.in_flight_sequence
                   FROM native_stream_bindings_v1 b
                   JOIN consumers c USING(consumer_id)
                   WHERE b.consumer_id=$1""",
                consumer_id,
            )
        if witness is None:
            raise NativeStreamAdapterError(
                "completed output has no existing consumer binding"
            )
        if (
            witness["input_stream_id"],
            witness["output_stream_id"],
            witness["channel"],
            witness["partition_id"],
        ) != (input_id, output_id, input_stream["channel"], "p0"):
            raise NativeStreamAdapterError(
                "completed output consumer binding does not match"
            )
        if (
            witness["in_flight_sequence"] is not None
            or witness["completed_through"] != input_stream["closed_through"]
        ):
            raise NativeStreamAdapterError(
                "completed output consumer has not reached the immutable input tail"
            )

    async def _assert_binding(self, binding: NativeStreamBinding) -> None:
        if not isinstance(binding, NativeStreamBinding):
            raise NativeStreamAdapterError("unknown native stream binding")
        async with self._store.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT input_stream_id,output_stream_id,owner_id,
                          owner_epoch,task_attempt
                   FROM native_stream_bindings_v1 WHERE consumer_id=$1""",
                binding.consumer_id,
            )
        if row is None or tuple(row) != (
            binding.input_id,
            binding.output_id,
            binding.owner_id,
            binding.owner_epoch,
            binding.attempt,
        ):
            raise StaleNativeStreamOwner("native stream binding is fenced")

    async def read(self, binding: NativeStreamBinding) -> StreamRecord[Any] | None:
        return await self._proxy(self._read(binding))

    async def _read(self, binding: NativeStreamBinding) -> StreamRecord[Any] | None:
        await self._assert_binding(binding)
        try:
            await self._store.renew(
                binding.consumer_id,
                binding.owner_id,
                binding.owner_epoch,
                lease_seconds=self.lease_seconds,
            )
            delivery = await self._store.deliver_next(
                binding.consumer_id, binding.owner_id, binding.owner_epoch
            )
        except self._prototype.StaleOwner as exc:
            raise StaleNativeStreamOwner(str(exc)) from exc
        if delivery is None:
            return None
        token, envelope = delivery
        position = await self._store.verified_position(binding.consumer_id)
        state = position["managed_state"]
        if isinstance(state, str):
            state = json.loads(state)
        if not isinstance(state, dict):
            raise NativeStreamAdapterError("managed state projection is not a dict")
        async with self._store.pool.acquire() as conn:
            count = await conn.fetchval(
                """INSERT INTO native_delivery_observations_v1(
                       consumer_id,input_sequence,delivery_count)
                   VALUES($1,$2,1)
                   ON CONFLICT(consumer_id,input_sequence) DO UPDATE
                   SET delivery_count=native_delivery_observations_v1.delivery_count+1
                   RETURNING delivery_count""",
                binding.consumer_id,
                token.sequence,
            )
            if count <= 0:
                raise AssertionError("delivery observation did not advance")
        return StreamRecord(
            sequence=token.sequence,
            value=_decode(envelope.payload),
            managed_state=json.loads(json.dumps(state)),
            token=token,
        )

    async def complete(
        self,
        binding: NativeStreamBinding,
        record: StreamRecord[Any],
        kind: Literal["YIELD", "RETURN"],
        value: Any,
        managed_state: dict[str, Any],
    ) -> None:
        await self._proxy(self._complete(binding, record, kind, value, managed_state))

    async def _complete(
        self,
        binding: NativeStreamBinding,
        record: StreamRecord[Any],
        kind: Literal["YIELD", "RETURN"],
        value: Any,
        managed_state: dict[str, Any],
    ) -> None:
        await self._assert_binding(binding)
        token = record.token
        if not isinstance(token, self._prototype.DeliveryToken) or (
            token.consumer_id,
            token.sequence,
            token.owner_id,
            token.owner_epoch,
        ) != (
            binding.consumer_id,
            record.sequence,
            binding.owner_id,
            binding.owner_epoch,
        ):
            raise StaleNativeStreamOwner("completion token is not current")
        if not isinstance(managed_state, dict):
            raise NativeStreamAdapterError("managed state must be a dictionary")
        try:
            state = json.loads(
                json.dumps(managed_state, sort_keys=True, allow_nan=False)
            )
        except (TypeError, ValueError) as exc:
            raise NativeStreamAdapterError("managed state must be JSON") from exc

        output = None
        if kind == "YIELD":
            payload = _encode(value)
            output = self._prototype.Output(
                binding.output_channel,
                "p0",
                f"native-output:{binding.consumer_id}:{record.sequence}",
                self._prototype.content_object_key(payload, self.schema_id),
                payload,
                self.schema_id,
                hashlib.sha256(
                    f"{binding.input_id}:{binding.consumer_id}:{record.sequence}".encode()
                ).hexdigest(),
            )
        elif kind == "RETURN":
            if value is not None:
                raise NativeStreamAdapterError("RETURN cannot publish an output value")
        else:
            raise NativeStreamAdapterError("unsupported terminal kind")

        def fault(point: str) -> None:
            if (
                point == "completion:after_progress_before_commit"
                and self._consume_fault("before_commit")
            ):
                raise InjectedNativeStreamFault(
                    "injected completion failure before commit"
                )

        try:
            await self._store.complete(
                token,
                kind,
                output=output,
                managed_state=state,
                state_schema="prefect-native-state/v1",
                state_version=1,
                fault=fault,
            )
        except self._prototype.StaleOwner as exc:
            raise StaleNativeStreamOwner(str(exc)) from exc
        except self._prototype.PostgresProtocolError as exc:
            raise NativeStreamAdapterError(str(exc)) from exc
        if self._consume_fault("lost_ack"):
            raise LostNativeStreamAcknowledgement(
                "completion committed before acknowledgement loss"
            )

    async def finish(self, stream_id: str, error: str | None = None) -> None:
        await self._proxy(self._finish(stream_id, error))

    async def _finish(self, stream_id: str, error: str | None) -> None:
        desired = "FAILED" if error is not None else "COMPLETED"
        committed = False
        async with self._store.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM native_streams_v1 WHERE stream_id=$1 FOR UPDATE",
                stream_id,
            )
            if row is None:
                raise NativeStreamAdapterError("unknown stream")
            if row["state"] != "OPEN":
                if (row["state"], row["error"]) != (desired, error):
                    raise NativeStreamAdapterError("conflicting stream finish")
            else:
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    f"{self.schema}|{row['channel']}|p0",
                )
                through = await conn.fetchval(
                    "SELECT next_sequence-1 FROM partitions "
                    "WHERE channel=$1 AND partition_id='p0' FOR UPDATE",
                    row["channel"],
                )
                updated = await conn.execute(
                    """UPDATE native_streams_v1 SET state=$2,error=$3,
                           closed_through=$4,finished_at=clock_timestamp()
                       WHERE stream_id=$1 AND state='OPEN'""",
                    stream_id,
                    desired,
                    error,
                    through,
                )
                if updated != "UPDATE 1":
                    raise NativeStreamAdapterError("stream finish race")
                committed = True
        if (
            committed
            and desired == "COMPLETED"
            and self._consume_fault("finish_lost_ack")
        ):
            raise LostNativeStreamAcknowledgement(
                "stream finish committed before acknowledgement loss"
            )

    async def status(self, stream_id: str) -> StreamStatus:
        return await self._proxy(self._status(stream_id))

    async def _status(self, stream_id: str) -> StreamStatus:
        async with self._store.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state,error FROM native_streams_v1 WHERE stream_id=$1",
                stream_id,
            )
        if row is None:
            raise NativeStreamAdapterError("unknown stream")
        return StreamStatus(row["state"], row["error"])

    async def read_prefix(self, stream_id: str, limit: int) -> list[Any]:
        return await self._proxy(self._read_prefix(stream_id, limit))

    async def _read_prefix(self, stream_id: str, limit: int) -> list[Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise NativeStreamAdapterError("prefix limit must be positive")
        async with self._store.pool.acquire() as conn:
            stream = await conn.fetchrow(
                "SELECT channel FROM native_streams_v1 WHERE stream_id=$1",
                stream_id,
            )
            if stream is None:
                raise NativeStreamAdapterError("unknown stream")
            rows = await conn.fetch(
                """SELECT o.payload FROM records r JOIN objects o USING(object_key)
                   WHERE r.channel=$1 AND r.partition_id='p0'
                     AND o.published_at IS NOT NULL
                   ORDER BY r.sequence LIMIT $2""",
                stream["channel"],
                limit,
            )
        return [_decode(bytes(row["payload"])) for row in rows]

    async def append(self, stream_id: str, value: Any) -> int:
        """Publish one deterministic fixture record while the stream is open."""

        return await self._proxy(self._append(stream_id, value))

    async def _append(self, stream_id: str, value: Any) -> int:
        payload = _encode(value)
        async with self._store.pool.acquire() as conn, conn.transaction():
            stream = await conn.fetchrow(
                "SELECT * FROM native_streams_v1 WHERE stream_id=$1 FOR UPDATE",
                stream_id,
            )
            if stream is None:
                raise NativeStreamAdapterError("unknown stream")
            if stream["state"] != "OPEN":
                raise NativeStreamAdapterError("cannot append after stream finish")
            ordinal = stream["next_publish_ordinal"]
            record_id = f"native-source:{stream_id}:{ordinal}"
            object_key = self._prototype.content_object_key(payload, self.schema_id)
            result = await self._store._append_with_connection(
                conn,
                f"native-source-attempt:{stream_id}:{ordinal}",
                stream["channel"],
                "p0",
                record_id,
                object_key,
                payload,
                self.schema_id,
                hashlib.sha256(record_id.encode()).hexdigest(),
            )
            await conn.execute(
                """UPDATE native_streams_v1 SET next_publish_ordinal=$2
                   WHERE stream_id=$1 AND state='OPEN'""",
                stream_id,
                ordinal + 1,
            )
            return result.sequence

    async def seed(
        self, stream_id: str, values: list[Any], *, close: bool = True
    ) -> list[int]:
        await self.create_stream(stream_id)
        sequences = [await self.append(stream_id, value) for value in values]
        if close:
            await self.finish(stream_id)
        return sequences

    async def inspect_consumer(self, consumer_id: str) -> dict[str, Any]:
        return await self._proxy(self._inspect_consumer(consumer_id))

    async def _inspect_consumer(self, consumer_id: str) -> dict[str, Any]:
        position = await self._store.verified_position(consumer_id)
        async with self._store.pool.acquire() as conn:
            binding = await conn.fetchrow(
                "SELECT * FROM native_stream_bindings_v1 WHERE consumer_id=$1",
                consumer_id,
            )
            deliveries = await conn.fetch(
                """SELECT input_sequence,delivery_count
                   FROM native_delivery_observations_v1 WHERE consumer_id=$1
                   ORDER BY input_sequence""",
                consumer_id,
            )
        if binding is None:
            raise NativeStreamAdapterError("unknown consumer binding")
        result = dict(position)
        if isinstance(result["managed_state"], str):
            result["managed_state"] = json.loads(result["managed_state"])
        result["binding"] = dict(binding)
        result["deliveries"] = [dict(row) for row in deliveries]
        result["replay_count"] = sum(
            max(0, row["delivery_count"] - 1) for row in deliveries
        )
        return result

    async def inspect_output(self, stream_id: str) -> list[Any]:
        return await self.read_prefix(stream_id, 10_000)

    async def counts(self) -> dict[str, int]:
        return await self._proxy(self._counts())

    async def _counts(self) -> dict[str, int]:
        async with self._store.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT
                    (SELECT count(*) FROM native_streams_v1) AS streams,
                    (SELECT count(*) FROM native_stream_bindings_v1) AS bindings,
                    (SELECT count(*) FROM consumers) AS consumers,
                    (SELECT count(*) FROM records) AS records,
                    (SELECT count(*) FROM turn_completions) AS completions,
                    (SELECT COALESCE(sum(delivery_count),0)
                       FROM native_delivery_observations_v1) AS deliveries,
                    (SELECT COALESCE(sum(GREATEST(delivery_count-1,0)),0)
                       FROM native_delivery_observations_v1) AS replays"""
            )
        return {key: int(value) for key, value in dict(row).items()}

    async def close(self, *, drop_schema: bool = True) -> None:
        if self._closed:
            return
        if self._store is not None:
            await self._proxy(self._store.close())
        if drop_schema:
            await self._proxy(self._drop_schema())
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        await asyncio.to_thread(self._thread.join, 10)
        if self._thread.is_alive():
            raise RuntimeError("native stream storage thread did not stop")

    async def _drop_schema(self) -> None:
        if not self.schema.startswith("native_stage1_"):
            raise NativeStreamAdapterError("refusing to drop a non-test schema")
        connection = await asyncpg.connect(self.dsn)
        try:
            quoted = self.schema.replace('"', '""')
            await connection.execute(f'DROP SCHEMA IF EXISTS "{quoted}" CASCADE')
        finally:
            await connection.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


__all__ = [
    "InjectedNativeStreamFault",
    "LostNativeStreamAcknowledgement",
    "NativeStreamAdapterError",
    "NativeStreamBinding",
    "PostgresNativeStreamStorage",
    "StaleNativeStreamOwner",
]
