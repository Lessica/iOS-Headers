#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from io import BytesIO
import os
import re
import sys
import time
from typing import Any, Iterable, TypeVar

try:
    from clickhouse_driver import Client as ClickHouseNativeClient
except ImportError:
    ClickHouseNativeClient = None

try:
    from minio import Minio
except ImportError:
    Minio = None


UMBRELLA_SUFFIX = "-Umbrella.h"
DYLIB_UMBRELLA_SUFFIX = ".dylib-Umbrella.h"
MODULE_NAME_RE = re.compile(r"[^0-9A-Za-z_]")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][0-9A-Za-z_]*$")
T = TypeVar("T")


@dataclass(frozen=True)
class PathUpdate:
    path_id: int
    old_absolute_path: str
    new_absolute_path: str
    old_file_name: str
    new_file_name: str
    old_guard: str
    new_guard: str


@dataclass(frozen=True)
class PathCollision:
    update: PathUpdate
    existing_path_ids: tuple[int, ...]


@dataclass(frozen=True)
class ContentRef:
    version_num: int
    version_id: str
    path_id: int
    old_absolute_path: str
    content_id: int
    pack_object_key: str
    pack_offset: int
    pack_length: int


@dataclass(frozen=True)
class ContentUpdate:
    content_id: int
    content_hash: str
    pack_object_key: str
    pack_length: int
    byte_size: int


class ClickHouseClient:
    def __init__(self, host: str, port: int, database: str, user: str, password: str) -> None:
        if ClickHouseNativeClient is None:
            raise RuntimeError(
                "Missing dependency: clickhouse-driver. Install with: python3 -m pip install clickhouse-driver"
            )
        self.client = ClickHouseNativeClient(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10,
            send_receive_timeout=600,
        )

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        retries: int,
        retry_sleep: float,
        settings: dict[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                result = self.client.execute(sql, params or {}, settings=settings)
                if isinstance(result, list):
                    return [tuple(row) if not isinstance(row, tuple) else row for row in result]
                return []
            except Exception as exc:
                last_exc = exc
                if attempt >= retries:
                    break
                time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"ClickHouse SQL failed after retries: {sql[:240]}...") from last_exc

    def insert_rows(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        *,
        retries: int,
        retry_sleep: float,
    ) -> None:
        if not rows:
            return
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                self.client.execute(query, rows)
                return
            except Exception as exc:
                last_exc = exc
                if attempt >= retries:
                    break
                time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"ClickHouse insert failed for table {table}") from last_exc

    def scalar(
        self,
        sql: str,
        params: dict[str, Any] | None,
        *,
        retries: int,
        retry_sleep: float,
    ) -> Any:
        rows = self.execute(sql, params, retries=retries, retry_sleep=retry_sleep)
        if not rows:
            return None
        return rows[0][0]


class MinioStore:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool) -> None:
        if Minio is None:
            raise RuntimeError("Missing dependency: minio. Install with: python3 -m pip install minio")
        self.bucket = bucket
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def read_slice(self, object_key: str, offset: int, length: int) -> bytes:
        response = self.client.get_object(
            bucket_name=self.bucket,
            object_name=object_key,
            offset=offset,
            length=length,
        )
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def upload_bytes(
        self,
        object_key: str,
        payload: bytes,
        *,
        retries: int,
        retry_sleep: float,
    ) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                self.client.put_object(
                    bucket_name=self.bucket,
                    object_name=object_key,
                    data=BytesIO(payload),
                    length=len(payload),
                    content_type="text/plain",
                )
                return
            except Exception as exc:
                last_exc = exc
                if attempt >= retries:
                    break
                time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"MinIO upload failed: {self.bucket}/{object_key}") from last_exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair ipsw-generated .dylib umbrella header names and include guards."
    )
    parser.add_argument("--clickhouse-host", default=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"))
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=int(os.getenv("CLICKHOUSE_NATIVE_PORT", os.getenv("CLICKHOUSE_PORT", "19000"))),
    )
    parser.add_argument("--clickhouse-db", default=os.getenv("CLICKHOUSE_DB", "ios_headers"))
    parser.add_argument("--clickhouse-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    parser.add_argument(
        "--minio-endpoint",
        default=os.getenv("MINIO_ENDPOINT", f"127.0.0.1:{os.getenv('MINIO_API_PORT', '19001')}"),
    )
    parser.add_argument("--minio-access-key", default=os.getenv("MINIO_ROOT_USER", "minioadmin"))
    parser.add_argument("--minio-secret-key", default=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    parser.add_argument("--minio-bucket", default=os.getenv("MINIO_BUCKET", "ios-headers"))
    parser.add_argument("--minio-prefix", default=os.getenv("MINIO_PREFIX", ""))
    parser.add_argument("--minio-secure", action="store_true")
    parser.add_argument("--repair-object-prefix", default="repairs/dylib-umbrella")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--check-content",
        action="store_true",
        help="Read MinIO content in dry-run mode to count actual include-guard changes.",
    )
    parser.add_argument("--skip-filename-repair", action="store_true")
    parser.add_argument("--skip-content-repair", action="store_true")
    parser.add_argument("--allow-path-collisions", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--query-chunk-size", type=int, default=5000)
    parser.add_argument("--content-update-batch-size", type=int, default=100)
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=2.0)
    parser.add_argument("--mutations-sync", type=int, choices=[0, 1, 2], default=1)
    return parser.parse_args()


def _format_duration(seconds: float) -> str:
    seconds_int = max(0, int(seconds))
    minutes, sec = divmod(seconds_int, 60)
    hours, minute = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minute:02d}:{sec:02d}"
    return f"{minute:02d}:{sec:02d}"


def print_progress(prefix: str, done: int, total: int, start_ts: float, extra: str = "") -> None:
    elapsed = max(0.001, time.time() - start_ts)
    rate = done / elapsed
    ratio = done / total if total > 0 else 0.0
    remain = max(0, total - done)
    eta = remain / rate if rate > 0 else 0.0
    suffix = f" {extra}" if extra else ""
    print(
        f"[progress] {prefix}: {done}/{total} ({ratio * 100:.2f}%) "
        f"rate={rate:.2f}/s eta={_format_duration(eta)} elapsed={_format_duration(elapsed)}{suffix}",
        flush=True,
    )


def require_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.match(value):
        raise SystemExit(f"Invalid {label}: {value!r}")
    return value


def qident(value: str) -> str:
    require_identifier(value, "identifier")
    return f"`{value}`"


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_uint64_array(values: Iterable[int]) -> str:
    return "[" + ", ".join(f"toUInt64({int(value)})" for value in values) + "]"


def sql_uint32_array(values: Iterable[int]) -> str:
    return "[" + ", ".join(f"toUInt32({int(value)})" for value in values) + "]"


def sql_fixed_hash_array(values: Iterable[str]) -> str:
    return "[" + ", ".join(f"toFixedString({sql_string(value)}, 32)" for value in values) + "]"


def sql_string_array(values: Iterable[str]) -> str:
    return "[" + ", ".join(sql_string(value) for value in values) + "]"


def file_name_from_path(absolute_path: str) -> str:
    return absolute_path.rsplit("/", maxsplit=1)[-1]


def replace_file_name(absolute_path: str, new_file_name: str) -> str:
    prefix, separator, _old_name = absolute_path.rpartition("/")
    if not separator:
        return new_file_name
    return f"{prefix}/{new_file_name}"


def sanitize_module_name(module_name: str) -> str:
    return MODULE_NAME_RE.sub("_", module_name)


def guard_name_for_old_file(file_name: str) -> str:
    if not file_name.endswith(".h"):
        return file_name.replace("-", "_").replace(".", "_")
    return file_name[:-2].replace("-", "_") + "_h"


def build_path_update(path_id: int, absolute_path: str) -> PathUpdate | None:
    old_file_name = file_name_from_path(absolute_path)
    if not old_file_name.endswith(DYLIB_UMBRELLA_SUFFIX):
        return None
    old_module_name = old_file_name[: -len(UMBRELLA_SUFFIX)]
    module_without_dylib = old_module_name[: -len(".dylib")]
    new_module_name = sanitize_module_name(module_without_dylib)
    new_file_name = f"{new_module_name}{UMBRELLA_SUFFIX}"
    new_absolute_path = replace_file_name(absolute_path, new_file_name)
    if new_absolute_path == absolute_path:
        return None
    return PathUpdate(
        path_id=path_id,
        old_absolute_path=absolute_path,
        new_absolute_path=new_absolute_path,
        old_file_name=old_file_name,
        new_file_name=new_file_name,
        old_guard=guard_name_for_old_file(old_file_name),
        new_guard=f"{new_module_name}_Umbrella_h",
    )


def repair_content_bytes(raw: bytes, update: PathUpdate) -> bytes:
    old_guard = update.old_guard.encode("utf-8")
    new_guard = update.new_guard.encode("utf-8")
    if old_guard not in raw:
        return raw
    return raw.replace(old_guard, new_guard)


def make_repair_object_key(args: argparse.Namespace, content_id: int, payload: bytes) -> str:
    digest = hashlib.md5(payload).hexdigest()
    prefix_parts = [
        args.minio_prefix.strip("/"),
        args.repair_object_prefix.strip("/"),
    ]
    prefix = "/".join(part for part in prefix_parts if part)
    base = f"{content_id}-{digest}.h"
    return f"{prefix}/{base}" if prefix else base


def chunked(items: list[T], size: int) -> Iterable[list[T]]:
    chunk_size = max(1, size)
    for offset in range(0, len(items), chunk_size):
        yield items[offset : offset + chunk_size]


def load_path_updates(ch: ClickHouseClient, args: argparse.Namespace) -> list[PathUpdate]:
    limit_sql = f"LIMIT {args.limit}" if args.limit > 0 else ""
    rows = ch.execute(
        f"""
        SELECT path_id, absolute_path
        FROM paths
        WHERE endsWith(file_name, %(suffix)s)
        ORDER BY absolute_path
        {limit_sql}
        """,
        {"suffix": DYLIB_UMBRELLA_SUFFIX},
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    updates: list[PathUpdate] = []
    for path_id, absolute_path in rows:
        update = build_path_update(int(path_id), str(absolute_path))
        if update is not None:
            updates.append(update)
    return updates


def split_collisions(
    ch: ClickHouseClient,
    updates: list[PathUpdate],
    args: argparse.Namespace,
) -> tuple[list[PathUpdate], list[PathCollision]]:
    if not updates:
        return ([], [])

    by_new_path = {update.new_absolute_path: update for update in updates}
    updates_by_new_path: dict[str, list[PathUpdate]] = {}
    for update in updates:
        updates_by_new_path.setdefault(update.new_absolute_path, []).append(update)
    collision_path_ids_by_path: dict[str, tuple[int, ...]] = {}
    for path_chunk in chunked(sorted(by_new_path), args.query_chunk_size):
        rows = ch.execute(
            """
            SELECT absolute_path, groupArray(path_id)
            FROM paths
            WHERE absolute_path IN %(paths)s
            GROUP BY absolute_path
            """,
            {"paths": tuple(path_chunk)},
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
        for absolute_path, path_ids in rows:
            collision_path_ids_by_path[str(absolute_path)] = tuple(int(item) for item in path_ids)

    clean: list[PathUpdate] = []
    collisions: list[PathCollision] = []
    for update in updates:
        duplicate_target_path_ids = tuple(
            item.path_id
            for item in updates_by_new_path.get(update.new_absolute_path, [])
            if item.path_id != update.path_id
        )
        existing_path_ids = tuple(
            path_id
            for path_id in collision_path_ids_by_path.get(update.new_absolute_path, ())
            if path_id != update.path_id
        )
        collision_path_ids = duplicate_target_path_ids + existing_path_ids
        if collision_path_ids:
            collisions.append(PathCollision(update=update, existing_path_ids=collision_path_ids))
            if args.allow_path_collisions:
                clean.append(update)
            continue
        clean.append(update)
    return (clean, collisions)


def load_content_refs(
    ch: ClickHouseClient,
    updates: list[PathUpdate],
    args: argparse.Namespace,
) -> list[ContentRef]:
    if not updates:
        return []

    refs: list[ContentRef] = []
    path_ids = [update.path_id for update in updates]
    for path_id_chunk in chunked(path_ids, args.query_chunk_size):
        rows = ch.execute(
            """
            SELECT DISTINCT
                fi.version_num,
                v.version_id,
                fi.path_id,
                p.absolute_path,
                fi.content_id,
                c.pack_object_key,
                c.pack_offset,
                c.pack_length
            FROM file_instances AS fi
            INNER JOIN paths AS p ON p.path_id = fi.path_id
            INNER JOIN versions AS v ON v.version_num = fi.version_num
            INNER JOIN contents AS c ON c.content_id = fi.content_id
            WHERE fi.path_id IN %(path_ids)s
            ORDER BY fi.path_id ASC, fi.version_num ASC
            """,
            {"path_ids": tuple(path_id_chunk)},
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
        for row in rows:
            refs.append(
                ContentRef(
                    version_num=int(row[0]),
                    version_id=str(row[1]),
                    path_id=int(row[2]),
                    old_absolute_path=str(row[3]),
                    content_id=int(row[4]),
                    pack_object_key=str(row[5]),
                    pack_offset=int(row[6]),
                    pack_length=int(row[7]),
                )
            )
    return refs


def update_contents_batch(
    ch: ClickHouseClient,
    updates: list[ContentUpdate],
    args: argparse.Namespace,
) -> None:
    if not updates:
        return
    ids = [item.content_id for item in updates]
    id_array = sql_uint64_array(ids)
    sql = f"""
    ALTER TABLE contents
    UPDATE
        content_hash = transform(content_id, {id_array}, {sql_fixed_hash_array(item.content_hash for item in updates)}, content_hash),
        pack_object_key = transform(content_id, {id_array}, {sql_string_array(item.pack_object_key for item in updates)}, pack_object_key),
        pack_offset = transform(content_id, {id_array}, {sql_uint64_array(0 for _item in updates)}, pack_offset),
        pack_length = transform(content_id, {id_array}, {sql_uint32_array(item.pack_length for item in updates)}, pack_length),
        byte_size = transform(content_id, {id_array}, {sql_uint32_array(item.byte_size for item in updates)}, byte_size)
    WHERE content_id IN {id_array}
    """
    ch.execute(
        sql,
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
        settings={"mutations_sync": args.mutations_sync},
    )


def repair_contents(
    ch: ClickHouseClient,
    store: MinioStore | None,
    refs: list[ContentRef],
    updates_by_path_id: dict[int, PathUpdate],
    args: argparse.Namespace,
) -> tuple[int, int, int]:
    if not refs:
        return (0, 0, 0)
    if store is None:
        if args.dry_run and not args.check_content:
            return (len(refs), 0, 0)
        raise RuntimeError("MinIO is required for content repair")

    start_ts = time.time()
    scanned = 0
    changed = 0
    unchanged = 0
    pending_updates: list[ContentUpdate] = []

    for ref in refs:
        path_update = updates_by_path_id[ref.path_id]
        raw = store.read_slice(ref.pack_object_key, ref.pack_offset, ref.pack_length)
        fixed = repair_content_bytes(raw, path_update)
        scanned += 1

        if fixed == raw:
            unchanged += 1
        else:
            changed += 1
            if not args.dry_run:
                object_key = make_repair_object_key(args, ref.content_id, fixed)
                store.upload_bytes(
                    object_key,
                    fixed,
                    retries=args.max_retries,
                    retry_sleep=args.retry_sleep,
                )
                pending_updates.append(
                    ContentUpdate(
                        content_id=ref.content_id,
                        content_hash=hashlib.md5(fixed).hexdigest(),
                        pack_object_key=object_key,
                        pack_length=len(fixed),
                        byte_size=len(fixed),
                    )
                )
                if len(pending_updates) >= args.content_update_batch_size:
                    update_contents_batch(ch, pending_updates, args)
                    pending_updates.clear()

        if args.progress_every > 0 and scanned % args.progress_every == 0:
            print_progress(
                "contents",
                scanned,
                len(refs),
                start_ts,
                extra=f"changed={changed} unchanged={unchanged}",
            )

    if pending_updates:
        update_contents_batch(ch, pending_updates, args)

    print_progress(
        "contents",
        scanned,
        len(refs),
        start_ts,
        extra=f"changed={changed} unchanged={unchanged}",
    )
    return (scanned, changed, unchanged)


def create_paths_table_sql(table_name: str) -> str:
    table = qident(table_name)
    return f"""
    CREATE TABLE {table} (
        path_id UInt64,
        absolute_path String,
        path_lc String MATERIALIZED lowerUTF8(absolute_path),
        file_name String MATERIALIZED extract(absolute_path, '[^/]+$'),
        file_name_lc String MATERIALIZED lowerUTF8(file_name),
        dir_path String MATERIALIZED replaceRegexpOne(absolute_path, '/[^/]+$', ''),
        dir_name String MATERIALIZED arrayStringConcat(
            arraySlice(
                arrayFilter(segment -> segment != '', splitByChar('/', dir_path)),
                -2
            ),
            '/'
        ),
        dir_name_lc String MATERIALIZED lowerUTF8(dir_name),
        created_at DateTime DEFAULT now(),
        INDEX idx_paths_bf path_lc TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 64,
        INDEX idx_paths_file_name_bf file_name_lc TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 64,
        INDEX idx_paths_file_name_ngram file_name_lc TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 64,
        INDEX idx_paths_dir_name_bf dir_name_lc TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 64,
        INDEX idx_paths_absolute_path_bf absolute_path TYPE bloom_filter(0.01) GRANULARITY 64
    )
    ENGINE = MergeTree
    ORDER BY (path_id)
    SETTINGS index_granularity = 8192
    """


def recreate_path_dictionaries(ch: ClickHouseClient, db: str, args: argparse.Namespace) -> None:
    db_qualified = qident(db)
    for dictionary in ["paths_by_absolute_path_dict", "paths_by_id_dict"]:
        ch.execute(
            f"DROP DICTIONARY IF EXISTS {db_qualified}.{qident(dictionary)}",
            None,
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )

    ch.execute(
        f"""
        CREATE DICTIONARY IF NOT EXISTS {db_qualified}.`paths_by_absolute_path_dict`
        (
            absolute_path String,
            path_id UInt64
        )
        PRIMARY KEY absolute_path
        SOURCE(
            CLICKHOUSE(
                NAME 'ios_headers_internal'
                DB {sql_string(db)}
                TABLE 'paths'
            )
        )
        LAYOUT(HASHED())
        LIFETIME(0)
        """,
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.execute(
        f"""
        CREATE DICTIONARY IF NOT EXISTS {db_qualified}.`paths_by_id_dict`
        (
            path_id UInt64,
            absolute_path String,
            dir_name String,
            dir_path String,
            file_name_lc String
        )
        PRIMARY KEY path_id
        SOURCE(
            CLICKHOUSE(
                NAME 'ios_headers_internal'
                DB {sql_string(db)}
                TABLE 'paths'
            )
        )
        LAYOUT(HASHED())
        LIFETIME(0)
        """,
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    for dictionary in ["paths_by_absolute_path_dict", "paths_by_id_dict"]:
        ch.execute(
            f"SYSTEM RELOAD DICTIONARY {db_qualified}.{qident(dictionary)}",
            None,
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )


def rebuild_paths_table(ch: ClickHouseClient, updates: list[PathUpdate], args: argparse.Namespace) -> str | None:
    if not updates:
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    tmp_table = f"paths_dylib_umbrella_tmp_{timestamp}"
    map_table = f"paths_dylib_umbrella_map_{timestamp}"
    backup_table = f"paths_before_dylib_umbrella_{timestamp}"

    total_paths_before = int(
        ch.scalar("SELECT count() FROM paths", None, retries=args.max_retries, retry_sleep=args.retry_sleep)
    )

    ch.execute(
        f"DROP TABLE IF EXISTS {qident(tmp_table)}",
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.execute(
        f"DROP TABLE IF EXISTS {qident(map_table)}",
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.execute(
        create_paths_table_sql(tmp_table),
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.execute(
        f"""
        CREATE TABLE {qident(map_table)} (
            path_id UInt64,
            new_absolute_path String
        )
        ENGINE = Memory
        """,
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.insert_rows(
        map_table,
        ["path_id", "new_absolute_path"],
        [(update.path_id, update.new_absolute_path) for update in updates],
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.execute(
        f"""
        INSERT INTO {qident(tmp_table)} (path_id, absolute_path, created_at)
        SELECT
            p.path_id,
            if(length(m.new_absolute_path) > 0, m.new_absolute_path, p.absolute_path) AS absolute_path,
            p.created_at
        FROM paths AS p
        LEFT JOIN {qident(map_table)} AS m ON p.path_id = m.path_id
        """,
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )

    total_paths_after = int(
        ch.scalar(
            f"SELECT count() FROM {qident(tmp_table)}",
            None,
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
    )
    if total_paths_after != total_paths_before:
        raise RuntimeError(
            f"Refuse to swap paths table: source rows={total_paths_before} tmp rows={total_paths_after}"
        )

    ch.execute(
        f"DROP DICTIONARY IF EXISTS {qident(args.clickhouse_db)}.`paths_by_absolute_path_dict`",
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.execute(
        f"DROP DICTIONARY IF EXISTS {qident(args.clickhouse_db)}.`paths_by_id_dict`",
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    ch.execute(
        f"RENAME TABLE paths TO {qident(backup_table)}, {qident(tmp_table)} TO paths",
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    recreate_path_dictionaries(ch, args.clickhouse_db, args)
    ch.execute(
        f"DROP TABLE IF EXISTS {qident(map_table)}",
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )
    return backup_table


def reload_content_dictionary(ch: ClickHouseClient, args: argparse.Namespace) -> None:
    ch.execute(
        f"SYSTEM RELOAD DICTIONARY {qident(args.clickhouse_db)}.`contents_by_content_id_dict`",
        None,
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    )


def print_collision_warnings(collisions: list[PathCollision]) -> None:
    for collision in collisions[:20]:
        existing = ",".join(str(item) for item in collision.existing_path_ids)
        print(
            f"[warn] path collision: path_id={collision.update.path_id} "
            f"new_path={collision.update.new_absolute_path} existing_path_ids={existing}",
            file=sys.stderr,
        )
    if len(collisions) > 20:
        print(f"[warn] omitted {len(collisions) - 20} additional path collisions", file=sys.stderr)


def main() -> None:
    args = parse_args()
    require_identifier(args.clickhouse_db, "clickhouse database")

    ch = ClickHouseClient(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        database=args.clickhouse_db,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )
    ch.execute("SELECT 1", None, retries=args.max_retries, retry_sleep=args.retry_sleep)

    print("[scan] loading .dylib umbrella path candidates", flush=True)
    raw_updates = load_path_updates(ch, args)
    updates, collisions = split_collisions(ch, raw_updates, args)
    if collisions:
        print_collision_warnings(collisions)

    print(
        f"[scan] candidate_paths={len(raw_updates)} repairable_paths={len(updates)} "
        f"collisions={len(collisions)} dry_run={args.dry_run}",
        flush=True,
    )

    content_refs = [] if args.skip_content_repair else load_content_refs(ch, updates, args)
    print(f"[scan] content_refs={len(content_refs)}", flush=True)

    if args.dry_run:
        if args.check_content and content_refs:
            store = MinioStore(
                endpoint=args.minio_endpoint,
                access_key=args.minio_access_key,
                secret_key=args.minio_secret_key,
                bucket=args.minio_bucket,
                secure=args.minio_secure,
            )
            scanned, changed, unchanged = repair_contents(
                ch,
                store,
                content_refs,
                {update.path_id: update for update in updates},
                args,
            )
            print(
                f"[dry-run] paths_to_repair={len(updates)} content_refs_scanned={scanned} "
                f"content_refs_to_repair={changed} content_refs_unchanged={unchanged}",
                flush=True,
            )
        else:
            print(
                f"[dry-run] paths_to_repair={len(updates)} content_refs_to_check={len(content_refs)}",
                flush=True,
            )
        return

    if not updates:
        print("[done] no repairable paths found", flush=True)
        return

    store = None
    if not args.skip_content_repair and content_refs:
        store = MinioStore(
            endpoint=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            bucket=args.minio_bucket,
            secure=args.minio_secure,
        )
        scanned, changed, unchanged = repair_contents(
            ch,
            store,
            content_refs,
            {update.path_id: update for update in updates},
            args,
        )
        print(
            f"[content-done] scanned={scanned} repaired={changed} unchanged={unchanged}",
            flush=True,
        )
        if changed > 0:
            reload_content_dictionary(ch, args)

    backup_table = None
    if not args.skip_filename_repair:
        backup_table = rebuild_paths_table(ch, updates, args)

    print(
        f"[done] repaired_paths={0 if args.skip_filename_repair else len(updates)} "
        f"path_backup_table={backup_table or 'none'}",
        flush=True,
    )


if __name__ == "__main__":
    main()
