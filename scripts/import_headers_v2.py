#!/usr/bin/env python3
from __future__ import annotations

import argparse
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import plistlib
import random
import re
import sys
import time
from typing import Any
from xml.parsers.expat import ExpatError

try:
    from clickhouse_driver import Client as ClickHouseNativeClient
except ImportError:
    ClickHouseNativeClient = None

try:
    from minio import Minio
except ImportError:
    Minio = None


SYMBOL_TYPES = {
    "ivar",
    "property",
    "class_method",
    "instance_method",
}


@dataclass(frozen=True)
class VersionInfo:
    version_num: int
    version_id: str
    ios_version: str
    build: str
    bundle_name: str


@dataclass(frozen=True)
class ParsedSymbol:
    owner_kind: str
    owner_name: str
    symbol_type: str
    symbol_key: str
    line_no: int


@dataclass(frozen=True)
class Issue:
    path: Path
    reason: str


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

    def execute(self, sql: str, retries: int = 3, retry_sleep: float = 1.0) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                result = self.client.execute(sql)
                if isinstance(result, list):
                    if not result:
                        return ""
                    first = result[0]
                    if isinstance(first, (tuple, list)):
                        return "\n".join(
                            "\t".join(_escape_tsv(value) for value in row)
                            for row in result
                            if isinstance(row, (tuple, list))
                        )
                    return "\n".join(_escape_tsv(item) for item in result)
                if result is None:
                    return ""
                return _escape_tsv(result)
            except Exception as exc:
                last_exc = exc
            if attempt == retries:
                break
            time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"ClickHouse SQL failed after retries: {sql[:200]}...") from last_exc

    def insert_tsv(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        retries: int = 3,
        retry_sleep: float = 1.0,
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
                if attempt == retries:
                    break
                time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"ClickHouse insert failed for table {table}") from last_exc


class MinioUploader:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool,
    ) -> None:
        if Minio is None:
            raise RuntimeError(
                "Missing dependency: minio. Install with: python3 -m pip install minio"
            )
        self.bucket = bucket
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    def upload_bytes(
        self,
        object_name: str,
        payload: bytes,
        retries: int = 3,
        retry_sleep: float = 1.0,
    ) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                stream = BytesIO(payload)
                self.client.put_object(
                    self.bucket,
                    object_name,
                    stream,
                    length=len(payload),
                    content_type="text/plain",
                )
                return
            except Exception as exc:
                last_exc = exc
                if attempt == retries:
                    break
                time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"MinIO upload failed: {self.bucket}/{object_name}") from last_exc


class PackedMinioWriter:
    def __init__(
        self,
        uploader: MinioUploader,
        *,
        minio_prefix: str,
        shards: int,
        target_bytes: int,
        retries: int,
        retry_sleep: float,
    ) -> None:
        self.uploader = uploader
        self.minio_prefix = minio_prefix.strip("/")
        self.shards = max(1, shards)
        self.target_bytes = max(1, target_bytes)
        self.retries = retries
        self.retry_sleep = retry_sleep
        self.buffers: dict[int, bytearray] = {}
        self.object_keys: dict[int, str] = {}
        self.sequence: dict[int, int] = {}

    def _next_object_key(self, shard_id: int) -> str:
        current = self.sequence.get(shard_id, 0) + 1
        self.sequence[shard_id] = current
        suffix = random.randint(0, 0xFFFFFF)
        base = f"packs/s{shard_id:03d}/p{current:09d}-{suffix:06x}.bin"
        return f"{self.minio_prefix}/{base}".lstrip("/") if self.minio_prefix else base

    def _ensure_shard_open(self, shard_id: int) -> None:
        if shard_id not in self.buffers:
            self.buffers[shard_id] = bytearray()
            self.object_keys[shard_id] = self._next_object_key(shard_id)

    def _flush_shard(self, shard_id: int) -> None:
        buffer = self.buffers.get(shard_id)
        if buffer is None or len(buffer) == 0:
            return
        object_key = self.object_keys[shard_id]
        self.uploader.upload_bytes(
            object_key,
            bytes(buffer),
            retries=self.retries,
            retry_sleep=self.retry_sleep,
        )
        self.buffers[shard_id] = bytearray()
        self.object_keys[shard_id] = self._next_object_key(shard_id)

    def add(self, shard_seed: str, payload: bytes) -> tuple[str, int, int]:
        digest = hashlib.blake2b(shard_seed.encode("utf-8"), digest_size=8).digest()
        shard_id = int.from_bytes(digest, "big") % self.shards
        self._ensure_shard_open(shard_id)

        if len(self.buffers[shard_id]) > 0 and len(self.buffers[shard_id]) + len(payload) > self.target_bytes:
            self._flush_shard(shard_id)

        offset = len(self.buffers[shard_id])
        self.buffers[shard_id].extend(payload)
        object_key = self.object_keys[shard_id]
        length = len(payload)

        if len(self.buffers[shard_id]) >= self.target_bytes:
            self._flush_shard(shard_id)

        return (object_key, offset, length)

    def flush_all(self) -> None:
        for shard_id in list(self.buffers.keys()):
            self._flush_shard(shard_id)


def _escape_tsv(value: Any) -> str:
    if value is None:
        return "\\N"
    text = str(value)
    text = text.replace("\\", "\\\\")
    text = text.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
    return text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import iOS headers into ClickHouse (v2, no content dedup)."
    )
    parser.add_argument("--headers-root", type=Path, default=Path(".headers"))
    parser.add_argument("--files-root", type=Path, default=Path(".files"))
    parser.add_argument("--state-file", type=Path, default=Path("data/import_state_v2_no_dedup.json"))
    parser.add_argument("--clickhouse-host", default="127.0.0.1")
    parser.add_argument("--clickhouse-port", type=int, default=19000)
    parser.add_argument("--clickhouse-db", default="ios_headers")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--batch-size", type=int, default=30000)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=2.0)
    parser.add_argument("--minio-endpoint", default="127.0.0.1:19001")
    parser.add_argument("--minio-access-key", default="minioadmin")
    parser.add_argument("--minio-secret-key", default="minioadmin")
    parser.add_argument("--minio-bucket", default="ios-headers")
    parser.add_argument("--minio-prefix", default="")
    parser.add_argument("--minio-secure", action="store_true")
    parser.add_argument("--skip-minio-upload", action="store_true")
    parser.add_argument("--pack-shards", type=int, default=64)
    parser.add_argument("--pack-target-bytes", type=int, default=134217728)
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--bundle", action="append", default=[])
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--truncate-all", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--progress-every", type=int, default=5000)
    parser.add_argument(
        "--allow-old-versions",
        action="store_true",
        help="Allow importing versions older than the current newest version in database",
    )
    return parser.parse_args()


def _format_duration(seconds: float) -> str:
    seconds_int = max(0, int(seconds))
    minutes, sec = divmod(seconds_int, 60)
    hours, minute = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minute:02d}:{sec:02d}"
    return f"{minute:02d}:{sec:02d}"


def _print_progress(prefix: str, done: int, total: int, start_ts: float) -> None:
    elapsed = max(0.001, time.time() - start_ts)
    rate = done / elapsed
    ratio = (done / total) if total > 0 else 0.0
    remain = max(0, total - done)
    eta = remain / rate if rate > 0 else 0.0
    print(
        f"[progress] {prefix}: {done}/{total} ({ratio * 100:.2f}%) "
        f"rate={rate:.2f} files/s eta={_format_duration(eta)} elapsed={_format_duration(elapsed)}"
    )


def parse_version_tuple(version: str) -> tuple[int, ...]:
    tokens: list[int] = []
    for token in version.split("."):
        if token.isdigit():
            tokens.append(int(token))
        else:
            tokens.append(-1)
    return tuple(tokens)


def version_sort_key(ios_version: str, build: str) -> tuple[tuple[int, ...], str]:
    return (parse_version_tuple(ios_version), build)


def to_absolute_path(path_text: str) -> str:
    normalized = path_text.strip()
    return "/" + normalized.lstrip("/")


def read_plist(path: Path) -> dict[str, Any]:
    with path.open("rb") as file_obj:
        data = plistlib.load(file_obj)
    if not isinstance(data, dict):
        raise ValueError("Plist root is not a dictionary")
    return data


def try_read_plist(path: Path, issues: list[Issue]) -> dict[str, Any] | None:
    try:
        return read_plist(path)
    except (OSError, plistlib.InvalidFileException, ExpatError, ValueError) as exc:
        issues.append(Issue(path=path, reason=str(exc)))
        return None


def build_version_info(bundle_name: str, files_root: Path, issues: list[Issue]) -> VersionInfo:
    fallback_build = bundle_name.split("__", maxsplit=1)[0]

    metadata_dir = files_root / bundle_name
    metadata = None

    system_version_plist = metadata_dir / "SystemVersion.plist"
    system_version_plist_nested = (
        metadata_dir / "System" / "Library" / "CoreServices" / "SystemVersion.plist"
    )
    restore_plist = metadata_dir / "Restore.plist"

    if system_version_plist.exists():
        metadata = try_read_plist(system_version_plist, issues)
    if metadata is None and system_version_plist_nested.exists():
        metadata = try_read_plist(system_version_plist_nested, issues)
    if metadata is None and restore_plist.exists():
        metadata = try_read_plist(restore_plist, issues)

    if metadata is None:
        raise FileNotFoundError(
            "Unable to determine iOS version metadata for "
            f"{bundle_name}; tried: {system_version_plist}, "
            f"{system_version_plist_nested}, {restore_plist}"
        )

    ios_version = str(metadata.get("ProductVersion", "")).strip()
    if not ios_version:
        raise ValueError(f"Missing ProductVersion in metadata for {bundle_name}")

    build = str(metadata.get("ProductBuildVersion", fallback_build))
    version_id = f"{ios_version}|{build}"
    return VersionInfo(
        version_num=0,
        version_id=version_id,
        ios_version=ios_version,
        build=build,
        bundle_name=bundle_name,
    )


def extract_selector(signature_line: str) -> str | None:
    match = re.match(r"^[+-]\s*\([^)]*\)\s*(.*);\s*$", signature_line)
    if not match:
        return None
    method_tail = match.group(1)
    parts = re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*:", method_tail)
    if parts:
        return "".join(f"{part}:" for part in parts)
    head = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", method_tail)
    if head:
        return head.group(1)
    return None


def extract_property_name(line: str) -> str | None:
    if "@property" not in line:
        return None
    match = re.match(r"^\s*@property\b.*\b([A-Za-z_][A-Za-z0-9_]*)\s*;\s*$", line)
    if match:
        return match.group(1)
    return None


def extract_ivar_name(line: str) -> str | None:
    if ";" not in line or line.strip().startswith("/*"):
        return None
    if "(" in line and ")" in line and line.lstrip().startswith(("+", "-")):
        return None

    cleaned = line.rsplit(";", maxsplit=1)[0]
    match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*$", cleaned)
    if match:
        return match.group(1)
    return None


def parse_owner_from_interface(line: str) -> tuple[str, str] | None:
    interface_match = re.match(r"^\s*@interface\s+([A-Za-z_][A-Za-z0-9_]*)", line)
    if interface_match:
        return ("interface", interface_match.group(1))

    protocol_match = re.match(r"^\s*@protocol\s+([A-Za-z_][A-Za-z0-9_]*)", line)
    if protocol_match:
        return ("protocol", protocol_match.group(1))
    return None


def parse_header_symbols(path: Path) -> list[ParsedSymbol]:
    symbols: list[ParsedSymbol] = []
    owner_kind = "global"
    owner_name = "(global)"
    in_ivar_block = False

    with path.open("r", encoding="utf-8", errors="replace") as file_obj:
        for index, raw_line in enumerate(file_obj, start=1):
            line = raw_line.rstrip("\n")
            stripped = line.strip()

            owner_info = parse_owner_from_interface(line)
            if owner_info is not None:
                owner_kind, owner_name = owner_info
                in_ivar_block = "{" in line
                continue

            if stripped.startswith("@end"):
                owner_kind = "global"
                owner_name = "(global)"
                in_ivar_block = False
                continue

            if stripped == "{":
                in_ivar_block = True
                continue
            if stripped == "}":
                in_ivar_block = False
                continue

            property_name = extract_property_name(line)
            if property_name:
                symbols.append(
                    ParsedSymbol(
                        owner_kind=owner_kind,
                        owner_name=owner_name,
                        symbol_type="property",
                        symbol_key=property_name,
                        line_no=index,
                    )
                )
                continue

            if in_ivar_block and stripped and not stripped.startswith("/*"):
                ivar_name = extract_ivar_name(line)
                if ivar_name:
                    symbols.append(
                        ParsedSymbol(
                            owner_kind=owner_kind,
                            owner_name=owner_name,
                            symbol_type="ivar",
                            symbol_key=ivar_name,
                            line_no=index,
                        )
                    )
                    continue

            if stripped.startswith(("+", "-")) and stripped.endswith(";"):
                selector = extract_selector(stripped)
                if selector:
                    symbol_type = "class_method" if stripped.startswith("+") else "instance_method"
                    symbols.append(
                        ParsedSymbol(
                            owner_kind=owner_kind,
                            owner_name=owner_name,
                            symbol_type=symbol_type,
                            symbol_key=selector,
                            line_no=index,
                        )
                    )

    return symbols


def content_id_for(version_id: str, absolute_path: str) -> int:
    digest = hashlib.blake2b(
        f"{version_id}\0{absolute_path}".encode("utf-8"), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big", signed=False)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"bundles": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"bundles": {}}
    if not isinstance(data, dict):
        return {"bundles": {}}
    bundles = data.get("bundles")
    if not isinstance(bundles, dict):
        data["bundles"] = {}
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def build_versions(headers_root: Path, files_root: Path, bundles: list[str]) -> list[VersionInfo]:
    issues: list[Issue] = []
    selected = set(bundles)
    bundle_dirs = sorted([item for item in headers_root.iterdir() if item.is_dir()])
    if selected:
        bundle_dirs = [item for item in bundle_dirs if item.name in selected]

    loaded: list[VersionInfo] = []
    for bundle_dir in bundle_dirs:
        loaded.append(build_version_info(bundle_dir.name, files_root, issues))

    if issues:
        for issue in issues:
            print(f"[issue] {issue.path}: {issue.reason}", file=sys.stderr)
    return loaded


def _existing_version_map(ch: ClickHouseClient, args: argparse.Namespace) -> dict[str, int]:
    rows = ch.execute(
        "SELECT version_id, version_num FROM versions FORMAT TSV",
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    ).splitlines()
    result: dict[str, int] = {}
    for row in rows:
        if not row:
            continue
        parts = row.split("\t")
        if len(parts) != 2:
            continue
        result[parts[0]] = int(parts[1])
    return result


def assign_version_numbers(
    raw_versions: list[VersionInfo],
    existing_map: dict[str, int],
    *,
    allow_old_versions: bool,
) -> list[VersionInfo]:
    sorted_versions = sorted(raw_versions, key=lambda item: version_sort_key(item.ios_version, item.build))

    existing_max_num = max(existing_map.values(), default=0)
    existing_max_key: tuple[tuple[int, ...], str] | None = None
    if existing_map:
        pairs = []
        for version_id in existing_map:
            ios_version, build = version_id.split("|", maxsplit=1)
            pairs.append(version_sort_key(ios_version, build))
        existing_max_key = max(pairs)

    next_new_num = existing_max_num + 1
    assigned: list[VersionInfo] = []

    for item in sorted_versions:
        if item.version_id in existing_map:
            assigned.append(
                VersionInfo(
                    version_num=existing_map[item.version_id],
                    version_id=item.version_id,
                    ios_version=item.ios_version,
                    build=item.build,
                    bundle_name=item.bundle_name,
                )
            )
            continue

        current_key = version_sort_key(item.ios_version, item.build)
        if not allow_old_versions and existing_max_key is not None and current_key <= existing_max_key:
            raise SystemExit(
                "Detected import of an older version than current latest. "
                "Use --allow-old-versions to override. "
                f"Incoming={item.version_id} Latest={max(existing_map, key=lambda k: existing_map[k])}"
            )

        assigned.append(
            VersionInfo(
                version_num=next_new_num,
                version_id=item.version_id,
                ios_version=item.ios_version,
                build=item.build,
                bundle_name=item.bundle_name,
            )
        )
        next_new_num += 1

    return assigned


def parse_file_task(file_path: Path) -> tuple[str, int, list[ParsedSymbol], bytes]:
    raw_bytes = file_path.read_bytes()
    text_md5 = hashlib.md5(raw_bytes).hexdigest()
    symbols = parse_header_symbols(file_path)
    return (text_md5, len(raw_bytes), symbols, raw_bytes)


def import_bundle(
    ch: ClickHouseClient,
    uploader: MinioUploader | None,
    packer: PackedMinioWriter | None,
    version: VersionInfo,
    headers_root: Path,
    state: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[int, int]:
    bundle_root = headers_root / version.bundle_name
    files = sorted(bundle_root.rglob("*.h"))
    if args.max_files > 0:
        files = files[: args.max_files]

    bundle_state = state.setdefault("bundles", {}).setdefault(version.bundle_name, {})
    start_index = int(bundle_state.get("next_index", 0)) if args.resume else 0
    if start_index >= len(files):
        print(f"[skip] {version.bundle_name}: already completed")
        return (0, 0)

    print(
        f"[bundle] {version.bundle_name} version={version.version_id} files={len(files)} start={start_index}"
    )

    contents_rows: list[tuple[Any, ...]] = []
    file_instance_rows: list[tuple[Any, ...]] = []
    symbols_rows: list[tuple[Any, ...]] = []
    total_files = 0
    total_symbols = 0
    total_pending = len(files) - start_index
    bundle_start_ts = time.time()

    def flush_batches() -> None:
        if packer is not None:
            packer.flush_all()

        if contents_rows:
            ch.insert_tsv(
                "contents",
                [
                    "content_id",
                    "content_hash",
                    "blob_key",
                    "pack_object_key",
                    "pack_offset",
                    "pack_length",
                    "byte_size",
                ],
                contents_rows,
                retries=args.max_retries,
                retry_sleep=args.retry_sleep,
            )
            contents_rows.clear()

        if file_instance_rows:
            ch.insert_tsv(
                "file_instances",
                ["version_num", "path_id", "content_id", "updated_at"],
                file_instance_rows,
                retries=args.max_retries,
                retry_sleep=args.retry_sleep,
            )
            file_instance_rows.clear()

        if symbols_rows:
            ch.insert_tsv(
                "symbols",
                [
                    "content_id",
                    "owner_kind",
                    "owner_name",
                    "symbol_type",
                    "symbol_key",
                    "line_no",
                ],
                symbols_rows,
                retries=args.max_retries,
                retry_sleep=args.retry_sleep,
            )
            symbols_rows.clear()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending_files = files[start_index:]
        chunk_size = max(1, args.batch_size)
        for chunk_base in range(0, len(pending_files), chunk_size):
            chunk_files = pending_files[chunk_base : chunk_base + chunk_size]
            futures = [pool.submit(parse_file_task, file_path) for file_path in chunk_files]

            for chunk_index, (current_file, future) in enumerate(zip(chunk_files, futures)):
                offset = start_index + chunk_base + chunk_index
                try:
                    text_md5, byte_size, parsed_symbols, raw_bytes = future.result()
                except Exception as exc:
                    if not args.continue_on_error:
                        raise
                    print(f"[error] parse failed: {current_file} reason={exc}", file=sys.stderr)
                    bundle_state["next_index"] = offset + 1
                    save_state(args.state_file, state)
                    continue

                absolute_path = to_absolute_path(current_file.relative_to(bundle_root).as_posix())
                path_id = content_id_for("path", absolute_path)
                content_id = content_id_for(version.version_id, absolute_path)
                blob_key = f"{version.bundle_name}{absolute_path}"
                object_name = f"{args.minio_prefix.strip('/')}/{blob_key.lstrip('/')}".lstrip("/")
                if uploader is not None and packer is not None:
                    pack_object_key, pack_offset, pack_length = packer.add(object_name, raw_bytes)
                else:
                    pack_object_key = object_name
                    pack_offset = 0
                    pack_length = len(raw_bytes)

                contents_rows.append(
                    (
                        content_id,
                        text_md5,
                        blob_key,
                        pack_object_key,
                        pack_offset,
                        pack_length,
                        byte_size,
                    )
                )
                file_instance_rows.append(
                    (
                        version.version_num,
                        path_id,
                        content_id,
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )

                for symbol in parsed_symbols:
                    if symbol.symbol_type not in SYMBOL_TYPES:
                        continue
                    symbols_rows.append(
                        (
                            content_id,
                            symbol.owner_kind,
                            symbol.owner_name,
                            symbol.symbol_type,
                            symbol.symbol_key,
                            symbol.line_no,
                        )
                    )

                total_files += 1
                total_symbols += len(parsed_symbols)

                if args.progress_every > 0 and total_files % args.progress_every == 0:
                    _print_progress(version.bundle_name, total_files, total_pending, bundle_start_ts)

                if len(contents_rows) >= args.batch_size:
                    flush_batches()
                    bundle_state["next_index"] = offset + 1
                    save_state(args.state_file, state)

    flush_batches()
    bundle_state["next_index"] = len(files)
    bundle_state["done"] = True
    save_state(args.state_file, state)

    _print_progress(version.bundle_name, total_files, total_pending, bundle_start_ts)
    print(f"[bundle-done] {version.bundle_name}: files={total_files} symbols={total_symbols}")

    return (total_files, total_symbols)


def _existing_versions(ch: ClickHouseClient, args: argparse.Namespace) -> set[tuple[int, str]]:
    rows = ch.execute(
        "SELECT version_num, version_id FROM versions FORMAT TSV",
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    ).splitlines()
    result: set[tuple[int, str]] = set()
    for row in rows:
        if not row:
            continue
        parts = row.split("\t")
        if len(parts) != 2:
            continue
        result.add((int(parts[0]), parts[1]))
    return result


def _existing_path_ids(ch: ClickHouseClient, path_ids: list[int], args: argparse.Namespace) -> set[int]:
    if not path_ids:
        return set()

    existing: set[int] = set()
    query_chunk_size = 500
    for offset in range(0, len(path_ids), query_chunk_size):
        chunk = path_ids[offset : offset + query_chunk_size]
        in_clause = ", ".join(str(item) for item in chunk)
        sql = f"SELECT path_id FROM paths WHERE path_id IN ({in_clause}) FORMAT TSV"
        rows = ch.execute(
            sql,
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        ).splitlines()
        for row in rows:
            text = row.strip()
            if text:
                existing.add(int(text))
    return existing


def _flush_new_paths(
    ch: ClickHouseClient,
    path_rows: list[tuple[Any, ...]],
    args: argparse.Namespace,
) -> int:
    if not path_rows:
        return 0

    deduped_by_path: dict[int, tuple[Any, ...]] = {}
    for row in path_rows:
        deduped_by_path[int(row[0])] = row

    deduped_rows = list(deduped_by_path.values())
    existing = _existing_path_ids(ch, [int(row[0]) for row in deduped_rows], args)
    new_rows = [row for row in deduped_rows if int(row[0]) not in existing]

    if new_rows:
        ch.insert_tsv(
            "paths",
            ["path_id", "absolute_path", "created_at"],
            new_rows,
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
    return len(new_rows)


def ensure_versions_and_paths(
    ch: ClickHouseClient,
    versions: list[VersionInfo],
    headers_root: Path,
    args: argparse.Namespace,
) -> None:
    scan_start_ts = time.time()
    total_headers = 0
    for version in versions:
        bundle_root = headers_root / version.bundle_name
        headers = sorted(bundle_root.rglob("*.h"))
        if args.max_files > 0:
            headers = headers[: args.max_files]
        total_headers += len(headers)

    scanned = 0
    existing_versions = _existing_versions(ch, args)
    version_rows = [
        (
            item.version_num,
            item.version_id,
            item.ios_version,
            item.build,
            item.bundle_name,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        )
        for item in versions
        if (item.version_num, item.version_id) not in existing_versions
    ]
    if version_rows:
        ch.insert_tsv(
            "versions",
            [
                "version_num",
                "version_id",
                "ios_version",
                "build",
                "bundle_name",
                "created_at",
            ],
            version_rows,
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )

    path_rows: list[tuple[Any, ...]] = []
    inserted_paths = 0
    for version in versions:
        bundle_root = headers_root / version.bundle_name
        files = sorted(bundle_root.rglob("*.h"))
        if args.max_files > 0:
            files = files[: args.max_files]
        for header_file in files:
            absolute_path = to_absolute_path(header_file.relative_to(bundle_root).as_posix())
            path_id = content_id_for("path", absolute_path)
            path_rows.append((path_id, absolute_path, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")))
            scanned += 1

            if args.progress_every > 0 and scanned % args.progress_every == 0:
                _print_progress("paths-scan", scanned, total_headers, scan_start_ts)

            if len(path_rows) >= max(5000, args.batch_size * 5):
                inserted_paths += _flush_new_paths(ch, path_rows, args)
                path_rows.clear()

    if path_rows:
        inserted_paths += _flush_new_paths(ch, path_rows, args)

    if total_headers > 0:
        _print_progress("paths-scan", scanned, total_headers, scan_start_ts)
    print(f"[setup] new versions inserted={len(version_rows)} new paths inserted={inserted_paths}")


def truncate_all(ch: ClickHouseClient, args: argparse.Namespace) -> None:
    for table in [
        "symbol_presence",
        "symbols",
        "file_instances",
        "contents",
        "paths",
        "versions",
    ]:
        ch.execute(
            f"TRUNCATE TABLE {table}",
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )


def main() -> None:
    args = parse_args()

    if not args.headers_root.exists() or not args.headers_root.is_dir():
        raise SystemExit(f"Invalid headers root: {args.headers_root}")
    if not args.files_root.exists() or not args.files_root.is_dir():
        raise SystemExit(f"Invalid files root: {args.files_root}")

    ch = ClickHouseClient(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        database=args.clickhouse_db,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )

    ch.execute("SELECT 1", retries=args.max_retries, retry_sleep=args.retry_sleep)

    raw_versions = build_versions(args.headers_root, args.files_root, args.bundle)
    if not raw_versions:
        raise SystemExit("No bundles found to import")

    existing_map = _existing_version_map(ch, args)
    versions = assign_version_numbers(
        raw_versions,
        existing_map,
        allow_old_versions=args.allow_old_versions,
    )

    state = load_state(args.state_file)

    uploader: MinioUploader | None = None
    packer: PackedMinioWriter | None = None
    if args.skip_minio_upload:
        print("[setup] skip MinIO upload enabled")
    else:
        uploader = MinioUploader(
            endpoint=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            bucket=args.minio_bucket,
            secure=args.minio_secure,
        )
        print(f"[setup] MinIO uploader ready: bucket={args.minio_bucket} endpoint={args.minio_endpoint}")
        packer = PackedMinioWriter(
            uploader,
            minio_prefix=args.minio_prefix,
            shards=args.pack_shards,
            target_bytes=args.pack_target_bytes,
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
        print(
            f"[setup] packed object mode enabled: shards={args.pack_shards} target_bytes={args.pack_target_bytes}"
        )

    if args.truncate_all and args.resume:
        raise SystemExit("--truncate-all and --resume cannot be used together")

    if args.truncate_all:
        print("[setup] truncating all tables")
        truncate_all(ch, args)

    if args.resume:
        print("[setup] resume mode: skip versions/paths refresh")
    else:
        print("[setup] importing versions and paths")
        ensure_versions_and_paths(ch, versions, args.headers_root, args)

    total_files = 0
    total_symbols = 0
    start = time.time()

    for version in versions:
        files_count, symbols_count = import_bundle(
            ch,
            uploader,
            packer,
            version,
            args.headers_root,
            state,
            args,
        )
        total_files += files_count
        total_symbols += symbols_count

    duration = time.time() - start
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_versions": len(versions),
        "imported_files": total_files,
        "imported_symbols": total_symbols,
        "duration_sec": round(duration, 2),
        "mode": "no-dedup",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
