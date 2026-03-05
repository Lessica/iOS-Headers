#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import plistlib
import re
import sqlite3
import sys
from typing import Any
from xml.parsers.expat import ExpatError


SYMBOL_TYPES = {
    "ivar",
    "property",
    "class_method",
    "instance_method",
}


@dataclass(frozen=True)
class VersionInfo:
    version_id: str
    ios_version: str
    build: str
    label: str
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build SQLite index for iOS headers cross-version search and diff."
    )
    parser.add_argument(
        "--headers-root",
        type=Path,
        default=Path("headers"),
        help="Root directory containing class-dumped headers by firmware bundle",
    )
    parser.add_argument(
        "--files-root",
        type=Path,
        default=Path("files"),
        help="Root directory containing extracted firmware metadata",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("sites/ios-headers/data"),
        help="Output directory for generated index and metadata",
    )
    parser.add_argument(
        "--bundle",
        action="append",
        default=[],
        help="Limit processing to one or multiple bundle names",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue when encountering unreadable/broken files",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Drop existing index content and rebuild from scratch",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Process up to N .h files per bundle (0 means no limit)",
    )
    return parser.parse_args()


def to_absolute_path(path_text: str) -> str:
    normalized = path_text.strip()
    return "/" + normalized.lstrip("/")


def parse_version_tuple(version: str) -> tuple[int, ...]:
    tokens: list[int] = []
    for token in version.split("."):
        if token.isdigit():
            tokens.append(int(token))
        else:
            tokens.append(-1)
    return tuple(tokens)


def is_visible_ascii(text: str) -> bool:
    if not text:
        return False
    return all(33 <= ord(ch) <= 126 for ch in text)


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
    fallback_version = "unknown"

    metadata_dir = files_root / bundle_name
    metadata = None

    system_version_plist = metadata_dir / "SystemVersion.plist"
    restore_plist = metadata_dir / "Restore.plist"

    if system_version_plist.exists():
        metadata = try_read_plist(system_version_plist, issues)
    if metadata is None and restore_plist.exists():
        metadata = try_read_plist(restore_plist, issues)

    ios_version = fallback_version
    build = fallback_build

    if metadata is not None:
        ios_version = str(metadata.get("ProductVersion", fallback_version))
        build = str(metadata.get("ProductBuildVersion", fallback_build))

    version_id = f"{ios_version}|{build}"
    label = f"iOS {ios_version} ({build})"
    return VersionInfo(
        version_id=version_id,
        ios_version=ios_version,
        build=build,
        label=label,
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


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;

        CREATE TABLE IF NOT EXISTS versions (
            version_id TEXT PRIMARY KEY,
            ios_version TEXT NOT NULL,
            build TEXT NOT NULL,
            label TEXT NOT NULL,
            bundle_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paths (
            path_id INTEGER PRIMARY KEY,
            absolute_path TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY,
            path_id INTEGER NOT NULL,
            version_id TEXT NOT NULL,
            FOREIGN KEY(path_id) REFERENCES paths(path_id),
            FOREIGN KEY(version_id) REFERENCES versions(version_id),
            UNIQUE(path_id, version_id)
        );

        CREATE TABLE IF NOT EXISTS symbols (
            symbol_id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            owner_kind TEXT NOT NULL,
            owner_name TEXT NOT NULL,
            symbol_type TEXT NOT NULL,
            symbol_key TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        );

        CREATE INDEX IF NOT EXISTS idx_files_path_version ON files(path_id, version_id);
        CREATE INDEX IF NOT EXISTS idx_files_version ON files(version_id);
        CREATE INDEX IF NOT EXISTS idx_paths_absolute ON paths(absolute_path);
        CREATE INDEX IF NOT EXISTS idx_symbols_lookup
        ON symbols(symbol_type, symbol_key, owner_name, file_id);
        CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id);
        """
    )


def write_json(path: Path, data: Any, *, pretty: bool = False) -> None:
    text = (
        json.dumps(data, ensure_ascii=False, indent=2)
        if pretty
        else json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    )
    path.write_text(text, encoding="utf-8")


def build_index(args: argparse.Namespace) -> None:
    headers_root: Path = args.headers_root
    files_root: Path = args.files_root
    output_dir: Path = args.output_dir

    if not headers_root.exists() or not headers_root.is_dir():
        raise SystemExit(f"Invalid headers root: {headers_root}")

    output_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = output_dir / "headers_index.sqlite"
    metadata_path = output_dir / "metadata.json"

    issues: list[Issue] = []
    selected_bundles = set(args.bundle)
    bundle_dirs = sorted([item for item in headers_root.iterdir() if item.is_dir()])
    if selected_bundles:
        bundle_dirs = [item for item in bundle_dirs if item.name in selected_bundles]

    versions: list[VersionInfo] = []
    for bundle_dir in bundle_dirs:
        versions.append(build_version_info(bundle_dir.name, files_root, issues))

    versions.sort(
        key=lambda item: (parse_version_tuple(item.ios_version), item.build),
        reverse=True,
    )

    conn = sqlite3.connect(sqlite_path)
    try:
        create_schema(conn)

        cur = conn.cursor()

        if args.full_rebuild:
            cur.execute("DELETE FROM symbols")
            cur.execute("DELETE FROM files")
            cur.execute("DELETE FROM paths")
            cur.execute("DELETE FROM versions")
            conn.commit()

        for version in versions:
            cur.execute(
                """
                DELETE FROM symbols
                WHERE file_id IN (
                    SELECT file_id FROM files WHERE version_id = ?
                )
                """,
                (version.version_id,),
            )
            cur.execute("DELETE FROM files WHERE version_id = ?", (version.version_id,))
            cur.execute("DELETE FROM versions WHERE version_id = ?", (version.version_id,))
            cur.execute(
                """
                INSERT INTO versions(version_id, ios_version, build, label, bundle_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    version.version_id,
                    version.ios_version,
                    version.build,
                    version.label,
                    version.bundle_name,
                ),
            )
        cur.execute(
            """
            DELETE FROM paths
            WHERE path_id NOT IN (
                SELECT DISTINCT path_id FROM files
            )
            """
        )
        conn.commit()

        files_count = 0
        symbols_count = 0
        path_cache: dict[str, int] = {
            str(row[0]): int(row[1])
            for row in cur.execute("SELECT absolute_path, path_id FROM paths")
        }

        for version in versions:
            bundle_root = headers_root / version.bundle_name
            header_files = sorted(bundle_root.rglob("*.h"))
            if args.max_files > 0:
                header_files = header_files[: args.max_files]

            for header_file in header_files:
                absolute_path = to_absolute_path(header_file.relative_to(bundle_root).as_posix())
                if absolute_path not in path_cache:
                    cur.execute("INSERT INTO paths(absolute_path) VALUES (?)", (absolute_path,))
                    inserted_path_id = cur.lastrowid
                    if inserted_path_id is None:
                        raise RuntimeError(f"Failed to insert path row: {absolute_path}")
                    path_cache[absolute_path] = int(inserted_path_id)

                path_id = path_cache[absolute_path]

                cur.execute(
                    """
                    INSERT INTO files(path_id, version_id)
                    VALUES (?, ?)
                    """,
                    (path_id, version.version_id),
                )
                inserted_file_id = cur.lastrowid
                if inserted_file_id is None:
                    raise RuntimeError(
                        f"Failed to insert file row: {absolute_path} @ {version.version_id}"
                    )
                file_id = int(inserted_file_id)
                files_count += 1

                try:
                    parsed_symbols = parse_header_symbols(header_file)
                except OSError as exc:
                    issues.append(Issue(path=header_file, reason=str(exc)))
                    if not args.continue_on_error:
                        raise
                    parsed_symbols = []

                valid_symbols: list[ParsedSymbol] = []
                if parsed_symbols:
                    for symbol in parsed_symbols:
                        if not is_visible_ascii(symbol.owner_name) or not is_visible_ascii(
                            symbol.symbol_key
                        ):
                            issue = Issue(
                                path=header_file,
                                reason=(
                                    "Invalid symbol text (owner_name/symbol_key must be visible ASCII): "
                                    f"owner_name={symbol.owner_name!r}, symbol_key={symbol.symbol_key!r}"
                                ),
                            )
                            issues.append(issue)
                            if not args.continue_on_error:
                                raise SystemExit(
                                    "Found invalid symbol. Re-run with --continue-on-error to skip invalid symbols. "
                                    f"File: {header_file}"
                                )
                            continue
                        valid_symbols.append(symbol)

                if valid_symbols:
                    cur.executemany(
                        """
                        INSERT INTO symbols(file_id, owner_kind, owner_name, symbol_type, symbol_key, line_no)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                file_id,
                                symbol.owner_kind,
                                symbol.owner_name,
                                symbol.symbol_type,
                                symbol.symbol_key,
                                symbol.line_no,
                            )
                            for symbol in valid_symbols
                        ],
                    )
                    symbols_count += len(valid_symbols)

            conn.commit()

        metadata = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "total_versions": len(versions),
            "total_paths": cur.execute("SELECT COUNT(*) FROM paths").fetchone()[0],
            "total_files": files_count,
            "total_symbols": symbols_count,
            "symbol_types": sorted(SYMBOL_TYPES),
            "sqlite_file": sqlite_path.name,
            "issues_count": len(issues),
        }

        write_json(metadata_path, metadata, pretty=True)

        if issues:
            for issue in issues:
                print(f"[issue] {issue.path}: {issue.reason}", file=sys.stderr)
            if not args.continue_on_error:
                raise SystemExit(
                    "Found problematic files. Re-run with --continue-on-error to keep valid subset."
                )

        print(f"Generated SQLite index: {sqlite_path}")
        print(f"Versions: {metadata['total_versions']}")
        print(f"Paths: {metadata['total_paths']}")
        print(f"Files: {metadata['total_files']}")
        print(f"Symbols: {metadata['total_symbols']}")
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    build_index(args)


if __name__ == "__main__":
    main()
