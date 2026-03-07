from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote

from flask import Flask, Response, abort, redirect, render_template, request, url_for

from web.config import load_settings
from web.data.cache import RedisCache
from web.data.ch_client import ClickHouseClient
from web.data.minio_store import MinioStore
from web.data.repository import FileContentRef, FileRef, Repository
from web.services.import_links import render_header_with_import_links
from web.services.search import DIRECTORY_HITS_LIMIT, OWNER_HITS_LIMIT, SearchService


@dataclass(frozen=True)
class ViewModel:
    ref: FileContentRef
    source_text: str
    rendered_source_html: str
    versions: list[tuple[int, str]]
    availability_rows: list[dict[str, Any]]
    source_line_availability: dict[int, list[str]]


settings = load_settings()
app = Flask(__name__)
app.logger.setLevel(logging.INFO)
timing_logger = logging.getLogger("gunicorn.error")
cache = RedisCache(settings)
repo = Repository(ClickHouseClient(settings), cache=cache)
store = MinioStore(settings)
search_service = SearchService(repo)
app.jinja_env.globals["encode_version_id"] = lambda version_id: _encode_version_id_for_url(version_id)
app.jinja_env.globals["format_version_id"] = lambda version_id: _format_version_id_for_display(version_id)
OWNER_VERSIONS_PILL_LIMIT = 15
DEFAULT_DIRECTORY_PAGE_SIZE = 100
MAX_DIRECTORY_PAGE_SIZE = 1000
SYMBOL_TYPE_PRIORITY = {
    "ivar": 0,
    "property": 1,
    "class method": 2,
    "instance method": 3,
}
SOURCE_HOVER_SYMBOL_TYPES = {
    "ivar",
    "property",
    "class method",
    "instance method",
    "class_method",
    "instance_method",
}


@app.get("/healthz")
def healthz() -> Response:
    return Response("ok", mimetype="text/plain")


@app.get("/")
def search_page() -> str:
    raw_query = request.args.get("q", "")
    raw_selected_dir_name = request.args.get("dir", "")
    raw_directory_cursor = request.args.get("dcursor", "")
    raw_directory_direction = request.args.get("ddir", "")
    raw_directory_page_size = request.args.get("dsize", "")

    query = raw_query.strip()
    selected_dir_name = raw_selected_dir_name.strip()
    directory_cursor = raw_directory_cursor.strip() or None
    directory_direction = _normalize_directory_direction(raw_directory_direction)
    directory_page_size = _parse_directory_page_size(raw_directory_page_size)
    has_effective_args = _has_effective_search_args(
        raw_query=raw_query,
        raw_selected_dir_name=raw_selected_dir_name,
        raw_directory_cursor=raw_directory_cursor,
        raw_directory_direction=raw_directory_direction,
        raw_directory_page_size=raw_directory_page_size,
    )

    return _render_search_page(
        query=query,
        selected_dir_name=selected_dir_name,
        directory_cursor=directory_cursor,
        directory_direction=directory_direction,
        directory_page_size=directory_page_size,
        has_effective_args=has_effective_args,
    )


@app.get("/d/<path:directory_name>")
def directory_page(directory_name: str) -> str:
    selected_dir_name = unquote(directory_name).strip()
    if not selected_dir_name:
        abort(404)

    raw_directory_cursor = request.args.get("dcursor", "")
    raw_directory_direction = request.args.get("ddir", "")
    raw_directory_page_size = request.args.get("dsize", "")

    directory_cursor = raw_directory_cursor.strip() or None
    directory_direction = _normalize_directory_direction(raw_directory_direction)
    directory_page_size = _parse_directory_page_size(raw_directory_page_size)
    has_effective_args = _has_effective_search_args(
        raw_query="",
        raw_selected_dir_name="",
        raw_directory_cursor=raw_directory_cursor,
        raw_directory_direction=raw_directory_direction,
        raw_directory_page_size=raw_directory_page_size,
    )

    return _render_search_page(
        query="",
        selected_dir_name=selected_dir_name,
        directory_cursor=directory_cursor,
        directory_direction=directory_direction,
        directory_page_size=directory_page_size,
        has_effective_args=has_effective_args,
    )


def _render_search_page(
    query: str,
    selected_dir_name: str,
    directory_cursor: str | None = None,
    directory_direction: str = "next",
    directory_page_size: int = DEFAULT_DIRECTORY_PAGE_SIZE,
    has_effective_args: bool = False,
) -> str:
    cache_key = _search_cache_key(
        query=query,
        selected_dir=selected_dir_name,
        directory_cursor=directory_cursor,
        directory_direction=directory_direction,
        directory_page_size=directory_page_size,
    )
    use_redis_cache = settings.enable_redis_page_cache and not has_effective_args
    if use_redis_cache:
        cached_html = cache.get_text(cache_key)
        if cached_html is not None:
            return cached_html

    query_started_at = time.perf_counter()

    search_result = search_service.search(query)

    latest = repo.get_latest_version()
    latest_version_num = latest[0] if latest else None
    latest_version_id = latest[1] if latest else None

    directory_files: list[FileRef] = []
    directory_total_unique_paths_count: int | None = None
    directory_total_count: int | None = None
    owner_total_count: int | None = None
    directory_has_next_page = False
    directory_has_prev_page = False
    directory_next_cursor: str | None = None
    directory_prev_cursor: str | None = None
    if selected_dir_name and latest_version_num is not None:
        directory_total_count = repo.count_distinct_directories()
        owner_total_count = repo.count_distinct_owners()
        directory_total_unique_paths_count = repo.count_unique_paths_in_directory_name(selected_dir_name)
        (
            directory_files,
            directory_has_prev_page,
            directory_has_next_page,
            directory_prev_cursor,
            directory_next_cursor,
        ) = repo.list_files_in_directory_name_page(
            version_num=latest_version_num,
            directory_name=selected_dir_name,
            page_size=directory_page_size,
            cursor=directory_cursor,
            direction=directory_direction,
        )

    owner_path_ids = [path_id for _version_id, _absolute_path, path_id in search_result.owner_hits]
    owner_version_ids_by_path = repo.list_version_ids_for_paths(owner_path_ids)
    owner_entries = [
        _build_owner_search_entry(
            version_id=version_id,
            absolute_path=absolute_path,
            version_ids=owner_version_ids_by_path.get(path_id, []),
        )
        for version_id, absolute_path, path_id in search_result.owner_hits
    ]
    directory_path_ids = [item.path_id for item in directory_files]
    directory_version_ids_by_path = repo.list_version_ids_for_paths(directory_path_ids)
    directory_file_entries = [
        _build_search_file_entry(
            item,
            version_ids=directory_version_ids_by_path.get(item.path_id, []),
        )
        for item in directory_files
    ]

    query_elapsed_ms = int((time.perf_counter() - query_started_at) * 1000)

    html = render_template(
        "search.html",
        query=query,
        selected_dir_name=selected_dir_name,
        directory_hits=search_result.directory_hits,
        owner_entries=owner_entries,
        directory_files=directory_files,
        directory_file_entries=directory_file_entries,
        directory_has_next_page=directory_has_next_page,
        directory_has_prev_page=directory_has_prev_page,
        directory_next_cursor=directory_next_cursor,
        directory_prev_cursor=directory_prev_cursor,
        directory_total_unique_paths_count=directory_total_unique_paths_count,
        directory_total_count=directory_total_count,
        owner_total_count=owner_total_count,
        directory_page_size=directory_page_size,
        directory_direction=directory_direction,
        latest_version_num=latest_version_num,
        latest_version_id=latest_version_id,
        directory_hits_limit=DIRECTORY_HITS_LIMIT,
        owner_hits_limit=OWNER_HITS_LIMIT,
        query_elapsed_ms=query_elapsed_ms,
        show_query_elapsed_ms=settings.show_query_elapsed_ms,
    )

    if use_redis_cache:
        cache.set_text(cache_key, html, settings.search_cache_ttl_seconds)
    return html


@app.get("/v/latest/<path:absolute_path>")
def view_latest_header(absolute_path: str) -> Any:
    normalized_path = _normalize_absolute_path(absolute_path)
    if not normalized_path:
        abort(404)

    result = repo.resolve_latest_for_path(normalized_path)
    if result is None:
        abort(404)

    return redirect(
        url_for(
            "view_header",
            version_id=_encode_version_id_for_url(result.version_id),
            absolute_path=result.absolute_path.lstrip("/"),
        )
    )


@app.get("/v/<version_id>/<path:absolute_path>")
def view_header(version_id: str, absolute_path: str) -> str:
    query_started_at = time.perf_counter()
    segment_started_at = query_started_at
    timings_ms: dict[str, int] = {}

    decoded_version_id = _decode_version_id_from_url(version_id)
    normalized_path = _normalize_absolute_path(absolute_path)
    if not normalized_path:
        abort(404)
    timings_ms["decode_and_normalize"] = int((time.perf_counter() - segment_started_at) * 1000)

    segment_started_at = time.perf_counter()
    version_num = repo.get_version_num(decoded_version_id)
    if version_num is None:
        abort(404)
    timings_ms["resolve_version_num"] = int((time.perf_counter() - segment_started_at) * 1000)

    segment_started_at = time.perf_counter()
    content_ref = repo.get_file_content_ref(version_num=version_num, absolute_path=normalized_path)
    if content_ref is None:
        abort(404)
    timings_ms["resolve_content_ref"] = int((time.perf_counter() - segment_started_at) * 1000)

    cache_key = _view_cache_key(
        version_num=content_ref.version_num,
        path_id=content_ref.path_id,
        enable_symbol_matrix=settings.enable_symbol_matrix,
    )
    if settings.enable_redis_page_cache:
        segment_started_at = time.perf_counter()
        cached_html = cache.get_text(cache_key)
        timings_ms["view_cache_lookup"] = int((time.perf_counter() - segment_started_at) * 1000)
        if cached_html is not None:
            total_ms = int((time.perf_counter() - query_started_at) * 1000)
            _log_view_timing(
                version_id=decoded_version_id,
                absolute_path=normalized_path,
                total_ms=total_ms,
                cache_hit=True,
                timings_ms=timings_ms,
            )
            return cached_html

    segment_started_at = time.perf_counter()
    source_bytes = store.read_slice(
        object_key=content_ref.pack_object_key,
        offset=content_ref.pack_offset,
        length=content_ref.pack_length,
    )
    source_text = source_bytes.decode("utf-8", errors="replace")
    timings_ms["minio_read_slice"] = int((time.perf_counter() - segment_started_at) * 1000)

    segment_started_at = time.perf_counter()
    versions = repo.list_versions_for_path(content_ref.path_id)
    timings_ms["query_versions_for_path"] = int((time.perf_counter() - segment_started_at) * 1000)

    segment_started_at = time.perf_counter()
    symbols = repo.list_symbols_for_content(content_ref.content_id)
    timings_ms["query_symbols_for_content"] = int((time.perf_counter() - segment_started_at) * 1000)

    segment_started_at = time.perf_counter()
    presence_map = repo.get_symbol_presence_map(content_ref.path_id)
    timings_ms["query_symbol_presence_map"] = int((time.perf_counter() - segment_started_at) * 1000)

    segment_started_at = time.perf_counter()
    same_directory = os.path.dirname(content_ref.absolute_path)
    same_directory_files = repo.list_paths_in_directory(content_ref.version_num, same_directory)
    timings_ms["query_same_directory_paths"] = int((time.perf_counter() - segment_started_at) * 1000)

    segment_started_at = time.perf_counter()
    model = _build_view_model(
        content_ref=content_ref,
        source_text=source_text,
        versions=versions,
        symbols=symbols,
        presence_map=presence_map,
        same_directory_files=same_directory_files,
    )
    timings_ms["build_view_model"] = int((time.perf_counter() - segment_started_at) * 1000)

    query_elapsed_ms = int((time.perf_counter() - query_started_at) * 1000)

    segment_started_at = time.perf_counter()
    html = render_template(
        "view.html",
        version_id=model.ref.version_id,
        absolute_path=model.ref.absolute_path,
        file_name=os.path.basename(model.ref.absolute_path.rstrip("/")) or model.ref.absolute_path,
        view_directory_name=_extract_directory_name(model.ref.absolute_path),
        versions=model.versions,
        rendered_source_html=model.rendered_source_html,
        line_count=len(model.source_text.splitlines()),
        file_size_text=_format_bytes_for_display(model.ref.pack_length),
        enable_symbol_matrix=settings.enable_symbol_matrix,
        availability_rows=model.availability_rows,
        source_line_availability=model.source_line_availability,
        query_elapsed_ms=query_elapsed_ms,
        show_query_elapsed_ms=settings.show_query_elapsed_ms,
    )
    timings_ms["render_template"] = int((time.perf_counter() - segment_started_at) * 1000)

    if settings.enable_redis_page_cache:
        segment_started_at = time.perf_counter()
        cache.set_text(cache_key, html, settings.view_cache_ttl_seconds)
        timings_ms["view_cache_store"] = int((time.perf_counter() - segment_started_at) * 1000)

    total_ms = int((time.perf_counter() - query_started_at) * 1000)
    _log_view_timing(
        version_id=decoded_version_id,
        absolute_path=normalized_path,
        total_ms=total_ms,
        cache_hit=False,
        timings_ms=timings_ms,
    )
    return html


@app.errorhandler(404)
def not_found(_: Exception) -> tuple[str, int]:
    return render_template("not_found.html"), 404


def _build_view_model(
    content_ref: FileContentRef,
    source_text: str,
    versions: list[tuple[int, str]],
    symbols: list[tuple[str, str, str, int]],
    presence_map: dict[tuple[str, str, str], set[int]],
    same_directory_files: set[str],
) -> ViewModel:
    version_by_num = {version_num: version_id for version_num, version_id in versions}
    version_label_by_num = {version_num: _version_label_for_display(version_id) for version_num, version_id in versions}

    availability_rows: list[dict[str, Any]] = []
    line_to_version_nums: dict[int, set[int]] = {}

    for owner_name, symbol_type, symbol_key, line_no in symbols:
        key = (owner_name, symbol_type, symbol_key)
        existing_versions = presence_map.get(key, set())
        states = [
            {
                "version_num": version_num,
                "version_id": version_by_num.get(version_num, str(version_num)),
                "exists": version_num in existing_versions,
            }
            for version_num, _ in versions
        ]
        availability_rows.append(
            {
                "owner_name": owner_name,
                "symbol_type": symbol_type,
                "symbol_key": symbol_key,
                "states": states,
            }
        )

        normalized_symbol_type = symbol_type.strip().lower()
        if line_no > 0 and normalized_symbol_type in SOURCE_HOVER_SYMBOL_TYPES and existing_versions:
            bucket = line_to_version_nums.setdefault(line_no, set())
            bucket.update(existing_versions)

    availability_rows.sort(
        key=lambda row: (
            SYMBOL_TYPE_PRIORITY.get(str(row.get("symbol_type", "")).strip().lower(), len(SYMBOL_TYPE_PRIORITY)),
            str(row.get("owner_name", "")).lower(),
            str(row.get("symbol_key", "")).lower(),
        )
    )

    rendered = render_header_with_import_links(
        source_text=source_text,
        version_id=content_ref.version_id,
        current_absolute_path=content_ref.absolute_path,
        directory_files=same_directory_files,
    )

    source_line_availability: dict[int, list[str]] = {}
    for line_no, version_nums in line_to_version_nums.items():
        labels: list[str] = []
        for version_num, _ in versions:
            if version_num not in version_nums:
                continue
            label = version_label_by_num.get(version_num, "")
            if label and label not in labels:
                labels.append(label)
        if labels:
            source_line_availability[line_no] = labels

    return ViewModel(
        ref=content_ref,
        source_text=source_text,
        rendered_source_html=rendered.html,
        versions=versions,
        availability_rows=availability_rows,
        source_line_availability=source_line_availability,
    )


def _normalize_absolute_path(raw_path: str) -> str:
    candidate = unquote(raw_path).strip()
    if not candidate:
        return ""
    if not candidate.startswith("/"):
        candidate = f"/{candidate}"
    return candidate


def _extract_directory_name(absolute_path: str) -> str | None:
    normalized = absolute_path.rstrip("/")
    parent_path = os.path.dirname(normalized)
    if not parent_path or parent_path == "/":
        return None

    directory_name = os.path.basename(parent_path.rstrip("/"))
    return directory_name or None


def _build_search_file_entry(file_ref: FileRef, version_ids: list[str] | None = None) -> dict[str, Any]:
    absolute_path = file_ref.absolute_path
    normalized = absolute_path.rstrip("/")
    file_name = os.path.basename(normalized) or absolute_path
    parent_path = os.path.dirname(normalized) or "/"
    parent_name = os.path.basename(parent_path.rstrip("/")) or "/"
    segments = [segment for segment in normalized.split("/") if segment]
    path_depth = len(segments)
    all_version_ids = version_ids or []
    visible_version_ids = all_version_ids[:OWNER_VERSIONS_PILL_LIMIT]
    remaining_versions_count = max(len(all_version_ids) - len(visible_version_ids), 0)

    return {
        "version_num": file_ref.version_num,
        "version_id": file_ref.version_id,
        "absolute_path": absolute_path,
        "file_name": file_name,
        "parent_name": parent_name,
        "parent_path": parent_path,
        "framework_name": _extract_framework_name(segments),
        "path_depth": path_depth,
        "file_size_bytes": file_ref.file_size_bytes,
        "file_size_text": _format_bytes_for_display(file_ref.file_size_bytes),
        "version_ids": visible_version_ids,
        "remaining_versions_count": remaining_versions_count,
    }


def _build_owner_search_entry(version_id: str, absolute_path: str, version_ids: list[str]) -> dict[str, Any]:
    normalized = absolute_path.rstrip("/")
    file_name = os.path.basename(normalized) or absolute_path
    visible_version_ids = version_ids[:OWNER_VERSIONS_PILL_LIMIT]
    remaining_versions_count = max(len(version_ids) - len(visible_version_ids), 0)
    return {
        "version_id": version_id,
        "absolute_path": absolute_path,
        "file_name": file_name,
        "version_ids": visible_version_ids,
        "remaining_versions_count": remaining_versions_count,
    }


def _extract_framework_name(path_segments: list[str]) -> str | None:
    for index, segment in enumerate(path_segments):
        if segment != "Frameworks":
            continue
        if index + 1 >= len(path_segments):
            return None
        framework_segment = path_segments[index + 1]
        if framework_segment.endswith(".framework"):
            return framework_segment[:-10]
        return framework_segment
    return None


def _encode_version_id_for_url(version_id: str) -> str:
    escaped = version_id.replace("_", "__")
    return escaped.replace("|", "_")


def _decode_version_id_from_url(raw_version_id: str) -> str:
    decoded = unquote(raw_version_id)
    chars: list[str] = []
    index = 0
    while index < len(decoded):
        char = decoded[index]
        if char != "_":
            chars.append(char)
            index += 1
            continue

        if index + 1 < len(decoded) and decoded[index + 1] == "_":
            chars.append("_")
            index += 2
            continue

        chars.append("|")
        index += 1

    return "".join(chars)


def _format_version_id_for_display(version_id: str) -> str:
    value = version_id.strip()
    if not value:
        return value
    if "|" not in value:
        return value

    major, build = value.split("|", 1)
    major = major.strip()
    build = build.strip()
    if major and build:
        return f"{major} · {build}"
    return major or build


def _version_label_for_display(version_id: str) -> str:
    value = version_id.strip()
    if not value:
        return value
    if "|" not in value:
        return value
    major, _build = value.split("|", 1)
    return major.strip() or value


def _format_bytes_for_display(size_bytes: int | None) -> str:
    if size_bytes is None:
        return "Size unknown"
    if size_bytes < 1024:
        return f"{size_bytes} B"

    size = float(size_bytes)
    units = ["KB", "MB", "GB", "TB"]
    unit_index = -1
    while size >= 1024 and unit_index + 1 < len(units):
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{size:.0f} {units[unit_index]}"
    return f"{size:.1f} {units[unit_index]}"


def _view_cache_key(version_num: int, path_id: int, enable_symbol_matrix: bool) -> str:
    matrix_flag = "1" if enable_symbol_matrix else "0"
    return f"html:view:vnum:{version_num}:pid:{path_id}:sm:{matrix_flag}"


def _search_cache_key(
    query: str,
    selected_dir: str,
    directory_cursor: str | None,
    directory_direction: str,
    directory_page_size: int,
) -> str:
    payload = (
        f"q={query}|dir={selected_dir}|dcursor={directory_cursor or ''}|"
        f"ddir={directory_direction}|dsize={directory_page_size}"
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"html:search:{digest}"


def _normalize_directory_direction(raw_direction: str) -> str:
    direction = raw_direction.strip().lower()
    if direction == "prev":
        return "prev"
    return "next"


def _parse_directory_page_size(raw_size: str) -> int:
    value = raw_size.strip()
    if not value:
        return DEFAULT_DIRECTORY_PAGE_SIZE
    try:
        size = int(value)
    except ValueError:
        return DEFAULT_DIRECTORY_PAGE_SIZE
    return max(10, min(size, MAX_DIRECTORY_PAGE_SIZE))


def _has_effective_search_args(
    raw_query: str,
    raw_selected_dir_name: str,
    raw_directory_cursor: str,
    raw_directory_direction: str,
    raw_directory_page_size: str,
) -> bool:
    if raw_query.strip():
        return True
    if raw_selected_dir_name.strip():
        return True
    if raw_directory_cursor.strip():
        return True

    direction = raw_directory_direction.strip().lower()
    if direction in {"next", "prev"}:
        return True

    size_value = raw_directory_page_size.strip()
    if not size_value:
        return False
    try:
        int(size_value)
    except ValueError:
        return False
    return True


def _log_view_timing(
    version_id: str,
    absolute_path: str,
    total_ms: int,
    cache_hit: bool,
    timings_ms: dict[str, int],
) -> None:
    segments = " ".join(f"{name}={value}ms" for name, value in timings_ms.items())
    timing_logger.info(
        "view_timing version_id=%s path=%s total=%dms cache_hit=%s %s",
        version_id,
        absolute_path,
        total_ms,
        str(cache_hit).lower(),
        segments,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
