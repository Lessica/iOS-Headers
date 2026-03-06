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
from web.services.search import SearchService


@dataclass(frozen=True)
class ViewModel:
    ref: FileContentRef
    source_text: str
    rendered_source_html: str
    versions: list[tuple[int, str]]
    availability_rows: list[dict[str, Any]]


settings = load_settings()
app = Flask(__name__)
app.logger.setLevel(logging.INFO)
timing_logger = logging.getLogger("gunicorn.error")
cache = RedisCache(settings)
repo = Repository(ClickHouseClient(settings), cache=cache)
store = MinioStore(settings)
search_service = SearchService(repo)
app.jinja_env.globals["encode_version_id"] = lambda version_id: _encode_version_id_for_url(version_id)


@app.get("/healthz")
def healthz() -> Response:
    return Response("ok", mimetype="text/plain")


@app.get("/")
def search_page() -> str:
    query = request.args.get("q", "").strip()
    selected_dir_name = request.args.get("dir", "").strip()

    return _render_search_page(query=query, selected_dir_name=selected_dir_name)


@app.get("/d/<path:directory_name>")
def directory_page(directory_name: str) -> str:
    selected_dir_name = unquote(directory_name).strip()
    if not selected_dir_name:
        abort(404)

    return _render_search_page(query="", selected_dir_name=selected_dir_name)


def _render_search_page(query: str, selected_dir_name: str) -> str:
    cache_key = _search_cache_key(query=query, selected_dir=selected_dir_name)
    if settings.enable_redis_page_cache:
        cached_html = cache.get_text(cache_key)
        if cached_html is not None:
            return cached_html

    query_started_at = time.perf_counter()

    search_result = search_service.search(query)

    latest = repo.get_latest_version()
    latest_version_num = latest[0] if latest else None

    directory_files: list[FileRef] = []
    if selected_dir_name and latest_version_num is not None:
        directory_files = repo.list_files_in_directory_name(latest_version_num, selected_dir_name)

    query_elapsed_ms = int((time.perf_counter() - query_started_at) * 1000)

    html = render_template(
        "search.html",
        query=query,
        selected_dir_name=selected_dir_name,
        directory_hits=search_result.directory_hits,
        owner_hits=search_result.owner_hits,
        directory_files=directory_files,
        latest_version_num=latest_version_num,
        query_elapsed_ms=query_elapsed_ms,
        show_query_elapsed_ms=settings.show_query_elapsed_ms,
    )

    if settings.enable_redis_page_cache:
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

    cache_key = _view_cache_key(version_num=content_ref.version_num, path_id=content_ref.path_id)
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
        versions=model.versions,
        rendered_source_html=model.rendered_source_html,
        line_count=len(model.source_text.splitlines()),
        availability_rows=model.availability_rows,
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
    symbols: list[tuple[str, str, str]],
    presence_map: dict[tuple[str, str, str], set[int]],
    same_directory_files: set[str],
) -> ViewModel:
    version_by_num = {version_num: version_id for version_num, version_id in versions}

    availability_rows: list[dict[str, Any]] = []
    for owner_name, symbol_type, symbol_key in symbols:
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

    rendered = render_header_with_import_links(
        source_text=source_text,
        version_id=content_ref.version_id,
        current_absolute_path=content_ref.absolute_path,
        directory_files=same_directory_files,
    )

    return ViewModel(
        ref=content_ref,
        source_text=source_text,
        rendered_source_html=rendered.html,
        versions=versions,
        availability_rows=availability_rows,
    )


def _normalize_absolute_path(raw_path: str) -> str:
    candidate = unquote(raw_path).strip()
    if not candidate:
        return ""
    if not candidate.startswith("/"):
        candidate = f"/{candidate}"
    return candidate


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


def _view_cache_key(version_num: int, path_id: int) -> str:
    return f"html:view:vnum:{version_num}:pid:{path_id}"


def _search_cache_key(query: str, selected_dir: str) -> str:
    payload = f"q={query}|dir={selected_dir}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"html:search:{digest}"


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
