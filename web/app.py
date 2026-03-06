from __future__ import annotations

import hashlib
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
cache = RedisCache(settings)
repo = Repository(ClickHouseClient(settings), cache=cache)
store = MinioStore(settings)
search_service = SearchService(repo)


@app.get("/healthz")
def healthz() -> Response:
    return Response("ok", mimetype="text/plain")


@app.get("/")
def search_page() -> str:
    query = request.args.get("q", "").strip()
    selected_dir_name = request.args.get("dir", "").strip()

    cache_key = _search_cache_key(query=query, selected_dir=selected_dir_name)
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
    )

    cache.set_text(cache_key, html, settings.search_cache_ttl_seconds)
    return html


@app.get("/open-latest")
def open_latest():
    absolute_path = _normalize_absolute_path(request.args.get("path", ""))
    if not absolute_path:
        abort(404)

    result = repo.resolve_latest_for_path(absolute_path)
    if result is None:
        abort(404)

    return redirect(url_for("view_header", version_id=result.version_id, absolute_path=result.absolute_path.lstrip("/")))


@app.get("/v/<version_id>/<path:absolute_path>")
def view_header(version_id: str, absolute_path: str) -> str:
    query_started_at = time.perf_counter()

    decoded_version_id = unquote(version_id)
    normalized_path = _normalize_absolute_path(absolute_path)
    if not normalized_path:
        abort(404)

    version_num = repo.get_version_num(decoded_version_id)
    if version_num is None:
        abort(404)

    content_ref = repo.get_file_content_ref(version_num=version_num, absolute_path=normalized_path)
    if content_ref is None:
        abort(404)

    cache_key = _view_cache_key(version_num=content_ref.version_num, path_id=content_ref.path_id)
    cached_html = cache.get_text(cache_key)
    if cached_html is not None:
        return cached_html

    model = _build_view_model(content_ref)
    query_elapsed_ms = int((time.perf_counter() - query_started_at) * 1000)

    html = render_template(
        "view.html",
        version_id=model.ref.version_id,
        absolute_path=model.ref.absolute_path,
        versions=model.versions,
        rendered_source_html=model.rendered_source_html,
        line_count=len(model.source_text.splitlines()),
        availability_rows=model.availability_rows,
        query_elapsed_ms=query_elapsed_ms,
    )
    cache.set_text(cache_key, html, settings.view_cache_ttl_seconds)
    return html


@app.errorhandler(404)
def not_found(_: Exception) -> tuple[str, int]:
    return render_template("not_found.html"), 404


def _build_view_model(content_ref: FileContentRef) -> ViewModel:
    source_bytes = store.read_slice(
        object_key=content_ref.pack_object_key,
        offset=content_ref.pack_offset,
        length=content_ref.pack_length,
    )
    source_text = source_bytes.decode("utf-8", errors="replace")

    versions = repo.list_versions_for_path(content_ref.path_id)
    version_by_num = {version_num: version_id for version_num, version_id in versions}

    symbols = repo.list_symbols_for_content(content_ref.content_id)
    presence_map = repo.get_symbol_presence_map(content_ref.path_id)

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

    same_directory = os.path.dirname(content_ref.absolute_path)
    same_directory_files = repo.list_paths_in_directory(content_ref.version_num, same_directory)
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


def _view_cache_key(version_num: int, path_id: int) -> str:
    return f"html:view:vnum:{version_num}:pid:{path_id}"


def _search_cache_key(query: str, selected_dir: str) -> str:
    payload = f"q={query}|dir={selected_dir}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"html:search:{digest}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
