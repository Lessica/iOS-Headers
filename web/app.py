from __future__ import annotations

import difflib
import html
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, unquote

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
    source_line_availability: dict[int, list[str]]


settings = load_settings()
app = Flask(__name__)
app.logger.setLevel(logging.INFO)
timing_logger = logging.getLogger("gunicorn.error")
cache = RedisCache(settings)
repo = Repository(
    ClickHouseClient(settings),
    cache=cache,
    version_cache_ttl_seconds=settings.version_cache_ttl_seconds,
    stats_cache_ttl_seconds=settings.stats_cache_ttl_seconds,
)
store = MinioStore(settings)
search_service = SearchService(repo)
app.jinja_env.globals["encode_version_id"] = lambda version_id: _encode_version_id_for_url(version_id)
app.jinja_env.globals["format_version_id"] = lambda version_id: _format_version_id_for_display(version_id, separator=" · ")
app.jinja_env.globals["format_directory_name"] = lambda directory_name: _format_directory_name_for_display(directory_name)
OWNER_VERSIONS_PILL_LIMIT = 15
DEFAULT_DIRECTORY_PAGE_SIZE = 50
CANONICAL_SITE_ORIGIN = "https://headers.82flex.com"
SITEMAP_CACHE_KEY = "xml:sitemap:v1"
SITEMAP_CACHE_TTL_SECONDS = 1800
SITEMAP_DIRECTORY_FETCH_LIMIT = 100000
SEARCH_SCOPE_NOTICE = (
    "Search supports directory names, framework names, and Objective-C header file names only; "
    "property, ivar, and method search is unavailable."
)
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


@app.get("/sitemap.xml")
def sitemap_xml() -> Response:
    if settings.enable_redis_page_cache:
        cached_xml = cache.get_text(SITEMAP_CACHE_KEY)
        if cached_xml is not None:
            return Response(cached_xml, mimetype="application/xml")

    urls: list[str] = [_canonical_url(url_for("search_page"))]
    directory_hits = repo.search_directories(prefix="", limit=SITEMAP_DIRECTORY_FETCH_LIMIT)
    if len(directory_hits) >= SITEMAP_DIRECTORY_FETCH_LIMIT:
        app.logger.warning(
            "sitemap directory list reached fetch limit=%d; consider raising SITEMAP_DIRECTORY_FETCH_LIMIT",
            SITEMAP_DIRECTORY_FETCH_LIMIT,
        )

    for directory_name, _sample_dir_path in directory_hits:
        urls.append(
            _canonical_url(
                url_for("directory_page", directory_name=directory_name),
            )
        )

    sitemap_xml_text = _build_sitemap_xml(urls)
    if settings.enable_redis_page_cache:
        cache.set_text(SITEMAP_CACHE_KEY, sitemap_xml_text, SITEMAP_CACHE_TTL_SECONDS)
    return Response(sitemap_xml_text, mimetype="application/xml")


@app.context_processor
def inject_seo_metadata() -> dict[str, Any]:
    endpoint = request.endpoint or ""
    args = request.args
    view_args = request.view_args or {}

    query = args.get("q", "").strip()
    selected_dir_name = ""
    if endpoint == "directory_page":
        selected_dir_name = unquote(str(view_args.get("directory_name", ""))).strip()

    has_pagination_cursor = bool(args.get("cursor", "").strip())
    seo_robots = "index, follow"
    if endpoint == "view_header_diff":
        seo_robots = "noindex, nofollow"
    elif endpoint in {"search_page", "directory_page"} and has_pagination_cursor:
        seo_robots = "noindex, follow"

    canonical_query: dict[str, str] = {}
    if query:
        canonical_query["q"] = query

    if endpoint == "search_page":
        canonical_path = url_for("search_page")
    elif endpoint == "directory_page" and selected_dir_name:
        canonical_path = url_for("directory_page", directory_name=selected_dir_name)
    else:
        canonical_path = request.path
    seo_canonical_url = _canonical_url(canonical_path, canonical_query)

    seo_title = "iOS Headers"
    seo_description = (
        "Explore iOS SDK headers across versions with searchable directories and version history. "
        f"{SEARCH_SCOPE_NOTICE}"
    )
    seo_og_type = "website"

    if endpoint == "search_page" and query:
        seo_title = f"Search: {query} · iOS Headers"
        seo_description = (
            f"Search iOS SDK headers for {query} across directories and versions. "
            f"{SEARCH_SCOPE_NOTICE}"
        )
    elif endpoint == "directory_page" and selected_dir_name:
        display_dir_name = _format_directory_name_for_display(selected_dir_name)
        seo_title = f"{display_dir_name} · iOS Headers"
        seo_description = (
            f"Browse headers in {display_dir_name} across indexed iOS SDK versions. "
            f"{SEARCH_SCOPE_NOTICE}"
        )
    elif endpoint == "view_header":
        absolute_path = _normalize_absolute_path(str(view_args.get("absolute_path", "")))
        file_name = os.path.basename(absolute_path.rstrip("/")) or absolute_path or "Header"
        seo_title = f"{file_name} · iOS Headers"
        seo_description = (
            f"View {absolute_path} with version history, source lines, and symbol-aware navigation. "
            f"{SEARCH_SCOPE_NOTICE}"
        )
        seo_og_type = "article"
    elif endpoint == "view_header_diff":
        absolute_path = _normalize_absolute_path(str(view_args.get("absolute_path", "")))
        seo_title = f"{absolute_path} · Compare · iOS Headers"
        seo_description = (
            f"Compare header changes for {absolute_path} across iOS SDK versions. "
            f"{SEARCH_SCOPE_NOTICE}"
        )

    seo_structured_data: list[dict[str, Any]] = [
        {
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": "iOS Headers",
            "url": _canonical_url("/"),
            "description": SEARCH_SCOPE_NOTICE,
            "potentialAction": {
                "@type": "SearchAction",
                "target": f"{_canonical_url('/')}?q={{search_term_string}}",
                "query-input": "required name=search_term_string",
            },
        }
    ]

    if endpoint in {"search_page", "directory_page"}:
        seo_structured_data.append(
            {
                "@context": "https://schema.org",
                "@type": "CollectionPage",
                "name": seo_title,
                "url": seo_canonical_url,
                "description": seo_description,
            }
        )

    if endpoint == "view_header":
        absolute_path = _normalize_absolute_path(str(view_args.get("absolute_path", "")))
        seo_structured_data.append(
            {
                "@context": "https://schema.org",
                "@type": "SoftwareSourceCode",
                "name": os.path.basename(absolute_path.rstrip("/")) or absolute_path,
                "url": seo_canonical_url,
                "programmingLanguage": "Objective-C",
                "description": seo_description,
            }
        )

    return {
        "seo_title": seo_title,
        "seo_description": seo_description,
        "seo_robots": seo_robots,
        "seo_canonical_url": seo_canonical_url,
        "seo_og_type": seo_og_type,
        "seo_og_title": seo_title,
        "seo_og_description": seo_description,
        "seo_og_url": seo_canonical_url,
        "seo_twitter_card": "summary",
        "seo_hreflang": "en",
        "seo_structured_data": seo_structured_data,
    }


@app.get("/")
def search_page() -> str:
    raw_query = request.args.get("q", "")
    raw_selected_dir_name = request.args.get("dir", "")
    raw_directory_cursor = request.args.get("cursor", "")
    raw_directory_direction = request.args.get("direction", "")

    query = raw_query.strip()
    selected_dir_name = raw_selected_dir_name.strip()
    directory_cursor = raw_directory_cursor.strip() or None
    directory_direction = _normalize_directory_direction(raw_directory_direction)
    directory_page_size = DEFAULT_DIRECTORY_PAGE_SIZE
    has_effective_args = _has_effective_search_args(
        raw_query=raw_query,
        raw_selected_dir_name=raw_selected_dir_name,
        raw_directory_cursor=raw_directory_cursor,
        raw_directory_direction=raw_directory_direction,
    )

    return _render_search_page(
        query=query,
        selected_dir_name=selected_dir_name,
        directory_cursor=directory_cursor,
        directory_direction=directory_direction,
        directory_page_size=directory_page_size,
        has_effective_args=has_effective_args,
    )


@app.get("/tree/<path:directory_name>")
def directory_page(directory_name: str) -> str:
    selected_dir_name = unquote(directory_name).strip()
    if not selected_dir_name:
        abort(404)

    raw_query = request.args.get("q", "")
    raw_directory_cursor = request.args.get("cursor", "")
    raw_directory_direction = request.args.get("direction", "")

    query = raw_query.strip()
    directory_cursor = raw_directory_cursor.strip() or None
    directory_direction = _normalize_directory_direction(raw_directory_direction)
    directory_page_size = DEFAULT_DIRECTORY_PAGE_SIZE
    has_effective_args = _has_effective_search_args(
        raw_query=raw_query,
        raw_selected_dir_name=selected_dir_name,
        raw_directory_cursor=raw_directory_cursor,
        raw_directory_direction=raw_directory_direction,
    )

    return _render_search_page(
        query=query,
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
    query_started_at = time.perf_counter()
    cache_key = _search_cache_key(
        query=query,
        selected_dir=selected_dir_name,
        directory_cursor=directory_cursor,
        directory_direction=directory_direction,
        directory_page_size=directory_page_size,
    )
    use_redis_cache = settings.enable_redis_page_cache
    if use_redis_cache:
        cached_html = cache.get_text(cache_key)
        if cached_html is not None:
            query_elapsed_ms = int((time.perf_counter() - query_started_at) * 1000)
            _log_search_timing(
                query=query,
                selected_dir_name=selected_dir_name,
                has_effective_args=has_effective_args,
                query_elapsed_ms=query_elapsed_ms,
                cache_hit=True,
            )
            return cached_html

    should_run_global_search = bool(query and not selected_dir_name)
    if should_run_global_search:
        search_result = search_service.search(query)
    else:
        search_result = search_service.search("")

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
    if selected_dir_name:
        directory_total_count = repo.count_distinct_directories()
        owner_total_count = repo.count_distinct_owners()
        directory_total_unique_paths_count = repo.count_unique_paths_in_directory_name(
            selected_dir_name,
            keyword=query,
        )
        (
            directory_files,
            directory_has_prev_page,
            directory_has_next_page,
            directory_prev_cursor,
            directory_next_cursor,
        ) = repo.list_files_in_directory_name_page(
            directory_name=selected_dir_name,
            page_size=directory_page_size,
            cursor=directory_cursor,
            direction=directory_direction,
            keyword=query,
        )

        if directory_cursor is None:
            selected_dir_leaf_name = selected_dir_name.rsplit("/", 1)[-1]
            umbrella_file_name = f"{selected_dir_leaf_name}-Umbrella.h"
            fallback_file_name = f"{selected_dir_leaf_name}.h"
            umbrella_file_ref = repo.get_file_in_directory_by_preferred_names(
                directory_name=selected_dir_name,
                preferred_names=[umbrella_file_name, fallback_file_name],
            )
            if umbrella_file_ref is not None:
                directory_files = [
                    item for item in directory_files if item.path_id != umbrella_file_ref.path_id
                ]
                directory_files.insert(0, umbrella_file_ref)

                if len(directory_files) > directory_page_size:
                    directory_files.pop()
                    directory_has_next_page = True
                    directory_next_cursor = directory_files[-1].absolute_path

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
        cache_ttl_seconds = settings.search_cache_ttl_seconds
        if query:
            cache_ttl_seconds = min(cache_ttl_seconds, settings.search_query_cache_max_ttl_seconds)
        cache.set_text(cache_key, html, cache_ttl_seconds)

    _log_search_timing(
        query=query,
        selected_dir_name=selected_dir_name,
        has_effective_args=has_effective_args,
        query_elapsed_ms=query_elapsed_ms,
        cache_hit=False,
    )
    return html


@app.get("/view/latest/<path:absolute_path>")
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


@app.get("/raw/latest/<path:absolute_path>")
def raw_latest_header(absolute_path: str) -> Any:
    normalized_path = _normalize_absolute_path(absolute_path)
    if not normalized_path:
        abort(404)

    result = repo.resolve_latest_for_path(normalized_path)
    if result is None:
        abort(404)

    return redirect(
        url_for(
            "raw_header",
            version_id=_encode_version_id_for_url(result.version_id),
            absolute_path=result.absolute_path.lstrip("/"),
        )
    )


@app.get("/diff/<from_version_id>...<to_version_id>/<path:absolute_path>")
def view_header_diff(from_version_id: str, to_version_id: str, absolute_path: str) -> str:
    query_started_at = time.perf_counter()

    decoded_from_version_id = _decode_version_id_from_url(from_version_id)
    decoded_to_version_id = _decode_version_id_from_url(to_version_id)
    normalized_path = _normalize_absolute_path(absolute_path)
    if not normalized_path:
        abort(404)

    from_version_num = repo.get_version_num(decoded_from_version_id)
    to_version_num = repo.get_version_num(decoded_to_version_id)
    if from_version_num is None or to_version_num is None:
        abort(404)

    from_content_ref = repo.get_file_content_ref(version_num=from_version_num, absolute_path=normalized_path)
    to_content_ref = repo.get_file_content_ref(version_num=to_version_num, absolute_path=normalized_path)
    if from_content_ref is None or to_content_ref is None:
        abort(404)

    from_source_bytes = store.read_slice(
        object_key=from_content_ref.pack_object_key,
        offset=from_content_ref.pack_offset,
        length=from_content_ref.pack_length,
    )
    to_source_bytes = store.read_slice(
        object_key=to_content_ref.pack_object_key,
        offset=to_content_ref.pack_offset,
        length=to_content_ref.pack_length,
    )

    from_source_text = from_source_bytes.decode("utf-8", errors="replace")
    to_source_text = to_source_bytes.decode("utf-8", errors="replace")
    versions = repo.list_versions_for_path(to_content_ref.path_id)
    diff_text = _build_unified_diff_text(
        absolute_path=normalized_path,
        from_version_id=from_content_ref.version_id,
        to_version_id=to_content_ref.version_id,
        from_source_text=from_source_text,
        to_source_text=to_source_text,
    )

    query_elapsed_ms = int((time.perf_counter() - query_started_at) * 1000)
    return render_template(
        "diff.html",
        absolute_path=normalized_path,
        file_name=os.path.basename(normalized_path.rstrip("/")) or normalized_path,
        view_directory_name=_extract_directory_name(normalized_path),
        from_version_id=from_content_ref.version_id,
        to_version_id=to_content_ref.version_id,
        versions=versions,
        from_line_count=len(from_source_text.splitlines()),
        to_line_count=len(to_source_text.splitlines()),
        has_changes=bool(diff_text),
        diff_text=diff_text,
        query_elapsed_ms=query_elapsed_ms,
        show_query_elapsed_ms=settings.show_query_elapsed_ms,
    )


@app.get("/view/<version_id>/<path:absolute_path>")
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
    model = _build_view_model(
        content_ref=content_ref,
        source_text=source_text,
        versions=versions,
        symbols=symbols,
        presence_map=presence_map,
    )
    timings_ms["build_view_model"] = int((time.perf_counter() - segment_started_at) * 1000)

    compare_from_version_id = _pick_compare_target_version_id(
        versions=model.versions,
        current_version_id=model.ref.version_id,
    )

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
        compare_from_version_id=compare_from_version_id,
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


@app.get("/raw/<version_id>/<path:absolute_path>")
def raw_header(version_id: str, absolute_path: str) -> Response:
    decoded_version_id = _decode_version_id_from_url(version_id)
    normalized_path = _normalize_absolute_path(absolute_path)
    if not normalized_path:
        abort(404)

    version_num = repo.get_version_num(decoded_version_id)
    if version_num is None:
        abort(404)

    content_ref = repo.get_file_content_ref(version_num=version_num, absolute_path=normalized_path)
    if content_ref is None:
        abort(404)

    source_bytes = store.read_slice(
        object_key=content_ref.pack_object_key,
        offset=content_ref.pack_offset,
        length=content_ref.pack_length,
    )
    source_text = source_bytes.decode("utf-8", errors="replace")
    return Response(source_text, mimetype="text/plain")


@app.errorhandler(404)
def not_found(_: Exception) -> tuple[str, int]:
    return (
        render_template(
            "not_found.html",
            seo_robots="noindex, nofollow",
            seo_canonical_url=_canonical_url(url_for("search_page")),
        ),
        404,
    )


def _build_view_model(
    content_ref: FileContentRef,
    source_text: str,
    versions: list[tuple[int, str]],
    symbols: list[tuple[str, str, str, int]],
    presence_map: dict[tuple[str, str, str], set[int]],
) -> ViewModel:
    version_label_by_num = {version_num: _version_label_for_display(version_id) for version_num, version_id in versions}
    line_to_version_nums: dict[int, set[int]] = {}

    for owner_name, symbol_type, symbol_key, line_no in symbols:
        if line_no <= 0:
            continue
        normalized_symbol_type = symbol_type.strip().lower()
        if normalized_symbol_type not in SOURCE_HOVER_SYMBOL_TYPES:
            continue
        existing_versions = presence_map.get((owner_name, symbol_type, symbol_key), set())
        if not existing_versions:
            continue
        bucket = line_to_version_nums.setdefault(line_no, set())
        bucket.update(existing_versions)

    rendered = render_header_with_import_links(
        source_text=source_text,
        version_id=content_ref.version_id,
        current_absolute_path=content_ref.absolute_path,
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

    path_segments = [segment for segment in parent_path.split("/") if segment]
    if not path_segments:
        return None

    directory_key_segments = path_segments[-2:]
    return "/".join(directory_key_segments)


def _format_directory_name_for_display(directory_name: str) -> str:
    normalized = directory_name.strip()
    if not normalized or "/" not in normalized:
        return normalized

    parent, leaf = normalized.rsplit("/", 1)
    if not leaf:
        return normalized

    parent_prefix = parent.split(".", 1)[0]
    if "." in parent and parent_prefix == leaf:
        return parent
    return leaf


def _pick_compare_target_version_id(
    versions: list[tuple[int, str]],
    current_version_id: str,
) -> str | None:
    if not versions:
        return None

    current_index: int | None = None
    for index, (_version_num, version_id) in enumerate(versions):
        if version_id == current_version_id:
            current_index = index
            break

    if current_index is None:
        for _version_num, version_id in versions:
            if version_id != current_version_id:
                return version_id
        return None

    if current_index + 1 < len(versions):
        return versions[current_index + 1][1]
    if current_index - 1 >= 0:
        return versions[current_index - 1][1]
    return None


def _build_search_file_entry(file_ref: FileRef, version_ids: list[str] | None = None) -> dict[str, Any]:
    absolute_path = file_ref.absolute_path
    normalized = absolute_path.rstrip("/")
    file_name = os.path.basename(normalized) or absolute_path
    parent_path = os.path.dirname(normalized) or "/"
    parent_name = os.path.basename(parent_path.rstrip("/")) or "/"
    segments = [segment for segment in normalized.split("/") if segment]
    path_depth = len(segments)
    all_version_ids = version_ids or []
    effective_version_id = all_version_ids[0] if all_version_ids else file_ref.version_id
    visible_version_ids = all_version_ids[:OWNER_VERSIONS_PILL_LIMIT]
    remaining_versions_count = max(len(all_version_ids) - len(visible_version_ids), 0)

    return {
        "version_num": file_ref.version_num,
        "version_id": effective_version_id,
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
    effective_version_id = version_ids[0] if version_ids else version_id
    visible_version_ids = version_ids[:OWNER_VERSIONS_PILL_LIMIT]
    remaining_versions_count = max(len(version_ids) - len(visible_version_ids), 0)
    return {
        "version_id": effective_version_id,
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


def _format_version_id_for_display(version_id: str, separator: str = "_") -> str:
    value = version_id.strip()
    if not value:
        return value
    if "|" not in value:
        return value

    major, build = value.split("|", 1)
    major = major.strip()
    build = build.strip()
    if major and build:
        return f"{major}{separator}{build}"
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


def _build_unified_diff_text(
    absolute_path: str,
    from_version_id: str,
    to_version_id: str,
    from_source_text: str,
    to_source_text: str,
) -> str:
    from_lines = from_source_text.splitlines()
    to_lines = to_source_text.splitlines()

    from_label = f"a/{_format_version_id_for_display(from_version_id)}{absolute_path}"
    to_label = f"b/{_format_version_id_for_display(to_version_id)}{absolute_path}"
    diff_lines = difflib.unified_diff(
        from_lines,
        to_lines,
        fromfile=from_label,
        tofile=to_label,
        lineterm="",
        n=3,
    )
    return "\n".join(diff_lines)


def _view_cache_key(version_num: int, path_id: int) -> str:
    return f"html:view:vnum:{version_num}:pid:{path_id}"


def _search_cache_key(
    query: str,
    selected_dir: str,
    directory_cursor: str | None,
    directory_direction: str,
    directory_page_size: int,
) -> str:
    payload = (
        f"q={query}|dir={selected_dir}|cursor={directory_cursor or ''}|"
        f"direction={directory_direction}|dsize={directory_page_size}"
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"html:search:{digest}"


def _normalize_directory_direction(raw_direction: str) -> str:
    direction = raw_direction.strip().lower()
    if direction == "prev":
        return "prev"
    return "next"


def _has_effective_search_args(
    raw_query: str,
    raw_selected_dir_name: str,
    raw_directory_cursor: str,
    raw_directory_direction: str,
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
    return False


def _canonical_url(path: str, query_params: dict[str, str] | None = None) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    if not query_params:
        return f"{CANONICAL_SITE_ORIGIN}{normalized_path}"

    filtered = {key: value for key, value in query_params.items() if value}
    if not filtered:
        return f"{CANONICAL_SITE_ORIGIN}{normalized_path}"
    return f"{CANONICAL_SITE_ORIGIN}{normalized_path}?{urlencode(filtered)}"


def _build_sitemap_xml(urls: list[str]) -> str:
    unique_urls = sorted(set(urls))
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for url in unique_urls:
        lines.append("  <url>")
        lines.append(f"    <loc>{html.escape(url, quote=True)}</loc>")
        lines.append("  </url>")
    lines.append("</urlset>")
    return "\n".join(lines)


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


def _log_search_timing(
    query: str,
    selected_dir_name: str,
    has_effective_args: bool,
    query_elapsed_ms: int,
    cache_hit: bool,
) -> None:
    timing_logger.info(
        "search_timing query=%s selected_dir=%s has_effective_args=%s total=%dms cache_hit=%s",
        query,
        selected_dir_name,
        str(has_effective_args).lower(),
        query_elapsed_ms,
        str(cache_hit).lower(),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
