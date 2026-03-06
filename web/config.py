from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str
    redis_host: str
    redis_port: int
    redis_db: int
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool
    view_cache_ttl_seconds: int
    search_cache_ttl_seconds: int
    enable_redis_page_cache: bool
    show_query_elapsed_ms: bool



def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}



def load_settings() -> Settings:
    return Settings(
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        clickhouse_port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        clickhouse_db=os.getenv("CLICKHOUSE_DB", "ios_headers"),
        clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
        clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        redis_host=os.getenv("REDIS_HOST", "redis"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_db=int(os.getenv("REDIS_DB", "0")),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        minio_bucket=os.getenv("MINIO_BUCKET", "ios-headers"),
        minio_secure=_as_bool(os.getenv("MINIO_SECURE"), False),
        view_cache_ttl_seconds=int(os.getenv("VIEW_CACHE_TTL_SECONDS", str(60 * 60 * 24 * 30))),
        search_cache_ttl_seconds=int(os.getenv("SEARCH_CACHE_TTL_SECONDS", "300")),
        enable_redis_page_cache=_as_bool(os.getenv("ENABLE_REDIS_PAGE_CACHE"), False),
        show_query_elapsed_ms=_as_bool(os.getenv("SHOW_QUERY_ELAPSED_MS"), False),
    )
