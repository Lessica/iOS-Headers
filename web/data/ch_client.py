from __future__ import annotations

import hashlib
import logging
from threading import local
from threading import Lock
import time
from typing import Any

from clickhouse_driver import Client, errors

from web.config import Settings


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._local = local()
        self._logger = logging.getLogger("gunicorn.error")
        self._explained_query_hashes: set[str] = set()
        self._explain_lock = Lock()

    def _create_client(self) -> Client:
        return Client(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            user=self._settings.clickhouse_user,
            password=self._settings.clickhouse_password,
            database=self._settings.clickhouse_db,
            send_receive_timeout=30,
        )

    def _get_client(self) -> Client:
        client = getattr(self._local, "client", None)
        if client is None:
            client = self._create_client()
            self._local.client = client
        return client

    def _reset_client(self) -> None:
        client = getattr(self._local, "client", None)
        if client is None:
            return
        if hasattr(client, "disconnect_connection"):
            client.disconnect_connection()
        self._local.client = None

    def _normalize_sql(self, sql: str) -> str:
        return " ".join(sql.split())

    def _preview_sql(self, sql: str) -> str:
        normalized_sql = self._normalize_sql(sql)
        preview_chars = max(120, self._settings.ch_query_preview_chars)
        if len(normalized_sql) <= preview_chars:
            return normalized_sql
        return f"{normalized_sql[:preview_chars]}..."

    def _query_hash(self, sql: str) -> str:
        normalized_sql = self._normalize_sql(sql)
        return hashlib.sha1(normalized_sql.encode("utf-8")).hexdigest()[:12]

    def _match_sql_debug_filter(self, sql: str) -> bool:
        debug_match = self._settings.ch_query_debug_match
        if not debug_match:
            return True
        return debug_match.lower() in sql.lower()

    def _run_explain_logs(self, client: Client, sql: str, params: dict[str, Any], query_hash: str) -> None:
        if not self._settings.enable_ch_query_explain:
            return
        if not self._match_sql_debug_filter(sql):
            return
        if self._settings.ch_query_explain_once:
            with self._explain_lock:
                if query_hash in self._explained_query_hashes:
                    return
                self._explained_query_hashes.add(query_hash)

        explain_modes = ["PLAN", "indexes=1", "PIPELINE", "ESTIMATE"]
        for mode in explain_modes:
            explain_started_at = time.perf_counter()
            try:
                explain_rows = client.execute(f"EXPLAIN {mode} {sql}", params)
                explain_elapsed_ms = int((time.perf_counter() - explain_started_at) * 1000)
                explain_text = "\n".join("\t".join(str(cell) for cell in row) for row in explain_rows)
                if len(explain_text) > 12000:
                    explain_text = f"{explain_text[:12000]}..."
                self._logger.info(
                    "CH_EXPLAIN mode=%s hash=%s elapsed_ms=%d\n%s",
                    mode,
                    query_hash,
                    explain_elapsed_ms,
                    explain_text,
                )
            except Exception as exc:
                self._logger.warning("CH_EXPLAIN_FAILED mode=%s hash=%s error=%s", mode, query_hash, exc)

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        safe_params = params or {}
        client = self._get_client()
        started_at = time.perf_counter()
        try:
            result = client.execute(sql, safe_params)
        except (errors.PartiallyConsumedQueryError, OSError):
            self._reset_client()
            client = self._get_client()
            result = client.execute(sql, safe_params)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        if isinstance(result, list):
            rows = [tuple(row) if not isinstance(row, tuple) else row for row in result]
            should_debug_log = self._settings.enable_ch_query_debug and self._match_sql_debug_filter(sql)
            if should_debug_log:
                query_hash = self._query_hash(sql)
                self._logger.info(
                    "CH_QUERY hash=%s elapsed_ms=%d rows=%d params=%s sql=%s",
                    query_hash,
                    elapsed_ms,
                    len(rows),
                    safe_params,
                    self._preview_sql(sql),
                )
                if elapsed_ms >= self._settings.ch_query_slow_ms:
                    self._logger.warning(
                        "CH_QUERY_SLOW hash=%s elapsed_ms=%d threshold_ms=%d",
                        query_hash,
                        elapsed_ms,
                        self._settings.ch_query_slow_ms,
                    )
                self._run_explain_logs(client, sql, safe_params, query_hash)
            return rows
        raise TypeError(f"Unexpected ClickHouse result type: {type(result).__name__}")
