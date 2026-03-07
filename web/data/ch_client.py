from __future__ import annotations

from threading import local
from typing import Any

from clickhouse_driver import Client, errors

from web.config import Settings


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._local = local()

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

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        safe_params = params or {}
        client = self._get_client()
        try:
            result = client.execute(sql, safe_params)
        except (errors.PartiallyConsumedQueryError, OSError):
            self._reset_client()
            result = self._get_client().execute(sql, safe_params)
        if isinstance(result, list):
            return [tuple(row) if not isinstance(row, tuple) else row for row in result]
        raise TypeError(f"Unexpected ClickHouse result type: {type(result).__name__}")
