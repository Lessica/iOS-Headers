from __future__ import annotations

from typing import Any

from clickhouse_driver import Client

from web.config import Settings


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._client = Client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
            send_receive_timeout=30,
        )

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        result = self._client.execute(sql, params or {})
        if isinstance(result, list):
            return [tuple(row) if not isinstance(row, tuple) else row for row in result]
        raise TypeError(f"Unexpected ClickHouse result type: {type(result).__name__}")
