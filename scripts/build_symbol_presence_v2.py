#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from urllib import error, parse, request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build symbol_presence aggregation table from symbols/file_instances (v2, no dedup)."
    )
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:18123")
    parser.add_argument("--clickhouse-db", default="ios_headers")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--bundle", action="append", default=[])
    parser.add_argument("--version-id", action="append", default=[])
    parser.add_argument("--truncate-first", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-sleep", type=float, default=1.0)
    return parser.parse_args()


class ClickHouseClient:
    def __init__(self, base_url: str, database: str, user: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.database = database
        self.user = user
        self.password = password

    def execute(self, sql: str, retries: int = 3, retry_sleep: float = 1.0) -> str:
        params = {
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "query": sql,
        }
        url = f"{self.base_url}/?{parse.urlencode(params)}"
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                req = request.Request(url=url, method="POST")
                with request.urlopen(req, timeout=1800) as resp:
                    return resp.read().decode("utf-8", errors="replace")
            except (error.HTTPError, error.URLError, TimeoutError) as exc:
                last_exc = exc
                if attempt == retries:
                    break
                time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"ClickHouse SQL failed after retries: {sql[:200]}...") from last_exc


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _build_filter_sql(bundles: list[str], version_ids: list[str]) -> str:
    filters: list[str] = []
    if bundles:
        filters.append("v.bundle_name IN (" + ", ".join(_quote(item) for item in bundles) + ")")
    if version_ids:
        filters.append("fi.version_id IN (" + ", ".join(_quote(item) for item in version_ids) + ")")
    if not filters:
        return ""
    return "WHERE " + " AND ".join(filters)


def main() -> None:
    args = parse_args()

    ch = ClickHouseClient(
        base_url=args.clickhouse_url,
        database=args.clickhouse_db,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )

    ch.execute("SELECT 1", retries=args.max_retries, retry_sleep=args.retry_sleep)

    filter_sql = _build_filter_sql(args.bundle, args.version_id)

    if args.truncate_first and not filter_sql:
        ch.execute(
            "TRUNCATE TABLE symbol_presence",
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
        print("[setup] truncated symbol_presence")

    start = time.time()

    insert_sql = f"""
    INSERT INTO symbol_presence
    (path_id, owner_name, symbol_type, symbol_key, version_nums, versions_count, updated_at)
    SELECT
        fi.path_id,
        s.owner_name,
        s.symbol_type,
        s.symbol_key,
        arraySort(groupUniqArray(fi.version_num)) AS version_nums,
        toUInt16(length(version_nums)) AS versions_count,
        now() AS updated_at
    FROM symbols AS s
    INNER JOIN file_instances AS fi ON fi.content_id = s.content_id
    INNER JOIN versions AS v ON v.version_num = fi.version_num
    {filter_sql}
    GROUP BY
        fi.path_id,
        s.owner_name,
        s.symbol_type,
        s.symbol_key
    """
    ch.execute(insert_sql, retries=args.max_retries, retry_sleep=args.retry_sleep)

    if filter_sql:
        count_sql = f"""
        SELECT count()
        FROM symbol_presence AS sp
        INNER JOIN paths AS p ON p.path_id = sp.path_id
        WHERE sp.path_id IN (
            SELECT DISTINCT fi.path_id
            FROM file_instances AS fi
            INNER JOIN versions AS v ON v.version_num = fi.version_num
            {filter_sql}
        )
        """
    else:
        count_sql = "SELECT count() FROM symbol_presence"

    total = ch.execute(count_sql, retries=args.max_retries, retry_sleep=args.retry_sleep).strip()
    elapsed = round(time.time() - start, 2)

    print(f"[done] symbol_presence rows={total} elapsed_sec={elapsed}")


if __name__ == "__main__":
    main()
