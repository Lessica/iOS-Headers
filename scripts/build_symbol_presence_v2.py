#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from urllib import error, parse, request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build symbol_presence aggregation table from symbols/file_instances (v2)."
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
    parser.add_argument("--progress-every", type=int, default=1)
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
        }
        url = f"{self.base_url}/?{parse.urlencode(params)}"
        data = sql.encode("utf-8")
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                req = request.Request(url=url, data=data, method="POST")
                with request.urlopen(req, timeout=1800) as resp:
                    return resp.read().decode("utf-8", errors="replace")
            except error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                last_exc = RuntimeError(f"ClickHouse HTTP {exc.code}: {body.strip()[:500]}")
            except (error.URLError, TimeoutError) as exc:
                last_exc = exc
            if attempt == retries:
                break
            time.sleep(retry_sleep * attempt)
        raise RuntimeError(f"ClickHouse SQL failed after retries: {sql[:200]}...") from last_exc


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _get_version_nums(
    ch: ClickHouseClient,
    bundles: list[str],
    version_ids: list[str],
    retries: int,
    retry_sleep: float,
) -> list[int]:
    """Return version_num values matching the given filters (all if no filters)."""
    filters: list[str] = []
    if bundles:
        filters.append("bundle_name IN (" + ", ".join(_quote(b) for b in bundles) + ")")
    if version_ids:
        filters.append("version_id IN (" + ", ".join(_quote(v) for v in version_ids) + ")")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    rows = ch.execute(
        f"SELECT version_num FROM versions {where} ORDER BY version_num",
        retries=retries,
        retry_sleep=retry_sleep,
    ).splitlines()
    return [int(row.strip()) for row in rows if row.strip()]


def _version_insert_sql(version_num: int) -> str:
    # symbol_presence uses AggregatingMergeTree with AggregateFunction(groupBitmapState, UInt64).
    # Inserting one version at a time keeps each query small (no cross-version JOIN) and
    # lets ClickHouse merge partial aggregate states lazily in the background.
    # version_num is stored directly as an element of the RoaringBitmap; there is no
    # upper bound on version_num beyond the UInt64 maximum.
    return f"""
    INSERT INTO symbol_presence
    (path_id, owner_name, symbol_type, symbol_key, version_bitmap, updated_at)
    SELECT
        fi.path_id,
        s.owner_name,
        s.symbol_type,
        s.symbol_key,
        groupBitmapState(toUInt64(fi.version_num)),
        now()
    FROM symbols AS s
    INNER JOIN file_instances AS fi ON fi.content_id = s.content_id
    WHERE fi.version_num = {version_num}
    GROUP BY fi.path_id, s.owner_name, s.symbol_type, s.symbol_key
    """


def main() -> None:
    args = parse_args()

    ch = ClickHouseClient(
        base_url=args.clickhouse_url,
        database=args.clickhouse_db,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )

    ch.execute("SELECT 1", retries=args.max_retries, retry_sleep=args.retry_sleep)

    if args.truncate_first and not args.bundle and not args.version_id:
        ch.execute(
            "TRUNCATE TABLE symbol_presence",
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
        print("[setup] truncated symbol_presence")

    start = time.time()
    print("[progress] stage 1/3 prepare")

    version_nums = _get_version_nums(
        ch, args.bundle, args.version_id, args.max_retries, args.retry_sleep
    )
    print(f"[progress] target versions={len(version_nums)}")

    print(f"[progress] stage 2/3 aggregate insert versions={len(version_nums)}")
    insert_start = time.time()

    for idx, version_num in enumerate(version_nums, 1):
        version_start = time.time()
        ch.execute(
            _version_insert_sql(version_num),
            retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
        if idx % args.progress_every == 0 or idx == len(version_nums):
            print(f"[progress] version {idx}/{len(version_nums)} version_num={version_num} elapsed_sec={time.time() - version_start:.2f}")

    print(f"[progress] aggregate insert done elapsed_sec={time.time() - insert_start:.2f}")

    print("[progress] stage 3/3 count rows")
    # FINAL forces ClickHouse to merge all partial aggregate states before counting,
    # returning the true number of distinct (path_id, symbol_type, symbol_key, owner_name) rows.
    total = ch.execute(
        "SELECT count() FROM symbol_presence FINAL",
        retries=args.max_retries,
        retry_sleep=args.retry_sleep,
    ).strip()
    elapsed = round(time.time() - start, 2)

    print(f"[done] symbol_presence rows={total} elapsed_sec={elapsed}")


if __name__ == "__main__":
    main()
