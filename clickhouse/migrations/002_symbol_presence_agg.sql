-- Migration 002: rebuild symbol_presence with AggregatingMergeTree
--
-- WHY: The previous ReplacingMergeTree(updated_at) design required a single massive
-- GROUP BY across ALL versions to produce a correct version_bitmap per
-- (path_id, owner_name, symbol_type, symbol_key) row.  That query joins the
-- symbols and file_instances tables for every version in one shot and regularly
-- exceeded ClickHouse's memory limit (Code: 241, MEMORY_LIMIT_EXCEEDED).
--
-- A per-bundle workaround is also incorrect: path_id is derived solely from the
-- absolute header path (independent of bundle_name), so the same path appears in
-- multiple bundles.  ReplacingMergeTree would keep only the last-written row per
-- key, silently discarding the version bits from all other bundles.
--
-- FIX: Switch to AggregatingMergeTree with AggregateFunction(groupBitmapState, UInt64).
-- Each insert covers exactly ONE version_num, storing it as an element in a
-- RoaringBitmap via groupBitmapState.  ClickHouse merges these states lazily in
-- the background (and eagerly on SELECT ... FINAL), so the correct combined bitmap
-- is always available without ever running a cross-version join.
--
-- HOW TO RUN (existing installations only — new installs use 001_schema.sql):
--   1. Connect to ClickHouse:
--        clickhouse-client --host 127.0.0.1 --port 9001 --database ios_headers
--   2. Execute this file:
--        SOURCE /path/to/002_symbol_presence_agg.sql;
--      or paste its contents directly.
--   3. Re-populate the table:
--        python3 scripts/build_symbol_presence_v2.py --truncate-first
--
-- NOTE: Readers that previously queried `version_bitmap` as a plain UInt64 must
-- now use `groupBitmapMerge(version_bitmap)` together with a GROUP BY to get the
-- correctly merged bitmap value.  Example:
--   SELECT path_id, owner_name, symbol_type, symbol_key,
--          groupBitmapMerge(version_bitmap) AS version_bitmap,
--          max(updated_at) AS updated_at
--   FROM symbol_presence
--   GROUP BY path_id, owner_name, symbol_type, symbol_key;

DROP TABLE IF EXISTS ios_headers.symbol_presence;

CREATE TABLE IF NOT EXISTS ios_headers.symbol_presence (
    path_id UInt64,
    owner_name String,
    symbol_type LowCardinality(String),
    symbol_key String,
    version_bitmap AggregateFunction(groupBitmapState, UInt64),
    updated_at SimpleAggregateFunction(max, DateTime) DEFAULT now()
)
ENGINE = AggregatingMergeTree()
ORDER BY (path_id, symbol_type, symbol_key, owner_name)
SETTINGS index_granularity = 8192;
