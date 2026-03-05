-- Migration 003: change version_bitmap column from groupBitOr to groupBitmapState
--
-- WHY: AggregateFunction(groupBitOr, UInt64) stores its aggregate state as a
-- plain 8-byte UInt64, NOT in RoaringBitmap format.  However, the ClickHouse
-- JDBC driver (com.clickhouse.client.api v0.6+, used by DataGrip and other Java
-- clients) dispatches to bitmap deserialization (readBitmap / Roaring64NavigableMap)
-- for all AggregateFunction column types whose function name starts with "groupBit".
-- Reading the 8-byte UInt64 state as a RoaringBitmap fails immediately with:
--   "org.roaringbitmap.InvalidRoaringFormat: I failed to find a valid cookie."
--
-- AggregateFunction(groupBitmapState, UInt64) stores its aggregate state as a
-- genuine RoaringBitmap, which the JDBC driver correctly deserializes.  Switching
-- to this type also simplifies the INSERT expression (no bit-shifting required)
-- and removes the 64-version upper limit imposed by the UInt64 bitmask approach.
--
-- SEMANTIC CHANGE:
--   Old: version_num N is encoded as bit (N-1) of a UInt64 bitmask.
--        Query with groupBitOrMerge(version_bitmap); test bit with bitTest(..., N-1).
--   New: version_num N is stored directly as an element in a RoaringBitmap.
--        Query with groupBitmapMerge(version_bitmap); test membership with
--        bitmapContains(groupBitmapMerge(version_bitmap), toUInt64(N)).
--
-- HOW TO RUN (existing installations only — new installs use 001_schema.sql):
--   1. BACKUP WARNING: The DROP TABLE below permanently deletes all existing data
--      in symbol_presence.  Back up the table first if needed:
--        CREATE TABLE ios_headers.symbol_presence_backup AS ios_headers.symbol_presence;
--        INSERT INTO ios_headers.symbol_presence_backup SELECT * FROM ios_headers.symbol_presence;
--   2. Connect to ClickHouse:
--        clickhouse-client --host 127.0.0.1 --port 9001 --database ios_headers
--   3. Execute this file:
--        SOURCE /path/to/003_symbol_presence_bitmap.sql;
--      or paste its contents directly.
--   4. Re-populate the table (this clears and rebuilds all rows from scratch):
--        python3 scripts/build_symbol_presence_v2.py --truncate-first

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
