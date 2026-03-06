CREATE DATABASE IF NOT EXISTS ios_headers;

CREATE TABLE IF NOT EXISTS ios_headers.versions (
    version_num UInt32,
    version_id String,
    version_id_lc String MATERIALIZED lowerUTF8(version_id),
    ios_version String,
    build String,
    bundle_name String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (version_num);

CREATE INDEX IF NOT EXISTS idx_versions_version_id_bf ON ios_headers.versions (version_id_lc)
TYPE tokenbf_v1(32768, 3, 0)
GRANULARITY 64;

CREATE TABLE IF NOT EXISTS ios_headers.paths (
    path_id UInt64,
    absolute_path String,
    path_lc String MATERIALIZED lowerUTF8(absolute_path),
    file_name String MATERIALIZED extract(absolute_path, '[^/]+$'),
    file_name_lc String MATERIALIZED lowerUTF8(file_name),
    dir_path String MATERIALIZED replaceRegexpOne(absolute_path, '/[^/]+$', ''),
    dir_name String MATERIALIZED extract(dir_path, '[^/]+$'),
    dir_name_lc String MATERIALIZED lowerUTF8(dir_name),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (path_id)
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_paths_bf ON ios_headers.paths (path_lc)
TYPE tokenbf_v1(32768, 3, 0)
GRANULARITY 64;

CREATE INDEX IF NOT EXISTS idx_paths_file_name_bf ON ios_headers.paths (file_name_lc)
TYPE tokenbf_v1(32768, 3, 0)
GRANULARITY 64;

CREATE INDEX IF NOT EXISTS idx_paths_file_name_ngram ON ios_headers.paths (file_name_lc)
TYPE ngrambf_v1(3, 32768, 3, 0)
GRANULARITY 64;

CREATE INDEX IF NOT EXISTS idx_paths_dir_name_bf ON ios_headers.paths (dir_name_lc)
TYPE tokenbf_v1(32768, 3, 0)
GRANULARITY 64;

CREATE TABLE IF NOT EXISTS ios_headers.contents (
    content_id UInt64,
    content_hash FixedString(32),
    blob_key String,
    pack_object_key String,
    pack_offset UInt64,
    pack_length UInt32,
    byte_size UInt32,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (content_id);

CREATE TABLE IF NOT EXISTS ios_headers.file_instances (
    version_num UInt32,
    path_id UInt64,
    content_id UInt64,
    updated_at DateTime DEFAULT now(),
    PROJECTION prj_latest_version_by_path
    (
        SELECT
            path_id,
            max(version_num)
        GROUP BY path_id
    )
)
ENGINE = MergeTree
PARTITION BY version_num
ORDER BY (path_id, version_num)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS ios_headers.symbols (
    content_id UInt64,
    owner_kind LowCardinality(String),
    owner_name String,
    owner_name_lc String MATERIALIZED lowerUTF8(owner_name),
    symbol_type LowCardinality(String),
    symbol_key String,
    line_no UInt32
)
ENGINE = MergeTree
ORDER BY (content_id, symbol_type, symbol_key, owner_name)
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_symbols_owner_name_ngram ON ios_headers.symbols (owner_name_lc)
TYPE ngrambf_v1(3, 32768, 3, 0)
GRANULARITY 64;

CREATE TABLE IF NOT EXISTS ios_headers.symbol_presence (
    path_id UInt64,
    owner_kind LowCardinality(String),
    owner_name String,
    owner_name_lc String,
    symbol_type LowCardinality(String),
    symbol_key String,
    version_bitmap AggregateFunction(groupBitmapState, UInt64),
    updated_at SimpleAggregateFunction(max, DateTime) DEFAULT now()
)
ENGINE = AggregatingMergeTree()
ORDER BY (path_id, owner_kind, symbol_type, symbol_key, owner_name)
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_symbol_presence_owner_name_ngram ON ios_headers.symbol_presence (owner_name_lc)
TYPE ngrambf_v1(3, 32768, 3, 0)
GRANULARITY 64;

CREATE VIEW IF NOT EXISTS ios_headers.symbol_presence_readable AS
SELECT
    path_id,
    owner_kind,
    owner_name,
    owner_name_lc,
    symbol_type,
    symbol_key,
    bitmapToArray(groupBitmapMergeState(version_bitmap)) AS version_nums,
    arrayStringConcat(
        arrayMap(v -> toString(v), bitmapToArray(groupBitmapMergeState(version_bitmap))),
        ','
    ) AS version_nums_csv,
    groupBitmapMerge(version_bitmap) AS version_count,
    max(updated_at) AS updated_at
FROM ios_headers.symbol_presence
GROUP BY
    path_id,
    owner_kind,
    owner_name,
    owner_name_lc,
    symbol_type,
    symbol_key;
