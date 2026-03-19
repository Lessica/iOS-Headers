-- Migrate ios_headers.paths.dir_name semantics from last segment to last2 key.
-- Example: /aaa/bbb/ccc/ddd.h -> dir_name = bbb/ccc.

DROP TABLE IF EXISTS ios_headers.paths_v1_backup;
DROP TABLE IF EXISTS ios_headers.paths_v2;

CREATE TABLE IF NOT EXISTS ios_headers.paths_v2 (
    path_id UInt64,
    absolute_path String,
    path_lc String MATERIALIZED lowerUTF8(absolute_path),
    file_name String MATERIALIZED extract(absolute_path, '[^/]+$'),
    file_name_lc String MATERIALIZED lowerUTF8(file_name),
    dir_path String MATERIALIZED replaceRegexpOne(absolute_path, '/[^/]+$', ''),
    dir_name String MATERIALIZED arrayStringConcat(
        arraySlice(
            arrayFilter(segment -> segment != '', splitByChar('/', dir_path)),
            -2
        ),
        '/'
    ),
    dir_name_lc String MATERIALIZED lowerUTF8(dir_name),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (path_id)
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS idx_paths_bf ON ios_headers.paths_v2 (path_lc)
TYPE tokenbf_v1(32768, 3, 0)
GRANULARITY 64;

CREATE INDEX IF NOT EXISTS idx_paths_file_name_bf ON ios_headers.paths_v2 (file_name_lc)
TYPE tokenbf_v1(32768, 3, 0)
GRANULARITY 64;

CREATE INDEX IF NOT EXISTS idx_paths_file_name_ngram ON ios_headers.paths_v2 (file_name_lc)
TYPE ngrambf_v1(3, 32768, 3, 0)
GRANULARITY 64;

CREATE INDEX IF NOT EXISTS idx_paths_dir_name_bf ON ios_headers.paths_v2 (dir_name_lc)
TYPE tokenbf_v1(32768, 3, 0)
GRANULARITY 64;

CREATE INDEX IF NOT EXISTS idx_paths_absolute_path_bf ON ios_headers.paths_v2 (absolute_path)
TYPE bloom_filter(0.01)
GRANULARITY 64;

INSERT INTO ios_headers.paths_v2 (path_id, absolute_path, created_at)
SELECT
    path_id,
    absolute_path,
    created_at
FROM ios_headers.paths;

DROP DICTIONARY IF EXISTS ios_headers.paths_by_absolute_path_dict;
DROP DICTIONARY IF EXISTS ios_headers.paths_by_id_dict;

RENAME TABLE
    ios_headers.paths TO ios_headers.paths_v1_backup,
    ios_headers.paths_v2 TO ios_headers.paths;

CREATE DICTIONARY IF NOT EXISTS ios_headers.paths_by_absolute_path_dict
(
    absolute_path String,
    path_id UInt64
)
PRIMARY KEY absolute_path
SOURCE(
    CLICKHOUSE(
        NAME 'ios_headers_internal'
        DB 'ios_headers'
        TABLE 'paths'
    )
)
LAYOUT(HASHED())
LIFETIME(0);

CREATE DICTIONARY IF NOT EXISTS ios_headers.paths_by_id_dict
(
    path_id UInt64,
    absolute_path String,
    dir_name String,
    dir_path String,
    file_name_lc String
)
PRIMARY KEY path_id
SOURCE(
    CLICKHOUSE(
        NAME 'ios_headers_internal'
        DB 'ios_headers'
        TABLE 'paths'
    )
)
LAYOUT(HASHED())
LIFETIME(0);

SYSTEM RELOAD DICTIONARY ios_headers.paths_by_absolute_path_dict;
SYSTEM RELOAD DICTIONARY ios_headers.paths_by_id_dict;
