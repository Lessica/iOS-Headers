ALTER TABLE ios_headers.symbol_presence
    ADD COLUMN IF NOT EXISTS owner_kind LowCardinality(String) AFTER path_id;

ALTER TABLE ios_headers.symbol_presence
    ADD COLUMN IF NOT EXISTS owner_name_lc String AFTER owner_name;

ALTER TABLE ios_headers.symbol_presence
    DROP COLUMN IF EXISTS latest_version_num;

ALTER TABLE ios_headers.symbol_presence
    MODIFY ORDER BY (path_id, owner_kind, symbol_type, symbol_key, owner_name);

TRUNCATE TABLE ios_headers.symbol_presence;

INSERT INTO ios_headers.symbol_presence
(path_id, owner_kind, owner_name, owner_name_lc, symbol_type, symbol_key, version_bitmap, updated_at)
SELECT
    fi.path_id,
    s.owner_kind,
    s.owner_name,
    lowerUTF8(s.owner_name) AS owner_name_lc,
    s.symbol_type,
    s.symbol_key,
    groupBitmapState(toUInt64(fi.version_num)) AS version_bitmap,
    now() AS updated_at
FROM ios_headers.symbols s
INNER JOIN ios_headers.file_instances fi ON fi.content_id = s.content_id
GROUP BY fi.path_id, s.owner_kind, s.owner_name, s.symbol_type, s.symbol_key;

DROP VIEW IF EXISTS ios_headers.symbol_presence_readable;

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
