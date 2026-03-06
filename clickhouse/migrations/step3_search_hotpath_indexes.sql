ALTER TABLE ios_headers.symbol_presence
    ADD INDEX IF NOT EXISTS idx_symbol_presence_owner_name_ngram (owner_name_lc)
    TYPE ngrambf_v1(3, 32768, 3, 0)
    GRANULARITY 64;

ALTER TABLE ios_headers.symbol_presence
    MATERIALIZE INDEX idx_symbol_presence_owner_name_ngram;

ALTER TABLE ios_headers.file_instances
    ADD PROJECTION IF NOT EXISTS prj_latest_version_by_path
    (
        SELECT
            path_id,
            max(version_num)
        GROUP BY path_id
    );

ALTER TABLE ios_headers.file_instances
    MATERIALIZE PROJECTION prj_latest_version_by_path;
