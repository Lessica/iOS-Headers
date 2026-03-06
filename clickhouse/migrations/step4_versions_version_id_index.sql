ALTER TABLE ios_headers.versions
    ADD COLUMN IF NOT EXISTS version_id_lc String MATERIALIZED lowerUTF8(version_id) AFTER version_id;

ALTER TABLE ios_headers.versions
    MATERIALIZE COLUMN version_id_lc;

ALTER TABLE ios_headers.versions
    ADD INDEX IF NOT EXISTS idx_versions_version_id_bf (version_id_lc)
    TYPE tokenbf_v1(32768, 3, 0)
    GRANULARITY 64;

ALTER TABLE ios_headers.versions
    MATERIALIZE INDEX idx_versions_version_id_bf;
