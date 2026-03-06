ALTER TABLE ios_headers.paths
    ADD INDEX IF NOT EXISTS idx_paths_file_name_ngram (file_name_lc)
    TYPE ngrambf_v1(3, 32768, 3, 0)
    GRANULARITY 64;

ALTER TABLE ios_headers.paths
    MATERIALIZE INDEX idx_paths_file_name_ngram;

ALTER TABLE ios_headers.symbols
    ADD COLUMN IF NOT EXISTS owner_name_lc String MATERIALIZED lowerUTF8(owner_name);

ALTER TABLE ios_headers.symbols
    MATERIALIZE COLUMN owner_name_lc;

ALTER TABLE ios_headers.symbols
    ADD INDEX IF NOT EXISTS idx_symbols_owner_name_ngram (owner_name_lc)
    TYPE ngrambf_v1(3, 32768, 3, 0)
    GRANULARITY 64;

ALTER TABLE ios_headers.symbols
    MATERIALIZE INDEX idx_symbols_owner_name_ngram;
