ALTER TABLE ios_headers.paths ADD COLUMN IF NOT EXISTS file_name String MATERIALIZED extract(absolute_path, '[^/]+$');
ALTER TABLE ios_headers.paths ADD COLUMN IF NOT EXISTS file_name_lc String MATERIALIZED lowerUTF8(file_name);
ALTER TABLE ios_headers.paths ADD COLUMN IF NOT EXISTS dir_path String MATERIALIZED replaceRegexpOne(absolute_path, '/[^/]+$', '');
ALTER TABLE ios_headers.paths ADD COLUMN IF NOT EXISTS dir_name String MATERIALIZED extract(dir_path, '[^/]+$');
ALTER TABLE ios_headers.paths ADD COLUMN IF NOT EXISTS dir_name_lc String MATERIALIZED lowerUTF8(dir_name);
ALTER TABLE ios_headers.paths ADD COLUMN IF NOT EXISTS is_category_file UInt8 MATERIALIZED toUInt8(positionUTF8(file_name, '+') > 0);

ALTER TABLE ios_headers.paths MATERIALIZE COLUMN file_name;
ALTER TABLE ios_headers.paths MATERIALIZE COLUMN file_name_lc;
ALTER TABLE ios_headers.paths MATERIALIZE COLUMN dir_path;
ALTER TABLE ios_headers.paths MATERIALIZE COLUMN dir_name;
ALTER TABLE ios_headers.paths MATERIALIZE COLUMN dir_name_lc;
ALTER TABLE ios_headers.paths MATERIALIZE COLUMN is_category_file;

ALTER TABLE ios_headers.paths ADD INDEX IF NOT EXISTS idx_paths_file_name_bf (file_name_lc) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 64;
ALTER TABLE ios_headers.paths ADD INDEX IF NOT EXISTS idx_paths_dir_name_bf (dir_name_lc) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 64;
