TRUNCATE TABLE ios_headers.path_versions;

INSERT INTO ios_headers.path_versions
SELECT
    path_id,
    version_num,
    toUInt8(1) AS seen
FROM ios_headers.file_instances
GROUP BY path_id, version_num;
