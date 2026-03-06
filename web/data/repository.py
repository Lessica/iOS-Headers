from __future__ import annotations

from dataclasses import dataclass

from web.data.ch_client import ClickHouseClient


@dataclass(frozen=True)
class FileRef:
    version_num: int
    version_id: str
    path_id: int
    absolute_path: str


@dataclass(frozen=True)
class FileContentRef:
    version_num: int
    version_id: str
    path_id: int
    content_id: int
    absolute_path: str
    pack_object_key: str
    pack_offset: int
    pack_length: int


class Repository:
    def __init__(self, ch: ClickHouseClient) -> None:
        self._ch = ch

    def get_latest_version(self) -> tuple[int, str] | None:
        rows = self._ch.query(
            """
            SELECT version_num, version_id
            FROM versions
            ORDER BY version_num DESC
            LIMIT 1
            """
        )
        if not rows:
            return None
        version_num, version_id = rows[0]
        return int(version_num), str(version_id)

    def get_version_num(self, version_id: str) -> int | None:
        rows = self._ch.query(
            """
            SELECT version_num
            FROM versions
            WHERE version_id = %(version_id)s
            LIMIT 1
            """,
            {"version_id": version_id},
        )
        if not rows:
            return None
        return int(rows[0][0])

    def get_version_id(self, version_num: int) -> str | None:
        rows = self._ch.query(
            """
            SELECT version_id
            FROM versions
            WHERE version_num = %(version_num)s
            LIMIT 1
            """,
            {"version_num": version_num},
        )
        if not rows:
            return None
        return str(rows[0][0])

    def resolve_latest_for_path(self, absolute_path: str) -> FileRef | None:
        rows = self._ch.query(
            """
            SELECT
                fi.version_num,
                v.version_id,
                p.path_id,
                p.absolute_path
            FROM paths p
            INNER JOIN file_instances fi ON fi.path_id = p.path_id
            INNER JOIN versions v ON v.version_num = fi.version_num
            WHERE p.absolute_path = %(absolute_path)s
            ORDER BY fi.version_num DESC
            LIMIT 1
            """,
            {"absolute_path": absolute_path},
        )
        if not rows:
            return None
        return FileRef(
            version_num=int(rows[0][0]),
            version_id=str(rows[0][1]),
            path_id=int(rows[0][2]),
            absolute_path=str(rows[0][3]),
        )

    def get_file_content_ref(self, version_num: int, absolute_path: str) -> FileContentRef | None:
        rows = self._ch.query(
            """
            SELECT
                fi.version_num,
                v.version_id,
                p.path_id,
                fi.content_id,
                p.absolute_path,
                c.pack_object_key,
                c.pack_offset,
                c.pack_length
            FROM file_instances fi
            INNER JOIN paths p ON p.path_id = fi.path_id
            INNER JOIN versions v ON v.version_num = fi.version_num
            INNER JOIN contents c ON c.content_id = fi.content_id
            WHERE fi.version_num = %(version_num)s
              AND p.absolute_path = %(absolute_path)s
            LIMIT 1
            """,
            {"version_num": version_num, "absolute_path": absolute_path},
        )
        if not rows:
            return None
        row = rows[0]
        return FileContentRef(
            version_num=int(row[0]),
            version_id=str(row[1]),
            path_id=int(row[2]),
            content_id=int(row[3]),
            absolute_path=str(row[4]),
            pack_object_key=str(row[5]),
            pack_offset=int(row[6]),
            pack_length=int(row[7]),
        )

    def search_directories(self, prefix: str, limit: int = 30) -> list[tuple[str, int]]:
        rows = self._ch.query(
            """
            WITH lowerUTF8(%(prefix)s) AS prefix_lc
            SELECT
                dir_name,
                count() AS file_count
            FROM paths
            WHERE startsWith(dir_name_lc, prefix_lc)
            GROUP BY dir_name
            ORDER BY dir_name ASC
            LIMIT %(limit)s
            """,
            {"prefix": prefix, "limit": limit},
        )
        return [(str(row[0]), int(row[1])) for row in rows]

    def search_owner_candidates(self, keyword: str, limit: int = 50) -> list[FileRef]:
        rows = self._ch.query(
            """
            WITH lowerUTF8(%(keyword)s) AS keyword_lc,
            latest_file_version_by_path AS (
                SELECT
                    path_id,
                    max(version_num) AS latest_version_num
                FROM file_instances
                GROUP BY path_id
            ),
            filename_candidates AS (
                SELECT
                    if(p.file_name_lc = keyword_lc, 0, 10) AS priority_rank,
                    lv.latest_version_num AS matched_version_num,
                    p.path_id AS path_id,
                    p.absolute_path AS absolute_path
                FROM paths p
                INNER JOIN latest_file_version_by_path lv ON lv.path_id = p.path_id
                WHERE p.file_name_lc = keyword_lc
                   OR positionUTF8(p.file_name_lc, keyword_lc) > 0
            ),
            owner_candidates AS (
                SELECT
                    min(
                        multiIf(
                            sp.owner_name_lc = keyword_lc AND sp.owner_kind = 'interface', 1,
                            sp.owner_name_lc = keyword_lc AND sp.owner_kind = 'protocol', 2,
                            sp.owner_name_lc = keyword_lc AND sp.owner_kind = 'category', 3,
                            sp.owner_name_lc = keyword_lc, 4,
                            positionUTF8(sp.owner_name_lc, keyword_lc) > 0 AND sp.owner_kind = 'interface', 11,
                            positionUTF8(sp.owner_name_lc, keyword_lc) > 0 AND sp.owner_kind = 'protocol', 12,
                            positionUTF8(sp.owner_name_lc, keyword_lc) > 0 AND sp.owner_kind = 'category', 13,
                            14
                        )
                    ) AS priority_rank,
                    bitmapMax(groupBitmapMergeState(sp.version_bitmap)) AS matched_version_num,
                    p.path_id AS path_id,
                    p.absolute_path AS absolute_path
                FROM symbol_presence sp
                INNER JOIN paths p ON p.path_id = sp.path_id
                WHERE sp.owner_name_lc = keyword_lc
                   OR positionUTF8(sp.owner_name_lc, keyword_lc) > 0
                GROUP BY p.path_id, p.absolute_path
            ),
            candidates AS (
                SELECT * FROM filename_candidates
                UNION ALL
                SELECT * FROM owner_candidates
            )
            SELECT
                best_priority_rank,
                max_version_num,
                max_version_id,
                path_id,
                absolute_path
            FROM
            (
                SELECT
                    c.priority_rank AS best_priority_rank,
                    c.matched_version_num AS max_version_num,
                    v.version_id AS max_version_id,
                    c.path_id AS path_id,
                    c.absolute_path AS absolute_path,
                    row_number() OVER (
                        PARTITION BY c.path_id
                        ORDER BY c.priority_rank ASC, c.matched_version_num DESC, c.absolute_path ASC
                    ) AS rn
                FROM
                    candidates c
                INNER JOIN versions v ON v.version_num = c.matched_version_num
                WHERE c.matched_version_num > 0
            )
            WHERE rn = 1
            ORDER BY best_priority_rank ASC, max_version_num DESC, absolute_path ASC
            LIMIT %(limit)s
            """,
            {"keyword": keyword, "limit": limit},
        )
        return [
            FileRef(
                version_num=int(row[1]),
                version_id=str(row[2]),
                path_id=int(row[3]),
                absolute_path=str(row[4]),
            )
            for row in rows
        ]

    def list_files_in_directory_name(self, version_num: int, directory_name: str, limit: int = 2000) -> list[FileRef]:
        rows = self._ch.query(
            """
            SELECT
                fi.version_num,
                v.version_id,
                p.path_id,
                p.absolute_path
            FROM file_instances fi
            INNER JOIN paths p ON p.path_id = fi.path_id
            INNER JOIN versions v ON v.version_num = fi.version_num
            WHERE fi.version_num = %(version_num)s
                            AND p.dir_name = %(directory_name)s
            ORDER BY p.absolute_path ASC
            LIMIT %(limit)s
            """,
                        {"version_num": version_num, "directory_name": directory_name, "limit": limit},
        )
        return [
            FileRef(
                version_num=int(row[0]),
                version_id=str(row[1]),
                path_id=int(row[2]),
                absolute_path=str(row[3]),
            )
            for row in rows
        ]

    def list_versions_for_path(self, path_id: int) -> list[tuple[int, str]]:
        rows = self._ch.query(
            """
            SELECT fi.version_num, v.version_id
            FROM file_instances fi
            INNER JOIN versions v ON v.version_num = fi.version_num
            WHERE fi.path_id = %(path_id)s
            GROUP BY fi.version_num, v.version_id
            ORDER BY fi.version_num DESC
            """,
            {"path_id": path_id},
        )
        return [(int(row[0]), str(row[1])) for row in rows]

    def list_symbols_for_content(self, content_id: int) -> list[tuple[str, str, str]]:
        rows = self._ch.query(
            """
            SELECT DISTINCT owner_name, symbol_type, symbol_key
            FROM symbols
            WHERE content_id = %(content_id)s
            ORDER BY owner_name ASC, symbol_type ASC, symbol_key ASC
            """,
            {"content_id": content_id},
        )
        return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]

    def get_symbol_presence_map(self, path_id: int) -> dict[tuple[str, str, str], set[int]]:
        rows = self._ch.query(
            """
            SELECT
                owner_name,
                symbol_type,
                symbol_key,
                bitmapToArray(groupBitmapMergeState(version_bitmap)) AS version_nums
            FROM symbol_presence
            WHERE path_id = %(path_id)s
            GROUP BY owner_name, symbol_type, symbol_key
            """,
            {"path_id": path_id},
        )
        presence: dict[tuple[str, str, str], set[int]] = {}
        for owner_name, symbol_type, symbol_key, version_nums in rows:
            key = (str(owner_name), str(symbol_type), str(symbol_key))
            presence[key] = {int(version_num) for version_num in version_nums}
        return presence

    def list_paths_in_directory(self, version_num: int, directory: str) -> set[str]:
        rows = self._ch.query(
            """
            SELECT p.absolute_path
            FROM file_instances fi
            INNER JOIN paths p ON p.path_id = fi.path_id
            WHERE fi.version_num = %(version_num)s
                            AND p.dir_path = %(directory)s
            """,
            {"version_num": version_num, "directory": directory},
        )
        return {str(row[0]) for row in rows}
