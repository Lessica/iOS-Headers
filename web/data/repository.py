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
            SELECT
                dir,
                count() AS file_count
            FROM
            (
                SELECT replaceRegexpOne(absolute_path, '/[^/]+$', '') AS dir
                FROM paths
            )
            WHERE positionCaseInsensitiveUTF8(dir, %(prefix)s) = 1
            GROUP BY dir
            ORDER BY dir ASC
            LIMIT %(limit)s
            """,
            {"prefix": prefix, "limit": limit},
        )
        return [(str(row[0]), int(row[1])) for row in rows]

    def search_owner_candidates(self, keyword: str, limit: int = 50) -> list[FileRef]:
        rows = self._ch.query(
            """
            SELECT
                max_version_num,
                max_version_id,
                path_id,
                absolute_path
            FROM
            (
                SELECT
                    max(fi.version_num) AS max_version_num,
                    argMax(v.version_id, fi.version_num) AS max_version_id,
                    p.path_id AS path_id,
                    p.absolute_path AS absolute_path
                FROM file_instances fi
                INNER JOIN paths p ON p.path_id = fi.path_id
                INNER JOIN versions v ON v.version_num = fi.version_num
                WHERE positionCaseInsensitiveUTF8(extract(p.absolute_path, '[^/]+$'), %(keyword)s) > 0
                GROUP BY p.path_id, p.absolute_path

                UNION ALL

                SELECT
                    max(fi.version_num) AS max_version_num,
                    argMax(v.version_id, fi.version_num) AS max_version_id,
                    p.path_id AS path_id,
                    argMax(p.absolute_path, fi.version_num) AS absolute_path
                FROM symbols s
                INNER JOIN file_instances fi ON fi.content_id = s.content_id
                INNER JOIN paths p ON p.path_id = fi.path_id
                INNER JOIN versions v ON v.version_num = fi.version_num
                WHERE positionCaseInsensitiveUTF8(s.owner_name, %(keyword)s) > 0
                GROUP BY s.owner_kind, s.owner_name, p.path_id
            )
            GROUP BY path_id, absolute_path, max_version_num, max_version_id
            ORDER BY max_version_num DESC, absolute_path ASC
            LIMIT %(limit)s
            """,
            {"keyword": keyword, "limit": limit},
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

    def list_files_in_directory(self, version_num: int, directory: str, limit: int = 2000) -> list[FileRef]:
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
              AND replaceRegexpOne(p.absolute_path, '/[^/]+$', '') = %(directory)s
            ORDER BY p.absolute_path ASC
            LIMIT %(limit)s
            """,
            {"version_num": version_num, "directory": directory, "limit": limit},
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
            SELECT owner_name, symbol_type, symbol_key, version_nums
            FROM symbol_presence_readable
            WHERE path_id = %(path_id)s
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
              AND replaceRegexpOne(p.absolute_path, '/[^/]+$', '') = %(directory)s
            """,
            {"version_num": version_num, "directory": directory},
        )
        return {str(row[0]) for row in rows}
