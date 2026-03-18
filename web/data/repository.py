from __future__ import annotations

from dataclasses import dataclass

from web.data.cache import RedisCache
from web.data.ch_client import ClickHouseClient


@dataclass(frozen=True)
class FileRef:
    version_num: int
    version_id: str
    path_id: int
    absolute_path: str
    file_size_bytes: int | None = None


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
    def __init__(
        self,
        ch: ClickHouseClient,
        version_cache_ttl_seconds: int,
        stats_cache_ttl_seconds: int,
        cache: RedisCache | None = None,
    ) -> None:
        self._ch = ch
        self._cache = cache
        self._version_num_by_id_lc: dict[str, int] = {}
        self._version_id_by_num: dict[int, str] = {}
        self._version_cache_ttl_seconds = version_cache_ttl_seconds
        self._stats_cache_ttl_seconds = stats_cache_ttl_seconds

    def get_latest_version(self) -> tuple[int, str] | None:
        rows = self._ch.query(
            """
            SELECT max(version_num)
            FROM versions
            """
        )
        if not rows or rows[0][0] is None:
            return None
        version_num = int(rows[0][0])
        version_id = self.get_version_id(version_num)
        if version_id is None:
            return None
        return version_num, version_id

    def get_version_num(self, version_id: str) -> int | None:
        version_id_lc = version_id.lower()
        cached = self._version_num_by_id_lc.get(version_id_lc)
        if cached is not None:
            return cached

        redis_key = f"cache:version:num-by-id:{version_id_lc}"
        if self._cache is not None:
            redis_value = self._cache.get_text(redis_key)
            if redis_value is not None:
                version_num = int(redis_value)
                self._version_num_by_id_lc[version_id_lc] = version_num
                return version_num

        rows = self._ch.query(
            """
            SELECT dictGetOrNull(
                'ios_headers.versions_by_id_lc_dict',
                'version_num',
                lowerUTF8(%(version_id)s)
            )
            """,
            {"version_id": version_id},
        )
        if not rows or rows[0][0] is None:
            return None
        version_num = int(rows[0][0])
        self._version_num_by_id_lc[version_id_lc] = version_num
        self._version_id_by_num[version_num] = version_id
        if self._cache is not None:
            self._cache.set_text(redis_key, str(version_num), self._version_cache_ttl_seconds)
            self._cache.set_text(
                f"cache:version:id-by-num:{version_num}",
                version_id,
                self._version_cache_ttl_seconds,
            )
        return version_num

    def get_version_id(self, version_num: int) -> str | None:
        cached = self._version_id_by_num.get(version_num)
        if cached is not None:
            return cached

        redis_key = f"cache:version:id-by-num:{version_num}"
        if self._cache is not None:
            redis_value = self._cache.get_text(redis_key)
            if redis_value is not None:
                version_id = str(redis_value)
                self._version_id_by_num[version_num] = version_id
                self._version_num_by_id_lc[version_id.lower()] = version_num
                return version_id

        rows = self._ch.query(
            """
            SELECT dictGetOrNull(
                'ios_headers.versions_by_num_dict',
                'version_id',
                toUInt32(%(version_num)s)
            )
            """,
            {"version_num": version_num},
        )
        if not rows or rows[0][0] is None:
            return None
        version_id = str(rows[0][0])
        self._version_id_by_num[version_num] = version_id
        self._version_num_by_id_lc[version_id.lower()] = version_num
        if self._cache is not None:
            self._cache.set_text(redis_key, version_id, self._version_cache_ttl_seconds)
            self._cache.set_text(
                f"cache:version:num-by-id:{version_id.lower()}",
                str(version_num),
                self._version_cache_ttl_seconds,
            )
        return version_id

    def resolve_latest_for_path(self, absolute_path: str) -> FileRef | None:
        rows = self._ch.query(
            """
            WITH dictGetOrNull(
                'ios_headers.paths_by_absolute_path_dict',
                'path_id',
                toString(%(absolute_path)s)
            ) AS target_path_id
            SELECT
                fi.version_num,
                dictGet(
                    'ios_headers.versions_by_num_dict',
                    'version_id',
                    toUInt32(fi.version_num)
                ) AS version_id,
                fi.path_id,
                %(absolute_path)s AS absolute_path
            FROM file_instances fi
            WHERE isNotNull(target_path_id)
                AND fi.path_id = target_path_id
                AND dictHas('ios_headers.versions_by_num_dict', toUInt32(fi.version_num))
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
            WITH dictGetOrNull(
                'ios_headers.paths_by_absolute_path_dict',
                'path_id',
                toString(%(absolute_path)s)
            ) AS target_path_id
            SELECT
                fi.version_num,
                dictGet(
                    'ios_headers.versions_by_num_dict',
                    'version_id',
                    toUInt32(fi.version_num)
                ) AS version_id,
                fi.path_id,
                fi.content_id,
                %(absolute_path)s AS absolute_path,
                dictGet(
                    'ios_headers.contents_by_content_id_dict',
                    'pack_object_key',
                    toUInt64(fi.content_id)
                ) AS pack_object_key,
                dictGet(
                    'ios_headers.contents_by_content_id_dict',
                    'pack_offset',
                    toUInt64(fi.content_id)
                ) AS pack_offset,
                dictGet(
                    'ios_headers.contents_by_content_id_dict',
                    'pack_length',
                    toUInt64(fi.content_id)
                ) AS pack_length
            FROM file_instances fi
            WHERE fi.version_num = %(version_num)s
                AND isNotNull(target_path_id)
                AND fi.path_id = target_path_id
                AND dictHas('ios_headers.versions_by_num_dict', toUInt32(fi.version_num))
                AND dictHas('ios_headers.contents_by_content_id_dict', toUInt64(fi.content_id))
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

    def search_directories(self, prefix: str, limit: int = 30) -> list[tuple[str, str]]:
        rows = self._ch.query(
            """
            WITH lowerUTF8(%(prefix)s) AS prefix_lc
            SELECT
                dir_name,
                min(dir_path) AS sample_dir_path
            FROM paths
            WHERE startsWith(dir_name_lc, prefix_lc)
            GROUP BY dir_name
            ORDER BY dir_name ASC
            LIMIT %(limit)s
            """,
            {"prefix": prefix, "limit": limit},
        )
        return [(str(row[0]), str(row[1])) for row in rows]

    def search_owner_candidates(self, keyword: str, limit: int = 50) -> list[tuple[str, str, int]]:
        keyword_lc = keyword.strip().lower()
        if not keyword_lc:
            return []

        filename_limit = max(limit * 12, 300)
        filename_rows = self._ch.query(
            """
            WITH lowerUTF8(%(keyword)s) AS keyword_lc
            SELECT
                if(file_name_lc = keyword_lc, 0, 10) AS priority_rank,
                path_id,
                absolute_path
            FROM paths
            WHERE file_name_lc = keyword_lc
               OR positionUTF8(file_name_lc, keyword_lc) > 0
            ORDER BY priority_rank ASC, absolute_path ASC
            LIMIT %(limit)s
            """,
            {"keyword": keyword_lc, "limit": filename_limit},
        )

        filename_candidates: dict[int, tuple[int, int, str, str]] = {}
        for priority_rank, path_id, absolute_path in filename_rows:
            path_id_int = int(path_id)
            filename_candidates[path_id_int] = (
                int(priority_rank),
                0,
                "",
                str(absolute_path),
            )

        owner_limit = max(limit * 12, 600)
        owner_rows = self._ch.query(
            """
            WITH lowerUTF8(%(keyword)s) AS keyword_lc
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
                dictGetOrNull(
                    'ios_headers.versions_by_num_dict',
                    'version_id',
                    toUInt32(matched_version_num)
                ) AS matched_version_id,
                sp.path_id AS path_id,
                dictGetOrNull(
                    'ios_headers.paths_by_id_dict',
                    'absolute_path',
                    toUInt64(sp.path_id)
                ) AS absolute_path
            FROM symbol_presence sp
            WHERE sp.owner_name_lc = keyword_lc
               OR positionUTF8(sp.owner_name_lc, keyword_lc) > 0
            GROUP BY sp.path_id
            HAVING absolute_path IS NOT NULL
                AND matched_version_num > 0
                AND matched_version_id IS NOT NULL
            ORDER BY priority_rank ASC, matched_version_num DESC, absolute_path ASC
            LIMIT %(limit)s
            """,
            {"keyword": keyword_lc, "limit": owner_limit},
        )

        merged_candidates: dict[int, tuple[int, int, str, str]] = dict(filename_candidates)
        for priority_rank, matched_version_num, matched_version_id, path_id, absolute_path in owner_rows:
            path_id_int = int(path_id)
            current = merged_candidates.get(path_id_int)
            candidate = (
                int(priority_rank),
                int(matched_version_num),
                str(matched_version_id),
                str(absolute_path),
            )
            if current is None:
                merged_candidates[path_id_int] = candidate
                continue

            current_priority, current_version_num, _current_version_id, current_absolute_path = current
            candidate_priority, candidate_version_num, _candidate_version_id, candidate_absolute_path = candidate
            if (
                candidate_priority < current_priority
                or (
                    candidate_priority == current_priority
                    and (
                        candidate_version_num > current_version_num
                        or (
                            candidate_version_num == current_version_num
                            and candidate_absolute_path < current_absolute_path
                        )
                    )
                )
            ):
                merged_candidates[path_id_int] = candidate

        ordered = sorted(
            (
                (priority_rank, version_num, version_id, path_id, absolute_path)
                for path_id, (priority_rank, version_num, version_id, absolute_path) in merged_candidates.items()
            ),
            key=lambda item: (item[0], -item[1], item[4]),
        )
        top_rows = ordered[:limit]
        return [(version_id, absolute_path, path_id) for _p, _vn, version_id, path_id, absolute_path in top_rows]

    def count_distinct_directories(self) -> int:
        redis_key = "cache:stats:distinct-directories"
        if self._cache is not None:
            redis_value = self._cache.get_text(redis_key)
            if redis_value is not None:
                return int(redis_value)

        rows = self._ch.query(
            """
            SELECT countDistinct(dir_name)
            FROM paths
            """
        )
        if not rows:
            return 0
        result = int(rows[0][0])
        if self._cache is not None:
            self._cache.set_text(redis_key, str(result), self._stats_cache_ttl_seconds)
        return result

    def count_distinct_owners(self) -> int:
        redis_key = "cache:stats:distinct-owners"
        if self._cache is not None:
            redis_value = self._cache.get_text(redis_key)
            if redis_value is not None:
                return int(redis_value)

        rows = self._ch.query(
            """
            SELECT countDistinct(owner_name_lc)
            FROM symbol_presence
            WHERE owner_name_lc != ''
            """
        )
        if not rows:
            return 0
        result = int(rows[0][0])
        if self._cache is not None:
            self._cache.set_text(redis_key, str(result), self._stats_cache_ttl_seconds)
        return result

    def list_version_ids_for_paths(self, path_ids: list[int]) -> dict[int, list[str]]:
        if not path_ids:
            return {}

        unique_path_ids = sorted(set(path_ids))
        rows = self._ch.query(
            """
            SELECT
                fi.path_id,
                fi.version_num,
                dictGet(
                    'ios_headers.versions_by_num_dict',
                    'version_id',
                    toUInt32(fi.version_num)
                ) AS version_id
            FROM file_instances fi
            WHERE fi.path_id IN %(path_ids)s
                AND dictHas('ios_headers.versions_by_num_dict', toUInt32(fi.version_num))
            GROUP BY fi.path_id, fi.version_num, version_id
            ORDER BY fi.path_id ASC, fi.version_num DESC
            """,
            {"path_ids": unique_path_ids},
        )

        version_ids_by_path: dict[int, list[str]] = {}
        for path_id, _version_num, version_id in rows:
            bucket = version_ids_by_path.setdefault(int(path_id), [])
            bucket.append(str(version_id))
        return version_ids_by_path

    def count_unique_paths_in_directory_name(self, directory_name: str, keyword: str = "") -> int:
        keyword_lc = keyword.strip().lower()
        keyword_clause = (
            """
            AND positionUTF8(file_name_lc, %(keyword_lc)s) > 0
            """
            if keyword_lc
            else ""
        )
        rows = self._ch.query(
            f"""
            SELECT count()
            FROM paths
            WHERE dir_name_lc = lowerUTF8(%(directory_name)s)
                {keyword_clause}
            """,
            {"directory_name": directory_name, "keyword_lc": keyword_lc},
        )
        if not rows:
            return 0
        return int(rows[0][0])

    def get_file_in_directory_by_preferred_names(
        self,
        directory_name: str,
        preferred_names: list[str],
    ) -> FileRef | None:
        preferred_file_names_lc: list[str] = []
        for name in preferred_names:
            normalized_name = name.strip().lower()
            if normalized_name and normalized_name not in preferred_file_names_lc:
                preferred_file_names_lc.append(normalized_name)

        if not preferred_file_names_lc:
            return None

        preferred_rows = self._ch.query(
            """
            SELECT
                path_id,
                absolute_path,
                indexOf(%(preferred_file_names_lc)s, file_name_lc) AS preferred_rank
            FROM paths
            WHERE dir_name_lc = lowerUTF8(%(directory_name)s)
                AND file_name_lc IN %(preferred_file_names_lc)s
            ORDER BY preferred_rank ASC, absolute_path ASC
            """,
            {
                "directory_name": directory_name,
                "preferred_file_names_lc": preferred_file_names_lc,
            },
        )
        if not preferred_rows:
            return None

        row = preferred_rows[0]
        path_id = int(row[0])
        absolute_path = str(row[1])
        return FileRef(
            version_num=0,
            version_id="",
            path_id=path_id,
            absolute_path=absolute_path,
            file_size_bytes=None,
        )

    def list_files_in_directory_name_page(
        self,
        directory_name: str,
        page_size: int,
        cursor: str | None,
        direction: str,
        keyword: str = "",
    ) -> tuple[list[FileRef], bool, bool, str | None, str | None]:
        safe_page_size = max(1, page_size)
        query_limit = safe_page_size + 1
        safe_direction = "prev" if direction == "prev" else "next"
        keyword_lc = keyword.strip().lower()
        keyword_clause = (
            """
            AND positionUTF8(file_name_lc, %(keyword_lc)s) > 0
            """
            if keyword_lc
            else ""
        )

        if safe_direction == "prev":
            order_sql = "DESC"
            cursor_clause = (
                """
                AND absolute_path < %(cursor)s
                """
                if cursor
                else ""
            )
        else:
            order_sql = "ASC"
            cursor_clause = (
                """
                AND absolute_path > %(cursor)s
                """
                if cursor
                else ""
            )

        paged_path_rows = self._ch.query(
            f"""
            SELECT
                path_id,
                absolute_path
            FROM paths
            WHERE dir_name_lc = lowerUTF8(%(directory_name)s)
                {keyword_clause}
                {cursor_clause}
            ORDER BY absolute_path {order_sql}
            LIMIT %(limit)s
            """,
            {
                "directory_name": directory_name,
                "keyword_lc": keyword_lc,
                "cursor": cursor or "",
                "limit": query_limit,
            },
        )

        files = [
            FileRef(
                version_num=0,
                version_id="",
                path_id=int(row[0]),
                absolute_path=str(row[1]),
                file_size_bytes=None,
            )
            for row in paged_path_rows
        ]

        has_more_in_direction = len(files) > safe_page_size
        if has_more_in_direction:
            files = files[:safe_page_size]

        if safe_direction == "prev":
            files.reverse()

        has_prev_page = bool(cursor) if safe_direction == "next" else has_more_in_direction
        has_next_page = has_more_in_direction if safe_direction == "next" else bool(cursor)

        prev_cursor = files[0].absolute_path if files and has_prev_page else None
        next_cursor = files[-1].absolute_path if files and has_next_page else None
        return files, has_prev_page, has_next_page, prev_cursor, next_cursor

    def list_versions_for_path(self, path_id: int) -> list[tuple[int, str]]:
        rows = self._ch.query(
            """
            SELECT
                fi.version_num,
                dictGet(
                    'ios_headers.versions_by_num_dict',
                    'version_id',
                    toUInt32(fi.version_num)
                ) AS version_id
            FROM file_instances fi
            WHERE fi.path_id = %(path_id)s
                AND dictHas('ios_headers.versions_by_num_dict', toUInt32(fi.version_num))
            GROUP BY fi.version_num, version_id
            ORDER BY fi.version_num DESC
            """,
            {"path_id": path_id},
        )
        return [(int(row[0]), str(row[1])) for row in rows]

    def list_symbols_for_content(self, content_id: int) -> list[tuple[str, str, str, int]]:
        rows = self._ch.query(
            """
            SELECT DISTINCT owner_name, symbol_type, symbol_key, line_no
            FROM symbols
            WHERE content_id = %(content_id)s
            ORDER BY owner_name ASC, symbol_type ASC, symbol_key ASC, line_no ASC
            """,
            {"content_id": content_id},
        )
        return [(str(row[0]), str(row[1]), str(row[2]), int(row[3])) for row in rows]

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
