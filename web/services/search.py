from __future__ import annotations

from dataclasses import dataclass

from web.data.repository import FileRef, Repository


@dataclass(frozen=True)
class SearchResult:
    directory_hits: list[tuple[str, int]]
    owner_hits: list[FileRef]


class SearchService:
    def __init__(self, repository: Repository) -> None:
        self._repo = repository

    def search(self, query: str) -> SearchResult:
        normalized = query.strip()
        if not normalized:
            return SearchResult(directory_hits=[], owner_hits=[])

        directory_hits = self._repo.search_directories(prefix=normalized)
        owner_hits = self._repo.search_owner_candidates(keyword=normalized)
        return SearchResult(directory_hits=directory_hits, owner_hits=owner_hits)
