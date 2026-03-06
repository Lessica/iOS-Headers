from __future__ import annotations

from dataclasses import dataclass

from web.data.repository import Repository


DIRECTORY_HITS_LIMIT = 30
OWNER_HITS_LIMIT = 50


@dataclass(frozen=True)
class SearchResult:
    directory_hits: list[tuple[str, str]]
    owner_hits: list[tuple[str, str, int]]


class SearchService:
    def __init__(self, repository: Repository) -> None:
        self._repo = repository

    def search(self, query: str) -> SearchResult:
        normalized = query.strip()
        if not normalized:
            return SearchResult(directory_hits=[], owner_hits=[])

        directory_hits = self._repo.search_directories(prefix=normalized, limit=DIRECTORY_HITS_LIMIT)
        owner_hits = self._repo.search_owner_candidates(keyword=normalized, limit=OWNER_HITS_LIMIT)
        return SearchResult(directory_hits=directory_hits, owner_hits=owner_hits)
