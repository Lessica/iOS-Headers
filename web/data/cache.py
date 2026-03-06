from __future__ import annotations

import redis

from web.config import Settings


class RedisCache:
    def __init__(self, settings: Settings) -> None:
        self._client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )

    def get_text(self, key: str) -> str | None:
        value = self._client.get(key)
        if value is None:
            return None
        return str(value)

    def set_text(self, key: str, value: str, ttl_seconds: int) -> None:
        self._client.setex(key, ttl_seconds, value)
