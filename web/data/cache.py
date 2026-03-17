from __future__ import annotations

import logging

import redis

from web.config import Settings


logger = logging.getLogger("gunicorn.error")


class RedisCache:
    def __init__(self, settings: Settings) -> None:
        self._client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )

    def get_text(self, key: str) -> str | None:
        try:
            value = self._client.get(key)
            if value is None:
                return None
            return str(value)
        except redis.RedisError:
            logger.warning("redis_get_failed key=%s", key, exc_info=True)
            return None

    def set_text(self, key: str, value: str, ttl_seconds: int) -> None:
        try:
            self._client.setex(key, ttl_seconds, value)
        except redis.RedisError:
            logger.warning("redis_set_failed key=%s ttl=%d", key, ttl_seconds, exc_info=True)
