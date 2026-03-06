from __future__ import annotations

from minio import Minio

from web.config import Settings


class MinioStore:
    def __init__(self, settings: Settings) -> None:
        self._bucket = settings.minio_bucket
        self._client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )

    def read_slice(self, object_key: str, offset: int, length: int) -> bytes:
        response = self._client.get_object(
            bucket_name=self._bucket,
            object_name=object_key,
            offset=offset,
            length=length,
        )
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
