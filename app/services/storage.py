from datetime import timedelta
from io import BytesIO
from pathlib import Path
from urllib.parse import quote
from urllib.parse import urlparse

from minio import Minio

from app.settings import settings


class ObjectStorage:
    def __init__(self) -> None:
        self.backend = settings.object_storage_backend

    def storage_key(self, contest_id: str, category: str, filename: str) -> str:
        safe_filename = filename.replace("/", "_")
        return f"contests/{contest_id}/{category}/{safe_filename}"

    def presigned_put_url(self, storage_key: str) -> str:
        if self.backend == "minio":
            return self._browser_proxy_url(storage_key)
        return self._local_file_url(storage_key)

    def presigned_get_url(self, storage_key: str) -> str:
        if self.backend == "minio":
            return self._browser_proxy_url(storage_key)
        return self._local_file_url(storage_key)

    def read_bytes(self, storage_key: str) -> bytes:
        if self.backend == "minio":
            response = self._client().get_object(settings.object_storage_bucket, storage_key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()
        path = Path(settings.local_object_storage_root) / storage_key
        return path.read_bytes()

    def read_text(self, storage_key: str) -> str:
        return self.read_bytes(storage_key).decode("utf-8")

    def write_bytes(self, storage_key: str, content: bytes, content_type: str = "application/octet-stream") -> None:
        if self.backend == "minio":
            self._client().put_object(
                settings.object_storage_bucket,
                storage_key,
                BytesIO(content),
                length=len(content),
                content_type=content_type,
            )
            return
        path = Path(settings.local_object_storage_root) / storage_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    def write_text(self, storage_key: str, content: str, content_type: str = "text/plain") -> None:
        self.write_bytes(storage_key, content.encode("utf-8"), content_type)

    def delete(self, storage_key: str) -> None:
        if self.backend == "minio":
            self._client().remove_object(settings.object_storage_bucket, storage_key)
            return
        path = Path(settings.local_object_storage_root) / storage_key
        if path.exists():
            path.unlink()

    def _client(self) -> Minio:
        parsed = urlparse(settings.object_storage_endpoint)
        endpoint = parsed.netloc or parsed.path
        secure = settings.object_storage_secure or parsed.scheme == "https"
        return Minio(
            endpoint,
            access_key=settings.object_storage_access_key,
            secret_key=settings.object_storage_secret_key,
            secure=secure,
        )

    def _local_file_url(self, storage_key: str) -> str:
        path = Path(settings.local_object_storage_root) / storage_key
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"file://{quote(str(path))}"

    def _browser_proxy_url(self, storage_key: str) -> str:
        return f"/api/storage/objects/{quote(storage_key, safe='/')}"


object_storage = ObjectStorage()
