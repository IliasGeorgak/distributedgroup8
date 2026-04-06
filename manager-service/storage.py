from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from minio import Minio


@dataclass(slots=True)
class MinioConfig:
    endpoint: str = "localhost:9000"
    access_key: str = "admin"
    secret_key: str = "password123"
    secure: bool = False


class MinioStorage:
    def __init__(self, config: MinioConfig | None = None) -> None:
        self.config = config or MinioConfig()
        self.client = Minio(
            self.config.endpoint,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            secure=self.config.secure,
        )

    def list_bucket_names(self) -> list[str]:
        return [bucket.name for bucket in self.client.list_buckets()]

    def ensure_bucket(self, bucket_name: str) -> None:
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def upload_file(self, bucket_name: str, object_name: str, file_path: str | Path) -> None:
        self.ensure_bucket(bucket_name)
        self.client.fput_object(bucket_name, object_name, str(file_path))
