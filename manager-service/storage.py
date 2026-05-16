from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from minio import Minio
import os


@dataclass(slots=True)
class MinioConfig:
    """endpoint: str = "localhost:9000"
    access_key: str = "admin"
    secret_key: str = "password123"
    secure: bool = False  """
    endpoint: str = os.environ["MINIO_SERVER_URL"]
    access_key: str = os.environ["MINIO_ROOT_USER"]
    secret_key: str = os.environ["MINIO_ROOT_PASSWORD"]
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

    def list_objects(self, bucket_name: str, prefix: str) -> list[str]:
        objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects if obj.object_name is not None]

    def download_json(self, bucket_name: str, object_name: str) -> dict[str, Any]:
        response = self.client.get_object(bucket_name, object_name)
        try:
            raw_content = response.read().decode("utf-8")
            payload = json.loads(raw_content)
        finally:
            response.close()
            response.release_conn()

        if not isinstance(payload, dict):
            raise ValueError(f"Object '{object_name}' does not contain a JSON object")
        return payload
