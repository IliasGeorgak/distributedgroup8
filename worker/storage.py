from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from minio import Minio
import os


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class MinioConfig:
    endpoint: str = os.environ["MINIO_SERVER_URL"]
    access_key: str = os.environ["MINIO_ROOT_USER"]
    secret_key: str = os.environ["MINIO_ROOT_PASSWORD"]
    secure: bool = _env_bool("MINIO_SECURE", False)


config = MinioConfig()

client = Minio(
    config.endpoint,
    access_key=config.access_key,
    secret_key=config.secret_key,
    secure=config.secure,
)


def ensure_bucket(bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def download_object(bucket_name: str, object_name: str, destination: str | Path) -> Path:
    destination_path = Path(destination)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    client.fget_object(bucket_name, object_name, str(destination_path))
    return destination_path


def upload_file(bucket_name: str, object_name: str, file_path: str | Path) -> None:
    ensure_bucket(bucket_name)
    client.fput_object(bucket_name, object_name, str(file_path))
