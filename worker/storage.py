from __future__ import annotations

from pathlib import Path

from minio import Minio


client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False,
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
