from pathlib import Path
import sys
import types

sys.path.append(str(Path(__file__).resolve().parents[1] / "worker"))


class StubMinio:
    def __init__(self, *args, **kwargs):
        pass


sys.modules.setdefault("minio", types.SimpleNamespace(Minio=StubMinio))

import storage


class FakeMinioClient:
    def __init__(self):
        self.buckets = set()
        self.uploads = []
        self.downloads = []

    def bucket_exists(self, bucket_name):
        return bucket_name in self.buckets

    def make_bucket(self, bucket_name):
        self.buckets.add(bucket_name)

    def fput_object(self, bucket_name, object_name, file_path):
        self.uploads.append((bucket_name, object_name, file_path))

    def fget_object(self, bucket_name, object_name, destination):
        self.downloads.append((bucket_name, object_name, destination))


def test_storage_upload_file_creates_bucket_and_uploads(monkeypatch, tmp_path):
    fake_client = FakeMinioClient()
    monkeypatch.setattr(storage, "client", fake_client)

    file_path = tmp_path / "map-1.json"
    file_path.write_text('{"hello": 1}', encoding="utf-8")

    storage.upload_file("intermediate", "map-1.json", file_path)

    assert "intermediate" in fake_client.buckets
    assert fake_client.uploads == [
        ("intermediate", "map-1.json", str(file_path))
    ]


def test_storage_download_object_returns_destination_path(monkeypatch, tmp_path):
    fake_client = FakeMinioClient()
    monkeypatch.setattr(storage, "client", fake_client)

    destination = tmp_path / "nested" / "input.txt"

    result = storage.download_object("data", "input.txt", destination)

    assert result == destination
    assert destination.parent.exists()
    assert fake_client.downloads == [
        ("data", "input.txt", str(destination))
    ]
