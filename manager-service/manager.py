from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from db import Database
from storage import MinioStorage


class ManagerService:
    def __init__(self, database: Database | None = None, storage: MinioStorage | None = None) -> None:
        self.database = database or Database()
        self.storage = storage or MinioStorage()

    def bootstrap(self, default_bucket: str, seed_task_count: int = 3) -> int:
        db_test_result = self.database.test_connection()
        print("Connected to database:", db_test_result)

        self.database.init_schema()
        print("Database initialized")

        job_id = self.database.create_job()
        self.database.create_tasks(job_id, seed_task_count)
        print(f"Created job {job_id} with {seed_task_count} tasks")

        self.storage.ensure_bucket(default_bucket)
        print(f"Ensured bucket exists: {default_bucket}")
        print("Available buckets:", self.storage.list_bucket_names())
        return job_id

    def upload_input_file(self, bucket_name: str, object_name: str, file_path: str | Path) -> None:
        source_path = Path(file_path)
        self.storage.upload_file(bucket_name, object_name, source_path)
        print(f"Uploaded {source_path} to {bucket_name}/{object_name}")

    def build_task_metadata(
        self,
        task_id: str,
        task_type: str,
        input_bucket: str,
        input_objects: list[str],
        output_bucket: str,
        output_object: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "task_type": task_type,
            "input_bucket": input_bucket,
            "input_objects": input_objects,
            "output_bucket": output_bucket,
            "output_object": output_object,
            "parameters": parameters or {},
        }

    def write_task_file(self, destination: str | Path, metadata: dict[str, Any]) -> Path:
        destination_path = Path(destination)
        destination_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        print(f"Wrote task metadata to {destination_path}")
        return destination_path


def main() -> None:
    manager = ManagerService()
    bucket_name = "betet"
    input_file = Path(__file__).with_name("test_input.txt")
    input_object = "test_input.txt"

    job_id = manager.bootstrap(default_bucket=bucket_name)
    manager.upload_input_file(bucket_name, input_object, input_file)

    task_metadata = manager.build_task_metadata(
        task_id=f"job-{job_id}-map-1",
        task_type="map",
        input_bucket=bucket_name,
        input_objects=[input_object],
        output_bucket=bucket_name,
        output_object=f"results/job-{job_id}-map-1.json",
        parameters={"case_sensitive": False},
    )
    task_file = Path(__file__).resolve().parent.parent / "worker" / "task.example.json"
    manager.write_task_file(task_file, task_metadata)


if __name__ == "__main__":
    main()
