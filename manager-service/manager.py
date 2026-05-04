from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from db import Database
from scheduler import Scheduler
from storage import MinioStorage


class ManagerService:
    def __init__(self, database: Database | None = None, storage: MinioStorage | None = None) -> None:
        self.database = database or Database()
        self.storage = storage or MinioStorage()

    def schedule_pending_tasks(
        self,
        task_type: str = "map",
        launch_worker: Callable[[dict[str, Any]], None] | None = None,
    ) -> int:
        pending_tasks = self.database.get_pending_tasks(task_type)

        for task in pending_tasks:
            task_id = int(task["task_id"])
            self.database.update_task_status(task_id, "running")

            try:
                if launch_worker is None:
                    self.launch_worker_for_task(task)
                else:
                    launch_worker(task)
            except Exception:
                self.database.mark_task_failed(task_id)
                raise

        return len(pending_tasks)

    def scheduling_loop(
        self,
        task_type: str = "map",
        poll_interval_seconds: float = 5.0,
        launch_worker: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        while True:
            scheduled_count = self.schedule_pending_tasks(
                task_type=task_type,
                launch_worker=launch_worker,
            )
            if scheduled_count == 0:
                time.sleep(poll_interval_seconds)

    def launch_worker_for_task(self, task: dict[str, Any]) -> None:
        print(f"Launching worker for {task['task_type']} task {task['task_id']}")

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

    def split_input_file(
        self,
        file_path: str | Path,
        split_count: int,
        destination_dir: str | Path,
    ) -> list[Path]:
        if split_count <= 0:
            raise ValueError("split_count must be >= 1")

        source_path = Path(file_path)
        lines = source_path.read_text(encoding="utf-8").splitlines()
        total_lines = len(lines)

        actual_splits = 1 if total_lines == 0 else min(split_count, total_lines)
        base_size = total_lines // actual_splits if actual_splits else 0
        remainder = total_lines % actual_splits if actual_splits else 0

        split_dir = Path(destination_dir)
        split_dir.mkdir(parents=True, exist_ok=True)

        split_paths: list[Path] = []
        start = 0
        for split_id in range(actual_splits):
            size = base_size + (1 if split_id < remainder else 0)
            end = start + size
            chunk_lines = lines[start:end]
            split_path = split_dir / f"split_{split_id}.txt"
            split_path.write_text("\n".join(chunk_lines), encoding="utf-8")
            split_paths.append(split_path)
            start = end

        print(f"Split {source_path} into {len(split_paths)} chunks under {split_dir}")
        return split_paths

    def upload_split_files(
        self,
        bucket_name: str,
        split_paths: list[Path],
        object_prefix: str = "inputs",
    ) -> list[str]:
        object_names: list[str] = []
        normalized_prefix = object_prefix.strip("/")
        for split_path in split_paths:
            object_name = (
                f"{normalized_prefix}/{split_path.name}" if normalized_prefix else split_path.name
            )
            self.storage.upload_file(bucket_name, object_name, split_path)
            object_names.append(object_name)
            print(f"Uploaded split {split_path} to {bucket_name}/{object_name}")
        return object_names

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

    def build_map_tasks_for_splits(
        self,
        job_id: int,
        input_bucket: str,
        split_objects: list[str],
        output_bucket: str,
        map_parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        tasks: list[dict[str, Any]] = []
        for index, split_object in enumerate(split_objects, start=1):
            tasks.append(
                self.build_task_metadata(
                    task_id=f"job-{job_id}-map-{index}",
                    task_type="map",
                    input_bucket=input_bucket,
                    input_objects=[split_object],
                    output_bucket=output_bucket,
                    output_object=f"results/job-{job_id}-map-{index}.json",
                    parameters=map_parameters or {},
                )
            )
        return tasks

    def prepare_map_tasks_from_input_file(
        self,
        job_id: int,
        input_file: str | Path,
        input_bucket: str,
        output_bucket: str,
        split_count: int,
        split_dir: str | Path,
        split_object_prefix: str,
        map_parameters: dict[str, Any] | None = None,
    ) -> tuple[list[Path], list[str], list[dict[str, Any]]]:
        split_paths = self.split_input_file(
            file_path=input_file,
            split_count=split_count,
            destination_dir=split_dir,
        )
        split_objects = self.upload_split_files(
            bucket_name=input_bucket,
            split_paths=split_paths,
            object_prefix=split_object_prefix,
        )
        map_tasks = self.build_map_tasks_for_splits(
            job_id=job_id,
            input_bucket=input_bucket,
            split_objects=split_objects,
            output_bucket=output_bucket,
            map_parameters=map_parameters,
        )
        return split_paths, split_objects, map_tasks

    def write_task_file(self, destination: str | Path, metadata: dict[str, Any]) -> Path:
        destination_path = Path(destination)
        destination_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        print(f"Wrote task metadata to {destination_path}")
        return destination_path

    def run_map_stage(self, map_tasks: list[dict[str, Any]], worker_ids: list[str]) -> dict[str, Any]:
        scheduler = Scheduler(worker_ids=worker_ids)
        result = scheduler.run_map_stage(map_tasks)
        print(
            "Map stage completed:",
            f"{result['map_task_count']} tasks, {len(result['map_results'])} result files",
        )
        return result

    def run_reduce_stage(
        self,
        reduce_tasks: list[dict[str, Any]],
        worker_ids: list[str],
    ) -> dict[str, Any]:
        scheduler = Scheduler(worker_ids=worker_ids)
        result = scheduler.run_reduce_stage(reduce_tasks)
        print(
            "Reduce stage completed:",
            f"{result['reduce_task_count']} tasks, {len(result['reduce_results'])} result files",
        )
        return result

    def collect_shuffle_partition_objects(
        self,
        map_result_paths: list[str | Path],
    ) -> dict[int, list[str]]:
        grouped: dict[int, list[str]] = defaultdict(list)

        for result_path in map_result_paths:
            path = Path(result_path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            shuffle_payload = payload.get("shuffle")
            if not isinstance(shuffle_payload, dict):
                raise ValueError(f"Map output missing 'shuffle': {path}")

            partition_objects = shuffle_payload.get("partition_objects")
            if not isinstance(partition_objects, list):
                raise ValueError(f"'shuffle.partition_objects' must be a list: {path}")

            for entry in partition_objects:
                if not isinstance(entry, dict):
                    raise ValueError(f"Invalid shuffle partition entry in {path}")
                partition_id = int(entry["partition_id"])
                object_name = str(entry["object_name"])
                grouped[partition_id].append(object_name)

        return dict(grouped)

    def build_reduce_tasks_for_partitions(
        self,
        job_id: int,
        input_bucket: str,
        output_bucket: str,
        partition_objects: dict[int, list[str]],
        r_partitions: int,
        partition_function: str,
    ) -> list[dict[str, Any]]:
        reduce_tasks: list[dict[str, Any]] = []
        for partition_id in range(r_partitions):
            input_objects = partition_objects.get(partition_id, [])
            if not input_objects:
                continue

            reduce_tasks.append(
                self.build_task_metadata(
                    task_id=f"job-{job_id}-reduce-{partition_id}",
                    task_type="reduce",
                    input_bucket=input_bucket,
                    input_objects=input_objects,
                    output_bucket=output_bucket,
                    output_object=f"results/job-{job_id}-reduce-{partition_id}.json",
                    parameters={
                        "r_partitions": r_partitions,
                        "partition_function": partition_function,
                        "reduce_partition_id": partition_id,
                    },
                )
            )

        return reduce_tasks

    def create_reduce_tasks_from_shuffle_outputs(
        self,
        job_id: int,
        input_bucket: str,
        output_bucket: str,
        map_result_paths: list[str | Path],
        r_partitions: int,
        partition_function: str,
    ) -> tuple[dict[int, list[str]], list[dict[str, Any]]]:
        partition_objects = self.collect_shuffle_partition_objects(map_result_paths)
        reduce_tasks = self.build_reduce_tasks_for_partitions(
            job_id=job_id,
            input_bucket=input_bucket,
            output_bucket=output_bucket,
            partition_objects=partition_objects,
            r_partitions=r_partitions,
            partition_function=partition_function,
        )
        return partition_objects, reduce_tasks

    @staticmethod
    def _stage_completed(snapshot: dict[str, list[dict[str, Any]]], task_type: str) -> bool:
        key = "map_tasks" if task_type == "map" else "reduce_tasks"
        tasks = snapshot.get(key, [])
        return bool(tasks) and all(task.get("state") == "completed" for task in tasks)

    def run_pipeline(
        self,
        job_id: int,
        input_file: str | Path,
        bucket_name: str,
        worker_ids: list[str],
        split_count: int,
        r_partitions: int,
        partition_function: str = "md5",
        case_sensitive: bool = False,
    ) -> dict[str, Any]:
        split_dir = Path(__file__).with_name("splits")
        worker_dir = Path(__file__).resolve().parent.parent / "worker"

        # Stage 1: create + assign + run map tasks
        _, _, map_tasks = self.prepare_map_tasks_from_input_file(
            job_id=job_id,
            input_file=input_file,
            input_bucket=bucket_name,
            output_bucket=bucket_name,
            split_count=split_count,
            split_dir=split_dir,
            split_object_prefix=f"inputs/job-{job_id}",
            map_parameters={
                "case_sensitive": case_sensitive,
                "r_partitions": r_partitions,
                "partition_function": partition_function,
            },
        )
        for index, task_metadata in enumerate(map_tasks, start=1):
            self.write_task_file(worker_dir / f"task.map.{index}.json", task_metadata)
        (worker_dir / "task.maps.json").write_text(json.dumps(map_tasks, indent=2), encoding="utf-8")

        map_stage_result = self.run_map_stage(map_tasks=map_tasks, worker_ids=worker_ids)
        (worker_dir / "map.stage.result.json").write_text(json.dumps(map_stage_result, indent=2), encoding="utf-8")

        if not self._stage_completed(map_stage_result["snapshot"], "map"):
            raise RuntimeError("Map stage did not fully complete; refusing to move to reduce stage")

        # Stage 2: create + assign + run reduce tasks from shuffle outputs
        partition_objects, reduce_tasks = self.create_reduce_tasks_from_shuffle_outputs(
            job_id=job_id,
            input_bucket=bucket_name,
            output_bucket=bucket_name,
            map_result_paths=map_stage_result["map_results"],
            r_partitions=r_partitions,
            partition_function=partition_function,
        )
        (worker_dir / "shuffle.partition.objects.json").write_text(
            json.dumps(partition_objects, indent=2), encoding="utf-8"
        )
        for index, task_metadata in enumerate(reduce_tasks, start=1):
            self.write_task_file(worker_dir / f"task.reduce.{index}.json", task_metadata)
        (worker_dir / "task.reduces.json").write_text(json.dumps(reduce_tasks, indent=2), encoding="utf-8")

        reduce_stage_result = self.run_reduce_stage(reduce_tasks=reduce_tasks, worker_ids=worker_ids)
        (worker_dir / "reduce.stage.result.json").write_text(
            json.dumps(reduce_stage_result, indent=2), encoding="utf-8"
        )

        if reduce_tasks and not self._stage_completed(reduce_stage_result["snapshot"], "reduce"):
            raise RuntimeError("Reduce stage did not fully complete")

        return {
            "job_id": job_id,
            "map_task_count": len(map_tasks),
            "reduce_task_count": len(reduce_tasks),
            "map_stage_result": map_stage_result,
            "reduce_stage_result": reduce_stage_result,
        }


def main() -> None:
    manager = ManagerService()
    bucket_name = "betet"
    input_file = Path(__file__).with_name("test_input.txt")
    split_count = 4
    worker_ids = ["worker-1", "worker-2", "worker-3"]
    r_partitions = 3
    partition_function = "md5"

    job_id = manager.bootstrap(default_bucket=bucket_name, seed_task_count=split_count)
    pipeline_result = manager.run_pipeline(
        job_id=job_id,
        input_file=input_file,
        bucket_name=bucket_name,
        worker_ids=worker_ids,
        split_count=split_count,
        r_partitions=r_partitions,
        partition_function=partition_function,
        case_sensitive=False,
    )
    print(
        "Pipeline completed:",
        f"{pipeline_result['map_task_count']} map tasks, {pipeline_result['reduce_task_count']} reduce tasks",
    )


if __name__ == "__main__":
    main()
