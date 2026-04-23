from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
WORKER_DIR = ROOT_DIR / "worker"
if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

from master import Master, TaskRecord, TaskState
from worker import Worker


class Scheduler:
    def __init__(self, worker_ids: list[str]) -> None:
        if not worker_ids:
            raise ValueError("worker_ids must be a non-empty list")
        self.master = Master()
        self.workers = [Worker(worker_id=worker_id) for worker_id in worker_ids]

    def run_job(
        self,
        job_id: str,
        input_bucket: str,
        input_objects: list[str],
        output_bucket: str,
        m_splits: int = 1,
        r_partitions: int = 1,
        partition_function: str = "md5",
        case_sensitive: bool = False,
    ) -> dict[str, Any]:
        map_tasks = self._build_map_tasks(
            job_id=job_id,
            input_bucket=input_bucket,
            input_objects=input_objects,
            output_bucket=output_bucket,
            m_splits=m_splits,
            r_partitions=r_partitions,
            partition_function=partition_function,
            case_sensitive=case_sensitive,
        )
        self.master.register_map_tasks(map_tasks)
        map_results = self._drain_phase(task_type="map")

        partition_input_objects = self._collect_shuffle_partition_objects(map_results)
        reduce_tasks = self._build_reduce_tasks(
            job_id=job_id,
            input_bucket=output_bucket,
            output_bucket=output_bucket,
            partition_input_objects=partition_input_objects,
            r_partitions=r_partitions,
            partition_function=partition_function,
        )
        self.master.register_reduce_tasks(reduce_tasks)
        reduce_results = self._drain_phase(task_type="reduce")

        return {
            "job_id": job_id,
            "map_task_count": len(map_tasks),
            "reduce_task_count": len(reduce_tasks),
            "map_results": [str(path) for path in map_results],
            "reduce_results": [str(path) for path in reduce_results],
            "snapshot": self.master.snapshot(),
        }

    def _drain_phase(self, task_type: str) -> list[Path]:
        results: list[Path] = []

        while not self._phase_completed(task_type):
            progress_made = False

            for worker in self.workers:
                if task_type == "map":
                    output_path = worker.run_assigned_map_task(self.master)
                elif task_type == "reduce":
                    output_path = worker.run_assigned_reduce_task(self.master)
                else:
                    raise ValueError("task_type must be 'map' or 'reduce'")

                if output_path is not None:
                    progress_made = True
                    results.append(output_path)

            if not progress_made:
                raise RuntimeError(f"No progress made while draining {task_type} phase")

        return results

    def _phase_completed(self, task_type: str) -> bool:
        tasks = self._task_registry(task_type)
        return bool(tasks) and all(task.state == TaskState.COMPLETED for task in tasks.values())

    def _task_registry(self, task_type: str) -> dict[str, TaskRecord]:
        if task_type == "map":
            return self.master.map_tasks
        if task_type == "reduce":
            return self.master.reduce_tasks
        raise ValueError("task_type must be 'map' or 'reduce'")

    def _build_map_tasks(
        self,
        job_id: str,
        input_bucket: str,
        input_objects: list[str],
        output_bucket: str,
        m_splits: int,
        r_partitions: int,
        partition_function: str,
        case_sensitive: bool,
    ) -> list[dict[str, Any]]:
        if not input_objects:
            raise ValueError("input_objects must be non-empty")

        tasks: list[dict[str, Any]] = []
        for index, object_name in enumerate(input_objects, start=1):
            tasks.append(
                {
                    "task_id": f"{job_id}-map-{index}",
                    "task_type": "map",
                    "input_bucket": input_bucket,
                    "input_objects": [object_name],
                    "output_bucket": output_bucket,
                    "output_object": f"results/{job_id}-map-{index}.json",
                    "parameters": {
                        "m_splits": m_splits,
                        "r_partitions": r_partitions,
                        "partition_function": partition_function,
                        "case_sensitive": case_sensitive,
                    },
                }
            )
        return tasks

    def _collect_shuffle_partition_objects(self, map_result_paths: list[Path]) -> dict[int, list[str]]:
        partition_inputs: dict[int, list[str]] = defaultdict(list)

        for result_path in map_result_paths:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            shuffle_payload = payload.get("shuffle")
            if not isinstance(shuffle_payload, dict):
                raise ValueError(f"Map result is missing 'shuffle' payload: {result_path}")

            partition_objects = shuffle_payload.get("partition_objects")
            if not isinstance(partition_objects, list):
                raise ValueError(f"'shuffle.partition_objects' must be a list: {result_path}")

            for entry in partition_objects:
                if not isinstance(entry, dict):
                    raise ValueError(f"Each shuffle partition entry must be an object: {result_path}")
                partition_id = int(entry["partition_id"])
                object_name = str(entry["object_name"])
                partition_inputs[partition_id].append(object_name)

        return partition_inputs

    def _build_reduce_tasks(
        self,
        job_id: str,
        input_bucket: str,
        output_bucket: str,
        partition_input_objects: dict[int, list[str]],
        r_partitions: int,
        partition_function: str,
    ) -> list[dict[str, Any]]:
        tasks: list[dict[str, Any]] = []
        for partition_id in range(r_partitions):
            input_objects = partition_input_objects.get(partition_id, [])
            if not input_objects:
                continue

            tasks.append(
                {
                    "task_id": f"{job_id}-reduce-{partition_id}",
                    "task_type": "reduce",
                    "input_bucket": input_bucket,
                    "input_objects": input_objects,
                    "output_bucket": output_bucket,
                    "output_object": f"results/{job_id}-reduce-{partition_id}.json",
                    "parameters": {
                        "r_partitions": r_partitions,
                        "partition_function": partition_function,
                        "reduce_partition_id": partition_id,
                    },
                }
            )

        return tasks
