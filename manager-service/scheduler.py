from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from master import Master, TaskRecord, TaskState
from k8 import Kuber   

class Scheduler:
    def __init__(
        self,
        master: Master | None = None,
        kuber: Kuber | None = None,
        database: Any | None = None,
        max_task_attempts: int = 3,
    ) -> None:
        self.master = master or Master()
        self.kuber = kuber or Kuber()
        self.database = database
        self.max_task_attempts = max(1, max_task_attempts)

    def execute_task(self, task:TaskRecord) -> bool:
       return self.kuber.exec(task)
        
    
    def run_job(
        self,
        job_id: str,
        input_bucket: str,
        input_objects: list[str],
        output_bucket: str,
        m_splits: int = 1,
        r_partitions: int = 1,
        partition_function: str = "sha256",
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

    def run_map_stage(self, map_tasks: list[dict[str, Any]]) -> dict[str, Any]:
        if not map_tasks:
            raise ValueError("map_tasks must be non-empty")

        self.master.register_map_tasks(map_tasks)
        map_results = self._drain_phase(task_type="map")
        return {
            "map_task_count": len(map_tasks),
            "map_results": [str(path) for path in map_results],
            "snapshot": self.master.snapshot(),
        }

    def run_reduce_stage(self, reduce_tasks: list[dict[str, Any]]) -> dict[str, Any]:
        if not reduce_tasks:
            raise ValueError("reduce_tasks must be non-empty")

        self.master.register_reduce_tasks(reduce_tasks)
        reduce_results = self._drain_phase(task_type="reduce")
        return {
            "reduce_task_count": len(reduce_tasks),
            "reduce_results": [str(path) for path in reduce_results],
            "snapshot": self.master.snapshot(),
        }

    # def _drain_phase(self, task_type: str) -> list[Path]:
    #     results: list[Path] = []

    #     while not self._phase_completed(task_type):
    #         progress_made = False
    #         if task_type == "map":
    #             task_rec = self.master.assign_map_task()
    #         elif task_type == "reduce":
    #             task_rec = self.master.assign_reduce_task()
    #         else:
    #             raise ValueError("task_type must be 'map' or 'reduce'")

    #         if self.execute_task(task_rec):
    #                 progress_made = True
    #                 results.append(output_path)

    #         if not progress_made:
    #             raise RuntimeError(f"No progress made while draining {task_type} phase")

    #     return results

    def _drain_phase(self, task_type: str) -> list[str]:
        results = []
        attempts_by_task_id: dict[str, int] = defaultdict(int)

        while not self._phase_completed(task_type):
            task_rec = (
                self.master.assign_map_task()
                if task_type == "map"
                else self.master.assign_reduce_task()
            )

            if task_rec is None:
                raise RuntimeError(f"No available {task_type} task")

            self._mark_task_running(task_rec)

            try:
                task_succeeded = self.execute_task(task_rec)
            except Exception:
                task_succeeded = False

            if not task_succeeded:
                attempts_by_task_id[task_rec.task_id] += 1
                attempts = attempts_by_task_id[task_rec.task_id]
                self._record_task_failure(task_rec, attempts)

                if attempts < self.max_task_attempts:
                    self.master.mark_status(
                        task_type,
                        task_rec.task_id,
                        TaskState.IDLE,
                    )
                    continue

                raise RuntimeError(
                    f"{task_type} task {task_rec.task_id} failed after "
                    f"{self.max_task_attempts} attempts"
                )

            self.master.mark_status(
                task_type,
                task_rec.task_id,
                TaskState.COMPLETED,
                task_rec.worker_id,
            )
            self._mark_task_completed(task_rec)

            results.append(str(task_rec.payload["output_object"]))

        return results

    def _mark_task_running(self, task_rec: TaskRecord) -> None:
        if self.database is None:
            return
        self.database.mark_task_running_by_external_id(
            external_task_id=task_rec.task_id,
            worker_id=task_rec.worker_id,
        )

    def _mark_task_completed(self, task_rec: TaskRecord) -> None:
        if self.database is None:
            return
        self.database.mark_task_completed_by_external_id(
            external_task_id=task_rec.task_id,
            worker_id=task_rec.worker_id,
        )

    def _record_task_failure(self, task_rec: TaskRecord, attempts: int) -> None:
        if self.database is None:
            return
        self.database.record_task_failure_by_external_id(
            external_task_id=task_rec.task_id,
            worker_id=task_rec.worker_id,
            max_attempts=self.max_task_attempts,
        )


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
