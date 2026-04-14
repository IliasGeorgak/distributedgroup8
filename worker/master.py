from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    IDLE = "idle"
    IN_PROGRESS = "in-progress"
    COMPLETED = "completed"

@dataclass(slots=True)
class TaskRecord:
    task_id: str
    task_type: str
    payload: dict[str, Any]
    state: TaskState = TaskState.IDLE
    worker_id: str | None = None


class Master:

    def __init__(self) -> None:
        self.map_tasks: dict[str, TaskRecord] = {}
        self.reduce_tasks: dict[str, TaskRecord] = {}

    def register_map_tasks(self, tasks: list[dict[str, Any]]) -> None:
        for task in tasks:
            task_id = str(task["task_id"])
            self.map_tasks[task_id] = TaskRecord(
                task_id=task_id,
                task_type="map",
                payload=dict(task),
            )

    def register_reduce_tasks(self, tasks: list[dict[str, Any]]) -> None:
        for task in tasks:
            task_id = str(task["task_id"])
            self.reduce_tasks[task_id] = TaskRecord(
                task_id=task_id,
                task_type="reduce",
                payload=dict(task),
            )

    def assign_task(self, task_type: str, worker_id: str) -> TaskRecord | None:
        tasks = self._task_registry(task_type)
        for task in tasks.values():
            if task.state == TaskState.IDLE:
                self.mark_status(
                    task_type=task.task_type,
                    task_id=task.task_id,
                    new_state=TaskState.IN_PROGRESS,
                    worker_id=worker_id,
                )
                return task
        return None


    def assign_map_task(self, worker_id: str) -> TaskRecord | None:
        return self.assign_task(task_type="map", worker_id=worker_id)

    def assign_reduce_task(self, worker_id: str) -> TaskRecord | None:
        return self.assign_task(task_type="reduce", worker_id=worker_id)

    def mark_status(
        self,
        task_type: str,
        task_id: str,
        new_state: TaskState | str,
        worker_id: str | None = None,
    ) -> None:
        task = self._get_task(task_type, task_id)
        target_state = TaskState(new_state)
        allowed_transitions: dict[TaskState, set[TaskState]] = {
            TaskState.IDLE: {TaskState.IN_PROGRESS},
            TaskState.IN_PROGRESS: {TaskState.IDLE, TaskState.COMPLETED},
            TaskState.COMPLETED: set(),
        }
        if target_state not in allowed_transitions[task.state]:
            raise ValueError(
                f"Invalid transition for task '{task_id}': "
                f"{task.state.value} -> {target_state.value}"
            )

        if target_state == TaskState.IN_PROGRESS:
            if not worker_id:
                raise ValueError("worker_id is required for transition to 'in-progress'")
            task.worker_id = worker_id
        elif target_state == TaskState.COMPLETED:
            if not worker_id:
                raise ValueError("worker_id is required for transition to 'completed'")
            if task.worker_id != worker_id:
                raise ValueError(
                    f"Task '{task_id}' is owned by worker '{task.worker_id}', not '{worker_id}'"
                )
        elif target_state == TaskState.IDLE:
            task.worker_id = None

        task.state = target_state

    def all_reduces_completed(self) -> bool:
        return all(task.state == TaskState.COMPLETED for task in self.reduce_tasks.values())

    def snapshot(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "map_tasks": [self._task_to_dict(task) for task in self.map_tasks.values()],
            "reduce_tasks": [self._task_to_dict(task) for task in self.reduce_tasks.values()],
        }

    def _assign_task(self, worker_id: str, tasks: dict[str, TaskRecord]) -> TaskRecord | None:
        for task in tasks.values():
            if task.state == TaskState.IDLE:
                task.state = TaskState.IN_PROGRESS
                task.worker_id = worker_id
                return task
        return None

    def _get_task(self, task_type: str, task_id: str) -> TaskRecord:
        tasks = self._task_registry(task_type)
        task = tasks.get(task_id)
        if task is None:
            raise ValueError(f"Unknown {task_type} task '{task_id}'")
        return task

    def _task_registry(self, task_type: str) -> dict[str, TaskRecord]:
        if task_type == "map":
            return self.map_tasks
        if task_type == "reduce":
            return self.reduce_tasks
        raise ValueError("task_type must be 'map' or 'reduce'")

    @staticmethod
    def _task_to_dict(task: TaskRecord) -> dict[str, Any]:
        return {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "state": task.state.value,
            "worker_id": task.worker_id,
            "payload": task.payload,
        }
