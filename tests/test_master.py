from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "worker"))

from master import Master, TaskState


def test_master_registers_and_assigns_map_task():
    master = Master()

    master.register_map_tasks([
        {
            "task_id": "map-1",
            "task_type": "map",
            "input_bucket": "data",
            "input_objects": ["Split-1.txt"],
            "output_bucket": "intermediate",
            "output_objects": "map-1.json",
            "parameters": {},
        }
    ])

    task = master.assign_map_task(worker_id="worker-1")

    assert task is not None
    assert task.task_id == "map-1"
    assert task.state == TaskState.IN_PROGRESS
    assert task.worker_id == "worker-1"


def test_master_completes_assigned_task():
    master = Master()

    master.register_map_tasks([
        {"task_id": "map-1", "task_type": "map"}
    ])

    task = master.assign_map_task(worker_id="worker-1")
    assert task is not None

    master.mark_status(
        task_type="map",
        task_id=task.task_id,
        new_state=TaskState.COMPLETED,
        worker_id="worker-1",
    )

    snapshot = master.snapshot()

    assert snapshot["map_tasks"][0]["state"] == "completed"
    assert snapshot["map_tasks"][0]["worker_id"] == "worker-1"


def test_master_returns_none_when_no_idle_tasks():
    master = Master()

    master.register_map_tasks([
        {"task_id": "map-1", "task_type": "map"}
    ])

    master.assign_map_task(worker_id="worker-1")

    next_task = master.assign_map_task(worker_id="worker-2")

    assert next_task is None


def test_master_resets_to_idle_after_failure():
    master = Master()

    master.register_map_tasks([
        {"task_id": "map-1", "task_type": "map"}
    ])

    task = master.assign_map_task(worker_id="worker-1")
    assert task is not None

    master.mark_status(
        task_type="map",
        task_id=task.task_id,
        new_state=TaskState.IDLE,
    )

    snapshot = master.snapshot()

    assert snapshot["map_tasks"][0]["state"] == "idle"
    assert snapshot["map_tasks"][0]["worker_id"] is None


def test_master_rejects_invalid_transition():
    master = Master()

    master.register_map_tasks([
        {"task_id": "map-1", "task_type": "map"}
    ])

    with pytest.raises(ValueError):
        master.mark_status(
            task_type="map",
            task_id="map-1",
            new_state=TaskState.COMPLETED,
            worker_id="worker-1",
        )
