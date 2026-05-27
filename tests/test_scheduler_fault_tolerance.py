from pathlib import Path
import sys
import types

import pytest

MANAGER_SERVICE_PATH = str(Path(__file__).resolve().parents[1] / "manager-service")
sys.path.insert(0, MANAGER_SERVICE_PATH)

fake_k8 = types.ModuleType("k8")
fake_k8.Kuber = object
sys.modules["k8"] = fake_k8

from scheduler import Scheduler

sys.path.remove(MANAGER_SERVICE_PATH)
sys.modules.pop("master", None)
sys.modules.pop("k8", None)


class FakeKuber:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def exec(self, task):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeDatabase:
    def __init__(self):
        self.events = []

    def mark_task_running_by_external_id(self, external_task_id, worker_id):
        self.events.append(("running", external_task_id, worker_id))

    def mark_task_completed_by_external_id(self, external_task_id, worker_id):
        self.events.append(("completed", external_task_id, worker_id))

    def record_task_failure_by_external_id(self, external_task_id, worker_id, max_attempts):
        self.events.append(("failed-attempt", external_task_id, worker_id, max_attempts))


class FakeParallelKuber:
    def __init__(self):
        self.events = []

    def submit_task(self, task):
        self.events.append(("submit", task.task_id))
        return f"k8s-{task.task_id}"

    def wait_for_tasks(self, task_jobs):
        self.events.append(("wait", tuple(task_jobs)))
        return {task_id: True for task_id in task_jobs}


def _map_task(task_id="job-1-map-1"):
    return {
        "task_id": task_id,
        "task_type": "map",
        "input_bucket": "mapreduce",
        "input_objects": ["inputs/split_0.txt"],
        "output_bucket": "mapreduce",
        "output_object": f"results/{task_id}.json",
        "parameters": {},
    }


def test_scheduler_submits_all_phase_tasks_before_waiting():
    kuber = FakeParallelKuber()
    scheduler = Scheduler(kuber=kuber)

    scheduler.run_map_stage([
        _map_task("job-1-map-1"),
        _map_task("job-1-map-2"),
        _map_task("job-1-map-3"),
    ])

    assert kuber.events == [
        ("submit", "job-1-map-1"),
        ("submit", "job-1-map-2"),
        ("submit", "job-1-map-3"),
        ("wait", ("job-1-map-1", "job-1-map-2", "job-1-map-3")),
    ]


def test_scheduler_retries_failed_task_before_completing():
    kuber = FakeKuber([False, True])
    database = FakeDatabase()
    scheduler = Scheduler(
        kuber=kuber,
        database=database,
        max_task_attempts=2,
    )

    result = scheduler.run_map_stage([_map_task()])

    assert kuber.calls == 2
    assert result["map_results"] == ["results/job-1-map-1.json"]
    assert result["snapshot"]["map_tasks"][0]["state"] == "completed"
    assert [event[0] for event in database.events] == [
        "running",
        "failed-attempt",
        "running",
        "completed",
    ]


def test_scheduler_fails_task_after_attempt_limit():
    kuber = FakeKuber([False, RuntimeError("pod failed")])
    database = FakeDatabase()
    scheduler = Scheduler(
        kuber=kuber,
        database=database,
        max_task_attempts=2,
    )

    with pytest.raises(RuntimeError, match="failed after 2 attempts"):
        scheduler.run_map_stage([_map_task()])

    assert kuber.calls == 2
    assert [event[0] for event in database.events] == [
        "running",
        "failed-attempt",
        "running",
        "failed-attempt",
    ]
