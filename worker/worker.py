from __future__ import annotations

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from master import Master, TaskState
from mapper import map_to_key_value_pairs
from reducer import PARTITION_FUNCTIONS, reduce_partitioned_word_count
from storage import download_object, upload_file
from task import TaskMetadata


TASK_HANDLERS = {
    "map": map_to_key_value_pairs,
    "reduce": reduce_partitioned_word_count,
}


class Worker:
    def __init__(self, worker_id: str = "worker-local") -> None:
        self.worker_id = worker_id

    def run_task(self, metadata: TaskMetadata) -> Path:
        task_handler = TASK_HANDLERS.get(metadata.task_type)
        if task_handler is None:
            supported = ", ".join(sorted(TASK_HANDLERS))
            raise ValueError(f"Unsupported task_type '{metadata.task_type}'. Supported: {supported}")

        with TemporaryDirectory(prefix=f"task-{metadata.task_id}-") as temp_dir:
            working_dir = Path(temp_dir)
            input_paths = self._download_inputs(metadata, working_dir)
            task_result = task_handler(input_paths, metadata.parameters)
            if metadata.task_type == "map":
                task_result = self._shuffle_map_output(task_result, metadata, working_dir)

            output_path = working_dir / "result.json"
            output_path.write_text(json.dumps(task_result, indent=2, sort_keys=True), encoding="utf-8")
            upload_file(metadata.output_bucket, metadata.output_object, output_path)

            persisted_output = Path.cwd() / f"{metadata.task_id}-result.json"
            persisted_output.write_text(output_path.read_text(encoding="utf-8"), encoding="utf-8")
            return persisted_output

    def _shuffle_map_output(
        self,
        map_result: dict[str, object],
        metadata: TaskMetadata,
        working_dir: Path,
    ) -> dict[str, object]:
        raw_pairs = map_result.get("intermediate_pairs")
        if not isinstance(raw_pairs, list):
            raise ValueError("Mapper output must include 'intermediate_pairs'")

        r_partitions = int(metadata.parameters.get("r_partitions", 1))
        if r_partitions <= 0:
            raise ValueError("'r_partitions' must be >= 1")

        partition_function_name = str(metadata.parameters.get("partition_function", "md5")).lower()
        hash_function = PARTITION_FUNCTIONS.get(partition_function_name)
        if hash_function is None:
            supported = ", ".join(sorted(PARTITION_FUNCTIONS))
            raise ValueError(
                f"Unsupported partition_function '{partition_function_name}'. Supported: {supported}"
            )

        partitioned_pairs: list[list[list[object]]] = [[] for _ in range(r_partitions)]
        for pair in raw_pairs:
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError("Each mapper intermediate pair must be [key, value]")
            key, value = pair
            partition_id = hash_function(str(key)) % r_partitions
            partitioned_pairs[partition_id].append([str(key), int(value)])

        base_output_object = Path(metadata.output_object)
        object_stem = base_output_object.stem
        object_parent = "" if str(base_output_object.parent) == "." else f"{base_output_object.parent}/"

        partition_objects: list[dict[str, object]] = []
        for partition_id, pairs in enumerate(partitioned_pairs):
            partition_payload = {
                "map_task_id": metadata.task_id,
                "partition_id": partition_id,
                "intermediate_pairs": pairs,
            }
            local_partition_path = working_dir / f"shuffle-partition-{partition_id}.json"
            local_partition_path.write_text(
                json.dumps(partition_payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            object_name = f"{object_parent}{object_stem}-shuffle-part-{partition_id}.json"
            upload_file(metadata.output_bucket, object_name, local_partition_path)
            partition_objects.append(
                {
                    "partition_id": partition_id,
                    "object_name": object_name,
                    "pair_count": len(pairs),
                }
            )

        enriched_result = dict(map_result)
        enriched_result["shuffle"] = {
            "r_partitions": r_partitions,
            "partition_function": partition_function_name,
            "partition_objects": partition_objects,
        }
        return enriched_result

    def run_assigned_map_task(self, master: Master) -> Path | None:
        return self._run_assigned_task(master, task_type="map")

    def run_assigned_reduce_task(self, master: Master) -> Path | None:
        return self._run_assigned_task(master, task_type="reduce")

    def _run_assigned_task(self, master: Master, task_type: str) -> Path | None:
        task = master.assign_task(task_type=task_type, worker_id=self.worker_id)

        if task is None:
            return None

        metadata = TaskMetadata.from_dict(task.payload)
        try:
            output_path = self.run_task(metadata)
        except Exception:
            master.mark_status(task_type=task_type, task_id=metadata.task_id, new_state=TaskState.IDLE)
            raise

        master.mark_status(
            task_type=task_type,
            task_id=metadata.task_id,
            new_state=TaskState.COMPLETED,
            worker_id=self.worker_id,
        )
        return output_path

    def _download_inputs(self, metadata: TaskMetadata, working_dir: Path) -> list[Path]:
        input_paths: list[Path] = []
        for index, object_name in enumerate(metadata.input_objects, start=1):
            local_path = working_dir / f"input-{index}{Path(object_name).suffix}"
            downloaded_path = download_object(metadata.input_bucket, object_name, local_path)
            input_paths.append(downloaded_path)
        return input_paths


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a MapReduce worker task from JSON or YAML metadata.")
    parser.add_argument("--task-file", help="Path to a JSON or YAML file containing task metadata.")
    parser.add_argument("--task-json", help="Raw JSON string containing task metadata.")
    parser.add_argument("--worker-id", default="worker-local", help="Worker machine identity.")
    return parser


def _load_task_metadata(args: argparse.Namespace) -> TaskMetadata:
    if args.task_file:
        return TaskMetadata.from_file(args.task_file)
    if args.task_json:
        return TaskMetadata.from_json(args.task_json)
    raise ValueError("Provide either --task-file or --task-json")


def main() -> None:
    args = _build_parser().parse_args()
    metadata = _load_task_metadata(args)
    worker = Worker(worker_id=args.worker_id)
    output_path = worker.run_task(metadata)
    print(f"Task {metadata.task_id} completed. Local result: {output_path}")


if __name__ == "__main__":
    main()
