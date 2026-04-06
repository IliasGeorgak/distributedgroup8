from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from storage import download_object, upload_file
from task import TaskMetadata


WORD_PATTERN = re.compile(r"\b\w+\b")


def map_word_count(input_paths: list[Path], parameters: dict[str, Any]) -> dict[str, int]:
    case_sensitive = bool(parameters.get("case_sensitive", False))
    counts: Counter[str] = Counter()

    for input_path in input_paths:
        text = input_path.read_text(encoding="utf-8")
        words = WORD_PATTERN.findall(text)
        if not case_sensitive:
            words = [word.lower() for word in words]
        counts.update(words)

    return dict(sorted(counts.items()))


def reduce_word_count(input_paths: list[Path], _: dict[str, Any]) -> dict[str, int]:
    merged_counts: Counter[str] = Counter()

    for input_path in input_paths:
        partial_counts = json.loads(input_path.read_text(encoding="utf-8"))
        if not isinstance(partial_counts, dict):
            raise ValueError(f"Reduce input must be a JSON object: {input_path}")
        merged_counts.update({str(key): int(value) for key, value in partial_counts.items()})

    return dict(sorted(merged_counts.items()))


TASK_HANDLERS = {
    "map": map_word_count,
    "reduce": reduce_word_count,
}


class Worker:
    def run_task(self, metadata: TaskMetadata) -> Path:
        task_handler = TASK_HANDLERS.get(metadata.task_type)
        if task_handler is None:
            supported = ", ".join(sorted(TASK_HANDLERS))
            raise ValueError(f"Unsupported task_type '{metadata.task_type}'. Supported: {supported}")

        with TemporaryDirectory(prefix=f"task-{metadata.task_id}-") as temp_dir:
            working_dir = Path(temp_dir)
            input_paths = self._download_inputs(metadata, working_dir)
            task_result = task_handler(input_paths, metadata.parameters)

            output_path = working_dir / "result.json"
            output_path.write_text(json.dumps(task_result, indent=2, sort_keys=True), encoding="utf-8")
            upload_file(metadata.output_bucket, metadata.output_object, output_path)

            persisted_output = Path.cwd() / f"{metadata.task_id}-result.json"
            persisted_output.write_text(output_path.read_text(encoding="utf-8"), encoding="utf-8")
            return persisted_output

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
    worker = Worker()
    output_path = worker.run_task(metadata)
    print(f"Task {metadata.task_id} completed. Local result: {output_path}")


if __name__ == "__main__":
    main()
