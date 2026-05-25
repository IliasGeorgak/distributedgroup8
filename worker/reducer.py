from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable
import json

from applications import get_reducer
from partitioner import PARTITION_FUNCTIONS, get_partition_function, normalize_key


def _extract_intermediate_pairs(payload: Any, source_path: Path) -> tuple[int, list[tuple[Any, int]]]:
    
    if not isinstance(payload, dict):
        raise ValueError(f"Reduce input must be a JSON object: {source_path}")

    if "map_task_id" not in payload:
        raise ValueError(f"'map_task_id' is required in shuffled payload: {source_path}")
    if "partition_id" not in payload:
        raise ValueError(f"'partition_id' is required in shuffled payload: {source_path}")

    partition_id = int(payload["partition_id"])
    raw_pairs = payload.get("intermediate_pairs")
    if not isinstance(raw_pairs, list):
        raise ValueError(f"'intermediate_pairs' must be a list in {source_path}")

    pairs: list[tuple[Any, int]] = []
    for pair in raw_pairs:
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError(f"Each intermediate pair must be [key, value] in {source_path}")
        key, value = pair
        pairs.append((normalize_key(key), int(value)))
    return partition_id, pairs


def _iter_jsonl_intermediate_pairs(source_path: Path) -> Iterable[tuple[int, Any, int]]:
    partition_id: int | None = None

    with source_path.open("r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            raw_line = line.strip()
            if not raw_line:
                continue

            payload = json.loads(raw_line)
            if line_number == 1 and isinstance(payload, dict):
                if "map_task_id" not in payload:
                    raise ValueError(f"'map_task_id' is required in shuffled payload: {source_path}")
                if "partition_id" not in payload:
                    raise ValueError(f"'partition_id' is required in shuffled payload: {source_path}")
                partition_id = int(payload["partition_id"])
                continue

            if partition_id is None:
                raise ValueError(f"JSONL shuffle input is missing metadata header: {source_path}")
            if not isinstance(payload, list) or len(payload) != 2:
                raise ValueError(f"Each JSONL intermediate pair must be [key, value] in {source_path}")

            key, value = payload
            yield partition_id, normalize_key(key), int(value)


def _iter_intermediate_pairs(source_path: Path) -> Iterable[tuple[int, Any, int]]:
    if source_path.suffix.lower() == ".jsonl":
        yield from _iter_jsonl_intermediate_pairs(source_path)
        return

    payload = json.loads(source_path.read_text(encoding="utf-8"))
    partition_id, intermediate_pairs = _extract_intermediate_pairs(payload, source_path)
    for key, value in intermediate_pairs:
        yield partition_id, key, value


def reduce_partitioned_word_count(input_paths: list[Path], parameters: dict[str, Any]) -> dict[str, Any]:
    r_partitions = int(parameters.get("r_partitions", 1))
    if r_partitions <= 0:
        raise ValueError("'r_partitions' must be >= 1")
    reduce_partition_id = parameters.get("reduce_partition_id")
    target_partition_id = int(reduce_partition_id) if reduce_partition_id is not None else None
    if target_partition_id is not None and not (0 <= target_partition_id < r_partitions):
        raise ValueError("'reduce_partition_id' must be in [0, r_partitions)")

    partition_function_name = str(parameters.get("partition_function", "md5")).lower()
    get_partition_function(partition_function_name)
    reducer = get_reducer(parameters)

    partitioned_key_values: dict[int, dict[Any, list[int]]] = {
        partition_id: defaultdict(list) for partition_id in range(r_partitions)
    }

    for input_path in input_paths:
        for partition_id, key, value in _iter_intermediate_pairs(input_path):
            if not (0 <= partition_id < r_partitions):
                raise ValueError(f"'partition_id' out of range in {input_path}: {partition_id}")
            if target_partition_id is not None and partition_id != target_partition_id:
                continue
            partitioned_key_values[partition_id][key].append(value)

    if target_partition_id is not None:
        reduced_items = sorted(
            partitioned_key_values[target_partition_id].items(),
            key=lambda item: json.dumps(item[0], sort_keys=True),
        )
        return {
            "r_partitions": r_partitions,
            "partition_function": partition_function_name,
            "partition_id": target_partition_id,
            "reduced": [[key, reducer(key, values, parameters)] for key, values in reduced_items],
        }

    partitions: list[dict[str, Any]] = []
    for partition_id in range(r_partitions):
        reduced_items = sorted(
            partitioned_key_values[partition_id].items(),
            key=lambda item: json.dumps(item[0], sort_keys=True),
        )
        partitions.append(
            {
                "partition_id": partition_id,
                "reduced": [[key, reducer(key, values, parameters)] for key, values in reduced_items],
            }
        )

    return {
        "r_partitions": r_partitions,
        "partition_function": partition_function_name,
        "partitions": partitions,
    }
