from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable
import json

def _hash_md5(key: str) -> int: #hash(key) mod R
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
 

PARTITION_FUNCTIONS: dict[str, Callable[[str], int]] = {
    "md5": _hash_md5
}


def _extract_intermediate_pairs(payload: Any, source_path: Path) -> tuple[int, list[tuple[str, int]]]:
    # Shuffled payload format (downloaded from MinIO):
    # {"map_task_id": "...", "partition_id": N, "intermediate_pairs": [[key, value], ...]}
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

    pairs: list[tuple[str, int]] = []
    for pair in raw_pairs:
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError(f"Each intermediate pair must be [key, value] in {source_path}")
        key, value = pair
        pairs.append((str(key), int(value)))
    return partition_id, pairs


def reduce_partitioned_word_count(input_paths: list[Path], parameters: dict[str, Any]) -> dict[str, Any]:
    r_partitions = int(parameters.get("r_partitions", 1))
    if r_partitions <= 0:
        raise ValueError("'r_partitions' must be >= 1")
    reduce_partition_id = parameters.get("reduce_partition_id")
    target_partition_id = int(reduce_partition_id) if reduce_partition_id is not None else None
    if target_partition_id is not None and not (0 <= target_partition_id < r_partitions):
        raise ValueError("'reduce_partition_id' must be in [0, r_partitions)")

    partition_function_name = str(parameters.get("partition_function", "md5")).lower()
    hash_function = PARTITION_FUNCTIONS.get(partition_function_name)
    if hash_function is None:
        supported = ", ".join(sorted(PARTITION_FUNCTIONS))
        raise ValueError(
            f"Unsupported partition_function '{partition_function_name}'. Supported: {supported}"
        )

    partitioned_key_values: dict[int, dict[str, int]] = {
        partition_id: defaultdict(int) for partition_id in range(r_partitions)
    }

    for input_path in input_paths:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        partition_id, intermediate_pairs = _extract_intermediate_pairs(payload, input_path)
        if not (0 <= partition_id < r_partitions):
            raise ValueError(f"'partition_id' out of range in {input_path}: {partition_id}")
        if target_partition_id is not None and partition_id != target_partition_id:
            continue

        for key, value in intermediate_pairs:
            partitioned_key_values[partition_id][key] += value

    if target_partition_id is not None:
        reduced_items = sorted(partitioned_key_values[target_partition_id].items(), key=lambda item: item[0])
        return {
            "r_partitions": r_partitions,
            "partition_function": partition_function_name,
            "partition_id": target_partition_id,
            "reduced": [[key, count] for key, count in reduced_items],
        }

    partitions: list[dict[str, Any]] = []
    for partition_id in range(r_partitions):
        reduced_items = sorted(partitioned_key_values[partition_id].items(), key=lambda item: item[0])
        partitions.append(
            {
                "partition_id": partition_id,
                "reduced": [[key, count] for key, count in reduced_items],
            }
        )

    return {
        "r_partitions": r_partitions,
        "partition_function": partition_function_name,
        "partitions": partitions,
    }
