from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from applications import get_mapper, iter_input_records
from partitioner import get_partition_function, normalize_key


def map_to_key_value_pairs(input_paths: list[Path], parameters: dict[str, Any]) -> dict[str, Any]:
    requested_splits = int(parameters.get("m_splits", len(input_paths) or 1))
    split_count = max(1, requested_splits)

    mapper = get_mapper(parameters)
    key_value_counts: dict[Any, int] = defaultdict(int)
    pair_count = 0

    for record in iter_input_records(input_paths, parameters):
        for key, value in mapper(record, parameters):
            key_value_counts[normalize_key(key)] += int(value)
            pair_count += 1

    return {
        "m_splits": split_count,
        "split_metadata": [{"split_id": 0, "pair_count": pair_count}],
        "intermediate_pairs": [[key, count] for key, count in key_value_counts.items()],
    }


def stream_to_shuffle_partitions(
    input_paths: list[Path],
    parameters: dict[str, Any],
    working_dir: Path,
    map_task_id: str,
) -> dict[str, Any]:
    requested_splits = int(parameters.get("m_splits", len(input_paths) or 1))
    split_count = max(1, requested_splits)

    r_partitions = int(parameters.get("r_partitions", 1))
    if r_partitions <= 0:
        raise ValueError("'r_partitions' must be >= 1")

    partition_function_name = str(parameters.get("partition_function", "md5")).lower()
    hash_function = get_partition_function(partition_function_name)
    mapper = get_mapper(parameters)
    combiner_flush_limit = int(parameters.get("combiner_flush_limit", 50000))
    if combiner_flush_limit <= 0:
        raise ValueError("'combiner_flush_limit' must be >= 1")

    partition_paths = [
        working_dir / f"shuffle-partition-{partition_id}.jsonl"
        for partition_id in range(r_partitions)
    ]
    partition_pair_counts = [0 for _ in range(r_partitions)]
    total_pair_count = 0
    combined_counts: list[dict[Any, int]] = [defaultdict(int) for _ in range(r_partitions)]

    def flush_combiner() -> None:
        for partition_id, counts in enumerate(combined_counts):
            for key, value in counts.items():
                handles[partition_id].write(json.dumps([key, value]) + "\n")
                partition_pair_counts[partition_id] += 1
            counts.clear()

    handles = [path.open("w", encoding="utf-8") for path in partition_paths]
    try:
        for partition_id, handle in enumerate(handles):
            handle.write(json.dumps({"map_task_id": map_task_id, "partition_id": partition_id}) + "\n")

        distinct_pair_count = 0
        for record in iter_input_records(input_paths, parameters):
            for key, value in mapper(record, parameters):
                key = normalize_key(key)
                partition_id = hash_function(key) % r_partitions
                if key not in combined_counts[partition_id]:
                    distinct_pair_count += 1
                combined_counts[partition_id][key] += int(value)
                total_pair_count += 1
                if distinct_pair_count >= combiner_flush_limit:
                    flush_combiner()
                    distinct_pair_count = 0

        flush_combiner()
    finally:
        for handle in handles:
            handle.close()

    return {
        "m_splits": split_count,
        "split_metadata": [{"split_id": 0, "pair_count": total_pair_count}],
        "shuffle": {
            "r_partitions": r_partitions,
            "partition_function": partition_function_name,
            "local_partition_paths": [str(path) for path in partition_paths],
            "partition_objects": [],
            "partition_pair_counts": partition_pair_counts,
        },
    }
