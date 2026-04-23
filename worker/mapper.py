from __future__ import annotations

import re
from pathlib import Path
from typing import Any


WORD_PATTERN = re.compile(r"\b\w+\b")


def _normalize_word(word: str, case_sensitive: bool) -> str:
    return word if case_sensitive else word.lower()


def _partition_records(records: list[str], split_count: int) -> list[list[str]]:
    if split_count <= 0:
        raise ValueError("Split count must be >= 1")

    total_records = len(records)
    if total_records == 0:
        return [[] for _ in range(split_count)]

    base_size = total_records // split_count
    remainder = total_records % split_count

    partitions: list[list[str]] = []
    start = 0
    for split_id in range(split_count):
        size = base_size + (1 if split_id < remainder else 0)
        end = start + size
        partitions.append(records[start:end])
        start = end

    return partitions


def map_to_key_value_pairs(input_paths: list[Path], parameters: dict[str, Any]) -> dict[str, Any]:
    case_sensitive = bool(parameters.get("case_sensitive", False))
    requested_splits = int(parameters.get("m_splits", len(input_paths) or 1))
    split_count = max(1, requested_splits)

    all_records: list[str] = []
    for input_path in input_paths:
        text = input_path.read_text(encoding="utf-8")
        all_records.extend(text.splitlines())

    split_records = _partition_records(all_records, split_count)
    intermediate_pairs: list[list[Any]] = []
    split_metadata: list[dict[str, int]] = []

    for split_id, records in enumerate(split_records):
        split_pair_count = 0
        for record in records:
            for word in WORD_PATTERN.findall(record):
                intermediate_pairs.append([_normalize_word(word, case_sensitive), 1])
                split_pair_count += 1
        split_metadata.append({"split_id": split_id, "pair_count": split_pair_count})

    return {
        "m_splits": split_count,
        "split_metadata": split_metadata,
        "intermediate_pairs": intermediate_pairs,
    }
