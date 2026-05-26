import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "worker"))

from reducer import reduce_partitioned_word_count


def test_reducer_basic(tmp_path):
    file1 = tmp_path / "part1.json"
    file2 = tmp_path / "part2.json"

    file1.write_text(json.dumps({
        "map_task_id": "m1",
        "partition_id": 0,
        "intermediate_pairs": [["hello", 1], ["world", 1]],
    }), encoding="utf-8")

    file2.write_text(json.dumps({
        "map_task_id": "m1",
        "partition_id": 0,
        "intermediate_pairs": [["hello", 1]],
    }), encoding="utf-8")

    result = reduce_partitioned_word_count(
        [file1, file2],
        {"r_partitions": 1, "reduce_partition_id": 0},
    )

    assert result["reduced"] == [
        ["hello", 2],
        ["world", 1],
    ]


def test_reducer_reads_streamed_jsonl_shuffle_input(tmp_path):
    file1 = tmp_path / "part1.jsonl"
    file1.write_text(
        "\n".join(
            [
                json.dumps({"map_task_id": "m1", "partition_id": 0}),
                json.dumps(["hello", 1]),
                json.dumps(["world", 1]),
                json.dumps(["hello", 1]),
            ]
        ),
        encoding="utf-8",
    )

    result = reduce_partitioned_word_count(
        [file1],
        {"r_partitions": 1, "reduce_partition_id": 0},
    )

    assert result["reduced"] == [
        ["hello", 2],
        ["world", 1],
    ]


def test_reducer_builds_inverted_index_from_document_maps(tmp_path):
    file1 = tmp_path / "part1.jsonl"
    file1.write_text(
        "\n".join(
            [
                json.dumps({"map_task_id": "m1", "partition_id": 0}),
                json.dumps(["hello", {"doc1": 2}]),
                json.dumps(["world", {"doc1": 1}]),
                json.dumps(["world", {"doc2": 1}]),
            ]
        ),
        encoding="utf-8",
    )

    result = reduce_partitioned_word_count(
        [file1],
        {
            "operation": "inverted_index",
            "r_partitions": 1,
            "reduce_partition_id": 0,
        },
    )

    assert result["reduced"] == [
        ["hello", ["doc1"]],
        ["world", ["doc1", "doc2"]],
    ]


def test_reducer_can_emit_inverted_index_frequencies(tmp_path):
    file1 = tmp_path / "part1.jsonl"
    file1.write_text(
        "\n".join(
            [
                json.dumps({"map_task_id": "m1", "partition_id": 0}),
                json.dumps(["hello", {"doc1": 2}]),
                json.dumps(["hello", {"doc1": 1, "doc2": 1}]),
            ]
        ),
        encoding="utf-8",
    )

    result = reduce_partitioned_word_count(
        [file1],
        {
            "operation": "inverted_index",
            "index_with_frequencies": True,
            "r_partitions": 1,
            "reduce_partition_id": 0,
        },
    )

    assert result["reduced"] == [
        ["hello", {"doc1": 3, "doc2": 1}],
    ]
