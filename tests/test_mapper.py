from pathlib import Path
import json
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]/"worker"))

from mapper import map_to_key_value_pairs, stream_to_shuffle_partitions

def test_mapper_outputs_key_value_pairs(tmp_path):
    input_file = tmp_path / "input.txt"
    input_file.write_text("Hello world\nhello nteroulas\n", encoding="utf-8")

    result = map_to_key_value_pairs(
        [input_file],
        {"case_sensitive": False, "m_splits": 2},
    )

    assert result["m_splits"] == 2
    assert result["intermediate_pairs"] == [
        ["hello", 2],
        ["world", 1],
        ["nteroulas", 1],
    ]

    assert result["split_metadata"] == [{"split_id": 0, "pair_count": 4}]


def test_streaming_mapper_writes_jsonl_shuffle_partitions(tmp_path):
    input_file = tmp_path / "input.txt"
    input_file.write_text("Hello world\nhello\n", encoding="utf-8")

    result = stream_to_shuffle_partitions(
        [input_file],
        {"case_sensitive": False, "r_partitions": 1},
        tmp_path,
        "map-1",
    )

    shuffle = result["shuffle"]
    assert shuffle["partition_pair_counts"] == [2]

    partition_path = Path(shuffle["local_partition_paths"][0])
    lines = partition_path.read_text(encoding="utf-8").splitlines()

    assert json.loads(lines[0]) == {"map_task_id": "map-1", "partition_id": 0}
    assert [json.loads(line) for line in lines[1:]] == [
        ["hello", 2],
        ["world", 1],
    ]


def test_streaming_mapper_reads_jsonl_records(tmp_path):
    input_file = tmp_path / "input.jsonl"
    input_file.write_text(
        "\n".join(
            [
                json.dumps({"text": "Hello structured world"}),
                json.dumps({"text": "hello json"}),
            ]
        ),
        encoding="utf-8",
    )

    result = stream_to_shuffle_partitions(
        [input_file],
        {
            "case_sensitive": False,
            "input_format": "jsonl",
            "text_field": "text",
            "r_partitions": 1,
        },
        tmp_path,
        "map-1",
    )

    partition_path = Path(result["shuffle"]["local_partition_paths"][0])
    lines = partition_path.read_text(encoding="utf-8").splitlines()

    assert [json.loads(line) for line in lines[1:]] == [
        ["hello", 2],
        ["structured", 1],
        ["world", 1],
        ["json", 1],
    ]


def test_streaming_mapper_supports_different_builtin_operation(tmp_path):
    input_file = tmp_path / "input.txt"
    input_file.write_text("first\nsecond\n", encoding="utf-8")

    result = stream_to_shuffle_partitions(
        [input_file],
        {"operation": "line_count", "r_partitions": 1},
        tmp_path,
        "map-1",
    )

    partition_path = Path(result["shuffle"]["local_partition_paths"][0])
    lines = partition_path.read_text(encoding="utf-8").splitlines()

    assert [json.loads(line) for line in lines[1:]] == [["lines", 2]]


def test_streaming_mapper_supports_inverted_index_document_ids(tmp_path):
    input_file = tmp_path / "doc1.txt"
    input_file.write_text("Hello world\nhello mapreduce\n", encoding="utf-8")

    result = stream_to_shuffle_partitions(
        [input_file],
        {"operation": "inverted_index", "case_sensitive": False, "r_partitions": 1},
        tmp_path,
        "map-1",
    )

    partition_path = Path(result["shuffle"]["local_partition_paths"][0])
    lines = partition_path.read_text(encoding="utf-8").splitlines()

    assert [json.loads(line) for line in lines[1:]] == [
        ["hello", {"doc1": 2}],
        ["world", {"doc1": 1}],
        ["mapreduce", {"doc1": 1}],
    ]
