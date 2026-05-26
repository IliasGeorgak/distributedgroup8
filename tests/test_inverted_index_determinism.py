from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "worker"))

from mapper import stream_to_shuffle_partitions
from reducer import reduce_partitioned_word_count


DATASET = {
    "doc1": ["hello world\n", "hello mapreduce\n"],
    "doc2": ["world distributed systems\n"],
}


def _write_splits(tmp_path: Path, split_count: int) -> list[tuple[str, Path]]:
    split_files: list[tuple[str, Path]] = []
    for document_id, lines in DATASET.items():
        actual_split_count = min(split_count, len(lines))
        for split_id in range(actual_split_count):
            split_path = tmp_path / f"{document_id}-split-{split_id}.txt"
            split_lines = lines[split_id::actual_split_count]
            split_path.write_text("".join(split_lines), encoding="utf-8")
            split_files.append((document_id, split_path))
    return split_files


def _run_inverted_index(tmp_path: Path, split_count: int, r_partitions: int) -> dict[str, list[str]]:
    partition_inputs: dict[int, list[Path]] = {
        partition_id: [] for partition_id in range(r_partitions)
    }

    for map_id, (document_id, split_path) in enumerate(_write_splits(tmp_path, split_count), start=1):
        map_dir = tmp_path / f"map-{map_id}"
        map_dir.mkdir()
        map_result = stream_to_shuffle_partitions(
            [split_path],
            {
                "operation": "inverted_index",
                "document_id": document_id,
                "case_sensitive": False,
                "r_partitions": r_partitions,
                "partition_function": "sha256",
            },
            map_dir,
            f"map-{map_id}",
        )

        for partition_id, raw_path in enumerate(map_result["shuffle"]["local_partition_paths"]):
            partition_inputs[partition_id].append(Path(raw_path))

    merged_index: dict[str, list[str]] = {}
    for partition_id, input_paths in partition_inputs.items():
        result = reduce_partitioned_word_count(
            input_paths,
            {
                "operation": "inverted_index",
                "r_partitions": r_partitions,
                "reduce_partition_id": partition_id,
                "partition_function": "sha256",
            },
        )
        for word, document_ids in result["reduced"]:
            merged_index[word] = document_ids

    return {
        word: merged_index[word]
        for word in sorted(merged_index)
    }


def test_inverted_index_is_deterministic_across_split_and_reduce_counts(tmp_path):
    expected = {
        "distributed": ["doc2"],
        "hello": ["doc1"],
        "mapreduce": ["doc1"],
        "systems": ["doc2"],
        "world": ["doc1", "doc2"],
    }

    for split_count in [1, 4, 16]:
        for r_partitions in [1, 2, 8]:
            run_dir = tmp_path / f"s{split_count}-r{r_partitions}"
            run_dir.mkdir()
            assert _run_inverted_index(run_dir, split_count, r_partitions) == expected
