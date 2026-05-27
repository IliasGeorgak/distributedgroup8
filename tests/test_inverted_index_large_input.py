import math
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "worker"))

from mapper import stream_to_shuffle_partitions
from reducer import reduce_partitioned_word_count


ONE_HUNDRED_MIB = 100 * 1024 * 1024
LARGE_DOCUMENT_LINE = "alpha beta beta gamma\n"


def _write_large_document(path: Path, minimum_size_bytes: int) -> int:
    line_bytes = LARGE_DOCUMENT_LINE.encode("utf-8")
    line_count = math.ceil(minimum_size_bytes / len(line_bytes))

    with path.open("w", encoding="utf-8") as output_file:
        chunk = LARGE_DOCUMENT_LINE * 8192
        full_chunks, remaining_lines = divmod(line_count, 8192)
        for _ in range(full_chunks):
            output_file.write(chunk)
        output_file.write(LARGE_DOCUMENT_LINE * remaining_lines)

    return line_count


def test_inverted_index_processes_100_mb_text_input(tmp_path):
    input_file = tmp_path / "large-doc.txt"
    line_count = _write_large_document(input_file, ONE_HUNDRED_MIB)

    assert input_file.stat().st_size >= ONE_HUNDRED_MIB

    map_result = stream_to_shuffle_partitions(
        [input_file],
        {
            "operation": "inverted_index",
            "case_sensitive": False,
            "r_partitions": 4,
            "partition_function": "sha256",
            "combiner_flush_limit": 1000,
        },
        tmp_path,
        "map-large",
    )

    partition_inputs = [
        Path(path)
        for path in map_result["shuffle"]["local_partition_paths"]
    ]
    reduced_index = {}
    for partition_id, partition_input in enumerate(partition_inputs):
        reduce_result = reduce_partitioned_word_count(
            [partition_input],
            {
                "operation": "inverted_index",
                "index_with_frequencies": True,
                "r_partitions": 4,
                "reduce_partition_id": partition_id,
                "partition_function": "sha256",
            },
        )
        reduced_index.update(dict(reduce_result["reduced"]))

    assert map_result["split_metadata"] == [
        {"split_id": 0, "pair_count": line_count * 4}
    ]
    assert reduced_index == {
        "alpha": {"large-doc": line_count},
        "beta": {"large-doc": line_count * 2},
        "gamma": {"large-doc": line_count},
    }
