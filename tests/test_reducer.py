import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]/"worker"))

from reducer import reduce_partitioned_word_count

def test_reducer_basic(tmp_path):
    file1 = tmp_path/"part1.json"
    file2 = tmp_path/"part2.json"

    file1.write_text(json.dumps({
        "map_task_id":"m1",
        "partition_id": 0,
        "intermediate_pairs" : [["hello",1],["world",1]]
    }))

    file2.write_text(json.dumps({
        "map_task_id":"m1",
        "partition_id": 0,
        "intermediate_pairs" : [["hello",1]]
    }))

    result = reduce_partitioned_word_count(
        [file1,file2],
        {"r_partitions":1 , "reduce_partition_id":0}
    )

    assert result["reduced"] == [
        ["hello,2"],
        ["world",1]
    ]