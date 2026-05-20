from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]/"worker"))

from mapper import map_to_key_value_pairs

def test_mapper_outputs_key_value_pairs(tmp_path):
    input_file = tmp_path / "input.txt"
    input_file.write_text("Hello world\nhello nteroulas\n", encoding="utf-8")

    result = map_to_key_value_pairs(
        [input_file],
        {"case_sensitive": False, "m_splits": 2},
    )

    assert result["m_splits"] == 2
    assert result["intermediate_pairs"] == [
        ["hello", 1],
        ["world", 1],
        ["hello", 1],
        ["nteroulas", 1],
    ]

    assert result["split_metadata"] == [
        {"split_id": 0, "pair_count": 2},
        {"split_id": 1, "pair_count": 2},
    ]
