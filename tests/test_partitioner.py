from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "worker"))

from partitioner import get_partition_function


def test_partition_hash_is_stable_for_same_key():
    hash_function = get_partition_function("md5")

    assert hash_function("hello") == hash_function("hello")
    assert hash_function("hello") % 4 == hash_function("hello") % 4


def test_partition_hash_supports_structured_keys():
    hash_function = get_partition_function("sha256")

    assert hash_function(("user", 1)) == hash_function(("user", 1))
