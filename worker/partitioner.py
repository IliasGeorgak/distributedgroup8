from __future__ import annotations

import hashlib
import json
from typing import Any, Callable


def _stable_key_bytes(key: Any) -> bytes:
    return normalize_key(key).encode("utf-8")


def normalize_key(key: Any) -> str:
    if isinstance(key, str):
        return key
    return json.dumps(key, sort_keys=True, separators=(",", ":"))


def _hash_md5(key: Any) -> int:
    return int(hashlib.md5(_stable_key_bytes(key)).hexdigest(), 16)


def _hash_sha256(key: Any) -> int:
    return int(hashlib.sha256(_stable_key_bytes(key)).hexdigest(), 16)


PARTITION_FUNCTIONS: dict[str, Callable[[Any], int]] = {
    "md5": _hash_md5,
    "sha256": _hash_sha256,
}


def get_partition_function(name: str) -> Callable[[Any], int]:
    normalized_name = name.lower()
    hash_function = PARTITION_FUNCTIONS.get(normalized_name)
    if hash_function is None:
        supported = ", ".join(sorted(PARTITION_FUNCTIONS))
        raise ValueError(f"Unsupported partition_function '{name}'. Supported: {supported}")
    return hash_function
