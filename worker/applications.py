from __future__ import annotations

import importlib
import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Callable


WORD_PATTERN = re.compile(r"\b\w+\b")

MapperFunction = Callable[[Any, dict[str, Any]], Iterable[tuple[Any, int]]]
ReducerFunction = Callable[[Any, list[int], dict[str, Any]], Any]


def _load_callable(dotted_path: str) -> Callable[..., Any]:
    module_name, separator, function_name = dotted_path.partition(":")
    if not separator:
        module_name, separator, function_name = dotted_path.rpartition(".")
    if not module_name or not function_name:
        raise ValueError("Custom functions must be formatted as 'module:function'")

    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    if not callable(function):
        raise ValueError(f"Custom function is not callable: {dotted_path}")
    return function


def iter_input_records(input_paths: list[Path], parameters: dict[str, Any]) -> Iterable[Any]:
    input_format = str(parameters.get("input_format", "auto")).lower()

    for input_path in input_paths:
        format_name = input_format
        if format_name == "auto":
            suffix = input_path.suffix.lower()
            format_name = "jsonl" if suffix == ".jsonl" else "json" if suffix == ".json" else "text"

        if format_name == "text":
            with input_path.open("r", encoding="utf-8") as input_file:
                yield from input_file
        elif format_name == "jsonl":
            with input_path.open("r", encoding="utf-8") as input_file:
                for line in input_file:
                    raw_line = line.strip()
                    if raw_line:
                        yield json.loads(raw_line)
        elif format_name == "json":
            payload = json.loads(input_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("records"), list):
                yield from payload["records"]
            elif isinstance(payload, list):
                yield from payload
            else:
                yield payload
        else:
            raise ValueError("input_format must be one of: auto, text, jsonl, json")


def _record_text(record: Any, parameters: dict[str, Any]) -> str:
    if isinstance(record, str):
        return record
    if isinstance(record, dict):
        text_field = str(parameters.get("text_field", "text"))
        if text_field in record:
            return str(record[text_field])
    return json.dumps(record, sort_keys=True)


def word_count_mapper(record: Any, parameters: dict[str, Any]) -> Iterable[tuple[str, int]]:
    case_sensitive = bool(parameters.get("case_sensitive", False))
    text = _record_text(record, parameters)
    for word in WORD_PATTERN.findall(text):
        yield (word if case_sensitive else word.lower()), 1


def line_count_mapper(record: Any, parameters: dict[str, Any]) -> Iterable[tuple[str, int]]:
    key = str(parameters.get("line_count_key", "lines"))
    yield key, 1


def sum_reducer(key: Any, values: list[int], parameters: dict[str, Any]) -> Any:
    return sum(values)


MAPPER_FUNCTIONS: dict[str, MapperFunction] = {
    "word_count": word_count_mapper,
    "line_count": line_count_mapper,
}

REDUCER_FUNCTIONS: dict[str, ReducerFunction] = {
    "sum": sum_reducer,
    "word_count": sum_reducer,
    "line_count": sum_reducer,
}


def get_mapper(parameters: dict[str, Any]) -> MapperFunction:
    custom_mapper = parameters.get("mapper")
    if custom_mapper:
        return _load_callable(str(custom_mapper))

    operation = str(parameters.get("operation", "word_count")).lower()
    mapper = MAPPER_FUNCTIONS.get(operation)
    if mapper is None:
        supported = ", ".join(sorted(MAPPER_FUNCTIONS))
        raise ValueError(f"Unsupported operation '{operation}'. Supported: {supported}")
    return mapper


def get_reducer(parameters: dict[str, Any]) -> ReducerFunction:
    custom_reducer = parameters.get("reducer")
    if custom_reducer:
        return _load_callable(str(custom_reducer))

    operation = str(parameters.get("operation", "word_count")).lower()
    reducer = REDUCER_FUNCTIONS.get(operation)
    if reducer is None:
        supported = ", ".join(sorted(REDUCER_FUNCTIONS))
        raise ValueError(f"Unsupported operation '{operation}'. Supported: {supported}")
    return reducer
