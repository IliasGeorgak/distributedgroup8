from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import json

try:
    import yaml
except ModuleNotFoundError:
    yaml = None


@dataclass(slots=True)
class TaskMetadata:
    task_id: str
    task_type: str
    input_bucket: str
    input_objects: list[str]
    output_bucket: str
    output_object: str
    parameters: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TaskMetadata":
        required_fields = (
            "task_id",
            "task_type",
            "input_bucket",
            "input_objects",
            "output_bucket",
            "output_object",
        )
        missing_fields = [field_name for field_name in required_fields if field_name not in payload]
        if missing_fields:
            missing = ", ".join(missing_fields)
            raise ValueError(f"Task metadata is missing required fields: {missing}")

        input_objects = payload["input_objects"]
        if not isinstance(input_objects, list) or not input_objects:
            raise ValueError("Task metadata field 'input_objects' must be a non-empty list")

        return cls(
            task_id=str(payload["task_id"]),
            task_type=str(payload["task_type"]).lower(),
            input_bucket=str(payload["input_bucket"]),
            input_objects=[str(obj) for obj in input_objects],
            output_bucket=str(payload["output_bucket"]),
            output_object=str(payload["output_object"]),
            parameters=dict(payload.get("parameters") or {}),
        )

    @classmethod
    def from_json(cls, raw_json: str) -> "TaskMetadata":
        return cls.from_dict(json.loads(raw_json))

    @classmethod
    def from_yaml(cls, raw_yaml: str) -> "TaskMetadata":
        if yaml is None:
            raise RuntimeError("YAML support requires PyYAML to be installed")
        payload = yaml.safe_load(raw_yaml)
        if not isinstance(payload, dict):
            raise ValueError("Task metadata YAML must define an object at the top level")
        return cls.from_dict(payload)

    @classmethod
    def from_file(cls, path: str | Path) -> "TaskMetadata":
        file_path = Path(path)
        raw_content = file_path.read_text(encoding="utf-8")

        if file_path.suffix.lower() in {".yaml", ".yml"}:
            return cls.from_yaml(raw_content)

        return cls.from_json(raw_content)
