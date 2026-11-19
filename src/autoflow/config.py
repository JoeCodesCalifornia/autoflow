"""YAML/JSON pipeline config loader with schema validation."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from autoflow.dag import DAG
from autoflow.exceptions import ConfigError, ValidationError
from autoflow.plugins import PluginRegistry, register_builtin_tasks, registry
from autoflow.task import TaskSpec

_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")

_REQUIRED_ROOT = {"name", "tasks"}
_ALLOWED_ROOT = {
    "name",
    "tasks",
    "version",
    "description",
    "max_concurrency",
    "vars",
    "schedule",
}
_ALLOWED_TASK = {
    "name",
    "type",
    "depends_on",
    "retries",
    "timeout",
    "backoff",
    "backoff_factor",
    "max_backoff",
    "params",
    "description",
}


@dataclass
class Pipeline:
    """A validated, in-memory pipeline definition."""

    name: str
    tasks: list[TaskSpec]
    version: str = "1.0"
    description: str = ""
    max_concurrency: int = 4
    vars: dict[str, Any] = field(default_factory=dict)
    schedule: str | None = None
    source: Path | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dag(self) -> DAG:
        dag = DAG(self.name)
        for spec in self.tasks:
            dag.add(spec)
        dag.validate()
        return dag


def interpolate(value: Any, variables: Mapping[str, Any]) -> Any:
    """Replace ``${VAR}`` and ``${VAR:default}`` in strings, recursively."""

    def _replace(text: str) -> str:
        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            default = match.group(2)
            if key in variables and variables[key] is not None:
                return str(variables[key])
            if default is not None:
                return default
            env = os.environ.get(key)
            if env is not None:
                return env
            raise ConfigError(f"Unresolved interpolation variable: {key}")

        return _VAR.sub(repl, text)

    if isinstance(value, str):
        return _replace(value)
    if isinstance(value, list):
        return [interpolate(v, variables) for v in value]
    if isinstance(value, dict):
        return {k: interpolate(v, variables) for k, v in value.items()}
    return value


def _load_raw(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Cannot read pipeline file {path}: {exc}") from exc
    suffix = path.suffix.lower()
    try:
        if suffix in {".yaml", ".yml"}:
            data = yaml.safe_load(text)
        elif suffix == ".json":
            data = json.loads(text)
        else:
            # Try YAML first, then JSON.
            try:
                data = yaml.safe_load(text)
            except yaml.YAMLError:
                data = json.loads(text)
    except (yaml.YAMLError, json.JSONDecodeError) as exc:
        raise ConfigError(f"Failed to parse {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"Pipeline file {path} must contain a mapping at the root")
    return data


def validate_document(data: Mapping[str, Any]) -> list[str]:
    """Return a list of schema problems (empty means the document is valid)."""
    errors: list[str] = []
    missing = _REQUIRED_ROOT - set(data)
    if missing:
        errors.append(f"Missing required field(s): {', '.join(sorted(missing))}")
    unknown = set(data) - _ALLOWED_ROOT
    if unknown:
        errors.append(f"Unknown top-level field(s): {', '.join(sorted(unknown))}")
    if "name" in data and not isinstance(data["name"], str):
        errors.append("'name' must be a string")
    if "max_concurrency" in data:
        mc = data["max_concurrency"]
        if not isinstance(mc, int) or isinstance(mc, bool) or mc < 1:
            errors.append("'max_concurrency' must be an integer >= 1")
    if "vars" in data and not isinstance(data["vars"], dict):
        errors.append("'vars' must be a mapping")
    schedule = data.get("schedule")
    if "schedule" in data and schedule is not None and not isinstance(schedule, str):
        errors.append("'schedule' must be a cron string")
    tasks = data.get("tasks")
    if "tasks" in data:
        if not isinstance(tasks, list) or not tasks:
            errors.append("'tasks' must be a non-empty list")
        else:
            names: list[str] = []
            for i, task in enumerate(tasks):
                prefix = f"tasks[{i}]"
                if not isinstance(task, dict):
                    errors.append(f"{prefix} must be a mapping")
                    continue
                if "name" not in task or not isinstance(task["name"], str) or not task["name"]:
                    errors.append(f"{prefix}.name is required")
                else:
                    if task["name"] in names:
                        errors.append(f"Duplicate task name: {task['name']!r}")
                    names.append(task["name"])
                extra = set(task) - _ALLOWED_TASK
                if extra:
                    errors.append(f"{prefix} has unknown field(s): {', '.join(sorted(extra))}")
                if "type" in task and not isinstance(task["type"], str):
                    errors.append(f"{prefix}.type must be a string")
                if "depends_on" in task:
                    deps = task["depends_on"]
                    if isinstance(deps, str):
                        pass
                    elif not isinstance(deps, list) or not all(isinstance(d, str) for d in deps):
                        errors.append(f"{prefix}.depends_on must be a string or list of strings")
                for numeric in ("retries",):
                    if numeric in task:
                        val = task[numeric]
                        if not isinstance(val, int) or isinstance(val, bool) or val < 0:
                            errors.append(f"{prefix}.{numeric} must be an integer >= 0")
                for numeric_f in ("timeout", "backoff", "backoff_factor", "max_backoff"):
                    if numeric_f in task:
                        val = task[numeric_f]
                        if not isinstance(val, (int, float)) or isinstance(val, bool) or val <= 0:
                            errors.append(f"{prefix}.{numeric_f} must be a number > 0")
                if "params" in task and not isinstance(task["params"], dict):
                    errors.append(f"{prefix}.params must be a mapping")
    return errors


def _as_deps(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def load_pipeline(
    path: str | Path,
    *,
    vars: Mapping[str, Any] | None = None,
    plugins: PluginRegistry | None = None,
    interpolate_vars: bool = True,
) -> Pipeline:
    """Load, interpolate, and validate a YAML or JSON pipeline file."""
    path = Path(path)
    raw = _load_raw(path)
    merged_vars: dict[str, Any] = {}
    if isinstance(raw.get("vars"), dict):
        merged_vars.update(raw["vars"])
    if vars:
        merged_vars.update(vars)
    data = interpolate(raw, merged_vars) if interpolate_vars else raw
    if not isinstance(data, dict):
        raise ConfigError("Interpolated pipeline must remain a mapping")
    errors = validate_document(data)
    if errors:
        raise ValidationError(f"Invalid pipeline {path}: " + "; ".join(errors), errors=errors)

    plugins = plugins or registry
    register_builtin_tasks(plugins)
    plugins.load_entry_points()

    specs: list[TaskSpec] = []
    for item in data["tasks"]:
        type_name = item.get("type", "shell")
        params = dict(item.get("params") or {})
        try:
            func = plugins.create_func(type_name, params)
        except Exception as exc:
            raise ValidationError(
                f"Task {item['name']!r} has unknown or unloadable type {type_name!r}: {exc}"
            ) from exc
        specs.append(
            TaskSpec(
                name=item["name"],
                func=func,
                depends_on=_as_deps(item.get("depends_on")),
                retries=int(item.get("retries", 0)),
                timeout=float(item["timeout"]) if "timeout" in item else None,
                backoff=float(item.get("backoff", 1.0)),
                backoff_factor=float(item.get("backoff_factor", 2.0)),
                max_backoff=float(item.get("max_backoff", 60.0)),
                task_type=type_name,
                params=params,
                description=str(item.get("description", "")),
            )
        )

    pipeline = Pipeline(
        name=str(data["name"]),
        tasks=specs,
        version=str(data.get("version", "1.0")),
        description=str(data.get("description", "")),
        max_concurrency=int(data.get("max_concurrency", 4)),
        vars=dict(data.get("vars") or {}) | dict(merged_vars),
        schedule=data.get("schedule"),
        source=path,
        raw=data,
    )
    # Semantic validation (cycles, missing deps) happens here so `validate`
    # catches them before a run.
    pipeline.to_dag()
    return pipeline


def loads_pipeline(
    text: str,
    *,
    name: str = "memory.yaml",
    vars: Mapping[str, Any] | None = None,
    plugins: PluginRegistry | None = None,
) -> Pipeline:
    """Load a pipeline from a YAML/JSON string (writes a temp-less parse)."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError:
        data = json.loads(text)
    if not isinstance(data, dict):
        raise ConfigError("Pipeline text must contain a mapping at the root")
    # Reuse load_pipeline via a small adapter: write is avoided by inlining.
    # We stash the document through a dummy path-like by calling internals.
    merged_vars: dict[str, Any] = {}
    if isinstance(data.get("vars"), dict):
        merged_vars.update(data["vars"])
    if vars:
        merged_vars.update(vars)
    data = interpolate(data, merged_vars)
    errors = validate_document(data)
    if errors:
        raise ValidationError(f"Invalid pipeline {name}: " + "; ".join(errors), errors=errors)
    plugins = plugins or registry
    register_builtin_tasks(plugins)
    plugins.load_entry_points()
    specs: list[TaskSpec] = []
    for item in data["tasks"]:
        type_name = item.get("type", "shell")
        params = dict(item.get("params") or {})
        func = plugins.create_func(type_name, params)
        specs.append(
            TaskSpec(
                name=item["name"],
                func=func,
                depends_on=_as_deps(item.get("depends_on")),
                retries=int(item.get("retries", 0)),
                timeout=float(item["timeout"]) if "timeout" in item else None,
                backoff=float(item.get("backoff", 1.0)),
                backoff_factor=float(item.get("backoff_factor", 2.0)),
                max_backoff=float(item.get("max_backoff", 60.0)),
                task_type=type_name,
                params=params,
                description=str(item.get("description", "")),
            )
        )
    pipeline = Pipeline(
        name=str(data["name"]),
        tasks=specs,
        version=str(data.get("version", "1.0")),
        description=str(data.get("description", "")),
        max_concurrency=int(data.get("max_concurrency", 4)),
        vars=dict(data.get("vars") or {}) | dict(merged_vars),
        schedule=data.get("schedule"),
        source=Path(name),
        raw=data,
    )
    pipeline.to_dag()
    return pipeline
