"""Tests for pipeline config loading, interpolation, and validation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoflow.config import interpolate, load_pipeline, loads_pipeline, validate_document
from autoflow.exceptions import ConfigError, CycleError, ValidationError


def test_load_yaml_and_json(fixtures_dir: Path) -> None:
    yml = load_pipeline(fixtures_dir / "valid.yaml")
    assert yml.name == "fixture-valid"
    assert [t.name for t in yml.tasks] == ["one", "two"]
    assert yml.max_concurrency == 3
    dag = yml.to_dag()
    assert dag.topological_order() == ["one", "two"]

    js = load_pipeline(fixtures_dir / "valid.json")
    assert js.name == "fixture-json"
    assert js.tasks[0].task_type == "shell"


def test_cycle_in_config_is_rejected(fixtures_dir: Path) -> None:
    with pytest.raises((CycleError, ValidationError)):
        load_pipeline(fixtures_dir / "cycle.yaml")


def test_invalid_document(fixtures_dir: Path) -> None:
    with pytest.raises(ValidationError):
        load_pipeline(fixtures_dir / "invalid.yaml")


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="Cannot read"):
        load_pipeline(tmp_path / "nope.yaml")


def test_non_mapping_root(tmp_path: Path) -> None:
    path = tmp_path / "list.yaml"
    path.write_text("- just a list\n")
    with pytest.raises(ConfigError, match="mapping"):
        load_pipeline(path)


def test_bad_yaml(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("name: [unterminated\n")
    with pytest.raises(ConfigError, match="Failed to parse"):
        load_pipeline(path)


def test_interpolation_and_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FROM_ENV", "env-value")
    path = tmp_path / "p.yaml"
    path.write_text(
        "\n".join(
            [
                "name: interp",
                "vars:",
                "  FILE: default.txt",
                "tasks:",
                "  - name: w",
                "    type: files.write",
                "    params:",
                "      path: ${FILE}",
                "      content: '${GREETING:hi} ${FROM_ENV}'",
                "",
            ]
        )
    )
    pipeline = load_pipeline(path, vars={"FILE": "custom.txt", "FROM_ENV": "env-value"})
    # FILE overridden, GREETING uses default, FROM_ENV from vars (or env).
    assert pipeline.tasks[0].params["path"] == "custom.txt"
    assert "hi" in pipeline.tasks[0].params["content"]
    assert "env-value" in pipeline.tasks[0].params["content"]


def test_unresolved_variable() -> None:
    with pytest.raises(ConfigError, match="Unresolved"):
        interpolate("hello ${MISSING}", {})


def test_interpolate_nested_structures() -> None:
    data = {"a": ["${X}", {"b": "${Y:z}"}], "n": 3}
    assert interpolate(data, {"X": "1"}) == {"a": ["1", {"b": "z"}], "n": 3}


def test_loads_pipeline_from_string() -> None:
    text = """
name: mem
tasks:
  - name: t
    type: files.mkdir
    params:
      path: work
"""
    pipeline = loads_pipeline(text)
    assert pipeline.name == "mem"
    assert pipeline.tasks[0].name == "t"

    blob = json.dumps(
        {
            "name": "from-json",
            "tasks": [{"name": "t", "type": "files.mkdir", "params": {"path": "w"}}],
        }
    )
    assert loads_pipeline(blob).name == "from-json"


def test_validate_document_collects_errors() -> None:
    errors = validate_document(
        {
            "max_concurrency": 0,
            "vars": "nope",
            "schedule": 5,
            "tasks": [
                "not-a-map",
                {"name": "a", "retries": -1, "timeout": 0, "params": [], "mystery": 1},
                {"name": "a", "type": 1, "depends_on": 3},
            ],
        }
    )
    joined = " ".join(errors)
    assert "Missing required field" in joined
    assert "max_concurrency" in joined
    assert "vars" in joined
    assert "schedule" in joined
    assert "Duplicate task name" in joined


def test_depends_on_string_form(tmp_path: Path) -> None:
    path = tmp_path / "p.yaml"
    path.write_text(
        "\n".join(
            [
                "name: s",
                "tasks:",
                "  - name: a",
                "    type: files.mkdir",
                "    params: {path: w}",
                "  - name: b",
                "    type: files.mkdir",
                "    depends_on: a",
                "    params: {path: w/x}",
                "",
            ]
        )
    )
    pipeline = load_pipeline(path)
    assert pipeline.tasks[1].depends_on == ["a"]
