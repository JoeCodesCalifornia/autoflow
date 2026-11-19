"""Tests for the autoflow CLI."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from autoflow.cli import build_parser, main, run_pipeline_file
from autoflow.models import Status


def _run(argv: list[str]) -> tuple[int, str, str]:
    out = io.StringIO()
    err = io.StringIO()
    # main prints errors to sys.stderr; capture via capsys in some tests.
    code = main(argv, stdout=out)
    return code, out.getvalue(), err.getvalue()


def test_parser_requires_command() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_validate_and_list(hello_pipeline: Path) -> None:
    code, out, _ = _run(["validate", str(hello_pipeline)])
    assert code == 0
    assert "OK" in out
    assert "greet" in out
    code, out, _ = _run(["list", str(hello_pipeline)])
    assert code == 0
    assert "Pipeline: hello" in out
    assert "greet" in out
    code, out, _ = _run(["list", "--types"])
    assert code == 0
    assert "shell" in out
    assert "http.request" in out


def test_validate_rejects_cycle(fixtures_dir: Path) -> None:
    code, _, _ = _run(["validate", str(fixtures_dir / "cycle.yaml")])
    assert code == 2


def test_run_pipeline_writes_history(hello_pipeline: Path, tmp_path: Path) -> None:
    hist = tmp_path / "h.sqlite"
    code, out, _ = _run(
        [
            "--history",
            str(hist),
            "run",
            str(hello_pipeline),
            "--workdir",
            str(tmp_path),
            "--var",
            "GREETING=yo",
        ]
    )
    assert code == 0
    assert "success" in out
    assert (tmp_path / "out.txt").read_text() == "yo"
    code, out, _ = _run(["--history", str(hist), "history"])
    assert code == 0
    assert "hello" in out
    code, out, _ = _run(["--history", str(hist), "history", "--json", "--limit", "5"])
    assert code == 0
    payload = json.loads(out)
    assert payload[0]["pipeline"] == "hello"
    run_id = payload[0]["run_id"]
    code, out, _ = _run(["--history", str(hist), "history", "--run-id", run_id, "--json"])
    assert code == 0
    detail = json.loads(out)
    assert detail["status"] == "success"
    assert "greet" in detail["tasks"]


def test_run_dry_run(hello_pipeline: Path, tmp_path: Path) -> None:
    code, out, _ = _run(
        [
            "run",
            str(hello_pipeline),
            "--dry-run",
            "--workdir",
            str(tmp_path),
            "--history",
            str(tmp_path / "h.db"),
        ]
    )
    assert code == 0
    assert "success" in out
    assert not (tmp_path / "out.txt").exists()


def test_run_pipeline_file_helper(hello_pipeline: Path, tmp_path: Path) -> None:
    result = run_pipeline_file(
        hello_pipeline,
        vars={"GREETING": "z"},
        history_path=tmp_path / "h.db",
        workdir=tmp_path,
    )
    assert result.status == Status.SUCCESS
    assert (tmp_path / "out.txt").read_text() == "z"


def test_history_missing_db_and_missing_run(tmp_path: Path) -> None:
    code, out, _ = _run(["--history", str(tmp_path / "none.db"), "history"])
    assert code == 0
    assert "No history" in out
    # Create an empty history via a dry run so the file exists, then look up a fake id.
    hello = tmp_path / "p.yaml"
    hello.write_text("name: p\ntasks:\n  - name: t\n    type: files.mkdir\n    params: {path: d}\n")
    _run(
        [
            "--history",
            str(tmp_path / "h.db"),
            "run",
            str(hello),
            "--dry-run",
            "--workdir",
            str(tmp_path),
        ]
    )
    code, out, _ = _run(["--history", str(tmp_path / "h.db"), "history", "--run-id", "missing"])
    assert code == 1
    assert "No run" in out


def test_bad_var_is_rejected(hello_pipeline: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(["run", str(hello_pipeline), "--var", "not-a-pair"])
    from autoflow.cli import _parse_vars

    with pytest.raises(Exception, match="key=value"):
        _parse_vars(args.vars)


def test_version_and_help() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--version"])
    assert exc.value.code == 0
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--help"])
    assert exc.value.code == 0
