"""Tests for autoflow runtest reporting helpers and CLI wiring."""

from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoflow.runtest_cmd import (
    _parse_coverage,
    _parse_junit,
    _render_html,
    cmd_runtest,
)

JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" tests="3" failures="1" errors="0" skipped="1" time="0.4">
    <testcase classname="tests.test_a" name="test_ok" time="0.1"/>
    <testcase classname="tests.test_a" name="test_fail" time="0.2">
      <failure message="boom">assertion failed</failure>
    </testcase>
    <testcase classname="tests.test_a" name="test_skip" time="0.0">
      <skipped message="not now"/>
    </testcase>
  </testsuite>
</testsuites>
"""

COVERAGE = """<?xml version="1.0" ?>
<coverage line-rate="0.875" branch-rate="0.5" version="7.0">
</coverage>
"""


def test_parse_junit_and_coverage(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(JUNIT)
    cov = tmp_path / "coverage.xml"
    cov.write_text(COVERAGE)
    summary = _parse_junit(junit)
    assert summary["tests"] == 3
    assert summary["failed"] == 1
    assert summary["skipped"] == 1
    assert summary["passed"] == 1
    assert summary["cases"][1]["status"] == "failed"
    assert _parse_coverage(cov) == 87.5


def test_render_html_escapes() -> None:
    html = _render_html(
        {
            "exit_code": 1,
            "generated_at": "2026-01-01T00:00:00Z",
            "tests": 1,
            "passed": 0,
            "failed": 1,
            "skipped": 0,
            "errors": 0,
            "time": 0.2,
            "coverage": 50.0,
            "cases": [
                {
                    "classname": "t",
                    "name": "test_<x>",
                    "status": "failed",
                    "time": 0.2,
                    "message": "a & b",
                }
            ],
        }
    )
    assert "test_&lt;x&gt;" in html
    assert "a &amp; b" in html
    assert "50.0%" in html
    assert "failed" in html
    # Empty cases path
    html2 = _render_html({"exit_code": 0, "cases": [], "coverage": None})
    assert "No test cases recorded" in html2


def test_cmd_runtest_invokes_pytest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reports = tmp_path / "reports"

    def fake_pytest(args: list[str]) -> int:
        junit = reports / "junit.xml"
        reports.mkdir(parents=True, exist_ok=True)
        junit.write_text(JUNIT)
        (reports / "coverage.xml").write_text(COVERAGE)
        assert "--cov=autoflow" in args
        assert "-n" in args and "auto" in args
        assert "-m" in args and "not slow" in args
        return 0

    monkeypatch.setattr("autoflow.runtest_cmd._run_pytest", fake_pytest)
    args = SimpleNamespace(
        reports_dir=reports,
        junit=True,
        coverage=True,
        parallel="auto",
        marker="not slow",
        html_report=True,
        pytest_args=["--", "-k", "test_ok"],
    )
    out = io.StringIO()
    code = cmd_runtest(args, out)
    assert code == 0
    text = out.getvalue()
    assert "wrote" in text
    assert "coverage: 87.5%" in text
    assert (reports / "summary.html").exists()
    assert "passed: 1" in text


def test_run_pytest_without_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    from autoflow import runtest_cmd

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "pytest":
            raise ImportError("no pytest")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert runtest_cmd._run_pytest(["-q"]) == 2
