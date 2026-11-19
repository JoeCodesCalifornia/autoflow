"""Implementation of ``autoflow runtest`` and the report generator."""

from __future__ import annotations

import argparse
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

from autoflow.logging import get_logger

log = get_logger("runtest")


def cmd_runtest(args: argparse.Namespace, out: TextIO) -> int:
    """Run pytest with Autoflow's preferred reporting options."""
    reports_dir: Path = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    junit_path = reports_dir / "junit.xml"
    coverage_xml = reports_dir / "coverage.xml"
    html_path = reports_dir / "summary.html"

    pytest_args: list[str] = ["-q", "--color=no"]
    if args.junit:
        pytest_args.extend(["--junitxml", str(junit_path)])
    if args.coverage:
        pytest_args.extend(
            [
                "--cov=autoflow",
                "--cov-report=term-missing",
                f"--cov-report=xml:{coverage_xml}",
            ]
        )
    if args.parallel:
        pytest_args.extend(["-n", str(args.parallel)])
    if args.marker:
        pytest_args.extend(["-m", args.marker])

    extra = list(args.pytest_args or [])
    # argparse.REMAINDER may include a leading "--" used as a separator.
    if extra and extra[0] == "--":
        extra = extra[1:]
    pytest_args.extend(extra)

    log.info("running pytest", extra={"pytest_args": pytest_args})
    print(f"autoflow runtest: pytest {' '.join(pytest_args)}", file=out)
    exit_code = _run_pytest(pytest_args)

    summary = {
        "exit_code": exit_code,
        "generated_at": datetime.now(tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "tests": 0,
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "errors": 0,
        "time": 0.0,
        "coverage": None,
        "cases": [],
    }
    if args.junit and junit_path.exists():
        summary.update(_parse_junit(junit_path))
    if args.coverage and coverage_xml.exists():
        summary["coverage"] = _parse_coverage(coverage_xml)
    if args.html_report:
        html_path.write_text(_render_html(summary), encoding="utf-8")
        print(f"wrote {html_path}", file=out)
    if args.junit and junit_path.exists():
        print(f"wrote {junit_path}", file=out)
    if args.coverage and coverage_xml.exists():
        print(f"wrote {coverage_xml}", file=out)
        if summary["coverage"] is not None:
            print(f"coverage: {summary['coverage']:.1f}%", file=out)

    print(
        (
            "tests: {tests}  passed: {passed}  failed: {failed}  "
            "skipped: {skipped}  errors: {errors}"
        ).format(**{k: summary[k] for k in ("tests", "passed", "failed", "skipped", "errors")}),
        file=out,
    )
    return int(exit_code)


def _run_pytest(pytest_args: list[str]) -> int:
    try:
        import pytest  # noqa: F401
    except ImportError:
        print(
            "error: pytest is not installed. Install autoflow with the dev extra:\n"
            "  pip install -e '.[dev]'",
            file=sys.stderr,
        )
        return 2
    # A fresh interpreter is required so pytest-cov can measure modules that
    # autoflow itself already imported to get this far.
    return int(subprocess.call([sys.executable, "-m", "pytest", *pytest_args]))


def _parse_junit(path: Path) -> dict[str, Any]:
    tree = ET.parse(path)
    root = tree.getroot()
    # pytest may write <testsuites> or a bare <testsuite>.
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    tests = failures = errors = skipped = 0
    time_s = 0.0
    cases: list[dict[str, Any]] = []
    for suite in suites:
        tests += int(suite.attrib.get("tests", 0))
        failures += int(suite.attrib.get("failures", 0))
        errors += int(suite.attrib.get("errors", 0))
        skipped += int(suite.attrib.get("skipped", 0) or suite.attrib.get("skips", 0))
        time_s += float(suite.attrib.get("time", 0) or 0)
        for case in suite.findall("testcase"):
            status = "passed"
            message = ""
            failure_el = case.find("failure")
            error_el = case.find("error")
            if failure_el is not None:
                status = "failed"
                message = (failure_el.attrib.get("message") or "")[:400]
            elif error_el is not None:
                status = "error"
                message = (error_el.attrib.get("message") or "")[:400]
            elif case.find("skipped") is not None:
                status = "skipped"
            cases.append(
                {
                    "name": case.attrib.get("name", ""),
                    "classname": case.attrib.get("classname", ""),
                    "time": float(case.attrib.get("time", 0) or 0),
                    "status": status,
                    "message": message,
                }
            )
    passed = tests - failures - errors - skipped
    return {
        "tests": tests,
        "passed": max(0, passed),
        "failed": failures,
        "skipped": skipped,
        "errors": errors,
        "time": time_s,
        "cases": cases,
    }


def _parse_coverage(path: Path) -> float | None:
    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return None
    root = tree.getroot()
    rate = root.attrib.get("line-rate")
    if rate is None:
        return None
    try:
        return round(float(rate) * 100.0, 2)
    except ValueError:
        return None


def _render_html(summary: dict[str, Any]) -> str:
    cov = summary.get("coverage")
    cov_cell = f"{cov:.1f}%" if isinstance(cov, float) else "n/a"
    status = "passed" if summary.get("exit_code") == 0 else "failed"
    rows = []
    for case in summary.get("cases") or []:
        css = case["status"]
        msg = _escape(case.get("message") or "")
        rows.append(
            "<tr class='{css}'>"
            "<td>{cls}</td><td>{name}</td><td>{status}</td>"
            "<td>{time:.3f}s</td><td>{msg}</td></tr>".format(
                css=css,
                cls=_escape(case.get("classname", "")),
                name=_escape(case.get("name", "")),
                status=case["status"],
                time=float(case.get("time") or 0),
                msg=msg,
            )
        )
    body = "\n".join(rows) or "<tr><td colspan='5'>No test cases recorded.</td></tr>"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Autoflow Test Report</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem; color: #111; }}
    h1 {{ margin-bottom: 0.2rem; }}
    .muted {{ color: #555; }}
    .badge {{ display: inline-block; padding: 0.15rem 0.6rem; border-radius: 999px; color: #fff; }}
    .badge.passed {{ background: #15803d; }}
    .badge.failed {{ background: #b91c1c; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 1.5rem; }}
    th, td {{ border: 1px solid #ddd; padding: 0.45rem 0.6rem; text-align: left;
              font-size: 0.92rem; }}
    th {{ background: #f4f4f5; }}
    tr.failed, tr.error {{ background: #fef2f2; }}
    tr.skipped {{ background: #fffbeb; }}
    .stats {{ display: flex; gap: 1.5rem; margin-top: 1rem; }}
    .stat {{ background: #f4f4f5; padding: 0.8rem 1rem; border-radius: 8px; min-width: 6rem; }}
    .stat b {{ display: block; font-size: 1.3rem; }}
  </style>
</head>
<body>
  <h1>Autoflow Test Report <span class="badge {status}">{status}</span></h1>
  <p class="muted">Generated at {_escape(str(summary.get("generated_at", "")))}</p>
  <div class="stats">
    <div class="stat"><b>{summary.get("tests", 0)}</b> tests</div>
    <div class="stat"><b>{summary.get("passed", 0)}</b> passed</div>
    <div class="stat"><b>{summary.get("failed", 0)}</b> failed</div>
    <div class="stat"><b>{summary.get("skipped", 0)}</b> skipped</div>
    <div class="stat"><b>{summary.get("errors", 0)}</b> errors</div>
    <div class="stat"><b>{cov_cell}</b> coverage</div>
    <div class="stat"><b>{float(summary.get("time") or 0):.2f}s</b> duration</div>
  </div>
  <table>
    <thead>
      <tr><th>Class</th><th>Test</th><th>Status</th><th>Time</th><th>Message</th></tr>
    </thead>
    <tbody>
      {body}
    </tbody>
  </table>
</body>
</html>
"""


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )
