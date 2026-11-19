"""Command-line interface for Autoflow."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TextIO

from autoflow import __version__
from autoflow.config import load_pipeline
from autoflow.exceptions import AutoflowError
from autoflow.executor import Executor
from autoflow.history import RunHistory
from autoflow.logging import configure_logging, get_logger
from autoflow.models import Status
from autoflow.plugins import register_builtin_tasks, registry
from autoflow.scheduler import Scheduler

DEFAULT_HISTORY = Path(".autoflow") / "history.sqlite"


def _history_path(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value)
    return DEFAULT_HISTORY


def _parse_vars(pairs: Sequence[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in pairs or ():
        if "=" not in raw:
            raise argparse.ArgumentTypeError(f"Expected key=value, got {raw!r}")
        key, value = raw.split("=", 1)
        if not key:
            raise argparse.ArgumentTypeError(f"Empty variable name in {raw!r}")
        result[key] = value
    return result


def _common_options(*, with_defaults: bool) -> argparse.ArgumentParser:
    """Flags accepted both before and after the subcommand.

    The top-level parser uses real defaults. Subparsers use
    ``argparse.SUPPRESS`` so a flag given *before* the subcommand is not
    overwritten by the subparser default.
    """
    common = argparse.ArgumentParser(add_help=False)
    log_default: str | object = "INFO" if with_defaults else argparse.SUPPRESS
    history_default: str | object | None = None if with_defaults else argparse.SUPPRESS
    common.add_argument(
        "--log-level",
        default=log_default,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    json_kwargs: dict[str, Any] = {
        "action": "store_true",
        "help": "Emit structured JSON logs to stderr.",
    }
    if not with_defaults:
        json_kwargs["default"] = argparse.SUPPRESS
    common.add_argument("--json-logs", **json_kwargs)
    common.add_argument(
        "--history",
        default=history_default,
        help=f"SQLite history path (default: {DEFAULT_HISTORY}).",
    )
    return common


def build_parser() -> argparse.ArgumentParser:
    top_opts = _common_options(with_defaults=True)
    sub_opts = _common_options(with_defaults=False)
    parser = argparse.ArgumentParser(
        prog="autoflow",
        description="Autoflow: run, validate, schedule, and test automation pipelines.",
        parents=[top_opts],
    )
    parser.add_argument("--version", action="version", version=f"autoflow {__version__}")

    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser(
        "run",
        help="Execute a pipeline from a YAML or JSON file.",
        parents=[sub_opts],
    )
    run.add_argument("pipeline", type=Path, help="Path to pipeline.yaml / pipeline.json")
    run.add_argument("--dry-run", action="store_true", help="Validate and plan, do not execute.")
    run.add_argument("--max-concurrency", type=int, default=None)
    run.add_argument(
        "--var",
        dest="vars",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override a pipeline variable (repeatable).",
    )
    run.add_argument("--workdir", type=Path, default=None)
    run.add_argument("--trigger", default="manual")

    ls = sub.add_parser(
        "list",
        help="List tasks in a pipeline, or registered task types.",
        parents=[sub_opts],
    )
    ls.add_argument("pipeline", type=Path, nargs="?", help="Optional pipeline file.")
    ls.add_argument(
        "--types",
        action="store_true",
        help="List registered task types instead of pipeline tasks.",
    )

    val = sub.add_parser(
        "validate",
        help="Validate a pipeline file without running it.",
        parents=[sub_opts],
    )
    val.add_argument("pipeline", type=Path)

    sched = sub.add_parser(
        "schedule",
        help="Run a pipeline on a cron schedule.",
        parents=[sub_opts],
    )
    sched.add_argument("pipeline", type=Path)
    sched.add_argument("--cron", default=None, help="Cron expression (overrides file schedule).")
    sched.add_argument("--interval", type=float, default=1.0, help="Poll interval in seconds.")
    sched.add_argument("--max-runs", type=int, default=None, help="Stop after N fires.")
    sched.add_argument(
        "--var",
        dest="vars",
        action="append",
        default=[],
        metavar="KEY=VALUE",
    )

    hist = sub.add_parser(
        "history",
        help="Show recorded pipeline runs.",
        parents=[sub_opts],
    )
    hist.add_argument("--run-id", default=None)
    hist.add_argument("--pipeline-name", default=None)
    hist.add_argument("--limit", type=int, default=20)
    hist.add_argument("--json", action="store_true", dest="as_json")

    test = sub.add_parser(
        "runtest",
        help="Run the project's pytest suite and write reports/ artifacts.",
        parents=[sub_opts],
    )
    test.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Extra args forwarded to pytest.",
    )
    test.add_argument("--coverage", action="store_true", help="Enable coverage collection.")
    test.add_argument(
        "--parallel",
        nargs="?",
        const="auto",
        default=None,
        help="Run tests in parallel (pytest-xdist). Optional worker count, default auto.",
    )
    test.add_argument(
        "-m",
        "--marker",
        default=None,
        help="Only run tests matching a pytest marker.",
    )
    test.add_argument("--junit", action="store_true", default=True, help="Write reports/junit.xml.")
    test.add_argument("--no-junit", action="store_false", dest="junit")
    test.add_argument(
        "--html-report",
        action="store_true",
        default=True,
        help="Write reports/summary.html (default: on).",
    )
    test.add_argument("--no-html-report", action="store_false", dest="html_report")
    test.add_argument("--reports-dir", type=Path, default=Path("reports"))
    return parser


def main(argv: Sequence[str] | None = None, *, stdout: TextIO | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    configure_logging(level=args.log_level, json_output=args.json_logs)
    out = stdout or sys.stdout
    try:
        return _dispatch(args, out)
    except AutoflowError as exc:
        get_logger("cli").error(str(exc))
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


def _dispatch(args: argparse.Namespace, out: TextIO) -> int:
    command = args.command
    if command == "run":
        return cmd_run(args, out)
    if command == "list":
        return cmd_list(args, out)
    if command == "validate":
        return cmd_validate(args, out)
    if command == "schedule":
        return cmd_schedule(args, out)
    if command == "history":
        return cmd_history(args, out)
    if command == "runtest":
        from autoflow.runtest_cmd import cmd_runtest

        return cmd_runtest(args, out)
    raise AssertionError(f"unhandled command: {command}")


def _load(args: argparse.Namespace, path: Path) -> Any:
    variables = _parse_vars(getattr(args, "vars", None))
    return load_pipeline(path, vars=variables)


def cmd_validate(args: argparse.Namespace, out: TextIO) -> int:
    pipeline = _load(args, args.pipeline)
    dag = pipeline.to_dag()
    levels = dag.levels()
    print(f"OK  {pipeline.name}  ({len(dag)} tasks, {len(levels)} level(s))", file=out)
    for i, level in enumerate(levels):
        print(f"  level {i}: {', '.join(level)}", file=out)
    return 0


def cmd_list(args: argparse.Namespace, out: TextIO) -> int:
    register_builtin_tasks(registry)
    registry.load_entry_points()
    if args.types or args.pipeline is None:
        names = registry.names()
        print("Registered task types:", file=out)
        for name in names:
            print(f"  - {name}", file=out)
        if args.pipeline is None and not args.types:
            return 0
        if args.pipeline is None:
            return 0
    pipeline = _load(args, args.pipeline)
    dag = pipeline.to_dag()
    print(f"Pipeline: {pipeline.name}", file=out)
    if pipeline.description:
        print(f"  {pipeline.description}", file=out)
    print(f"  version: {pipeline.version}", file=out)
    print(f"  max_concurrency: {pipeline.max_concurrency}", file=out)
    if pipeline.schedule:
        print(f"  schedule: {pipeline.schedule}", file=out)
    print("  tasks:", file=out)
    for spec in dag.topological_order():
        task = dag.get(spec)
        deps = f"  depends_on={task.depends_on}" if task.depends_on else ""
        print(f"    - {task.name}  [{task.task_type}]{deps}", file=out)
    return 0


def cmd_run(args: argparse.Namespace, out: TextIO) -> int:
    pipeline = _load(args, args.pipeline)
    dag = pipeline.to_dag()
    workdir = args.workdir or (pipeline.source.parent if pipeline.source else Path.cwd())
    hist_path = _history_path(args.history)
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    history = RunHistory(hist_path)
    try:
        executor = Executor(
            max_concurrency=args.max_concurrency or pipeline.max_concurrency,
            history=history,
            workdir=workdir,
        )
        result = asyncio.run(
            executor.run(
                dag,
                vars=pipeline.vars,
                trigger=args.trigger,
                dry_run=args.dry_run,
            )
        )
    finally:
        history.close()
    _print_run(result, out)
    return 0 if result.status == Status.SUCCESS else 1


def _print_run(result: Any, out: TextIO) -> None:
    duration = f"{result.duration_s:.3f}s" if result.duration_s is not None else "?"
    print(
        f"run {result.run_id}  {result.pipeline}  {result.status.value}  ({duration})",
        file=out,
    )
    for name, task in result.tasks.items():
        extra = ""
        if task.error:
            extra = f"  error={task.error}"
        elif task.skipped_reason:
            extra = f"  reason={task.skipped_reason}"
        print(
            f"  {task.status.value:<9} {name}  attempts={task.attempts}{extra}",
            file=out,
        )


def cmd_history(args: argparse.Namespace, out: TextIO) -> int:
    path = _history_path(args.history)
    if not path.exists() and path != Path(":memory:"):
        print(f"No history database at {path}", file=out)
        return 0
    history = RunHistory(path)
    try:
        if args.run_id:
            run = history.get_run(args.run_id)
            if run is None:
                print(f"No run with id {args.run_id}", file=out)
                return 1
            if args.as_json:
                print(_run_to_json(run), file=out)
            else:
                _print_run(run, out)
            return 0
        runs = history.list_runs(pipeline=args.pipeline_name, limit=args.limit)
        if args.as_json:
            payload = [
                {
                    "run_id": r.run_id,
                    "pipeline": r.pipeline,
                    "status": r.status.value,
                    "trigger": r.trigger,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                }
                for r in runs
            ]
            print(json.dumps(payload, indent=2), file=out)
            return 0
        if not runs:
            print("No runs recorded.", file=out)
            return 0
        for run in runs:
            started = run.started_at.isoformat() if run.started_at else "-"
            print(
                f"{run.run_id}  {run.pipeline:<20}  {run.status.value:<9}  {started}",
                file=out,
            )
        return 0
    finally:
        history.close()


def _run_to_json(run: Any) -> str:
    return json.dumps(
        {
            "run_id": run.run_id,
            "pipeline": run.pipeline,
            "status": run.status.value,
            "trigger": run.trigger,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "error": run.error,
            "tasks": {
                name: {
                    "status": t.status.value,
                    "attempts": t.attempts,
                    "error": t.error,
                    "output": t.output,
                    "skipped_reason": t.skipped_reason,
                }
                for name, t in run.tasks.items()
            },
        },
        indent=2,
        default=str,
    )


def cmd_schedule(args: argparse.Namespace, out: TextIO) -> int:
    pipeline = _load(args, args.pipeline)
    cron = args.cron or pipeline.schedule
    if not cron:
        print("error: no cron expression given and pipeline has no schedule", file=sys.stderr)
        return 2

    hist_path = _history_path(args.history)
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    workdir = pipeline.source.parent if pipeline.source else Path.cwd()

    async def fire() -> None:
        history = RunHistory(hist_path)
        try:
            executor = Executor(
                max_concurrency=pipeline.max_concurrency,
                history=history,
                workdir=workdir,
            )
            result = await executor.run(
                pipeline.to_dag(),
                vars=pipeline.vars,
                trigger=f"schedule:{cron}",
            )
            _print_run(result, out)
        finally:
            history.close()

    scheduler = Scheduler()
    scheduler.add(pipeline.name, cron, fire, max_runs=args.max_runs)
    print(
        f"scheduling {pipeline.name} with cron {cron!r}"
        + (f" (max_runs={args.max_runs})" if args.max_runs else ""),
        file=out,
    )
    try:
        asyncio.run(scheduler.run_forever(interval=args.interval))
    except KeyboardInterrupt:
        scheduler.stop()
        print("scheduler stopped", file=out)
    return 0


# Imported by tests that want to pass extra kwargs without going through argv.
def run_pipeline_file(
    path: Path,
    *,
    vars: Mapping[str, str] | None = None,
    dry_run: bool = False,
    history_path: Path | None = None,
    workdir: Path | None = None,
    max_concurrency: int | None = None,
) -> Any:
    pipeline = load_pipeline(path, vars=vars)
    dag = pipeline.to_dag()
    history = RunHistory(history_path or ":memory:")
    executor = Executor(
        max_concurrency=max_concurrency or pipeline.max_concurrency,
        history=history,
        workdir=workdir or Path.cwd(),
    )
    try:
        return asyncio.run(executor.run(dag, vars=pipeline.vars, dry_run=dry_run))
    finally:
        history.close()
