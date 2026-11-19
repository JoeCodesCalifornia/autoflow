"""Tests for SQLite run history."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from autoflow.history import RunHistory
from autoflow.models import RunResult, Status, TaskResult


def _run(run_id: str = "r1", pipeline: str = "p", status: Status = Status.RUNNING) -> RunResult:
    now = datetime.now(tz=timezone.utc)
    return RunResult(
        run_id=run_id,
        pipeline=pipeline,
        status=status,
        started_at=now,
        trigger="manual",
        tasks={
            "t": TaskResult(
                name="t",
                status=Status.SUCCESS,
                attempts=1,
                started_at=now,
                finished_at=now,
                output={"x": 1},
            )
        },
    )


def test_record_and_get_roundtrip(tmp_path: Path) -> None:
    with RunHistory(tmp_path / "h.db") as hist:
        run = _run()
        hist.record_run_start(run)
        hist.record_task(run.run_id, run.tasks["t"])
        run.status = Status.SUCCESS
        run.finished_at = run.started_at
        hist.record_run_finish(run)
        got = hist.get_run("r1")
        assert got is not None
        assert got.status == Status.SUCCESS
        assert got.tasks["t"].output == {"x": 1}
        assert got.tasks["t"].attempts == 1


def test_list_runs_filters(tmp_path: Path) -> None:
    hist = RunHistory(tmp_path / "h.db")
    try:
        a = _run("a", "pipe-a", Status.SUCCESS)
        b = _run("b", "pipe-b", Status.FAILED)
        a.finished_at = a.started_at
        b.finished_at = b.started_at
        hist.record_run_start(a)
        hist.record_run_finish(a)
        hist.record_run_start(b)
        hist.record_run_finish(b)
        all_runs = hist.list_runs()
        assert {r.run_id for r in all_runs} == {"a", "b"}
        only_a = hist.list_runs(pipeline="pipe-a")
        assert [r.run_id for r in only_a] == ["a"]
        failed = hist.list_runs(status=Status.FAILED)
        assert [r.run_id for r in failed] == ["b"]
        assert hist.get_run("missing") is None
    finally:
        hist.close()


def test_memory_history_and_non_jsonable_output() -> None:
    hist = RunHistory(":memory:")
    try:
        run = _run("m")
        run.tasks["t"].output = object()  # not JSON-serializable
        hist.record_run_start(run)
        hist.record_task("m", run.tasks["t"])
        tasks = hist.task_results("m")
        assert len(tasks) == 1
        # Stored as a JSON string of repr(...)
        assert tasks[0].output is not None
    finally:
        hist.close()


def test_task_upsert_updates_existing_row() -> None:
    hist = RunHistory(":memory:")
    try:
        run = _run("u")
        hist.record_run_start(run)
        first = TaskResult(name="t", status=Status.RUNNING, attempts=1)
        hist.record_task("u", first)
        second = TaskResult(name="t", status=Status.SUCCESS, attempts=2, output="done")
        hist.record_task("u", second)
        tasks = hist.task_results("u")
        assert len(tasks) == 1
        assert tasks[0].status == Status.SUCCESS
        assert tasks[0].attempts == 2
        assert tasks[0].output == "done"
    finally:
        hist.close()
