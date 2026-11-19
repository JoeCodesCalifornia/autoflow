"""Tests for status and result models."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from autoflow.models import RunResult, Status, TaskResult


def test_status_flags() -> None:
    assert Status.SUCCESS.is_terminal
    assert Status.FAILED.is_terminal
    assert Status.SKIPPED.is_terminal
    assert Status.CANCELLED.is_terminal
    assert not Status.PENDING.is_terminal
    assert not Status.RUNNING.is_terminal
    assert Status.SUCCESS.ok
    assert Status.SKIPPED.ok
    assert not Status.FAILED.ok


def test_durations_and_run_lists() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=2.5)
    t_ok = TaskResult(name="a", status=Status.SUCCESS, started_at=start, finished_at=end)
    t_fail = TaskResult(name="b", status=Status.FAILED)
    t_skip = TaskResult(name="c", status=Status.SKIPPED)
    assert t_ok.duration_s == 2.5
    assert t_fail.duration_s is None
    run = RunResult(
        run_id="r",
        pipeline="p",
        status=Status.FAILED,
        tasks={"a": t_ok, "b": t_fail, "c": t_skip},
        started_at=start,
        finished_at=end,
    )
    assert run.duration_s == 2.5
    assert run.succeeded == ["a"]
    assert run.failed == ["b"]
    assert run.skipped == ["c"]
