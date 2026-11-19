"""Tests for cron parsing and the in-process scheduler."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from autoflow.exceptions import CronError
from autoflow.scheduler import CronSchedule, Scheduler


def ts(year=2026, month=1, day=5, hour=10, minute=0) -> datetime:
    # 2026-01-05 is a Monday.
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("expr", "when", "expected"),
    [
        ("* * * * *", ts(), True),
        ("0 0 * * *", ts(hour=0, minute=0), True),
        ("0 0 * * *", ts(hour=0, minute=1), False),
        ("*/5 * * * *", ts(minute=10), True),
        ("*/5 * * * *", ts(minute=11), False),
        ("0 9-17 * * 1-5", ts(hour=9, minute=0), True),
        ("0 9-17 * * 1-5", ts(hour=8, minute=0), False),
        ("0 9-17 * * 1-5", ts(day=4, hour=9, minute=0), False),  # Sunday
        ("0 0 1 JAN *", ts(month=1, day=1, hour=0, minute=0), True),
        ("0 0 * * MON", ts(hour=0, minute=0), True),
        ("0,30 * * * *", ts(minute=30), True),
        ("0,30 * * * *", ts(minute=15), False),
    ],
)
def test_cron_matches(expr: str, when: datetime, expected: bool) -> None:
    assert CronSchedule(expr).matches(when) is expected


def test_next_after_crosses_hour() -> None:
    sched = CronSchedule("0 * * * *")
    nxt = sched.next_after(ts(hour=10, minute=15))
    assert nxt == ts(hour=11, minute=0)


def test_next_after_inclusive() -> None:
    sched = CronSchedule("0 10 * * *")
    at = ts(hour=10, minute=0)
    assert sched.next_after(at, inclusive=True) == at
    assert sched.next_after(at, inclusive=False) == ts(day=6, hour=10, minute=0)


def test_invalid_cron_expressions() -> None:
    with pytest.raises(CronError, match="5 fields"):
        CronSchedule("* * *")
    with pytest.raises(CronError):
        CronSchedule("60 * * * *")
    with pytest.raises(CronError):
        CronSchedule("abc * * * *")
    with pytest.raises(CronError):
        CronSchedule("*/0 * * * *")
    with pytest.raises(CronError):
        CronSchedule("")


def test_dow_seven_is_sunday() -> None:
    # 2026-01-04 is a Sunday.
    sunday = ts(day=4, hour=0, minute=0)
    assert CronSchedule("0 0 * * 7").matches(sunday)
    assert CronSchedule("0 0 * * 0").matches(sunday)


@pytest.mark.asyncio
async def test_scheduler_tick_fires_and_reschedules() -> None:
    clock = {"now": ts()}

    def now() -> datetime:
        return clock["now"]

    fired: list[str] = []

    async def job() -> None:
        fired.append("x")

    sched = Scheduler(clock=now)
    sched.add("x", "0 11 * * *", job, max_runs=2)
    assert sched.get("x").next_run_at == ts(hour=11, minute=0)
    # Not due yet.
    assert await sched.tick(ts(hour=10, minute=0)) == []
    # Due at 11:00.
    clock["now"] = ts(hour=11, minute=0)
    assert await sched.tick() == ["x"]
    assert fired == ["x"]
    assert sched.get("x").runs == 1
    assert sched.get("x").next_run_at == ts(day=6, hour=11, minute=0)
    # Second fire hits max_runs and clears next_run_at.
    clock["now"] = ts(day=6, hour=11, minute=0)
    assert await sched.tick() == ["x"]
    assert sched.get("x").next_run_at is None
    assert await sched.tick() == []


@pytest.mark.asyncio
async def test_scheduler_rejects_duplicate_and_remove() -> None:
    sched = Scheduler(clock=lambda: ts())
    sched.add("a", "* * * * *", lambda: None)
    with pytest.raises(ValueError, match="already scheduled"):
        sched.add("a", "* * * * *", lambda: None)
    assert len(sched.jobs()) == 1
    sched.remove("a")
    assert sched.jobs() == []


@pytest.mark.asyncio
async def test_run_until_simulated_clock() -> None:
    fired: list[datetime] = []
    start = ts(minute=0)

    def job() -> None:
        fired.append(True)  # type: ignore[arg-type]

    sched = Scheduler(clock=lambda: start)
    sched.add("every5", "*/5 * * * *", job)
    names = await sched.run_until(ts(minute=16), step=timedelta(minutes=1))
    # Fires at 10:05, 10:10, 10:15 (not 10:00 because next_after is exclusive).
    assert names.count("every5") == 3


@pytest.mark.asyncio
async def test_run_forever_stops() -> None:
    sched = Scheduler()
    sched.add("n", "* * * * *", lambda: None, max_runs=0)

    async def stopper() -> None:
        await asyncio_sleep_tiny()
        sched.stop()

    async def asyncio_sleep_tiny() -> None:
        import asyncio

        await asyncio.sleep(0.02)

    import asyncio

    await asyncio.gather(sched.run_forever(interval=0.01), stopper())
