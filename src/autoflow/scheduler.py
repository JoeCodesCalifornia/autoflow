"""Cron-like scheduler for repeating pipeline runs."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from autoflow.exceptions import CronError
from autoflow.logging import get_logger

log = get_logger("scheduler")

# 5-field cron: minute hour day-of-month month day-of-week
_FIELD_RANGES = {
    "minute": (0, 59),
    "hour": (0, 23),
    "dom": (1, 31),
    "month": (1, 12),
    "dow": (0, 6),  # 0 = Sunday, 7 also accepted as Sunday
}

_MONTH_NAMES = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_DOW_NAMES = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}

_TOKEN = re.compile(r"^[A-Za-z0-9*/,?-]+$")


def _expand_field(expr: str, kind: str) -> set[int]:
    lo, hi = _FIELD_RANGES[kind]
    expr = expr.strip()
    if not expr:
        raise CronError(f"Empty {kind} field")
    if kind == "month":
        for name, num in _MONTH_NAMES.items():
            expr = re.sub(name, str(num), expr, flags=re.IGNORECASE)
    if kind == "dow":
        for name, num in _DOW_NAMES.items():
            expr = re.sub(name, str(num), expr, flags=re.IGNORECASE)
    if not _TOKEN.match(expr):
        raise CronError(f"Invalid {kind} field: {expr!r}")

    values: set[int] = set()
    for part in expr.split(","):
        if not part:
            raise CronError(f"Invalid {kind} field: {expr!r}")
        step = 1
        body = part
        if "/" in part:
            body, step_s = part.split("/", 1)
            if not step_s.isdigit() or int(step_s) < 1:
                raise CronError(f"Invalid step in {kind} field: {part!r}")
            step = int(step_s)
        if body in {"*", "?"}:
            start, end = lo, hi
        elif "-" in body:
            a, b = body.split("-", 1)
            if not a.isdigit() or not b.isdigit():
                raise CronError(f"Invalid range in {kind} field: {part!r}")
            start, end = int(a), int(b)
        else:
            if not body.isdigit():
                raise CronError(f"Invalid value in {kind} field: {part!r}")
            start = end = int(body)
        if kind == "dow":
            # Accept 7 as Sunday.
            if start == 7:
                start = 0
            if end == 7:
                end = 0 if start == 0 else 7
        out_of_range = start > end or start < lo or (end > hi and not (kind == "dow" and end == 7))
        # Allow end==7 for dow ranges like 1-7, then map 7 -> 0
        if out_of_range and not (kind == "dow" and start >= lo and end == 7):
            raise CronError(f"Out of range {kind} field: {part!r}")
        if kind == "dow" and end == 7:
            for v in range(start, 8, step):
                values.add(0 if v == 7 else v)
        else:
            for v in range(start, end + 1, step):
                values.add(v)
    return values


class CronSchedule:
    """A 5-field cron expression (minute hour day month weekday)."""

    def __init__(self, expr: str) -> None:
        self.expr = expr.strip()
        parts = self.expr.split()
        if len(parts) != 5:
            raise CronError(f"Cron expression must have 5 fields (got {len(parts)}): {expr!r}")
        self.minutes = _expand_field(parts[0], "minute")
        self.hours = _expand_field(parts[1], "hour")
        self.doms = _expand_field(parts[2], "dom")
        self.months = _expand_field(parts[3], "month")
        self.dows = _expand_field(parts[4], "dow")

    def matches(self, dt: datetime) -> bool:
        """Return True if ``dt`` (to the minute) matches this schedule."""
        # Sunday is 6 in datetime.weekday(); cron uses 0.
        dow = (dt.weekday() + 1) % 7
        return (
            dt.minute in self.minutes
            and dt.hour in self.hours
            and dt.day in self.doms
            and dt.month in self.months
            and dow in self.dows
        )

    def next_after(self, dt: datetime, *, inclusive: bool = False) -> datetime:
        """Return the next matching minute strictly after ``dt`` (or at ``dt``)."""
        candidate = dt.replace(second=0, microsecond=0)
        if not inclusive or candidate != dt:
            candidate += timedelta(minutes=1)
        # Bound the search: 366 days of minutes is plenty for any valid cron.
        for _ in range(366 * 24 * 60 + 2):
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise CronError(f"No upcoming match for cron {self.expr!r}")

    def __repr__(self) -> str:
        return f"CronSchedule({self.expr!r})"


JobFn = Callable[[], Coroutine[Any, Any, Any] | Any]


@dataclass
class ScheduledJob:
    name: str
    cron: CronSchedule
    fn: JobFn
    max_runs: int | None = None
    runs: int = 0
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class Scheduler:
    """In-process scheduler that fires jobs when their cron matches."""

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(tz=timezone.utc))
        self._jobs: dict[str, ScheduledJob] = {}
        self._stop = asyncio.Event()

    def add(
        self,
        name: str,
        cron: str | CronSchedule,
        fn: JobFn,
        *,
        max_runs: int | None = None,
        **extra: Any,
    ) -> ScheduledJob:
        if name in self._jobs:
            raise ValueError(f"Job {name!r} is already scheduled")
        schedule = cron if isinstance(cron, CronSchedule) else CronSchedule(cron)
        now = self._clock()
        job = ScheduledJob(
            name=name,
            cron=schedule,
            fn=fn,
            max_runs=max_runs,
            next_run_at=schedule.next_after(now, inclusive=False),
            extra=extra,
        )
        self._jobs[name] = job
        return job

    def remove(self, name: str) -> None:
        self._jobs.pop(name, None)

    def jobs(self) -> list[ScheduledJob]:
        return list(self._jobs.values())

    def get(self, name: str) -> ScheduledJob:
        return self._jobs[name]

    def stop(self) -> None:
        self._stop.set()

    def due(self, now: datetime | None = None) -> list[ScheduledJob]:
        now = now or self._clock()
        due: list[ScheduledJob] = []
        for job in self._jobs.values():
            if job.max_runs is not None and job.runs >= job.max_runs:
                continue
            if job.next_run_at is not None and job.next_run_at <= now:
                due.append(job)
        return due

    async def tick(self, now: datetime | None = None) -> list[str]:
        """Fire every job that is due, then reschedule them. Returns job names."""
        now = now or self._clock()
        fired: list[str] = []
        for job in self.due(now):
            await self._fire(job, now)
            fired.append(job.name)
        return fired

    async def _fire(self, job: ScheduledJob, now: datetime) -> None:
        log.info("firing job", extra={"job": job.name, "cron": job.cron.expr})
        result = job.fn()
        if asyncio.iscoroutine(result):
            await result
        job.runs += 1
        job.last_run_at = now
        if job.max_runs is not None and job.runs >= job.max_runs:
            job.next_run_at = None
        else:
            job.next_run_at = job.cron.next_after(now, inclusive=False)

    async def run_forever(self, *, interval: float = 1.0) -> None:
        """Block, ticking every ``interval`` seconds, until :meth:`stop`."""
        self._stop.clear()
        while not self._stop.is_set():
            await self.tick()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except TimeoutError:
                continue
            except asyncio.TimeoutError:
                continue

    async def run_until(
        self,
        until: datetime,
        *,
        step: timedelta = timedelta(minutes=1),
    ) -> list[str]:
        """Advance a virtual clock from now to ``until``, firing due jobs.

        Intended for tests: the scheduler clock is not automatically stepped
        unless the caller supplied a mutable clock. When using the default
        wall clock this simply ticks once per ``step`` of real waiting is
        *not* performed; instead, ``tick(now)`` is called for each simulated
        timestamp.
        """
        fired: list[str] = []
        now = self._clock()
        current = now
        while current <= until:
            fired.extend(await self.tick(current))
            current += step
        return fired
