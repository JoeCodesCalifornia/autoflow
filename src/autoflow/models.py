"""Shared data models for pipeline runs and task results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Status(str, Enum):
    """Lifecycle status for a run or a single task."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in {
            Status.SUCCESS,
            Status.FAILED,
            Status.SKIPPED,
            Status.CANCELLED,
        }

    @property
    def ok(self) -> bool:
        return self in {Status.SUCCESS, Status.SKIPPED}


@dataclass
class TaskResult:
    """Outcome of a single task execution."""

    name: str
    status: Status = Status.PENDING
    attempts: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    output: Any = None
    error: str | None = None
    skipped_reason: str | None = None

    @property
    def duration_s(self) -> float | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()


@dataclass
class RunResult:
    """Outcome of a full pipeline run."""

    run_id: str
    pipeline: str
    status: Status
    tasks: dict[str, TaskResult] = field(default_factory=dict)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    trigger: str = "manual"
    error: str | None = None

    @property
    def duration_s(self) -> float | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def succeeded(self) -> list[str]:
        return [n for n, t in self.tasks.items() if t.status == Status.SUCCESS]

    @property
    def failed(self) -> list[str]:
        return [n for n, t in self.tasks.items() if t.status == Status.FAILED]

    @property
    def skipped(self) -> list[str]:
        return [n for n, t in self.tasks.items() if t.status == Status.SKIPPED]
