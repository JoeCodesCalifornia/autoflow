"""SQLite-backed persistence for pipeline run history."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from autoflow.models import RunResult, Status, TaskResult

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    status TEXT NOT NULL,
    trigger TEXT NOT NULL DEFAULT 'manual',
    started_at TEXT,
    finished_at TEXT,
    error TEXT,
    extra TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    finished_at TEXT,
    error TEXT,
    output TEXT,
    skipped_reason TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON runs(pipeline);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
"""


def _dt_to_str(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _str_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _dump(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return json.dumps(repr(value))


def _load(value: str | None) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


class RunHistory:
    """Append-only history of pipeline and task runs stored in SQLite."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = Path(path) if path != ":memory:" else path
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> RunHistory:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def record_run_start(self, run: RunResult) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO runs
                (run_id, pipeline, status, trigger, started_at, finished_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run.run_id,
                run.pipeline,
                run.status.value,
                run.trigger,
                _dt_to_str(run.started_at),
                _dt_to_str(run.finished_at),
                run.error,
            ),
        )
        self._conn.commit()

    def record_run_finish(self, run: RunResult) -> None:
        self._conn.execute(
            """
            UPDATE runs
               SET status = ?, finished_at = ?, error = ?
             WHERE run_id = ?
            """,
            (run.status.value, _dt_to_str(run.finished_at), run.error, run.run_id),
        )
        for task in run.tasks.values():
            if task.status.is_terminal:
                self._upsert_task(run.run_id, task)
        self._conn.commit()

    def record_task(self, run_id: str, task: TaskResult) -> None:
        self._upsert_task(run_id, task)
        self._conn.commit()

    def _upsert_task(self, run_id: str, task: TaskResult) -> None:
        existing = self._conn.execute(
            "SELECT id FROM task_runs WHERE run_id = ? AND task_name = ?",
            (run_id, task.name),
        ).fetchone()
        values = (
            task.status.value,
            task.attempts,
            _dt_to_str(task.started_at),
            _dt_to_str(task.finished_at),
            task.error,
            _dump(task.output),
            task.skipped_reason,
            run_id,
            task.name,
        )
        if existing:
            self._conn.execute(
                """
                UPDATE task_runs
                   SET status = ?, attempts = ?, started_at = ?, finished_at = ?,
                       error = ?, output = ?, skipped_reason = ?
                 WHERE run_id = ? AND task_name = ?
                """,
                values,
            )
        else:
            self._conn.execute(
                """
                INSERT INTO task_runs
                    (status, attempts, started_at, finished_at, error, output,
                     skipped_reason, run_id, task_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    def get_run(self, run_id: str) -> RunResult | None:
        row = self._conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        tasks = {t.name: t for t in self._iter_tasks(run_id)}
        return self._row_to_run(row, tasks)

    def list_runs(
        self,
        *,
        pipeline: str | None = None,
        status: Status | None = None,
        limit: int = 50,
    ) -> list[RunResult]:
        sql = "SELECT * FROM runs"
        clauses: list[str] = []
        params: list[Any] = []
        if pipeline:
            clauses.append("pipeline = ?")
            params.append(pipeline)
        if status:
            clauses.append("status = ?")
            params.append(status.value)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_run(row, {}) for row in rows]

    def task_results(self, run_id: str) -> list[TaskResult]:
        return list(self._iter_tasks(run_id))

    def _iter_tasks(self, run_id: str) -> Iterator[TaskResult]:
        rows = self._conn.execute(
            "SELECT * FROM task_runs WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        for row in rows:
            yield TaskResult(
                name=row["task_name"],
                status=Status(row["status"]),
                attempts=row["attempts"],
                started_at=_str_to_dt(row["started_at"]),
                finished_at=_str_to_dt(row["finished_at"]),
                output=_load(row["output"]),
                error=row["error"],
                skipped_reason=row["skipped_reason"],
            )

    @staticmethod
    def _row_to_run(row: sqlite3.Row, tasks: dict[str, TaskResult]) -> RunResult:
        return RunResult(
            run_id=row["run_id"],
            pipeline=row["pipeline"],
            status=Status(row["status"]),
            tasks=tasks,
            started_at=_str_to_dt(row["started_at"]),
            finished_at=_str_to_dt(row["finished_at"]),
            trigger=row["trigger"],
            error=row["error"],
        )
