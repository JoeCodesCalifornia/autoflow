"""Async pipeline executor with concurrency limits, retries, and timeouts."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autoflow.dag import DAG
from autoflow.exceptions import TaskRetryError, TaskTimeoutError
from autoflow.history import RunHistory
from autoflow.hooks import (
    RUN_COMPLETE,
    RUN_START,
    TASK_FAILURE,
    TASK_RETRY,
    TASK_SKIPPED,
    TASK_START,
    TASK_SUCCESS,
    HookBus,
)
from autoflow.logging import get_logger
from autoflow.models import RunResult, Status, TaskResult
from autoflow.task import Context, TaskSpec, invoke

log = get_logger("executor")


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class Executor:
    """Run a DAG, honoring dependencies, retries, timeouts, and concurrency."""

    def __init__(
        self,
        *,
        max_concurrency: int = 4,
        hooks: HookBus | None = None,
        history: RunHistory | None = None,
        workdir: Path | None = None,
        retry_sleep: bool = True,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.max_concurrency = max_concurrency
        self.hooks = hooks or HookBus()
        self.history = history
        self.workdir = Path(workdir) if workdir is not None else Path.cwd()
        self.retry_sleep = retry_sleep

    async def run(
        self,
        dag: DAG,
        *,
        vars: Mapping[str, Any] | None = None,
        trigger: str = "manual",
        run_id: str | None = None,
        dry_run: bool = False,
    ) -> RunResult:
        """Execute ``dag`` and return a :class:`RunResult`."""
        dag.validate()
        started = _utcnow()
        run_id = run_id or uuid.uuid4().hex
        results: dict[str, TaskResult] = {
            name: TaskResult(name=name, status=Status.PENDING) for name in dag.tasks
        }
        run = RunResult(
            run_id=run_id,
            pipeline=dag.name,
            status=Status.RUNNING,
            tasks=results,
            started_at=started,
            trigger=trigger,
        )
        if self.history is not None:
            self.history.record_run_start(run)

        await self.hooks.emit(
            RUN_START, run_id=run_id, pipeline=dag.name, trigger=trigger, dry_run=dry_run
        )
        log.info(
            "run started",
            extra={"run_id": run_id, "pipeline": dag.name, "dry_run": dry_run},
        )

        try:
            if dry_run:
                for name in dag.topological_order():
                    results[name].status = Status.SUCCESS
                    results[name].output = {"dry_run": True}
                    results[name].started_at = started
                    results[name].finished_at = started
                    results[name].attempts = 0
            else:
                await self._execute(dag, results, run_id, vars or {})
        except Exception as exc:
            run.error = str(exc)
            run.status = Status.FAILED
            log.exception("run aborted", extra={"run_id": run_id})
            raise
        finally:
            run.finished_at = _utcnow()
            if run.status == Status.RUNNING:
                if any(t.status == Status.FAILED for t in results.values()):
                    run.status = Status.FAILED
                elif all(t.status.ok for t in results.values()):
                    run.status = Status.SUCCESS
                else:
                    run.status = Status.FAILED
            if self.history is not None:
                self.history.record_run_finish(run)
            await self.hooks.emit(
                RUN_COMPLETE,
                run_id=run_id,
                pipeline=dag.name,
                status=run.status,
                result=run,
            )
            log.info(
                "run finished",
                extra={
                    "run_id": run_id,
                    "pipeline": dag.name,
                    "status": run.status.value,
                    "duration_s": run.duration_s,
                },
            )
        return run

    async def _execute(
        self,
        dag: DAG,
        results: dict[str, TaskResult],
        run_id: str,
        vars: Mapping[str, Any],
    ) -> None:
        semaphore = asyncio.Semaphore(self.max_concurrency)
        pending = set(dag.tasks)
        in_flight: dict[str, asyncio.Task[None]] = {}

        async def run_one(name: str) -> None:
            async with semaphore:
                await self._run_task(dag.get(name), results, run_id, dag.name, vars)

        while pending or in_flight:
            completed = [n for n, r in results.items() if r.status == Status.SUCCESS]
            failed = [n for n, r in results.items() if r.status == Status.FAILED]
            for name in dag.blocked_by_failure(failed):
                if results[name].status == Status.PENDING:
                    results[name].status = Status.SKIPPED
                    results[name].skipped_reason = "upstream failure"
                    pending.discard(name)
                    await self.hooks.emit(
                        TASK_SKIPPED,
                        run_id=run_id,
                        task=name,
                        reason="upstream failure",
                    )
                    if self.history is not None:
                        self.history.record_task(run_id, results[name])

            ready = [n for n in dag.ready(completed, failed) if n in pending and n not in in_flight]
            for name in ready:
                pending.discard(name)
                in_flight[name] = asyncio.create_task(run_one(name), name=f"task:{name}")

            if not in_flight:
                # Nothing running and nothing newly ready: remaining work is stuck.
                for name in list(pending):
                    if results[name].status == Status.PENDING:
                        results[name].status = Status.SKIPPED
                        results[name].skipped_reason = "not runnable"
                        pending.discard(name)
                break

            done, _ = await asyncio.wait(in_flight.values(), return_when=asyncio.FIRST_COMPLETED)
            finished_names = []
            for task in done:
                # Surface unexpected exceptions rather than swallowing them.
                exc = task.exception()
                if exc is not None:
                    raise exc
                for name, handle in list(in_flight.items()):
                    if handle is task:
                        finished_names.append(name)
                        del in_flight[name]
            for name in finished_names:
                if self.history is not None:
                    self.history.record_task(run_id, results[name])

    async def _run_task(
        self,
        spec: TaskSpec,
        results: dict[str, TaskResult],
        run_id: str,
        pipeline: str,
        vars: Mapping[str, Any],
    ) -> None:
        result = results[spec.name]
        result.status = Status.RUNNING
        result.started_at = _utcnow()
        ctx = Context(
            params=dict(spec.params),
            results={n: r for n, r in results.items() if n != spec.name and r.status.is_terminal},
            vars=dict(vars),
            workdir=self.workdir,
            run_id=run_id,
            pipeline=pipeline,
            task_name=spec.name,
        )
        await self.hooks.emit(TASK_START, run_id=run_id, task=spec.name, spec=spec)
        log.info("task started", extra={"run_id": run_id, "task": spec.name})

        last_error: BaseException | None = None
        max_attempts = spec.retries + 1
        for attempt in range(max_attempts):
            result.attempts = attempt + 1
            try:
                output = await self._call_with_timeout(spec, ctx)
            except Exception as exc:
                last_error = exc
                if attempt < spec.retries:
                    delay = spec.backoff_delay(attempt)
                    await self.hooks.emit(
                        TASK_RETRY,
                        run_id=run_id,
                        task=spec.name,
                        attempt=attempt + 1,
                        delay=delay,
                        error=exc,
                    )
                    log.warning(
                        "task retry scheduled",
                        extra={
                            "run_id": run_id,
                            "task": spec.name,
                            "attempt": attempt + 1,
                            "delay": delay,
                            "error": str(exc),
                        },
                    )
                    if self.retry_sleep and delay > 0:
                        await asyncio.sleep(delay)
                    continue
                result.status = Status.FAILED
                result.error = str(exc)
                result.finished_at = _utcnow()
                await self.hooks.emit(
                    TASK_FAILURE,
                    run_id=run_id,
                    task=spec.name,
                    error=exc,
                    attempts=result.attempts,
                )
                log.error(
                    "task failed",
                    extra={
                        "run_id": run_id,
                        "task": spec.name,
                        "attempts": result.attempts,
                        "error": str(exc),
                    },
                )
                return
            else:
                result.status = Status.SUCCESS
                result.output = output
                result.finished_at = _utcnow()
                await self.hooks.emit(
                    TASK_SUCCESS,
                    run_id=run_id,
                    task=spec.name,
                    output=output,
                    attempts=result.attempts,
                )
                log.info(
                    "task succeeded",
                    extra={
                        "run_id": run_id,
                        "task": spec.name,
                        "attempts": result.attempts,
                    },
                )
                return

        # Defensive: loop should always return, but keep a terminal failure.
        result.status = Status.FAILED
        result.finished_at = _utcnow()
        if last_error is not None:
            result.error = str(last_error)
            raise TaskRetryError(spec.name, result.attempts, last_error)

    async def _call_with_timeout(self, spec: TaskSpec, ctx: Context) -> Any:
        coro = invoke(spec, ctx)
        if spec.timeout is None:
            return await coro
        try:
            return await asyncio.wait_for(coro, timeout=spec.timeout)
        except TimeoutError:
            raise TaskTimeoutError(spec.name, spec.timeout) from None
        except asyncio.TimeoutError:
            raise TaskTimeoutError(spec.name, spec.timeout) from None
