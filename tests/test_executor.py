"""Tests for the async executor."""

from __future__ import annotations

import asyncio

import pytest

from autoflow.dag import DAG
from autoflow.exceptions import TaskTimeoutError
from autoflow.executor import Executor
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
from autoflow.models import Status
from autoflow.task import task


def _dag(*fns, name: str = "p") -> DAG:
    return DAG.from_functions(name, fns)


@pytest.mark.asyncio
async def test_successful_linear_run(executor: Executor, simple_dag: DAG) -> None:
    result = await executor.run(simple_dag)
    assert result.status == Status.SUCCESS
    assert result.tasks["start"].output == 1
    assert result.tasks["double"].output == 2
    assert result.succeeded == ["start", "double"]


@pytest.mark.asyncio
async def test_dry_run_skips_functions() -> None:
    called = {"n": 0}

    @task
    def boom(ctx):
        called["n"] += 1
        raise RuntimeError("should not run")

    exe = Executor(retry_sleep=False)
    result = await exe.run(_dag(boom), dry_run=True)
    assert result.status == Status.SUCCESS
    assert called["n"] == 0
    assert result.tasks["boom"].output == {"dry_run": True}


@pytest.mark.asyncio
async def test_failure_skips_dependents_but_runs_siblings() -> None:
    @task
    def bad(ctx):
        raise RuntimeError("nope")

    @task(depends_on=["bad"])
    def downstream(ctx):
        return "should skip"

    @task
    def sibling(ctx):
        return "ok"

    exe = Executor(retry_sleep=False)
    result = await exe.run(_dag(bad, downstream, sibling, name="mix"))
    assert result.status == Status.FAILED
    assert result.tasks["bad"].status == Status.FAILED
    assert result.tasks["downstream"].status == Status.SKIPPED
    assert result.tasks["sibling"].status == Status.SUCCESS
    assert result.tasks["sibling"].output == "ok"


@pytest.mark.asyncio
async def test_retries_then_succeeds() -> None:
    state = {"n": 0}

    @task(retries=2, backoff=0.01)
    def flaky(ctx):
        state["n"] += 1
        if state["n"] < 3:
            raise RuntimeError(f"fail {state['n']}")
        return "recovered"

    exe = Executor(retry_sleep=False)
    result = await exe.run(_dag(flaky))
    assert result.status == Status.SUCCESS
    assert result.tasks["flaky"].attempts == 3
    assert result.tasks["flaky"].output == "recovered"


@pytest.mark.asyncio
async def test_retries_exhausted() -> None:
    @task(retries=1, backoff=0.01)
    def always(ctx):
        raise RuntimeError("still bad")

    exe = Executor(retry_sleep=False)
    result = await exe.run(_dag(always))
    assert result.status == Status.FAILED
    assert result.tasks["always"].attempts == 2
    assert "still bad" in (result.tasks["always"].error or "")


@pytest.mark.asyncio
async def test_timeout() -> None:
    @task(timeout=0.05)
    async def slow(ctx):
        await asyncio.sleep(1)
        return "late"

    exe = Executor(retry_sleep=False)
    result = await exe.run(_dag(slow))
    assert result.status == Status.FAILED
    assert "timed out" in (result.tasks["slow"].error or "")
    assert result.tasks["slow"].status == Status.FAILED


@pytest.mark.asyncio
async def test_concurrency_limit_is_respected() -> None:
    current = 0
    peak = 0
    lock = asyncio.Lock()

    def make(name: str):
        @task(name=name)
        async def worker(ctx):
            nonlocal current, peak
            async with lock:
                current += 1
                peak = max(peak, current)
            await asyncio.sleep(0.05)
            async with lock:
                current -= 1
            return name

        return worker

    dag = DAG("conc")
    for i in range(6):
        dag.add_function(make(f"w{i}"))
    exe = Executor(max_concurrency=2, retry_sleep=False)
    result = await exe.run(dag)
    assert result.status == Status.SUCCESS
    assert peak <= 2
    assert peak >= 1


@pytest.mark.asyncio
async def test_hooks_fire_in_expected_sequence() -> None:
    events: list[str] = []
    hooks = HookBus()

    @hooks.on(RUN_START)
    def _rs(**_: object) -> None:
        events.append("run_start")

    @hooks.on(TASK_START)
    def _ts(task: str, **_: object) -> None:
        events.append(f"start:{task}")

    @hooks.on(TASK_SUCCESS)
    def _ok(task: str, **_: object) -> None:
        events.append(f"ok:{task}")

    @hooks.on(TASK_FAILURE)
    def _fail(task: str, **_: object) -> None:
        events.append(f"fail:{task}")

    @hooks.on(TASK_RETRY)
    def _retry(task: str, **_: object) -> None:
        events.append(f"retry:{task}")

    @hooks.on(TASK_SKIPPED)
    def _skip(task: str, **_: object) -> None:
        events.append(f"skip:{task}")

    @hooks.on(RUN_COMPLETE)
    def _rc(**_: object) -> None:
        events.append("run_complete")

    state = {"n": 0}

    @task(retries=1, backoff=0.01)
    def flaky(ctx):
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("once")
        return 1

    @task
    def bad(ctx):
        raise RuntimeError("no")

    @task(depends_on=["bad"])
    def skipped(ctx):
        return 0

    exe = Executor(hooks=hooks, retry_sleep=False)
    await exe.run(_dag(flaky, bad, skipped, name="hooks"))
    assert events[0] == "run_start"
    assert events[-1] == "run_complete"
    assert "retry:flaky" in events
    assert "ok:flaky" in events
    assert "fail:bad" in events
    assert "skip:skipped" in events


@pytest.mark.asyncio
async def test_history_is_persisted(executor: Executor, simple_dag: DAG, history) -> None:
    result = await executor.run(simple_dag, trigger="test")
    stored = history.get_run(result.run_id)
    assert stored is not None
    assert stored.status == Status.SUCCESS
    assert stored.trigger == "test"
    names = {t.name for t in history.task_results(result.run_id)}
    assert names == {"start", "double"}


@pytest.mark.asyncio
async def test_invalid_concurrency() -> None:
    with pytest.raises(ValueError):
        Executor(max_concurrency=0)


@pytest.mark.asyncio
async def test_timeout_error_type_maps() -> None:
    @task(timeout=0.01)
    async def slow(ctx):
        await asyncio.sleep(1)

    exe = Executor(retry_sleep=False)
    result = await exe.run(_dag(slow))
    assert result.tasks["slow"].status == Status.FAILED
    # The stored error string is from TaskTimeoutError
    assert "slow" in (result.tasks["slow"].error or "")
    err = TaskTimeoutError("slow", 0.01)
    assert "0.01" in str(err)
