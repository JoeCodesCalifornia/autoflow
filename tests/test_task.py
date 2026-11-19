"""Tests for task decorators, TaskSpec, and Context."""

from __future__ import annotations

import pytest

from autoflow.task import Context, TaskSpec, invoke, spec_from_callable, task


def test_decorator_without_parens_uses_function_name() -> None:
    @task
    def ping(ctx):
        return "pong"

    assert ping.task_spec.name == "ping"
    assert ping.task_spec.retries == 0
    assert ping.task_spec.func is ping


def test_decorator_with_options() -> None:
    @task(name="fetch", retries=3, timeout=12.5, depends_on=["auth"], description="get data")
    async def fetch(ctx):
        return 1

    spec = fetch.task_spec
    assert spec.name == "fetch"
    assert spec.retries == 3
    assert spec.timeout == 12.5
    assert spec.depends_on == ["auth"]
    assert spec.description == "get data"
    assert spec.is_async() is True


def test_decorator_uses_first_doc_line_as_description() -> None:
    @task
    def work(ctx):
        """Do the work.

        Longer notes here.
        """
        return None

    assert work.task_spec.description == "Do the work."


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(0, 1.0), (1, 2.0), (2, 4.0), (3, 8.0), (10, 60.0), (-1, 0.0)],
)
def test_backoff_delay_is_capped(attempt: int, expected: float) -> None:
    spec = TaskSpec(
        name="t",
        func=lambda ctx: None,
        backoff=1.0,
        backoff_factor=2.0,
        max_backoff=60.0,
    )
    assert spec.backoff_delay(attempt) == expected


def test_invalid_retries_rejected() -> None:
    with pytest.raises(ValueError, match="retries"):

        @task(retries=-1)
        def bad(ctx):
            return None


def test_invalid_timeout_rejected() -> None:
    with pytest.raises(ValueError, match="timeout"):

        @task(timeout=0)
        def bad(ctx):
            return None


def test_invalid_backoff_rejected() -> None:
    with pytest.raises(ValueError, match="backoff"):

        @task(backoff_factor=0.5)
        def bad(ctx):
            return None


def test_spec_from_callable_reads_decorator() -> None:
    @task(name="x")
    def fn(ctx):
        return 1

    assert spec_from_callable(fn).name == "x"


def test_spec_from_callable_synthesizes_default() -> None:
    def raw(ctx):
        return 2

    spec = spec_from_callable(raw)
    assert spec.name == "raw"
    assert spec.func is raw


@pytest.mark.asyncio
async def test_invoke_sync_and_async() -> None:
    @task
    def sync(ctx):
        return ctx.params["n"] + 1

    @task
    async def async_fn(ctx):
        return ctx.params["n"] + 2

    ctx = Context(params={"n": 10})
    assert await invoke(sync.task_spec, ctx) == 11
    assert await invoke(async_fn.task_spec, ctx) == 12


def test_context_upstream_and_vars(tmp_path) -> None:
    from autoflow.models import Status, TaskResult

    ctx = Context(
        params={"p": 1},
        vars={"v": 2},
        results={"up": TaskResult(name="up", status=Status.SUCCESS, output=99)},
        workdir=tmp_path,
        task_name="here",
    )
    assert ctx.upstream("up") == 99
    assert ctx.get_var("v") == 2
    assert ctx.get_var("p") == 1
    assert ctx.get_var("missing", "d") == "d"
    with pytest.raises(KeyError):
        ctx.upstream("nope")
    logger = ctx.log()
    assert "here" in logger.name
