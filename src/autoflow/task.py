"""Decorator-based task definitions and execution context."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar, cast, overload

from autoflow.logging import get_logger
from autoflow.models import TaskResult

F = TypeVar("F", bound=Callable[..., Any])
TaskFunc = Callable[["Context"], Any]


@dataclass
class TaskSpec:
    """A named unit of work in a pipeline, with retry and dependency metadata."""

    name: str
    func: TaskFunc
    depends_on: list[str] = field(default_factory=list)
    retries: int = 0
    timeout: float | None = None
    backoff: float = 1.0
    backoff_factor: float = 2.0
    max_backoff: float = 60.0
    task_type: str = "python"
    params: dict[str, Any] = field(default_factory=dict)
    description: str = ""

    def backoff_delay(self, attempt: int) -> float:
        """Return the sleep duration after a failed attempt (0-based)."""
        if attempt < 0:
            return 0.0
        delay = self.backoff * (self.backoff_factor**attempt)
        return min(self.max_backoff, delay)

    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.func)


@dataclass
class Context:
    """Runtime context passed into every task function."""

    params: dict[str, Any] = field(default_factory=dict)
    results: dict[str, TaskResult] = field(default_factory=dict)
    vars: dict[str, Any] = field(default_factory=dict)
    workdir: Path = field(default_factory=Path.cwd)
    run_id: str = ""
    pipeline: str = ""
    task_name: str = ""

    def log(self) -> Any:
        return get_logger(f"task.{self.task_name or 'unknown'}")

    def upstream(self, name: str) -> Any:
        """Return the output of a completed upstream task."""
        if name not in self.results:
            raise KeyError(f"No result recorded for upstream task {name!r}")
        return self.results[name].output

    def get_var(self, key: str, default: Any = None) -> Any:
        if key in self.vars:
            return self.vars[key]
        if key in self.params:
            return self.params[key]
        return default


@overload
def task(func: F) -> F: ...


@overload
def task(
    func: None = None,
    *,
    name: str | None = None,
    depends_on: list[str] | None = None,
    retries: int = 0,
    timeout: float | None = None,
    backoff: float = 1.0,
    backoff_factor: float = 2.0,
    max_backoff: float = 60.0,
    description: str = "",
) -> Callable[[F], F]: ...


def task(
    func: F | None = None,
    *,
    name: str | None = None,
    depends_on: list[str] | None = None,
    retries: int = 0,
    timeout: float | None = None,
    backoff: float = 1.0,
    backoff_factor: float = 2.0,
    max_backoff: float = 60.0,
    description: str = "",
) -> F | Callable[[F], F]:
    """Register a Python function as an Autoflow task.

    Can be used as ``@task`` or ``@task(name=..., retries=3, timeout=30)``.
    The decorated function is returned unchanged, with a ``.task_spec``
    attribute attached.
    """

    def decorator(fn: F) -> F:
        resolved_name = name or fn.__name__
        if retries < 0:
            raise ValueError("retries must be >= 0")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0")
        if backoff < 0 or backoff_factor < 1 or max_backoff < 0:
            raise ValueError("invalid backoff configuration")
        spec = TaskSpec(
            name=resolved_name,
            func=cast(TaskFunc, fn),
            depends_on=list(depends_on or []),
            retries=retries,
            timeout=timeout,
            backoff=backoff,
            backoff_factor=backoff_factor,
            max_backoff=max_backoff,
            description=description or (fn.__doc__ or "").strip().split("\n")[0],
        )
        fn.task_spec = spec  # type: ignore[attr-defined]
        return fn

    if func is not None:
        return decorator(func)
    return decorator


def spec_from_callable(fn: Callable[..., Any]) -> TaskSpec:
    """Return the TaskSpec attached by ``@task``, or synthesize a default one."""
    existing = getattr(fn, "task_spec", None)
    if isinstance(existing, TaskSpec):
        return existing
    return TaskSpec(name=fn.__name__, func=cast(TaskFunc, fn))


async def invoke(spec: TaskSpec, ctx: Context) -> Any:
    """Call a task function, awaiting it when it is a coroutine function."""
    result = spec.func(ctx)
    if inspect.isawaitable(result):
        return await result
    return result
