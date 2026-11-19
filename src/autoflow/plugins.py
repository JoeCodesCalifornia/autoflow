"""Plugin registry for built-in and third-party task types."""

from __future__ import annotations

import importlib.metadata
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Protocol, runtime_checkable

from autoflow.exceptions import UnknownTaskError
from autoflow.logging import get_logger
from autoflow.task import Context

log = get_logger("plugins")

ENTRY_POINT_GROUP = "autoflow.tasks"


@runtime_checkable
class TaskHandler(Protocol):
    """A callable or class that can execute a configured task."""

    async def run(self, ctx: Context, params: Mapping[str, Any]) -> Any: ...


HandlerFactory = Callable[[], TaskHandler] | type[TaskHandler] | TaskHandler


class PluginRegistry:
    """Name-to-handler registry with optional setuptools entry-point loading."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFactory] = {}
        self._loaded_entry_points = False

    def register(self, name: str, handler: HandlerFactory, *, overwrite: bool = False) -> None:
        if not name:
            raise ValueError("plugin name must be non-empty")
        if name in self._handlers and not overwrite:
            raise ValueError(f"plugin {name!r} is already registered")
        self._handlers[name] = handler

    def unregister(self, name: str) -> None:
        self._handlers.pop(name, None)

    def get(self, name: str) -> TaskHandler:
        if name not in self._handlers:
            raise UnknownTaskError(name, kind="task type")
        factory = self._handlers[name]
        if isinstance(factory, type):
            return factory()  # type: ignore[no-any-return]
        if callable(factory) and not hasattr(factory, "run"):
            return factory()
        return factory  # type: ignore[return-value]

    def contains(self, name: str) -> bool:
        return name in self._handlers

    def names(self) -> list[str]:
        return sorted(self._handlers)

    def load_entry_points(self, group: str = ENTRY_POINT_GROUP, *, force: bool = False) -> int:
        """Load task handlers advertised under a package entry-point group."""
        if self._loaded_entry_points and not force:
            return 0
        loaded = 0
        try:
            eps = importlib.metadata.entry_points()
        except Exception as exc:  # pragma: no cover - extremely defensive
            log.warning("failed to read entry points: %s", exc)
            return 0
        selected: Iterable[importlib.metadata.EntryPoint]
        if hasattr(eps, "select"):  # noqa: SIM108
            selected = eps.select(group=group)
        else:  # pragma: no cover - Python < 3.10 compatibility shim
            selected = eps.get(group, [])
        for ep in selected:
            try:
                handler = ep.load()
            except Exception as exc:
                log.warning("failed to load entry point %s: %s", ep.name, exc)
                continue
            if ep.name not in self._handlers:
                self._handlers[ep.name] = handler
                loaded += 1
        self._loaded_entry_points = True
        return loaded

    def create_func(self, type_name: str, params: Mapping[str, Any]) -> Callable[[Context], Any]:
        """Wrap a named handler as a task function closed over ``params``."""
        handler = self.get(type_name)

        async def _run(ctx: Context) -> Any:
            merged = {**params, **ctx.params}
            return await handler.run(ctx, merged)

        _run.__name__ = type_name.replace(".", "_")
        _run.__doc__ = f"Auto-generated runner for task type {type_name!r}."
        return _run


registry = PluginRegistry()


def register_builtin_tasks(reg: PluginRegistry | None = None) -> PluginRegistry:
    """Register the built-in shell, HTTP, and file task types."""
    from autoflow.tasks import files, http, shell

    target = reg or registry
    builtins: dict[str, HandlerFactory] = {
        "shell": shell.ShellTask,
        "http.request": http.HttpRequestTask,
        "files.copy": files.CopyTask,
        "files.move": files.MoveTask,
        "files.delete": files.DeleteTask,
        "files.write": files.WriteTask,
        "files.read": files.ReadTask,
        "files.mkdir": files.MkdirTask,
    }
    for name, cls in builtins.items():
        if not target.contains(name):
            target.register(name, cls)
    return target
