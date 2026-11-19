"""Event hooks fired around pipeline and task lifecycle events."""

from __future__ import annotations

import inspect
from collections import defaultdict
from collections.abc import Callable
from typing import Any

HookCallback = Callable[..., Any]

RUN_START = "run_start"
RUN_COMPLETE = "run_complete"
TASK_START = "task_start"
TASK_SUCCESS = "task_success"
TASK_FAILURE = "task_failure"
TASK_RETRY = "task_retry"
TASK_SKIPPED = "task_skipped"

ALL_EVENTS = (
    RUN_START,
    RUN_COMPLETE,
    TASK_START,
    TASK_SUCCESS,
    TASK_FAILURE,
    TASK_RETRY,
    TASK_SKIPPED,
)


class HookBus:
    """A simple async-aware pub/sub bus for pipeline events."""

    def __init__(self) -> None:
        self._subs: dict[str, list[HookCallback]] = defaultdict(list)

    def on(
        self, event: str, callback: HookCallback | None = None
    ) -> HookCallback | Callable[[HookCallback], HookCallback]:
        """Subscribe a callback to an event.

        Usable as ``bus.on(EVENT, cb)`` or as a decorator: ``@bus.on(EVENT)``.
        """
        if event not in ALL_EVENTS:
            raise ValueError(f"Unknown hook event: {event!r}")

        def decorator(cb: HookCallback) -> HookCallback:
            self._subs[event].append(cb)
            return cb

        if callback is None:
            return decorator
        return decorator(callback)

    def off(self, event: str, callback: HookCallback) -> None:
        subs = self._subs.get(event, [])
        self._subs[event] = [c for c in subs if c is not callback]

    def clear(self, event: str | None = None) -> None:
        if event is None:
            self._subs.clear()
        else:
            self._subs.pop(event, None)

    def listeners(self, event: str) -> list[HookCallback]:
        return list(self._subs.get(event, []))

    async def emit(self, event: str, **payload: Any) -> None:
        """Invoke all subscribers for ``event``. Async callbacks are awaited."""
        for callback in list(self._subs.get(event, [])):
            result = callback(**payload)
            if inspect.isawaitable(result):
                await result
