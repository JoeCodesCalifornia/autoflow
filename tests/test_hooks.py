"""Tests for the hook bus."""

from __future__ import annotations

import pytest

from autoflow.hooks import RUN_START, TASK_SUCCESS, HookBus


@pytest.mark.asyncio
async def test_sync_and_async_hooks_fire_in_order() -> None:
    bus = HookBus()
    seen: list[str] = []

    @bus.on(RUN_START)
    def sync_hook(run_id: str, **_: object) -> None:
        seen.append(f"sync:{run_id}")

    @bus.on(RUN_START)
    async def async_hook(run_id: str, **_: object) -> None:
        seen.append(f"async:{run_id}")

    await bus.emit(RUN_START, run_id="r1")
    assert seen == ["sync:r1", "async:r1"]
    assert len(bus.listeners(RUN_START)) == 2


@pytest.mark.asyncio
async def test_off_and_clear() -> None:
    bus = HookBus()
    calls: list[int] = []

    def cb(**_: object) -> None:
        calls.append(1)

    bus.on(TASK_SUCCESS, cb)
    await bus.emit(TASK_SUCCESS, run_id="r", task="t", output=None, attempts=1)
    bus.off(TASK_SUCCESS, cb)
    await bus.emit(TASK_SUCCESS, run_id="r", task="t", output=None, attempts=1)
    assert calls == [1]
    bus.on(TASK_SUCCESS, cb)
    bus.clear(TASK_SUCCESS)
    bus.clear()
    await bus.emit(TASK_SUCCESS, run_id="r", task="t", output=None, attempts=1)
    assert calls == [1]


def test_unknown_event_rejected() -> None:
    bus = HookBus()
    with pytest.raises(ValueError, match="Unknown hook event"):
        bus.on("not-an-event", lambda **_: None)
