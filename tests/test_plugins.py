"""Tests for the plugin registry."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from autoflow.exceptions import UnknownTaskError
from autoflow.plugins import PluginRegistry, register_builtin_tasks
from autoflow.task import Context


class DummyTask:
    async def run(self, ctx: Context, params: Mapping[str, Any]) -> Any:
        return params.get("value", 1)


def test_register_get_and_names() -> None:
    reg = PluginRegistry()
    reg.register("dummy", DummyTask)
    assert reg.contains("dummy")
    handler = reg.get("dummy")
    assert isinstance(handler, DummyTask)
    assert "dummy" in reg.names()
    reg.unregister("dummy")
    assert not reg.contains("dummy")
    with pytest.raises(UnknownTaskError, match="task type"):
        reg.get("dummy")


def test_register_rejects_duplicates_and_empty_names() -> None:
    reg = PluginRegistry()
    reg.register("dummy", DummyTask)
    with pytest.raises(ValueError, match="already registered"):
        reg.register("dummy", DummyTask)
    reg.register("dummy", DummyTask, overwrite=True)
    with pytest.raises(ValueError, match="non-empty"):
        reg.register("", DummyTask)


@pytest.mark.asyncio
async def test_create_func_and_builtins(plugins: PluginRegistry, tmp_path: Path) -> None:
    func = plugins.create_func("files.mkdir", {"path": "x"})
    ctx = Context(params={}, workdir=tmp_path)
    # Built-ins are registered by the fixture.
    assert "shell" in plugins.names()
    assert "http.request" in plugins.names()
    assert "files.copy" in plugins.names()
    result = await func(ctx)
    assert result["created"] is True or "path" in result


def test_register_builtin_is_idempotent() -> None:
    reg = PluginRegistry()
    register_builtin_tasks(reg)
    names = set(reg.names())
    register_builtin_tasks(reg)
    assert set(reg.names()) == names


def test_load_entry_points_does_not_crash() -> None:
    reg = PluginRegistry()
    # May load the autoflow built-ins if the package is installed, or none.
    first = reg.load_entry_points()
    second = reg.load_entry_points()  # cached, no reload
    assert second == 0
    forced = reg.load_entry_points(force=True)
    assert forced >= 0
    assert first >= 0
