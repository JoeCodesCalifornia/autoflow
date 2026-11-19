"""Tests for the shell task type."""

from __future__ import annotations

from pathlib import Path

import pytest

from autoflow.task import Context
from autoflow.tasks.shell import ShellError, ShellTask


def _ctx(tmp_path: Path) -> Context:
    return Context(workdir=tmp_path)


@pytest.mark.asyncio
async def test_echo_success(tmp_path: Path) -> None:
    result = await ShellTask().run(_ctx(tmp_path), {"command": "echo hello"})
    assert result["returncode"] == 0
    assert "hello" in result["stdout"]


@pytest.mark.asyncio
async def test_argv_list_and_env(tmp_path: Path) -> None:
    result = await ShellTask().run(
        _ctx(tmp_path),
        {
            "command": ["python3", "-c", "import os; print(os.environ['AF_X'])"],
            "env": {"AF_X": "42"},
        },
    )
    assert result["returncode"] == 0
    assert "42" in result["stdout"]


@pytest.mark.asyncio
async def test_nonzero_raises_unless_check_false(tmp_path: Path) -> None:
    with pytest.raises(ShellError) as exc:
        await ShellTask().run(_ctx(tmp_path), {"command": "exit 3"})
    assert exc.value.returncode == 3
    result = await ShellTask().run(_ctx(tmp_path), {"command": "exit 3", "check": False})
    assert result["returncode"] == 3


@pytest.mark.asyncio
async def test_stdin_and_cwd(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    result = await ShellTask().run(
        _ctx(tmp_path),
        {"command": "cat > here.txt", "input": "xyz", "cwd": str(tmp_path / "sub")},
    )
    assert result["returncode"] == 0
    assert (tmp_path / "sub" / "here.txt").read_text() == "xyz"


@pytest.mark.asyncio
async def test_missing_command_and_bad_types(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="command"):
        await ShellTask().run(_ctx(tmp_path), {})
    with pytest.raises(ValueError, match="command"):
        await ShellTask().run(_ctx(tmp_path), {"command": 123})
    with pytest.raises(ValueError, match="env"):
        await ShellTask().run(_ctx(tmp_path), {"command": "true", "env": ["bad"]})


@pytest.mark.asyncio
async def test_timeout(tmp_path: Path) -> None:
    with pytest.raises(TimeoutError, match="timed out"):
        await ShellTask().run(_ctx(tmp_path), {"command": "sleep 2", "timeout": 0.05})
