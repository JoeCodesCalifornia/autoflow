"""Tests for built-in file tasks."""

from __future__ import annotations

from pathlib import Path

import pytest

from autoflow.task import Context
from autoflow.tasks.files import (
    CopyTask,
    DeleteTask,
    MkdirTask,
    MoveTask,
    ReadTask,
    WriteTask,
)


def _ctx(tmp_path: Path) -> Context:
    return Context(workdir=tmp_path)


@pytest.mark.asyncio
async def test_write_read_roundtrip(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    written = await WriteTask().run(ctx, {"path": "a.txt", "content": "hello"})
    assert Path(written["path"]).exists()
    read = await ReadTask().run(ctx, {"path": "a.txt"})
    assert read["content"] == "hello"
    assert read["bytes"] == 5


@pytest.mark.asyncio
async def test_write_bytes_mode(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    result = await WriteTask().run(ctx, {"path": "b.bin", "content": b"xyz", "mode": "wb"})
    assert result["bytes"] == 3
    assert (tmp_path / "b.bin").read_bytes() == b"xyz"


@pytest.mark.asyncio
async def test_copy_move_delete_file(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    await WriteTask().run(ctx, {"path": "src.txt", "content": "data"})
    copied = await CopyTask().run(ctx, {"src": "src.txt", "dest": "dst.txt"})
    assert Path(copied["dest"]).read_text() == "data"
    moved = await MoveTask().run(ctx, {"src": "dst.txt", "dest": "sub/moved.txt"})
    assert Path(moved["dest"]).exists()
    assert not (tmp_path / "dst.txt").exists()
    deleted = await DeleteTask().run(ctx, {"path": "sub/moved.txt"})
    assert deleted["deleted"] is True
    missing = await DeleteTask().run(ctx, {"path": "sub/moved.txt", "missing_ok": True})
    assert missing["deleted"] is False
    with pytest.raises(FileNotFoundError):
        await DeleteTask().run(ctx, {"path": "sub/moved.txt", "missing_ok": False})


@pytest.mark.asyncio
async def test_copy_and_delete_directory(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    (tmp_path / "tree" / "inner").mkdir(parents=True)
    (tmp_path / "tree" / "inner" / "f.txt").write_text("z")
    copied = await CopyTask().run(ctx, {"src": "tree", "dest": "tree2"})
    assert copied["kind"] == "directory"
    assert (tmp_path / "tree2" / "inner" / "f.txt").read_text() == "z"
    deleted = await DeleteTask().run(ctx, {"path": "tree2"})
    assert deleted["kind"] == "directory"
    assert not (tmp_path / "tree2").exists()


@pytest.mark.asyncio
async def test_mkdir_and_missing_params(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    result = await MkdirTask().run(ctx, {"path": "new/dir"})
    assert Path(result["path"]).is_dir()
    with pytest.raises(ValueError, match="Missing required"):
        await WriteTask().run(ctx, {})
