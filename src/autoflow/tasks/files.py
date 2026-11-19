"""File-operation task types."""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from autoflow.task import Context


def _resolve(ctx: Context, value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (ctx.workdir / path).resolve()


def _require(params: Mapping[str, Any], *keys: str) -> None:
    missing = [k for k in keys if not params.get(k)]
    if missing:
        raise ValueError(f"Missing required parameter(s): {', '.join(missing)}")


class CopyTask:
    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        _require(params, "src", "dest")
        src = _resolve(ctx, params["src"])
        dest = _resolve(ctx, params["dest"])
        dest.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dest, dirs_exist_ok=bool(params.get("dirs_exist_ok", True)))
            kind = "directory"
        else:
            shutil.copy2(src, dest)
            kind = "file"
        return {"src": str(src), "dest": str(dest), "kind": kind}


class MoveTask:
    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        _require(params, "src", "dest")
        src = _resolve(ctx, params["src"])
        dest = _resolve(ctx, params["dest"])
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
        return {"src": str(src), "dest": str(dest)}


class DeleteTask:
    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        _require(params, "path")
        path = _resolve(ctx, params["path"])
        missing_ok = bool(params.get("missing_ok", True))
        if not path.exists():
            if missing_ok:
                return {"path": str(path), "deleted": False, "reason": "missing"}
            raise FileNotFoundError(str(path))
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
            kind = "directory"
        else:
            path.unlink()
            kind = "file"
        return {"path": str(path), "deleted": True, "kind": kind}


class WriteTask:
    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        _require(params, "path")
        path = _resolve(ctx, params["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        content = params.get("content", "")
        mode = params.get("mode", "w")
        if "b" in str(mode):
            data = content if isinstance(content, bytes) else str(content).encode("utf-8")
            path.write_bytes(data)
            size = len(data)
        else:
            text = content if isinstance(content, str) else str(content)
            path.write_text(text, encoding=str(params.get("encoding", "utf-8")))
            size = len(text.encode("utf-8"))
        return {"path": str(path), "bytes": size}


class ReadTask:
    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        _require(params, "path")
        path = _resolve(ctx, params["path"])
        encoding = str(params.get("encoding", "utf-8"))
        text = path.read_text(encoding=encoding)
        return {"path": str(path), "content": text, "bytes": path.stat().st_size}


class MkdirTask:
    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        _require(params, "path")
        path = _resolve(ctx, params["path"])
        parents = bool(params.get("parents", True))
        exist_ok = bool(params.get("exist_ok", True))
        path.mkdir(parents=parents, exist_ok=exist_ok)
        return {"path": str(path), "created": True}
