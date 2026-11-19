"""Tests for the HTTP request task, served from a local stdlib server."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest

from autoflow.task import Context
from autoflow.tasks.http import HttpRequestTask


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _send(self, code: int, body: bytes, content_type: str = "text/plain") -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/ok":
            self._send(200, b"hello")
        elif self.path == "/json":
            self._send(200, json.dumps({"n": 7}).encode("utf-8"), "application/json")
        elif self.path == "/missing":
            self._send(404, b"nope")
        else:
            self._send(404, b"not found")

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        self._send(201, b"got:" + raw, "text/plain")


@pytest.fixture
def http_server() -> Any:
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    yield f"http://{host}:{port}"
    server.shutdown()
    thread.join(timeout=2)


@pytest.mark.asyncio
async def test_get_ok_and_save(http_server: str, tmp_path: Path) -> None:
    ctx = Context(workdir=tmp_path)
    result = await HttpRequestTask().run(
        ctx, {"url": f"{http_server}/ok", "dest": "body.txt", "expected_status": 200}
    )
    assert result["status"] == 200
    assert result["body"] == "hello"
    assert (tmp_path / "body.txt").read_text() == "hello"
    assert result["bytes"] == 5


@pytest.mark.asyncio
async def test_get_json(http_server: str, tmp_path: Path) -> None:
    result = await HttpRequestTask().run(Context(workdir=tmp_path), {"url": f"{http_server}/json"})
    assert result["json"] == {"n": 7}


@pytest.mark.asyncio
async def test_post_body(http_server: str, tmp_path: Path) -> None:
    result = await HttpRequestTask().run(
        Context(workdir=tmp_path),
        {"url": f"{http_server}/", "method": "POST", "body": {"a": 1}, "expected_status": 201},
    )
    assert result["status"] == 201
    assert result["body"] and "got:" in result["body"]


@pytest.mark.asyncio
async def test_http_error_and_expected_status(http_server: str, tmp_path: Path) -> None:
    ctx = Context(workdir=tmp_path)
    with pytest.raises(RuntimeError, match="HTTP 404"):
        await HttpRequestTask().run(ctx, {"url": f"{http_server}/missing"})
    # expected_status can accept a 404 without raising that path... wait, the
    # task raises if expected is set and doesn't match, and also raises on 4xx
    # when expected is None. When expected is 404 it should succeed.
    result = await HttpRequestTask().run(
        ctx, {"url": f"{http_server}/missing", "expected_status": 404}
    )
    assert result["status"] == 404
    with pytest.raises(RuntimeError, match="Expected HTTP 200"):
        await HttpRequestTask().run(ctx, {"url": f"{http_server}/missing", "expected_status": 200})


@pytest.mark.asyncio
async def test_missing_url(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="url"):
        await HttpRequestTask().run(Context(workdir=tmp_path), {})
