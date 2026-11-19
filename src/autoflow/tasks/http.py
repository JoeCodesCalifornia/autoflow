"""HTTP request task type (stdlib urllib, with optional httpx)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from autoflow.task import Context


def _use_httpx() -> bool:
    try:
        import httpx  # noqa: F401

        return True
    except ImportError:
        return False


class HttpRequestTask:
    """Perform an HTTP request and optionally persist the body to disk."""

    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        url = params.get("url")
        if not url or not isinstance(url, str):
            raise ValueError("http.request task requires a string 'url' parameter")
        method = str(params.get("method", "GET")).upper()
        headers = dict(params.get("headers") or {})
        timeout = float(params.get("timeout", 30))
        body = params.get("body")
        dest = params.get("dest")
        expected = params.get("expected_status")

        data: bytes | None
        if body is None:
            data = None
        elif isinstance(body, bytes):
            data = body
        elif isinstance(body, (dict, list)):
            data = json.dumps(body).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")
        else:
            data = str(body).encode("utf-8")

        if _use_httpx():
            status, response_headers, content = await self._via_httpx(
                url, method, headers, data, timeout
            )
        else:
            status, response_headers, content = self._via_urllib(
                url, method, headers, data, timeout
            )

        if expected is not None and int(status) != int(expected):
            raise RuntimeError(f"Expected HTTP {expected} from {method} {url}, got {status}")
        if int(status) >= 400 and expected is None:
            raise RuntimeError(f"HTTP {status} from {method} {url}")

        text: str | None
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = None

        saved_to = None
        if dest:
            path = Path(dest)
            if not path.is_absolute():
                path = ctx.workdir / path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            saved_to = str(path)

        parsed: Any = None
        ctype = response_headers.get("Content-Type") or response_headers.get("content-type") or ""
        if text is not None and "json" in ctype:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
        elif text is not None:
            stripped = text.lstrip()
            if stripped.startswith("{") or stripped.startswith("["):
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    parsed = None

        return {
            "url": url,
            "method": method,
            "status": int(status),
            "headers": dict(response_headers),
            "body": text if text is not None else None,
            "json": parsed,
            "bytes": len(content),
            "saved_to": saved_to,
        }

    @staticmethod
    def _via_urllib(
        url: str,
        method: str,
        headers: dict[str, str],
        data: bytes | None,
        timeout: float,
    ) -> tuple[int, dict[str, str], bytes]:
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=timeout) as response:
                content = response.read()
                status = int(getattr(response, "status", 200))
                resp_headers = dict(response.headers.items())
                return status, resp_headers, content
        except HTTPError as exc:
            content = exc.read()
            return int(exc.code), dict(exc.headers.items()) if exc.headers else {}, content
        except URLError as exc:
            raise RuntimeError(f"Request to {url} failed: {exc}") from exc

    @staticmethod
    async def _via_httpx(
        url: str,
        method: str,
        headers: dict[str, str],
        data: bytes | None,
        timeout: float,
    ) -> tuple[int, dict[str, str], bytes]:
        import httpx

        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.request(method, url, headers=headers, content=data)
            return response.status_code, dict(response.headers), response.content
