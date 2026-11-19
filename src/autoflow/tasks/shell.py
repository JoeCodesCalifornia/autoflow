"""Shell command task type."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from typing import Any

from autoflow.task import Context


class ShellError(RuntimeError):
    """Raised when a shell command exits non-zero."""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"Command {command!r} exited with {returncode}: {stderr}")


class ShellTask:
    """Run a shell command in the pipeline workdir."""

    async def run(self, ctx: Context, params: Mapping[str, Any]) -> dict[str, Any]:
        command = params.get("command")
        if not command:
            raise ValueError("shell task requires a 'command' parameter")

        use_shell = False
        argv: list[str]
        display: str
        if isinstance(command, str):
            use_shell = True
            argv = [command]
            display = command
        elif isinstance(command, list) and all(isinstance(c, str) for c in command):
            argv = list(command)
            display = " ".join(command)
        else:
            raise ValueError("'command' must be a string or list of strings")

        cwd = params.get("cwd") or str(ctx.workdir)
        env = os.environ.copy()
        extra_env = params.get("env") or {}
        if not isinstance(extra_env, dict):
            raise ValueError("'env' must be a mapping")
        env.update({str(k): str(v) for k, v in extra_env.items()})
        check = bool(params.get("check", True))
        input_data = params.get("input")
        timeout = params.get("timeout")
        stdin = asyncio.subprocess.PIPE if input_data is not None else None

        if use_shell:
            proc = await asyncio.create_subprocess_shell(
                display,
                cwd=cwd,
                env=env,
                stdin=stdin,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=cwd,
                env=env,
                stdin=stdin,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

        stdin_bytes = None if input_data is None else str(input_data).encode("utf-8")
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(input=stdin_bytes),
                timeout=float(timeout) if timeout is not None else None,
            )
        except (TimeoutError, asyncio.TimeoutError):
            proc.kill()
            await proc.communicate()
            raise TimeoutError(f"Command {display!r} timed out") from None

        stdout = stdout_b.decode("utf-8", errors="replace")
        stderr = stderr_b.decode("utf-8", errors="replace")
        returncode = proc.returncode or 0
        if check and returncode != 0:
            raise ShellError(display, returncode, stderr)
        return {
            "command": display,
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
        }
