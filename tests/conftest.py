"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from autoflow.dag import DAG
from autoflow.executor import Executor
from autoflow.history import RunHistory
from autoflow.hooks import HookBus
from autoflow.plugins import PluginRegistry, register_builtin_tasks
from autoflow.task import task


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def history(tmp_path: Path) -> RunHistory:
    db = tmp_path / "history.sqlite"
    hist = RunHistory(db)
    yield hist
    hist.close()


@pytest.fixture
def hooks() -> HookBus:
    return HookBus()


@pytest.fixture
def executor(workdir: Path, history: RunHistory, hooks: HookBus) -> Executor:
    return Executor(
        max_concurrency=4,
        hooks=hooks,
        history=history,
        workdir=workdir,
        retry_sleep=False,
    )


@pytest.fixture
def plugins() -> PluginRegistry:
    reg = PluginRegistry()
    register_builtin_tasks(reg)
    return reg


@pytest.fixture
def simple_dag() -> DAG:
    @task
    def start(ctx):
        return 1

    @task(depends_on=["start"])
    def double(ctx):
        return ctx.upstream("start") * 2

    return DAG.from_functions("simple", [start, double])


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures" / "pipelines"


@pytest.fixture
def hello_pipeline(tmp_path: Path) -> Path:
    path = tmp_path / "hello.yaml"
    path.write_text(
        "\n".join(
            [
                "name: hello",
                "max_concurrency: 2",
                "vars:",
                "  GREETING: hi",
                "tasks:",
                "  - name: greet",
                "    type: shell",
                "    params:",
                '      command: echo "${GREETING}"',
                "  - name: save",
                "    type: files.write",
                "    depends_on: [greet]",
                "    params:",
                "      path: out.txt",
                '      content: "${GREETING}"',
                "",
            ]
        )
    )
    return path
