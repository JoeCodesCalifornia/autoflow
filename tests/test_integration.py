"""End-to-end integration tests that run example-style pipelines."""

from __future__ import annotations

from pathlib import Path

import pytest

from autoflow.config import load_pipeline
from autoflow.executor import Executor
from autoflow.history import RunHistory
from autoflow.models import Status


@pytest.mark.integration
@pytest.mark.asyncio
async def test_etl_example_style_pipeline(tmp_path: Path) -> None:
    source = tmp_path / "input.txt"
    source.write_text("hello autoflow\n")
    pipeline_path = tmp_path / "etl.yaml"
    pipeline_path.write_text(
        "\n".join(
            [
                "name: etl",
                "max_concurrency: 2",
                "vars:",
                "  SOURCE: input.txt",
                "  WORK: work/copied.txt",
                "  OUTPUT: work/upper.txt",
                "tasks:",
                "  - name: prepare",
                "    type: files.mkdir",
                "    params:",
                "      path: work",
                "  - name: copy",
                "    type: files.copy",
                "    depends_on: [prepare]",
                "    params:",
                "      src: ${SOURCE}",
                "      dest: ${WORK}",
                "  - name: transform",
                "    type: shell",
                "    depends_on: [copy]",
                "    retries: 1",
                "    timeout: 15",
                "    params:",
                "      command: \"tr '[:lower:]' '[:upper:]' < ${WORK} > ${OUTPUT}\"",
                "  - name: read_result",
                "    type: files.read",
                "    depends_on: [transform]",
                "    params:",
                "      path: ${OUTPUT}",
                "",
            ]
        )
    )
    pipeline = load_pipeline(pipeline_path)
    history = RunHistory(tmp_path / "h.db")
    try:
        exe = Executor(max_concurrency=2, history=history, workdir=tmp_path, retry_sleep=False)
        result = await exe.run(pipeline.to_dag(), vars=pipeline.vars)
        assert result.status == Status.SUCCESS
        text = (tmp_path / "work" / "upper.txt").read_text()
        assert "HELLO AUTOFLOW" in text
        stored = history.get_run(result.run_id)
        assert stored is not None
        assert stored.tasks["read_result"].status == Status.SUCCESS
        assert "HELLO" in stored.tasks["read_result"].output["content"]
    finally:
        history.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hello_example_file(tmp_path: Path) -> None:
    example = Path(__file__).resolve().parents[1] / "examples" / "hello.yaml"
    pipeline = load_pipeline(example, vars={"GREETING": "example-hi"})
    exe = Executor(workdir=tmp_path, retry_sleep=False)
    result = await exe.run(pipeline.to_dag(), vars=pipeline.vars)
    assert result.status == Status.SUCCESS
    assert (tmp_path / "out" / "hello.txt").read_text().startswith("example-hi")
