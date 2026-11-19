"""Tests for structured and pretty logging."""

from __future__ import annotations

import io
import json
import logging

from autoflow.logging import configure_logging, get_logger


def test_json_logs_are_valid_and_include_context() -> None:
    stream = io.StringIO()
    logger = configure_logging(
        level="DEBUG", json_output=True, stream=stream, context={"run_id": "abc"}
    )
    logger.info("hello %s", "world", extra={"task": "t1"})
    line = stream.getvalue().strip()
    payload = json.loads(line)
    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert payload["run_id"] == "abc"
    assert payload["task"] == "t1"
    assert "ts" in payload


def test_pretty_logs_and_get_logger() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", json_output=False, stream=stream)
    log = get_logger("cli")
    assert log.name == "autoflow.cli"
    log.info("booted", extra={"k": "v"})
    text = stream.getvalue()
    assert "booted" in text
    assert "k=v" in text
    assert get_logger("autoflow").name == "autoflow"
    assert get_logger("autoflow.executor").name == "autoflow.executor"


def test_exception_included_in_json() -> None:
    stream = io.StringIO()
    logger = configure_logging(level="ERROR", json_output=True, stream=stream)
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logger.exception("failed")
    payload = json.loads(stream.getvalue().strip())
    assert "boom" in payload.get("exc", "")


def test_reconfigure_replaces_handlers() -> None:
    first = io.StringIO()
    second = io.StringIO()
    configure_logging(stream=first)
    configure_logging(stream=second)
    logging.getLogger("autoflow").info("only-second")
    assert "only-second" not in first.getvalue()
    assert "only-second" in second.getvalue()
