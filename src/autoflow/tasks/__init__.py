"""Built-in Autoflow task types."""

from autoflow.tasks.files import (
    CopyTask,
    DeleteTask,
    MkdirTask,
    MoveTask,
    ReadTask,
    WriteTask,
)
from autoflow.tasks.http import HttpRequestTask
from autoflow.tasks.shell import ShellTask

__all__ = [
    "CopyTask",
    "DeleteTask",
    "HttpRequestTask",
    "MkdirTask",
    "MoveTask",
    "ReadTask",
    "ShellTask",
    "WriteTask",
]
