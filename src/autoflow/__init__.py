"""Autoflow: a general-purpose automation framework."""

from autoflow.dag import DAG
from autoflow.executor import Executor
from autoflow.history import RunHistory
from autoflow.hooks import HookBus
from autoflow.models import RunResult, Status, TaskResult
from autoflow.plugins import PluginRegistry, registry
from autoflow.scheduler import CronSchedule, Scheduler
from autoflow.task import Context, TaskSpec, task

__version__ = "0.1.0"
__all__ = [
    "DAG",
    "Context",
    "CronSchedule",
    "Executor",
    "HookBus",
    "PluginRegistry",
    "RunHistory",
    "RunResult",
    "Scheduler",
    "Status",
    "TaskResult",
    "TaskSpec",
    "__version__",
    "registry",
    "task",
]
