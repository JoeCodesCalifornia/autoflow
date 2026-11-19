"""Autoflow-specific exceptions."""


class AutoflowError(Exception):
    """Base class for all Autoflow errors."""


class CycleError(AutoflowError):
    """Raised when a pipeline DAG contains a cycle."""

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        path = " -> ".join(cycle)
        super().__init__(f"Cycle detected in pipeline DAG: {path}")


class UnknownTaskError(AutoflowError):
    """Raised when a task or task type cannot be resolved."""

    def __init__(self, name: str, kind: str = "task") -> None:
        self.name = name
        self.kind = kind
        super().__init__(f"Unknown {kind}: {name!r}")


class ValidationError(AutoflowError):
    """Raised when a pipeline definition fails schema or semantic checks."""

    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        self.errors = errors or [message]
        super().__init__(message)


class TaskTimeoutError(AutoflowError):
    """Raised when a task exceeds its configured timeout."""

    def __init__(self, name: str, timeout: float) -> None:
        self.name = name
        self.timeout = timeout
        super().__init__(f"Task {name!r} timed out after {timeout}s")


class TaskRetryError(AutoflowError):
    """Raised when a task exhausts all retry attempts."""

    def __init__(self, name: str, attempts: int, cause: BaseException) -> None:
        self.name = name
        self.attempts = attempts
        self.cause = cause
        super().__init__(f"Task {name!r} failed after {attempts} attempt(s): {cause}")


class CronError(AutoflowError):
    """Raised when a cron expression is invalid."""


class ConfigError(AutoflowError):
    """Raised when a pipeline config file cannot be loaded."""
