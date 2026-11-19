"""Directed acyclic graph of tasks with cycle detection and scheduling order."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable, Iterable
from typing import Any

from autoflow.exceptions import CycleError, UnknownTaskError, ValidationError
from autoflow.task import TaskSpec, spec_from_callable


class DAG:
    """A named collection of tasks and their dependency edges."""

    def __init__(self, name: str = "pipeline") -> None:
        self.name = name
        self._tasks: dict[str, TaskSpec] = {}

    def __contains__(self, name: str) -> bool:
        return name in self._tasks

    def __len__(self) -> int:
        return len(self._tasks)

    def __iter__(self) -> Iterable[TaskSpec]:
        return iter(self._tasks.values())

    @property
    def tasks(self) -> dict[str, TaskSpec]:
        return dict(self._tasks)

    def add(self, spec: TaskSpec) -> TaskSpec:
        """Add a task specification. Raises if the name is already taken."""
        if spec.name in self._tasks:
            raise ValidationError(f"Duplicate task name: {spec.name!r}")
        self._tasks[spec.name] = spec
        return spec

    def add_function(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        depends_on: list[str] | None = None,
    ) -> TaskSpec:
        """Add a (possibly already decorated) function as a task."""
        spec = spec_from_callable(fn)
        if name or depends_on is not None:
            spec = TaskSpec(
                name=name or spec.name,
                func=spec.func,
                depends_on=list(depends_on) if depends_on is not None else list(spec.depends_on),
                retries=spec.retries,
                timeout=spec.timeout,
                backoff=spec.backoff,
                backoff_factor=spec.backoff_factor,
                max_backoff=spec.max_backoff,
                task_type=spec.task_type,
                params=dict(spec.params),
                description=spec.description,
            )
        return self.add(spec)

    def get(self, name: str) -> TaskSpec:
        try:
            return self._tasks[name]
        except KeyError:
            raise UnknownTaskError(name) from None

    def predecessors(self, name: str) -> list[str]:
        return list(self.get(name).depends_on)

    def successors(self, name: str) -> list[str]:
        if name not in self._tasks:
            raise UnknownTaskError(name)
        return [t.name for t in self._tasks.values() if name in t.depends_on]

    def edges(self) -> list[tuple[str, str]]:
        """Return (upstream, downstream) edges."""
        result: list[tuple[str, str]] = []
        for spec in self._tasks.values():
            for dep in spec.depends_on:
                result.append((dep, spec.name))
        return result

    def validate(self) -> None:
        """Raise if any dependency is missing or a cycle exists."""
        errors: list[str] = []
        if not self._tasks:
            raise ValidationError("Pipeline has no tasks")
        for spec in self._tasks.values():
            seen_deps: set[str] = set()
            for dep in spec.depends_on:
                if dep == spec.name:
                    errors.append(f"Task {spec.name!r} depends on itself")
                if dep not in self._tasks:
                    errors.append(f"Task {spec.name!r} depends on unknown task {dep!r}")
                if dep in seen_deps:
                    errors.append(f"Task {spec.name!r} lists dependency {dep!r} more than once")
                seen_deps.add(dep)
        if errors:
            raise ValidationError("; ".join(errors), errors=errors)
        cycle = self._find_cycle()
        if cycle is not None:
            raise CycleError(cycle)

    def _find_cycle(self) -> list[str] | None:
        """Return a cycle path (including the repeated start node) or None.

        Uses DFS with a recursion stack. The reported path is the actual cycle
        plus the repeated start node so callers can render ``A -> B -> A``.
        """
        white, gray, black = 0, 1, 2
        color: dict[str, int] = dict.fromkeys(self._tasks, white)
        parent: dict[str, str | None] = dict.fromkeys(self._tasks)

        def dfs(node: str) -> list[str] | None:
            color[node] = gray
            for dep in self._tasks[node].depends_on:
                if dep not in color:
                    continue
                if color[dep] == gray:
                    cycle = [dep]
                    cur: str | None = node
                    while cur is not None and cur != dep:
                        cycle.append(cur)
                        cur = parent[cur]
                    cycle.append(dep)
                    cycle.reverse()
                    return cycle
                if color[dep] == white:
                    parent[dep] = node
                    found = dfs(dep)
                    if found is not None:
                        return found
            color[node] = black
            return None

        for name in self._tasks:
            if color[name] == white:
                found = dfs(name)
                if found is not None:
                    return found
        return None

    def topological_order(self) -> list[str]:
        """Return a valid topological ordering (Kahn's algorithm)."""
        self.validate()
        indegree: dict[str, int] = dict.fromkeys(self._tasks, 0)
        children: dict[str, list[str]] = defaultdict(list)
        for spec in self._tasks.values():
            for dep in spec.depends_on:
                indegree[spec.name] += 1
                children[dep].append(spec.name)
        queue = deque(sorted(n for n, d in indegree.items() if d == 0))
        order: list[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for child in sorted(children[node]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if len(order) != len(self._tasks):
            raise CycleError(self._find_cycle() or list(self._tasks))
        return order

    def levels(self) -> list[list[str]]:
        """Group tasks into waves that can run concurrently.

        Level 0 has no dependencies; each later level depends only on earlier
        ones. Tasks inside a level are independent of each other.
        """
        order = self.topological_order()
        level_of: dict[str, int] = {}
        for name in order:
            deps = self._tasks[name].depends_on
            level_of[name] = 0 if not deps else 1 + max(level_of[d] for d in deps)
        grouped: dict[int, list[str]] = defaultdict(list)
        for name, lvl in level_of.items():
            grouped[lvl].append(name)
        return [grouped[i] for i in range(len(grouped))]

    def ready(self, completed: Iterable[str], failed: Iterable[str] = ()) -> list[str]:
        """Return tasks whose dependencies have all succeeded and are not done."""
        done = set(completed) | set(failed)
        failed_set = set(failed)
        ready: list[str] = []
        for spec in self._tasks.values():
            if spec.name in done:
                continue
            if any(dep in failed_set for dep in spec.depends_on):
                continue
            if all(dep in completed for dep in spec.depends_on):
                ready.append(spec.name)
        return sorted(ready)

    def blocked_by_failure(self, failed: Iterable[str]) -> list[str]:
        """Return tasks that (transitively) depend on a failed task."""
        failed_set = set(failed)
        blocked: set[str] = set()
        changed = True
        while changed:
            changed = False
            for spec in self._tasks.values():
                if spec.name in failed_set or spec.name in blocked:
                    continue
                if any(dep in failed_set or dep in blocked for dep in spec.depends_on):
                    blocked.add(spec.name)
                    changed = True
        return sorted(blocked)

    @classmethod
    def from_functions(cls, name: str, functions: Iterable[Callable[..., Any]]) -> DAG:
        dag = cls(name)
        for fn in functions:
            dag.add_function(fn)
        return dag
