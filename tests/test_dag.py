"""Tests for DAG construction, validation, and scheduling helpers."""

from __future__ import annotations

import pytest

from autoflow.dag import DAG
from autoflow.exceptions import CycleError, UnknownTaskError, ValidationError
from autoflow.task import TaskSpec, task


def _fn(name: str, deps: list[str] | None = None) -> TaskSpec:
    def inner(ctx):
        return name

    inner.__name__ = name
    return TaskSpec(name=name, func=inner, depends_on=list(deps or []))


def test_add_and_get() -> None:
    dag = DAG("d")
    dag.add(_fn("a"))
    assert "a" in dag
    assert len(dag) == 1
    assert dag.get("a").name == "a"
    assert list(dag.tasks) == ["a"]


def test_duplicate_name_rejected() -> None:
    dag = DAG("d")
    dag.add(_fn("a"))
    with pytest.raises(ValidationError, match="Duplicate"):
        dag.add(_fn("a"))


def test_unknown_task() -> None:
    dag = DAG("d")
    with pytest.raises(UnknownTaskError):
        dag.get("missing")


def test_empty_dag_is_invalid() -> None:
    with pytest.raises(ValidationError, match="no tasks"):
        DAG("empty").validate()


def test_missing_dependency() -> None:
    dag = DAG("d")
    dag.add(_fn("a", ["ghost"]))
    with pytest.raises(ValidationError, match="unknown task"):
        dag.validate()


def test_self_dependency() -> None:
    dag = DAG("d")
    dag.add(_fn("a", ["a"]))
    with pytest.raises(ValidationError, match="itself"):
        dag.validate()


def test_duplicate_dependency_listed() -> None:
    dag = DAG("d")
    dag.add(_fn("a"))
    dag.add(_fn("b", ["a", "a"]))
    with pytest.raises(ValidationError, match="more than once"):
        dag.validate()


def test_simple_cycle_detected() -> None:
    dag = DAG("d")
    dag.add(_fn("a", ["b"]))
    dag.add(_fn("b", ["a"]))
    with pytest.raises(CycleError) as exc:
        dag.validate()
    assert "a" in exc.value.cycle and "b" in exc.value.cycle


def test_three_node_cycle_detected() -> None:
    dag = DAG("d")
    dag.add(_fn("a", ["b"]))
    dag.add(_fn("b", ["c"]))
    dag.add(_fn("c", ["a"]))
    with pytest.raises(CycleError):
        dag.topological_order()


def test_diamond_is_valid_and_levels() -> None:
    dag = DAG("diamond")
    dag.add(_fn("a"))
    dag.add(_fn("b", ["a"]))
    dag.add(_fn("c", ["a"]))
    dag.add(_fn("d", ["b", "c"]))
    order = dag.topological_order()
    assert order[0] == "a"
    assert order[-1] == "d"
    levels = dag.levels()
    assert levels[0] == ["a"]
    assert set(levels[1]) == {"b", "c"}
    assert levels[2] == ["d"]


def test_linear_chain_order() -> None:
    dag = DAG("chain")
    dag.add(_fn("a"))
    dag.add(_fn("b", ["a"]))
    dag.add(_fn("c", ["b"]))
    assert dag.topological_order() == ["a", "b", "c"]


def test_predecessors_successors_edges() -> None:
    dag = DAG("d")
    dag.add(_fn("a"))
    dag.add(_fn("b", ["a"]))
    assert dag.predecessors("b") == ["a"]
    assert dag.successors("a") == ["b"]
    assert dag.edges() == [("a", "b")]
    with pytest.raises(UnknownTaskError):
        dag.successors("nope")


def test_ready_and_blocked_by_failure() -> None:
    dag = DAG("d")
    dag.add(_fn("a"))
    dag.add(_fn("b"))
    dag.add(_fn("c", ["a"]))
    dag.add(_fn("d", ["c"]))
    assert dag.ready([]) == ["a", "b"]
    assert dag.ready(["a"]) == ["b", "c"]
    assert dag.blocked_by_failure(["a"]) == ["c", "d"]
    # Independent branch b is not blocked.
    assert "b" not in dag.blocked_by_failure(["a"])
    assert dag.ready(["a"], failed=["c"]) == ["b"]


def test_from_functions_and_add_function_overrides() -> None:
    @task
    def alpha(ctx):
        return 1

    @task(depends_on=["alpha"])
    def beta(ctx):
        return 2

    dag = DAG.from_functions("fn", [alpha, beta])
    assert len(dag) == 2
    dag2 = DAG("fn2")
    dag2.add_function(alpha, name="start")
    dag2.add_function(beta, name="next", depends_on=["start"])
    assert dag2.get("start").name == "start"
    assert dag2.get("next").depends_on == ["start"]
    dag2.validate()
