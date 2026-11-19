.PHONY: help venv install test lint format typecheck check clean ci

PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

help:
	@echo "Autoflow development targets:"
	@echo "  make venv        Create a virtualenv in $(VENV)"
	@echo "  make install     Install the package with dev extras"
	@echo "  make test        Run the test suite with coverage"
	@echo "  make lint        Run ruff"
	@echo "  make format      Auto-format with ruff"
	@echo "  make typecheck   Run mypy"
	@echo "  make check       lint + typecheck + test"
	@echo "  make clean       Remove caches, reports, and build artifacts"

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(BIN)/python -m pip install --upgrade pip
	$(BIN)/python -m pip install -e ".[dev]"

test:
	./runtest

lint:
	$(BIN)/ruff check src tests

format:
	$(BIN)/ruff format src tests
	$(BIN)/ruff check --fix src tests

typecheck:
	$(BIN)/mypy src/autoflow

check: lint typecheck test

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov reports
	rm -rf build dist *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} +

ci: check
