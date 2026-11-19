# Contributing to Autoflow

Thanks for taking the time to improve Autoflow. This document describes how
to work on the project locally.

## Setup

You need Python 3.10 or later.

```bash
git clone https://github.com/autoflow-dev/autoflow.git
cd autoflow
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

## Checks to run before opening a pull request

```bash
ruff check src tests
ruff format src tests
mypy src/autoflow
./runtest
```

Or, equivalently: `make check`.

`./runtest` writes `reports/junit.xml`, `reports/coverage.xml`, and
`reports/summary.html`. Please do not commit the `reports/` directory.

## Style

- Type-annotate public functions and keep `mypy --strict` clean.
- Follow the existing ruff configuration (line length 100).
- Prefer small, focused modules over one large file.
- Tests live under `tests/` and should use pytest fixtures and parametrization
  where they keep things shorter, not longer.

## Commit messages

Use short, imperative subjects (`Add cron range support`, not `Added` or
`Adding`). Keep unrelated changes in separate commits.

## License

By contributing, you agree that your work is released under the MIT License
and is copyright The Autoflow Authors.
