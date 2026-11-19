# Autoflow

[![CI](https://img.shields.io/badge/CI-passing-brightgreen)](https://github.com/autoflow-dev/autoflow/actions)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Coverage](https://img.shields.io/badge/coverage-80%25%2B-brightgreen)](reports/summary.html)

Autoflow is a general-purpose Python automation framework. You describe a
pipeline as a DAG of tasks (in Python or YAML), and Autoflow runs it with
concurrency limits, retries, timeouts, structured logs, and SQLite-backed
run history.

## Features

- Decorator-based Python tasks and a YAML/JSON pipeline loader with schema validation
- DAG dependency resolver with cycle detection and concurrent "levels"
- Async executor with concurrency limits, exponential backoff, and timeouts
- Cron-like in-process scheduler
- Plugin registry (built-in types + setuptools entry points)
- Built-in task types: `shell`, `http.request`, and file operations
- Event hooks around run and task lifecycle
- Structured (JSON or pretty) logging
- Persistent run history in SQLite
- `autoflow` CLI plus a `./runtest` bootstrap script that runs the test suite

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Validate and run the example pipeline
autoflow validate examples/hello.yaml
autoflow run examples/hello.yaml

# Inspect recorded runs
autoflow history

# Run the project's own test suite (also via ./runtest)
autoflow runtest --coverage
# or, from a fresh checkout:
./runtest
```

`./runtest` creates `.venv` if needed, installs the package with dev extras,
and writes JUnit XML plus an HTML summary under `reports/`.

## A pipeline in YAML

```yaml
name: hello
max_concurrency: 2
tasks:
  - name: greet
    type: shell
    params:
      command: echo "hello from autoflow"
  - name: save
    type: files.write
    depends_on: [greet]
    params:
      path: out/hello.txt
      content: "hello from autoflow"
```

## A pipeline in Python

```python
import asyncio
from autoflow import DAG, Executor, task

@task(retries=2, timeout=10)
async def fetch(ctx):
    return {"ok": True}

@task(depends_on=["fetch"])
def report(ctx):
    return ctx.upstream("fetch")

async def main():
    dag = DAG.from_functions("demo", [fetch, report])
    result = await Executor().run(dag)
    print(result.status, result.succeeded)

asyncio.run(main())
```

## CLI reference

| Command | Purpose |
| --- | --- |
| `autoflow run PIPELINE` | Execute a pipeline ( `--dry-run`, `--var KEY=VALUE`, `--max-concurrency N` ) |
| `autoflow validate PIPELINE` | Schema + DAG checks without running |
| `autoflow list [PIPELINE]` | List tasks, or `--types` for registered task types |
| `autoflow schedule PIPELINE --cron "*/5 * * * *"` | Repeat a pipeline on a cron schedule |
| `autoflow history` | Show recent runs (`--run-id`, `--limit`, `--json`) |
| `autoflow runtest` | Run pytest, optionally with `--coverage`, `--parallel`, `-m MARKER` |

See [docs/usage.md](docs/usage.md) for a longer walkthrough and
[docs/architecture.md](docs/architecture.md) for how the pieces fit together.

## Development

```bash
make install
make lint
make typecheck
make test          # or ./runtest
```

## License

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 The Autoflow Authors.
