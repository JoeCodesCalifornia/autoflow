# Usage

## Installing

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

The `http` extra (`pip install -e ".[http]"`) adds `httpx` for the HTTP
task. Without it, `http.request` uses the standard library.

## Running a pipeline

```bash
autoflow validate examples/hello.yaml
autoflow run examples/hello.yaml
autoflow run examples/etl.yaml --var SOURCE=data/input.txt --workdir .
autoflow run examples/hello.yaml --dry-run
```

`--var KEY=VALUE` is applied during interpolation, so a field like
`content: "build ${ENV:dev}"` can be overridden from the command line.

Logs default to a compact pretty format. Pass `--json-logs` for one JSON
object per line, and `--log-level DEBUG` for more detail.

## Inspecting history

After a run, Autoflow writes to `.autoflow/history.sqlite` (override with
`--history PATH`):

```bash
autoflow history
autoflow history --limit 5 --json
autoflow history --run-id <id>
```

## Scheduling

A pipeline file may include `schedule: "*/5 * * * *"` (every 5 minutes).
You can also pass `--cron` on the CLI:

```bash
autoflow schedule examples/hello.yaml --cron "0 * * * *" --max-runs 1
```

`--max-runs` is useful for a smoke test of the scheduler. `--interval`
controls how often the process wakes up to check the clock.

Cron fields are the usual five: minute, hour, day of month, month, day of
week (0 or 7 is Sunday). Lists, ranges, steps, and month/weekday names are
supported.

## Writing Python tasks

```python
from autoflow import task, DAG, Executor

@task(name="prepare", retries=1, timeout=5)
def prepare(ctx):
    (ctx.workdir / "data").mkdir(exist_ok=True)
    return "ok"

@task(depends_on=["prepare"])
async def work(ctx):
    ctx.log().info("starting work")
    return ctx.upstream("prepare")
```

Register event hooks if you need custom metrics or notifications:

```python
from autoflow import HookBus, Executor
from autoflow.hooks import TASK_FAILURE

hooks = HookBus()

@hooks.on(TASK_FAILURE)
def on_fail(run_id, task, error, **_):
    print(f"{run_id}: {task} failed: {error}")

executor = Executor(hooks=hooks, max_concurrency=4)
```

## Built-in task types

| Type | Required params | Notes |
| --- | --- | --- |
| `shell` | `command` (string or argv list) | Optional `cwd`, `env`, `input`, `check`, `timeout` |
| `http.request` | `url` | Optional `method`, `headers`, `body`, `dest`, `timeout`, `expected_status` |
| `files.copy` | `src`, `dest` | Copies files or directories |
| `files.move` | `src`, `dest` | |
| `files.delete` | `path` | `missing_ok` defaults to true |
| `files.write` | `path` | `content`, optional `mode` / `encoding` |
| `files.read` | `path` | Returns `{path, content, bytes}` |
| `files.mkdir` | `path` | `parents` and `exist_ok` default to true |

## Adding a plugin

In your package:

```python
class PingTask:
    async def run(self, ctx, params):
        return {"pong": params.get("host")}
```

And in `pyproject.toml`:

```toml
[project.entry-points."autoflow.tasks"]
ping = "mypkg.tasks:PingTask"
```

Then a pipeline can use `type: ping`.

## Running the test suite

```bash
./runtest
# equivalent:
autoflow runtest --coverage --html-report
autoflow runtest --parallel auto -m "not slow"
```

Reports land in `reports/`: `junit.xml`, `coverage.xml`, and `summary.html`.
