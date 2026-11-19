# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-18

### Added

- Initial public release of Autoflow.
- Decorator-based task definitions and a DAG with cycle detection.
- Async executor with concurrency limits, retries, exponential backoff, and timeouts.
- Cron-like scheduler with 5-field expressions.
- Plugin registry and setuptools entry points under `autoflow.tasks`.
- Built-in task types: `shell`, `http.request`, `files.copy`, `files.move`,
  `files.delete`, `files.write`, `files.read`, `files.mkdir`.
- YAML/JSON pipeline loader with schema validation and `${VAR}` interpolation.
- Structured logging (pretty or JSON).
- SQLite-backed run history.
- Lifecycle event hooks.
- `autoflow` CLI (`run`, `list`, `validate`, `schedule`, `history`, `runtest`).
- `./runtest` bootstrap script, GitHub Actions CI matrix, Docker image, and docs.
