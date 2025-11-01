<!-- Git commit message: Remove advisory lock tasks and enforce shared default retries -->
# Repository Guidelines

## Project Structure & Module Organization
- `dags/` holds the production DAGs; `load_pagila_dag.py` bootstraps the source schema and `replicate_pagila_to_target.py` streams data into the target.
- `include/pagila/` stores the canonical `schema.sql` and `data.sql` assets that Airflow mounts inside the scheduler.
- `plugins/` is reserved for custom Airflow components; keep it empty unless you add operators or hooks tested against Astro.
- `tests/dags/` contains pytest suites that exercise import hygiene, tagging, and retry defaults for every DAG.

## Build, Test, and Development Commands
- `astro dev start` spins up the local Airflow environment; run it from the repo root after Docker and Astro prerequisites are met.
- `astro dev restart` reloads dependencies when `requirements.txt` or DAG code changes.
- `astro dev run pytest tests/dags` executes the DAG-focused pytest suite inside the containerized runtime.
- `astro dev run dags test replicate_pagila_to_target <iso8601>` dry-runs a specific DAG; substitute the execution date you want to validate.

## Coding Style & Naming Conventions
- Use Python 3.10+ with PEP 8 spacing (four-space indents) and type hints, mirroring existing DAG modules.
- Name DAG ids and task ids in `snake_case`, matching the Airflow conventions already in `replicate_pagila_to_target`.
- Store shared constants (connection ids, table lists) in uppercase module-level variables so the hooks remain easy to audit.
- Keep airflow-specific SQL templates in `include/` and reference them with relative paths (`pagila/schema.sql`) via `template_searchpath`.

## Testing Guidelines
- Tests use `pytest` plus Airflow’s `DagBag`; failures usually mean import errors, missing tags, or retries under two.
- Add new DAG tests under `tests/dags/` and mirror the parametrized patterns in `test_dag_example.py`.
- Name fixtures and test functions descriptively (`test_copy_task_idempotent`) so pytest output maps to specific DAG behaviors.
- Run tests inside Astro with `astro dev run pytest` to ensure plugins and Airflow configs load exactly as they do in production.

## Commit & Pull Request Guidelines
- Follow the repo’s history: start commit subjects with an imperative verb and keep them under ~72 characters (`Enhance audit logging...`).
- Group related Astro, SQL, and documentation updates into a single commit to keep DAG revisions traceable.
- Open pull requests with: scope summary, key validation notes (e.g., `astro dev run pytest` output), linked tickets, and screenshots/log excerpts for UI or data-impacting changes.
- Highlight any new connections, environment variables, or credentials required so reviewers can reproduce the pipeline locally.
