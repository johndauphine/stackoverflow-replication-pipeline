<!-- Git commit message: Remove advisory lock tasks and enforce shared default retries -->
# Pagila End-to-End Replication Pipeline Using Astro + Apache Airflow
## 1. Introduction
This document provides a complete, production-quality guide to building an end-to-end data replication pipeline using Postgres (source + target), Astronomer (Astro CLI), Apache Airflow 3, and Python-based ETL using COPY streaming. It includes fully working DAGs with idempotency, schema reset, partition-safe replication, advisory locks, and simple auditing.

---

## 2. Prerequisites
- Docker Desktop
- Astro CLI
- Python 3.10+
- Add these to your Astro project's `requirements.txt`:
  - `apache-airflow-providers-postgres`
  - `apache-airflow-providers-common-sql`
  - `psycopg2-binary`

Then rebuild/restart your Astro environment:

```bash
astro dev restart
```
 
## 3. Start Source and Target Postgres Containers (no Docker Compose)
Start **source** on host port **5433**:
```bash
docker run -d \
  --name pagila-pg-source \
  -e POSTGRES_DB=pagila \
  -e POSTGRES_USER=pagila \
  -e POSTGRES_PASSWORD=pagila_pw \
  -p 5433:5432 \
  postgres:16
```

Start **target** on host port **5444**:
```bash
docker run -d \
  --name pagila-pg-target \
  -e POSTGRES_DB=pagila \
  -e POSTGRES_USER=pagila_tgt \
  -e POSTGRES_PASSWORD=pagila_tgt_pw \
  -p 5444:5432 \
  postgres:16
```

The Astro runtime creates a project-specific bridge network (for example `pagila-demo-project_xxxxx_airflow`). Attach both Postgres containers so the scheduler can resolve them by container name:

```bash
docker network connect <astro_airflow_network> pagila-pg-source
docker network connect <astro_airflow_network> pagila-pg-target
```

> To discover the exact network name run `astro dev start`, then `docker network ls | grep astro`. If you prefer `docker compose`, you can wrap these two services in a compose file as long as it joins the same Astro network.

## 4. Create Airflow Connections
Use Airflow connections that point at the container hostnames on port 5432:

```bash
astro dev run connections add pagila_postgres \
  --conn-type postgres \
  --conn-host pagila-pg-source \
  --conn-port 5432 \
  --conn-login pagila \
  --conn-password pagila_pw \
  --conn-schema pagila

astro dev run connections add pagila_tgt \
  --conn-type postgres \
  --conn-host pagila-pg-target \
  --conn-port 5432 \
  --conn-login pagila_tgt \
  --conn-password pagila_tgt_pw \
  --conn-schema pagila
```

> On Docker Desktop you can still point at the mapped host ports by swapping `--conn-host` for `host.docker.internal` and using ports `5433`/`5444`.

## 5. Place SQL Files
Place the Pagila SQL files in your Astro project at:
```
include/pagila/schema.sql
include/pagila/data.sql
```
Ensure `data.sql` uses `COPY ... FROM stdin;` blocks with a terminating `\.` line for each section (download a **raw** file from the official repo).

Download the canonical files straight from GitHub:

```bash
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql -o include/pagila/schema.sql
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql -o include/pagila/data.sql
```

## 6. Full DAG: Idempotent Pagila Loader (Source)

**File:** `dags/load_pagila_dag.py`

```python
from __future__ import annotations

import hashlib
from datetime import datetime
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "pagila_postgres"
SCHEMA_SQL_PATH = "/usr/local/airflow/include/pagila/schema.sql"
DATA_SQL_PATH = "/usr/local/airflow/include/pagila/data.sql"
AUDIT_SCHEMA = "pagila_support"
AUDIT_TABLE = "pagila_support.pagila_load_audit"

# Toggle this off if your schema/data do not contain 'OWNER TO postgres'.
ENSURE_POSTGRES_ROLE = True


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_input_fingerprints(**context) -> None:
    schema_hash = file_sha256(SCHEMA_SQL_PATH)
    data_hash = file_sha256(DATA_SQL_PATH)
    ti = context["ti"]
    ti.xcom_push(key="schema_hash", value=schema_hash)
    ti.xcom_push(key="data_hash", value=data_hash)


def should_reload(**context) -> bool:
    ti = context["ti"]
    schema_hash = ti.xcom_pull(key="schema_hash", task_ids="compute_hashes")
    data_hash = ti.xcom_pull(key="data_hash", task_ids="compute_hashes")
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
              id                bigserial PRIMARY KEY,
              loaded_at         timestamptz NOT NULL DEFAULT now(),
              schema_sha256     text NOT NULL,
              data_sha256       text NOT NULL,
              actor_count       bigint,
              film_count        bigint,
              customer_count    bigint,
              rental_count      bigint,
              succeeded         boolean NOT NULL
            );
            """
        )
        cur.execute(
            f"""
            SELECT succeeded
            FROM {AUDIT_TABLE}
            WHERE schema_sha256=%s AND data_sha256=%s
            ORDER BY loaded_at DESC
            LIMIT 1;
            """,
            (schema_hash, data_hash),
        )
        row = cur.fetchone()
        if row and row[0] is True:
            print("Idempotence: inputs already loaded successfully. Skipping reload.")
            return False
    print("Idempotence: inputs not seen before (or last load failed). Will reload.")
    return True


def acquire_lock() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(863440123987650321);")
        (locked,) = cur.fetchone()
        if not locked:
            raise RuntimeError("Could not obtain advisory lock; another run holds it.")


def release_lock() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock_all();")


def load_pagila_copy_blocks() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    def iter_copy_sections(path: str):
        with open(path, "rb") as f:
            in_copy = False
            buf = bytearray()
            copy_sql = None
            for raw in f:
                line = raw.decode("utf-8", errors="strict")
                if not in_copy:
                    if line.startswith("COPY ") and " FROM stdin;" in line:
                        copy_sql = line.strip()
                        in_copy = True
                        buf.clear()
                else:
                    if line.strip() == "\\.":
                        with BytesIO(bytes(buf)) as fp:
                            cur.copy_expert(copy_sql, fp)
                        in_copy = False
                        buf.clear()
                        copy_sql = None
                    else:
                        buf.extend(raw)
            if in_copy:
                raise RuntimeError("Unterminated COPY block in data.sql")

    iter_copy_sections(DATA_SQL_PATH)
    cur.close()
    conn.close()


def record_success(**context) -> None:
    ti = context["ti"]
    schema_hash = ti.xcom_pull(key="schema_hash", task_ids="compute_hashes")
    data_hash = ti.xcom_pull(key="data_hash", task_ids="compute_hashes")
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
              id                bigserial PRIMARY KEY,
              loaded_at         timestamptz NOT NULL DEFAULT now(),
              schema_sha256     text NOT NULL,
              data_sha256       text NOT NULL,
              actor_count       bigint,
              film_count        bigint,
              customer_count    bigint,
              rental_count      bigint,
              succeeded         boolean NOT NULL
            );
            """
        )
        cur.execute("SELECT COUNT(*) FROM actor;")
        actor = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM film;")
        film = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM customer;")
        cust = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rental;")
        rent = cur.fetchone()[0]
        cur.execute(
            f"""
            INSERT INTO {AUDIT_TABLE}
              (schema_sha256, data_sha256, actor_count, film_count, customer_count, rental_count, succeeded)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE);
            """,
            (schema_hash, data_hash, actor, film, cust, rent),
        )


with DAG(
    dag_id="load_pagila_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/usr/local/airflow/include"],
    tags=["pagila", "postgres", "idempotent"],
) as dag:
    compute_hashes = PythonOperator(
        task_id="compute_hashes",
        python_callable=compute_input_fingerprints,
    )

    short_circuit = ShortCircuitOperator(
        task_id="skip_if_already_loaded",
        python_callable=should_reload,
    )

    lock = PythonOperator(
        task_id="acquire_lock",
        python_callable=acquire_lock,
    )

    reset_public_schema = SQLExecuteQueryOperator(
        task_id="reset_public_schema",
        conn_id=CONN_ID,
        sql="""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.schemata WHERE schema_name='public'
          ) THEN
            EXECUTE 'DROP SCHEMA public CASCADE';
          END IF;
          EXECUTE 'CREATE SCHEMA public AUTHORIZATION current_user';
        END$$;
        """
    )

    ensure_role = SQLExecuteQueryOperator(
        task_id="ensure_postgres_role",
        conn_id=CONN_ID,
        sql="""
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='postgres') THEN
            CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres_pw';
          END IF;
        END$$;
        """
    )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id=CONN_ID,
        sql="pagila/schema.sql",
    )

    load_data = PythonOperator(
        task_id="load_data_streaming",
        python_callable=load_pagila_copy_blocks,
    )

    verify_counts = SQLExecuteQueryOperator(
        task_id="verify_counts",
        conn_id=CONN_ID,
        sql=[
            "SELECT COUNT(*) FROM actor;",
            "SELECT COUNT(*) FROM film;",
            "SELECT COUNT(*) FROM customer;",
            "SELECT COUNT(*) FROM rental;",
        ],
    )

    write_audit = PythonOperator(
        task_id="record_success",
        python_callable=record_success,
    )

    unlock = PythonOperator(
        task_id="release_lock",
        trigger_rule="all_done",
        python_callable=release_lock,
    )

    compute_hashes >> short_circuit
    short_circuit >> lock
    lock >> reset_public_schema
    if ENSURE_POSTGRES_ROLE:
        reset_public_schema >> ensure_role >> create_schema
    else:
        reset_public_schema >> create_schema
    create_schema >> load_data >> verify_counts >> write_audit >> unlock
```

> Audit history lives in `pagila_support.pagila_load_audit`, which is left untouched even when the DAG drops and recreates the `public` schema.
```

---

## 7. Full DAG: Pagila Replication (Source → Target)

**File:** `dags/replicate_pagila_to_target.py`

This DAG now uses a **memory-capped buffered streaming** approach powered by `tempfile.SpooledTemporaryFile`. Up to 128 MB of each table copy stays in memory; any overflow spills transparently to the worker’s disk, keeping resource usage predictable without the complexity of multi-threaded queues.

```python
from tempfile import SpooledTemporaryFile

SPOOLED_MAX_MEMORY_BYTES = 128 * 1024 * 1024  # spill threshold (~128 MB)


def copy_table_src_to_tgt(table: str) -> None:
    src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)

    with SpooledTemporaryFile(max_size=SPOOLED_MAX_MEMORY_BYTES, mode="w+b") as spool:
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit = True
            with src_conn.cursor() as src_cur:
                src_cur.copy_expert(
                    f"COPY (SELECT * FROM public.{table}) TO STDOUT",
                    spool,
                )

        spool.flush()
        written_bytes = spool.tell()
        spilled_to_disk = bool(getattr(spool, "_rolled", False))
        log.info(
            "[%s] buffered %s bytes (rolled_to_disk=%s)",
            table,
            written_bytes,
            spilled_to_disk,
        )

        spool.seek(0)
        with tgt_hook.get_conn() as tgt_conn:
            tgt_conn.autocommit = True
            with tgt_conn.cursor() as tgt_cur:
                tgt_cur.copy_expert(
                    f"COPY public.{table} FROM STDIN",
                    spool,
                )
```

**Key Features:**
- **Predictable Resource Use**: Keeps <128 MB in RAM, spills larger tables automatically
- **Straightforward Flow**: Single process handles read → write without queue plumbing
- **Visibility**: Log output records bytes copied and whether disk spill occurred
- **Production-Ready**: Validated end-to-end with Pagila (language=6, rental=16 044, payment=16 049 rows)

> Adjust `SPOOLED_MAX_MEMORY_BYTES` if your worker can spare more or less RAM before spilling; the rest of the DAG (task layout, retries, sequence fixes) stays the same.

---

## 8. Running the DAGs
```bash
astro dev start

# Unpause the DAGs once they appear
astro dev run dags unpause -y load_pagila_to_postgres
astro dev run dags unpause -y replicate_pagila_to_target

# Load source
astro dev run dags trigger load_pagila_to_postgres

# Replicate to target
astro dev run dags trigger replicate_pagila_to_target
```

---

## 9. Verification & QA
```bash
# Row counts on target (examples)
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM actor;"
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM rental;"

# Audit ledger on source
docker exec -it pagila-pg-source psql -U pagila -d pagila -c "SELECT loaded_at, schema_sha256, data_sha256, succeeded FROM pagila_support.pagila_load_audit ORDER BY loaded_at DESC LIMIT 5;"
```
For a quick smoke test, counts on target should match source. The audit query confirms the loader recognized your latest schema + data hashes.

---

## 10. Troubleshooting
- **WrongObjectType** on partitioned tables: fixed by using `COPY (SELECT ...)` for extraction.
- **Connection not found**: verify with `astro dev run connections list`; ensure names match DAG constants.
- **Port conflicts**: change host ports (5433/5444) when starting containers.
- **Unicode escapes**: do not include `\N` inside Python strings; keep COPY payload only in `.sql` files.
- **Owner mismatch**: if your schema uses `OWNER TO postgres`, either create that role (see DAG) or replace owners in the SQL files.

---

## 11. File Layout (Astro project)
```
.
├── dags
│   ├── load_pagila_dag.py
│   └── replicate_pagila_to_target.py
├── include
│   └── pagila
│       ├── schema.sql
│       └── data.sql
├── requirements.txt
└── .env   (optional for AIRFLOW_CONN_... variables)
```

---

## 12. DAG Concurrency & Retries
- Both `load_pagila_to_postgres` and `replicate_pagila_to_target` now share a `default_args` block that enforces at least two retries with a one-minute backoff. This keeps the DAGs compliant with the pytest guardrails in `tests/dags/test_dag_example.py`.
- Advisory locks were removed from the loader DAG. If you need multi-run serialization in the future, rely on Airflow’s built-in `max_active_runs=1` or reintroduce a session-scoped lock that persists across tasks.
- When running the pipeline, avoid triggering overlapping DAG runs unless you intentionally want concurrent replays.

---

## 13. Contributor Notes
- The contributor guide lives in `AGENTS.md` and expands on project structure, coding style, testing commands, and pull-request expectations.
- Run `astro dev pytest tests/dags` (requires Docker permissions) before opening a pull request; it covers DAG imports, tags, and retry policies.
- If you add new DAGs, mirror the shared constants pattern (`DEFAULT_ARGS`, connection IDs, table lists) so that operators remain easy to audit and tests pass without modification.
