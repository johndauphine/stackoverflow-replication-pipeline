# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Pagila End-to-End Data Replication Pipeline** using Apache Airflow 3 and PostgreSQL. It demonstrates production-quality data engineering practices including idempotent data loading, memory-capped streaming replication, audit logging, and resource management.

**Key Components:**
- Source PostgreSQL database (port 5433) with Pagila sample data (DVD rental store)
- Target PostgreSQL database (port 5444) for replication
- Apache Airflow DAGs for orchestration
- Audit trail and data validation

## Development Commands

### Environment Management
```bash
astro dev start          # Start local Airflow environment
astro dev restart        # Reload after dependency changes (requirements.txt, DAG code)
astro dev stop           # Stop all containers
```

### Testing
```bash
astro dev run pytest tests/dags                    # Run DAG validation tests
astro dev run dags test load_pagila_to_postgres <date>  # Dry-run specific DAG
```

### DAG Operations
```bash
astro dev run dags unpause load_pagila_to_postgres     # Enable DAG scheduling
astro dev run dags trigger load_pagila_to_postgres     # Manual trigger
astro dev run dags trigger replicate_pagila_to_target  # Trigger replication
astro dev run connections list                         # View Airflow connections
```

### Database Setup
Start PostgreSQL containers and connect to Astro network:
```bash
# Start source database
docker run -d --name pagila-pg-source -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila -e POSTGRES_PASSWORD=pagila_pw -p 5433:5432 postgres:16

# Start target database  
docker run -d --name pagila-pg-target -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila_tgt -e POSTGRES_PASSWORD=pagila_tgt_pw -p 5444:5432 postgres:16

# Connect to Astro network (find network name with: docker network ls | grep astro)
docker network connect <astro_airflow_network> pagila-pg-source
docker network connect <astro_airflow_network> pagila-pg-target
```

### Download SQL Files
```bash
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql -o include/pagila/schema.sql
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql -o include/pagila/data.sql
```

## Architecture Overview

### DAGs Structure
1. **`load_pagila_dag.py`** - Idempotent source data loading
   - Uses SHA256 hashing to prevent duplicate loads
   - Records audit trail in `pagila_support.pagila_load_audit`
   - Resets `public` schema and loads fresh data
   - Stream processes COPY blocks from SQL files

2. **`replicate_pagila_to_target.py`** - Memory-capped streaming replication
   - Uses `SpooledTemporaryFile` (128MB memory limit) for buffered streaming
   - Copies tables in dependency order to maintain referential integrity
   - Fixes auto-increment sequences after copy
   - Logs memory usage and disk spill events

### Table Dependency Order
```
language → category → actor → film → film_actor → film_category → 
country → city → address → store → customer → staff → 
inventory → rental → payment
```

### Key Configuration
- **Resource Allocation**: Scheduler (2Gi), Webserver (1Gi), Triggerer (512Mi)
- **Parallelism**: 16 parallel tasks, 8 active tasks per DAG, 2 active runs per DAG
- **Memory Streaming**: 128MB in-memory buffer before disk spill

## Code Conventions

### DAG Requirements (Enforced by Tests)
- All DAGs must have tags
- All DAGs must have `retries >= 2` in `default_args`
- No import errors allowed

### Connection IDs
- `pagila_postgres` - Source database connection
- `pagila_tgt` - Target database connection

### File Paths
- SQL files: `/usr/local/airflow/include/pagila/schema.sql`, `/usr/local/airflow/include/pagila/data.sql`
- Use `template_searchpath=["/usr/local/airflow/include"]` in DAG definition

### Python Style
- Python 3.10+ with type hints
- PEP 8 spacing (4-space indents)
- `snake_case` for DAG IDs and task IDs
- Module-level uppercase constants for shared values

## Technology Stack

- **Apache Airflow 3** on Astro Runtime 3.1-3
- **PostgreSQL 16** for source and target databases
- **Python 3.10+** with psycopg2-binary
- **Docker** for database containerization
- **pytest** for DAG validation

## Key Files

- `dags/load_pagila_dag.py` - Source data loading with idempotency
- `dags/replicate_pagila_to_target.py` - Streaming replication logic
- `include/pagila/schema.sql` - Pagila database schema
- `include/pagila/data.sql` - Pagila sample data
- `tests/dags/test_dag_example.py` - DAG validation tests
- `.astro/config.yaml` - Resource allocation settings
- `Dockerfile` - Airflow parallelism configuration

## Verification Commands

```bash
# Check row counts on target
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM actor;"

# View audit history on source
docker exec -it pagila-pg-source psql -U pagila -d pagila -c "SELECT loaded_at, schema_sha256, data_sha256, succeeded FROM pagila_support.pagila_load_audit ORDER BY loaded_at DESC LIMIT 5;"
```