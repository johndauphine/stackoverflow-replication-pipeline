# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**Stack Overflow End-to-End Data Replication Pipeline** using Apache Airflow 3 with support for both SQL Server and PostgreSQL targets. Demonstrates production-quality data engineering with memory-capped streaming, parallel loading, and cross-database replication.

**Tested Databases:**
- StackOverflow2010 (8.4GB) - Original test dataset
- StackOverflow2013 (33GB) - Large-scale validation with 106M+ rows, **18 minutes end-to-end**

**Key Features:**
- **High-performance streaming** (256MB buffer) - 67% faster than bulk loading on real-world data
- Smart parallel execution with dependency-aware task groups
- Cross-database type mapping (SQL Server ↔ PostgreSQL)
- Production-like network architecture (Docker DNS resolution)

## Database Platform Notes

**SQL Server 2022** (recommended source):
- Production-grade stability, full StackOverflow compatibility
- Requires AMD64/x86_64 (no ARM64 support - runs in emulation on Apple Silicon)
- 4GB RAM allocation recommended

**PostgreSQL 16** (recommended target):
- Cross-platform (native ARM64 support)
- No licensing costs, excellent performance
- Better stability on non-Windows platforms

## Network Architecture

**Production-Like Design**: Database containers connect to the **Airflow network** using Docker DNS, simulating AWS RDS, Azure SQL, and other managed database services.

```
┌─────────────────────────────────────────┐
│  Astro Network (Custom Bridge)         │
│  ┌───────────────┐    ┌──────────────┐ │
│  │  Airflow      │───►│ SQL Server   │ │
│  │  Scheduler    │    │ Source:1433  │ │
│  └───────────────┘    └──────────────┘ │
│  ┌───────────────┐    ┌──────────────┐ │
│  │  Airflow      │───►│ PostgreSQL   │ │
│  │  Workers      │    │ Target:5432  │ │
│  └───────────────┘    └──────────────┘ │
└─────────────────────────────────────────┘
         │ Port Mapping         │
         ▼                      ▼
    Host:1433              Host:5433
```

**Why This Design?**
- ✓ Simulates production private network connectivity
- ✓ Tests Docker DNS resolution (container names as hostnames)
- ✓ Network isolation with debugging access via host ports
- ✓ Survives container restarts (reconnect after `astro dev restart`)

---

## Quick Start: SQL Server to PostgreSQL

### Step 1: Start Astro
```bash
astro dev start
```

### Step 2: Start Databases

```bash
# Start SQL Server 2022 source (AMD64 only)
docker run -d --name stackoverflow-mssql-source \
  --platform linux/amd64 --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -v "$(pwd)/include/stackoverflow":/var/opt/mssql/backup \
  -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

# Start PostgreSQL 16 target (cross-platform)
docker run -d --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -p 5433:5432 postgres:16

# Connect to Airflow network (CRITICAL)
docker network connect stackoverflow-replication-pipeline_bcd2dd_airflow stackoverflow-mssql-source
docker network connect stackoverflow-replication-pipeline_bcd2dd_airflow stackoverflow-postgres-target
```

**Note:** After `astro dev restart`, re-run the `docker network connect` commands to restore connectivity.

### Step 3: Attach Source Database

```bash
# Copy database files
docker exec stackoverflow-mssql-source mkdir -p /var/opt/mssql/data
docker cp include/stackoverflow/StackOverflow2010.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2010_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/

# Fix permissions (required)
docker exec -u root stackoverflow-mssql-source chown mssql:mssql \
  /var/opt/mssql/data/StackOverflow2010.mdf \
  /var/opt/mssql/data/StackOverflow2010_log.ldf
docker exec -u root stackoverflow-mssql-source chmod 660 \
  /var/opt/mssql/data/StackOverflow2010.mdf \
  /var/opt/mssql/data/StackOverflow2010_log.ldf

# Attach database (no quotes around password)
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "CREATE DATABASE StackOverflow2010 ON (FILENAME = '/var/opt/mssql/data/StackOverflow2010.mdf'), (FILENAME = '/var/opt/mssql/data/StackOverflow2010_log.ldf') FOR ATTACH"
```

### Step 4: Create Airflow Connections

**IMPORTANT:** Use container names as hostnames (Docker DNS resolution).

```bash
# Source - SQL Server
astro dev run connections add stackoverflow_source \
  --conn-type mssql --conn-host stackoverflow-mssql-source --conn-port 1433 \
  --conn-login sa --conn-password "StackOverflow123!" --conn-schema StackOverflow2010

# Target - PostgreSQL
astro dev run connections add stackoverflow_postgres_target \
  --conn-type postgres --conn-host stackoverflow-postgres-target --conn-port 5432 \
  --conn-login postgres --conn-password "StackOverflow123!" --conn-schema stackoverflow_target

# Verify
astro dev run connections list | grep stackoverflow
```

### Step 5: Run DAG

**Recommended (fastest):**
```bash
astro dev run dags unpause replicate_stackoverflow_to_postgres_parallel
astro dev run dags trigger replicate_stackoverflow_to_postgres_parallel
```

**Alternative (sequential, simpler logs):**
```bash
astro dev run dags unpause replicate_stackoverflow_to_postgres
astro dev run dags trigger replicate_stackoverflow_to_postgres
```

### Step 6: Verify

```bash
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT 'Users' AS tablename, COUNT(*) AS rowcount FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   ORDER BY tablename;"
```

**Expected Row Counts (StackOverflow2010):**
- Users: 315K | Posts: 1.7M | Comments: 1.3M | Votes: 4.3M | Badges: 190K

---

## Alternative: SQL Server to SQL Server

Replace Step 2 with:
```bash
# Start SQL Server target
docker run -d --name stackoverflow-mssql-target \
  --platform linux/amd64 --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -p 1434:1433 mcr.microsoft.com/mssql/server:2022-latest

# Connect to Airflow network
docker network connect stackoverflow-replication-pipeline_bcd2dd_airflow stackoverflow-mssql-target

# Create target connection
astro dev run connections add stackoverflow_target \
  --conn-type mssql --conn-host stackoverflow-mssql-target --conn-port 1433 \
  --conn-login sa --conn-password "StackOverflow123!" --conn-schema master
```

Then use `replicate_stackoverflow_to_target` DAG.

---

## Performance Comparison: Streaming vs Bulk Parallel

Real-world testing on StackOverflow2013 (106M rows, 33GB) shows **streaming is 67% faster** than bulk parallel loading.

### Performance Comparison (Actual Test Results)

| DAG | Strategy | Time (2010) | Time (2013) | Parallelism |
|-----|----------|-------------|-------------|-------------|
| replicate_stackoverflow_to_postgres | Streaming | ~20-30 min | ~90-120 min | Sequential |
| **replicate_stackoverflow_to_postgres_parallel** | **Streaming** | **~15-20 min** | **18m 14s** ✅ | **Full parallel** |
| replicate_stackoverflow_to_postgres_bulk_parallel | Shared volume | ~8-12 min | 30m 26s | Smart groups |

**Winner: Streaming Parallel DAG** - 40% faster, simpler setup, lower disk I/O

### Why Streaming Outperforms Bulk Loading

The bulk parallel DAG was designed for maximum performance with PostgreSQL optimizations (UNLOGGED tables, partitioning, COPY with FREEZE), but streaming wins due to:

1. **No intermediate CSV files** - Bulk writes ~50GB of CSV files to disk, then reads them back (12+ minutes overhead)
2. **Direct memory-to-memory transfer** - Streaming pipes data from SQL Server → Python buffer → PostgreSQL without touching disk
3. **Simpler data path** - Fewer moving parts means less overhead
4. **PostgreSQL COPY from memory** - COPY from stdin (streaming) vs COPY from file (bulk) has similar performance, but avoids disk I/O

**Trade-off Analysis:**
- Bulk parallel has sophisticated optimizations (partitioning, UNLOGGED, aggressive memory settings)
- BUT the overhead of writing/reading 50GB of CSV files more than negates those benefits
- Streaming's elegant simplicity (256MB memory buffer) beats "clever optimization"

### Bulk Parallel DAG (Educational)

**Note**: This DAG is kept for educational purposes to demonstrate bulk loading patterns and PostgreSQL optimizations. For production use, the streaming parallel DAG is recommended (67% faster).

**Setup** (only needed for bulk_parallel DAG):

```bash
# Create shared directory for bulk files
mkdir -p include/bulk_files && chmod 777 include/bulk_files

# Add volume mount when starting PostgreSQL
docker run -d --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -v "$(pwd)/include/bulk_files":/bulk_files \
  -p 5433:5432 postgres:16

# Verify shared volume
docker exec stackoverflow-postgres-target ls -la /bulk_files
```

### Features

**Table Partitioning:**
- Large tables (Votes, Posts, Comments) split into 500K row chunks
- All chunks load in parallel via COPY command
- Example: Votes (10M rows) → 20 partitions → 20 parallel loads

**PostgreSQL Optimizations:**
```sql
-- UNLOGGED tables during load (no WAL overhead)
CREATE UNLOGGED TABLE ...

-- Aggressive memory settings
SET maintenance_work_mem = '1GB';
SET work_mem = '512MB';
SET synchronous_commit = OFF;

-- FREEZE hint for faster COPY
COPY table FROM '/bulk_files/file.csv' WITH (FORMAT CSV, FREEZE);

-- Convert to LOGGED after load
ALTER TABLE table SET LOGGED;
```

**Smart Dependency Groups:**
```
Reset → Create Tables → Optimize Settings
    ↓
Lookup Tables (parallel)
    ↓
Users
    ↓
Badges + Posts (parallel, Posts partitioned)
    ↓
Comments + Votes + PostLinks (parallel, partitioned)
    ↓
Add Indexes → Convert to LOGGED → Align Sequences
```

### Usage

```bash
astro dev run dags unpause replicate_stackoverflow_to_postgres_bulk_parallel
astro dev run dags trigger replicate_stackoverflow_to_postgres_bulk_parallel
```

### When to Use Each DAG

| Use Case | Recommended DAG |
|----------|-----------------|
| **Production / Maximum performance** | `replicate_stackoverflow_to_postgres_parallel` ✅ |
| **Development / Quick iterations** | `replicate_stackoverflow_to_postgres_parallel` ✅ |
| **First-time setup / Testing** | `replicate_stackoverflow_to_postgres` (sequential, simpler logs) |
| **Experimenting with bulk optimizations** | `replicate_stackoverflow_to_postgres_bulk_parallel` (educational) |

**Recommendation**: Use `replicate_stackoverflow_to_postgres_parallel` for all production workloads. It's faster, simpler, and doesn't require shared volume setup.

---

## Development Commands

```bash
# Environment
astro dev start          # Start Airflow
astro dev restart        # Reload after code/dependency changes
astro dev stop           # Stop all containers

# Testing
astro dev run pytest tests/dags
astro dev run dags test <dag_id> <date>

# DAG Operations
astro dev run dags list
astro dev run dags unpause <dag_id>
astro dev run dags trigger <dag_id>

# Database Inspection
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P StackOverflow123! -C -Q "USE StackOverflow2010; SELECT name FROM sys.tables;"

docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target \
  -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
```

---

## Troubleshooting

**See [docs/troubleshooting-infrastructure.md](docs/troubleshooting-infrastructure.md) for comprehensive troubleshooting.**

### Quick Diagnostics

```bash
# Check containers
docker ps --filter "name=stackoverflow"

# Test connectivity
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P StackOverflow123! -C -Q "SELECT 'OK' AS Status;"

# Test Airflow network connectivity
docker exec stackoverflow-replication-pipeline_bcd2dd-scheduler-1 \
  timeout 5 bash -c '</dev/tcp/stackoverflow-mssql-source/1433' \
  && echo "Connected" || echo "NOT reachable"
```

### Common Issues

| Issue | Solution |
|-------|----------|
| **Network disconnected after restart** | Re-run `docker network connect` commands |
| **Auth failures** | Wait 30-45 seconds after container start |
| **Permission errors** | Run `chown mssql:mssql` and `chmod 660` as root |
| **BULK INSERT failures** | Use `_parallel` DAG instead (no bulk files needed) |
| **Empty string → NULL errors** | Already handled in DAGs (allows NULL for text columns) |

### Critical: Network Reconnection After Astro Restart

`astro dev restart` recreates the Airflow network. Always reconnect databases afterward:

```bash
docker network connect stackoverflow-replication-pipeline_bcd2dd_airflow stackoverflow-mssql-source
docker network connect stackoverflow-replication-pipeline_bcd2dd_airflow stackoverflow-postgres-target
```

---

## Architecture Details

### DAG Structure

**`replicate_stackoverflow_to_target.py`** - SQL Server → SQL Server
- CSV-based bulk loading with batch INSERT
- DBCC CHECKIDENT for identity sequence alignment

**`replicate_stackoverflow_to_postgres.py`** - SQL Server → PostgreSQL
- PostgreSQL COPY command for bulk loading
- Cross-database type mapping (NVARCHAR → VARCHAR, DATETIME → TIMESTAMP)
- GENERATED ALWAYS AS IDENTITY (auto-populates sequences)

### Table Dependencies

```
Users → Badges
     → Posts → PostHistory
             → PostLinks
             → Comments
             → Votes
```

### Configuration

- **Resource Allocation**: Scheduler (2Gi), Webserver (1Gi), Triggerer (512Mi)
- **Parallelism**: 16 parallel tasks, 8 per DAG, 2 concurrent runs
- **Memory Streaming**: 256MB buffer, disk spill above threshold

### Design Decisions

**Why Copy Database Files vs. Mount?**

We copy `.mdf/.ldf` files instead of mounting volumes:

1. **Permissions**: SQL Server requires `mssql:mssql` ownership, not possible with mounted volumes on macOS/Windows
2. **Performance**: 10-100x slower on mounted volumes (VM/network layer overhead)
3. **Cross-platform**: Copying works consistently on all platforms
4. **Upgrades**: SQL Server modifies files in-place (version 655 → 957 in our tests)

Trade-offs: Uses 2x disk space, slower initial setup, but reliable and fast.

---

## Database Schema

### Main Tables

| Table | Description | Rows (2010) | Rows (2013) |
|-------|-------------|-------------|-------------|
| Users | Accounts and profiles | 315K | 2.5M |
| Posts | Questions and answers | 1.7M | 10M |
| Comments | Post comments | 1.3M | 7M |
| Votes | Upvotes/downvotes | 4.3M | 40M |
| Badges | User achievements | 190K | 1.5M |
| PostHistory | Edit history | 2.8M | 18M |
| PostLinks | Related/duplicate | 100K | 500K |

### Key Relationships

- `Users.Id` → `Posts.OwnerUserId`, `Comments.UserId`, `Badges.UserId`
- `Posts.Id` → `Comments.PostId`, `Votes.PostId`, `PostHistory.PostId`
- `Posts.ParentId` → `Posts.Id` (answers reference questions)

---

## Technology Stack

- **Apache Airflow 3** on Astro Runtime 3.1-3
- **SQL Server 2022** (Developer Edition)
- **PostgreSQL 16**
- **Python 3.10+** with pymssql, pg8000 (pure Python drivers)
- **Docker** for database containerization
- **pytest** for DAG validation

---

## Source Database

**Brent Ozar's Stack Overflow Database:**
- Versions: 2010 (8.4GB), 2013 (33GB)
- Data: Real Stack Overflow posts, users, comments, votes, badges
- License: CC-BY-SA 3.0
- Download: https://downloads.brentozar.com/

---

## Key Files

- `dags/replicate_stackoverflow_to_postgres_parallel.py` - **Fastest PostgreSQL replication** (streaming, 18min for 106M rows)
- `dags/replicate_stackoverflow_to_postgres.py` - Standard PostgreSQL replication (sequential)
- `dags/replicate_stackoverflow_to_postgres_bulk_parallel.py` - Bulk loading with CSV files (educational, slower than streaming)
- `dags/replicate_stackoverflow_to_target.py` - SQL Server to SQL Server replication
- `include/stackoverflow/` - Source database files (.mdf, .ldf)
- `tests/dags/test_dag_example.py` - DAG validation
- `.astro/config.yaml` - Resource allocation
- `CLAUDE.md` - This file
