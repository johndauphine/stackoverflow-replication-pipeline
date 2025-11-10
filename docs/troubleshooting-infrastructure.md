# Infrastructure Troubleshooting Guide

## Overview

This document captures infrastructure issues encountered during Stack Overflow replication pipeline testing, particularly on macOS ARM64 (Apple Silicon) systems.

## Known Issues

### 1. SQL Server Container Instability on ARM64

**Symptoms:**
- SQL Server containers crash with core dumps
- Intermittent authentication failures
- "Login failed for user 'sa'" errors even with correct credentials
- Container exits with code 1

**Root Cause:**
- SQL Server 2022 does not natively support ARM64
- Runs in x86_64 emulation mode (Rosetta 2) on Apple Silicon
- Azure SQL Edge (ARM64 build) crashes under heavy write loads

**Evidence:**
```
Dump already generated: /var/opt/mssql/log/core.sqlservr.11_10_2025_2_51_34.27
Mon Nov 10 02:51:36 UTC 2025 Capturing program information
```

**Solutions:**

**Option 1: Use AMD64/x86_64 Hardware (Recommended for SQL Server)**
```bash
# Run SQL Server on cloud AMD64 VM (AWS, Azure, GCP)
# Or use Intel-based Mac hardware
docker run --platform linux/amd64 mcr.microsoft.com/mssql/server:2022-latest
```

**Option 2: Use PostgreSQL Target (Recommended for ARM64)**
```bash
# PostgreSQL has native ARM64 support and is more stable
docker run -d --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -p 5433:5432 postgres:16
```

**Option 3: Add Health Checks and Retry Logic**
```bash
# Wait for SQL Server to be fully ready
docker run -d --name stackoverflow-mssql-target \
  --platform linux/amd64 \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -p 1434:1433 \
  --health-cmd='/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q "SELECT 1"' \
  --health-interval=10s \
  --health-timeout=5s \
  --health-retries=10 \
  mcr.microsoft.com/mssql/server:2022-latest

# Wait for healthy status
timeout 120 bash -c 'until docker inspect --format="{{.State.Health.Status}}" stackoverflow-mssql-target | grep -q healthy; do sleep 2; done'
```

---

### 2. Airflow Connection Configuration

**Symptoms:**
- "Unable to connect: Adaptive Server is unavailable"
- Connection refused errors
- Tasks fail immediately with connection errors

**Root Cause:**
- Database containers run on **separate Docker networks** from Airflow (production-like design)
- Connections must use host IP, not container names
- Platform-specific host IP addresses

**Solutions:**

**For macOS/Windows:**
```bash
astro dev run connections add stackoverflow_source \
  --conn-type mssql \
  --conn-host host.docker.internal \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema StackOverflow2010

astro dev run connections add stackoverflow_target \
  --conn-type mssql \
  --conn-host host.docker.internal \
  --conn-port 1434 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema master
```

**For Linux (ChromeOS, Ubuntu, etc.):**
```bash
astro dev run connections add stackoverflow_source \
  --conn-type mssql \
  --conn-host 172.17.0.1 \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema StackOverflow2010

astro dev run connections add stackoverflow_target \
  --conn-type mssql \
  --conn-host 172.17.0.1 \
  --conn-port 1434 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema master
```

**Verification:**
```bash
# Test connection from within Airflow scheduler
astro dev run tasks test <dag_id> <task_id> <execution_date>
```

---

### 3. BCP DAG Docker CLI Limitation

**Symptoms:**
- BCP export tasks succeed
- BCP import tasks fail with: `/bin/sh: 1: docker: not found`
- Exit code 127 (command not found)

**Root Cause:**
- BCP DAG requires `docker cp` and `docker exec` commands
- Astro runtime does not include Docker CLI in worker containers
- Cannot execute Docker commands from within Airflow tasks

**Solutions:**

**Option 1: Use Shared Volume Mount**
```bash
# Mount same directory in both Airflow and SQL Server containers
docker run -d --name stackoverflow-mssql-target \
  -v /tmp/bcp_data:/var/opt/mssql/bcp_data \
  mcr.microsoft.com/mssql/server:2022-latest

# Airflow can write to /tmp/bcp_data, SQL Server can read from it
```

**Option 2: Use Network-Based Transfer**
```python
# Stream data over SQL connection instead of file-based transfer
# Use BULK INSERT with network path or direct INSERT statements
```

**Option 3: Use Alternative Optimization Approach**
```bash
# Use the optimized DAG instead (no Docker CLI required)
astro dev run dags trigger replicate_stackoverflow_to_target_optimized
```

**Recommendation:**
For standard Airflow deployments, **avoid BCP DAG**. Use:
- `replicate_stackoverflow_to_target_optimized.py` (53% faster, no Docker CLI needed)
- `replicate_stackoverflow_to_target_no_indexes.py` (drop/rebuild indexes approach)

---

### 4. SQL Server Startup Time

**Symptoms:**
- Authentication failures immediately after container start
- "Login failed for user 'sa'" despite correct credentials
- Database not accepting connections

**Root Cause:**
- SQL Server requires 30-60 seconds to fully initialize
- SA password setup happens asynchronously
- Database recovery must complete before accepting connections

**Solutions:**

**Wait for Ready State:**
```bash
# Method 1: Fixed delay
docker start stackoverflow-mssql-target
sleep 45

# Method 2: Health check loop
docker start stackoverflow-mssql-target
timeout 120 bash -c 'while ! docker exec stackoverflow-mssql-target /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q "SELECT 1" &> /dev/null; do sleep 2; done'

# Method 3: Docker health check (recommended)
docker run -d --name stackoverflow-mssql-target \
  --health-cmd='/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q "SELECT 1"' \
  --health-interval=10s \
  mcr.microsoft.com/mssql/server:2022-latest

docker start stackoverflow-mssql-target
docker wait --condition=healthy stackoverflow-mssql-target
```

**Best Practice:**
Always wait for SQL Server to report healthy status before running DAGs.

---

### 5. Memory Constraints

**Symptoms:**
- SQL Server container crashes under load
- Out of memory errors
- Slow query performance

**Root Cause:**
- Default Docker memory limit may be insufficient
- Large datasets (10M+ rows) require significant RAM
- SQL Server minimum 2GB, recommended 4GB

**Solutions:**

```bash
# Allocate sufficient memory (4GB recommended)
docker run -d --name stackoverflow-mssql-target \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  mcr.microsoft.com/mssql/server:2022-latest

# Monitor memory usage
docker stats stackoverflow-mssql-target
```

**Expected Memory Usage:**
- Idle: 500MB - 1GB
- During replication: 2GB - 3.5GB
- Peak (large table load): Up to 4GB

---

## Testing Workflow Checklist

When testing Stack Overflow replication DAGs:

1. **✓ Verify Platform**
   ```bash
   uname -m  # aarch64/arm64 = Apple Silicon, x86_64 = Intel/AMD
   ```

2. **✓ Start SQL Server Containers with Health Checks**
   ```bash
   docker run -d --name stackoverflow-mssql-source \
     --platform linux/amd64 --memory="4g" \
     --health-cmd='/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q "SELECT 1"' \
     -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
     -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

   docker run -d --name stackoverflow-mssql-target \
     --platform linux/amd64 --memory="4g" \
     --health-cmd='/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q "SELECT 1"' \
     -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
     -p 1434:1433 mcr.microsoft.com/mssql/server:2022-latest
   ```

3. **✓ Wait for Healthy Status**
   ```bash
   timeout 120 bash -c 'until docker inspect --format="{{.State.Health.Status}}" stackoverflow-mssql-source | grep -q healthy; do echo "Waiting..."; sleep 5; done'
   timeout 120 bash -c 'until docker inspect --format="{{.State.Health.Status}}" stackoverflow-mssql-target | grep -q healthy; do echo "Waiting..."; sleep 5; done'
   ```

4. **✓ Attach Source Database** (if needed)
   ```bash
   docker exec stackoverflow-mssql-source mkdir -p /var/opt/mssql/data
   docker cp include/stackoverflow/StackOverflow2010.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
   docker cp include/stackoverflow/StackOverflow2010_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/
   docker exec -u root stackoverflow-mssql-source chown mssql:mssql /var/opt/mssql/data/StackOverflow2010.*
   docker exec -u root stackoverflow-mssql-source chmod 660 /var/opt/mssql/data/StackOverflow2010.*
   docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
     "CREATE DATABASE StackOverflow2010 ON (FILENAME = '/var/opt/mssql/data/StackOverflow2010.mdf'), (FILENAME = '/var/opt/mssql/data/StackOverflow2010_log.ldf') FOR ATTACH"
   ```

5. **✓ Configure Airflow Connections**
   ```bash
   # macOS/Windows
   astro dev run connections add stackoverflow_source \
     --conn-type mssql --conn-host host.docker.internal --conn-port 1433 \
     --conn-login sa --conn-password "StackOverflow123!" --conn-schema StackOverflow2010

   astro dev run connections add stackoverflow_target \
     --conn-type mssql --conn-host host.docker.internal --conn-port 1434 \
     --conn-login sa --conn-password "StackOverflow123!" --conn-schema master
   ```

6. **✓ Test Connection**
   ```bash
   astro dev run tasks test <dag_id> reset_target_schema 2025-11-10
   ```

7. **✓ Monitor During Execution**
   ```bash
   docker stats --no-stream
   docker logs -f stackoverflow-replication-pipeline_*-scheduler-1
   ```

---

## Performance Expectations by Platform

| Platform | SQL Server Performance | Recommendation |
|----------|----------------------|----------------|
| **Intel/AMD Mac** | Good | ✅ Use SQL Server |
| **Apple Silicon (M1/M2/M3)** | Poor (emulation) | ⚠️ Use PostgreSQL target or cloud AMD64 VM |
| **Linux AMD64** | Excellent | ✅ Use SQL Server |
| **Linux ARM64** | Poor (emulation) | ⚠️ Use PostgreSQL target |

---

## Quick Diagnosis Commands

```bash
# Check container status
docker ps -a --filter "name=stackoverflow"

# Check container logs for errors
docker logs stackoverflow-mssql-target --tail 50

# Test SQL Server connectivity
docker exec stackoverflow-mssql-target /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "StackOverflow123!" -C -Q "SELECT @@VERSION;"

# Check Airflow connections
astro dev run connections list | grep stackoverflow

# Test specific task
astro dev run tasks test <dag_id> <task_id> 2025-11-10

# Monitor resource usage
docker stats --no-stream stackoverflow-mssql-source stackoverflow-mssql-target
```

---

## Related Documentation

- Main setup guide: [CLAUDE.md](../CLAUDE.md)
- Performance analysis: [optimization-summary.md](./optimization-summary.md)
- Parallel DAG benchmarks: [parallel-dag-performance-analysis.md](./parallel-dag-performance-analysis.md)
