# Streaming Performance Optimization Analysis

## Current Implementation Performance Profile

### Architecture
**Two-Phase Buffered Approach:**
```
Phase 1: Source → Fetch ALL rows → CSV Buffer (SpooledTemporaryFile)
Phase 2: CSV Buffer → Batch INSERT (1000 rows) → Target DB
         └─ Commit every 10,000 rows
```

### Measured Performance (Live Test)
- **Posts Table**: 3,729,195 rows
  - Buffer size: 3.19 GB
  - Disk spill: Yes (exceeded 128MB cap)
  - Estimated time: ~25-30 minutes

- **Users Table**: 299,398 rows
  - Buffer size: 41 MB
  - Disk spill: No (stayed in memory)
  - Completion time: 2.6 minutes (~115K rows/min)

### Bottlenecks Identified

1. **No Pipelining** (`replicate_stackoverflow_to_target.py:234-261`)
   - Source fetch must complete 100% before target writes begin
   - Network/CPU sits idle during phase transitions
   - Total time = Source Read Time + Target Write Time

2. **CSV Serialization Overhead** (`replicate_stackoverflow_to_target.py:238-249`)
   ```python
   # Type conversion happens twice:
   # 1. Source row → CSV string (line 240-249)
   # 2. CSV string → Target row (line 330-342)
   ```
   - Datetime formatting: `v.strftime('%Y-%m-%d %H:%M:%S')`
   - String conversion: `str(v)` for all values
   - CSV escaping for special characters

3. **Disk I/O on Large Tables** (`replicate_stackoverflow_to_target.py:224-226`)
   - SpooledTemporaryFile rolls to disk at 128MB
   - Posts (3.19GB), Comments (est. 3.5GB), Votes (est. 9GB) all trigger disk spill
   - Additional latency: write to disk → read from disk

4. **Sequential Table Processing** (`replicate_stackoverflow_to_target.py:456-464`)
   ```python
   prev = create_schema
   for tbl in TABLE_ORDER:
       t = PythonOperator(task_id=f"copy_{tbl}", ...)
       prev >> t  # Sequential dependency
       prev = t
   ```
   - Tables processed one at a time
   - No parallelism despite LocalExecutor(parallelism=16)

5. **Small Batch Size** (`replicate_stackoverflow_to_target.py:324`)
   - Batch size: 1,000 rows
   - Large tables require thousands of INSERT statements
   - Network round trips: ~3,729 for Posts table

---

## Optimization Approach 1: Streaming Pipeline (Producer-Consumer)

### Concept
Decouple source reads from target writes using a queue/buffer:

```
┌─────────────┐       ┌────────┐       ┌─────────────┐
│ Source Read │──────▶│ Queue  │──────▶│Target Write │
│  (Thread 1) │       │(Bounded)│      │  (Thread 2) │
└─────────────┘       └────────┘       └─────────────┘
     Cursor              10K rows          Batch INSERT
```

### Implementation Strategy

```python
from queue import Queue
from threading import Thread, Event
import logging

def copy_table_streaming_pipeline(table: str, queue_size: int = 10000) -> None:
    """
    Streaming pipeline with concurrent source read and target write.

    Performance Improvement:
    - Eliminates phase 1 vs phase 2 wait time
    - Reduces memory footprint (bounded queue vs full buffering)
    - Overlaps network I/O for source and target
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    # Bounded queue to prevent memory exhaustion
    row_queue = Queue(maxsize=queue_size)
    stop_event = Event()
    error_event = Event()

    def producer():
        """Read from source and enqueue rows"""
        try:
            with src_hook.get_conn() as src_conn:
                src_conn.autocommit(True
                with src_conn.cursor() as src_cur:
                    src_cur.execute(f"SELECT * FROM dbo.[{table}]")

                    batch = []
                    batch_size = 1000

                    for row in src_cur:
                        batch.append(row)

                        if len(batch) >= batch_size:
                            row_queue.put(batch)
                            batch = []

                    # Final batch
                    if batch:
                        row_queue.put(batch)

                    # Signal completion
                    row_queue.put(None)
        except Exception as e:
            logging.error(f"Producer error: {e}")
            error_event.set()
            row_queue.put(None)

    def consumer():
        """Dequeue rows and write to target"""
        try:
            with tgt_hook.get_conn() as tgt_conn:
                tgt_conn.autocommit(False
                with tgt_conn.cursor() as tgt_cur:
                    tgt_cur.execute("USE stackoverflow_target;")

                    # Enable IDENTITY_INSERT if needed
                    # ... (schema detection logic)

                    total_rows = 0

                    while not error_event.is_set():
                        batch = row_queue.get()

                        if batch is None:  # Sentinel value
                            break

                        # Batch INSERT
                        placeholders = ", ".join(
                            f"({', '.join(['%s'] * len(batch[0]))})"
                            for _ in batch
                        )
                        insert_sql = f"INSERT INTO dbo.[{table}] VALUES {placeholders}"
                        flat_values = [val for row in batch for val in row]
                        tgt_cur.execute(insert_sql, flat_values)

                        total_rows += len(batch)

                        # Commit every 10K rows
                        if total_rows % 10000 == 0:
                            tgt_conn.commit()
                            logging.info(f"[{table}] committed {total_rows} rows")

                    tgt_conn.commit()
                    logging.info(f"[{table}] completed: {total_rows} rows")
        except Exception as e:
            logging.error(f"Consumer error: {e}")
            error_event.set()

    # Start producer and consumer threads
    producer_thread = Thread(target=producer, name=f"{table}-producer")
    consumer_thread = Thread(target=consumer, name=f"{table}-consumer")

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

    if error_event.is_set():
        raise RuntimeError(f"Streaming pipeline failed for {table}")
```

### Performance Benefits
- **Pipelined Execution**: Source read and target write overlap
  - Current: `Total = Read + Write`
  - Pipeline: `Total ≈ max(Read, Write)` + startup overhead

- **Reduced Memory**: Bounded queue (10K rows) vs full buffering (3.7M rows)
  - Current: 3.19 GB for Posts
  - Pipeline: ~100 MB queue (assuming 10K rows × 10KB/row avg)

- **Expected Improvement**: 30-50% faster for large tables
  - Posts: 25 min → 12-17 min
  - Votes: 45 min → 22-30 min

### Trade-offs
- ✅ Lower memory footprint
- ✅ Faster for large tables (pipelining)
- ❌ More complex error handling (two threads)
- ❌ LocalExecutor may not handle threading well (use CeleryExecutor in production)

---

## Optimization Approach 2: Native Bulk Copy (BCP Protocol)

### Concept
Use SQL Server's native BCP (Bulk Copy Program) protocol for maximum throughput:

```
┌─────────────┐       ┌──────────────┐       ┌─────────────┐
│ Source DB   │──────▶│ BCP Export   │──────▶│ BCP Import  │──────▶│ Target DB   │
│ SELECT      │       │ (binary)     │       │ (binary)    │       │             │
└─────────────┘       └──────────────┘       └─────────────┘
                        Fast native          Fast native
                        serialization        deserialization
```

### Implementation Strategy

```python
import subprocess
import tempfile
import os

def copy_table_bcp(table: str) -> None:
    """
    Use SQL Server BCP utility for high-performance bulk copy.

    BCP Performance Characteristics:
    - Native binary format (no CSV overhead)
    - Optimized for SQL Server (Microsoft's official tool)
    - Minimal CPU usage (no Python serialization)
    - Batch sizes of 10,000+ rows automatically
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    # Get connection details
    src_conn = src_hook.get_connection(SRC_CONN_ID)
    tgt_conn = tgt_hook.get_connection(TGT_CONN_ID)

    # Create temp file for BCP data
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.dat') as tmp_file:
        tmp_path = tmp_file.name

    try:
        # Step 1: BCP OUT from source
        bcp_out_cmd = [
            'bcp',
            f'dbo.{table}',
            'out',
            tmp_path,
            '-n',  # Native format (binary)
            '-S', src_conn.host,
            '-U', src_conn.login,
            '-P', src_conn.password,
            '-d', src_conn.schema,
            '-b', '10000',  # Batch size
            '-a', '16384',  # Packet size (16KB)
        ]

        logging.info(f"[{table}] BCP export starting...")
        result = subprocess.run(bcp_out_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"BCP export failed: {result.stderr}")

        logging.info(f"[{table}] BCP export completed: {os.path.getsize(tmp_path)} bytes")

        # Step 2: BCP IN to target
        bcp_in_cmd = [
            'bcp',
            f'dbo.{table}',
            'in',
            tmp_path,
            '-n',  # Native format (binary)
            '-S', tgt_conn.host,
            '-U', tgt_conn.login,
            '-P', tgt_conn.password,
            '-d', 'stackoverflow_target',
            '-b', '10000',  # Batch size
            '-a', '16384',  # Packet size (16KB)
            '-h', 'TABLOCK',  # Table lock for faster insert
            '-E',  # Keep identity values
        ]

        logging.info(f"[{table}] BCP import starting...")
        result = subprocess.run(bcp_in_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"BCP import failed: {result.stderr}")

        logging.info(f"[{table}] BCP import completed")
    finally:
        # Clean up temp file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
```

### Performance Benefits
- **Native Binary Format**: No CSV serialization overhead
  - Current: row → CSV string → row (2 conversions)
  - BCP: row → binary → row (optimized by SQL Server)

- **Optimized Batch Processing**:
  - Current: Python manages batching (1000 rows)
  - BCP: SQL Server native batching (10,000+ rows)

- **Expected Improvement**: 50-70% faster than current approach
  - Posts: 25 min → 7-12 min
  - Votes: 45 min → 13-22 min

### Trade-offs
- ✅ Fastest option for SQL Server → SQL Server
- ✅ Minimal CPU/memory usage
- ✅ Production-grade (Microsoft's official tool)
- ❌ Requires BCP utility in Docker container
- ❌ Platform-specific (SQL Server only, not portable to PostgreSQL)
- ❌ Temp file I/O (network transfer would be better)

---

## Optimization Approach 3: Parallel Table Copying

### Concept
Copy independent tables concurrently using Airflow's task parallelism:

```
Current (Sequential):
VoteTypes → PostTypes → LinkTypes → Users → Badges → Posts → ...
   1s         1s          1s         2.5m     2.5m     25m

Optimized (Parallel):
┌─ VoteTypes (1s) ─┐
├─ PostTypes (1s) ─┤──▶ Users (2.5m) ──▶ ┌─ Badges (2.5m) ─┐
└─ LinkTypes (1s) ─┘                     ├─ Posts (25m)    ─┤──▶ Comments ──▶ Votes
                                         └─ PostLinks      ─┘
```

### Implementation Strategy

```python
# Current DAG structure (Sequential)
prev = create_schema
for tbl in TABLE_ORDER:
    t = PythonOperator(task_id=f"copy_{tbl}", ...)
    prev >> t  # Sequential
    prev = t

# Optimized DAG structure (Parallel with dependencies)
with DAG(...) as dag:
    reset_tgt = PythonOperator(task_id="reset_target_schema", ...)
    create_schema = PythonOperator(task_id="create_target_schema", ...)

    # Independent tables (no foreign keys) - run in parallel
    copy_votetypes = PythonOperator(task_id="copy_VoteTypes", ...)
    copy_posttypes = PythonOperator(task_id="copy_PostTypes", ...)
    copy_linktypes = PythonOperator(task_id="copy_LinkTypes", ...)

    # Parent table (Users) - depends on lookup tables
    copy_users = PythonOperator(task_id="copy_Users", ...)

    # Tables depending on Users - run in parallel
    copy_badges = PythonOperator(task_id="copy_Badges", ...)
    copy_posts = PythonOperator(task_id="copy_Posts", ...)

    # Tables depending on Posts - run in parallel
    copy_postlinks = PythonOperator(task_id="copy_PostLinks", ...)
    copy_comments = PythonOperator(task_id="copy_Comments", ...)
    copy_votes = PythonOperator(task_id="copy_Votes", ...)

    fix_sequences = PythonOperator(task_id="align_target_sequences", ...)

    # Dependency graph
    reset_tgt >> create_schema

    # Parallel: VoteTypes, PostTypes, LinkTypes
    create_schema >> [copy_votetypes, copy_posttypes, copy_linktypes]

    # Users depends on all lookup tables
    [copy_votetypes, copy_posttypes, copy_linktypes] >> copy_users

    # Parallel: Badges, Posts (both depend on Users)
    copy_users >> [copy_badges, copy_posts]

    # Parallel: PostLinks, Comments, Votes (all depend on Posts)
    copy_posts >> [copy_postlinks, copy_comments, copy_votes]

    # All tables must complete before fixing sequences
    [copy_postlinks, copy_comments, copy_votes, copy_badges] >> fix_sequences
```

### Dependency Analysis

```
Table Dependencies (Foreign Keys):
- VoteTypes: None (lookup table)
- PostTypes: None (lookup table)
- LinkTypes: None (lookup table)
- Users: None (parent table)
- Badges: Users
- Posts: Users, PostTypes
- PostLinks: Posts, LinkTypes
- Comments: Posts, Users
- Votes: Posts, Users, VoteTypes
```

### Performance Benefits
- **Parallel Execution**:
  - Level 1: VoteTypes, PostTypes, LinkTypes (3 parallel) → ~1s total
  - Level 2: Users (1 task) → 2.5m
  - Level 3: Badges, Posts (2 parallel) → max(2.5m, 25m) = 25m
  - Level 4: PostLinks, Comments, Votes (3 parallel) → max(1m, 15m, 40m) = 40m

- **Expected Total Time**:
  - Current: 1s + 1s + 1s + 2.5m + 2.5m + 25m + 1m + 15m + 40m ≈ **86 minutes**
  - Parallel: 1s + 2.5m + 25m + 40m ≈ **67.5 minutes** (22% faster)

- **Resource Utilization**:
  - Current: 1/16 cores used (6.25%)
  - Parallel: 3/16 cores peak (18.75%), better CPU utilization

### Trade-offs
- ✅ Simple to implement (Airflow native)
- ✅ No code changes to copy logic
- ✅ Better resource utilization
- ❌ Limited improvement (22%) - bottleneck is large table I/O
- ❌ Increased load on source/target databases (3 concurrent queries)

---

## Optimization Approach 4: Chunk-Based Streaming

### Concept
Process table in chunks to reduce memory footprint and enable progress tracking:

```
┌──────────────────────────────────────┐
│ Table: Posts (3.7M rows)             │
├──────────────────────────────────────┤
│ Chunk 1: Rows 1-100K    → Write     │
│ Chunk 2: Rows 100K-200K → Write     │
│ Chunk 3: Rows 200K-300K → Write     │
│ ...                                  │
│ Chunk 37: Rows 3.6M-3.7M → Write    │
└──────────────────────────────────────┘
```

### Implementation Strategy

```python
def copy_table_chunked(table: str, chunk_size: int = 100000) -> None:
    """
    Copy table in chunks to reduce memory usage and enable progress tracking.

    Performance Characteristics:
    - Memory footprint: O(chunk_size) instead of O(table_size)
    - Progress tracking: Log after each chunk
    - Early failure detection: Don't wait for full table read
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    # Get primary key for chunking
    with src_hook.get_conn() as src_conn:
        src_conn.autocommit(True)
        with src_conn.cursor() as src_cur:
            src_cur.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = %s AND CONSTRAINT_NAME LIKE 'PK_%%'
                ORDER BY ORDINAL_POSITION
            """, (table,))
            pk_cols = [row[0] for row in src_cur.fetchall()]

    if not pk_cols:
        logging.warning(f"No primary key found for {table}, using OFFSET/FETCH")
        use_offset = True
        pk_col = None
    else:
        use_offset = False
        pk_col = pk_cols[0]  # Use first PK column for chunking

    # Get total row count
    with src_hook.get_conn() as src_conn:
        src_conn.autocommit(True)
        with src_conn.cursor() as src_cur:
            src_cur.execute(f"SELECT COUNT(*) FROM dbo.[{table}]")
            total_rows = src_cur.fetchone()[0]

    logging.info(f"[{table}] starting chunked copy ({total_rows} rows, chunk_size={chunk_size})")

    # Process chunks
    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit(False
    tgt_cur = tgt_conn.cursor()
    tgt_cur.execute("USE stackoverflow_target;")

    # Enable IDENTITY_INSERT if needed
    # ... (schema detection logic)

    offset = 0
    total_copied = 0

    try:
        while offset < total_rows:
            # Read chunk from source
            with src_hook.get_conn() as src_conn:
                src_conn.autocommit(True)
                with src_conn.cursor() as src_cur:
                    if use_offset:
                        # Use OFFSET/FETCH for tables without PK
                        src_cur.execute(f"""
                            SELECT * FROM dbo.[{table}]
                            ORDER BY (SELECT NULL)
                            OFFSET {offset} ROWS
                            FETCH NEXT {chunk_size} ROWS ONLY
                        """)
                    else:
                        # Use PK range for better performance
                        if offset == 0:
                            src_cur.execute(f"""
                                SELECT * FROM dbo.[{table}]
                                ORDER BY [{pk_col}]
                                OFFSET 0 ROWS
                                FETCH NEXT {chunk_size} ROWS ONLY
                            """)
                        else:
                            src_cur.execute(f"""
                                SELECT * FROM dbo.[{table}]
                                WHERE [{pk_col}] > {last_pk_value}
                                ORDER BY [{pk_col}]
                                FETCH NEXT {chunk_size} ROWS ONLY
                            """)

                    rows = src_cur.fetchall()

                    if not rows:
                        break

                    # Remember last PK value for next chunk
                    if not use_offset and pk_col:
                        pk_idx = 0  # Assume PK is first column
                        last_pk_value = rows[-1][pk_idx]

            # Write chunk to target
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]

                placeholders = ", ".join(
                    f"({', '.join(['%s'] * len(batch[0]))})"
                    for _ in batch
                )
                insert_sql = f"INSERT INTO dbo.[{table}] VALUES {placeholders}"
                flat_values = [val for row in batch for val in row]
                tgt_cur.execute(insert_sql, flat_values)

            total_copied += len(rows)
            tgt_conn.commit()

            logging.info(f"[{table}] copied chunk: {total_copied}/{total_rows} rows ({100*total_copied/total_rows:.1f}%)")

            offset += chunk_size

        logging.info(f"[{table}] completed: {total_copied} rows")
    finally:
        tgt_cur.close()
        tgt_conn.close()
```

### Performance Benefits
- **Memory Efficiency**:
  - Current: 3.19 GB for Posts (full table in memory/disk)
  - Chunked: ~100 MB per chunk (100K rows × 1KB/row avg)

- **Progress Visibility**:
  - Current: No progress until completion
  - Chunked: Log every 100K rows (37 checkpoints for Posts)

- **Failure Recovery**:
  - Current: Fail at end, lose all work
  - Chunked: Fail at chunk N, only lose N-th chunk

- **Expected Performance**: Similar to current (no pipelining)
  - Slightly slower due to OFFSET/FETCH overhead
  - Better for monitoring and debugging

### Trade-offs
- ✅ Low memory footprint
- ✅ Progress tracking
- ✅ Early failure detection
- ❌ No performance improvement (still sequential)
- ❌ OFFSET/FETCH gets slower for later chunks (O(n) scan)

---

## Optimization Approach 5: Direct Row Streaming (No CSV)

### Concept
Eliminate CSV intermediate format and stream rows directly:

```
Current:
Row → CSV String → SpooledFile → CSV String → Row
     (serialize)   (disk I/O)    (deserialize)

Optimized:
Row → Queue → Row
     (in-memory, no serialization)
```

### Implementation Strategy

```python
def copy_table_direct_stream(table: str, batch_size: int = 5000) -> None:
    """
    Stream rows directly from source to target without CSV intermediate.

    Eliminates:
    - CSV serialization overhead
    - Disk I/O for SpooledTemporaryFile
    - CSV deserialization overhead
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    logging.info(f"[{table}] starting direct stream (batch_size={batch_size})")

    # Open target connection
    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit(False
    tgt_cur = tgt_conn.cursor()
    tgt_cur.execute("USE stackoverflow_target;")

    # Get column info
    # ... (schema detection logic)

    # Enable IDENTITY_INSERT if needed
    # ... (identity column logic)

    try:
        # Open source connection and stream
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit(True)
            with src_conn.cursor() as src_cur:
                src_cur.execute(f"SELECT * FROM dbo.[{table}]")

                batch = []
                total_rows = 0

                # Stream rows directly without intermediate buffering
                for row in src_cur:
                    batch.append(row)

                    if len(batch) >= batch_size:
                        # Insert batch
                        placeholders = ", ".join(
                            f"({', '.join(['%s'] * len(batch[0]))})"
                            for _ in batch
                        )
                        insert_sql = f"INSERT INTO dbo.[{table}] VALUES {placeholders}"
                        flat_values = [val for row in batch for val in row]
                        tgt_cur.execute(insert_sql, flat_values)

                        total_rows += len(batch)
                        batch = []

                        # Commit every 10K rows
                        if total_rows % 10000 == 0:
                            tgt_conn.commit()
                            logging.info(f"[{table}] committed {total_rows} rows")

                # Insert remaining batch
                if batch:
                    placeholders = ", ".join(
                        f"({', '.join(['%s'] * len(batch[0]))})"
                        for _ in batch
                    )
                    insert_sql = f"INSERT INTO dbo.[{table}] VALUES {placeholders}"
                    flat_values = [val for row in batch for val in row]
                    tgt_cur.execute(insert_sql, flat_values)
                    total_rows += len(batch)

                tgt_conn.commit()
                logging.info(f"[{table}] completed: {total_rows} rows")
    finally:
        tgt_cur.close()
        tgt_conn.close()
```

### Performance Benefits
- **Eliminates CSV Overhead**:
  - No `str(v)` conversion for every value
  - No `strftime()` for datetime values
  - No CSV escaping for special characters

- **No Disk I/O**:
  - Current: Write 3.19GB to disk, then read 3.19GB
  - Direct: Stream through memory (0 disk I/O)

- **Larger Batch Size**:
  - Current: 1,000 rows/batch
  - Direct: 5,000 rows/batch (fewer INSERT statements)

- **Expected Improvement**: 20-30% faster
  - Posts: 25 min → 17-20 min
  - Votes: 45 min → 31-36 min

### Trade-offs
- ✅ Simpler code (no CSV handling)
- ✅ Faster (no serialization overhead)
- ✅ Less disk I/O
- ❌ Still sequential (no pipelining)
- ❌ Higher memory usage if source is slow

---

## Performance Comparison Matrix

| Approach | Speed | Memory | Complexity | Portability | Recommendation |
|----------|-------|--------|------------|-------------|----------------|
| **Current (CSV Buffering)** | Baseline (100%) | High (3.19GB) | Low | High (any DB) | ❌ Slow for large tables |
| **Streaming Pipeline** | 130-150% | Low (100MB) | Medium | High | ✅ **Best for production** |
| **Native BCP** | 150-200% | Very Low | Medium | Low (SQL Server only) | ✅ Best for SQL Server |
| **Parallel Tables** | 120% | High | Low | High | ✅ Easy quick win |
| **Chunked Streaming** | 95-100% | Very Low | Medium | High | ✅ Best for monitoring |
| **Direct Streaming** | 120-130% | Medium | Low | High | ✅ Simple improvement |

---

## Recommended Implementation Plan

### Phase 1: Quick Wins (Immediate)
1. **Parallel Table Copying** - 1-2 hours implementation
   - Modify DAG dependencies to allow concurrent table copies
   - No changes to copy logic
   - Expected: 22% faster

2. **Direct Row Streaming** - 2-3 hours implementation
   - Remove CSV intermediate
   - Increase batch size to 5,000
   - Expected: additional 20-30% faster

### Phase 2: Production Optimization (1-2 days)
3. **Streaming Pipeline** - 1 day implementation
   - Implement producer-consumer pattern
   - Bounded queue for memory control
   - Expected: 30-50% faster than Phase 1

4. **Testing and Benchmarking**
   - Compare performance across approaches
   - Measure memory usage
   - Validate data integrity

### Phase 3: Platform-Specific (Optional)
5. **Native BCP** for SQL Server environments
   - Fastest option for SQL Server → SQL Server
   - Add as alternative copy strategy

---

## Benchmarking Methodology

### Test Scenario
- **Source**: StackOverflow2010 database
- **Target**: Empty SQL Server database
- **Tables**: All 9 tables (focus on Posts, Comments, Votes)
- **Hardware**: Current macOS setup

### Metrics to Collect
1. **Total Runtime**: Start to finish (all tables)
2. **Per-Table Runtime**: Individual table copy times
3. **Memory Usage**: Peak RSS during copy
4. **CPU Usage**: Average CPU % during copy
5. **Disk I/O**: Read/write bytes (for disk spill)
6. **Network I/O**: Bytes sent/received

### Measurement Code
```python
import time
import psutil
import logging

def benchmark_copy(copy_function, table: str):
    """Benchmark a table copy function"""
    process = psutil.Process()

    # Initial measurements
    start_time = time.time()
    start_mem = process.memory_info().rss
    start_cpu = process.cpu_percent()
    start_io = process.io_counters()

    # Run copy
    copy_function(table)

    # Final measurements
    end_time = time.time()
    end_mem = process.memory_info().rss
    end_cpu = process.cpu_percent()
    end_io = process.io_counters()

    # Calculate metrics
    runtime = end_time - start_time
    peak_mem = end_mem - start_mem
    avg_cpu = (start_cpu + end_cpu) / 2
    disk_read = end_io.read_bytes - start_io.read_bytes
    disk_write = end_io.write_bytes - start_io.write_bytes

    logging.info(f"""
    Benchmark Results for {table}:
    - Runtime: {runtime:.2f}s
    - Peak Memory: {peak_mem / 1024**2:.2f} MB
    - Avg CPU: {avg_cpu:.1f}%
    - Disk Read: {disk_read / 1024**2:.2f} MB
    - Disk Write: {disk_write / 1024**2:.2f} MB
    """)

    return {
        'table': table,
        'runtime': runtime,
        'peak_mem_mb': peak_mem / 1024**2,
        'avg_cpu_pct': avg_cpu,
        'disk_read_mb': disk_read / 1024**2,
        'disk_write_mb': disk_write / 1024**2,
    }
```

---

## Conclusion

**Top 3 Recommendations:**

1. **Immediate**: Implement parallel table copying + direct row streaming
   - Expected improvement: 40-50% faster
   - Low complexity, high impact
   - No architectural changes

2. **Production**: Implement streaming pipeline (producer-consumer)
   - Expected improvement: 60-80% faster
   - Better memory management
   - Scalable to very large tables

3. **SQL Server Optimized**: Add BCP as alternative strategy
   - Expected improvement: 100-150% faster
   - Best performance for SQL Server → SQL Server
   - Platform-specific but production-grade

**Expected Total Improvement:**
- Current: ~86 minutes for full dataset
- After Phase 1: ~43 minutes (2x faster)
- After Phase 2: ~25 minutes (3.4x faster)
- With BCP: ~15 minutes (5.7x faster)
