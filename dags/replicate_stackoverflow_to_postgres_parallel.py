from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

SRC_CONN_ID = "stackoverflow_source"
TGT_CONN_ID = "stackoverflow_postgres_target"

# Memory buffer - 256MB is sufficient since large tables are partitioned
SPOOLED_MAX_MEMORY_BYTES = 256 * 1024 * 1024  # spill to disk above ~256 MB

log = logging.getLogger(__name__)

# Tables to copy without partitioning
REGULAR_TABLES = [
    "VoteTypes",      # Small lookup table
    "PostTypes",      # Small lookup table
    "LinkTypes",      # Small lookup table
    "Users",          # 2.5M rows (2013)
    "Badges",         # 8M rows (2013)
    "PostLinks",      # ~546K rows (2013)
]

# Large tables to partition for parallel loading
PARTITIONED_TABLES = {
    "Posts": 4,       # 17M rows (2013) - split into 4 parallel chunks
    "Comments": 4,    # 24.5M rows (2013) - split into 4 parallel chunks
    "Votes": 4,       # 52.9M rows (2013) - split into 4 parallel chunks
}

# All tables (for schema creation)
ALL_TABLES = REGULAR_TABLES + list(PARTITIONED_TABLES.keys())

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def ensure_target_database() -> None:
    """Ensure stackoverflow_target database exists (creates if needed, does not drop)."""
    hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)

    conn = hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Check if database exists
    cur.execute("""
        SELECT 1 FROM pg_database WHERE datname = 'stackoverflow_target'
    """)

    if cur.fetchone() is None:
        cur.execute("CREATE DATABASE stackoverflow_target")
        log.info("Created stackoverflow_target database")
    else:
        log.info("stackoverflow_target database already exists")

    cur.close()
    conn.close()


def map_sqlserver_to_postgres_type(data_type: str, char_len: int | None, num_prec: int | None, num_scale: int | None) -> str:
    """
    Map SQL Server data types to PostgreSQL data types.

    Args:
        data_type: SQL Server data type name
        char_len: Character length for string types (-1 for MAX)
        num_prec: Numeric precision
        num_scale: Numeric scale

    Returns:
        PostgreSQL data type string
    """
    data_type_lower = data_type.lower()

    # String types
    if data_type_lower in ('varchar', 'nvarchar', 'char', 'nchar'):
        if char_len == -1:
            return "TEXT"
        else:
            return f"VARCHAR({char_len})"

    # Numeric types
    elif data_type_lower in ('decimal', 'numeric'):
        return f"NUMERIC({num_prec},{num_scale or 0})"
    elif data_type_lower in ('int', 'integer'):
        return "INTEGER"
    elif data_type_lower == 'bigint':
        return "BIGINT"
    elif data_type_lower == 'smallint':
        return "SMALLINT"
    elif data_type_lower == 'tinyint':
        return "SMALLINT"  # PostgreSQL doesn't have TINYINT, use SMALLINT
    elif data_type_lower == 'bit':
        return "BOOLEAN"
    elif data_type_lower == 'float':
        return "DOUBLE PRECISION"
    elif data_type_lower == 'real':
        return "REAL"

    # Date/Time types
    elif data_type_lower in ('datetime', 'datetime2', 'smalldatetime'):
        return "TIMESTAMP"
    elif data_type_lower == 'date':
        return "DATE"
    elif data_type_lower == 'time':
        return "TIME"

    # Binary types
    elif data_type_lower in ('varbinary', 'binary'):
        if char_len == -1:
            return "BYTEA"
        else:
            return "BYTEA"

    # Text types
    elif data_type_lower in ('text', 'ntext'):
        return "TEXT"

    # XML
    elif data_type_lower == 'xml':
        return "XML"

    # UUID/uniqueidentifier
    elif data_type_lower == 'uniqueidentifier':
        return "UUID"

    # Default fallback
    else:
        log.warning(f"Unknown SQL Server type: {data_type}, using TEXT as fallback")
        return "TEXT"


def create_or_truncate_tables() -> None:
    """
    Create tables if they don't exist, or truncate if they do.
    Converts SQL Server data types to PostgreSQL equivalents.
    Creates new tables as UNLOGGED for faster bulk loading.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    src_conn = src_hook.get_conn()
    src_conn.autocommit(True)
    src_cur = src_conn.cursor()

    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit = True
    tgt_cur = tgt_conn.cursor()

    # Optimize PostgreSQL for bulk loading
    tgt_cur.execute("SET maintenance_work_mem = '256MB'")
    log.info("Configured PostgreSQL for bulk loading (maintenance_work_mem=256MB)")

    for table in ALL_TABLES:
        # Check if table exists
        tgt_cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        """, (table,))
        table_exists = tgt_cur.fetchone() is not None

        if table_exists:
            # Truncate first (fast on any table)
            tgt_cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')

            # Then set UNLOGGED (fast on empty table, slow on populated table)
            tgt_cur.execute("""
                SELECT relpersistence FROM pg_class
                WHERE relname = %s AND relkind = 'r'
            """, (table,))
            persistence = tgt_cur.fetchone()[0]

            if persistence != 'u':  # 'u' = unlogged, 'p' = permanent (logged)
                tgt_cur.execute(f'ALTER TABLE "{table}" SET UNLOGGED')
                log.info(f"Truncated {table} and set to UNLOGGED")
            else:
                log.info(f"Truncated {table} (already UNLOGGED)")
        else:
            # Table doesn't exist - create it
            log.info(f"Creating table schema for {table}")

            # Get column definitions from source
            src_cur.execute("""
                SELECT
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.NUMERIC_PRECISION,
                    c.NUMERIC_SCALE,
                    c.IS_NULLABLE,
                    COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = %s
                ORDER BY c.ORDINAL_POSITION
            """, (table,))

            columns = src_cur.fetchall()

            if not columns:
                log.warning(f"Table {table} not found in source database, skipping")
                continue

            # Build CREATE TABLE statement
            col_defs = []
            for col in columns:
                col_name, data_type, char_len, num_prec, num_scale, is_nullable, is_identity = col

                # Map SQL Server type to PostgreSQL type
                pg_type = map_sqlserver_to_postgres_type(data_type, char_len, num_prec, num_scale)

                # Build column definition
                col_def = f'"{col_name}" {pg_type}'

                # Add GENERATED AS IDENTITY if source column is identity
                if is_identity:
                    col_def += " GENERATED ALWAYS AS IDENTITY"

                # Add NULL/NOT NULL
                # Special case: Allow NULL for text columns even if source says NOT NULL
                # to handle empty strings that get converted to NULL during CSV export
                if is_nullable == 'NO' and pg_type not in ('TEXT', 'VARCHAR') and not pg_type.startswith('VARCHAR('):
                    col_def += " NOT NULL"
                else:
                    col_def += " NULL"

                col_defs.append(col_def)

            # Get primary key
            src_cur.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = %s
                  AND CONSTRAINT_NAME LIKE 'PK_%'
                ORDER BY ORDINAL_POSITION
            """, (table,))

            pk_cols = [row[0] for row in src_cur.fetchall()]

            if pk_cols:
                pk_def = f"PRIMARY KEY ({', '.join(f'"{col}"' for col in pk_cols)})"
                col_defs.append(pk_def)

            # Create UNLOGGED table for faster bulk loading (skips WAL)
            create_sql = f'CREATE UNLOGGED TABLE "{table}" (\n  {",\n  ".join(col_defs)}\n);'
            tgt_cur.execute(create_sql)
            log.info(f"Created UNLOGGED table {table} (no WAL overhead)")

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Table setup completed (created missing tables, truncated existing)")


def copy_table_src_to_tgt(table: str) -> None:
    """
    Copy table from SQL Server source to PostgreSQL target.

    Strategy: Read from source via SELECT, write to target via CSV bulk insert.
    Uses memory-capped streaming with SpooledTemporaryFile (256MB buffer).
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    log.info(
        "[%s] starting buffered copy (memory cap≈%.1f MB) - FULL PARALLEL MODE",
        table,
        SPOOLED_MAX_MEMORY_BYTES / (1024 * 1024),
    )

    # Get target table columns
    with tgt_hook.get_conn() as tgt_conn_tmp:
        tgt_conn_tmp.autocommit = True
        with tgt_conn_tmp.cursor() as tgt_cur_tmp:
            tgt_cur_tmp.execute(
                """
                SELECT c.column_name, c.is_nullable, ident.is_identity
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT column_name, 'YES' as is_identity
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_default LIKE 'nextval%%'
                ) ident ON c.column_name = ident.column_name
                WHERE c.table_schema = 'public'
                  AND c.table_name = %s
                ORDER BY c.ordinal_position
                """,
                (table, table),
            )
            column_info = tgt_cur_tmp.fetchall()
            target_columns = [row[0] for row in column_info]
            nullable_columns = {row[0] for row in column_info if row[1] == 'YES'}
            identity_columns = {row[0] for row in column_info if row[2] == 'YES'}

    if not target_columns:
        raise RuntimeError(f"Target table {table} has no columns")

    column_list_for_select = ", ".join(f"[{col}]" for col in target_columns)

    # Use text mode for CSV writing
    with SpooledTemporaryFile(
        max_size=SPOOLED_MAX_MEMORY_BYTES, mode="w+", encoding='utf-8', newline=''
    ) as spool:
        csv_writer = csv.writer(spool)

        # Read from source SQL Server
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit(True)
            with src_conn.cursor() as src_cur:
                # Select all rows from source table
                src_cur.execute(f"SELECT {column_list_for_select} FROM dbo.[{table}]")

                # Write rows to CSV in memory/disk
                row_count_src = 0
                for row in src_cur:
                    # Convert values to CSV-safe format
                    csv_row = []
                    for v in row:
                        if v is None:
                            csv_row.append('')
                        elif isinstance(v, datetime):
                            # Format datetime as ISO string without microseconds
                            csv_row.append(v.strftime('%Y-%m-%d %H:%M:%S'))
                        elif isinstance(v, bool):
                            # PostgreSQL expects 't'/'f' or 'true'/'false' for boolean
                            csv_row.append('t' if v else 'f')
                        else:
                            csv_row.append(str(v))
                    csv_writer.writerow(csv_row)
                    row_count_src += 1

        spool.flush()
        written_bytes = spool.tell()
        spilled_to_disk = bool(getattr(spool, "_rolled", False))
        log.info(
            "[%s] buffered %s bytes, %s rows from source (rolled_to_disk=%s) - FULL PARALLEL",
            table,
            written_bytes,
            row_count_src,
            spilled_to_disk,
        )

        spool.seek(0)

        # Write to PostgreSQL target using COPY command for better performance
        tgt_conn = tgt_hook.get_conn()
        tgt_conn.autocommit = False
        tgt_cur = tgt_conn.cursor()

        try:
            # Verify target table exists
            tgt_cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            if tgt_cur.fetchone() is None:
                raise RuntimeError(
                    f"Target table {table} does not exist; did schema creation run?"
                )

            # Table was already truncated in create_or_truncate_tables

            # Disable triggers and autovacuum for this table
            tgt_cur.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER ALL')
            tgt_cur.execute(f'ALTER TABLE "{table}" SET (autovacuum_enabled = false)')

            # Build column list (excluding identity columns for COPY)
            non_identity_columns = [col for col in target_columns if col not in identity_columns]

            # If table has identity columns, we need to override them
            if identity_columns:
                column_list = ", ".join(f'"{col}"' for col in target_columns)
                copy_sql = f'COPY "{table}" ({column_list}) FROM STDIN WITH (FORMAT CSV, NULL \'\')'

                # Temporarily allow manual values by dropping IDENTITY
                for id_col in identity_columns:
                    tgt_cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "{id_col}" DROP IDENTITY IF EXISTS')
            else:
                column_list = ", ".join(f'"{col}"' for col in target_columns)
                copy_sql = f'COPY "{table}" ({column_list}) FROM STDIN WITH (FORMAT CSV, NULL \'\')'

            # Use PostgreSQL's COPY command for bulk loading
            tgt_cur.copy_expert(copy_sql, spool)

            # Re-add identity if we removed it
            if identity_columns:
                for id_col in identity_columns:
                    tgt_cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "{id_col}" ADD GENERATED ALWAYS AS IDENTITY')

            # Re-enable triggers and autovacuum
            tgt_cur.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER ALL')
            tgt_cur.execute(f'ALTER TABLE "{table}" SET (autovacuum_enabled = true)')

            # Get row count
            tgt_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            row_count = tgt_cur.fetchone()[0]

            tgt_conn.commit()
            log.info("[%s] copy completed (bytes=%s, rows=%s) - FULL PARALLEL", table, written_bytes, row_count)
        except Exception as e:
            tgt_conn.rollback()
            log.error(f"[{table}] copy failed: {e}")
            raise
        finally:
            tgt_cur.close()
            tgt_conn.close()


def get_source_row_count(table: str) -> int:
    """Get current row count from source table."""
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    with src_hook.get_conn() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM dbo.[{table}]")
            return cur.fetchone()[0]


def get_partition_ranges(table: str, num_partitions: int) -> list[tuple[int, int]]:
    """
    Get ID ranges for partitioning a table by row count (not ID range).
    Uses NTILE to divide actual rows evenly.
    Returns list of (min_id, max_id) tuples for each partition.

    Caches ranges in Airflow Variables to avoid expensive NTILE queries
    on subsequent runs when data hasn't changed significantly.
    """
    cache_key = f"partition_ranges_{table}"

    # Check cache first
    try:
        cached_json = Variable.get(cache_key, default_var=None)
        if cached_json:
            cached = json.loads(cached_json)
            if cached.get("num_partitions") == num_partitions:
                # Quick row count check (much faster than NTILE)
                current_count = get_source_row_count(table)
                cached_count = cached.get("row_count", 0)
                # Allow 1% variance before recalculating
                if abs(current_count - cached_count) < max(1000, cached_count * 0.01):
                    log.info(f"[{table}] Using cached partition ranges (row_count: {cached_count}, current: {current_count})")
                    ranges = [tuple(r) for r in cached["ranges"]]
                    for i, (min_id, max_id) in enumerate(ranges):
                        log.info(f"[{table}] Partition {i}: Id [{min_id}, {max_id}] (cached)")
                    return ranges
                else:
                    log.info(f"[{table}] Row count changed ({cached_count} -> {current_count}), recalculating partitions")
    except Exception as e:
        log.warning(f"[{table}] Cache read failed: {e}, calculating fresh partitions")

    # Calculate fresh ranges with NTILE
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)

    with src_hook.get_conn() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            # Use NTILE to divide rows evenly and get boundary IDs
            cur.execute(f"""
                WITH Partitioned AS (
                    SELECT Id, NTILE({num_partitions}) OVER (ORDER BY Id) as partition_num
                    FROM dbo.[{table}]
                )
                SELECT partition_num, MIN(Id) as min_id, MAX(Id) as max_id, COUNT(*) as row_count
                FROM Partitioned
                GROUP BY partition_num
                ORDER BY partition_num
            """)

            ranges = []
            total_rows = 0
            for row in cur.fetchall():
                partition_num, min_id, max_id, row_count = row
                ranges.append((min_id, max_id))
                total_rows += row_count
                log.info(f"[{table}] Partition {partition_num-1}: Id [{min_id}, {max_id}] = {row_count} rows")

    log.info(f"[{table}] Partitioned into {num_partitions} ranges by row count")

    # Cache for next run
    try:
        cache_data = {
            "ranges": ranges,
            "row_count": total_rows,
            "num_partitions": num_partitions,
        }
        Variable.set(cache_key, json.dumps(cache_data))
        log.info(f"[{table}] Cached partition ranges for future runs")
    except Exception as e:
        log.warning(f"[{table}] Failed to cache partition ranges: {e}")

    return ranges


def copy_table_partition(table: str, partition_num: int, min_id: int | str, max_id: int | str) -> dict:
    """
    Copy a partition of a table from SQL Server to PostgreSQL.
    Uses WHERE Id BETWEEN min_id AND max_id for the partition.
    Returns stats dict with row count and duration.
    """
    import time
    start_time = time.time()

    # Convert string IDs to int (from Jinja templating)
    min_id = int(min_id)
    max_id = int(max_id)

    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    log.info(
        "[%s] partition %d: copying Id range [%d, %d]",
        table, partition_num, min_id, max_id,
    )

    # Get target table columns
    with tgt_hook.get_conn() as tgt_conn_tmp:
        tgt_conn_tmp.autocommit = True
        with tgt_conn_tmp.cursor() as tgt_cur_tmp:
            tgt_cur_tmp.execute(
                """
                SELECT c.column_name, c.is_nullable, ident.is_identity
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT column_name, 'YES' as is_identity
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_default LIKE 'nextval%%'
                ) ident ON c.column_name = ident.column_name
                WHERE c.table_schema = 'public'
                  AND c.table_name = %s
                ORDER BY c.ordinal_position
                """,
                (table, table),
            )
            column_info = tgt_cur_tmp.fetchall()
            target_columns = [row[0] for row in column_info]
            identity_columns = {row[0] for row in column_info if row[2] == 'YES'}

    column_list_for_select = ", ".join(f"[{col}]" for col in target_columns)

    # Use text mode for CSV writing
    with SpooledTemporaryFile(
        max_size=SPOOLED_MAX_MEMORY_BYTES, mode="w+", encoding='utf-8', newline=''
    ) as spool:
        csv_writer = csv.writer(spool)

        # Read partition from source SQL Server
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit(True)
            with src_conn.cursor() as src_cur:
                # Select partition by ID range
                src_cur.execute(
                    f"SELECT {column_list_for_select} FROM dbo.[{table}] WHERE Id BETWEEN %s AND %s",
                    (min_id, max_id)
                )

                row_count_src = 0
                for row in src_cur:
                    csv_row = []
                    for v in row:
                        if v is None:
                            csv_row.append('')
                        elif isinstance(v, datetime):
                            csv_row.append(v.strftime('%Y-%m-%d %H:%M:%S'))
                        elif isinstance(v, bool):
                            csv_row.append('t' if v else 'f')
                        else:
                            csv_row.append(str(v))
                    csv_writer.writerow(csv_row)
                    row_count_src += 1

        spool.flush()
        written_bytes = spool.tell()
        spool.seek(0)

        # Write to PostgreSQL target using COPY command
        tgt_conn = tgt_hook.get_conn()
        tgt_conn.autocommit = False
        tgt_cur = tgt_conn.cursor()

        try:
            column_list = ", ".join(f'"{col}"' for col in target_columns)
            copy_sql = f'COPY "{table}" ({column_list}) FROM STDIN WITH (FORMAT CSV, NULL \'\')'
            tgt_cur.copy_expert(copy_sql, spool)
            tgt_conn.commit()

            duration = time.time() - start_time
            log.info(
                "[%s] partition %d completed: %d rows, %.1f MB, %.1f sec",
                table, partition_num, row_count_src, written_bytes / (1024*1024), duration
            )
            return {"rows": row_count_src, "bytes": written_bytes, "duration": duration}
        except Exception as e:
            tgt_conn.rollback()
            log.error(f"[{table}] partition {partition_num} failed: {e}")
            raise
        finally:
            tgt_cur.close()
            tgt_conn.close()


def prepare_partitioned_table(table: str) -> None:
    """
    Prepare a partitioned table for loading by dropping identity constraint.
    Must be called before partition tasks run.
    Table was already truncated in create_or_truncate_tables.
    """
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    with tgt_hook.get_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Table was already truncated in create_or_truncate_tables

            # Disable triggers and autovacuum
            cur.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER ALL')
            cur.execute(f'ALTER TABLE "{table}" SET (autovacuum_enabled = false)')

            # Drop identity to allow manual ID insertion
            cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "Id" DROP IDENTITY IF EXISTS')

            log.info(f"[{table}] Prepared for partitioned loading")


def copy_table_partition_from_xcom(table: str, partition_num: int, ranges) -> dict:
    """Wrapper to extract range from XCom and call copy_table_partition."""
    import json

    # Handle string (from Jinja templating without render_template_as_native_obj)
    if isinstance(ranges, str):
        ranges = json.loads(ranges)

    range_item = ranges[partition_num]
    # Handle both tuple and XCom serialization formats
    if isinstance(range_item, dict) and "__data__" in range_item:
        min_id, max_id = range_item["__data__"]
    elif isinstance(range_item, (list, tuple)):
        min_id, max_id = range_item
    else:
        raise ValueError(f"Unexpected range format: {range_item}")
    return copy_table_partition(table, partition_num, min_id, max_id)


def finalize_partitioned_table(table: str) -> None:
    """
    Finalize a partitioned table after all partitions loaded.
    Re-adds identity constraint and re-enables triggers.
    """
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    with tgt_hook.get_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Re-add identity
            cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "Id" ADD GENERATED ALWAYS AS IDENTITY')

            # Re-enable triggers and autovacuum
            cur.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER ALL')
            cur.execute(f'ALTER TABLE "{table}" SET (autovacuum_enabled = true)')

            # Get row count
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            row_count = cur.fetchone()[0]

            log.info(f"[{table}] Finalized partitioned load: {row_count} total rows")


def convert_tables_to_logged() -> None:
    """Convert all UNLOGGED tables to regular logged tables for durability."""
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')
    conn = tgt_hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    log.info("Converting UNLOGGED tables to regular logged tables")

    for table in ALL_TABLES:
        try:
            cur.execute(f'ALTER TABLE "{table}" SET LOGGED')
            log.info(f"Converted {table} to LOGGED table")
        except Exception as e:
            log.warning(f"Failed to convert {table} to LOGGED: {e}")

    cur.close()
    conn.close()
    log.info("Table conversion completed")


def set_identity_sequences() -> None:
    """Reset identity sequences on PostgreSQL target for Stack Overflow tables."""
    tgt = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')
    conn = tgt.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Stack Overflow identity columns (table, primary_key_column)
    identity_tables = [
        ("Users", "Id"),
        ("Posts", "Id"),
        ("Comments", "Id"),
        ("Votes", "Id"),
        ("Badges", "Id"),
        ("PostLinks", "Id"),
        ("VoteTypes", "Id"),
        ("PostTypes", "Id"),
        ("LinkTypes", "Id"),
    ]

    for table, pk in identity_tables:
        # Check if table exists (use exact case match)
        cur.execute(
            'SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s',
            ('public', table)
        )
        if cur.fetchone() is None:
            log.info(f"Table {table} not found, skipping sequence alignment")
            continue

        # Get max ID (use quoted identifiers for case-sensitive table/column names)
        cur.execute(f'SELECT COALESCE(MAX("{pk}"), 0) FROM "{table}"')
        (max_id,) = cur.fetchone()

        if max_id > 0:
            # Get the sequence name for this identity column
            cur.execute("""
                SELECT pg_get_serial_sequence(%s, %s)
            """, (f'"{table}"', pk))
            seq_result = cur.fetchone()

            if seq_result and seq_result[0]:
                sequence_name = seq_result[0]
                # Reset sequence to max value + 1
                cur.execute(f"SELECT setval(%s, %s)", (sequence_name, max_id))
                log.info(f"Reset sequence for {table}.{pk} to {max_id}")
            else:
                log.warning(f"No sequence found for {table}.{pk}")
        else:
            log.info(f"Table {table} is empty, skipping sequence alignment")

    cur.close()
    conn.close()


with DAG(
    dag_id="replicate_stackoverflow_to_postgres_parallel",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "postgres", "replication", "full-parallel", "optimized", "partitioned"],
    template_searchpath=["/usr/local/airflow/include"],
    description="FULL PARALLEL with PARTITIONED large tables: Posts split into 4 parallel chunks",
    render_template_as_native_obj=True,  # Parse XCom as native Python objects
) as dag:
    ensure_db = PythonOperator(
        task_id="ensure_target_database",
        python_callable=ensure_target_database,
    )

    setup_tables = PythonOperator(
        task_id="create_or_truncate_tables",
        python_callable=create_or_truncate_tables,
    )

    # Regular tables run in parallel
    copy_regular_tasks = [
        PythonOperator(
            task_id=f"copy_{tbl}",
            python_callable=copy_table_src_to_tgt,
            op_kwargs={"table": tbl},
        )
        for tbl in REGULAR_TABLES
    ]

    # Partitioned tables: prepare → parallel partitions → finalize
    partitioned_tasks = []

    for table, num_partitions in PARTITIONED_TABLES.items():
        # Get partition ranges at DAG parse time (will be recalculated at runtime)
        # We create fixed partition tasks based on num_partitions

        prepare_task = PythonOperator(
            task_id=f"prepare_{table}",
            python_callable=prepare_partitioned_table,
            op_kwargs={"table": table},
        )

        # Create partition copy tasks
        partition_tasks = []
        for i in range(num_partitions):
            partition_task = PythonOperator(
                task_id=f"copy_{table}_p{i}",
                python_callable=copy_table_partition_from_xcom,
                op_kwargs={
                    "table": table,
                    "partition_num": i,
                    "ranges": "{{ ti.xcom_pull(task_ids='get_" + table + "_ranges') }}",
                },
            )
            partition_tasks.append(partition_task)

        finalize_task = PythonOperator(
            task_id=f"finalize_{table}",
            python_callable=finalize_partitioned_table,
            op_kwargs={"table": table},
        )

        # Get ranges task
        get_ranges_task = PythonOperator(
            task_id=f"get_{table}_ranges",
            python_callable=get_partition_ranges,
            op_kwargs={"table": table, "num_partitions": num_partitions},
        )

        # Dependencies: setup_tables → get_ranges → prepare → partitions (parallel) → finalize
        setup_tables >> get_ranges_task >> prepare_task >> partition_tasks >> finalize_task
        partitioned_tasks.append(finalize_task)

    convert_to_logged = PythonOperator(
        task_id="convert_tables_to_logged",
        python_callable=convert_tables_to_logged,
    )

    fix_sequences = PythonOperator(
        task_id="align_target_sequences",
        python_callable=set_identity_sequences,
    )

    # DEPENDENCY GRAPH:
    # ensure_db → setup_tables → [regular tables in parallel] → convert → fix_sequences
    #                          → [partitioned: get_ranges → prepare → partitions → finalize] →
    ensure_db >> setup_tables >> copy_regular_tasks >> convert_to_logged >> fix_sequences

    # Partitioned tables also flow into convert_to_logged
    for task in partitioned_tasks:
        task >> convert_to_logged
