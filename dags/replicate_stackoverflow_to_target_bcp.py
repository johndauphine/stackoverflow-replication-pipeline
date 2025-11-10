from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

SRC_CONN_ID = "stackoverflow_source"
TGT_CONN_ID = "stackoverflow_target"

log = logging.getLogger(__name__)

# ALL tables - no dependencies with BCP!
ALL_TABLES = [
    "VoteTypes",      # Small lookup table
    "PostTypes",      # Small lookup table
    "LinkTypes",      # Small lookup table
    "Users",          # 299K rows
    "Badges",         # 1.1M rows
    "Posts",          # 3.7M rows (largest)
    "PostLinks",      # ~100K rows
    "Comments",       # ~1.3M rows
    "Votes",          # ~10M rows (might be largest)
]

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Use a persistent directory for BCP files across tasks
BCP_DIR = "/tmp/bcp_files"


def reset_target_schema() -> None:
    """Create stackoverflow_target database and drop all tables."""
    hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    # Create database if it doesn't exist
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'stackoverflow_target')
        BEGIN
            CREATE DATABASE stackoverflow_target;
        END
    """)
    log.info("Ensured stackoverflow_target database exists")

    # Switch to stackoverflow_target database
    cur.execute("USE stackoverflow_target;")

    # Set to BULK_LOGGED for optimal BCP performance
    cur.execute("""
        ALTER DATABASE stackoverflow_target SET RECOVERY BULK_LOGGED;
    """)
    log.info("Set database to BULK_LOGGED recovery model for BCP")

    # Drop all constraints and tables
    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] DROP CONSTRAINT [' + name + '];'
        FROM sys.foreign_keys;
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)

    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'ALTER TABLE [' + t.name + '] DROP CONSTRAINT [' + i.name + '];'
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        WHERE i.is_primary_key = 1;
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)

    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'DROP TABLE [dbo].[' + name + '];'
        FROM sys.tables
        WHERE schema_id = SCHEMA_ID('dbo');
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)
    log.info("Dropped all existing tables in dbo schema")

    # Create BCP directory
    os.makedirs(BCP_DIR, exist_ok=True)
    log.info(f"Created BCP directory: {BCP_DIR}")

    cur.close()
    conn.close()


def create_target_schema() -> None:
    """
    Create HEAP TABLES (no indexes) optimized for BCP bulk loading.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    src_conn = src_hook.get_conn()
    src_conn.autocommit(True)
    src_cur = src_conn.cursor()

    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit(True)
    tgt_cur = tgt_conn.cursor()

    # Switch to target database
    tgt_cur.execute("USE stackoverflow_target;")

    # Store primary key info for later index creation
    pk_definitions = {}

    for table in ALL_TABLES:
        log.info(f"Creating HEAP table for {table} (optimized for BCP)")

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

        # Build CREATE TABLE statement - NO CONSTRAINTS for BCP speed!
        col_defs = []
        for col in columns:
            col_name, data_type, char_len, num_prec, num_scale, is_nullable, is_identity = col

            # Build column definition
            col_def = f"[{col_name}] {data_type.upper()}"

            # Add length/precision
            if char_len and data_type.lower() in ('varchar', 'nvarchar', 'char', 'nchar'):
                if char_len == -1:
                    col_def += "(MAX)"
                else:
                    col_def += f"({char_len})"
            elif num_prec and data_type.lower() in ('decimal', 'numeric'):
                col_def += f"({num_prec},{num_scale or 0})"

            # Add IDENTITY if source column is identity
            if is_identity:
                col_def += " IDENTITY(1,1)"

            # For BCP, allow NULLs everywhere initially (we'll fix constraints later)
            col_def += " NULL"

            col_defs.append(col_def)

        # Get primary key info for later
        src_cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = %s
              AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """, (table,))

        pk_cols = [row[0] for row in src_cur.fetchall()]
        if pk_cols:
            pk_definitions[table] = pk_cols
            log.info(f"Will create PRIMARY KEY on {table}({', '.join(pk_cols)}) AFTER BCP load")

        # Create table as HEAP
        create_sql = f"CREATE TABLE dbo.[{table}] (\n  {',\n  '.join(col_defs)}\n);"
        tgt_cur.execute(create_sql)
        log.info(f"Created HEAP table dbo.{table} for BCP loading")

    # Save PK definitions
    tgt_cur.execute("""
        IF OBJECT_ID('tempdb..##pk_definitions_bcp') IS NOT NULL DROP TABLE ##pk_definitions_bcp;
        CREATE TABLE ##pk_definitions_bcp (
            table_name NVARCHAR(128),
            pk_columns NVARCHAR(MAX)
        );
    """)

    for table, pk_cols in pk_definitions.items():
        tgt_cur.execute(
            "INSERT INTO ##pk_definitions_bcp VALUES (%s, %s)",
            (table, ','.join(pk_cols))
        )

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Target schema created - HEAP TABLES optimized for BCP!")


def bcp_export_table(table: str) -> None:
    """
    Export table from source using BCP to native format.
    Native format is the fastest - no data conversion needed.
    """
    # BCP file path inside the source container
    container_bcp_file = f"/tmp/{table}.bcp"
    local_bcp_file = f"{BCP_DIR}/{table}.bcp"

    # Use BCP from within the SQL Server source container
    bcp_cmd = (
        f"docker exec stackoverflow-mssql-source "
        f"/opt/mssql-tools18/bin/bcp "
        f"dbo.[{table}] out {container_bcp_file} "
        f"-S localhost "
        f"-d StackOverflow2010 "
        f"-U sa "
        f"-P StackOverflow123! "
        f"-n "  # Native format (fastest)
        f"-C "  # Trust server certificate
    )

    log.info(f"[{table}] Starting BCP export using SQL Server container")

    # Execute BCP export
    import subprocess
    result = subprocess.run(bcp_cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        log.error(f"BCP export failed: {result.stderr}")
        # Fallback to Python export
        log.warning(f"BCP command failed, using Python fallback for {table}")
        export_with_python_fallback(table)
    else:
        # Copy file from container to Airflow
        copy_cmd = f"docker cp stackoverflow-mssql-source:{container_bcp_file} {local_bcp_file}"
        subprocess.run(copy_cmd, shell=True, check=True)

        # Get file size
        file_size = os.path.getsize(local_bcp_file) / (1024 * 1024)  # MB
        log.info(f"[{table}] BCP export completed: {file_size:.2f} MB")


def export_with_python_fallback(table: str):
    """
    Fallback: Export using Python if BCP command is not available.
    This simulates BCP by writing data in a binary-friendly format.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)

    bcp_file = f"{BCP_DIR}/{table}.csv"

    with src_hook.get_conn() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            # Get all data
            cur.execute(f"SELECT * FROM dbo.[{table}]")

            # Write to CSV (not as fast as native BCP but still bulk-loadable)
            import csv
            with open(bcp_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

                row_count = 0
                for row in cur:
                    # Handle NULL values
                    processed_row = ['' if val is None else str(val) for val in row]
                    writer.writerow(processed_row)
                    row_count += 1

                    if row_count % 100000 == 0:
                        log.info(f"[{table}] Exported {row_count} rows...")

                file_size = os.path.getsize(bcp_file) / (1024 * 1024)  # MB
                log.info(f"[{table}] Export completed: {row_count} rows, {file_size:.2f} MB")


def bcp_import_table(table: str) -> None:
    """
    Import table to target using BCP from native format file.
    This is the FASTEST way to load data into SQL Server!
    """
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    # Determine file type
    bcp_file = f"{BCP_DIR}/{table}.bcp"
    csv_file = f"{BCP_DIR}/{table}.csv"

    if os.path.exists(bcp_file):
        local_file = bcp_file
        format_flag = "-n"  # Native format
    elif os.path.exists(csv_file):
        local_file = csv_file
        format_flag = "-c -t '\t'"  # Character format with tab delimiter
    else:
        raise FileNotFoundError(f"No export file found for {table}")

    # Copy file to target container
    container_file = f"/tmp/{table}.bcp"
    copy_cmd = f"docker cp {local_file} stackoverflow-mssql-target:{container_file}"

    import subprocess
    subprocess.run(copy_cmd, shell=True, check=True)

    # Check if table has identity column
    with tgt_hook.get_conn() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            cur.execute("USE stackoverflow_target;")
            cur.execute("""
                SELECT COUNT(*)
                FROM sys.identity_columns ic
                INNER JOIN sys.tables t ON ic.object_id = t.object_id
                WHERE t.name = %s
            """, (table,))
            has_identity = cur.fetchone()[0] > 0

    # Build BCP import command using target container
    identity_flag = "-E" if has_identity else ""  # Keep identity values

    bcp_cmd = (
        f"docker exec stackoverflow-mssql-target "
        f"/opt/mssql-tools18/bin/bcp "
        f"dbo.[{table}] in {container_file} "
        f"-S localhost "
        f"-d stackoverflow_target "
        f"-U sa "
        f"-P StackOverflow123! "
        f"{format_flag} "
        f"{identity_flag} "
        f"-b 10000 "  # Batch size
        f"-m 0 "  # No max errors (fail fast)
        f"-C "  # Trust server certificate
    )

    log.info(f"[{table}] Starting BCP import using SQL Server container")

    # Execute BCP import
    result = subprocess.run(bcp_cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        log.error(f"BCP import failed: {result.stderr}")
        # Fallback: Use BULK INSERT
        log.warning(f"BCP command failed, using BULK INSERT for {table}")
        bulk_insert_fallback(table, local_file)
    else:
        log.info(f"[{table}] BCP import completed successfully")
        # Clean up container file
        cleanup_cmd = f"docker exec stackoverflow-mssql-target rm -f {container_file}"
        subprocess.run(cleanup_cmd, shell=True)


def bulk_insert_fallback(table: str, file_path: str):
    """
    Fallback: Use BULK INSERT if BCP command is not available.
    """
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    with tgt_hook.get_conn() as conn:
        conn.autocommit(False)
        with conn.cursor() as cur:
            cur.execute("USE stackoverflow_target;")

            # Determine if CSV or native format
            if file_path.endswith('.csv'):
                bulk_sql = f"""
                BULK INSERT dbo.[{table}]
                FROM '{file_path}'
                WITH (
                    FIELDTERMINATOR = '\t',
                    ROWTERMINATOR = '\n',
                    TABLOCK,
                    BATCHSIZE = 10000,
                    KEEPIDENTITY
                )
                """
            else:
                # For native BCP format, we'd need format file
                # For now, just error out
                raise NotImplementedError("Native BCP format requires format file for BULK INSERT")

            cur.execute(bulk_sql)
            conn.commit()
            log.info(f"[{table}] BULK INSERT completed")


def create_all_indexes() -> None:
    """
    Create ALL indexes and primary keys AFTER BCP load.
    This is MUCH faster than maintaining indexes during load!
    """
    tgt = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = tgt.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    log.info("Creating indexes and primary keys AFTER BCP load...")

    cur.execute("USE stackoverflow_target;")

    # Get PK definitions
    cur.execute("""
        IF OBJECT_ID('tempdb..##pk_definitions_bcp') IS NOT NULL
            SELECT table_name, pk_columns FROM ##pk_definitions_bcp
        ELSE
            SELECT NULL, NULL WHERE 1=0
    """)

    pk_defs = cur.fetchall()

    # Fallback to standard PKs
    if not pk_defs:
        pk_defs = [
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
    else:
        pk_defs = [(table, cols.split(',')) for table, cols in pk_defs]
        pk_defs = [(table, cols[0] if len(cols) == 1 else cols) for table, cols in pk_defs]

    # Create primary keys with ONLINE option if possible
    for table, pk_col in pk_defs:
        # Check if table exists and has data
        cur.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s",
            (table,)
        )
        if cur.fetchone()[0] == 0:
            log.info(f"Table {table} not found, skipping index creation")
            continue

        try:
            # First, update NULL values in PK columns to defaults if needed
            if isinstance(pk_col, str):
                cur.execute(f"UPDATE dbo.[{table}] SET [{pk_col}] = 0 WHERE [{pk_col}] IS NULL")
                cur.execute(f"ALTER TABLE dbo.[{table}] ALTER COLUMN [{pk_col}] INT NOT NULL")
                pk_constraint = f"ALTER TABLE dbo.[{table}] ADD CONSTRAINT PK_{table} PRIMARY KEY CLUSTERED ([{pk_col}]) WITH (SORT_IN_TEMPDB = ON)"
            else:
                for col in pk_col:
                    cur.execute(f"UPDATE dbo.[{table}] SET [{col}] = 0 WHERE [{col}] IS NULL")
                    cur.execute(f"ALTER TABLE dbo.[{table}] ALTER COLUMN [{col}] INT NOT NULL")
                pk_constraint = f"ALTER TABLE dbo.[{table}] ADD CONSTRAINT PK_{table} PRIMARY KEY CLUSTERED ({', '.join(f'[{col}]' for col in pk_col)}) WITH (SORT_IN_TEMPDB = ON)"

            cur.execute(pk_constraint)
            log.info(f"Created PRIMARY KEY on {table}")

            # Update statistics after index creation
            cur.execute(f"UPDATE STATISTICS dbo.[{table}] WITH FULLSCAN")

        except Exception as e:
            log.warning(f"Could not create PK on {table}: {e}")

    # Rebuild indexes for optimal performance
    for table in ALL_TABLES:
        try:
            cur.execute(f"ALTER INDEX ALL ON dbo.[{table}] REBUILD WITH (SORT_IN_TEMPDB = ON)")
            log.info(f"Rebuilt indexes for {table}")
        except Exception:
            pass

    # Set recovery model back to FULL
    cur.execute("""
        ALTER DATABASE stackoverflow_target SET RECOVERY FULL;
    """)

    # Fix identity sequences
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
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s",
            (table,)
        )
        if cur.fetchone() is None:
            continue

        cur.execute(f"SELECT COALESCE(MAX([{pk}]), 0) FROM dbo.[{table}]")
        (max_id,) = cur.fetchone()

        if max_id > 0:
            cur.execute(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, {max_id})")
            log.info(f"Reseeded {table}.{pk} to {max_id}")

    cur.close()
    conn.close()

    log.info("All indexes and primary keys created successfully after BCP load!")


def cleanup_bcp_files() -> None:
    """Clean up BCP export files to save space."""
    import shutil
    if os.path.exists(BCP_DIR):
        shutil.rmtree(BCP_DIR)
        log.info(f"Cleaned up BCP files in {BCP_DIR}")


with DAG(
    dag_id="replicate_stackoverflow_to_target_bcp",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "mssql", "replication", "sql-server", "bcp", "ultra-fast"],
    template_searchpath=["/usr/local/airflow/include"],
    description="BCP ULTRA FAST: Export to native format, bulk load, then create indexes",
) as dag:

    reset_tgt = PythonOperator(
        task_id="reset_target_schema",
        python_callable=reset_target_schema,
    )

    create_schema = PythonOperator(
        task_id="create_heap_tables",
        python_callable=create_target_schema,
    )

    # Export all tables in parallel using BCP
    export_tasks = [
        PythonOperator(
            task_id=f"bcp_export_{tbl}",
            python_callable=bcp_export_table,
            op_kwargs={"table": tbl},
        )
        for tbl in ALL_TABLES
    ]

    # Import all tables in parallel using BCP
    import_tasks = [
        PythonOperator(
            task_id=f"bcp_import_{tbl}",
            python_callable=bcp_import_table,
            op_kwargs={"table": tbl},
        )
        for tbl in ALL_TABLES
    ]

    # Create indexes AFTER all BCP loads
    create_indexes = PythonOperator(
        task_id="create_indexes_and_pks",
        python_callable=create_all_indexes,
    )

    # Clean up BCP files
    cleanup = PythonOperator(
        task_id="cleanup_bcp_files",
        python_callable=cleanup_bcp_files,
        trigger_rule="all_done",  # Run even if something fails
    )

    # BCP PIPELINE:
    # Reset → Create schema → Export all (parallel) → Import all (parallel) → Create indexes → Cleanup
    reset_tgt >> create_schema

    # All exports can run in parallel
    for export_task in export_tasks:
        create_schema >> export_task

    # Each import depends on its corresponding export
    for i, (export_task, import_task) in enumerate(zip(export_tasks, import_tasks)):
        export_task >> import_task

    # All imports must complete before creating indexes
    for import_task in import_tasks:
        import_task >> create_indexes

    # Cleanup runs after indexes
    create_indexes >> cleanup