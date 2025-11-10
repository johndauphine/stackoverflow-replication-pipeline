from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

SRC_CONN_ID = "stackoverflow_source"
TGT_CONN_ID = "stackoverflow_target"

log = logging.getLogger(__name__)

# SQL Server limit: 1000 rows per INSERT VALUES statement
BATCH_SIZE = 1000
COMMIT_FREQUENCY = 50000  # Larger commits for heap tables

# ALL tables - no dependencies, no indexes!
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
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def reset_target_schema() -> None:
    """Create stackoverflow_target database and drop all tables in dbo schema on SQL Server target."""
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

    # Drop all foreign key constraints first
    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] DROP CONSTRAINT [' + name + '];'
        FROM sys.foreign_keys;
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)

    # Drop all primary key constraints
    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'ALTER TABLE [' + t.name + '] DROP CONSTRAINT [' + i.name + '];'
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        WHERE i.is_primary_key = 1;
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)

    # Drop all tables in dbo schema
    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'DROP TABLE [dbo].[' + name + '];'
        FROM sys.tables
        WHERE schema_id = SCHEMA_ID('dbo');
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)
    log.info("Dropped all existing tables in dbo schema")

    cur.close()
    conn.close()


def create_target_schema() -> None:
    """
    Create HEAP TABLES (no indexes, no primary keys) for MAXIMUM INSERT SPEED!
    Indexes will be created AFTER data load.
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

    # For each table, get CREATE TABLE script from source
    for table in ALL_TABLES:
        log.info(f"Creating HEAP table for {table} (NO INDEXES)")

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

        # Build CREATE TABLE statement - NO CONSTRAINTS!
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

            # Add NULL/NOT NULL
            if is_nullable == 'NO':
                col_def += " NOT NULL"
            else:
                col_def += " NULL"

            col_defs.append(col_def)

        # Get primary key info for later (but don't create it now!)
        src_cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = %s
              AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """, (table,))

        pk_cols = [row[0] for row in src_cur.fetchall()]
        if pk_cols:
            # Store for later, but DON'T add to table creation
            pk_definitions[table] = pk_cols
            log.info(f"Will create PRIMARY KEY on {table}({', '.join(pk_cols)}) AFTER data load")

        # Create table as HEAP (no primary key, no indexes!)
        create_sql = f"CREATE TABLE dbo.[{table}] (\n  {',\n  '.join(col_defs)}\n);"
        tgt_cur.execute(create_sql)
        log.info(f"Created HEAP table dbo.{table} (no indexes for fast loading)")

    # Save PK definitions to a temp table for the create_indexes task
    tgt_cur.execute("""
        IF OBJECT_ID('tempdb..##pk_definitions') IS NOT NULL DROP TABLE ##pk_definitions;
        CREATE TABLE ##pk_definitions (
            table_name NVARCHAR(128),
            pk_columns NVARCHAR(MAX)
        );
    """)

    for table, pk_cols in pk_definitions.items():
        tgt_cur.execute(
            "INSERT INTO ##pk_definitions VALUES (%s, %s)",
            (table, ','.join(pk_cols))
        )

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Target schema created - HEAP TABLES for maximum insert speed!")


def copy_table_ultra_fast(table: str) -> None:
    """
    ULTRA FAST: Copy to HEAP tables (no index overhead).
    Expected 3-10x faster than indexed tables.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    log.info(f"[{table}] ULTRA FAST copy starting (heap table, no indexes)")

    # Get target table columns
    with tgt_hook.get_conn() as tgt_conn_tmp:
        tgt_conn_tmp.autocommit(True)
        with tgt_conn_tmp.cursor() as tgt_cur_tmp:
            tgt_cur_tmp.execute("USE stackoverflow_target;")
            tgt_cur_tmp.execute(
                """
                SELECT COLUMN_NAME, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
                """,
                (table,),
            )
            column_info = tgt_cur_tmp.fetchall()
            target_columns = [row[0] for row in column_info]

    if not target_columns:
        raise RuntimeError(f"Target table dbo.{table} has no columns")

    column_list_for_select = ", ".join(f"[{col}]" for col in target_columns)
    column_list_for_insert = ", ".join(f"[{col}]" for col in target_columns)

    # Open target connection
    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit(False)
    tgt_cur = tgt_conn.cursor()

    try:
        tgt_cur.execute("USE stackoverflow_target;")

        # Set recovery model to BULK_LOGGED for minimal logging
        tgt_cur.execute("""
            IF (SELECT recovery_model_desc FROM sys.databases WHERE name = 'stackoverflow_target') != 'BULK_LOGGED'
                ALTER DATABASE stackoverflow_target SET RECOVERY BULK_LOGGED;
        """)

        # Delete all rows from target table
        tgt_cur.execute(f"TRUNCATE TABLE dbo.[{table}]")  # Faster than DELETE for heap tables

        # Check if table has an identity column
        tgt_cur.execute("""
            SELECT COUNT(*)
            FROM sys.identity_columns ic
            INNER JOIN sys.tables t ON ic.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'dbo' AND t.name = %s
        """, (table,))
        has_identity = tgt_cur.fetchone()[0] > 0

        # Enable IDENTITY_INSERT if needed
        if has_identity:
            tgt_cur.execute(f"SET IDENTITY_INSERT dbo.[{table}] ON")

        # Stream rows from source to target
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit(True)
            with src_conn.cursor() as src_cur:
                # Select all rows from source table
                src_cur.execute(f"SELECT {column_list_for_select} FROM dbo.[{table}]")

                batch = []
                total_rows = 0

                # Stream rows directly
                for row in src_cur:
                    batch.append(row)

                    if len(batch) >= BATCH_SIZE:
                        # Insert batch - HEAP TABLE = FAST!
                        placeholders = ", ".join(
                            f"({', '.join(['%s' for _ in target_columns])})"
                            for _ in batch
                        )
                        insert_sql = f"INSERT INTO dbo.[{table}] ({column_list_for_insert}) VALUES {placeholders}"
                        flat_values = [val for row in batch for val in row]
                        tgt_cur.execute(insert_sql, flat_values)

                        total_rows += len(batch)
                        batch = []

                        # Larger commits for heap tables (less overhead)
                        if total_rows % COMMIT_FREQUENCY == 0:
                            tgt_conn.commit()
                            log.info(f"[{table}] ULTRA FAST: {total_rows} rows")

                # Insert remaining rows
                if batch:
                    placeholders = ", ".join(
                        f"({', '.join(['%s' for _ in target_columns])})"
                        for _ in batch
                    )
                    insert_sql = f"INSERT INTO dbo.[{table}] ({column_list_for_insert}) VALUES {placeholders}"
                    flat_values = [val for row in batch for val in row]
                    tgt_cur.execute(insert_sql, flat_values)
                    total_rows += len(batch)

        # Disable IDENTITY_INSERT if we enabled it
        if has_identity:
            tgt_cur.execute(f"SET IDENTITY_INSERT dbo.[{table}] OFF")

        tgt_conn.commit()
        log.info(f"[{table}] ULTRA FAST completed: {total_rows} rows (heap table)")
    finally:
        tgt_cur.close()
        tgt_conn.close()


def create_all_indexes() -> None:
    """
    Create ALL indexes and primary keys AFTER data is loaded.
    This is MUCH faster than maintaining indexes during INSERTs!
    """
    tgt = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = tgt.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    log.info("Creating indexes and primary keys AFTER data load...")

    # Switch to target database
    cur.execute("USE stackoverflow_target;")

    # Get PK definitions saved earlier
    cur.execute("""
        IF OBJECT_ID('tempdb..##pk_definitions') IS NOT NULL
            SELECT table_name, pk_columns FROM ##pk_definitions
        ELSE
            SELECT NULL, NULL WHERE 1=0
    """)

    pk_defs = cur.fetchall()

    # Fallback: if temp table doesn't exist, use standard PK columns
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
        # Convert comma-separated string back to individual columns
        pk_defs = [(table, cols.split(',')) for table, cols in pk_defs]
        # Flatten for single column PKs
        pk_defs = [(table, cols[0] if len(cols) == 1 else cols) for table, cols in pk_defs]

    # Create primary keys (clustered indexes)
    for table, pk_col in pk_defs:
        # Check if table exists
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s",
            (table,)
        )
        if cur.fetchone() is None:
            log.info(f"Table {table} not found, skipping index creation")
            continue

        try:
            if isinstance(pk_col, str):
                pk_constraint = f"ALTER TABLE dbo.[{table}] ADD CONSTRAINT PK_{table} PRIMARY KEY CLUSTERED ([{pk_col}])"
            else:
                # Multiple columns
                pk_constraint = f"ALTER TABLE dbo.[{table}] ADD CONSTRAINT PK_{table} PRIMARY KEY CLUSTERED ({', '.join(f'[{col}]' for col in pk_col)})"

            cur.execute(pk_constraint)
            log.info(f"Created PRIMARY KEY on {table}")
        except Exception as e:
            log.warning(f"Could not create PK on {table}: {e}")

    # Update statistics for query optimizer
    for table in ALL_TABLES:
        try:
            cur.execute(f"UPDATE STATISTICS dbo.[{table}] WITH FULLSCAN")
            log.info(f"Updated statistics for {table}")
        except Exception:
            pass

    # Set recovery model back to FULL
    cur.execute("""
        IF (SELECT recovery_model_desc FROM sys.databases WHERE name = 'stackoverflow_target') = 'BULK_LOGGED'
            ALTER DATABASE stackoverflow_target SET RECOVERY FULL;
    """)

    # Also fix identity sequences
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
        # Check if table exists
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s",
            (table,)
        )
        if cur.fetchone() is None:
            continue

        # Get max ID
        cur.execute(f"SELECT COALESCE(MAX([{pk}]), 0) FROM dbo.[{table}]")
        (max_id,) = cur.fetchone()

        if max_id > 0:
            # Reseed identity to max value
            cur.execute(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, {max_id})")
            log.info(f"Reseeded {table}.{pk} to {max_id}")

    cur.close()
    conn.close()

    log.info("All indexes and primary keys created successfully!")


with DAG(
    dag_id="replicate_stackoverflow_to_target_no_indexes",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "mssql", "replication", "sql-server", "ultra-fast", "no-indexes"],
    template_searchpath=["/usr/local/airflow/include"],
    description="ULTRA FAST: Load to HEAP tables (no indexes), create indexes AFTER",
) as dag:
    reset_tgt = PythonOperator(
        task_id="reset_target_schema",
        python_callable=reset_target_schema,
    )

    create_schema = PythonOperator(
        task_id="create_heap_tables",
        python_callable=create_target_schema,
    )

    # ALL tables run in parallel into HEAP tables (no index overhead!)
    copy_tasks = [
        PythonOperator(
            task_id=f"copy_{tbl}",
            python_callable=copy_table_ultra_fast,
            op_kwargs={"table": tbl},
        )
        for tbl in ALL_TABLES
    ]

    # Create indexes AFTER all data is loaded
    create_indexes = PythonOperator(
        task_id="create_indexes_and_pks",
        python_callable=create_all_indexes,
    )

    # ULTRA FAST DEPENDENCY GRAPH
    # Schema → ALL tables parallel (heap) → Create indexes
    reset_tgt >> create_schema
    create_schema >> copy_tasks >> create_indexes