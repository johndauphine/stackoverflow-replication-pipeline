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
COMMIT_FREQUENCY = 10000  # Commit every 10K rows

# ALL tables - no dependencies since no foreign keys!
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

    # Create database if it doesn't exist (connected to master)
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'stackoverflow_target')
        BEGIN
            CREATE DATABASE stackoverflow_target;
        END
    """)
    log.info("Ensured stackoverflow_target database exists")

    # Switch to stackoverflow_target database
    cur.execute("USE stackoverflow_target;")

    # Drop all foreign key constraints first (though there shouldn't be any)
    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] DROP CONSTRAINT [' + name + '];'
        FROM sys.foreign_keys;
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
    Dynamically create target schema by copying table structures from source database.
    This replicates the Stack Overflow schema WITHOUT foreign keys for maximum parallel loading.
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

    # For each table, get CREATE TABLE script from source
    for table in ALL_TABLES:
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
            pk_def = f"PRIMARY KEY ({', '.join(f'[{col}]' for col in pk_cols)})"
            col_defs.append(pk_def)

        # Create table on target - NO FOREIGN KEYS!
        create_sql = f"CREATE TABLE dbo.[{table}] (\n  {',\n  '.join(col_defs)}\n);"
        tgt_cur.execute(create_sql)
        log.info(f"Created table dbo.{table} (no foreign keys)")

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Target schema created successfully (no foreign keys for parallel loading)")


def copy_table_direct_stream(table: str) -> None:
    """
    FULLY PARALLEL: Copy table using direct row streaming.
    No dependencies - all tables can run simultaneously!
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    log.info(f"[{table}] starting direct stream copy (batch_size={BATCH_SIZE}) - FULL PARALLEL MODE")

    # Get target table columns and nullability
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

    # Open target connection (will be used throughout streaming)
    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit(False)
    tgt_cur = tgt_conn.cursor()

    try:
        tgt_cur.execute("USE stackoverflow_target;")

        # Delete all rows from target table
        tgt_cur.execute(f"DELETE FROM dbo.[{table}]")

        # No need to disable FK checks - we don't have any!

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

                # Stream rows directly without intermediate buffering
                for row in src_cur:
                    batch.append(row)

                    if len(batch) >= BATCH_SIZE:
                        # Insert batch
                        placeholders = ", ".join(
                            f"({', '.join(['%s' for _ in target_columns])})"
                            for _ in batch
                        )
                        insert_sql = f"INSERT INTO dbo.[{table}] ({column_list_for_insert}) VALUES {placeholders}"
                        flat_values = [val for row in batch for val in row]
                        tgt_cur.execute(insert_sql, flat_values)

                        total_rows += len(batch)
                        batch = []

                        # Commit periodically to avoid large transactions
                        if total_rows % COMMIT_FREQUENCY == 0:
                            tgt_conn.commit()
                            log.info(f"[{table}] committed {total_rows} rows")

                # Insert remaining rows in final batch
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
        log.info(f"[{table}] direct stream completed: {total_rows} rows (FULL PARALLEL)")
    finally:
        tgt_cur.close()
        tgt_conn.close()


def set_identity_sequences() -> None:
    """Reseed identity columns on SQL Server target for Stack Overflow tables."""
    tgt = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = tgt.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    # Switch to stackoverflow_target database
    cur.execute("USE stackoverflow_target;")

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
        # Check if table exists
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s",
            (table,)
        )
        if cur.fetchone() is None:
            log.info(f"Table {table} not found, skipping sequence alignment")
            continue

        # Get max ID
        cur.execute(f"SELECT COALESCE(MAX([{pk}]), 0) FROM dbo.[{table}]")
        (max_id,) = cur.fetchone()

        if max_id > 0:
            # Reseed identity to max value
            cur.execute(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, {max_id})")
            log.info(f"Reseeded {table}.{pk} to {max_id}")
        else:
            log.info(f"Table {table} is empty, skipping sequence alignment")

    cur.close()
    conn.close()


with DAG(
    dag_id="replicate_stackoverflow_to_target_full_parallel",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "mssql", "replication", "sql-server", "full-parallel"],
    template_searchpath=["/usr/local/airflow/include"],
    description="FULL PARALLEL: All tables load simultaneously (no FK dependencies)",
) as dag:
    reset_tgt = PythonOperator(
        task_id="reset_target_schema",
        python_callable=reset_target_schema,
    )

    create_schema = PythonOperator(
        task_id="create_target_schema",
        python_callable=create_target_schema,
    )

    # ALL tables run in parallel!
    copy_tasks = [
        PythonOperator(
            task_id=f"copy_{tbl}",
            python_callable=copy_table_direct_stream,
            op_kwargs={"table": tbl},
        )
        for tbl in ALL_TABLES
    ]

    fix_sequences = PythonOperator(
        task_id="align_target_sequences",
        python_callable=set_identity_sequences,
    )

    # FULLY PARALLEL DEPENDENCY GRAPH
    # Schema creation → ALL tables in parallel → fix sequences
    reset_tgt >> create_schema
    create_schema >> copy_tasks >> fix_sequences