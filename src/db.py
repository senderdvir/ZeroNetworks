import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging
import os
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_postgres_connection() -> Engine:
    """
    Establish a SQLAlchemy Engine connected to the PostgreSQL database.

    Returns:
        Engine: SQLAlchemy Engine instance for PostgreSQL.

    Raises:
        Exception: If the engine creation fails.
    """
    try:
        engine = create_engine('postgresql+psycopg2://spacex:spacex@localhost:5432/launches')
        logger.info("Successfully created PostgreSQL engine.")
        return engine
    except Exception as e:
        logger.exception("Failed to create PostgreSQL engine.")
        raise


def insert_data_to_postgres(data: pd.DataFrame, table_name: str) -> None:
    """
    Insert a pandas DataFrame into a PostgreSQL table.

    Args:
        data (pd.DataFrame): Data to insert.
        table_name (str): Target table name.

    Notes:
        - Uses multi-row inserts for performance.
        - Assumes DataFrame columns match DB table.
    """
    if data.empty:
        logger.warning(f"No data to insert into '{table_name}'. Skipping.")
        return

    engine = get_postgres_connection()
    try:
        data.to_sql(
            table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        logger.info(f"Inserted {len(data)} rows into '{table_name}'.")
    except Exception as e:
        logger.exception(f"Failed to insert data into '{table_name}'.")
        


def execute_aggregate_query(sql_path: str = 'sql/aggregate_table.sql') -> None:
    """
    Execute the aggregate query SQL script to populate or update summary tables.

    Args:
        sql_path (str): Path to the SQL file containing the aggregate query.
    """
    if not os.path.isfile(sql_path):
        logger.error(f"SQL file not found: {sql_path}")
        raise FileNotFoundError(f"{sql_path} does not exist.")

    engine = get_postgres_connection()
    try:
        with open(sql_path, 'r') as file:
            query = file.read()

        with engine.begin() as conn:
            conn.execute(text(query))
        logger.info(f"Aggregate query from '{sql_path}' executed successfully.")
    except Exception as e:
        logger.exception(f"Failed to execute aggregate query from '{sql_path}'.")
        raise


def init_tables(sql_files: List[str] = None) -> None:
    """
    Initialize required PostgreSQL tables using provided SQL DDL files.

    Args:
        sql_files (List[str], optional): List of SQL DDL file paths. Defaults to standard files.
    """
    if sql_files is None:
        sql_files = [
            'sql/create_payloads_table.sql',
            'sql/create_launches_table.sql',
            'sql/create_aggregate_table.sql',
            'sql/create_launchpad_table.sql'
        ]

    engine = get_postgres_connection()

    for file_path in sql_files:
        if not file_path.endswith('.sql') or not os.path.isfile(file_path):
            logger.warning(f"Skipping invalid or missing file: {file_path}")
            continue

        try:
            with open(file_path, 'r') as f:
                ddl_statement = f.read()

            with engine.begin() as conn:
                conn.execute(text(ddl_statement))

            logger.info(f"Executed DDL from '{file_path}' successfully.")
        except Exception as e:
            logger.exception(f"Failed to execute DDL from '{file_path}'.")
            raise

def truncate_tables() -> None:
    """
    Truncate all tables in the PostgreSQL database to reset data.
    This is useful for cleaning up before re-ingesting data.
    """
    engine = get_postgres_connection()
    try:
        with open("sql/truncate_tables.sql", 'r') as f:
            ddl_statement = f.read()
        with engine.begin() as conn:
            conn.execute(text(ddl_statement))
        logger.info("All tables truncated successfully.")
    except Exception as e:
        logger.exception("Failed to truncate tables.")
        raise

def run_db_rules() -> None:
    """
    Execute database rules or constraints to ensure data integrity.
    This can include triggers, constraints, etc.
    """
    engine = get_postgres_connection()
    try:
        with open("sql/db_rules.sql", 'r') as f:
            ddl_statement = f.read()
        with engine.begin() as conn:
            conn.execute(text(ddl_statement))
        logger.info("Database rules executed successfully.")
    except Exception as e:
        logger.exception("Failed to execute database rules.")
        raise
    
