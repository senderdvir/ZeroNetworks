import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import psycopg2


def get_postgres_connection() -> Engine:
    """
    Establish a SQLAlchemy Engine connected to the PostgreSQL database.

    Returns:
        sqlalchemy.engine.Engine: A SQLAlchemy engine for PostgreSQL.

    Raises:
        Exception: If the connection fails.
    """
    return create_engine('postgresql+psycopg2://spacex:spacex@localhost:5432/launches')


def insert_data_to_postgres(data: pd.DataFrame, table_name: str) -> None:
    """
    Insert a pandas DataFrame into a PostgreSQL table using SQLAlchemy.

    Args:
        data (pd.DataFrame): The DataFrame to insert.
        table_name (str): Target table name in PostgreSQL.

    Notes:
        - Appends data to the table.
        - Uses multi-row insert for performance.
    """
    engine = get_postgres_connection()

    # Ensure columns are ordered to match the DB schema if needed
    data.to_sql(
        table_name,
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )

def execute_aggregate_query()-> None:
    
    engine = get_postgres_connection()
    with open('sql/aggregate_table.sql', 'r') as file:
        create_payloads_query = file.read()
    
    with engine.begin() as connection:
        connection.execute(text(create_payloads_query))


def init_tables() -> None:
    """
    Initialize required tables by executing SQL schema files.

    Reads SQL DDL files and executes them to create tables
    if they do not already exist.
    """
    engine = get_postgres_connection()
    files_name = ['sql/create_payloads_table.sql', 'sql/create_launches_table.sql', 'sql/create_aggregate_table.sql', 'sql/create_launchpad_table.sql']
    for file in files_name:
        if not file.endswith('.sql'):
            continue
        with open(file, 'r') as f:
            ddl_statement = f.read()
    # Read SQL schema files
    
        with engine.begin() as connection:
            # Execute each DDL statement
            connection.execute(text(ddl_statement))