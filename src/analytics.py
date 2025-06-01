import os
import logging
from trino.dbapi import connect
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_connection():
    """
    Establish a connection to the Trino query engine.

    Returns:
        trino.dbapi.Connection: A Trino connection object.

    Raises:
        Exception: If the connection fails.
    """
    try:
        conn = connect(
            host=os.getenv("TRINO_HOST", "localhost"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "trino"),
            catalog=os.getenv("TRINO_CATALOG", "postgres"),
            schema=os.getenv("TRINO_SCHEMA", "public"),
        )
        logger.info("Connected to Trino successfully.")
        return conn
    except Exception as e:
        logger.critical("Failed to connect to Trino.", exc_info=True)
        raise


def execute_analytics_queries(sql_file_paths: List[str]) -> None:
    """
    Execute a list of analytics SQL queries from file via Trino.

    Args:
        sql_file_paths (List[str]): List of file paths to SQL files.

    Logs:
        Query execution status and errors.
    """
    conn = get_connection()
    cursor = conn.cursor()

    for path in sql_file_paths:
        if not path.endswith(".sql"):
            logger.warning(f"Skipping non-SQL file: {path}")
            continue

        try:
            with open(path, 'r') as f:
                query = f.read()

            logger.info(f"Executing query: {path}")
            cursor.execute(query)
            results = cursor.fetchall()

            logger.info(f"Query executed successfully: {path}")
            logger.debug(f"Query result (first 5 rows): {results[:5]}")
        except Exception:
            logger.error(f"Failed to execute query: {path}", exc_info=True)


def run_analytics_queries() -> None:
    """
    Entrypoint to execute pre-defined analytics SQL queries.
    """
    query_files = [
        "sql/top_payload_masses.sql",
        "sql/launch_performance_over_time.sql",
        "sql/launch_site_utilization.sql"
    ]
    execute_analytics_queries(query_files)