import os
import time
import requests
import logging
from sqlalchemy import create_engine
from analytics import run_analytics_queries

from config import (
    SPACEX_API_URL_LATEST_LAUNCHES,
    SPACEX_API_URL_PAYLOADS,
    SPACEX_API_URL_LAUNCHPAD,
    SPACEX_API_URL_LAUNCHES
)
from db import insert_data_to_postgres, init_tables, execute_aggregate_query
from utils import create_df_from_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create SQLAlchemy engine (credentials should come from env vars or secrets in prod)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://spacex:spacex@localhost:5432/postgres")
engine = create_engine(DATABASE_URL)


def fetch_data(url: str) -> dict:
    """
    Fetch JSON data from a given API URL.

    Args:
        url (str): The API endpoint URL.

    Returns:
        dict: Parsed JSON response.

    Raises:
        requests.HTTPError: If the request fails.
    """
    logger.info(f"Fetching data from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.exception(f"Failed to fetch data from {url}")
        raise


def map_launch_data(launch: dict) -> dict:
    """
    Transform a launch record from API format to DB-ready format.

    Args:
        launch (dict): Launch record from API.

    Returns:
        dict: Transformed launch record.
    """
    return {
        "id": launch.get("id"),
        "name": launch.get("name"),
        "date_utc": launch.get("date_utc"),
        "rocket": launch.get("rocket"),
        "success": launch.get("success"),
        "flight_number": launch.get("flight_number"),
        "payloads": launch.get("payloads"),
        "launchpad": launch.get("launchpad"),
        "inserted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def map_payloads_data(payload: dict) -> dict:
    """
    Transform a payload record from API format to DB-ready format.

    Args:
        payload (dict): Payload record from API.

    Returns:
        dict: Transformed payload record.
    """
    return {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "launch": payload.get("launch"),
        "mass_kg": payload.get("mass_kg"),
        "mass_lbs": payload.get("mass_lbs"),
    }


def map_launchpad_data(launchpad: dict) -> dict:
    """
    Transform a launchpad record from API format to DB-ready format.

    Args:
        launchpad (dict): Launchpad record from API.

    Returns:
        dict: Transformed launchpad record.
    """
    return {
        "id": launchpad.get("id"),
        "name": launchpad.get("name"),
        "full_name": launchpad.get("full_name"),
        "region": launchpad.get("region"),
        "timezone": launchpad.get("timezone"),
        "launch_attempts": launchpad.get("launch_attempts", 0),
        "launch_successes": launchpad.get("launch_successes", 0),
        "status": launchpad.get("status"),
    }

def ingest_launches_data() -> None:
    """
    Fetch, transform, and ingest latest launche into the database.
    """
    launch_json = fetch_data(SPACEX_API_URL_LATEST_LAUNCHES)
    logger.info(f"Ingesting {len(launch_json)} launches")

    try:
        mapped = map_launch_data(launch_json)
        df = create_df_from_json(mapped)
        df = df.explode("payloads").reset_index(drop=True)
        insert_data_to_postgres(df, "launches_raw_data")
    except Exception:
        logger.error(f"Failed to process launch with ID: {launch_json.get('id')}", exc_info=True)

# Uncomment the following function if you want to ingest all launches data (history)
# def ingest_launches_data() -> None:
#     """
#     Fetch, transform, and ingest all historical launches into the database.
#     Each launch can contain multiple payloads, which are exploded.
#     """
#     launches_json = fetch_data(SPACEX_API_URL_LAUNCHES)
#     logger.info(f"Ingesting {len(launches_json)} launches")

#     for launch in launches_json:
#         try:
#             mapped = map_launch_data(launch)
#             df = create_df_from_json(mapped)
#             df = df.explode("payloads").reset_index(drop=True)
#             insert_data_to_postgres(df, "launches_raw_data")
#         except Exception:
#             logger.error(f"Failed to process launch with ID: {launch.get('id')}", exc_info=True)


def ingest_payloads_data() -> None:
    """
    Fetch, transform, and ingest all payload data into the database.
    """
    payloads_json = fetch_data(SPACEX_API_URL_PAYLOADS)
    logger.info(f"Ingesting {len(payloads_json)} payloads")

    for payload in payloads_json:
        try:
            mapped = map_payloads_data(payload)
            df = create_df_from_json(mapped)
            insert_data_to_postgres(df, "payloads_raw_data")
        except Exception:
            logger.error(f"Failed to process payload with ID: {payload.get('id')}", exc_info=True)


def ingest_launchpad_data() -> None:
    """
    Fetch, transform, and ingest all launchpad data into the database.
    """
    launchpad_json = fetch_data(SPACEX_API_URL_LAUNCHPAD)
    logger.info(f"Ingesting {len(launchpad_json)} launchpads")

    for launchpad in launchpad_json:
        try:
            mapped = map_launchpad_data(launchpad)
            df = create_df_from_json(mapped)
            insert_data_to_postgres(df, "launchpad_raw_data")
        except Exception:
            logger.error(f"Failed to process launchpad with ID: {launchpad.get('id')}", exc_info=True)


def create_aggregated_table() -> None:
    """
    Run the aggregation SQL script to produce analytics-ready data.
    """
    try:
        execute_aggregate_query()
        logger.info("Successfully executed aggregation script.")
    except Exception:
        logger.error("Failed to execute aggregation script.", exc_info=True)


def run() -> None:
    """
    Entry point for the ETL pipeline.
    Initializes schema and ingests SpaceX API data into PostgreSQL.
    """
    logger.info("Starting ETL pipeline.")
    try:
        init_tables()
        ingest_launches_data()
        ingest_payloads_data()
        ingest_launchpad_data()
        create_aggregated_table()
        logger.info("ETL pipeline completed successfully.")
        logger.info("You can now run analytics queries on the ingested data.")
        run_analytics_queries()
        logger.info("Analytics queries executed successfully.")

    except Exception as e:
        logger.critical("ETL pipeline failed.", exc_info=True)


if __name__ == "__main__":
    run()