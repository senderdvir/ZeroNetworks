import time
import requests
import pandas as pd
from sqlalchemy import create_engine

from config import SPACEX_API_URL_LAUNCHES, SPACEX_API_URL_PAYLOADS, SPACEX_API_URL_LAUNCHPAD
from db import insert_data_to_postgres, init_tables, execute_aggregate_query
from utils import create_df_from_json

# --- CONFIGURE SQLAlchemy ENGINE ---
# Update credentials and DB details in production (consider reading from env vars or secrets manager)
engine = create_engine("postgresql+psycopg2://spacex:spacex@localhost:5432/postgres")


def map_launch_data(launch: dict) -> dict:
    """Map raw launch JSON into structured format."""
    return {
        "id": launch.get("id"),
        "name": launch.get("name"),
        "date_utc": launch.get("date_utc"),
        "rocket": launch.get("rocket"),
        "success": launch.get("success"),
        "flight_number": launch.get("flight_number"),
        "payloads": launch.get("payloads"),  # Will explode into rows
        "launchpad": launch.get("launchpad"),
        "inserted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def map_payloads_data(payload: dict) -> dict:
    """Map raw payload JSON into structured format."""
    return {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "launch": payload.get("launch"),
        "mass_kg": payload.get("mass_kg"),
        "mass_lbs": payload.get("mass_lbs"),
    }

def map_launchpad_data(payload: dict) -> dict:
    """Map raw payload JSON into structured format."""
    return {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "full_name": payload.get("full_name"),
        "region": payload.get("region"),
        "timezone": payload.get("timezone"),
        "launch_attempts": payload.get("launch_attempts", 0),
        "launch_successes": payload.get("launch_successes", 0),
        "status": payload.get("status"),
    }


def fetch_data(url: str) -> dict:
    """
    Fetch JSON data from a given API URL.

    Raises:
        HTTPError if the response status is not 200.
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def ingest_launch_data() -> None:
    """
    Fetch, transform, and ingest launch data into the database.
    """
    launch_json = fetch_data(SPACEX_API_URL_LAUNCHES)
    mapped = map_launch_data(launch_json)

    df = create_df_from_json(mapped)

    # Explode payloads into individual rows (1 launch -> N payload rows)
    df = df.explode("payloads").reset_index(drop=True)

    insert_data_to_postgres(df, "launches_raw_data")


def ingest_payloads_data() -> None:
    """
    Fetch, transform, and ingest payload data into the database.
    """
    payloads_json = fetch_data(SPACEX_API_URL_PAYLOADS)

    # Stream each payload to reduce memory usage
    for payload in payloads_json:
        mapped = map_payloads_data(payload)
        df = create_df_from_json(mapped)
        insert_data_to_postgres(df, "payloads_raw_data")

def ingest_launchpad_data() -> None:
    """
    Fetch, transform, and ingest payload data into the database.
    """
    launchpad_json = fetch_data(SPACEX_API_URL_LAUNCHPAD)

    # Stream each payload to reduce memory usage
    for launchpad in launchpad_json:
        mapped = map_launchpad_data(launchpad)
        df = create_df_from_json(mapped)
        insert_data_to_postgres(df, "launchpad_raw_data")


def create_aggregated_table() -> None:
    execute_aggregate_query()


def run() -> None:
    """
    Entry point for the ETL pipeline.
    Initializes schema, then ingests and processes SpaceX API data.
    """
    init_tables()  # Creates tables if needed
    ingest_launch_data()
    ingest_payloads_data()
    ingest_launchpad_data()
    create_aggregated_table()  # Uncomment to build analytics layer


if __name__ == "__main__":
    run()
