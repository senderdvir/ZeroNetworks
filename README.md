### README for Analytics Project

# Analytics Project

This project is designed to execute pre-defined SQL queries for analytics purposes. It reads SQL query files, executes them against a database, and logs the results for further analysis.

## Features
- Init the Data env tools (postgres and trino).
- Connect trino to postgres as spource.
- Init the relevant tables in the post gres with a query in the sql folder.
- Run ingestion.py to get dat from the spacexAPI
- Provides an entry point to run multiple analytics queries.
- Logs query execution status and results.
- Clean the env from unrelevant data for the next run.
- If history data needed, there is commented code that do the work.


## Prerequisites

- Python 3.8 or higher
- A database connection to PostgreSQL
- SQL query files in the directory

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Run the docker compose to init the data env tools.
   ```bash
   cd docker
   docker compose up
   ```

4. Run the ingestion.py.

## Usage

1. Configure your database connection in the script.
2. Run the ingestion.py it will run all the relevant code.

## Logging

- **Info Logs**: Logs the execution status of each query.
- **Debug Logs**: Logs the first 5 rows of query results.
- **Error Logs**: Logs any errors encountered during query execution.

## SQL Queries

The project includes the following pre-defined SQL queries:

1. **Top Payload Masses**: Analyzes the largest payloads launched.
2. **Launch Performance Over Time**: Tracks launch performance trends.
3. **Launch Site Utilization**: Evaluates the usage of different launch sites.



