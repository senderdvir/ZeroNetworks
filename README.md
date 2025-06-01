### README for Analytics Project

# Analytics Project

This project is designed to execute pre-defined SQL queries for analytics purposes. It reads SQL query files, executes them against a database, and logs the results for further analysis.

## Features

- Executes SQL queries stored in `.sql` files.
- Logs query execution status and results.
- Provides an entry point to run multiple analytics queries.

## Project Structure

```
.
├── analytics.py       # Main script to execute analytics queries
├── sql/               # Directory containing SQL query files
│   ├── top_payload_masses.sql
│   ├── launch_performance_over_time.sql
│   └── launch_site_utilization.sql
├── requirements.txt   # Python dependencies
└── README.md          # Project documentation
```

## Prerequisites

- Python 3.8 or higher
- A database connection (e.g., PostgreSQL, MySQL, etc.)
- SQL query files in the directory

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure the SQL files.

## Usage

1. Configure your database connection in the script.
2. Run the analytics queries:
   ```bash
   python analytics.py
   ```

## Logging

- **Info Logs**: Logs the execution status of each query.
- **Debug Logs**: Logs the first 5 rows of query results.
- **Error Logs**: Logs any errors encountered during query execution.

## SQL Queries

The project includes the following pre-defined SQL queries:

1. **Top Payload Masses**: Analyzes the largest payloads launched.
2. **Launch Performance Over Time**: Tracks launch performance trends.
3. **Launch Site Utilization**: Evaluates the usage of different launch sites.


## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any suggestions or improvements.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
