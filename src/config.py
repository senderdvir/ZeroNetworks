import os

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "datavase": os.getenv("POSTGRES_DATABASE", "launches"),
    "user": os.getenv("POSTGRES_USER", "spacex"),
    "passowrd": os.getenv("POSRGRES_PASSWORD", "spacex")
}

SPACEX_API_URL_LAUNCHES = "https://api.spacexdata.com/v5/launches"
SPACEX_API_URL_PAYLOADS = "https://api.spacexdata.com/v4/payloads"
SPACEX_API_URL_LAUNCHPAD = "https://api.spacexdata.com/v4/launchpads"
SPACEX_API_URL_LATEST_LAUNCHES = "https://api.spacexdata.com/v4/launches/latest"

# SPACEX_API_URL_ALL_LAUNCHES = "https://api.spacexdata.com/v4/launches"
