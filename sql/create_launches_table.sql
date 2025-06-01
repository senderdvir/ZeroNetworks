CREATE TABLE IF NOT EXISTS public.launches_raw_data
(
    id TEXT PRIMARY KEY,
    name TEXT,
    date_utc TIMESTAMP,
    rocket TEXT,
    success BOOLEAN,
    flight_number BIGINT,
    payloads TEXT,
    launchpad TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);