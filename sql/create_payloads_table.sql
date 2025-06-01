CREATE TABLE IF NOT EXISTS public.payloads_raw_data
(
    id TEXT PRIMARY KEY,
    name TEXT,    
    launch TEXT,
    mass_kg BIGINT,
    mass_lbs BIGINT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
