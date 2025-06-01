CREATE TABLE IF NOT EXISTS public.payloads_raw_data
(
    id TEXT ,
    name TEXT,    
    launch TEXT,
    mass_kg NUMERIC,
    mass_lbs NUMERIC,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
