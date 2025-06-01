CREATE TABLE IF NOT EXISTS public.aggregated_data
(
    year TEXT,
    total_launches BIGINT,
    successful_launches BIGINT,
    avg_payload_mass NUMERIC,
    is_current BOOLEAN DEFAULT TRUE,
    calculate_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);