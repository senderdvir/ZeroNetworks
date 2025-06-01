-- Step 1: Update current flags
UPDATE public.aggregated_data tgt
SET is_current = FALSE
WHERE is_current = TRUE
  AND year IN (
      SELECT EXTRACT(YEAR FROM lch.date_utc::timestamp)::TEXT
      FROM public.launches_raw_data AS lch
      WHERE date_trunc('hour', lch.inserted_at::timestamp) = (
          SELECT date_trunc('hour', MAX(inserted_at::timestamp))
          FROM public.launches_raw_data
      )
      GROUP BY EXTRACT(YEAR FROM lch.date_utc::timestamp)
  );

-- Step 2: Insert new current records
INSERT INTO public.aggregated_data (
    year,
    total_launches,
    successful_launches,
    avg_payload_mass,
    is_current,
    calculate_time
)
SELECT 
    EXTRACT(YEAR FROM lch.date_utc::timestamp)::TEXT AS year,
    COUNT(*) AS total_launches,
    COUNT(CASE WHEN success = true THEN 1 END) AS successful_launches,
    AVG(pld.mass_kg) AS avg_payload_mass,
    TRUE AS is_current,
    NOW() AS calculate_time
FROM public.launches_raw_data AS lch
INNER JOIN public.payloads_raw_data AS pld
    ON lch.id = pld.launch
WHERE date_trunc('hour', lch.inserted_at::timestamp) = (
    SELECT date_trunc('hour', MAX(inserted_at::timestamp))
    FROM public.launches_raw_data
)
GROUP BY EXTRACT(YEAR FROM lch.date_utc::timestamp);