UPDATE public.aggregated_data tgt
SET is_current = FALSE
WHERE is_current = TRUE
  AND year IN (
      SELECT EXTRACT(YEAR FROM lch.date_utc)::TEXT
      FROM public.launches_raw_data AS lch
      WHERE date_trunc('hour', lch.inserted_at) = (
          SELECT date_trunc('hour', MAX(inserted_at))
          FROM public.launches_raw_data
      )
      GROUP BY EXTRACT(YEAR FROM lch.date_utc)
  );

-- Step 2: Insert new current records for the latest hour
INSERT INTO public.aggregated_data (
    year,
    total_launches,
    successful_launches,
    avg_payload_mass,
    is_current,
    calculate_time
)
SELECT 
    EXTRACT(YEAR FROM lch.date_utc)::TEXT AS year,
    COUNT(*) AS total_launches,
    COUNT(CASE WHEN success = true THEN 1 END) AS successful_launches,
    AVG(pld.mass_kg) AS avg_payload_mass,
    TRUE AS is_current,
    NOW() AS calculate_time
FROM public.launches_raw_data AS lch
INNER JOIN public.payloads_raw_data AS pld
    ON lch.id = pld.launch
WHERE date_trunc('hour', lch.inserted_at) = (
    SELECT date_trunc('hour', MAX(inserted_at))
    FROM public.launches_raw_data
)
GROUP BY EXTRACT(YEAR FROM lch.date_utc);