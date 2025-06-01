SELECT 
    year,
    total_launches,
    successful_launches,
    ROUND(successful_launches / NULLIF(total_launches, 0), 4) AS success_rate
FROM 
    postgres.public.aggregated_data
WHERE 
    is_current = TRUE
ORDER BY 
    year