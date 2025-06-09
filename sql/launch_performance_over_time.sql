SELECT 
    year,
    total_launches,
    successful_launches,
	  CAST(successful_launches AS DOUBLE) / CAST(total_launches AS DOUBLE) AS success_rate
FROM 
    postgres.public.aggregated_data
WHERE 
    is_current = TRUE
ORDER BY 
    year
