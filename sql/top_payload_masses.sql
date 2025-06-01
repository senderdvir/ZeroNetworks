WITH clean_data AS (
	SELECT
		
        lnc.name AS launch_name,
		SUM(pld.mass_kg) AS sum_of_mass
	FROM postgres.public.launches_raw_data AS lnc
		INNER JOIN postgres.public.payloads_raw_data AS pld
			ON lnc.id = pld.launch
	GROUP BY lnc.name)
SELECT * 
FROM clean_data
WHERE sum_of_mass IS NOT NULL
ORDER BY sum_of_mass DESC
LIMIT 5