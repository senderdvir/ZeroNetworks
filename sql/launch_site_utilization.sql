SELECT
    lp.name AS launch_site,
    COUNT(DISTINCT l.id) AS total_launches,
    AVG(p.mass_kg) AS avg_payload_mass_kg
FROM
    postgres.public.launches_raw_data l
    JOIN
        postgres.public.launchpad_raw_data lp ON l.launchpad = lp.id
    JOIN
        postgres.public.payloads_raw_data p ON p.launch = l.id
GROUP BY
    lp.name
ORDER BY
    total_launches DESC