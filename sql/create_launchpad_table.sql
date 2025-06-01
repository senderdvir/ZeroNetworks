CREATE TABLE IF NOT EXISTS public.launchpad_raw_data
(   
    id TEXT PRIMARY KEY,
    name TEXT,
    full_name TEXT,
    region TEXT,
    timezone TEXT,
    launch_attempts BIGINT,
    launch_successes BIGINT,
    status TEXT
);
	