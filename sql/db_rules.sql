CREATE RULE no_update AS
ON UPDATE TO launches_raw_data DO INSTEAD NOTHING;

CREATE RULE no_delete AS
ON DELETE TO launches_raw_data DO INSTEAD NOTHING;