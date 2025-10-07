-- SQL to create the ClickUp Lists dimension table in BigQuery
-- This table stores the Space → Folder → List hierarchy

-- Note: The Python code will automatically create this table,
-- but if you prefer to create it manually in the BigQuery UI, use this SQL:

CREATE TABLE IF NOT EXISTS `nettsmed-internal.clickup_data.dim_lists` (
  space_id STRING NOT NULL,
  space_name STRING NOT NULL,
  folder_id STRING,
  folder_name STRING,
  list_id STRING NOT NULL,
  list_name STRING NOT NULL
)
OPTIONS(
  description="ClickUp Lists dimension table with Space → Folder → List hierarchy"
);

-- Example query to view the data:
-- SELECT * FROM `nettsmed-internal.clickup_data.dim_lists` ORDER BY space_name, folder_name, list_name;

-- Example query to join with time entries:
-- SELECT 
--   t.id,
--   t.start_date_oslo,
--   t.duration_hours,
--   l.space_name,
--   l.folder_name,
--   l.list_name
-- FROM `nettsmed-internal.clickup_data.fact_time_entries` t
-- LEFT JOIN `nettsmed-internal.clickup_data.dim_lists` l
--   ON t.task_location_list_id = l.list_id
-- WHERE t.start_date_oslo >= '2024-01-01'
-- ORDER BY t.start_date_oslo DESC;

