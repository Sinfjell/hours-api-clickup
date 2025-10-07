-- SQL to create the ClickUp Tasks dimension table in BigQuery
-- This table stores ALL tasks (open, closed, archived, subtasks)

-- Note: The Python code will automatically create this table,
-- but if you prefer to create it manually in the BigQuery UI, use this SQL:

CREATE TABLE IF NOT EXISTS `nettsmed-internal.clickup_data.dim_tasks` (
  space_id STRING NOT NULL,
  space_name STRING NOT NULL,
  folder_id STRING,
  folder_name STRING,
  list_id STRING NOT NULL,
  list_name STRING NOT NULL,
  task_id STRING NOT NULL,
  task_name STRING,
  status STRING,
  time_estimate_hrs FLOAT64,
  url STRING,
  closed BOOLEAN,
  archived BOOLEAN
)
OPTIONS(
  description="ClickUp Tasks dimension table with all tasks (open, closed, archived, subtasks)"
);

-- Example query to view the data:
-- SELECT * FROM `nettsmed-internal.clickup_data.dim_tasks` ORDER BY space_name, folder_name, list_name, task_name;

-- Example query to see only open tasks:
-- SELECT 
--   space_name,
--   folder_name,
--   list_name,
--   task_name,
--   status,
--   time_estimate_hrs,
--   url
-- FROM `nettsmed-internal.clickup_data.dim_tasks`
-- WHERE closed = FALSE AND archived = FALSE
-- ORDER BY space_name, list_name, task_name;

-- Example query to join with time entries:
-- SELECT 
--   t.start_date_oslo,
--   t.duration_hours,
--   t.task_name as time_entry_task,
--   tasks.task_name as task_details,
--   tasks.status,
--   tasks.time_estimate_hrs,
--   tasks.closed,
--   tasks.archived
-- FROM `nettsmed-internal.clickup_data.fact_time_entries` t
-- LEFT JOIN `nettsmed-internal.clickup_data.dim_tasks` tasks
--   ON t.task_id = tasks.task_id
-- WHERE t.start_date_oslo >= '2024-01-01'
-- ORDER BY t.start_date_oslo DESC;

-- Example: Summary of tasks by status:
-- SELECT 
--   space_name,
--   status,
--   COUNT(*) as task_count,
--   SUM(time_estimate_hrs) as total_estimated_hours,
--   SUM(CASE WHEN closed = TRUE THEN 1 ELSE 0 END) as closed_count,
--   SUM(CASE WHEN archived = TRUE THEN 1 ELSE 0 END) as archived_count
-- FROM `nettsmed-internal.clickup_data.dim_tasks`
-- GROUP BY space_name, status
-- ORDER BY space_name, status;

