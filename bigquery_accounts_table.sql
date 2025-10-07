-- SQL to create the ClickUp Accounts dimension table in BigQuery
-- This table stores Account tasks with custom fields

-- Note: The Python code will automatically create this table,
-- but if you prefer to create it manually in the BigQuery UI, use this SQL:

CREATE TABLE IF NOT EXISTS `nettsmed-internal.clickup_data.dim_accounts` (
  account_task_id STRING NOT NULL,
  account_name STRING,
  connected_list_id STRING,
  hours_discount FLOAT64,
  status STRING,
  date_created TIMESTAMP,
  assignees STRING,
  arr FLOAT64
)
OPTIONS(
  description="ClickUp Accounts dimension table with custom fields (Connected List IDs, Hours Discount, ARR)"
);

-- Note: One account task can create multiple rows (one per connected list ID)

-- Example query to view the data:
-- SELECT * FROM `nettsmed-internal.clickup_data.dim_accounts` ORDER BY account_name;

-- Example query to see accounts with their connected lists:
-- SELECT 
--   account_task_id,
--   account_name,
--   connected_list_id,
--   hours_discount,
--   arr,
--   status
-- FROM `nettsmed-internal.clickup_data.dim_accounts`
-- WHERE connected_list_id != ''
-- ORDER BY account_name, connected_list_id;

-- Example: Join accounts with lists to get list names:
-- SELECT 
--   a.account_task_id,
--   a.account_name,
--   a.connected_list_id,
--   l.list_name as connected_list_name,
--   a.hours_discount,
--   a.arr,
--   a.status
-- FROM `nettsmed-internal.clickup_data.dim_accounts` a
-- LEFT JOIN `nettsmed-internal.clickup_data.dim_lists` l
--   ON a.connected_list_id = l.list_id
-- ORDER BY a.account_name;

-- Example: Join accounts with time entries via connected lists:
-- SELECT 
--   a.account_name,
--   a.hours_discount,
--   a.arr,
--   SUM(t.duration_hours) as total_hours_logged,
--   SUM(t.duration_hours * (1 - a.hours_discount/100)) as billable_hours
-- FROM `nettsmed-internal.clickup_data.fact_time_entries` t
-- JOIN `nettsmed-internal.clickup_data.dim_accounts` a
--   ON t.task_location_list_id = a.connected_list_id
-- WHERE t.start_date_oslo >= '2024-01-01'
--   AND a.connected_list_id != ''
-- GROUP BY a.account_name, a.hours_discount, a.arr
-- ORDER BY total_hours_logged DESC;

-- Example: Account summary with ARR and hours:
-- SELECT 
--   a.account_name,
--   a.status,
--   a.hours_discount,
--   a.arr,
--   COUNT(DISTINCT a.connected_list_id) as connected_lists_count,
--   STRING_AGG(DISTINCT l.list_name, ', ') as connected_list_names
-- FROM `nettsmed-internal.clickup_data.dim_accounts` a
-- LEFT JOIN `nettsmed-internal.clickup_data.dim_lists` l
--   ON a.connected_list_id = l.list_id
-- WHERE a.connected_list_id != ''
-- GROUP BY a.account_name, a.status, a.hours_discount, a.arr
-- ORDER BY a.arr DESC NULLS LAST;

