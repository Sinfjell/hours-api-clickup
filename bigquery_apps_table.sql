-- SQL to create the ClickUp Applications dimension table in BigQuery
-- This table stores Application tasks (custom_item_id 1005) from team level

CREATE TABLE IF NOT EXISTS `nettsmed-internal.clickup_data.dim_apps` (
  task_id STRING NOT NULL,
  application_name STRING,
  account_task_ids STRING,
  arr FLOAT64,
  last_updated TIMESTAMP,
  status STRING,
  maintenance BOOLEAN
)
OPTIONS(
  description="ClickUp Applications dimension table with custom fields (ARR, Last Updated, Maintenance, Account relationships)"
);

-- Example query to view the data:
-- SELECT * FROM `nettsmed-internal.clickup_data.dim_apps` ORDER BY application_name;

-- Example: Apps with their account relationships:
-- SELECT 
--   task_id,
--   application_name,
--   account_task_ids,
--   arr,
--   last_updated,
--   status,
--   maintenance
-- FROM `nettsmed-internal.clickup_data.dim_apps`
-- ORDER BY application_name;

-- Example: Join apps with accounts:
-- SELECT 
--   app.task_id as app_task_id,
--   app.application_name,
--   app.arr as app_arr,
--   app.maintenance,
--   acc.account_task_id,
--   acc.account_name,
--   acc.arr as account_arr
-- FROM `nettsmed-internal.clickup_data.dim_apps` app
-- CROSS JOIN UNNEST(SPLIT(app.account_task_ids, ', ')) as account_id
-- LEFT JOIN `nettsmed-internal.clickup_data.dim_accounts` acc
--   ON acc.account_task_id = account_id
-- WHERE app.account_task_ids != ''
-- ORDER BY app.application_name;

-- Example: Apps with maintenance status:
-- SELECT 
--   application_name,
--   status,
--   maintenance,
--   arr,
--   last_updated
-- FROM `nettsmed-internal.clickup_data.dim_apps`
-- WHERE maintenance = TRUE
-- ORDER BY arr DESC NULLS LAST;

-- Example: Total ARR by maintenance status:
-- SELECT 
--   maintenance,
--   COUNT(*) as app_count,
--   SUM(arr) as total_arr,
--   AVG(arr) as avg_arr
-- FROM `nettsmed-internal.clickup_data.dim_apps`
-- WHERE arr IS NOT NULL
-- GROUP BY maintenance
-- ORDER BY maintenance DESC;

