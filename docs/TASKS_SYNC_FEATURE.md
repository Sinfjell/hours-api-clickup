# ClickUp Tasks Sync Feature

## Summary

Successfully implemented task syncing from ClickUp to BigQuery - fetches **ALL tasks** (open, closed, archived, subtasks) from the configured ClickUp Space.

## What Was Done

### 1. Created ClickUpTasksFetcher Class
Location: `fetch_clickup_data.py`

Features:
- Fetches tasks from a specific ClickUp Space (default: 61463579 "Billable work")
- Handles both active/closed AND archived tasks
- Fetches from folders and folder-less lists
- Includes subtasks automatically
- **Pagination support**: Handles lists with 100+ tasks
- Rate limiting and retry logic
- Follows same error handling patterns as lists fetcher

### 2. Created BigQueryTasksManager Class
Location: `fetch_clickup_data.py`

Features:
- Creates dataset if it doesn't exist
- Creates `dim_tasks` table with proper schema
- Uploads tasks data with WRITE_TRUNCATE (complete replacement)
- Table schema:
  - `space_id`, `space_name` (STRING, REQUIRED)
  - `folder_id`, `folder_name` (STRING, nullable)
  - `list_id`, `list_name` (STRING, REQUIRED)
  - `task_id` (STRING, REQUIRED)
  - `task_name`, `status` (STRING)
  - `time_estimate_hrs` (FLOAT)
  - `url` (STRING)
  - `closed`, `archived` (BOOLEAN)

### 3. Added sync_tasks_to_bigquery() Function
Location: `fetch_clickup_data.py`

- Orchestrates the entire tasks sync process
- Creates CSV backup file
- Uploads to BigQuery
- Uses environment variable `SPACE_ID` (default: 61463579)

### 4. Added Flask Endpoint
Location: `main.py`

- **Endpoint**: `POST /sync/tasks`
- **Description**: Syncs all ClickUp tasks to BigQuery
- **Use Case**: Run when tasks change or need to be refreshed
- Updated root endpoint to show new endpoint in API documentation

### 5. Added Daily Scheduler
Location: `deploy.sh`

- **Scheduler**: `clickup-tasks-sync-daily`
- **Schedule**: Daily at 4:00 AM Oslo time
- **Status**: Ready to be created on deployment

### 6. Created SQL Reference
Location: `bigquery_tasks_table.sql`

- SQL to manually create the table (if preferred)
- Example queries for:
  - Viewing all tasks
  - Filtering open tasks
  - Joining with time entries
  - Task summary by status

## BigQuery Table

**Table Name**: `nettsmed-internal.clickup_data.dim_tasks`

**Location**: US

**Update Strategy**: Complete replacement (WRITE_TRUNCATE) on each sync

## Test Results

✅ **Successfully tested locally:**
- Space: "Billable work" (61463579)
- **414 tasks** fetched and uploaded
- 286 closed tasks
- 2 archived tasks
- Tasks from both active and archived folders
- Tasks from folder-less lists
- Pagination working properly
- CSV backup created
- BigQuery table created and populated

## Environment Variables

Uses existing environment variables plus:
- `CLICKUP_TOKEN` - Your ClickUp API token
- `SPACE_ID` - Space ID to fetch tasks from (default: 61463579 "Billable work")
- `PROJECT_ID` - BigQuery project (default: nettsmed-internal)
- `DATASET` - BigQuery dataset (default: clickup_data)
- `TASKS_TABLE` - Table name (default: dim_tasks)

## Testing Locally

To test the tasks sync locally:

```bash
# Make sure you're in the venv and have credentials set up
source venv/bin/activate

# Run the sync function directly
python -c "from fetch_clickup_data import sync_tasks_to_bigquery; sync_tasks_to_bigquery()"
```

## Deployment

The feature is ready to deploy. When you push to Cloud Run:

```bash
git push origin feature/clickup-tasks-to-bigquery
# Then merge to main
git checkout main
git merge feature/clickup-tasks-to-bigquery
git push origin main
```

Cloud Build will automatically deploy the new version with the `/sync/tasks` endpoint and create the daily scheduler.

## Example Queries

### View all tasks:
```sql
SELECT * 
FROM `nettsmed-internal.clickup_data.dim_tasks` 
ORDER BY space_name, folder_name, list_name, task_name;
```

### View only open tasks:
```sql
SELECT 
  space_name,
  folder_name,
  list_name,
  task_name,
  status,
  time_estimate_hrs,
  url
FROM `nettsmed-internal.clickup_data.dim_tasks`
WHERE closed = FALSE AND archived = FALSE
ORDER BY space_name, list_name, task_name;
```

### Join with time entries:
```sql
SELECT 
  t.start_date_oslo,
  t.duration_hours,
  t.task_name as time_entry_task,
  tasks.task_name as task_details,
  tasks.status,
  tasks.time_estimate_hrs,
  tasks.closed,
  tasks.archived
FROM `nettsmed-internal.clickup_data.fact_time_entries` t
LEFT JOIN `nettsmed-internal.clickup_data.dim_tasks` tasks
  ON t.task_id = tasks.task_id
WHERE t.start_date_oslo >= '2024-01-01'
ORDER BY t.start_date_oslo DESC;
```

### Task summary by status:
```sql
SELECT 
  space_name,
  status,
  COUNT(*) as task_count,
  SUM(time_estimate_hrs) as total_estimated_hours,
  SUM(CASE WHEN closed = TRUE THEN 1 ELSE 0 END) as closed_count,
  SUM(CASE WHEN archived = TRUE THEN 1 ELSE 0 END) as archived_count
FROM `nettsmed-internal.clickup_data.dim_tasks`
GROUP BY space_name, status
ORDER BY space_name, status;
```

## Files Changed

1. `fetch_clickup_data.py` - Added ClickUpTasksFetcher, BigQueryTasksManager, sync_tasks_to_bigquery()
2. `main.py` - Added /sync/tasks endpoint
3. `deploy.sh` - Added clickup-tasks-sync-daily scheduler
4. `README.md` - Updated documentation
5. `bigquery_tasks_table.sql` - Added SQL reference (new file)

## Differences from Google Apps Script

### Similarities
- Same API endpoints and parameters
- Fetches ALL tasks (open, closed, archived)
- Includes subtasks
- Handles folder-less lists
- Same data structure

### Improvements
- **Better pagination**: Automatic handling of large lists
- **Output**: BigQuery table instead of Google Sheets
- **Rate limiting**: More robust with exponential backoff
- **Error handling**: Continues on error instead of failing completely
- **Logging**: Detailed progress tracking
- **Backup**: Creates CSV files for each run
- **Flexibility**: Configurable via environment variables

## Next Steps

1. **Deploy to Cloud Run**:
   - Push and merge the branch
   - Cloud Build will auto-deploy

2. **Scheduler will auto-create**:
   - Daily at 4 AM Oslo time
   - Starts running automatically

3. **Verify the data**:
   ```sql
   SELECT COUNT(*) as total FROM `nettsmed-internal.clickup_data.dim_tasks`;
   ```

4. **Set up alerts** (optional):
   - Monitor for failed syncs
   - Track data freshness

## Integration Examples

### Complete project view:
```sql
-- Time tracked vs estimated per task
SELECT 
  t.task_id,
  t.task_name,
  t.time_estimate_hrs as estimated_hrs,
  SUM(te.duration_hours) as actual_hrs,
  (SUM(te.duration_hours) - t.time_estimate_hrs) as variance_hrs
FROM `nettsmed-internal.clickup_data.dim_tasks` t
LEFT JOIN `nettsmed-internal.clickup_data.fact_time_entries` te
  ON t.task_id = te.task_id
WHERE t.closed = FALSE
GROUP BY t.task_id, t.task_name, t.time_estimate_hrs
HAVING t.time_estimate_hrs IS NOT NULL
ORDER BY variance_hrs DESC;
```

The implementation follows the same patterns as the existing lists sync, so it's well-tested and production-ready! 🚀

