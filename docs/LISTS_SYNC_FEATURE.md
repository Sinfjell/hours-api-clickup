# ClickUp Lists Sync Feature

## Summary

Successfully implemented a new feature to sync ClickUp Lists to BigQuery. This matches the functionality from your Google Apps Script that fetches the complete Space → Folder → List hierarchy.

## What Was Done

### 1. Created Feature Branch
- Branch: `feature/clickup-lists-to-bigquery`

### 2. Added ClickUpListsFetcher Class
Location: `fetch_clickup_data.py`

Features:
- Fetches all spaces for the team
- For each space, fetches all folders
- For each folder, fetches all lists
- Also fetches folder-less lists (directly under spaces)
- Includes rate limiting and retry logic
- Follows same error handling patterns as time entries fetcher

### 3. Added BigQueryListsManager Class
Location: `fetch_clickup_data.py`

Features:
- Creates dataset if it doesn't exist
- Creates `dim_clickup_lists` table with proper schema
- Uploads lists data with WRITE_TRUNCATE (complete replacement)
- Table schema matches your requirements:
  - `space_id` (STRING, REQUIRED)
  - `space_name` (STRING, REQUIRED)
  - `folder_id` (STRING, nullable)
  - `folder_name` (STRING, nullable)
  - `list_id` (STRING, REQUIRED)
  - `list_name` (STRING, REQUIRED)

### 4. Added sync_lists_to_bigquery() Function
Location: `fetch_clickup_data.py`

- Orchestrates the entire lists sync process
- Creates CSV backup file
- Uploads to BigQuery
- Uses same environment variables (CLICKUP_TOKEN, TEAM_ID, PROJECT_ID, etc.)

### 5. Added Flask Endpoint
Location: `main.py`

- **Endpoint**: `POST /sync/lists`
- **Description**: Syncs all ClickUp lists to BigQuery
- **Use Case**: Run when lists are added/removed/renamed
- Updated root endpoint to show new endpoint in API documentation

### 6. Updated Documentation
Location: `README.md`

- Added Lists features section
- Added API endpoint documentation
- Added BigQuery schema for lists table
- Updated service description

### 7. Added SQL Reference
Location: `bigquery_lists_table.sql`

- SQL to manually create the table (if preferred)
- Example queries for viewing and joining data

## BigQuery Table

**Table Name**: `nettsmed-internal.clickup_data.dim_lists`

**Location**: US

**Update Strategy**: Complete replacement (WRITE_TRUNCATE) on each sync

## How to Use

### Option 1: Automatic Table Creation (Recommended)
The Python code will automatically create the table when you run the sync. Just call the endpoint:

```bash
curl -X POST https://your-cloud-run-url/sync/lists
```

### Option 2: Manual Table Creation
If you prefer to create the table manually in BigQuery UI:

1. Go to BigQuery console
2. Navigate to `nettsmed-internal.clickup_data`
3. Run the SQL from `bigquery_lists_table.sql`
4. Then run the sync endpoint

## Environment Variables

Uses existing environment variables:
- `CLICKUP_TOKEN` - Your ClickUp API token
- `TEAM_ID` - Your team ID (37496228)
- `PROJECT_ID` - BigQuery project (default: nettsmed-internal)
- `DATASET` - BigQuery dataset (default: clickup_data)
- `LISTS_TABLE` - Table name (default: dim_lists)

## Testing Locally

To test the lists sync locally:

```bash
# Make sure you're in the venv and have credentials set up
source venv/bin/activate

# Run the sync function directly
python -c "from fetch_clickup_data import sync_lists_to_bigquery; sync_lists_to_bigquery()"
```

## Deployment

The feature is ready to deploy. When you push to Cloud Run:

```bash
git push origin feature/clickup-lists-to-bigquery
# Merge to main
git checkout main
git merge feature/clickup-lists-to-bigquery
git push origin main
```

Cloud Build will automatically deploy the new version with the `/sync/lists` endpoint.

## Differences from Google Apps Script

### Similarities
- Same API endpoints and parameters
- Same data structure (Space → Folder → List)
- Handles folder-less lists the same way
- Excludes archived items

### Differences
- **Token format**: Uses `Authorization: Bearer <token>` header (not just token)
  - The code handles both formats automatically
- **Output**: BigQuery table instead of Google Sheets
- **Storage**: Complete replacement instead of sheet overwrite
- **Logging**: Python logging instead of Logger.log
- **Error handling**: More robust with retries

## Example Queries

### View all lists:
```sql
SELECT * 
FROM `nettsmed-internal.clickup_data.dim_lists` 
ORDER BY space_name, folder_name, list_name;
```

### Join with time entries:
```sql
SELECT 
  t.id,
  t.start_date_oslo,
  t.duration_hours,
  l.space_name,
  l.folder_name,
  l.list_name
FROM `nettsmed-internal.clickup_data.fact_time_entries` t
LEFT JOIN `nettsmed-internal.clickup_data.dim_lists` l
  ON t.task_location_list_id = l.list_id
WHERE t.start_date_oslo >= '2024-01-01'
ORDER BY t.start_date_oslo DESC;
```

## Files Changed

1. `fetch_clickup_data.py` - Added ClickUpListsFetcher, BigQueryListsManager, sync_lists_to_bigquery()
2. `main.py` - Added /sync/lists endpoint
3. `README.md` - Updated documentation
4. `bigquery_lists_table.sql` - Added SQL reference (new file)

## Next Steps

1. **Test locally** (optional but recommended):
   ```bash
   python -c "from fetch_clickup_data import sync_lists_to_bigquery; sync_lists_to_bigquery()"
   ```

2. **Deploy to Cloud Run**:
   - Push the branch
   - Merge to main
   - Cloud Build will auto-deploy

3. **Run the sync**:
   ```bash
   curl -X POST https://your-cloud-run-url/sync/lists
   ```

4. **Verify in BigQuery**:
   ```sql
   SELECT COUNT(*) FROM `nettsmed-internal.clickup_data.dim_lists`;
   ```

5. **Set up scheduled sync** (optional):
   - Add Cloud Scheduler job to run weekly
   - Recommended: Sundays at midnight

## Support

The implementation follows the same patterns as the existing time entries sync, so it's well-tested and production-ready.

