# Bug Fix: Incomplete MERGE in Full Reindex Mode

## Problem

Full backfill (full_reindex mode) was not inserting new time entries into BigQuery fact table.

### Symptoms
- Muhammad Umer had **373 entries** in local CSV after backfill
- BigQuery fact table only had **345 entries**
- **Missing 28 entries** from August 15 - October 13, 2025
- Last entry in BigQuery: August 14, 2025
- Last entry in CSV: October 13, 2025

## Root Cause

The `MERGE` statement in `merge_full_reindex_mode()` used `INSERT ROW` syntax:

```sql
WHEN NOT MATCHED THEN
  INSERT ROW
```

This syntax was not properly inserting new records from the staging table into the fact table.

## Solution

Changed to explicit `INSERT` with column names and `VALUES`:

```sql
WHEN NOT MATCHED THEN
  INSERT (
    id, start_utc, end_utc, duration_ms, duration_hours, billable, description,
    source, `at`, is_locked, approval_id, task_url, task_id, task_name,
    task_custom_type, task_custom_id, task_status_status, task_status_color,
    task_status_type, task_status_orderindex, user_id, user_username, user_email,
    user_email_sha256, user_color, user_initials, user_profilePicture,
    task_location_list_id, task_location_folder_id, task_location_space_id,
    start_date_oslo
  )
  VALUES (
    S.id, S.start_utc, S.end_utc, S.duration_ms, S.duration_hours, S.billable,
    S.description, S.source, S.`at`, S.is_locked, S.approval_id, S.task_url,
    S.task_id, S.task_name, S.task_custom_type, S.task_custom_id,
    S.task_status_status, S.task_status_color, S.task_status_type,
    S.task_status_orderindex, S.user_id, S.user_username, S.user_email,
    S.user_email_sha256, S.user_color, S.user_initials, S.user_profilePicture,
    S.task_location_list_id, S.task_location_folder_id, S.task_location_space_id,
    S.start_date_oslo
  )
```

## Testing Results

### Before Fix
```
Muhammad Umer in BigQuery: 345 entries
Last entry: 2025-08-14
Missing entries: 28 (from Aug 15 - Oct 13, 2025)
```

### After Fix
```
Muhammad Umer in BigQuery: 373 entries ✓
Date range: 2024-06-14 to 2025-10-13 ✓
All entries present: Yes ✓
```

### Integration Test
1. **Full Backfill**: Successfully inserted all 1,899 entries
2. **60-Day Refresh**: Successfully updated recent entries without deleting historical data
3. **Data Integrity**: All 373 entries for Muhammad Umer preserved after refresh

## Files Changed

- `fetch_clickup_data.py` - Line 1372-1391: Updated `merge_full_reindex_mode()` method

## Impact

- ✅ Fixes data completeness issue for all employees
- ✅ Ensures backfills properly populate BigQuery with all time entries
- ✅ Both sync modes (full_reindex and refresh) now work correctly together
- ✅ No data loss when running sequential syncs
- ✅ No breaking changes to existing functionality

## Deployment

After merging this fix:
1. Deploy to Cloud Run using `./deploy.sh`
2. Optionally trigger a manual full reindex to ensure all historical data is complete
3. Normal 6-hour refresh syncs will continue to work as expected

