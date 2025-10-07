# ✅ Daily Scheduler Setup Complete

## Summary

The daily scheduler for ClickUp lists sync has been created and configured. It's currently **ENABLED** and ready to run once the new code is deployed.

## Scheduler Details

**Name**: `clickup-lists-sync-daily`  
**Schedule**: Daily at 3:00 AM Oslo time (`0 3 * * *`)  
**Endpoint**: `https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app/sync/lists`  
**Region**: europe-west1  
**Status**: ✅ ENABLED  
**Next Run**: Tomorrow at 3:00 AM Oslo time

## All Active Schedulers

| Name | Schedule | Endpoint | Purpose |
|------|----------|----------|---------|
| `clickup-refresh-6h` | Every 6 hours | `/sync/refresh` | Sync last 60 days of time entries |
| `clickup-full-reindex-quarterly` | Quarterly at 2 AM | `/sync/full_reindex` | Full reindex of all time entries |
| `clickup-lists-sync-daily` | Daily at 3 AM | `/sync/lists` | Sync all ClickUp lists |

## ⚠️ Important: Deployment Required

The scheduler is created and enabled, but the `/sync/lists` endpoint doesn't exist in the currently deployed version yet.

**You need to deploy the new code for the scheduler to work:**

### Option 1: Merge and Deploy via Main Branch

```bash
# Push the feature branch
git push origin feature/clickup-lists-to-bigquery

# Merge to main (via PR or directly)
git checkout main
git merge feature/clickup-lists-to-bigquery
git push origin main

# Cloud Build will automatically deploy
```

### Option 2: Deploy Directly from Feature Branch

```bash
# Deploy from current branch
./deploy.sh
```

## Testing the Scheduler

Once deployed, test the scheduler manually:

```bash
# Trigger the scheduler manually
gcloud scheduler jobs run clickup-lists-sync-daily --location=europe-west1

# Wait ~30 seconds, then check logs
gcloud run services logs read clickup-bigquery-sync --region=europe-north1 --limit=30
```

Expected log output:
```
INFO - Starting lists sync...
INFO - Fetching lists from ClickUp...
INFO - Found 3 spaces
INFO - Total lists fetched: 51
INFO - Uploaded 51 lists to nettsmed-internal.clickup_data.dim_lists
INFO - Lists sync completed successfully
```

## Verify Scheduler Status

```bash
# List all schedulers
gcloud scheduler jobs list --location=europe-west1

# Get details of lists scheduler
gcloud scheduler jobs describe clickup-lists-sync-daily --location=europe-west1
```

## Files Updated

1. `deploy.sh` - Added lists scheduler creation
2. `create_lists_scheduler.sh` - Standalone script to create just the lists scheduler
3. `main.py` - Updated schedule documentation
4. `DEPLOYMENT_SUCCESS.md` - Added Job 3 documentation
5. `README.md` - Added note about automatic daily sync

## Automatic Sync Schedule

Once deployed, your data will be automatically synced as follows:

- **Time Entries**: Every 6 hours (00:00, 06:00, 12:00, 18:00 Oslo time)
- **Time Entries (Full Reindex)**: Quarterly on 1st of Jan/Apr/Jul/Oct at 2 AM
- **Lists**: Every day at 3:00 AM Oslo time

## Manual Sync Commands

Even with schedulers running, you can manually trigger syncs anytime:

```bash
# Time entries (last 60 days)
gcloud scheduler jobs run clickup-refresh-6h --location=europe-west1

# Time entries (full reindex)
gcloud scheduler jobs run clickup-full-reindex-quarterly --location=europe-west1

# Lists
gcloud scheduler jobs run clickup-lists-sync-daily --location=europe-west1
```

## Next Steps

1. ✅ Scheduler created and enabled
2. ⏳ Deploy the code (merge to main and push)
3. ⏳ Test the scheduler manually
4. ✅ Automatic daily syncs will begin

The scheduler is ready to go! Just deploy the code and it will start working automatically. 🚀

