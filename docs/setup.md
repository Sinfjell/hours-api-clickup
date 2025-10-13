# Setup and Handover — ClickUp to BigQuery Sync

Last updated: 2025-10-07

## ✅ Quick Start

The service is already deployed and running in production on Google Cloud Run.

**Service URL**: `https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app`

### For New Deployments

1. Clone the repository:
```bash
git clone https://github.com/Sinfjell/hours-api-clickup.git
cd hours-api-clickup
```

2. Deploy using the automated script:
```bash
chmod +x deploy.sh
./deploy.sh
```

The script will:
- Enable required Google Cloud APIs
- Create Artifact Registry repository
- Build and push Docker image
- Deploy to Cloud Run (europe-north1)
- Create Cloud Scheduler jobs (europe-west1)
- Configure service accounts and IAM permissions

## 🔑 Dependencies

### Infrastructure
- **Google Cloud Project**: `nettsmed-internal`
- **Python**: 3.13+
- **Docker**: For containerization
- **Google Cloud SDK**: For deployment

### Environment Variables (in Cloud Run)
- `CLICKUP_TOKEN`: ClickUp API token
- `TEAM_ID`: ClickUp team ID (37496228)
- `ASSIGNEES`: Comma-separated user IDs (55424762,55427758,88552909)
- `PROJECT_ID`: GCP project ID (nettsmed-internal)
- `DATASET`: BigQuery dataset (clickup_data)
- `STAGING_TABLE`: BigQuery staging table (staging_time_entries)
- `FACT_TABLE`: BigQuery fact table (fact_time_entries)

### Required GCP APIs
- Cloud Run API
- Cloud Build API
- Cloud Scheduler API
- BigQuery API
- Artifact Registry API

## 🔍 Verify

### Health Check
```bash
# With authentication
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app/health
```

Expected output:
```json
{
  "service": "clickup-bigquery-sync",
  "status": "healthy",
  "version": "2.0.0"
}
```

### Manual Trigger
```bash
# Trigger refresh sync
gcloud scheduler jobs run clickup-refresh-6h --location=europe-west1

# Check logs
gcloud run services logs read clickup-bigquery-sync --region=europe-north1 --limit=50
```

### Verify Schedulers
```bash
gcloud scheduler jobs list --location=europe-west1
```

Expected: Two jobs (ENABLED status):
- `clickup-refresh-6h` - Every 6 hours
- `clickup-full-reindex-quarterly` - Quarterly at 2 AM

### Verify BigQuery Data
```sql
SELECT 
    COUNT(*) as total_entries,
    MIN(start_date_oslo) as earliest_date,
    MAX(start_date_oslo) as latest_date,
    COUNT(DISTINCT user_id) as unique_users
FROM `nettsmed-internal.clickup_data.fact_time_entries`;
```

## ⬆️ Updates

### Deploying Code Changes

1. Make changes to the code
2. Commit and push to git
3. Run deployment script:
```bash
./deploy.sh
```

The Cloud Build will automatically:
- Build new Docker image with version tag
- Deploy to Cloud Run
- Update service with zero downtime

### Rollback

If needed, rollback to previous revision:
```bash
# List revisions
gcloud run revisions list --service=clickup-bigquery-sync --region=europe-north1

# Rollback to specific revision
gcloud run services update-traffic clickup-bigquery-sync \
  --to-revisions=REVISION_NAME=100 \
  --region=europe-north1
```

### Region Migration

If you need to migrate regions again:
1. Update `REGION` and `SCHEDULER_REGION` in `deploy.sh`
2. Update references in `cloudbuild.yaml`
3. Run `./deploy.sh`
4. Delete old resources:
```bash
gcloud run services delete clickup-bigquery-sync --region=OLD_REGION --quiet
gcloud scheduler jobs delete JOB_NAME --location=OLD_LOCATION --quiet
```

## 🚨 Troubleshooting

### Service Not Responding
- Check logs: `gcloud run services logs read clickup-bigquery-sync --region=europe-north1 --limit=100`
- Verify service is running: `gcloud run services describe clickup-bigquery-sync --region=europe-north1`

### Scheduler Not Triggering
- Check scheduler status: `gcloud scheduler jobs list --location=europe-west1`
- Verify IAM permissions: Service account must have `roles/run.invoker`
- Test manually: `gcloud scheduler jobs run clickup-refresh-6h --location=europe-west1`

### BigQuery Errors
- Verify dataset exists: `bq ls --project_id=nettsmed-internal`
- Check IAM permissions: Cloud Run service account needs BigQuery Data Editor role
- Review logs for detailed error messages
