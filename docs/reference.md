# Project Reference — ClickUp to BigQuery Sync
_Last updated: 2025-10-07_

## 🧩 Overview

Production-ready pipeline that syncs ClickUp time tracking data to Google BigQuery. Runs as a serverless Flask application on Google Cloud Run with automated scheduling via Cloud Scheduler.

**Key Components:**
- **Flask API**: HTTP endpoints for triggering sync operations
- **ClickUp API Integration**: Fetches time entries with rate limiting and retry logic
- **BigQuery Integration**: Loads data using staging tables and MERGE operations
- **Cloud Scheduler**: Automated triggers for regular syncs and quarterly reindexing

## ⚙️ Installation & Setup

### Local Development

1. Clone repository and create virtual environment:
```bash
git clone https://github.com/Sinfjell/hours-api-clickup.git
cd hours-api-clickup
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create `.env` file with credentials:
```env
CLICKUP_TOKEN=your_token_here
TEAM_ID=37496228
ASSIGNEES=55424762,55427758,88552909
PROJECT_ID=nettsmed-internal
DATASET=clickup_data
STAGING_TABLE=staging_time_entries
FACT_TABLE=fact_time_entries
```

4. Run locally:
```bash
# Test the data fetching script
python fetch_clickup_data.py --mode refresh --days 7

# Run Flask API locally
python main.py
```

### Production Deployment

See [setup.md](setup.md) for full deployment instructions.

## 🧠 Key Components

### ClickUpDataFetcher
**Purpose**: Fetches time entries from ClickUp API
- Respects 30-day window limitations
- Implements exponential backoff for rate limits
- Returns normalized data ready for transformation

### DataTransformer
**Purpose**: Transforms ClickUp data to BigQuery schema
- Converts timestamps to Oslo timezone
- Calculates durations in hours and milliseconds
- Hashes sensitive user data (emails)
- Deduplicates entries by ID

### BigQueryManager
**Purpose**: Manages BigQuery operations
- Creates/manages datasets and tables
- Uploads data to staging tables
- Executes MERGE operations (refresh or full reindex)

## 🌐 API Endpoints

**Base URL:** `https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app`

| Endpoint | Method | Description | Auth | Response |
|----------|--------|-------------|------|----------|
| `/` | GET | Service information | None | JSON with service details |
| `/health` | GET | Health check | OIDC | JSON with status |
| `/sync/refresh` | POST | Sync last 60 days | OIDC | JSON with sync results |
| `/sync/full_reindex` | POST | Full reindex since 2024 | OIDC | JSON with reindex results |

### Authentication

All endpoints except `/` require OIDC authentication via Cloud Scheduler service account.

**Manual Testing:**
```bash
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app/health
```

### Response Examples

**Health Check** (`GET /health`):
```json
{
  "status": "healthy",
  "service": "clickup-bigquery-sync",
  "version": "2.0.0"
}
```

**Refresh Sync** (`POST /sync/refresh`):
```json
{
  "status": "success",
  "mode": "refresh",
  "days": 60,
  "message": "ClickUp refresh sync completed successfully"
}
```

**Error Response**:
```json
{
  "status": "error",
  "mode": "refresh",
  "error": "Error message details"
}
```

### External Integrations

- **ClickUp API**: Time tracking data - Bearer token authentication
- **Google BigQuery**: Data warehouse - ADC authentication
- **Cloud Scheduler**: Automated triggers - OIDC tokens

## 🗄️ Data Schema

### BigQuery Tables

#### Staging Table: `staging_time_entries`
Temporary table for incoming data before MERGE.

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | ClickUp time entry ID (Primary Key) |
| `task_id` | STRING | ClickUp task ID |
| `task_name` | STRING | Task name |
| `user_id` | INTEGER | ClickUp user ID |
| `user_username` | STRING | Username |
| `user_email_hash` | STRING | SHA256 hash of email |
| `start` | INTEGER | Start timestamp (ms) |
| `end` | INTEGER | End timestamp (ms) |
| `time` | INTEGER | Duration in milliseconds |
| `source` | STRING | Entry source |
| `at` | INTEGER | Creation timestamp |
| `task_url` | STRING | ClickUp task URL |
| `start_date_oslo` | DATE | Start date in Oslo TZ |
| `start_datetime_oslo` | TIMESTAMP | Start datetime in Oslo TZ |
| `end_datetime_oslo` | TIMESTAMP | End datetime in Oslo TZ |
| `duration_hours` | FLOAT | Duration in hours |

#### Fact Table: `fact_time_entries`
Production table with all historical data.

Same schema as staging table. Updated via MERGE operations:
- **Refresh mode**: Updates/inserts only last 60 days
- **Full reindex mode**: Replaces all data since 2024

### Environment Variables

| Variable | Type | Required | Description | Default |
|----------|------|----------|-------------|---------|
| `CLICKUP_TOKEN` | string | Yes | ClickUp API token | - |
| `TEAM_ID` | string | Yes | ClickUp team ID | - |
| `ASSIGNEES` | string | Yes | Comma-separated user IDs | - |
| `PROJECT_ID` | string | Yes | GCP project ID | - |
| `DATASET` | string | Yes | BigQuery dataset | - |
| `STAGING_TABLE` | string | Yes | Staging table name | - |
| `FACT_TABLE` | string | Yes | Fact table name | - |
| `PORT` | integer | No | Flask port | 8080 |

## 🪄 Design Notes

### Architecture Decisions

1. **Serverless Deployment**: Cloud Run chosen for:
   - Zero management overhead
   - Automatic scaling (0-1 instances)
   - Cost efficiency (~$1/month)
   - Built-in health checks and monitoring

2. **Two-Mode Operation**:
   - **Refresh Mode**: Daily syncs with 60-day lookback for efficiency
   - **Full Reindex Mode**: Quarterly validation to catch any missed data

3. **Staging + MERGE Pattern**:
   - Upload to staging table first
   - MERGE to fact table for atomic updates
   - Allows validation before committing data

4. **30-Day Chunking**:
   - ClickUp API has 30-day query window limit
   - Pipeline automatically chunks date ranges
   - Handles any time period gracefully

5. **Region Selection**:
   - Cloud Run: `europe-north1` (Finland - closest to Norway)
   - Scheduler: `europe-west1` (Belgium - Scheduler doesn't support europe-north1)
   - Data residency in Europe for compliance

### Known Limitations

- ClickUp API rate limit: 100 requests/minute
- Maximum Cloud Run timeout: 900 seconds (15 minutes)
- BigQuery MERGE operations lock the table briefly
- Scheduler minimum frequency: 1 minute (using 6 hours)

### Performance Characteristics

- **Refresh (60 days)**: ~12-30 seconds, 200-300 entries
- **Full reindex (2024-present)**: ~60-90 seconds, 1000-2000 entries
- **Cold start**: ~2-3 seconds for container warmup
- **Memory usage**: ~256MB typical, 1GB allocated

## 🧾 Changelog

See [changelog.md](changelog.md) for complete version history.

**Current Version: 2.0.0** (2025-10-07)
- Production deployment on Cloud Run
- Automated scheduling with Cloud Scheduler
- Europe region deployment (Finland/Belgium)
- Two-mode operation (refresh/full_reindex)
- Comprehensive error handling and logging

---

## 📊 Monitoring

### Cloud Console Links
- **Cloud Run**: https://console.cloud.google.com/run/detail/europe-north1/clickup-bigquery-sync?project=nettsmed-internal
- **Scheduler**: https://console.cloud.google.com/cloudscheduler?project=nettsmed-internal
- **BigQuery**: https://console.cloud.google.com/bigquery?project=nettsmed-internal&d=clickup_data

### Key Metrics to Monitor
- **Error Rate**: Should be 0% (no 4xx/5xx responses)
- **Execution Time**: Should be under 60 seconds for refresh
- **Data Freshness**: Check latest `start_date_oslo` in BigQuery
- **Scheduler Status**: Both jobs should show ENABLED

---

**Purpose:** Complete technical reference for developers maintaining or extending the pipeline.
