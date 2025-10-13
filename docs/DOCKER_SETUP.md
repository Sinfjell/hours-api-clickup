# Docker Setup & Testing Guide

This guide will help you test the Docker setup locally before deploying to Cloud Run.

## Prerequisites

1. **Docker Desktop** installed and running
   - Download from: https://www.docker.com/products/docker-desktop
   - Make sure Docker daemon is running (check Docker Desktop app)

2. **Google Cloud CLI** installed
   - Download from: https://cloud.google.com/sdk/docs/install

3. **Authenticated with Google Cloud**
   ```bash
   gcloud auth login
   gcloud auth application-default login
   gcloud config set project nettsmed-internal
   ```

## Local Testing Steps

### Step 1: Start Docker Desktop

Make sure Docker Desktop is running before proceeding.

### Step 2: Build the Docker Image

```bash
docker build -t clickup-bigquery-pipeline:latest .
```

This should take 2-3 minutes to complete.

### Step 3: Test the Container Locally

#### Option A: Test with your .env file
```bash
docker run --env-file .env -p 8080:8080 clickup-bigquery-pipeline:latest
```

#### Option B: Test with environment variables
```bash
docker run \
  -e CLICKUP_TOKEN=55424762_44fc0fcb470696596dc4894e3aa4fd17a265af257f6048373acb9e1e877e7f8b \
  -e TEAM_ID=37496228 \
  -e ASSIGNEES=55424762,55427758,88552909 \
  -e PROJECT_ID=nettsmed-internal \
  -e DATASET=clickup_data \
  -e STAGING_TABLE=staging_time_entries \
  -e FACT_TABLE=fact_time_entries \
  -p 8080:8080 \
  clickup-bigquery-pipeline:latest
```

### Step 4: Test the Endpoints

Open a new terminal and run these tests:

#### Test health endpoint:
```bash
curl http://localhost:8080/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "clickup-bigquery-sync",
  "version": "2.0.0"
}
```

#### Test root endpoint:
```bash
curl http://localhost:8080/
```

#### Test refresh sync:
```bash
curl -X POST http://localhost:8080/sync/refresh
```

#### Test full reindex (only if you want to test with all data):
```bash
curl -X POST http://localhost:8080/sync/full_reindex
```

### Step 5: Check Logs

The container will output logs in real-time. You should see:
- ClickUp API calls
- Data transformation progress
- BigQuery upload status
- MERGE operation completion

### Step 6: Verify in BigQuery

After a successful run, check BigQuery:

```bash
# Using bq command-line tool
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) as total_entries,
          MIN(start_date_oslo) as earliest_date,
          MAX(start_date_oslo) as latest_date
   FROM `nettsmed-internal.clickup_data.fact_time_entries`'
```

Or visit BigQuery Console:
https://console.cloud.google.com/bigquery?project=nettsmed-internal

## Troubleshooting

### Issue: "Cannot connect to Docker daemon"
**Solution**: Start Docker Desktop application

### Issue: "Permission denied"
**Solution**: Make sure Docker Desktop has proper permissions

### Issue: "Authentication error" when accessing BigQuery
**Solution**: Run `gcloud auth application-default login` again

### Issue: "Port 8080 already in use"
**Solution**: Use a different port:
```bash
docker run -p 9090:8080 ... clickup-bigquery-pipeline:latest
curl http://localhost:9090/health
```

## Next Steps

Once local testing is successful:

1. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: Add Cloud Run deployment setup with Docker"
   ```

2. **Push to GitHub**
   ```bash
   git push origin feature/cloud-run-deployment
   ```

3. **Deploy to Cloud Run**
   ```bash
   ./deploy.sh
   ```

## Docker Commands Cheat Sheet

```bash
# Build image
docker build -t clickup-bigquery-pipeline:latest .

# Run container
docker run --env-file .env -p 8080:8080 clickup-bigquery-pipeline:latest

# Run container in background
docker run -d --env-file .env -p 8080:8080 clickup-bigquery-pipeline:latest

# View running containers
docker ps

# Stop container
docker stop <container_id>

# View logs
docker logs <container_id>

# Remove container
docker rm <container_id>

# Remove image
docker rmi clickup-bigquery-pipeline:latest

# Clean up all stopped containers
docker container prune
```

## Cost Estimate

**Local Testing**: Free (uses your local machine)

**Cloud Run Deployment**:
- Build: $0.05 per build
- Storage: $0.10/month (Artifact Registry)
- Runtime: $0.30-0.50/month
- Scheduler: $0.50/month
- **Total**: ~$1.00/month
