#!/bin/bash

# Script to create just the lists scheduler without full redeployment
# Useful for updating existing deployments

set -e

# Configuration
PROJECT_ID="nettsmed-internal"
SCHEDULER_REGION="europe-west1"
SERVICE_URL="https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app"

echo "🕐 Creating ClickUp Lists Daily Scheduler"
echo "=================================================="

# Set project
gcloud config set project $PROJECT_ID

# Create scheduler
echo "Creating lists sync scheduler (daily at 3 AM Oslo time)..."
gcloud scheduler jobs create http clickup-lists-sync-daily \
    --location=$SCHEDULER_REGION \
    --schedule="0 3 * * *" \
    --uri="${SERVICE_URL}/sync/lists" \
    --http-method=POST \
    --oidc-service-account-email=clickup-scheduler@${PROJECT_ID}.iam.gserviceaccount.com \
    --time-zone="Europe/Oslo" \
    --description="Sync ClickUp lists daily at 3 AM" \
    2>/dev/null || echo "✓ Scheduler already exists, updating..."

# If it already exists, update it
if [ $? -ne 0 ]; then
    echo "Updating existing scheduler..."
    gcloud scheduler jobs update http clickup-lists-sync-daily \
        --location=$SCHEDULER_REGION \
        --schedule="0 3 * * *" \
        --uri="${SERVICE_URL}/sync/lists" \
        --http-method=POST \
        --oidc-service-account-email=clickup-scheduler@${PROJECT_ID}.iam.gserviceaccount.com \
        --time-zone="Europe/Oslo" \
        --description="Sync ClickUp lists daily at 3 AM"
fi

echo ""
echo "✅ Lists scheduler created/updated!"
echo ""
echo "Scheduler details:"
echo "  - Name: clickup-lists-sync-daily"
echo "  - Schedule: Daily at 3:00 AM Oslo time (0 3 * * *)"
echo "  - Endpoint: ${SERVICE_URL}/sync/lists"
echo "  - Region: $SCHEDULER_REGION"
echo ""
echo "📊 Test the scheduler:"
echo "  gcloud scheduler jobs run clickup-lists-sync-daily --location=$SCHEDULER_REGION"
echo ""
echo "📋 List all schedulers:"
echo "  gcloud scheduler jobs list --location=$SCHEDULER_REGION"
echo ""
echo "📝 View logs:"
echo "  gcloud run services logs read clickup-bigquery-sync --region=europe-north1 --limit=50"

