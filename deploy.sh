#!/bin/bash

set -e

# Configuration
PROJECT_ID="nettsmed-internal"
REGION="us-central1"
SERVICE_NAME="clickup-bigquery-sync"
REPOSITORY="clickup-pipeline"

echo "🚀 Deploying ClickUp to BigQuery Pipeline to Cloud Run"
echo "=================================================="

# Set project
echo "📋 Setting project: $PROJECT_ID"
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "📋 Enabling required APIs..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# Create Artifact Registry repository
echo "📦 Creating Artifact Registry repository..."
gcloud artifacts repositories create $REPOSITORY \
    --repository-format=docker \
    --location=$REGION \
    --description="ClickUp to BigQuery pipeline container images" \
    2>/dev/null || echo "✓ Repository already exists"

# Configure Docker authentication
echo "🔐 Configuring Docker authentication..."
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# Build and deploy using Cloud Build
echo "🔨 Building and deploying to Cloud Run..."
gcloud builds submit --config cloudbuild.yaml

# Get service URL
echo "🔗 Getting service URL..."
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
    --region=$REGION --format='value(status.url)')

echo "✅ Service deployed at: $SERVICE_URL"

# Create service account for scheduler
echo "👤 Creating service account for Cloud Scheduler..."
gcloud iam service-accounts create clickup-scheduler \
    --display-name="ClickUp Scheduler Service Account" \
    2>/dev/null || echo "✓ Service account already exists"

# Grant Cloud Run Invoker role
echo "🔑 Granting Cloud Run Invoker permissions..."
gcloud run services add-iam-policy-binding $SERVICE_NAME \
    --member="serviceAccount:clickup-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --region=$REGION

# Create schedulers
echo "⏰ Creating Cloud Scheduler jobs..."

# Scheduler 1: Every 6 hours (refresh 60 days)
echo "  Creating refresh scheduler (every 6 hours)..."
gcloud scheduler jobs create http clickup-refresh-6h \
    --location=$REGION \
    --schedule="0 */6 * * *" \
    --uri="${SERVICE_URL}/sync/refresh" \
    --http-method=POST \
    --oidc-service-account-email=clickup-scheduler@${PROJECT_ID}.iam.gserviceaccount.com \
    --time-zone="Europe/Oslo" \
    --description="Sync ClickUp time entries every 6 hours (60 days lookback)" \
    2>/dev/null || echo "  ✓ Refresh scheduler already exists"

# Scheduler 2: Quarterly (full reindex)
echo "  Creating full reindex scheduler (quarterly)..."
gcloud scheduler jobs create http clickup-full-reindex-quarterly \
    --location=$REGION \
    --schedule="0 2 1 */3 *" \
    --uri="${SERVICE_URL}/sync/full_reindex" \
    --http-method=POST \
    --oidc-service-account-email=clickup-scheduler@${PROJECT_ID}.iam.gserviceaccount.com \
    --time-zone="Europe/Oslo" \
    --description="Full reindex of all ClickUp data (quarterly at 2 AM on 1st)" \
    2>/dev/null || echo "  ✓ Full reindex scheduler already exists"

echo ""
echo "🎉 Deployment Complete!"
echo "=================================================="
echo "Service URL: $SERVICE_URL"
echo ""
echo "Endpoints:"
echo "  - POST $SERVICE_URL/sync/refresh"
echo "  - POST $SERVICE_URL/sync/full_reindex"
echo "  - GET  $SERVICE_URL/health"
echo ""
echo "Schedulers:"
echo "  - Every 6 hours: clickup-refresh-6h"
echo "  - Quarterly (Jan/Apr/Jul/Oct 1st at 2 AM): clickup-full-reindex-quarterly"
echo ""
echo "📊 Test the service:"
echo "  gcloud scheduler jobs run clickup-refresh-6h --location=$REGION"
echo ""
echo "📝 View logs:"
echo "  gcloud run services logs read $SERVICE_NAME --region=$REGION --limit=50"
echo ""
echo "🔍 Monitor service:"
echo "  https://console.cloud.google.com/run/detail/$REGION/$SERVICE_NAME"
