# Documentation Index — ClickUp to BigQuery Sync
_Last updated: 2025-10-07_

## 📚 Documentation Overview

This project is a production-ready pipeline that syncs ClickUp time tracking data to Google BigQuery. It runs on Google Cloud Run with automated scheduling.

## 📖 Quick Links

| Document | Purpose | Audience |
|----------|---------|----------|
| [Setup Guide](setup.md) | Deployment & configuration | DevOps, Admins |
| [Technical Reference](reference.md) | API endpoints & architecture | Developers |
| [Changelog](changelog.md) | Version history & updates | All users |

## 🚀 Getting Started

1. **For Deployment**: Start with [Setup Guide](setup.md)
2. **For Development**: Read [Technical Reference](reference.md)
3. **For Updates**: Check [Changelog](changelog.md)

## 🌍 Current Deployment

- **Environment**: Production
- **Version**: 2.0.0
- **Cloud Run Region**: `europe-north1` (Finland)
- **Scheduler Region**: `europe-west1` (Belgium)
- **Service URL**: `https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app`

## 📊 Key Features

- ✅ Automated sync every 6 hours
- ✅ Quarterly full reindex for data validation
- ✅ Deployed on Google Cloud Run (serverless)
- ✅ Secure authentication with OIDC tokens
- ✅ Comprehensive logging and monitoring
- ✅ Cost-optimized (~$1/month)

## 🎯 Documentation Standards

- All docs are Markdown (.md) files
- Include `Last updated: YYYY-MM-DD` at the top
- Use consistent emoji headers for scanning
- Prefer lists and short paragraphs
- Fenced code blocks with language tags

---

**Purpose:** Overview/entry point for the ClickUp to BigQuery sync pipeline
