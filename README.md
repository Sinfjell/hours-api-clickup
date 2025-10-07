# ClickUp to BigQuery Pipeline

A Python service that fetches ClickUp time entries and lists from ClickUp API and automatically uploads them to Google BigQuery with proper data transformations and upsert logic.

## Problem Solved

ClickUp's API only returns data for 30-day intervals, making it impossible to get complete historical data with a single API call. This script solves this by:

1. Fetching data month by month from 2024 to present
2. Creating a comprehensive CSV file with all available fields
3. Automatically uploading to Google BigQuery with proper data transformations
4. Using MERGE logic for upsert operations (update existing, insert new)

## Features

### Time Entries
- **Two Operation Modes**: 
  - `refresh`: Fetch only recent data (last N days) with windowed delete
  - `full_reindex`: Fetch all data from 2024 to present
- **30-Day Chunking**: Respects ClickUp's API limitations with strict 30-day windows
- **Advanced MERGE Logic**: 
  - Refresh mode: Windowed delete for recent data only
  - Full reindex: Complete data replacement
- **Data Deduplication**: Keeps latest entry per ID based on timestamp

### Lists
- **Full Hierarchy**: Fetches complete Space → Folder → List structure
- **Folder-less Lists**: Handles lists directly under spaces
- **Complete Replacement**: Each sync replaces all data with current state

### General
- **Robust HTTP Handling**: Exponential backoff retry logic for 429/5xx errors
- **Rate Limiting**: Built-in delays to respect API limits
- **Environment Configuration**: Secure credential management via .env file
- **CLI Interface**: Command-line arguments for all configuration options
- **BigQuery Integration**: Automatic upload with proper data transformations
- **Comprehensive Logging**: Detailed progress and error reporting

## Setup

1. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

2. **Authenticate with Google Cloud:**
```bash
gcloud auth application-default login
```

3. **Set up environment variables:**
   ```bash
   # Copy the example file
   cp .env.example .env
   
   # Edit .env with your actual values
   CLICKUP_TOKEN=your_clickup_token
   TEAM_ID=your_team_id
   ASSIGNEES=assignee_id_1,assignee_id_2
   ```

## Usage

### Cloud Run API Endpoints

The service is deployed on Google Cloud Run and provides HTTP endpoints:

**Sync time entries (last 60 days):**
```bash
curl -X POST https://your-service-url/sync/refresh
```

**Full reindex of time entries:**
```bash
curl -X POST https://your-service-url/sync/full_reindex
```

**Sync ClickUp lists:**
```bash
curl -X POST https://your-service-url/sync/lists
```

**Health check:**
```bash
curl https://your-service-url/health
```

### Local CLI Usage

**Refresh mode (recommended for regular sync):**
```bash
python fetch_clickup_data.py --mode refresh --days 60
```

**Full reindex mode (for initial setup or complete refresh):**
```bash
python fetch_clickup_data.py --mode full_reindex
```

### Advanced Usage

**Custom BigQuery settings:**
```bash
python fetch_clickup_data.py --mode refresh \
  --project_id my-project \
  --dataset my_dataset \
  --staging_table my_staging \
  --fact_table my_fact
```

**Custom date range for refresh:**
```bash
python fetch_clickup_data.py --mode refresh --days 30
```

### What the script does:
- Fetches time entries using 30-day chunks (respects ClickUp API limits)
- Creates timestamped CSV files for backup
- Uploads data to BigQuery staging table
- Executes MERGE operation to update fact table

## CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--mode` | `refresh` | Operation mode: `refresh` or `full_reindex` |
| `--days` | `60` | Number of days to fetch in refresh mode |
| `--project_id` | `nettsmed-internal` | BigQuery project ID |
| `--dataset` | `clickup_data` | BigQuery dataset name |
| `--staging_table` | `staging_time_entries` | BigQuery staging table name |
| `--fact_table` | `fact_time_entries` | BigQuery fact table name |

All arguments can also be set via environment variables in `.env` file.

## BigQuery Integration

### Tables Created
- **Time Entries Staging**: `nettsmed-internal.clickup_data.staging_time_entries`
- **Time Entries Fact**: `nettsmed-internal.clickup_data.fact_time_entries`
- **Lists Dimension**: `nettsmed-internal.clickup_data.dim_lists`

### Data Transformations
- **Timestamps**: Converted from milliseconds to UTC timestamps
- **Duration**: Calculated in hours (`duration_hours = duration_ms / 3600000`)
- **Timezone**: Oslo timezone date (`start_date_oslo`)
- **Upsert Logic**: Uses `id` as primary key for MERGE operations

### Schema

**Time Entries Tables:**
- `start_utc`: Start time as UTC timestamp
- `end_utc`: End time as UTC timestamp  
- `at`: Last updated as UTC timestamp
- `start_date_oslo`: Start date in Oslo timezone
- `duration_hours`: Duration in hours (float)
- `duration_ms`: Duration in milliseconds (integer)
- `user_email_sha256`: SHA256 hash of user email

**Lists Table:**
- `space_id`: Space ID (STRING, REQUIRED)
- `space_name`: Space name (STRING, REQUIRED)
- `folder_id`: Folder ID (STRING, empty if folder-less)
- `folder_name`: Folder name (STRING, empty if folder-less)
- `list_id`: List ID (STRING, REQUIRED)
- `list_name`: List name (STRING, REQUIRED)

## Output

### CSV File
Creates `clickup_time_entries.csv` with 28 columns including:
- Basic time entry data (id, start, end, duration, billable, etc.)
- Task information (id, name, status, custom fields)
- User details (id, username, email, color, initials, etc.)
- Task location (list_id, folder_id, space_id)

### BigQuery Tables
- **Staging**: Temporary table for data loading
- **Fact**: Main table with upsert logic for data updates

## Requirements

- Python 3.7+
- Google Cloud SDK (`gcloud`)
- BigQuery Data Editor and Job User roles
- Access to `nettsmed-internal` project

## Troubleshooting

1. **API Token**: Ensure your ClickUp API token is valid and has the necessary permissions
2. **Team ID**: Verify the team ID is correct
3. **User IDs**: Check that the assignee user IDs are valid
4. **BigQuery Access**: Run `gcloud auth application-default login` and verify project access
5. **BigQuery Permissions**: Ensure you have BigQuery Data Editor and Job User roles

## Data Quality

The script handles:
- Missing or null values
- Invalid timestamps
- API errors
- Network timeouts
- BigQuery upload errors (falls back to CSV-only mode)

All data is preserved exactly as returned by the ClickUp API with proper transformations for BigQuery compatibility.

## Files

- `fetch_clickup_data.py` - Main script
- `requirements.txt` - Python dependencies
- `README.md` - This documentation
- `clickup_time_entries.csv` - Generated CSV file (after running)