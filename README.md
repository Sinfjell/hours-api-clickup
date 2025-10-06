# ClickUp Time Entries to BigQuery Pipeline

A Python script that fetches all time entries from ClickUp API and automatically uploads them to Google BigQuery with proper data transformations and upsert logic.

## Problem Solved

ClickUp's API only returns data for 30-day intervals, making it impossible to get complete historical data with a single API call. This script solves this by:

1. Fetching data month by month from 2024 to present
2. Creating a comprehensive CSV file with all available fields
3. Automatically uploading to Google BigQuery with proper data transformations
4. Using MERGE logic for upsert operations (update existing, insert new)

## Features

- **Complete Data Coverage**: Fetches all data from 2024 to present
- **Rate Limiting**: Includes delays to respect API limits
- **Error Handling**: Continues processing even if individual months fail
- **BigQuery Integration**: Automatically uploads data to Google BigQuery
- **Data Transformation**: Converts timestamps, calculates duration in hours, handles timezones
- **Upsert Logic**: Uses MERGE statements to update existing records or insert new ones
- **Schema Compliance**: Matches your exact BigQuery table schema

## Setup

1. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

2. **Authenticate with Google Cloud:**
```bash
gcloud auth application-default login
```

3. **Update configuration in `fetch_clickup_data.py`:**
   - `API_TOKEN`: Your ClickUp API token
   - `TEAM_ID`: Your ClickUp team ID
   - `ASSIGNEES`: Comma-separated list of user IDs

## Usage

Run the script:
```bash
python fetch_clickup_data.py
```

This will:
- Fetch all time entries from January 2024 to present
- Create `clickup_time_entries.csv` with all 28 columns
- Upload to BigQuery staging table (`staging_time_entries`)
- Merge into fact table (`fact_time_entries`)
- Clean up temporary files

## BigQuery Integration

### Tables Created
- **Staging Table**: `nettsmed-internal.clickup_data.staging_time_entries`
- **Fact Table**: `nettsmed-internal.clickup_data.fact_time_entries`

### Data Transformations
- **Timestamps**: Converted from milliseconds to UTC timestamps
- **Duration**: Calculated in hours (`duration_hours = duration_ms / 3600000`)
- **Timezone**: Oslo timezone date (`start_date_oslo`)
- **Upsert Logic**: Uses `id` as primary key for MERGE operations

### Schema
The BigQuery tables include all CSV columns plus:
- `start_utc`: Start time as UTC timestamp
- `end_utc`: End time as UTC timestamp  
- `at`: Last updated as UTC timestamp
- `start_date_oslo`: Start date in Oslo timezone
- `duration_hours`: Duration in hours (float)
- `duration_ms`: Duration in milliseconds (integer)
- `user_email_sha256`: SHA256 hash of user email

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