#!/usr/bin/env python3
"""
ClickUp Time Entries to BigQuery Pipeline

This script fetches time entries from ClickUp API and uploads them to BigQuery
with support for both refresh and full reindex operations.

Usage:
    python fetch_clickup_data.py --mode refresh --days 60
    python fetch_clickup_data.py --mode full_reindex
"""

import argparse
import os
import sys
import time
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlencode

import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickUpDataFetcher:
    """Fetches time entries from ClickUp API with robust error handling."""
    
    def __init__(self, token: str, team_id: str, assignees: List[str]):
        self.token = token
        self.team_id = team_id
        self.assignees = assignees
        self.base_url = "https://api.clickup.com/api/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        })
        
    def _make_request(self, url: str, params: Dict[str, Any], max_retries: int = 3) -> Dict[str, Any]:
        """Make HTTP request with exponential backoff retry logic."""
        for attempt in range(max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                elif response.status_code >= 500:
                    # Server error - retry with backoff
                    wait_time = 2 ** attempt
                    logger.warning(f"Server error {response.status_code}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    logger.error(f"Request failed after {max_retries + 1} attempts: {e}")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Request failed: {e}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
        
        raise Exception(f"Request failed after {max_retries + 1} attempts")
    
    def fetch_time_entries_30day_chunk(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Fetch time entries for a 30-day chunk respecting ClickUp's API limitations."""
        all_entries = []
        
        # Convert to milliseconds since epoch
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        
        # Prepare assignees parameter
        assignees_param = ','.join(self.assignees) if self.assignees else None
        
        params = {
            'start_date': start_ms,
            'end_date': end_ms,
            'assignee': assignees_param
        }
        
        url = f"{self.base_url}/team/{self.team_id}/time_entries"
        
        try:
            logger.info(f"Fetching time entries from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            # Add small delay between requests to respect rate limits
            time.sleep(0.5)
            
            data = self._make_request(url, params)
            entries = data.get('data', [])
            
            logger.info(f"Found {len(entries)} entries for this chunk")
            all_entries.extend(entries)
            
        except Exception as e:
            logger.error(f"Error fetching data for chunk {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}")
            raise
        
        return all_entries
    
    def fetch_all_time_entries(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Fetch all time entries using 30-day chunks."""
        all_entries = []
        current_start = start_date
        
        while current_start < end_date:
            # Calculate chunk end (30 days from current start)
            chunk_end = min(current_start + timedelta(days=30), end_date)
            
            try:
                chunk_entries = self.fetch_time_entries_30day_chunk(current_start, chunk_end)
                all_entries.extend(chunk_entries)
                
                # Move to next chunk
                current_start = chunk_end
                
            except Exception as e:
                logger.error(f"Failed to fetch chunk {current_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}: {e}")
                # Continue with next chunk instead of failing completely
                current_start = chunk_end
                continue
        
        logger.info(f"Total entries fetched: {len(all_entries)}")
        return all_entries


class DataTransformer:
    """Transforms raw ClickUp data into BigQuery-ready format."""
    
    @staticmethod
    def safe_bool(value: Any) -> bool:
        """Safely convert various boolean representations to Python bool."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        if isinstance(value, (int, float)):
            return bool(value)
        return False
    
    @staticmethod
    def safe_int(value: Any) -> Optional[int]:
        """Safely convert value to integer, returning None for invalid values."""
        if pd.isna(value) or value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def transform_time_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single time entry to BigQuery format."""
        try:
            # Extract basic fields
            entry_id = str(entry.get('id', ''))
            start_ms = entry.get('start', 0)
            end_ms = entry.get('end', 0)
            duration_ms = entry.get('duration', 0)
            at_ms = entry.get('at', 0)
            
            # Convert timestamps
            start_utc = pd.to_datetime(start_ms, unit='ms', utc=True) if start_ms else None
            end_utc = pd.to_datetime(end_ms, unit='ms', utc=True) if end_ms else None
            at_utc = pd.to_datetime(at_ms, unit='ms', utc=True) if at_ms else None
            
            # Calculate duration in hours
            duration_hours = duration_ms / 3600000.0 if duration_ms else 0.0
            
            # Convert to Oslo timezone for date calculation
            start_date_oslo = None
            if start_utc:
                start_date_oslo = start_utc.tz_convert('Europe/Oslo').date()
            
            # Extract task information
            task = entry.get('task', {})
            task_id = str(task.get('id', '')) if task.get('id') else None
            task_name = task.get('name', '')
            task_custom_type = str(task.get('custom_type', '')) if task.get('custom_type') is not None else None
            task_custom_id = str(task.get('custom_id', '')) if task.get('custom_id') is not None else None
            
            # Extract task status
            task_status = task.get('status', {})
            task_status_status = task_status.get('status', '')
            task_status_color = task_status.get('color', '')
            task_status_type = task_status.get('type', '')
            task_status_orderindex = DataTransformer.safe_int(task_status.get('orderindex'))
            
            # Extract user information
            user = entry.get('user', {})
            user_id = str(user.get('id', '')) if user.get('id') else None
            user_username = user.get('username', '')
            user_email = user.get('email', '')
            user_email_sha256 = hashlib.sha256(user_email.encode()).hexdigest() if user_email else None
            user_color = user.get('color', '')
            user_initials = user.get('initials', '')
            user_profile_picture = user.get('profilePicture', '')
            
            # Extract task location
            task_location = entry.get('task_location', {})
            task_location_list_id = str(task_location.get('list_id', '')) if task_location.get('list_id') else None
            task_location_folder_id = str(task_location.get('folder_id', '')) if task_location.get('folder_id') else None
            task_location_space_id = str(task_location.get('space_id', '')) if task_location.get('space_id') else None
            
            return {
                'id': entry_id,
                'start_utc': start_utc,
                'end_utc': end_utc,
                'duration_ms': DataTransformer.safe_int(duration_ms),
                'duration_hours': duration_hours,
                'billable': DataTransformer.safe_bool(entry.get('billable')),
                'description': entry.get('description', ''),
                'source': entry.get('source', ''),
                'at': at_utc,
                'is_locked': DataTransformer.safe_bool(entry.get('is_locked')),
                'approval_id': str(entry.get('approval_id', '')) if entry.get('approval_id') else None,
                'task_url': entry.get('task_url', ''),
                'task_id': task_id,
                'task_name': task_name,
                'task_custom_type': task_custom_type,
                'task_custom_id': task_custom_id,
                'task_status_status': task_status_status,
                'task_status_color': task_status_color,
                'task_status_type': task_status_type,
                'task_status_orderindex': task_status_orderindex,
                'user_id': user_id,
                'user_username': user_username,
                'user_email': user_email,
                'user_email_sha256': user_email_sha256,
                'user_color': user_color,
                'user_initials': user_initials,
                'user_profilePicture': user_profile_picture,
                'task_location_list_id': task_location_list_id,
                'task_location_folder_id': task_location_folder_id,
                'task_location_space_id': task_location_space_id,
                'start_date_oslo': start_date_oslo
            }
            
        except Exception as e:
            logger.error(f"Error transforming entry {entry.get('id', 'unknown')}: {e}")
            # Return minimal valid entry
            return {
                'id': str(entry.get('id', '')),
                'start_utc': None,
                'end_utc': None,
                'duration_ms': None,
                'duration_hours': 0.0,
                'billable': False,
                'description': '',
                'source': '',
                'at': None,
                'is_locked': False,
                'approval_id': None,
                'task_url': '',
                'task_id': None,
                'task_name': '',
                'task_custom_type': None,
                'task_custom_id': None,
                'task_status_status': '',
                'task_status_color': '',
                'task_status_type': '',
                'task_status_orderindex': None,
                'user_id': None,
                'user_username': '',
                'user_email': '',
                'user_email_sha256': None,
                'user_color': '',
                'user_initials': '',
                'user_profilePicture': '',
                'task_location_list_id': None,
                'task_location_folder_id': None,
                'task_location_space_id': None,
                'start_date_oslo': None
            }


class BigQueryManager:
    """Manages BigQuery operations for time entries data."""
    
    def __init__(self, project_id: str, dataset: str, staging_table: str, fact_table: str):
        self.project_id = project_id
        self.dataset = dataset
        self.staging_table = staging_table
        self.fact_table = fact_table
        self.client = bigquery.Client(project=project_id)
        
    def ensure_dataset_exists(self):
        """Ensure the dataset exists."""
        dataset_id = f"{self.project_id}.{self.dataset}"
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"  # Set location as needed
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset_id}")
    
    def create_staging_table(self, df: pd.DataFrame):
        """Create or recreate staging table with proper schema."""
        table_id = f"{self.project_id}.{self.dataset}.{self.staging_table}"
        
        # Define schema based on DataFrame
        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("start_utc", "TIMESTAMP"),
            bigquery.SchemaField("end_utc", "TIMESTAMP"),
            bigquery.SchemaField("duration_ms", "INTEGER"),
            bigquery.SchemaField("duration_hours", "FLOAT"),
            bigquery.SchemaField("billable", "BOOLEAN"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("at", "TIMESTAMP"),
            bigquery.SchemaField("is_locked", "BOOLEAN"),
            bigquery.SchemaField("approval_id", "STRING"),
            bigquery.SchemaField("task_url", "STRING"),
            bigquery.SchemaField("task_id", "STRING"),
            bigquery.SchemaField("task_name", "STRING"),
            bigquery.SchemaField("task_custom_type", "STRING"),
            bigquery.SchemaField("task_custom_id", "STRING"),
            bigquery.SchemaField("task_status_status", "STRING"),
            bigquery.SchemaField("task_status_color", "STRING"),
            bigquery.SchemaField("task_status_type", "STRING"),
            bigquery.SchemaField("task_status_orderindex", "INTEGER"),
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("user_username", "STRING"),
            bigquery.SchemaField("user_email", "STRING"),
            bigquery.SchemaField("user_email_sha256", "STRING"),
            bigquery.SchemaField("user_color", "STRING"),
            bigquery.SchemaField("user_initials", "STRING"),
            bigquery.SchemaField("user_profilePicture", "STRING"),
            bigquery.SchemaField("task_location_list_id", "STRING"),
            bigquery.SchemaField("task_location_folder_id", "STRING"),
            bigquery.SchemaField("task_location_space_id", "STRING"),
            bigquery.SchemaField("start_date_oslo", "DATE")
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table, exists_ok=True)
        logger.info(f"Staging table {table_id} ready")
    
    def upload_to_staging(self, df: pd.DataFrame):
        """Upload DataFrame to staging table."""
        table_id = f"{self.project_id}.{self.dataset}.{self.staging_table}"
        
        # Ensure proper data types for BigQuery
        df_upload = df.copy()
        
        # Convert nullable integers
        df_upload['duration_ms'] = df_upload['duration_ms'].astype('Int64')
        df_upload['task_status_orderindex'] = df_upload['task_status_orderindex'].astype('Int64')
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        job = self.client.load_table_from_dataframe(df_upload, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        
        logger.info(f"Uploaded {len(df_upload)} rows to staging table")
    
    def merge_refresh_mode(self, days: int):
        """Execute MERGE in refresh mode with windowed delete."""
        query = f"""
        DECLARE refresh_days INT64 DEFAULT @days;

        MERGE `{self.project_id}.{self.dataset}.{self.fact_table}` T
        USING (
          SELECT * FROM `{self.project_id}.{self.dataset}.{self.staging_table}`
          WHERE start_date_oslo BETWEEN DATE_SUB(CURRENT_DATE("Europe/Oslo"), INTERVAL refresh_days DAY)
                                   AND CURRENT_DATE("Europe/Oslo")
        ) S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET
          start_utc = S.start_utc,
          end_utc = S.end_utc,
          duration_ms = S.duration_ms,
          duration_hours = S.duration_hours,
          billable = S.billable,
          description = S.description,
          source = S.source,
          `at` = S.`at`,
          is_locked = S.is_locked,
          approval_id = S.approval_id,
          task_url = S.task_url,
          task_id = S.task_id,
          task_name = S.task_name,
          task_custom_type = S.task_custom_type,
          task_custom_id = S.task_custom_id,
          task_status_status = S.task_status_status,
          task_status_color = S.task_status_color,
          task_status_type = S.task_status_type,
          task_status_orderindex = S.task_status_orderindex,
          user_id = S.user_id,
          user_username = S.user_username,
          user_email = S.user_email,
          user_email_sha256 = S.user_email_sha256,
          user_color = S.user_color,
          user_initials = S.user_initials,
          user_profilePicture = S.user_profilePicture,
          task_location_list_id = S.task_location_list_id,
          task_location_folder_id = S.task_location_folder_id,
          task_location_space_id = S.task_location_space_id,
          start_date_oslo = S.start_date_oslo
        WHEN NOT MATCHED THEN
          INSERT ROW
        WHEN NOT MATCHED BY SOURCE
          AND T.start_date_oslo BETWEEN DATE_SUB(CURRENT_DATE("Europe/Oslo"), INTERVAL refresh_days DAY)
                                    AND CURRENT_DATE("Europe/Oslo")
        THEN DELETE;
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days)
            ]
        )
        
        job = self.client.query(query, job_config=job_config)
        job.result()
        
        logger.info(f"Refresh mode MERGE completed for last {days} days")
    
    def merge_full_reindex_mode(self):
        """Execute MERGE in full reindex mode."""
        query = f"""
        MERGE `{self.project_id}.{self.dataset}.{self.fact_table}` T
        USING `{self.project_id}.{self.dataset}.{self.staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET
          start_utc = S.start_utc,
          end_utc = S.end_utc,
          duration_ms = S.duration_ms,
          duration_hours = S.duration_hours,
          billable = S.billable,
          description = S.description,
          source = S.source,
          `at` = S.`at`,
          is_locked = S.is_locked,
          approval_id = S.approval_id,
          task_url = S.task_url,
          task_id = S.task_id,
          task_name = S.task_name,
          task_custom_type = S.task_custom_type,
          task_custom_id = S.task_custom_id,
          task_status_status = S.task_status_status,
          task_status_color = S.task_status_color,
          task_status_type = S.task_status_type,
          task_status_orderindex = S.task_status_orderindex,
          user_id = S.user_id,
          user_username = S.user_username,
          user_email = S.user_email,
          user_email_sha256 = S.user_email_sha256,
          user_color = S.user_color,
          user_initials = S.user_initials,
          user_profilePicture = S.user_profilePicture,
          task_location_list_id = S.task_location_list_id,
          task_location_folder_id = S.task_location_folder_id,
          task_location_space_id = S.task_location_space_id,
          start_date_oslo = S.start_date_oslo
        WHEN NOT MATCHED THEN
          INSERT ROW
        WHEN NOT MATCHED BY SOURCE THEN
          DELETE;
        """
        
        job = self.client.query(query)
        job.result()
        
        logger.info("Full reindex mode MERGE completed")
    
    def create_fact_table_if_not_exists(self):
        """Create fact table if it doesn't exist."""
        table_id = f"{self.project_id}.{self.dataset}.{self.fact_table}"
        
        try:
            self.client.get_table(table_id)
            logger.info(f"Fact table {table_id} already exists")
        except NotFound:
            # Create table with same schema as staging
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("start_utc", "TIMESTAMP"),
                bigquery.SchemaField("end_utc", "TIMESTAMP"),
                bigquery.SchemaField("duration_ms", "INTEGER"),
                bigquery.SchemaField("duration_hours", "FLOAT"),
                bigquery.SchemaField("billable", "BOOLEAN"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("at", "TIMESTAMP"),
                bigquery.SchemaField("is_locked", "BOOLEAN"),
                bigquery.SchemaField("approval_id", "STRING"),
                bigquery.SchemaField("task_url", "STRING"),
                bigquery.SchemaField("task_id", "STRING"),
                bigquery.SchemaField("task_name", "STRING"),
                bigquery.SchemaField("task_custom_type", "STRING"),
                bigquery.SchemaField("task_custom_id", "STRING"),
                bigquery.SchemaField("task_status_status", "STRING"),
                bigquery.SchemaField("task_status_color", "STRING"),
                bigquery.SchemaField("task_status_type", "STRING"),
                bigquery.SchemaField("task_status_orderindex", "INTEGER"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("user_username", "STRING"),
                bigquery.SchemaField("user_email", "STRING"),
                bigquery.SchemaField("user_email_sha256", "STRING"),
                bigquery.SchemaField("user_color", "STRING"),
                bigquery.SchemaField("user_initials", "STRING"),
                bigquery.SchemaField("user_profilePicture", "STRING"),
                bigquery.SchemaField("task_location_list_id", "STRING"),
                bigquery.SchemaField("task_location_folder_id", "STRING"),
                bigquery.SchemaField("task_location_space_id", "STRING"),
                bigquery.SchemaField("start_date_oslo", "DATE")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created fact table {table_id}")


def main():
    """Main function with CLI argument parsing."""
    parser = argparse.ArgumentParser(description='ClickUp Time Entries to BigQuery Pipeline')
    
    parser.add_argument(
        '--mode',
        choices=['refresh', 'full_reindex'],
        default='refresh',
        help='Operation mode: refresh (last N days) or full_reindex (all data since 2024)'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        default=60,
        help='Number of days to fetch in refresh mode (default: 60)'
    )
    
    parser.add_argument(
        '--project_id',
        default=os.getenv('PROJECT_ID', 'nettsmed-internal'),
        help='BigQuery project ID (default: from env or nettsmed-internal)'
    )
    
    parser.add_argument(
        '--dataset',
        default=os.getenv('DATASET', 'clickup_data'),
        help='BigQuery dataset name (default: from env or clickup_data)'
    )
    
    parser.add_argument(
        '--staging_table',
        default=os.getenv('STAGING_TABLE', 'staging_time_entries'),
        help='BigQuery staging table name (default: from env or staging_time_entries)'
    )
    
    parser.add_argument(
        '--fact_table',
        default=os.getenv('FACT_TABLE', 'fact_time_entries'),
        help='BigQuery fact table name (default: from env or fact_time_entries)'
    )
    
    args = parser.parse_args()
    
    # Validate required environment variables
    clickup_token = os.getenv('CLICKUP_TOKEN')
    team_id = os.getenv('TEAM_ID')
    assignees_str = os.getenv('ASSIGNEES', '')
    
    if not clickup_token:
        logger.error("CLICKUP_TOKEN environment variable is required")
        sys.exit(1)
    
    if not team_id:
        logger.error("TEAM_ID environment variable is required")
        sys.exit(1)
    
    # Parse assignees
    assignees = [a.strip() for a in assignees_str.split(',') if a.strip()] if assignees_str else []
    
    logger.info(f"Starting ClickUp data pipeline in {args.mode} mode")
    logger.info(f"Project: {args.project_id}, Dataset: {args.dataset}")
    logger.info(f"Staging table: {args.staging_table}, Fact table: {args.fact_table}")
    
    try:
        # Initialize components
        fetcher = ClickUpDataFetcher(clickup_token, team_id, assignees)
        transformer = DataTransformer()
        bq_manager = BigQueryManager(args.project_id, args.dataset, args.staging_table, args.fact_table)
        
        # Determine date range
        end_date = datetime.now(timezone.utc)
        
        if args.mode == 'refresh':
            start_date = end_date - timedelta(days=args.days)
            logger.info(f"Refresh mode: fetching last {args.days} days")
        else:  # full_reindex
            start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            logger.info("Full reindex mode: fetching all data since 2024-01-01")
        
        logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Fetch data
        logger.info("Fetching time entries from ClickUp...")
        raw_entries = fetcher.fetch_all_time_entries(start_date, end_date)
        
        if not raw_entries:
            logger.warning("No time entries found")
            return
        
        # Transform data
        logger.info("Transforming data...")
        transformed_entries = [transformer.transform_time_entry(entry) for entry in raw_entries]
        
        # Create DataFrame
        df = pd.DataFrame(transformed_entries)
        
        # Remove duplicates (keep latest by 'at' timestamp)
        if 'at' in df.columns and not df['at'].isna().all():
            df = df.sort_values('at', na_position='last').drop_duplicates(subset=['id'], keep='last')
            logger.info(f"Removed duplicates, {len(df)} unique entries remaining")
        
        # Save CSV for backup
        csv_filename = f"clickup_time_entries_{args.mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_filename, index=False)
        logger.info(f"Saved {len(df)} entries to {csv_filename}")
        
        # BigQuery operations
        logger.info("Setting up BigQuery...")
        bq_manager.ensure_dataset_exists()
        bq_manager.create_staging_table(df)
        bq_manager.create_fact_table_if_not_exists()
        
        logger.info("Uploading to staging table...")
        bq_manager.upload_to_staging(df)
        
        logger.info("Executing MERGE operation...")
        if args.mode == 'refresh':
            bq_manager.merge_refresh_mode(args.days)
        else:
            bq_manager.merge_full_reindex_mode()
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()