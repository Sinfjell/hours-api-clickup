#!/usr/bin/env python3
"""
ClickUp Time Entries Data Fetcher
Fetches all time entries from ClickUp API month by month and creates a comprehensive CSV
"""

import requests
import csv
import json
import time
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class ClickUpDataFetcher:
    def __init__(self, api_token: str, team_id: str, assignees: str):
        self.api_token = api_token
        self.team_id = team_id
        self.assignees = assignees
        self.base_url = "https://api.clickup.com/api/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        })
        
        # Define the exact columns we want
        self.columns = [
            'id',
            'start',
            'end', 
            'duration',
            'billable',
            'description',
            'source',
            'at',
            'is_locked',
            'approval_id',
            'task_url',
            'task_id',
            'task_name',
            'task_custom_type',
            'task_custom_id',
            'task_status_status',
            'task_status_color',
            'task_status_type',
            'task_status_orderindex',
            'user_id',
            'user_username',
            'user_email',
            'user_color',
            'user_initials',
            'user_profilePicture',
            'task_location_list_id',
            'task_location_folder_id',
            'task_location_space_id'
        ]

    def get_timestamp(self, date: datetime) -> int:
        """Convert datetime to ClickUp timestamp (milliseconds)"""
        return int(date.timestamp() * 1000)

    def fetch_time_entries_for_month(self, year: int, month: int) -> List[Dict[str, Any]]:
        """Fetch time entries for a specific month"""
        # Get first and last day of the month
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        start_timestamp = self.get_timestamp(start_date)
        end_timestamp = self.get_timestamp(end_date)
        
        url = f"{self.base_url}/team/{self.team_id}/time_entries"
        params = {
            'assignee': self.assignees,
            'start_date': start_timestamp,
            'end_date': end_timestamp
        }
        
        print(f"Fetching data for {year}-{month:02d} ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            entries = data.get('data', [])
            
            print(f"Found {len(entries)} entries for {year}-{month:02d}")
            return entries
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {year}-{month:02d}: {e}")
            return []

    def flatten_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten a ClickUp time entry into our CSV format"""
        flattened = {}
        
        # Basic fields
        flattened['id'] = entry.get('id', '')
        flattened['start'] = entry.get('start', '')
        flattened['end'] = entry.get('end', '')
        flattened['duration'] = entry.get('duration', '')
        flattened['billable'] = entry.get('billable', False)
        flattened['description'] = entry.get('description', '')
        flattened['source'] = entry.get('source', '')
        flattened['at'] = entry.get('at', '')
        flattened['is_locked'] = entry.get('is_locked', False)
        flattened['approval_id'] = entry.get('approval_id', '')
        flattened['task_url'] = entry.get('task_url', '')
        
        # Task fields
        task = entry.get('task', {})
        flattened['task_id'] = task.get('id', '')
        flattened['task_name'] = task.get('name', '')
        flattened['task_custom_type'] = task.get('custom_type', '')
        flattened['task_custom_id'] = task.get('custom_id', '')
        
        # Task status fields
        task_status = task.get('status', {})
        flattened['task_status_status'] = task_status.get('status', '')
        flattened['task_status_color'] = task_status.get('color', '')
        flattened['task_status_type'] = task_status.get('type', '')
        flattened['task_status_orderindex'] = task_status.get('orderindex', '')
        
        # User fields
        user = entry.get('user', {})
        flattened['user_id'] = user.get('id', '')
        flattened['user_username'] = user.get('username', '')
        flattened['user_email'] = user.get('email', '')
        flattened['user_color'] = user.get('color', '')
        flattened['user_initials'] = user.get('initials', '')
        flattened['user_profilePicture'] = user.get('profilePicture', '')
        
        # Task location fields
        task_location = entry.get('task_location', {})
        flattened['task_location_list_id'] = task_location.get('list_id', '')
        flattened['task_location_folder_id'] = task_location.get('folder_id', '')
        flattened['task_location_space_id'] = task_location.get('space_id', '')
        
        return flattened

    def fetch_all_data(self, start_year: int = 2024) -> List[Dict[str, Any]]:
        """Fetch all time entries from start_year to current date"""
        all_entries = []
        current_date = datetime.now()
        
        # Generate all months from start_year to current month
        year = start_year
        month = 1
        
        while year < current_date.year or (year == current_date.year and month <= current_date.month):
            entries = self.fetch_time_entries_for_month(year, month)
            all_entries.extend(entries)
            
            # Add delay to respect rate limits
            time.sleep(1)
            
            # Move to next month
            month += 1
            if month > 12:
                month = 1
                year += 1
        
        print(f"Total entries fetched: {len(all_entries)}")
        return all_entries

    def save_to_csv(self, entries: List[Dict[str, Any]], filename: str = "clickup_time_entries.csv"):
        """Save entries to CSV file"""
        if not entries:
            print("No entries to save")
            return
        
        # Flatten all entries
        flattened_entries = [self.flatten_entry(entry) for entry in entries]
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(flattened_entries)
        
        print(f"Data saved to {filename}")
        print(f"Total rows: {len(flattened_entries)}")
        print(f"Columns: {len(self.columns)}")

    def upload_csv_to_bigquery(self, csv_path: str, project_id: str, dataset: str, staging_table: str, fact_table: str):
        """Upload CSV to BigQuery staging table and merge into fact table"""
        print("Uploading CSV to BigQuery...")
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Read CSV into DataFrame
        print("Reading CSV file...")
        df = pd.read_csv(csv_path)
        
        # Data transformations
        print("Transforming data...")
        
        # Rename duration to duration_ms to match schema
        df['duration_ms'] = df['duration']
        
        # Convert timestamps from milliseconds to datetime
        df['start_utc'] = pd.to_datetime(pd.to_numeric(df['start']), unit='ms', utc=True)
        df['end_utc'] = pd.to_datetime(pd.to_numeric(df['end']), unit='ms', utc=True)
        df['at'] = pd.to_datetime(pd.to_numeric(df['at']), unit='ms', utc=True)
        
        # Convert to Oslo timezone for start_date_oslo
        df['start_date_oslo'] = df['start_utc'].dt.tz_convert('Europe/Oslo').dt.date
        
        # Calculate duration in hours
        df['duration_hours'] = df['duration_ms'] / 3600000
        
        # Convert boolean fields
        df['billable'] = df['billable'].map({'True': True, 'False': False, True: True, False: False})
        df['is_locked'] = df['is_locked'].map({'True': True, 'False': False, True: True, False: False})
        
        # Convert user_id to string to match schema
        df['user_id'] = df['user_id'].astype(str)
        
        # Add user_email_sha256 (you can implement hashing if needed)
        import hashlib
        df['user_email_sha256'] = df['user_email'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) else '')
        
        # Convert numeric fields
        numeric_fields = ['duration_hours']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Convert integer fields (no decimal places)
        df['duration_ms'] = df['duration_ms'].astype('Int64')  # Use nullable integer type
        df['task_status_orderindex'] = df['task_status_orderindex'].astype('Int64')  # Use nullable integer type
        
        # Reorder columns to match the schema exactly
        schema_columns = [
            'id', 'start_utc', 'end_utc', 'duration_ms', 'duration_hours', 'billable',
            'description', 'source', 'at', 'is_locked', 'approval_id', 'task_url',
            'task_id', 'task_name', 'task_custom_type', 'task_custom_id',
            'task_status_status', 'task_status_color', 'task_status_type', 'task_status_orderindex',
            'user_id', 'user_username', 'user_email', 'user_email_sha256', 'user_color',
            'user_initials', 'user_profilePicture', 'task_location_list_id',
            'task_location_folder_id', 'task_location_space_id', 'start_date_oslo'
        ]
        
        # Reorder DataFrame to match schema
        df = df[schema_columns]
        
        # Create a temporary CSV with transformed data for BigQuery
        temp_csv_path = csv_path.replace('.csv', '_bigquery.csv')
        df.to_csv(temp_csv_path, index=False)
        print(f"Created transformed CSV: {temp_csv_path}")
        
        # Define table schemas based on the provided schema
        staging_schema = [
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
            bigquery.SchemaField("start_date_oslo", "DATE"),
        ]
        
        # Create dataset if it doesn't exist
        dataset_id = f"{project_id}.{dataset}"
        try:
            client.get_dataset(dataset_id)
            print(f"Dataset {dataset} already exists")
        except NotFound:
            print(f"Creating dataset {dataset}...")
            dataset_obj = bigquery.Dataset(dataset_id)
            client.create_dataset(dataset_obj)
        
        # Upload to staging table
        staging_table_id = f"{dataset_id}.{staging_table}"
        print(f"Uploading to staging table {staging_table}...")
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            schema=staging_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False
        )
        
        # Load data to staging table
        with open(temp_csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, staging_table_id, job_config=job_config)
        
        job.result()  # Wait for job to complete
        print(f"Staging table {staging_table} loaded with {job.output_rows} rows")
        
        # Create fact table if it doesn't exist
        fact_table_id = f"{dataset_id}.{fact_table}"
        try:
            client.get_table(fact_table_id)
            print(f"Fact table {fact_table} already exists")
        except NotFound:
            print(f"Creating fact table {fact_table}...")
            fact_schema = staging_schema.copy()
            fact_schema.append(bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"))
            
            table = bigquery.Table(fact_table_id, schema=fact_schema)
            client.create_table(table)
        
        # Perform MERGE operation
        print("Performing MERGE operation...")
        
        merge_query = f"""
        MERGE `{fact_table_id}` AS target
        USING `{staging_table_id}` AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                start_utc = source.start_utc,
                end_utc = source.end_utc,
                duration_ms = source.duration_ms,
                duration_hours = source.duration_hours,
                billable = source.billable,
                description = source.description,
                source = source.source,
                `at` = source.`at`,
                is_locked = source.is_locked,
                approval_id = source.approval_id,
                task_url = source.task_url,
                task_id = source.task_id,
                task_name = source.task_name,
                task_custom_type = source.task_custom_type,
                task_custom_id = source.task_custom_id,
                task_status_status = source.task_status_status,
                task_status_color = source.task_status_color,
                task_status_type = source.task_status_type,
                task_status_orderindex = source.task_status_orderindex,
                user_id = source.user_id,
                user_username = source.user_username,
                user_email = source.user_email,
                user_email_sha256 = source.user_email_sha256,
                user_color = source.user_color,
                user_initials = source.user_initials,
                user_profilePicture = source.user_profilePicture,
                task_location_list_id = source.task_location_list_id,
                task_location_folder_id = source.task_location_folder_id,
                task_location_space_id = source.task_location_space_id,
                start_date_oslo = source.start_date_oslo
        WHEN NOT MATCHED THEN
            INSERT ROW
        """
        
        query_job = client.query(merge_query)
        query_job.result()  # Wait for query to complete
        
        print("Merge complete ✅")
        print(f"Fact table {fact_table} updated successfully")
        
        # Clean up temporary file
        import os
        try:
            os.remove(temp_csv_path)
            print(f"Cleaned up temporary file: {temp_csv_path}")
        except:
            pass

    def run(self, start_year: int = 2024):
        """Main execution function"""
        print("Starting ClickUp data fetch...")
        print(f"Fetching data from {start_year} to present")
        
        # Fetch all data
        all_entries = self.fetch_all_data(start_year)
        
        if all_entries:
            # Save to CSV
            csv_filename = "clickup_time_entries.csv"
            self.save_to_csv(all_entries, csv_filename)
            
            # Print summary
            print("\n=== SUMMARY ===")
            print(f"Total entries: {len(all_entries)}")
            print(f"Date range: {start_year} to present")
            print(f"CSV file: {csv_filename}")
            
            # Show sample of first entry
            if all_entries:
                print("\n=== SAMPLE ENTRY ===")
                sample = self.flatten_entry(all_entries[0])
                for key, value in sample.items():
                    if value:  # Only show non-empty values
                        print(f"{key}: {value}")
            
            # Upload to BigQuery
            print("\n=== UPLOADING TO BIGQUERY ===")
            try:
                self.upload_csv_to_bigquery(
                    csv_path=csv_filename,
                    project_id="nettsmed-internal",
                    dataset="clickup_data",
                    staging_table="staging_time_entries",
                    fact_table="fact_time_entries"
                )
            except Exception as e:
                print(f"BigQuery upload failed: {e}")
                print("CSV file is still available for manual upload")
        else:
            print("No data found")

def main():
    # Configuration - UPDATE THESE VALUES
    API_TOKEN = "55424762_44fc0fcb470696596dc4894e3aa4fd17a265af257f6048373acb9e1e877e7f8b"
    TEAM_ID = "37496228"
    ASSIGNEES = "55424762,55427758"  # Comma-separated list of user IDs
    
    # Create fetcher and run
    fetcher = ClickUpDataFetcher(API_TOKEN, TEAM_ID, ASSIGNEES)
    fetcher.run(start_year=2024)

if __name__ == "__main__":
    main()
