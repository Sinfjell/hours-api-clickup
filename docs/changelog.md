# Changelog

All notable changes. Use semantic versioning (MAJOR.MINOR.PATCH):
- **PATCH** (1.0.1): Bug fixes, small improvements
- **MINOR** (1.1.0): New features, backward compatible
- **MAJOR** (2.0.0): Breaking changes, incompatible updates

## [2.0.1] - 2025-10-07

### 🌍 Region Migration

#### Infrastructure Changes
- **Migrated to Europe**: Moved from `us-central1` to `europe-north1` (Finland)
- **Cloud Run**: Now deployed in `europe-north1` (closest to Norway)
- **Cloud Scheduler**: Using `europe-west1` (Belgium - Scheduler doesn't support europe-north1)
- **Artifact Registry**: Docker images stored in `europe-north1`

#### Service Updates
- **New Service URL**: `https://clickup-bigquery-sync-b3fljnwepq-lz.a.run.app`
- **Region-specific Configuration**: Updated all deployment scripts and documentation
- **Cleaned Up Old Resources**: Removed US region services and schedulers

#### Documentation Updates
- Updated all documentation files with Europe region information
- Added region-specific deployment instructions
- Updated Cloud Console monitoring links
- Clarified scheduler region limitations

### ✅ Deployment Status
- ✅ Cloud Run service deployed in europe-north1
- ✅ Cloud Scheduler jobs created in europe-west1
- ✅ Old US resources cleaned up
- ✅ Health checks passing
- ✅ Manual test run successful (233 entries synced)

## [2.0.0] - 2025-10-07

### 🚀 Major Features Added

#### Enhanced CLI Interface
- **Two Operation Modes**: 
  - `refresh`: Fetch only recent data (last N days) with windowed delete
  - `full_reindex`: Fetch all data from 2024 to present
- **Command-line Arguments**: Full CLI support with `--mode`, `--days`, `--project_id`, `--dataset`, `--staging_table`, `--fact_table`
- **Environment Configuration**: Secure credential management via `.env` file
- **Help Documentation**: Comprehensive `--help` output with all options

#### Advanced Data Processing
- **30-Day Chunking**: Respects ClickUp's API limitations with strict 30-day windows
- **Robust HTTP Handling**: Exponential backoff retry logic for 429/5xx errors
- **Rate Limiting**: Built-in delays to respect API limits
- **Data Deduplication**: Keeps latest entry per ID based on timestamp
- **Enhanced Type Handling**: Better boolean and integer conversion with error handling

#### BigQuery Integration Improvements
- **Advanced MERGE Logic**: 
  - Refresh mode: Windowed delete for recent data only
  - Full reindex: Complete data replacement
- **Schema Compliance**: Proper handling of BigQuery reserved keywords
- **Data Type Optimization**: Correct mapping for all BigQuery data types
- **Error Recovery**: Graceful handling of BigQuery upload failures

#### Production Readiness
- **Comprehensive Logging**: Detailed progress and error reporting
- **Error Handling**: Continues processing even if individual chunks fail
- **CSV Backups**: Timestamped backup files for all data
- **Dependency Management**: All required packages in requirements.txt

### 🔧 Technical Improvements

#### Code Architecture
- **Modular Design**: Separated concerns into ClickUpDataFetcher, DataTransformer, BigQueryManager classes
- **Type Hints**: Full type annotations for better code maintainability
- **Error Recovery**: Robust error handling throughout the pipeline
- **Performance**: Optimized data processing and BigQuery operations

#### Dependencies Added
- `python-dotenv>=1.0.0`: Environment variable management
- `pyarrow>=21.0.0`: BigQuery data serialization
- `pandas-gbq>=0.29.0`: Enhanced BigQuery integration

#### Configuration Management
- **Environment Variables**: All configuration via `.env` file
- **CLI Overrides**: Command-line arguments override environment variables
- **Secure Credentials**: No hardcoded secrets in code
- **Flexible Settings**: Easy configuration for different environments

### 📊 Data Processing Enhancements

#### ClickUp API Integration
- **30-Day Window Compliance**: Proper handling of ClickUp's API limitations
- **Retry Logic**: Exponential backoff for failed requests
- **Rate Limiting**: Respects API rate limits with configurable delays
- **Error Recovery**: Continues processing even if individual chunks fail

#### Data Transformation
- **Type Safety**: Robust conversion of all data types
- **Timezone Handling**: Proper UTC to Oslo timezone conversion
- **Duration Calculations**: Accurate time calculations in hours and milliseconds
- **User Data Hashing**: SHA256 hashing for user email privacy

#### BigQuery Operations
- **Staging Table**: Safe data upload to staging before merge
- **MERGE Operations**: Efficient upsert logic for both modes
- **Schema Management**: Automatic table creation and schema updates
- **Data Validation**: Comprehensive data quality checks

### 🎯 Usage Examples

#### Basic Usage
```bash
# Refresh mode (recommended for regular sync)
python fetch_clickup_data.py --mode refresh --days 60

# Full reindex mode (for initial setup)
python fetch_clickup_data.py --mode full_reindex
```

#### Advanced Usage
```bash
# Custom BigQuery settings
python fetch_clickup_data.py --mode refresh \
  --project_id my-project \
  --dataset my_dataset \
  --staging_table my_staging \
  --fact_table my_fact

# Custom date range
python fetch_clickup_data.py --mode refresh --days 30
```

### 🔒 Security Improvements
- **No Hardcoded Secrets**: All credentials via environment variables
- **Secure .env**: Template file with placeholder values
- **Git Ignore**: Proper exclusion of sensitive files
- **Credential Validation**: Checks for required environment variables

### 📈 Performance Metrics
- **Tested with Real Data**: Successfully processed 1,496 time entries
- **Efficient Processing**: ~30 seconds for 60 days of data
- **Memory Optimized**: Minimal memory footprint
- **Scalable**: Handles large datasets efficiently

### 🐛 Bug Fixes
- **Data Type Issues**: Fixed string to integer conversion problems
- **BigQuery Schema**: Resolved schema update options conflicts
- **Missing Dependencies**: Added all required packages
- **Error Handling**: Improved error messages and recovery

### 📚 Documentation Updates
- **Comprehensive README**: Updated with all new features
- **CLI Reference**: Complete command-line argument documentation
- **Setup Guide**: Step-by-step installation and configuration
- **Examples**: Real-world usage examples

## [1.0.0] - 2025-10-06

### Initial Release
- Basic ClickUp to BigQuery pipeline
- Month-by-month data fetching
- CSV file generation
- BigQuery upload and merge operations
- Basic error handling
