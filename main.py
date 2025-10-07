#!/usr/bin/env python3
"""
Flask wrapper for Cloud Run deployment.
Provides HTTP endpoints for ClickUp to BigQuery sync operations.
"""

import os
import sys
from flask import Flask, request, jsonify
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route('/sync/refresh', methods=['POST'])
def sync_refresh():
    """
    Refresh mode - sync last 60 days.
    
    This endpoint runs the pipeline in refresh mode, fetching only
    the most recent data (last 60 days) and using windowed delete
    in BigQuery to update only recent records.
    """
    try:
        logger.info("Starting refresh sync (60 days)...")
        
        # Set CLI arguments programmatically
        sys.argv = ['fetch_clickup_data.py', '--mode', 'refresh', '--days', '60']
        
        # Import and run main function
        from fetch_clickup_data import main
        main()
        
        logger.info("Refresh sync completed successfully")
        return jsonify({
            'status': 'success',
            'mode': 'refresh',
            'days': 60,
            'message': 'ClickUp refresh sync completed successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Refresh sync failed: {e}", exc_info=True)
        return jsonify({
            'status': 'error',
            'mode': 'refresh',
            'error': str(e)
        }), 500


@app.route('/sync/full_reindex', methods=['POST'])
def sync_full_reindex():
    """
    Full reindex mode - sync all data since 2024.
    
    This endpoint runs the pipeline in full reindex mode, fetching
    all historical data from January 2024 to present and performing
    a complete replacement of the BigQuery fact table.
    """
    try:
        logger.info("Starting full reindex...")
        
        # Set CLI arguments programmatically
        sys.argv = ['fetch_clickup_data.py', '--mode', 'full_reindex']
        
        # Import and run main function
        from fetch_clickup_data import main
        main()
        
        logger.info("Full reindex completed successfully")
        return jsonify({
            'status': 'success',
            'mode': 'full_reindex',
            'message': 'ClickUp full reindex completed successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Full reindex failed: {e}", exc_info=True)
        return jsonify({
            'status': 'error',
            'mode': 'full_reindex',
            'error': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.
    
    Returns the service status and version information.
    Used by Cloud Run for container health monitoring.
    """
    return jsonify({
        'status': 'healthy',
        'service': 'clickup-bigquery-sync',
        'version': '2.0.0'
    }), 200


@app.route('/', methods=['GET'])
def root():
    """
    Root endpoint with service information.
    
    Returns an overview of available endpoints and their usage.
    """
    return jsonify({
        'service': 'ClickUp to BigQuery Sync Pipeline',
        'version': '2.0.0',
        'endpoints': {
            '/sync/refresh': {
                'method': 'POST',
                'description': 'Sync last 60 days of data',
                'use_case': 'Regular scheduled updates'
            },
            '/sync/full_reindex': {
                'method': 'POST',
                'description': 'Full reindex since 2024',
                'use_case': 'Quarterly validation or after data issues'
            },
            '/health': {
                'method': 'GET',
                'description': 'Health check endpoint',
                'use_case': 'Container health monitoring'
            }
        },
        'schedule': {
            'refresh': 'Every 6 hours',
            'full_reindex': 'Quarterly (Jan 1, Apr 1, Jul 1, Oct 1)'
        }
    }), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting ClickUp BigQuery Sync service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
