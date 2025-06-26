import logging
from flask import Blueprint, request, jsonify
import json
import uuid
from datetime import datetime
import pandas as pd
from src.document_utils import sanitize_document
from src.utils import get_index_name
from src.db_connector import DatabaseConnector

# Configure logging
logger = logging.getLogger(__name__)

# Create blueprint
data_source_bp = Blueprint('data_sources', __name__)

# Initialize database connector
db_connector = DatabaseConnector()

def process_data_source_data(df_data_sources, data_source_id):
    """
    Process data source data into a document ready for Elasticsearch.
    
    Args:
        df_data_sources: DataFrame containing data source data
        data_source_id: The ID of the data source
    
    Returns:
        dict: The processed document ready for indexing
    """
    # Process the single data source
    row = df_data_sources.iloc[0]
    
    # Create document with all fields
    doc = {}
    for col in df_data_sources.columns:
        val = row[col]
        # Convert UUID to string
        if isinstance(val, uuid.UUID):
            doc[col] = str(val)
        # Convert timestamps to strings
        elif isinstance(val, pd.Timestamp):
            doc[col] = val.isoformat() if not pd.isna(val) else None
        # Handle NaT and NaN
        elif pd.isna(val):
            doc[col] = None
        else:
            doc[col] = val
    
    # Handle JSON data field
    if isinstance(doc.get('dataMap'), str):
        try:
            parsed_data = json.loads(doc['dataMap'])
            doc['dataMap'] = sanitize_document(parsed_data)
        except json.JSONDecodeError:
            logger.warning(f"Field 'dataMap' is not valid JSON for data source {data_source_id}")
    
    # Sanitize document
    return sanitize_document(doc)

def index_data_source_to_es(es_client, data_source_id, document):
    """
    Index a data source document into Elasticsearch.
    
    Args:
        es_client: Elasticsearch client
        data_source_id: The ID of the data source
        document: The document to index
    
    Returns:
        dict: The Elasticsearch response
    """
    # Get the index name for data sources
    index_name = f"{get_index_name()}data_sources"
    
    # Index the document in Elasticsearch
    return es_client.index(
        index=index_name,
        id=data_source_id,
        body=document,
        refresh=True
    )

@data_source_bp.route('/data-sources', methods=['POST'])
def add_data_source(es_client):
    """Add a new data source to the Elasticsearch index"""
    try:
        # Get the JSON data from the request
        data_source_data = request.json
        
        if not data_source_data:
            logger.error("No data provided in request")
            return jsonify({"error": "No data provided"}), 400
        
        # Process and sanitize the data
        sanitized_doc = sanitize_document(data_source_data)
        
        # Index the document in Elasticsearch
        result = es_client.index(
            index=f"{get_index_name()}data_sources",
            id=sanitized_doc["id"],
            body=sanitized_doc,
            refresh=True
        )
        
        logger.info(f"Added new data source: {sanitized_doc['id']}")
        
        return jsonify({
            "status": "success",
            "data_source_id": sanitized_doc["id"],
            "result": result
        }), 201
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing data source: {str(e)}")
        return jsonify({"error": str(e)}), 500

@data_source_bp.route('/data-sources/batch', methods=['POST'])
def add_data_sources_batch(es_client):
    """Add multiple data sources in a batch"""
    try:
        # Get the JSON data from the request
        batch_data = request.json
        
        if not batch_data or not isinstance(batch_data, list):
            logger.error("Invalid batch format - must be a list of data source objects")
            return jsonify({"error": "Invalid batch format"}), 400
        
        if len(batch_data) == 0:
            return jsonify({"status": "success", "count": 0}), 200
            
        # Process each data source
        successful = 0
        failed = 0
        results = []
        
        for data_source_data in batch_data:
            try:
                # Process and sanitize the data
                sanitized_doc = sanitize_document(data_source_data)
                
                # Index the document in Elasticsearch
                es_client.index(
                    index=f"{get_index_name()}data_sources",
                    id=sanitized_doc["id"],
                    body=sanitized_doc,
                    refresh=True
                )
                
                results.append({
                    "data_source_id": sanitized_doc["id"],
                    "status": "success"
                })
                successful += 1
                
            except Exception as e:
                logger.error(f"Error processing data source in batch: {str(e)}")
                results.append({
                    "data_source_id": data_source_data.get("id", "unknown"),
                    "status": "error",
                    "error": str(e)
                })
                failed += 1
        
        # Ensure index is refreshed after batch
        es_client.indices.refresh(index=f"{get_index_name()}data_sources")
        
        return jsonify({
            "status": "completed",
            "successful": successful,
            "failed": failed,
            "results": results
        }), 200 if failed == 0 else 207
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        return jsonify({"error": str(e)}), 500

@data_source_bp.route('/data-sources/sync', methods=['POST'])
def sync_db_data_source(es_client):
    """
    Sync a specific data source from database to Elasticsearch.
    This route is called when a data source is added or updated in the database.
    """
    try:
        # Get the data source ID from the request
        data = request.json
        
        if not data or "data_source_id" not in data:
            logger.error("No data_source_id provided in request")
            return jsonify({"error": "data_source_id is required"}), 400
            
        data_source_id = data["data_source_id"]
        logger.info(f"Received sync request for data source ID: {data_source_id}")
        
        # Fetch data source data from database
        df_data_sources = db_connector.get_data_sources(data_source_id)
        
        if df_data_sources is None or len(df_data_sources) == 0:
            return jsonify({"error": f"Data source {data_source_id} not found"}), 404
        
        # Process the data source data
        processed_doc = process_data_source_data(df_data_sources, data_source_id)
        
        # Index the document in Elasticsearch
        result = index_data_source_to_es(es_client, data_source_id, processed_doc)

        logger.info(f"Indexing result for data source {data_source_id}: {result}")
        
        if result and (result.get('result') == 'created' or result.get('result') == 'updated'):
            logger.info(f"Successfully synced data source {data_source_id} to Elasticsearch")
            return jsonify({
                "status": "success",
                "data_source_id": data_source_id,
                "message": "Data source successfully synced to data lake"
            }), 200
        else:
            logger.warning(f"Elasticsearch indexing returned unexpected result: {result}")
            return jsonify({
                "status": "warning",
                "data_source_id": data_source_id,
                "message": "Data source sync completed with warnings",
                "result": result
            }), 200
            
    except Exception as e:
        logger.error(f"Error processing sync request: {str(e)}")
        return jsonify({"error": str(e)}), 500 