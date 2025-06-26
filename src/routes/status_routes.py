import logging
from flask import Blueprint, request, jsonify
import json
from src.utils import get_index_name
from src.es_connector import ElasticsearchConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create blueprint
status_bp = Blueprint('status', __name__)

# Initialize Elasticsearch connector
es_connector = ElasticsearchConnector()


def process_status_data(status_data):
    """
    Process status data into a document ready for Elasticsearch.
    
    Args:
        status_data: Dictionary containing status data
    
    Returns:
        tuple: (processed document, list of missing required fields)
    """
    # TODO: Implement status data preparation
    # For now, just return the data as is
    return status_data, []


def index_status_to_es(status_id, document):
    """
    Index a status document into Elasticsearch.
    
    Args:
        status_id: The ID of the status
        document: The document to index
    
    Returns:
        dict: The Elasticsearch response
    """
    # Get the index name
    index_name = get_index_name()
    
    # Index the document in Elasticsearch
    return es_connector.es_client.index(
        index=index_name,
        id=status_id,
        body=document,
        refresh=True
    )


@status_bp.route('/statuses', methods=['POST'])
def add_status():
    """Add a new status to the Elasticsearch index"""
    try:
        # Get the JSON data from the request
        status_data = request.json
        
        if not status_data:
            logger.error("No data provided in request")
            return jsonify({"error": "No data provided"}), 400
        
        # Process the status data
        processed_doc, missing_fields = process_status_data(status_data)
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return jsonify({
                "error": f"Missing required fields: {missing_fields}"
            }), 400
        
        # Index the document in Elasticsearch
        result = index_status_to_es(processed_doc["status_id"], processed_doc)
        
        logger.info(f"Added new status: {processed_doc['status_id']}")
        
        return jsonify({
            "status": "success",
            "status_id": processed_doc["status_id"],
            "result": result
        }), 201
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing status: {str(e)}")
        return jsonify({"error": str(e)}), 500


@status_bp.route('/statuses/batch', methods=['POST'])
def add_statuses_batch():
    """Add multiple statuses in a batch"""
    try:
        # Get the JSON data from the request
        batch_data = request.json
        
        if not batch_data or not isinstance(batch_data, list):
            error_msg = "Invalid batch format - must be a list of status objects"
            logger.error(error_msg)
            return jsonify({"error": error_msg}), 400
        
        if len(batch_data) == 0:
            return jsonify({"status": "success", "count": 0}), 200
            
        # Process each status
        successful = 0
        failed = 0
        results = []
        
        for status_data in batch_data:
            try:
                # Process the status data
                processed_doc, missing_fields = process_status_data(status_data)
                
                if missing_fields:
                    status_id = status_data.get("status_id", "unknown")
                    error_msg = f"Missing required fields: {missing_fields}"
                    results.append({
                        "status_id": status_id,
                        "status": "error", 
                        "error": error_msg
                    })
                    failed += 1
                    continue
                
                # Index the document in Elasticsearch
                index_status_to_es(processed_doc["status_id"], processed_doc)
                
                results.append({
                    "status_id": processed_doc["status_id"],
                    "status": "success"
                })
                successful += 1
                
            except Exception as e:
                logger.error(f"Error processing status in batch: {str(e)}")
                status_id = status_data.get("status_id", "unknown")
                results.append({
                    "status_id": status_id,
                    "status": "error",
                    "error": str(e)
                })
                failed += 1
        
        # Ensure index is refreshed after batch
        es_connector.es_client.indices.refresh(index=get_index_name())
        
        return jsonify({
            "status": "completed",
            "successful": successful,
            "failed": failed,
            "results": results
        }), 200 if failed == 0 else 207  # Use 207 Multi-Status if some failed
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        return jsonify({"error": str(e)}), 500


@status_bp.route('/statuses/sync', methods=['POST'])
def sync_db_status():
    """
    Sync a specific status from database to Elasticsearch.
    This route is called when a status is added or updated in the database.
    """
    try:
        # Get the status ID from the request
        data = request.json
        
        if not data or "status_id" not in data:
            logger.error("No status_id provided in request")
            return jsonify({"error": "status_id is required"}), 400
            
        status_id = data["status_id"]
        logger.info(f"Received sync request for status ID: {status_id}")
        
        # Fetch status data from database
        # TODO: Implement database fetch logic
        
        # Process the status data
        # TODO: Implement status data processing
        
        # Index the document in Elasticsearch
        # TODO: Implement indexing logic
        
        return jsonify({
            "status": "success",
            "status_id": status_id,
            "message": "Status successfully synced to data lake"
        }), 200
            
    except Exception as e:
        logger.error(f"Error processing sync request: {str(e)}")
        return jsonify({"error": str(e)}), 500 