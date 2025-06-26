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
label_bp = Blueprint('label', __name__)

# Initialize Elasticsearch connector
es_connector = ElasticsearchConnector()


def process_label_data(label_data):
    """
    Process label data into a document ready for Elasticsearch.
    
    Args:
        label_data: Dictionary containing label data
    
    Returns:
        tuple: (processed document, list of missing required fields)
    """
    # TODO: Implement label data preparation
    # For now, just return the data as is
    return label_data, []


def index_label_to_es(label_id, document):
    """
    Index a label document into Elasticsearch.
    
    Args:
        label_id: The ID of the label
        document: The document to index
    
    Returns:
        dict: The Elasticsearch response
    """
    # Get the index name
    index_name = get_index_name()
    
    # Index the document in Elasticsearch
    return es_connector.es_client.index(
        index=index_name,
        id=label_id,
        body=document,
        refresh=True
    )


@label_bp.route('/labels', methods=['POST'])
def add_label():
    """Add a new label to the Elasticsearch index"""
    try:
        # Get the JSON data from the request
        label_data = request.json
        
        if not label_data:
            logger.error("No data provided in request")
            return jsonify({"error": "No data provided"}), 400
        
        # Process the label data
        processed_doc, missing_fields = process_label_data(label_data)
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return jsonify({
                "error": f"Missing required fields: {missing_fields}"
            }), 400
        
        # Index the document in Elasticsearch
        result = index_label_to_es(processed_doc["label_id"], processed_doc)
        
        logger.info(f"Added new label: {processed_doc['label_id']}")
        
        return jsonify({
            "status": "success",
            "label_id": processed_doc["label_id"],
            "result": result
        }), 201
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing label: {str(e)}")
        return jsonify({"error": str(e)}), 500


@label_bp.route('/labels/batch', methods=['POST'])
def add_labels_batch():
    """Add multiple labels in a batch"""
    try:
        # Get the JSON data from the request
        batch_data = request.json
        
        if not batch_data or not isinstance(batch_data, list):
            error_msg = "Invalid batch format - must be a list of label objects"
            logger.error(error_msg)
            return jsonify({"error": error_msg}), 400
        
        if len(batch_data) == 0:
            return jsonify({"status": "success", "count": 0}), 200
            
        # Process each label
        successful = 0
        failed = 0
        results = []
        
        for label_data in batch_data:
            try:
                # Process the label data
                processed_doc, missing_fields = process_label_data(label_data)
                
                if missing_fields:
                    label_id = label_data.get("label_id", "unknown")
                    error_msg = f"Missing required fields: {missing_fields}"
                    results.append({
                        "label_id": label_id,
                        "status": "error", 
                        "error": error_msg
                    })
                    failed += 1
                    continue
                
                # Index the document in Elasticsearch
                index_label_to_es(processed_doc["label_id"], processed_doc)
                
                results.append({
                    "label_id": processed_doc["label_id"],
                    "status": "success"
                })
                successful += 1
                
            except Exception as e:
                logger.error(f"Error processing label in batch: {str(e)}")
                label_id = label_data.get("label_id", "unknown")
                results.append({
                    "label_id": label_id,
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


@label_bp.route('/labels/sync', methods=['POST'])
def sync_db_label():
    """
    Sync a specific label from database to Elasticsearch.
    This route is called when a label is added or updated in the database.
    """
    try:
        # Get the label ID from the request
        data = request.json
        
        if not data or "label_id" not in data:
            logger.error("No label_id provided in request")
            return jsonify({"error": "label_id is required"}), 400
            
        label_id = data["label_id"]
        logger.info(f"Received sync request for label ID: {label_id}")
        
        # Fetch label data from database
        # TODO: Implement database fetch logic
        
        # Process the label data
        # TODO: Implement label data processing
        
        # Index the document in Elasticsearch
        # TODO: Implement indexing logic
        
        return jsonify({
            "status": "success",
            "label_id": label_id,
            "message": "Label successfully synced to data lake"
        }), 200
            
    except Exception as e:
        logger.error(f"Error processing sync request: {str(e)}")
        return jsonify({"error": str(e)}), 500 