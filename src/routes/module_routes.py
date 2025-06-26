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
module_bp = Blueprint('module', __name__)

# Initialize Elasticsearch connector
es_connector = ElasticsearchConnector()


def process_module_data(module_data):
    """
    Process module data into a document ready for Elasticsearch.
    
    Args:
        module_data: Dictionary containing module data
    
    Returns:
        tuple: (processed document, list of missing required fields)
    """
    # TODO: Implement module data preparation
    # For now, just return the data as is
    return module_data, []


def index_module_to_es(module_id, document):
    """
    Index a module document into Elasticsearch.
    
    Args:
        module_id: The ID of the module
        document: The document to index
    
    Returns:
        dict: The Elasticsearch response
    """
    # Get the index name
    index_name = get_index_name()
    
    # Index the document in Elasticsearch
    return es_connector.es_client.index(
        index=index_name,
        id=module_id,
        body=document,
        refresh=True
    )


@module_bp.route('/modules', methods=['POST'])
def add_module():
    """Add a new module to the Elasticsearch index"""
    try:
        # Get the JSON data from the request
        module_data = request.json
        
        if not module_data:
            logger.error("No data provided in request")
            return jsonify({"error": "No data provided"}), 400
        
        # Process the module data
        processed_doc, missing_fields = process_module_data(module_data)
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return jsonify({
                "error": f"Missing required fields: {missing_fields}"
            }), 400
        
        # Index the document in Elasticsearch
        result = index_module_to_es(processed_doc["module_id"], processed_doc)
        
        logger.info(f"Added new module: {processed_doc['module_id']}")
        
        return jsonify({
            "status": "success",
            "module_id": processed_doc["module_id"],
            "result": result
        }), 201
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing module: {str(e)}")
        return jsonify({"error": str(e)}), 500


@module_bp.route('/modules/batch', methods=['POST'])
def add_modules_batch():
    """Add multiple modules in a batch"""
    try:
        # Get the JSON data from the request
        batch_data = request.json
        
        if not batch_data or not isinstance(batch_data, list):
            error_msg = "Invalid batch format - must be a list of module objects"
            logger.error(error_msg)
            return jsonify({"error": error_msg}), 400
        
        if len(batch_data) == 0:
            return jsonify({"status": "success", "count": 0}), 200
            
        # Process each module
        successful = 0
        failed = 0
        results = []
        
        for module_data in batch_data:
            try:
                # Process the module data
                processed_doc, missing_fields = process_module_data(module_data)
                
                if missing_fields:
                    module_id = module_data.get("module_id", "unknown")
                    error_msg = f"Missing required fields: {missing_fields}"
                    results.append({
                        "module_id": module_id,
                        "status": "error", 
                        "error": error_msg
                    })
                    failed += 1
                    continue
                
                # Index the document in Elasticsearch
                index_module_to_es(processed_doc["module_id"], processed_doc)
                
                results.append({
                    "module_id": processed_doc["module_id"],
                    "status": "success"
                })
                successful += 1
                
            except Exception as e:
                logger.error(f"Error processing module in batch: {str(e)}")
                module_id = module_data.get("module_id", "unknown")
                results.append({
                    "module_id": module_id,
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


@module_bp.route('/modules/sync', methods=['POST'])
def sync_db_module():
    """
    Sync a specific module from database to Elasticsearch.
    This route is called when a module is added or updated in the database.
    """
    try:
        # Get the module ID from the request
        data = request.json
        
        if not data or "module_id" not in data:
            logger.error("No module_id provided in request")
            return jsonify({"error": "module_id is required"}), 400
            
        module_id = data["module_id"]
        logger.info(f"Received sync request for module ID: {module_id}")
        
        # Fetch module data from database
        # TODO: Implement database fetch logic
        
        # Process the module data
        # TODO: Implement module data processing
        
        # Index the document in Elasticsearch
        # TODO: Implement indexing logic
        
        return jsonify({
            "status": "success",
            "module_id": module_id,
            "message": "Module successfully synced to data lake"
        }), 200
            
    except Exception as e:
        logger.error(f"Error processing sync request: {str(e)}")
        return jsonify({"error": str(e)}), 500 