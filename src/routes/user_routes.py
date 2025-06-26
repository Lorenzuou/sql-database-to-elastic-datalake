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
user_bp = Blueprint('users', __name__)

# Initialize database connector
db_connector = DatabaseConnector()


def process_user_data(df_users, user_id):
    """
    Process user data into a document ready for Elasticsearch.
    
    Args:
        df_users: DataFrame containing user data
        user_id: The ID of the user
    
    Returns:
        dict: The processed document ready for indexing
    """
    # Process the single user
    row = df_users.iloc[0]
    
    # Create document with all fields
    doc = {}
    for col in df_users.columns:
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
    if isinstance(doc.get('preferences'), str):
        try:
            parsed_data = json.loads(doc['preferences'])
            doc['preferences'] = sanitize_document(parsed_data)
        except json.JSONDecodeError:
            logger.warning(f"Field 'preferences' is not valid JSON for user {user_id}")
    
    # Sanitize document
    return sanitize_document(doc)


def index_user_to_es(es_client, user_id, document):
    """
    Index a user document into Elasticsearch.
    
    Args:
        es_client: Elasticsearch client
        user_id: The ID of the user
        document: The document to index
    
    Returns:
        dict: The Elasticsearch response
    """
    # Get the index name for users
    index_name = f"{get_index_name()}users"
    
    # Index the document in Elasticsearch
    return es_client.index(
        index=index_name,
        id=user_id,
        body=document,
        refresh=True
    )


@user_bp.route('/users', methods=['POST'])
def add_user(es_client):
    """Add a new user to the Elasticsearch index"""
    try:
        # Get the JSON data from the request
        user_data = request.json
        
        if not user_data:
            logger.error("No data provided in request")
            return jsonify({"error": "No data provided"}), 400
        
        # Process and sanitize the data
        sanitized_doc = sanitize_document(user_data)
        
        # Index the document in Elasticsearch
        result = es_client.index(
            index=f"{get_index_name()}users",
            id=sanitized_doc["id"],
            body=sanitized_doc,
            refresh=True
        )
        
        logger.info(f"Added new user: {sanitized_doc['id']}")
        
        return jsonify({
            "status": "success",
            "user_id": sanitized_doc["id"],
            "result": result
        }), 201
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing user: {str(e)}")
        return jsonify({"error": str(e)}), 500


@user_bp.route('/users/batch', methods=['POST'])
def add_users_batch(es_client):
    """Add multiple users in a batch"""
    try:
        # Get the JSON data from the request
        batch_data = request.json
        
        if not batch_data or not isinstance(batch_data, list):
            logger.error("Invalid batch format - must be a list of user objects")
            return jsonify({"error": "Invalid batch format"}), 400
        
        if len(batch_data) == 0:
            return jsonify({"status": "success", "count": 0}), 200
            
        # Process each user
        successful = 0
        failed = 0
        results = []
        
        for user_data in batch_data:
            try:
                # Process and sanitize the data
                sanitized_doc = sanitize_document(user_data)
                
                # Index the document in Elasticsearch
                es_client.index(
                    index=f"{get_index_name()}users",
                    id=sanitized_doc["id"],
                    body=sanitized_doc,
                    refresh=True
                )
                
                results.append({
                    "user_id": sanitized_doc["id"],
                    "status": "success"
                })
                successful += 1
                
            except Exception as e:
                logger.error(f"Error processing user in batch: {str(e)}")
                results.append({
                    "user_id": user_data.get("id", "unknown"),
                    "status": "error",
                    "error": str(e)
                })
                failed += 1
        
        # Ensure index is refreshed after batch
        es_client.indices.refresh(index=f"{get_index_name()}users")
        
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


@user_bp.route('/users/sync', methods=['POST'])
def sync_db_user(es_client):
    """
    Sync a specific user from database to Elasticsearch.
    This route is called when a user is added or updated in the database.
    """
    try:
        # Get the user ID from the request
        data = request.json
        
        if not data or "user_id" not in data:
            logger.error("No user_id provided in request")
            return jsonify({"error": "user_id is required"}), 400
            
        user_id = data["user_id"]
        logger.info(f"Received sync request for user ID: {user_id}")
        
        # Fetch user data from database
        df_users = db_connector.get_users(user_id)
        
        if df_users is None or len(df_users) == 0:
            return jsonify({"error": f"User {user_id} not found"}), 404
        
        # Process the user data
        processed_doc = process_user_data(df_users, user_id)
        
        # Index the document in Elasticsearch
        result = index_user_to_es(es_client, user_id, processed_doc)

        logger.info(f"Indexing result for user {user_id}: {result}")
        
        if result and (result.get('result') == 'created' or result.get('result') == 'updated'):
            logger.info(f"Successfully synced user {user_id} to Elasticsearch")
            return jsonify({
                "status": "success",
                "user_id": user_id,
                "message": "User successfully synced to data lake"
            }), 200
        else:
            logger.warning(f"Elasticsearch indexing returned unexpected result: {result}")
            return jsonify({
                "status": "warning",
                "user_id": user_id,
                "message": "User sync completed with warnings",
                "result": result
            }), 200
            
    except Exception as e:
        logger.error(f"Error processing sync request: {str(e)}")
        return jsonify({"error": str(e)}), 500 