import logging
from flask import Blueprint, request, jsonify, current_app
import json
import uuid
from datetime import datetime
import pandas as pd
from src.document_utils import sanitize_document
from src.utils import prepare_entity_data, get_index_name, ensure_index_exists
from src.db_connector import DatabaseConnector

# Configure logging
logger = logging.getLogger(__name__)

# Create blueprint
ticket_bp = Blueprint('tickets', __name__)

# Initialize database connector
db_connector = DatabaseConnector()

def process_ticket_data(df_tickets, df_labels, ticket_id):
    """
    Process ticket and label data into a document ready for Elasticsearch.
    
    Args:
        df_tickets: DataFrame containing ticket data
        df_labels: DataFrame containing label data
        ticket_id: The ID of the ticket
    
    Returns:
        dict: The processed document ready for indexing
    """
    # Process the single ticket
    row = df_tickets.iloc[0]
    
    # Create document with all fields
    doc = {}
    for col in df_tickets.columns:
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
    if isinstance(doc.get('ticket_data'), str):
        try:
            parsed_data = json.loads(doc['ticket_data'])
            doc['ticket_data'] = sanitize_document(parsed_data)
        except json.JSONDecodeError:
            logger.warning(f"Field 'ticket_data' is not valid JSON for ticket {ticket_id}")
    
    # Process labels
    labels = []
    if not df_labels.empty:
        for _, label_row in df_labels.iterrows():
            labels.append({
                "id": str(label_row["label_id"]) if isinstance(label_row["label_id"], uuid.UUID) else label_row["label_id"],
                "name": label_row["label_name"],
                "color": label_row["color"]
            })
    
    # Add labels array
    doc['labels'] = labels
    
    # Sanitize document
    return sanitize_document(doc)

def index_ticket_to_es(es_client, ticket_id, document):
    """
    Index a ticket document into Elasticsearch.
    
    Args:
        es_client: Elasticsearch client
        ticket_id: The ID of the ticket
        document: The document to index
    
    Returns:
        dict: The Elasticsearch response
    """
    # Get the index name for denormalized tickets
    index_name = get_index_name()
    
    # Index the document in Elasticsearch
    return es_client.index(
        index=index_name,
        id=ticket_id,
        body=document,
        refresh=True
    )

@ticket_bp.route('/tickets', methods=['POST'])
def add_ticket(es_client):
    """Add a new ticket to the Elasticsearch index"""
    try:
        # Get the JSON data from the request
        ticket_data = request.json
        
        if not ticket_data:
            logger.error("No data provided in request")
            return jsonify({"error": "No data provided"}), 400
        
        # Use shared function to prepare the ticket data
        sanitized_doc, missing_fields = prepare_entity_data(ticket_data, "ticket")
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return jsonify({"error": f"Missing required fields: {missing_fields}"}), 400
        
        # Index the document in Elasticsearch
        result = es_client.index(
            index=get_index_name(),
            id=sanitized_doc["ticket_id"],
            body=sanitized_doc,
            refresh=True  # Ensure document is immediately available for search
        )
        
        logger.info(f"Added new ticket: {sanitized_doc['ticket_id']}")
        
        return jsonify({
            "status": "success",
            "ticket_id": sanitized_doc["ticket_id"],
            "result": result
        }), 201
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return jsonify({"error": "Invalid JSON in request body"}), 400
    except Exception as e:
        logger.error(f"Error processing ticket: {str(e)}")
        return jsonify({"error": str(e)}), 500

@ticket_bp.route('/tickets/batch', methods=['POST'])
def add_tickets_batch(es_client):
    """Add multiple tickets in a batch"""
    try:
        # Get the JSON data from the request
        batch_data = request.json
        
        if not batch_data or not isinstance(batch_data, list):
            logger.error("Invalid batch format - must be a list of ticket objects")
            return jsonify({"error": "Invalid batch format - must be a list of ticket objects"}), 400
        
        if len(batch_data) == 0:
            return jsonify({"status": "success", "count": 0}), 200
            
        # Process each ticket
        successful = 0
        failed = 0
        results = []
        
        for ticket_data in batch_data:
            try:
                # Use shared function to prepare the ticket data
                sanitized_doc, missing_fields = prepare_entity_data(ticket_data, "ticket")
                
                if missing_fields:
                    results.append({
                        "ticket_id": ticket_data.get("ticket_id", "unknown"),
                        "status": "error", 
                        "error": f"Missing required fields: {missing_fields}"
                    })
                    failed += 1
                    continue
                
                # Index the document in Elasticsearch
                es_client.index(
                    index=get_index_name(),
                    id=sanitized_doc["ticket_id"],
                    body=sanitized_doc,
                    refresh=True
                )
                
                results.append({
                    "ticket_id": sanitized_doc["ticket_id"],
                    "status": "success"
                })
                successful += 1
                
            except Exception as e:
                logger.error(f"Error processing ticket in batch: {str(e)}")
                results.append({
                    "ticket_id": ticket_data.get("ticket_id", "unknown"),
                    "status": "error",
                    "error": str(e)
                })
                failed += 1
        
        # Ensure index is refreshed after batch
        es_client.indices.refresh(index=get_index_name())
        
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

@ticket_bp.route('/sync', methods=['POST'])
def sync_db_ticket():
    """
    Sync a specific ticket from database to Elasticsearch.
    This route is called when a ticket is added or updated in the database.
    """
    try:
        # Get the ticket ID from the request
        data = request.json
        
        if not data or "ticket_id" not in data:
            logger.error("No ticket_id provided in request")
            return jsonify({"error": "ticket_id is required"}), 400
            
        ticket_id = data["ticket_id"]
        logger.info(f"Received sync request for ticket ID: {ticket_id}")
        
        # Fetch ticket data from database
        df_tickets, df_labels = db_connector.get_tickets_and_labels(ticket_id)
        
        if df_tickets is None:
            return jsonify({"error": f"Ticket {ticket_id} not found or database error occurred"}), 404
        
        # Process the ticket data
        processed_doc = process_ticket_data(df_tickets, df_labels, ticket_id)
        
        # Get Elasticsearch client from application context
        es_client = current_app.config['ES_CLIENT']
        
        # Index the document in Elasticsearch
        result = index_ticket_to_es(es_client, ticket_id, processed_doc)

        logger.info(f"Indexing result for ticket {ticket_id}: {result}")
        
        if result and (result.get('result') == 'created' or result.get('result') == 'updated'):
            logger.info(f"Successfully synced ticket {ticket_id} to Elasticsearch")
            return jsonify({
                "status": "success",
                "ticket_id": ticket_id,
                "message": "Ticket successfully synced to data lake"
            }), 200
        else:
            logger.warning(f"Elasticsearch indexing returned unexpected result: {result}")
            return jsonify({
                "status": "warning",
                "ticket_id": ticket_id,
                "message": "Ticket sync completed with warnings",
                "result": result
            }), 200
            
    except Exception as e:
        logger.error(f"Error processing sync request: {str(e)}")
        return jsonify({"error": str(e)}), 500 