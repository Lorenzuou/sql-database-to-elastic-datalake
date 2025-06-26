import json
import uuid
import logging
import pandas as pd
import numpy as np
from datetime import datetime
import src.config as config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_index_name(index_type="denormalized_tickets"):
    """Generate standardized index name based on config prefix"""
    return f"{config.SYNC_CONFIG['index_prefix']}{index_type}"

def sanitize_document(doc):
    """
    Process document to ensure it can be serialized to JSON.
    Handle NaT, nan, numpy values, and other special cases.
    """
    if not isinstance(doc, dict):
        # Return non-dict values directly
        return doc
        
    sanitized = {}
    for k, v in doc.items():
        try:
            # Handle pandas NaT (Not a Time) values and None
            if v is None or pd.isna(v):
                sanitized[k] = None
            # Handle numpy int types
            elif hasattr(v, 'dtype') and np.issubdtype(v.dtype, np.integer):
                sanitized[k] = int(v)
            # Handle numpy float types
            elif hasattr(v, 'dtype') and np.issubdtype(v.dtype, np.floating):
                sanitized[k] = float(v) if not np.isnan(v) else None
            # Handle numpy bool types
            elif hasattr(v, 'dtype') and np.issubdtype(v.dtype, np.bool_):
                sanitized[k] = bool(v)
            # Handle UUIDs
            elif isinstance(v, uuid.UUID):
                sanitized[k] = str(v)
            # Handle pandas Timestamp
            elif isinstance(v, pd.Timestamp):
                sanitized[k] = v.isoformat() if not pd.isna(v) else None
            # Handle binary data - potentially causing serialization issues
            elif isinstance(v, bytes):
                sanitized[k] = v.decode('utf-8', errors='ignore')
            # Recursively handle dictionaries
            elif isinstance(v, dict):
                sanitized[k] = sanitize_document(v)
            # Recursively handle lists
            elif isinstance(v, list):
                sanitized[k] = [sanitize_document(item) for item in v]
            # Pass through everything else
            else:
                sanitized[k] = v
        except Exception as e:
            logger.error(f"Error sanitizing field {k}: {str(e)}")
            # Use a safe default if there's an error
            sanitized[k] = None
            
    return sanitized

def prepare_entity_data(entity_data, entity_type):
    """
    Prepare entity data for insertion by adding missing fields
    and validating required fields.
    
    Args:
        entity_data (dict): The entity data to prepare
        entity_type (str): The type of entity (ticket, datasource, status, etc.)
    
    Returns:
        tuple: (sanitized_doc, missing_fields)
    """
    # Define required fields for each entity type
    required_fields = {
        "ticket": ["ticket_number"],
        "datasource": ["name"],
        "status": ["name"],
        "module": ["name"],
        "label": ["name"],
        "user": ["email"]
    }
    
    # Get required fields for this entity type
    entity_required_fields = required_fields.get(entity_type, [])
    missing_fields = [field for field in entity_required_fields if field not in entity_data]
    
    if missing_fields:
        return None, missing_fields
    
    # Generate a new ID if not provided
    id_field = f"{entity_type}_id"
    if id_field not in entity_data:
        entity_data[id_field] = str(uuid.uuid4())
    
    # Add timestamps if not provided
    current_time = datetime.now().isoformat()
    created_at_field = f"{entity_type}_createdAt"
    updated_at_field = f"{entity_type}_updatedAt"
    
    if created_at_field not in entity_data:
        entity_data[created_at_field] = current_time
    if updated_at_field not in entity_data:
        entity_data[updated_at_field] = current_time
    
    # Handle JSON data field if present
    data_field = f"{entity_type}_data"
    if isinstance(entity_data.get(data_field), str):
        try:
            parsed_data = json.loads(entity_data[data_field])
            entity_data[data_field] = sanitize_document(parsed_data)
        except json.JSONDecodeError:
            logger.warning(f"Field '{data_field}' is not valid JSON for {entity_type} {entity_data.get(id_field)}")
    
    # Sanitize the document for Elasticsearch
    sanitized_doc = sanitize_document(entity_data)
    
    return sanitized_doc, []

def create_index_mapping(entity_type="ticket"):
    """
    Create the mappings for different entity types in Elasticsearch.
    
    Args:
        entity_type (str): The type of entity to create mapping for
    """
    base_mapping = {
        "settings": {
            "refresh_interval": config.SYNC_CONFIG['refresh_interval']
        },
        "mappings": {
            "properties": {}
        }
    }
    
    # Define mappings for different entity types
    entity_mappings = {
        "ticket": {
            "ticket_id": {"type": "keyword"},
            "ticket_number": {"type": "long"},
            "ticket_scheduleDate": {"type": "date"},
            "ticket_scheduleDateEnd": {"type": "date"},
            "ticket_data": {"type": "object"},
            "ticket_createdAt": {"type": "date"},
            "ticket_updatedAt": {"type": "date"},
            "status_id": {"type": "keyword"},
            "status_name": {"type": "keyword"},
            "status_isFinalStatus": {"type": "boolean"},
            "labels": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "color": {"type": "keyword"}
                }
            },
            "module_id": {"type": "keyword"},
            "module_name": {"type": "keyword"},
            "datasource_id": {"type": "keyword"},
            "datasource_name": {"type": "keyword"},
            "user_id": {"type": "keyword"},
            "user_name": {"type": "keyword"},
            "user_email": {"type": "keyword"}
        },
        "datasource": {
            "datasource_id": {"type": "keyword"},
            "name": {"type": "keyword"},
            "description": {"type": "text"},
            "datasource_data": {"type": "object"},
            "datasource_createdAt": {"type": "date"},
            "datasource_updatedAt": {"type": "date"}
        },
        "status": {
            "status_id": {"type": "keyword"},
            "name": {"type": "keyword"},
            "description": {"type": "text"},
            "isFinalStatus": {"type": "boolean"},
            "status_data": {"type": "object"},
            "status_createdAt": {"type": "date"},
            "status_updatedAt": {"type": "date"}
        },
        "module": {
            "module_id": {"type": "keyword"},
            "name": {"type": "keyword"},
            "description": {"type": "text"},
            "module_data": {"type": "object"},
            "module_createdAt": {"type": "date"},
            "module_updatedAt": {"type": "date"}
        },
        "label": {
            "label_id": {"type": "keyword"},
            "name": {"type": "keyword"},
            "color": {"type": "keyword"},
            "description": {"type": "text"},
            "label_data": {"type": "object"},
            "label_createdAt": {"type": "date"},
            "label_updatedAt": {"type": "date"}
        },
        "user": {
            "user_id": {"type": "keyword"},
            "name": {"type": "keyword"},
            "email": {"type": "keyword"},
            "user_data": {"type": "object"},
            "user_createdAt": {"type": "date"},
            "user_updatedAt": {"type": "date"}
        }
    }
    
    # Get the mapping for the specified entity type
    entity_mapping = entity_mappings.get(entity_type, {})
    base_mapping["mappings"]["properties"] = entity_mapping
    
    return base_mapping

def ensure_index_exists(es_client, index_name=None, entity_type="ticket"):
    """
    Check if an index exists, create it if it doesn't.
    
    Args:
        es_client: Elasticsearch client
        index_name: The name of the index to check/create.
                   If None, defaults to entity_type index.
        entity_type: The type of entity (ticket, datasource, etc.)
    
    Returns:
        bool: True if index exists or was created, False on error
    """
    if index_name is None:
        index_name = get_index_name(entity_type)
        
    try:
        if not es_client.indices.exists(index=index_name):
            logger.info(f"Creating index: {index_name}")
            mapping = create_index_mapping(entity_type)
            es_client.indices.create(
                index=index_name, 
                body=json.loads(json.dumps(mapping))
            )
            logger.info(f"Created index: {index_name}")
        return True
    except Exception as e:
        logger.error(f"Error ensuring index exists: {str(e)}")
        return False
