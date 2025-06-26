import logging
import json
import uuid
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

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
            # Special handling for numpy arrays and pandas Series
            if isinstance(v, (np.ndarray, pd.Series)):
                # Check if array is empty
                if v.size == 0:
                    sanitized[k] = []
                # If it's a single-element array, extract the value
                elif v.size == 1:
                    # Extract and sanitize the single value
                    single_val = v.item() if hasattr(v, 'item') else v[0]
                    if pd.isna(single_val):
                        sanitized[k] = None
                    else:
                        sanitized[k] = single_val
                else:
                    # For multi-element arrays, convert to a sanitized list
                    sanitized[k] = [
                        None if pd.isna(x) else 
                        x.item() if hasattr(x, 'item') else x
                        for x in v
                    ]
            # Handle pandas NaT (Not a Time) values and None
            elif v is None or (pd.api.types.is_scalar(v) and pd.isna(v)):
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

def process_ticket_labels(df_labels):
    """Group labels by ticket ID."""
    labels_by_ticket = {}
    for _, row in df_labels.iterrows():
        ticket_id = row["ticketId"]
        if isinstance(ticket_id, uuid.UUID):
            ticket_id = str(ticket_id)
        
        if ticket_id not in labels_by_ticket:
            labels_by_ticket[ticket_id] = []
            
        label_id = row["label_id"]
        if isinstance(label_id, uuid.UUID):
            label_id = str(label_id)
            
        labels_by_ticket[ticket_id].append({
            "id": label_id,
            "name": row["label_name"],
            "color": row["color"] if pd.notna(row["color"]) else None
        })
    
    return labels_by_ticket
