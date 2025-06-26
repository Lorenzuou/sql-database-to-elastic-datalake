import json
import uuid
from datetime import datetime
from pandas import Timestamp

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle UUID objects
        if isinstance(obj, uuid.UUID):
            return str(obj)
        
        # Handle pandas Timestamp objects
        if isinstance(obj, Timestamp):
            return obj.isoformat()
        
        # Handle datetime objects (just in case)
        if isinstance(obj, datetime):
            return obj.isoformat()

        # Let the base class handle everything else
        return super().default(obj)

def _attempt_parse_json_string(value):
    """Try to parse a string as JSON. Return original if it fails or is not a dict/list."""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, (dict, list)):
                return parsed
        except Exception:
            pass
    return value

def _normalize_json_fields(data):
    """Recursively convert stringified JSON fields into real objects."""
    if isinstance(data, dict):
        return {k: _normalize_json_fields(_attempt_parse_json_string(v)) for k, v in data.items()}
    elif isinstance(data, list):
        return [_normalize_json_fields(item) for item in data]
    else:
        return data

def json_serialize(data):
    """Serialize data to JSON with custom type handling and JSON string cleanup."""
    normalized = _normalize_json_fields(data)
    return json.dumps(normalized, cls=CustomJSONEncoder)
