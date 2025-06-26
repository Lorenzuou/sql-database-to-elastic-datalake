import logging
import json
from flask import Flask, request, jsonify
import src.config as config
from src.json_encoder import CustomJSONEncoder

logger = logging.getLogger(__name__)

def create_search_api(es_connector):
    """Set up a Flask API for searching across the data lake."""
    app = Flask(__name__)
    
    # Configure Flask to use our CustomJSONEncoder
    app.json_encoder = CustomJSONEncoder
    
    @app.route('/search', methods=['POST'])
    def search():
        query_data = request.json
        search_term = query_data.get('search_term')
        fields = query_data.get('fields', ['*'])
        
        query = {
            "query": {
                "multi_match": {
                    "query": search_term,
                    "fields": fields if fields != ['*'] else ["*"]
                }
            }
        }
        
        results = es_connector.search(
            index_pattern=f"{config.SYNC_CONFIG['index_prefix']}*",
            query_body=query
        )
        
        return jsonify(results['hits'])
    
    @app.route('/search/advanced', methods=['POST'])
    def advanced_search():
        query_body = request.json
        results = es_connector.search(
            index_pattern=f"{config.SYNC_CONFIG['index_prefix']}*",
            query_body=query_body
        )
        
        return jsonify(results['hits'])
    
    return app
