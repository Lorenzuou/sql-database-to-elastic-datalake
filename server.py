import logging
from flask import Flask, jsonify
from datetime import datetime
import src.config as config
from src.es_connector import ElasticsearchConnector
from src.utils import ensure_index_exists, get_index_name

# Import route blueprints
from src.routes.ticket_routes import ticket_bp
from src.routes.data_source_routes import data_source_bp
from src.routes.user_routes import user_bp
from src.routes.module_routes import module_bp
from src.routes.status_routes import status_bp
from src.routes.label_routes import label_bp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask application
app = Flask(__name__)

# Initialize Elasticsearch connector
es_connector = ElasticsearchConnector()

# Store Elasticsearch client in app config
app.config['ES_CLIENT'] = es_connector.es_client

# Register blueprints with URL prefixes
app.register_blueprint(ticket_bp, url_prefix='/tickets')
app.register_blueprint(data_source_bp, url_prefix='/data-sources')
app.register_blueprint(user_bp, url_prefix='/users')
app.register_blueprint(module_bp, url_prefix='/modules')
app.register_blueprint(status_bp, url_prefix='/statuses')
app.register_blueprint(label_bp, url_prefix='/labels')

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat()
    }), 200


if __name__ == "__main__":
    # Ensure the index exists before starting the server
    ensure_index_exists(es_connector.es_client, get_index_name())

    # Start the server
    port = 5000  # Standardized port
    app.run(host='0.0.0.0', port=port, debug=True)
    logger.info(f"Server started on port {port}")
