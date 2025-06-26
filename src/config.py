import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'your_database'),
    'user': os.getenv('DB_USER', 'your_user'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'db_type': os.getenv('DB_TYPE', 'postgresql')  # postgresql or mysql
}

# Elasticsearch configuration
ES_CONFIG = {
    'host': os.getenv('ES_HOST', 'localhost'),
    'port': os.getenv('ES_PORT', '9200'),
    'scheme': os.getenv('ES_SCHEME', 'http'),
    'username': None,  # Security disabled in Docker setup
    'password': None   # Security disabled in Docker setup
}

# Sync configuration
SYNC_CONFIG = {
    'batch_size': 1000,
    'index_prefix': 'data_lake_',
    'refresh_interval': '1s'
} 



SEARCH_API_ENABLED = os.getenv('SEARCH_API_ENABLED', 'false').lower() == 'true'