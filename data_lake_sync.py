import logging
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tqdm import tqdm
import config
from  json_encoder import json_serialize, CustomJSONEncoder
import json
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLakeSync:
    def __init__(self):
        self.db_engine = self._create_db_engine()
        self.es_client = self._create_es_client()
        self.inspector = inspect(self.db_engine)
        self.db_type = config.DB_CONFIG['db_type']

    def _create_db_engine(self):
        """Create SQLAlchemy engine based on database type."""
        db_type = config.DB_CONFIG['db_type']
        if db_type == 'postgresql':
            connection_string = (
                f"postgresql://{config.DB_CONFIG['user']}:"
                f"{config.DB_CONFIG['password']}@{config.DB_CONFIG['host']}:"
                f"{config.DB_CONFIG['port']}/{config.DB_CONFIG['database']}"
            )
        elif db_type == 'mysql':
            connection_string = (
                f"mysql+mysqlconnector://{config.DB_CONFIG['user']}:"
                f"{config.DB_CONFIG['password']}@{config.DB_CONFIG['host']}:"
                f"{config.DB_CONFIG['port']}/{config.DB_CONFIG['database']}"
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        return create_engine(connection_string)

    def _create_es_client(self):
        """Create Elasticsearch client."""
        es_url = f"{config.ES_CONFIG['scheme']}://{config.ES_CONFIG['host']}:{config.ES_CONFIG['port']}"
        return Elasticsearch([es_url])

    def get_table_names(self) -> List[str]:
        """Get all table names from the database."""
        return self.inspector.get_table_names()

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a table."""
        columns = self.inspector.get_columns(table_name)
        return {col['name']: str(col['type']) for col in columns}
        
    def _quote_table_name(self, table_name: str) -> str:
        """Quote table name according to database type."""
        if self.db_type == 'postgresql':
            return f'"{table_name}"'
        # MySQL uses backticks for quoting
        elif self.db_type == 'mysql':
            return f'`{table_name}`'
        return table_name

    def sync_table_to_elasticsearch(self, table_name: str):
        """Sync a single table to Elasticsearch."""
        # Convert table name to lowercase for Elasticsearch index
        index_name = f"{config.SYNC_CONFIG['index_prefix']}{table_name.lower()}"
        
        # Quote the table name for SQL queries
        quoted_table_name = self._quote_table_name(table_name)
        
        # Create index with mapping
        if not self.es_client.indices.exists(index=index_name):
            mapping = {
                "mappings": {
                    "dynamic": True,
                    "properties": {
                        field: {
                           "type": (
                                "object" if "json" in type_str.lower()
                                else "keyword" if "varchar" in type_str.lower()
                                else "text" if "text" in type_str.lower()
                                else "long" if "int" in type_str.lower()
                                else "double" if "float" in type_str.lower()
                                else "date" if "date" in type_str.lower()
                                else "text"
                            )
                        }
                        for field, type_str in self.get_table_schema(table_name).items()
                    }
                },
                "settings": {
                    "refresh_interval": config.SYNC_CONFIG['refresh_interval']
                }
            }
            # Use json_serialize for the mapping
            self.es_client.indices.create(index=index_name, body=json.loads(json_serialize(mapping)))

        # Get total count for progress bar using connection context
        count_query = text(f"SELECT COUNT(*) FROM {quoted_table_name}")
        with self.db_engine.connect() as connection:
            total_count = connection.execute(count_query).scalar()
        
        # Process data in batches
        offset = 0
        with tqdm(total=total_count, desc=f"Syncing {table_name}") as pbar:
            while True:
                query = text(
                    f"SELECT * FROM {quoted_table_name} "
                    f"LIMIT {config.SYNC_CONFIG['batch_size']} "
                    f"OFFSET {offset}"
                )
                df = pd.read_sql(query, self.db_engine)
                
                if df.empty:
                    break
                
                # Prepare documents for bulk indexing
                actions = []
                for _, row in df.iterrows():
                    doc = row.to_dict()

                    # Se o campo 'data' for string, converta para dict
                    if isinstance(doc.get('data'), str):
                        try:
                            doc['data'] = json.loads(doc['data'])
                        except json.JSONDecodeError:
                            logger.warning(f"Campo 'data' não é um JSON válido: {doc['data']}")

                    actions.append({
                        "_index": index_name,
                        "_source": json.loads(json_serialize(doc))
                    })

                
                # Bulk index documents
                success, failed = bulk(
                    self.es_client,
                    actions,
                    raise_on_error=False
                )
                if failed:
                    logger.warning(
                        f"Failed to index {len(failed)} documents "
                        f"for table {table_name}"
                    )
                
                offset += len(df)
                pbar.update(len(df))

    def sync_all_tables(self):
        """Sync all tables to Elasticsearch."""
        tables = self.get_table_names()
        logger.info(f"Found {len(tables)} tables to sync")
        
        for table in tables:
            logger.info(f"Starting sync for table: {table}")
            try:
                self.sync_table_to_elasticsearch(table)
                logger.info(f"Completed sync for table: {table}")
            except Exception as e:
                logger.error(f"Error syncing table {table}: {str(e)}")

    def setup_search_api(self):
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
            
            # Use json_serialize for the query
            results = self.es_client.search(
                index=f"{config.SYNC_CONFIG['index_prefix']}*",
                body=json.loads(json_serialize(query))
            )
            
            return jsonify(results['hits'])
        
        @app.route('/search/advanced', methods=['POST'])
        def advanced_search():
            query_body = request.json
            # Use json_serialize for the query_body
            results = self.es_client.search(
                index=f"{config.SYNC_CONFIG['index_prefix']}*",
                body=json.loads(json_serialize(query_body))
            )
            
            return jsonify(results['hits'])
        
        return app

def main():
    sync = DataLakeSync()
    sync.sync_all_tables()
    
    # Optionally start the search API
    if config.SEARCH_API_ENABLED:
        app = sync.setup_search_api()
        app.run(host='0.0.0.0', port=5000)

if __name__ == "__main__":
    main()