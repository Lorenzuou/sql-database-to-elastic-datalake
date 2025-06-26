import logging
import json
from elasticsearch import Elasticsearch
import src.config as config
from src.json_encoder import json_serialize
from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)


class ElasticsearchConnector:
    def __init__(self):
        self.es_client = self._create_es_client()

    def _create_es_client(self):
        """Create Elasticsearch client."""
        es_url = f"{config.ES_CONFIG['scheme']}://{config.ES_CONFIG['host']}:{config.ES_CONFIG['port']}"
        logger.info(f"Connecting to Elasticsearch at {es_url}")
        return Elasticsearch([es_url])
        
    def create_index(self, index_name, mapping=None):
        """Create an Elasticsearch index with optional mapping."""
        if self.es_client.indices.exists(index=index_name):
            logger.info(f"Deleting existing index: {index_name}")
            self.es_client.indices.delete(index=index_name)
            
        if mapping:
            logger.info(f"Creating index with mapping: {index_name}")
            self.es_client.indices.create(
                index=index_name, 
                body=json.loads(json_serialize(mapping))
            )
        else:
            logger.info(f"Creating index without mapping: {index_name}")
            self.es_client.indices.create(index=index_name)
            
    def index_document(self, index_name, doc_id, document, refresh=False):
        """Index a single document."""
        try:
            result = self.es_client.index(
                index=index_name,
                id=doc_id,
                body=document,
                refresh=refresh
            )
            return result
        except Exception as e:
            logger.error(f"Error indexing document {doc_id}: {str(e)}")
            return None
    
    def bulk_index(self, actions, refresh=False):
        """Bulk index multiple documents.
        
        Args:
            actions: List of actions for bulk API
            refresh: Whether to refresh the index after indexing
            
        Returns:
            tuple: (success_count, error_items)
        """
        try:
            success, failed = bulk(
                client=self.es_client,
                actions=actions,
                refresh=refresh,
                raise_on_error=False,
                stats_only=False
            )
            if failed:
                logger.warning(f"Bulk indexing completed with {len(failed)} errors")
            return success, failed
        except Exception as e:
            logger.error(f"Error during bulk indexing: {str(e)}")
            return 0, []
            
    def get_document_count(self, index_name):
        """Get number of documents in an index."""
        try:
            count = self.es_client.count(index=index_name)
            return count.get('count', 0)
        except Exception as e:
            logger.error(f"Error counting documents in {index_name}: {str(e)}")
            return 0
            
    def search(self, index_pattern, query_body):
        """Execute a search query."""
        try:
            results = self.es_client.search(
                index=index_pattern,
                body=json.loads(json_serialize(query_body))
            )
            return results
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            return {"hits": {"total": {"value": 0}, "hits": []}}
