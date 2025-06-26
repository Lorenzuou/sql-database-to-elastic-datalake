import logging
import json
import uuid
import pandas as pd
from tqdm import tqdm
import src.config as config
from src.db_connector import DatabaseConnector
from src.es_connector import ElasticsearchConnector
from src.document_utils import sanitize_document
from src.json_encoder import CustomJSONEncoder
from datetime import datetime

logger = logging.getLogger(__name__)

class DataLakeSync:
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.es_connector = ElasticsearchConnector()

    def sync_data_sources(self):
        """Create and sync data source data to Elasticsearch."""
        index_name = f"{config.SYNC_CONFIG['index_prefix']}data_sources"
        
        # Define mapping for data sources
        mapping = {
            "mappings": {
                "properties": {
                    "document_id": {"type": "keyword"},
                    "indexed_at": {"type": "date"},
                    "data_source_id": {"type": "keyword"},
                    "data_source_name": {"type": "keyword"},
                    "data_source_description": {"type": "text"},
                    "data_source_dataMap": {"type": "object"},
                    "data_source_entityName": {"type": "keyword"},
                    "data_source_coverVisibleData": {"type": "text"},
                    "data_source_gatewayType": {"type": "keyword"},
                    "data_source_gatewayId": {"type": "keyword"},
                    "data_source_moduleId": {"type": "keyword"},
                    "data_source_statusId": {"type": "keyword"},
                    "data_source_voidStatusId": {"type": "keyword"},
                    "data_source_dailyLimit": {"type": "integer"},
                    "data_source_wipEnabled": {"type": "boolean"},
                    "data_source_wipValue": {"type": "integer"},
                    "data_source_createdAt": {"type": "date"},
                    "data_source_updatedAt": {"type": "date"}
                }
            },
            "settings": {
                "refresh_interval": config.SYNC_CONFIG['refresh_interval']
            }
        }
        
        # Create the Elasticsearch index
        self.es_connector.create_index(index_name, mapping)
        
        # Get data sources from database
        df_data_sources = self.db_connector.get_data_sources()
        if df_data_sources is None or len(df_data_sources) == 0:
            logger.error("No data sources available to sync.")
            return
        
        # Get total count for progress bar
        total_count = len(df_data_sources)
        
        # Process data in batches for Elasticsearch
        successful_docs = 0
        failed_docs = 0
        
        with tqdm(total=total_count, desc="Syncing data sources") as pbar:
            batch_size = min(config.SYNC_CONFIG.get('batch_size', 100), 50)
            logger.info(f"Using batch size of {batch_size} for indexing")
            
            # Get current timestamp for this indexing run
            index_timestamp = datetime.utcnow().isoformat()
            
            # Process in batches
            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                batch_df = df_data_sources.iloc[batch_start:batch_end]
                
                # Prepare batch of actions for bulk indexing
                bulk_actions = []
                
                for idx, row in batch_df.iterrows():
                    try:
                        data_source_id = row["id"]
                        if isinstance(data_source_id, uuid.UUID):
                            data_source_id = str(data_source_id)
                        
                        # Create document with all fields
                        doc = {}
                        # Map fields according to our schema
                        doc['data_source_id'] = str(data_source_id)
                        doc['data_source_name'] = row.get('name')
                        doc['data_source_description'] = row.get('description')
                        doc['data_source_dataMap'] = row.get('dataMap')
                        doc['data_source_entityName'] = row.get('entityName')
                        doc['data_source_coverVisibleData'] = row.get('coverVisibleData')
                        doc['data_source_gatewayType'] = row.get('gatewayType')
                        doc['data_source_gatewayId'] = row.get('gatewayId')
                        doc['data_source_moduleId'] = str(row.get('moduleId')) if row.get('moduleId') else None
                        doc['data_source_statusId'] = str(row.get('statusId')) if row.get('statusId') else None
                        doc['data_source_voidStatusId'] = str(row.get('voidStatusId')) if row.get('voidStatusId') else None
                        doc['data_source_dailyLimit'] = row.get('dailyLimit')
                        doc['data_source_wipEnabled'] = row.get('wipEnabled')
                        doc['data_source_wipValue'] = row.get('wipValue')
                        doc['data_source_createdAt'] = row.get('createdAt')
                        doc['data_source_updatedAt'] = row.get('updatedAt')
                        
                        # Add historical tracking fields
                        doc['indexed_at'] = index_timestamp
                        
                        # Generate a unique document ID combining data source ID and timestamp
                        document_id = f"{data_source_id}_{index_timestamp}"
                        doc['document_id'] = document_id
                        
                        # Thoroughly sanitize document
                        sanitized_doc = sanitize_document(doc)
                        
                        # Verify document is serializable before adding to batch
                        try:
                            json_str = json.dumps(sanitized_doc, cls=CustomJSONEncoder)
                            # Create action for bulk API
                            action = {
                                "_op_type": "index",
                                "_index": index_name,
                                "_id": document_id,
                                "_source": sanitized_doc
                            }
                            bulk_actions.append(action)
                        except Exception as json_err:
                            logger.error(f"Document not serializable for ID {data_source_id}: {str(json_err)}")
                            # Try one more sanitization pass with default serialization
                            try:
                                json_str = json.dumps(sanitized_doc, default=str)
                                sanitized_doc = json.loads(json_str)
                                action = {
                                    "_op_type": "index",
                                    "_index": index_name,
                                    "_id": document_id,
                                    "_source": sanitized_doc
                                }
                                bulk_actions.append(action)
                            except:
                                logger.error(f"Failed to sanitize document {data_source_id} even with default serializer, skipping")
                                failed_docs += 1
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error processing document at index {idx}: {str(e)}")
                        failed_docs += 1
                
                # Execute bulk indexing for the batch
                if bulk_actions:
                    success_count, failed_items = self.es_connector.bulk_index(
                        actions=bulk_actions,
                        refresh=(batch_end == total_count)  # Only refresh on the last batch
                    )
                    
                    successful_docs += success_count
                    failed_docs += len(failed_items)
                    
                    # Log any errors from bulk operation
                    for error in failed_items:
                        if 'index' in error and 'error' in error['index']:
                            logger.error(f"Bulk indexing error: {error['index']['error']}")
                
                pbar.update(batch_end - batch_start)
                
                # Report progress after each batch
                logger.info(f"Progress: {successful_docs} indexed successfully, {failed_docs} failed")
                
                # Verify documents are showing up every few batches
                if batch_start % (batch_size * 5) == 0 or batch_end == total_count:
                    count = self.es_connector.get_document_count(index_name)
                    logger.info(f"Current document count in ES: {count}")
        
        # Verify the documents were indexed
        final_count = self.es_connector.get_document_count(index_name)
        logger.info(f"Final document count in Elasticsearch index {index_name}: {final_count}")
        
        if final_count == 0:
            logger.error("No documents were indexed to Elasticsearch!")
            logger.error("Possible causes:")
            logger.error("1. Serialization errors when converting documents to JSON")
            logger.error("2. Connection issues during bulk indexing")
            logger.error("3. Mapping incompatibilities")
            logger.error("4. Date format issues in timestamp fields")
        elif final_count < total_count:
            logger.warning(f"Only {final_count} of {total_count} documents were indexed!")
            logger.warning("Some documents failed to index. Check the logs for specific errors.")
        else:
            logger.info("All documents were successfully indexed!")

    def sync_users(self):
        """Create and sync user data to Elasticsearch."""
        index_name = f"{config.SYNC_CONFIG['index_prefix']}users"
        
        # Define mapping for users
        mapping = {
            "mappings": {
                "properties": {
                    "document_id": {"type": "keyword"},
                    "indexed_at": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "user_name": {"type": "keyword"},
                    "user_username": {"type": "keyword"},
                    "user_email": {"type": "keyword"},
                    "user_preferences": {"type": "object"},
                    "user_createdAt": {"type": "date"},
                    "user_updatedAt": {"type": "date"}
                }
            },
            "settings": {
                "refresh_interval": config.SYNC_CONFIG['refresh_interval']
            }
        }
        
        # Create the Elasticsearch index
        self.es_connector.create_index(index_name, mapping)
        
        # Get users from database
        df_users = self.db_connector.get_users()
        if df_users is None or len(df_users) == 0:
            logger.error("No users available to sync.")
            return
        
        # Get total count for progress bar
        total_count = len(df_users)
        
        # Process data in batches for Elasticsearch
        successful_docs = 0
        failed_docs = 0
        
        with tqdm(total=total_count, desc="Syncing users") as pbar:
            batch_size = min(config.SYNC_CONFIG.get('batch_size', 100), 50)
            logger.info(f"Using batch size of {batch_size} for indexing")
            
            # Get current timestamp for this indexing run
            index_timestamp = datetime.utcnow().isoformat()
            
            # Process in batches
            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                batch_df = df_users.iloc[batch_start:batch_end]
                
                # Prepare batch of actions for bulk indexing
                bulk_actions = []
                
                for idx, row in batch_df.iterrows():
                    try:
                        user_id = row["id"]
                        if isinstance(user_id, uuid.UUID):
                            user_id = str(user_id)
                        
                        # Create document with all fields
                        doc = {}
                        # Map fields according to our schema
                        doc['user_id'] = str(user_id)
                        doc['user_name'] = row.get('name')
                        doc['user_username'] = row.get('username')
                        doc['user_email'] = row.get('email')
                        doc['user_preferences'] = row.get('preferences')
                        doc['user_createdAt'] = row.get('createdAt')
                        doc['user_updatedAt'] = row.get('updatedAt')
                        
                        # Add historical tracking fields
                        doc['indexed_at'] = index_timestamp
                        
                        # Generate a unique document ID combining user ID and timestamp
                        document_id = f"{user_id}_{index_timestamp}"
                        doc['document_id'] = document_id
                        
                        # Thoroughly sanitize document
                        sanitized_doc = sanitize_document(doc)
                        
                        # Verify document is serializable before adding to batch
                        try:
                            json_str = json.dumps(sanitized_doc, cls=CustomJSONEncoder)
                            # Create action for bulk API
                            action = {
                                "_op_type": "index",
                                "_index": index_name,
                                "_id": document_id,
                                "_source": sanitized_doc
                            }
                            bulk_actions.append(action)
                        except Exception as json_err:
                            logger.error(f"Document not serializable for ID {user_id}: {str(json_err)}")
                            # Try one more sanitization pass with default serialization
                            try:
                                json_str = json.dumps(sanitized_doc, default=str)
                                sanitized_doc = json.loads(json_str)
                                action = {
                                    "_op_type": "index",
                                    "_index": index_name,
                                    "_id": document_id,
                                    "_source": sanitized_doc
                                }
                                bulk_actions.append(action)
                            except:
                                logger.error(f"Failed to sanitize document {user_id} even with default serializer, skipping")
                                failed_docs += 1
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error processing document at index {idx}: {str(e)}")
                        failed_docs += 1
                
                # Execute bulk indexing for the batch
                if bulk_actions:
                    success_count, failed_items = self.es_connector.bulk_index(
                        actions=bulk_actions,
                        refresh=(batch_end == total_count)  # Only refresh on the last batch
                    )
                    
                    successful_docs += success_count
                    failed_docs += len(failed_items)
                    
                    # Log any errors from bulk operation
                    for error in failed_items:
                        if 'index' in error and 'error' in error['index']:
                            logger.error(f"Bulk indexing error: {error['index']['error']}")
                
                pbar.update(batch_end - batch_start)
                
                # Report progress after each batch
                logger.info(f"Progress: {successful_docs} indexed successfully, {failed_docs} failed")
                
                # Verify documents are showing up every few batches
                if batch_start % (batch_size * 5) == 0 or batch_end == total_count:
                    count = self.es_connector.get_document_count(index_name)
                    logger.info(f"Current document count in ES: {count}")
        
        # Verify the documents were indexed
        final_count = self.es_connector.get_document_count(index_name)
        logger.info(f"Final document count in Elasticsearch index {index_name}: {final_count}")
        
        if final_count == 0:
            logger.error("No documents were indexed to Elasticsearch!")
            logger.error("Possible causes:")
            logger.error("1. Serialization errors when converting documents to JSON")
            logger.error("2. Connection issues during bulk indexing")
            logger.error("3. Mapping incompatibilities")
            logger.error("4. Date format issues in timestamp fields")
        elif final_count < total_count:
            logger.warning(f"Only {final_count} of {total_count} documents were indexed!")
            logger.warning("Some documents failed to index. Check the logs for specific errors.")
        else:
            logger.info("All documents were successfully indexed!")

    def sync_modules(self):
        """Create and sync module data to Elasticsearch."""
        index_name = f"{config.SYNC_CONFIG['index_prefix']}modules"
        
        # Define mapping with all the fields we'll need
        mapping = {
            "mappings": {
                "properties": {
                    "document_id": {"type": "keyword"},  # Historical unique ID
                    "indexed_at": {"type": "date"},      # When this version was indexed
                    "module_id": {"type": "keyword"},
                    "module_name": {"type": "keyword"},
                    "module_description": {"type": "text"},
                    "module_type": {"type": "keyword"},
                    "module_icon": {"type": "keyword"},
                    "module_logo": {"type": "keyword"},
                    "module_createdAt": {"type": "date"},
                    "module_updatedAt": {"type": "date"},
                    "parent_module_id": {"type": "keyword"},
                    "parent_module_name": {"type": "keyword"},
                    "statuses": {
                        "type": "nested",
                        "properties": {
                            "id": {"type": "keyword"},
                            "name": {"type": "keyword"},
                            "description": {"type": "text"},
                            "isFinalStatus": {"type": "boolean"},
                            "isVisible": {"type": "boolean"},
                            "createdAt": {"type": "date"},
                            "updatedAt": {"type": "date"}
                        }
                    },
                    "labels": {
                        "type": "nested",
                        "properties": {
                            "id": {"type": "keyword"},
                            "name": {"type": "keyword"},
                            "description": {"type": "text"},
                            "color": {"type": "keyword"},
                            "icon": {"type": "keyword"},
                            "type": {"type": "keyword"},
                            "isVisible": {"type": "boolean"},
                            "createdAt": {"type": "date"},
                            "updatedAt": {"type": "date"}
                        }
                    },
                    "data_sources": {
                        "type": "nested",
                        "properties": {
                            "id": {"type": "keyword"},
                            "name": {"type": "keyword"},
                            "description": {"type": "text"},
                            "entityName": {"type": "keyword"},
                            "gatewayType": {"type": "keyword"},
                            "gatewayId": {"type": "keyword"},
                            "dailyLimit": {"type": "integer"},
                            "wipEnabled": {"type": "boolean"},
                            "wipValue": {"type": "integer"},
                            "createdAt": {"type": "date"},
                            "updatedAt": {"type": "date"}
                        }
                    }
                }
            },
            "settings": {
                "refresh_interval": config.SYNC_CONFIG['refresh_interval']
            }
        }
        
        # Create the Elasticsearch index
        self.es_connector.create_index(index_name, mapping)
        
        # Get modules data from database
        df_modules = self.db_connector.get_modules()
        if df_modules is None or len(df_modules) == 0:
            logger.error("No modules available to sync.")
            return
        
        # Get related data
        df_statuses = self.db_connector.get_statuses()
        df_labels = self.db_connector.get_labels()
        df_data_sources = self.db_connector.get_data_sources()
        
        # Process relationships
        statuses_by_module = {}
        if df_statuses is not None:
            for _, status in df_statuses.iterrows():
                module_id = status.get('moduleId')
                if module_id:
                    if module_id not in statuses_by_module:
                        statuses_by_module[module_id] = []
                    statuses_by_module[module_id].append(status.to_dict())
        
        labels_by_module = {}
        if df_labels is not None:
            for _, label in df_labels.iterrows():
                module_id = label.get('moduleId')
                if module_id:
                    if module_id not in labels_by_module:
                        labels_by_module[module_id] = []
                    labels_by_module[module_id].append(label.to_dict())
        
        data_sources_by_module = {}
        if df_data_sources is not None:
            for _, data_source in df_data_sources.iterrows():
                module_id = data_source.get('moduleId')
                if module_id:
                    if module_id not in data_sources_by_module:
                        data_sources_by_module[module_id] = []
                    data_sources_by_module[module_id].append(data_source.to_dict())
        
        # Get total count for progress bar
        total_count = len(df_modules)
        
        # Process data in batches for Elasticsearch
        successful_docs = 0
        failed_docs = 0
        
        with tqdm(total=total_count, desc="Syncing modules") as pbar:
            batch_size = min(config.SYNC_CONFIG.get('batch_size', 100), 50)
            logger.info(f"Using batch size of {batch_size} for indexing")
            
            # Get current timestamp for this indexing run
            index_timestamp = datetime.utcnow().isoformat()
            
            # Process in batches
            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                batch_df = df_modules.iloc[batch_start:batch_end]
                
                # Prepare batch of actions for bulk indexing
                bulk_actions = []
                
                for idx, row in batch_df.iterrows():
                    try:
                        module_id = row["id"]
                        if isinstance(module_id, uuid.UUID):
                            module_id = str(module_id)
                        
                        # Create document with all fields
                        doc = {}
                        # Map fields according to our schema
                        doc['module_id'] = str(module_id)
                        doc['module_name'] = row.get('name')
                        doc['module_description'] = row.get('description')
                        doc['module_type'] = row.get('type')
                        doc['module_icon'] = row.get('icon')
                        doc['module_logo'] = row.get('logo')
                        doc['module_createdAt'] = row.get('createdAt')
                        doc['module_updatedAt'] = row.get('updatedAt')
                        
                        # Add relationships
                        doc['statuses'] = statuses_by_module.get(module_id, [])
                        doc['labels'] = labels_by_module.get(module_id, [])
                        doc['data_sources'] = data_sources_by_module.get(module_id, [])
                        
                        # Add parent module info if exists
                        parent_id = row.get('parentId')
                        if parent_id:
                            parent_module = df_modules[df_modules['id'] == parent_id]
                            if not parent_module.empty:
                                doc['parent_module_id'] = str(parent_id)
                                doc['parent_module_name'] = parent_module.iloc[0]['name']
                        
                        # Add historical tracking fields
                        doc['indexed_at'] = index_timestamp
                        
                        # Generate a unique document ID combining module ID and timestamp
                        document_id = f"{module_id}_{index_timestamp}"
                        doc['document_id'] = document_id
                        
                        # Thoroughly sanitize document
                        sanitized_doc = sanitize_document(doc)
                        
                        # Verify document is serializable before adding to batch
                        try:
                            json_str = json.dumps(sanitized_doc, cls=CustomJSONEncoder)
                            # Create action for bulk API
                            action = {
                                "_op_type": "index",
                                "_index": index_name,
                                "_id": document_id,
                                "_source": sanitized_doc
                            }
                            bulk_actions.append(action)
                        except Exception as json_err:
                            logger.error(f"Document not serializable for ID {module_id}: {str(json_err)}")
                            # Try one more sanitization pass with default serialization
                            try:
                                json_str = json.dumps(sanitized_doc, default=str)
                                sanitized_doc = json.loads(json_str)
                                action = {
                                    "_op_type": "index",
                                    "_index": index_name,
                                    "_id": document_id,
                                    "_source": sanitized_doc
                                }
                                bulk_actions.append(action)
                            except:
                                logger.error(f"Failed to sanitize document {module_id} even with default serializer, skipping")
                                failed_docs += 1
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error processing document at index {idx}: {str(e)}")
                        failed_docs += 1
                
                # Execute bulk indexing for the batch
                if bulk_actions:
                    success_count, failed_items = self.es_connector.bulk_index(
                        actions=bulk_actions,
                        refresh=(batch_end == total_count)  # Only refresh on the last batch
                    )
                    
                    successful_docs += success_count
                    failed_docs += len(failed_items)
                    
                    # Log any errors from bulk operation
                    for error in failed_items:
                        if 'index' in error and 'error' in error['index']:
                            logger.error(f"Bulk indexing error: {error['index']['error']}")
                
                pbar.update(batch_end - batch_start)
                
                # Report progress after each batch
                logger.info(f"Progress: {successful_docs} indexed successfully, {failed_docs} failed")
                
                # Verify documents are showing up every few batches
                if batch_start % (batch_size * 5) == 0 or batch_end == total_count:
                    count = self.es_connector.get_document_count(index_name)
                    logger.info(f"Current document count in ES: {count}")
        
        # Verify the documents were indexed
        final_count = self.es_connector.get_document_count(index_name)
        logger.info(f"Final document count in Elasticsearch index {index_name}: {final_count}")
        
        if final_count == 0:
            logger.error("No documents were indexed to Elasticsearch!")
            logger.error("Possible causes:")
            logger.error("1. Serialization errors when converting documents to JSON")
            logger.error("2. Connection issues during bulk indexing")
            logger.error("3. Mapping incompatibilities")
            logger.error("4. Date format issues in timestamp fields")
        elif final_count < total_count:
            logger.warning(f"Only {final_count} of {total_count} documents were indexed!")
            logger.warning("Some documents failed to index. Check the logs for specific errors.")
        else:
            logger.info("All documents were successfully indexed!")

    def sync_statuses(self):
        """Create and sync status data to Elasticsearch."""
        index_name = f"{config.SYNC_CONFIG['index_prefix']}statuses"
        
        # Define mapping for statuses
        mapping = {
            "mappings": {
                "properties": {
                    "document_id": {"type": "keyword"},
                    "indexed_at": {"type": "date"},
                    "status_id": {"type": "keyword"},
                    "status_name": {"type": "keyword"},
                    "status_isFinalStatus": {"type": "boolean"},
                    "status_description": {"type": "text"},
                    "status_moduleId": {"type": "keyword"},
                    "status_isVisible": {"type": "boolean"},
                    "status_createdAt": {"type": "date"},
                    "status_updatedAt": {"type": "date"}
                }
            },
            "settings": {
                "refresh_interval": config.SYNC_CONFIG['refresh_interval']
            }
        }
        
        # Create the Elasticsearch index
        self.es_connector.create_index(index_name, mapping)
        
        # Get statuses from database
        df_statuses = self.db_connector.get_statuses()
        if df_statuses is None or len(df_statuses) == 0:
            logger.error("No statuses available to sync.")
            return
        
        # Get total count for progress bar
        total_count = len(df_statuses)
        
        # Process data in batches for Elasticsearch
        successful_docs = 0
        failed_docs = 0
        
        with tqdm(total=total_count, desc="Syncing statuses") as pbar:
            batch_size = min(config.SYNC_CONFIG.get('batch_size', 100), 50)
            logger.info(f"Using batch size of {batch_size} for indexing")
            
            # Get current timestamp for this indexing run
            index_timestamp = datetime.utcnow().isoformat()
            
            # Process in batches
            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                batch_df = df_statuses.iloc[batch_start:batch_end]
                
                # Prepare batch of actions for bulk indexing
                bulk_actions = []
                
                for idx, row in batch_df.iterrows():
                    try:
                        status_id = row["id"]
                        if isinstance(status_id, uuid.UUID):
                            status_id = str(status_id)
                        
                        # Create document with all fields
                        doc = {}
                        # Map fields according to our schema
                        doc['status_id'] = str(status_id)
                        doc['status_name'] = row.get('name')
                        doc['status_isFinalStatus'] = row.get('isFinalStatus')
                        doc['status_description'] = row.get('description')
                        doc['status_moduleId'] = str(row.get('moduleId')) if row.get('moduleId') else None
                        doc['status_isVisible'] = row.get('isVisible')
                        doc['status_createdAt'] = row.get('createdAt')
                        doc['status_updatedAt'] = row.get('updatedAt')
                        
                        # Add historical tracking fields
                        doc['indexed_at'] = index_timestamp
                        
                        # Generate a unique document ID combining status ID and timestamp
                        document_id = f"{status_id}_{index_timestamp}"
                        doc['document_id'] = document_id
                        
                        # Thoroughly sanitize document
                        sanitized_doc = sanitize_document(doc)
                        
                        # Verify document is serializable before adding to batch
                        try:
                            json_str = json.dumps(sanitized_doc, cls=CustomJSONEncoder)
                            # Create action for bulk API
                            action = {
                                "_op_type": "index",
                                "_index": index_name,
                                "_id": document_id,
                                "_source": sanitized_doc
                            }
                            bulk_actions.append(action)
                        except Exception as json_err:
                            logger.error(f"Document not serializable for ID {status_id}: {str(json_err)}")
                            # Try one more sanitization pass with default serialization
                            try:
                                json_str = json.dumps(sanitized_doc, default=str)
                                sanitized_doc = json.loads(json_str)
                                action = {
                                    "_op_type": "index",
                                    "_index": index_name,
                                    "_id": document_id,
                                    "_source": sanitized_doc
                                }
                                bulk_actions.append(action)
                            except:
                                logger.error(f"Failed to sanitize document {status_id} even with default serializer, skipping")
                                failed_docs += 1
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error processing document at index {idx}: {str(e)}")
                        failed_docs += 1
                
                # Execute bulk indexing for the batch
                if bulk_actions:
                    success_count, failed_items = self.es_connector.bulk_index(
                        actions=bulk_actions,
                        refresh=(batch_end == total_count)  # Only refresh on the last batch
                    )
                    
                    successful_docs += success_count
                    failed_docs += len(failed_items)
                    
                    # Log any errors from bulk operation
                    for error in failed_items:
                        if 'index' in error and 'error' in error['index']:
                            logger.error(f"Bulk indexing error: {error['index']['error']}")
                
                pbar.update(batch_end - batch_start)
                
                # Report progress after each batch
                logger.info(f"Progress: {successful_docs} indexed successfully, {failed_docs} failed")
                
                # Verify documents are showing up every few batches
                if batch_start % (batch_size * 5) == 0 or batch_end == total_count:
                    count = self.es_connector.get_document_count(index_name)
                    logger.info(f"Current document count in ES: {count}")
        
        # Verify the documents were indexed
        final_count = self.es_connector.get_document_count(index_name)
        logger.info(f"Final document count in Elasticsearch index {index_name}: {final_count}")
        
        if final_count == 0:
            logger.error("No documents were indexed to Elasticsearch!")
            logger.error("Possible causes:")
            logger.error("1. Serialization errors when converting documents to JSON")
            logger.error("2. Connection issues during bulk indexing")
            logger.error("3. Mapping incompatibilities")
            logger.error("4. Date format issues in timestamp fields")
        elif final_count < total_count:
            logger.warning(f"Only {final_count} of {total_count} documents were indexed!")
            logger.warning("Some documents failed to index. Check the logs for specific errors.")
        else:
            logger.info("All documents were successfully indexed!")

    def sync_labels(self):
        """Create and sync label data to Elasticsearch."""
        index_name = f"{config.SYNC_CONFIG['index_prefix']}labels"
        
        # Define mapping for labels
        mapping = {
            "mappings": {
                "properties": {
                    "document_id": {"type": "keyword"},
                    "indexed_at": {"type": "date"},
                    "label_id": {"type": "keyword"},
                    "label_name": {"type": "keyword"},
                    "label_description": {"type": "text"},
                    "label_moduleId": {"type": "keyword"},
                    "label_color": {"type": "keyword"},
                    "label_icon": {"type": "keyword"},
                    "label_type": {"type": "keyword"},
                    "label_isVisible": {"type": "boolean"},
                    "label_createdAt": {"type": "date"},
                    "label_updatedAt": {"type": "date"}
                }
            },
            "settings": {
                "refresh_interval": config.SYNC_CONFIG['refresh_interval']
            }
        }
        
        # Create the Elasticsearch index
        self.es_connector.create_index(index_name, mapping)
        
        # Get labels from database
        df_labels = self.db_connector.get_labels()
        if df_labels is None or len(df_labels) == 0:
            logger.error("No labels available to sync.")
            return
        
        # Get total count for progress bar
        total_count = len(df_labels)
        
        # Process data in batches for Elasticsearch
        successful_docs = 0
        failed_docs = 0
        
        with tqdm(total=total_count, desc="Syncing labels") as pbar:
            batch_size = min(config.SYNC_CONFIG.get('batch_size', 100), 50)
            logger.info(f"Using batch size of {batch_size} for indexing")
            
            # Get current timestamp for this indexing run
            index_timestamp = datetime.utcnow().isoformat()
            
            # Process in batches
            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                batch_df = df_labels.iloc[batch_start:batch_end]
                
                # Prepare batch of actions for bulk indexing
                bulk_actions = []
                
                for idx, row in batch_df.iterrows():
                    try:
                        label_id = row["id"]
                        if isinstance(label_id, uuid.UUID):
                            label_id = str(label_id)
                        
                        # Create document with all fields
                        doc = {}
                        # Map fields according to our schema
                        doc['label_id'] = str(label_id)
                        doc['label_name'] = row.get('name')
                        doc['label_description'] = row.get('description')
                        doc['label_moduleId'] = str(row.get('moduleId')) if row.get('moduleId') else None
                        doc['label_color'] = row.get('color')
                        doc['label_icon'] = row.get('icon')
                        doc['label_type'] = row.get('type')
                        doc['label_isVisible'] = row.get('isVisible')
                        doc['label_createdAt'] = row.get('createdAt')
                        doc['label_updatedAt'] = row.get('updatedAt')
                        
                        # Add historical tracking fields
                        doc['indexed_at'] = index_timestamp
                        
                        # Generate a unique document ID combining label ID and timestamp
                        document_id = f"{label_id}_{index_timestamp}"
                        doc['document_id'] = document_id
                        
                        # Thoroughly sanitize document
                        sanitized_doc = sanitize_document(doc)
                        
                        # Verify document is serializable before adding to batch
                        try:
                            json_str = json.dumps(sanitized_doc, cls=CustomJSONEncoder)
                            # Create action for bulk API
                            action = {
                                "_op_type": "index",
                                "_index": index_name,
                                "_id": document_id,
                                "_source": sanitized_doc
                            }
                            bulk_actions.append(action)
                        except Exception as json_err:
                            logger.error(f"Document not serializable for ID {label_id}: {str(json_err)}")
                            # Try one more sanitization pass with default serialization
                            try:
                                json_str = json.dumps(sanitized_doc, default=str)
                                sanitized_doc = json.loads(json_str)
                                action = {
                                    "_op_type": "index",
                                    "_index": index_name,
                                    "_id": document_id,
                                    "_source": sanitized_doc
                                }
                                bulk_actions.append(action)
                            except:
                                logger.error(f"Failed to sanitize document {label_id} even with default serializer, skipping")
                                failed_docs += 1
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error processing document at index {idx}: {str(e)}")
                        failed_docs += 1
                
                # Execute bulk indexing for the batch
                if bulk_actions:
                    success_count, failed_items = self.es_connector.bulk_index(
                        actions=bulk_actions,
                        refresh=(batch_end == total_count)  # Only refresh on the last batch
                    )
                    
                    successful_docs += success_count
                    failed_docs += len(failed_items)
                    
                    # Log any errors from bulk operation
                    for error in failed_items:
                        if 'index' in error and 'error' in error['index']:
                            logger.error(f"Bulk indexing error: {error['index']['error']}")
                
                pbar.update(batch_end - batch_start)
                
                # Report progress after each batch
                logger.info(f"Progress: {successful_docs} indexed successfully, {failed_docs} failed")
                
                # Verify documents are showing up every few batches
                if batch_start % (batch_size * 5) == 0 or batch_end == total_count:
                    count = self.es_connector.get_document_count(index_name)
                    logger.info(f"Current document count in ES: {count}")
        
        # Verify the documents were indexed
        final_count = self.es_connector.get_document_count(index_name)
        logger.info(f"Final document count in Elasticsearch index {index_name}: {final_count}")
        
        if final_count == 0:
            logger.error("No documents were indexed to Elasticsearch!")
            logger.error("Possible causes:")
            logger.error("1. Serialization errors when converting documents to JSON")
            logger.error("2. Connection issues during bulk indexing")
            logger.error("3. Mapping incompatibilities")
            logger.error("4. Date format issues in timestamp fields")
        elif final_count < total_count:
            logger.warning(f"Only {final_count} of {total_count} documents were indexed!")
            logger.warning("Some documents failed to index. Check the logs for specific errors.")
        else:
            logger.info("All documents were successfully indexed!")

    def sync_all_tables(self):
        """Sync all data to the data lake."""
        logger.info("Starting data sync")
        try:
            self.sync_data_sources()
            self.sync_users()
            self.sync_modules()
            self.sync_statuses()
            self.sync_labels()
            logger.info("Completed data sync")
        except Exception as e:
            logger.error(f"Error during data sync: {str(e)}") 