import logging
import json
import uuid
import pandas as pd
from tqdm import tqdm
import src.config as config
from src.db_connector import DatabaseConnector
from src.es_connector import ElasticsearchConnector
from src.document_utils import sanitize_document, process_ticket_labels
from src.json_encoder import CustomJSONEncoder
from datetime import datetime

logger = logging.getLogger(__name__)


class DataLakeSync:
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.es_connector = ElasticsearchConnector()

    def sync_denormalized_tickets(self):
        """Create and sync denormalized ticket data to Elasticsearch."""
        index_name = f"{config.SYNC_CONFIG['index_prefix']}denormalized_tickets"
        
        # Verify database schema before proceeding
        self.db_connector.verify_database_schema()
        
        # Define mapping with all the fields we'll need
        mapping = {
            "mappings": {
                "properties": {
                    "document_id": {"type": "keyword"},  # New historical unique ID
                    "indexed_at": {"type": "date"},      # When this version was indexed
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
                    "labels": {"type": "nested", 
                              "properties": {
                                 "id": {"type": "keyword"},
                                 "name": {"type": "keyword"},
                                 "color": {"type": "keyword"}
                              }},
                    "module_id": {"type": "keyword"},
                    "module_name": {"type": "keyword"},
                    "datasource_id": {"type": "keyword"},
                    "datasource_name": {"type": "keyword"},
                    "user_id": {"type": "keyword"},
                    "user_name": {"type": "keyword"},
                    "user_email": {"type": "keyword"}
                }
            },
            "settings": {
                "refresh_interval": config.SYNC_CONFIG['refresh_interval']
            }
        }
        
        # Create the Elasticsearch index
        self.es_connector.create_index(index_name, mapping)
        
        # Get tickets and labels data from database
        df_tickets, df_labels = self.db_connector.get_tickets_and_labels()
        if df_tickets is None or len(df_tickets) == 0:
            logger.error("No tickets available to sync.")
            return
        
        # Process labels
        labels_by_ticket = process_ticket_labels(df_labels) if df_labels is not None else {}
        logger.info(f"Processed labels for {len(labels_by_ticket)} tickets")
        
        # Get total count for progress bar
        total_count = len(df_tickets)
        
        # Process data in batches for Elasticsearch
        successful_docs = 0
        failed_docs = 0
        
        with tqdm(total=total_count, desc="Syncing denormalized tickets") as pbar:
            batch_size = min(config.SYNC_CONFIG.get('batch_size', 100), 50)
            logger.info(f"Using batch size of {batch_size} for indexing")
            
            # Get current timestamp for this indexing run
            index_timestamp = datetime.utcnow().isoformat()
            
            # Process in batches
            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                batch_df = df_tickets.iloc[batch_start:batch_end]
                
                # Prepare batch of actions for bulk indexing
                bulk_actions = []
                
                for idx, row in batch_df.iterrows():
                    try:
                        ticket_id = row["ticket_id"]
                        if isinstance(ticket_id, uuid.UUID):
                            ticket_id = str(ticket_id)
                        
                        # Create document with all fields
                        doc = {}
                        for col in batch_df.columns:
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
                                # Also sanitize the parsed JSON data
                                doc['ticket_data'] = sanitize_document(parsed_data)
                            except json.JSONDecodeError:
                                logger.warning(f"Field 'ticket_data' is not valid JSON for ticket {ticket_id}")
                        
                        # Add labels array
                        doc['labels'] = labels_by_ticket.get(ticket_id, [])
                        
                        # Add historical tracking fields
                        doc['indexed_at'] = index_timestamp
                        
                        # Generate a unique document ID combining ticket ID and timestamp
                        # This ensures we create a new document for each version of the ticket
                        document_id = f"{ticket_id}_{index_timestamp}"
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
                            logger.error(f"Document not serializable for ID {ticket_id}: {str(json_err)}")
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
                                logger.error(f"Failed to sanitize document {ticket_id} even with default serializer, skipping")
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
        logger.info("Starting denormalized ticket sync")
        try:
            self.sync_denormalized_tickets()
            logger.info("Completed denormalized ticket sync")
        except Exception as e:
            logger.error(f"Error syncing denormalized tickets: {str(e)}")