import logging
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tqdm import tqdm
import config
from json_encoder import json_serialize, CustomJSONEncoder
import json
from flask import Flask, request, jsonify
import uuid
import numpy as np

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
        # Define the specific tables to sync
        self.tables_to_sync = [
            "Ticket", 
            "TicketStatus", 
            "TicketLabel", 
            "Status", 
            "Label", 
            "Module", 
            "User", 
            "DataSource"
        ]

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
        
        logger.info(f"Creating database engine with connection string: {connection_string.split(':')[0]}:****")
        return create_engine(connection_string)

    def _create_es_client(self):
        """Create Elasticsearch client."""
        es_url = f"{config.ES_CONFIG['scheme']}://{config.ES_CONFIG['host']}:{config.ES_CONFIG['port']}"
        logger.info(f"Connecting to Elasticsearch at {es_url}")
        return Elasticsearch([es_url])

    def get_table_names(self) -> List[str]:
        """Get relevant table names from the database."""
        try:
            all_tables = self.inspector.get_table_names(schema="copy")
            filtered_tables = [table for table in all_tables if table in self.tables_to_sync]
            logger.info(f"Found tables in schema 'copy': {filtered_tables}")
            return filtered_tables
        except Exception as e:
            logger.error(f"Error getting table names: {str(e)}")
            # Try without schema for compatibility
            all_tables = self.inspector.get_table_names()
            logger.info(f"Found tables without schema: {all_tables}")
            return [table for table in all_tables if table in self.tables_to_sync]

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a table."""
        try:
            columns = self.inspector.get_columns(table_name, schema="copy")
            return {col['name']: str(col['type']) for col in columns}
        except Exception as e:
            logger.warning(f"Error getting schema with 'copy' schema: {str(e)}")
            # Try without schema
            columns = self.inspector.get_columns(table_name)
            return {col['name']: str(col['type']) for col in columns}
    
    def _quote_table_name(self, table_name: str) -> str:
        """Quote table name according to database type."""
        schema = "copy"  # Use the copy schema
        
        try:
            # First check if the table exists in the copy schema
            tables_in_copy = self.inspector.get_table_names(schema="copy")
            if table_name in tables_in_copy:
                if self.db_type == 'postgresql':
                    return f'"{schema}"."{table_name}"'
                elif self.db_type == 'mysql':
                    return f'`{schema}`.`{table_name}`'
                return f"{schema}.{table_name}"
            else:
                # Check default schema
                if table_name in self.inspector.get_table_names():
                    logger.info(f"Table {table_name} found in default schema")
                    if self.db_type == 'postgresql':
                        return f'"{table_name}"'
                    elif self.db_type == 'mysql':
                        return f'`{table_name}`'
                    return table_name
                else:
                    logger.warning(f"Table {table_name} not found in any schema")
                    # Default to copy schema anyway
                    if self.db_type == 'postgresql':
                        return f'"{schema}"."{table_name}"'
                    elif self.db_type == 'mysql':
                        return f'`{schema}`.`{table_name}`'
                    return f"{schema}.{table_name}"
        except Exception as e:
            logger.error(f"Error in _quote_table_name: {str(e)}")
            # Fallback
            if self.db_type == 'postgresql':
                return f'"{schema}"."{table_name}"'
            elif self.db_type == 'mysql':
                return f'`{schema}`.`{table_name}`'
            return f"{schema}.{table_name}"
    
    def _verify_database_schema(self):
        """Verify that the tables exist in the database."""
        try:
            logger.info("Verifying database schema...")
            schemas = self.inspector.get_schema_names()
            logger.info(f"Available schemas: {schemas}")
            
            if "copy" in schemas:
                tables_in_copy = self.inspector.get_table_names(schema="copy")
                logger.info(f"Tables in 'copy' schema: {tables_in_copy}")
                
                # Check if our required tables exist
                missing_tables = [table for table in self.tables_to_sync 
                                 if table not in tables_in_copy]
                if missing_tables:
                    logger.warning(f"Missing tables in 'copy' schema: {missing_tables}")
            else:
                logger.warning("Schema 'copy' not found!")
                # Check default schema
                default_tables = self.inspector.get_table_names()
                logger.info(f"Tables in default schema: {default_tables}")
                
                # Check if our required tables exist in default schema
                missing_tables = [table for table in self.tables_to_sync 
                                 if table not in default_tables]
                if missing_tables:
                    logger.warning(f"Missing tables in default schema: {missing_tables}")
        except Exception as e:
            logger.error(f"Error verifying schema: {str(e)}")

    def _sanitize_document(self, doc):
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
                    sanitized[k] = self._sanitize_document(v)
                # Recursively handle lists
                elif isinstance(v, list):
                    sanitized[k] = [self._sanitize_document(item) for item in v]
                # Pass through everything else
                else:
                    sanitized[k] = v
            except Exception as e:
                logger.error(f"Error sanitizing field {k}: {str(e)}")
                # Use a safe default if there's an error
                sanitized[k] = None
                
        return sanitized

    def sync_denormalized_tickets(self):
        """Create and sync denormalized ticket data to Elasticsearch."""
        index_name = f"{config.SYNC_CONFIG['index_prefix']}denormalized_tickets"
        
        # Verify database schema before proceeding
        self._verify_database_schema()
        
        # Create or update the Elasticsearch index
        if self.es_client.indices.exists(index=index_name):
            logger.info(f"Deleting existing index: {index_name}")
            self.es_client.indices.delete(index=index_name)
        
        # Define mapping with all the fields we'll need
        mapping = {
            "mappings": {
                "properties": {
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
        logger.info(f"Creating index: {index_name}")
        self.es_client.indices.create(index=index_name, body=json.loads(json_serialize(mapping)))
        
        # Try to determine the correct schema to use
        schema_name = "copy"  # Default
        try:
            schemas = self.inspector.get_schema_names()
            if "copy" not in schemas:
                logger.warning("Schema 'copy' not found! Trying without schema prefix.")
                schema_name = None
        except Exception as e:
            logger.error(f"Error getting schemas: {str(e)}")
            schema_name = None
        
        # Build the SQL query for denormalized view based on schema
        if schema_name:
            table_prefix = f'"{schema_name}".'
        else:
            table_prefix = ''
        
        # Debug: Check if tables exist before querying
        logger.info("Checking table existence before querying:")
        for table_name in self.tables_to_sync:
            try:
                if schema_name:
                    exists = self.inspector.has_table(table_name, schema_name)
                else:
                    exists = self.inspector.has_table(table_name)
                logger.info(f"Table {table_name}: {'EXISTS' if exists else 'NOT FOUND'}")
            except Exception as e:
                logger.error(f"Error checking table {table_name}: {str(e)}")
        
        # Build the SQL query for denormalized view
        query = f"""
        WITH latest_status AS (
            SELECT DISTINCT ON (ts."ticketId") ts."ticketId", ts."statusId", s."name" as status_name, 
                s."isFinalStatus", ts."createdAt" as status_created_at
            FROM {table_prefix}"TicketStatus" ts
            JOIN {table_prefix}"Status" s ON ts."statusId" = s.id
            WHERE ts."deletedAt" IS NULL
            ORDER BY ts."ticketId", ts."createdAt" DESC
        )
        SELECT 
            t.id as ticket_id,
            t."number" as ticket_number,
            t."scheduleDate" as ticket_scheduleDate,
            t."scheduleDateEnd" as ticket_scheduleDateEnd,
            t."data" as ticket_data,
            t."createdAt" as ticket_createdAt,
            t."updatedAt" as ticket_updatedAt,
            ls."statusId" as status_id,
            ls.status_name,
            ls."isFinalStatus",
            m.id as module_id,
            m."name" as module_name,
            ds.id as datasource_id,
            ds."name" as datasource_name,
            u.id as user_id,
            u."name" as user_name,
            u.email as user_email
        FROM {table_prefix}"Ticket" t
        LEFT JOIN latest_status ls ON t.id = ls."ticketId"
        LEFT JOIN {table_prefix}"Module" m ON t."moduleId" = m.id
        LEFT JOIN {table_prefix}"DataSource" ds ON t."dataSourceId" = ds.id
        LEFT JOIN {table_prefix}"User" u ON t."userId" = u.id
        WHERE t."deletedAt" IS NULL
        ORDER BY t."number"
        """
        
        logger.info(f"Executing SQL query:\n{query}")
        
        # Execute the query and get all tickets
        try:
            with self.db_engine.connect() as connection:
                # First try a test query to verify connection
                test_query = f"SELECT COUNT(*) FROM {table_prefix}\"Ticket\""
                try:
                    result = connection.execute(text(test_query))
                    ticket_count = result.fetchone()[0]
                    logger.info(f"Test query successful. Found {ticket_count} tickets in database.")
                    if ticket_count == 0:
                        logger.warning("No tickets in database. Nothing to sync.")
                        return
                except Exception as test_err:
                    logger.error(f"Test query failed: {str(test_err)}")
                    # Try with public schema
                    try:
                        test_query = "SELECT COUNT(*) FROM \"Ticket\""
                        result = connection.execute(text(test_query))
                        ticket_count = result.fetchone()[0]
                        logger.info(f"Test query with public schema successful. Found {ticket_count} tickets.")
                        # Update the query to use public schema
                        query = query.replace(f"{table_prefix}", "")
                    except Exception as e:
                        logger.error(f"Test query with public schema failed: {str(e)}")
                        logger.error("Cannot connect to database tables. Check schema and table names.")
                        return
                
                # Now try the full query
                df_tickets = pd.read_sql(query, connection)
                
                # Debug: Check if we got any tickets
                logger.info(f"Retrieved {len(df_tickets)} tickets from database")
                if len(df_tickets) == 0:
                    logger.error("No tickets found in the database. Check the query and table structure.")
                    # Check if the tables have any data
                    for table in self.tables_to_sync:
                        try:
                            count_query = f"SELECT COUNT(*) FROM {table_prefix}\"{table}\""
                            result = connection.execute(text(count_query))
                            count = result.fetchone()[0]
                            logger.info(f"Table {table}: {count} records")
                        except Exception as e:
                            logger.warning(f"Could not check count for {table}: {str(e)}")
                    return
                
                # Check column data types and sample values
                logger.info("Column information:")
                for column in df_tickets.columns:
                    try:
                        dtype = df_tickets[column].dtype
                        non_null_count = df_tickets[column].count()
                        sample = df_tickets[column].iloc[0] if non_null_count > 0 else None
                        logger.info(f"Column: {column}, Type: {dtype}, Non-null: {non_null_count}, Sample: {repr(sample)}")
                    except Exception as e:
                        logger.error(f"Error getting column info for {column}: {str(e)}")
                
                # Get total count for progress bar
                total_count = len(df_tickets)
                
                # Fetch all labels for all tickets
                labels_query = f"""
                SELECT tl."ticketId", l.id as label_id, l."name" as label_name, l.color
                FROM {table_prefix}"TicketLabel" tl
                JOIN {table_prefix}"Label" l ON tl."labelId" = l.id
                WHERE tl."deletedAt" IS NULL
                """
                
                logger.info(f"Executing labels query:\n{labels_query}")
                df_labels = pd.read_sql(labels_query, connection)
                logger.info(f"Retrieved {len(df_labels)} label records")
                
                # Group labels by ticket ID
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
                
                logger.info(f"Grouped labels for {len(labels_by_ticket)} tickets")
        except Exception as e:
            logger.error(f"Error executing database query: {str(e)}")
            return
        
        # Process data in batches for Elasticsearch
        successful_docs = 0
        failed_docs = 0
        
        with tqdm(total=total_count, desc="Syncing denormalized tickets") as pbar:
            batch_size = min(config.SYNC_CONFIG.get('batch_size', 100), 50)  # Use smaller batches to debug
            logger.info(f"Using batch size of {batch_size} for indexing")
            
            # Process in smaller batches for easier debugging
            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                batch_df = df_tickets.iloc[batch_start:batch_end]
                
                actions = []
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
                                doc['ticket_data'] = self._sanitize_document(parsed_data)
                            except json.JSONDecodeError:
                                logger.warning(f"Field 'ticket_data' is not valid JSON for ticket {ticket_id}")
                        
                        # Add labels array
                        doc['labels'] = labels_by_ticket.get(ticket_id, [])
                        
                        # Thoroughly sanitize document
                        sanitized_doc = self._sanitize_document(doc)
                        
                        # Verify document is serializable
                        try:
                            json_str = json.dumps(sanitized_doc, cls=CustomJSONEncoder)
                        except Exception as json_err:
                            logger.error(f"Document not serializable for ID {ticket_id}: {str(json_err)}")
                            # Try one more sanitization pass with default serialization
                            try:
                                json_str = json.dumps(sanitized_doc, default=str)
                                sanitized_doc = json.loads(json_str)
                            except:
                                logger.error(f"Failed to sanitize document {ticket_id} even with default serializer, skipping")
                                failed_docs += 1
                                pbar.update(1)
                                continue
                        
                        # Add to bulk actions
                        actions.append({
                            "_index": index_name,
                            "_id": ticket_id,  # Use ticket_id as document ID
                            "_source": sanitized_doc
                        })
                    except Exception as e:
                        logger.error(f"Error processing document at index {idx}: {str(e)}")
                        failed_docs += 1
                
                # Index the batch directly without bulk for better error handling
                if actions:
                    try:
                        # Use bulk helper for better performance
                        success, failed = helpers.bulk(
                            self.es_client,
                            actions,
                            stats_only=True,
                            raise_on_error=False,  # Don't stop on first error
                            chunk_size=100         # Process in smaller chunks
                        )
                        
                        successful_docs += success
                        failed_docs += len(failed) if isinstance(failed, list) else failed
                        
                        # Force refresh to ensure documents are visible immediately
                        self.es_client.indices.refresh(index=index_name)
                        
                    except Exception as e:
                        logger.error(f"Bulk indexing error: {str(e)}")
                        
                        # Fallback to individual indexing if bulk fails completely
                        for action in actions:
                            try:
                                result = self.es_client.index(
                                    index=action['_index'],
                                    id=action['_id'],
                                    body=action['_source'],
                                    refresh=True
                                )
                                if result.get('result') in ['created', 'updated']:
                                    successful_docs += 1
                                else:
                                    logger.warning(f"Document {action['_id']} indexing returned unexpected result: {result}")
                                    failed_docs += 1
                            except Exception as doc_e:
                                logger.error(f"Error indexing document ID {action['_id']}: {str(doc_e)}")
                                failed_docs += 1

                pbar.update(batch_end - batch_start)
                                
                # Report progress after each batch
                logger.info(f"Progress: {successful_docs} indexed successfully, {failed_docs} failed")
                
                # Verify documents are showing up
                try:
                    batch_count = self.es_client.count(index=index_name)
                    logger.info(f"Current document count in ES: {batch_count['count']}")
                except Exception as e:
                    logger.error(f"Error checking ES document count: {str(e)}")
        
        # Verify the documents were indexed
        try:
            count = self.es_client.count(index=index_name)
            logger.info(f"Final document count in Elasticsearch index {index_name}: {count['count']}")
            if count['count'] == 0:
                logger.error("No documents were indexed to Elasticsearch!")
                logger.error("Possible causes:")
                logger.error("1. Serialization errors when converting documents to JSON")
                logger.error("2. Connection issues during bulk indexing")
                logger.error("3. Mapping incompatibilities")
                logger.error("4. Date format issues in timestamp fields")
            elif count['count'] < total_count:
                logger.warning(f"Only {count['count']} of {total_count} documents were indexed!")
                logger.warning("Some documents failed to index. Check the logs for specific errors.")
            else:
                logger.info("All documents were successfully indexed!")
        except Exception as e:
            logger.error(f"Error counting documents: {str(e)}")

    def sync_all_tables(self):
        """Sync only the denormalized ticket view."""
        logger.info("Starting denormalized ticket sync")
        try:
            self.sync_denormalized_tickets()
            logger.info("Completed denormalized ticket sync")
        except Exception as e:
            logger.error(f"Error syncing denormalized tickets: {str(e)}")

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