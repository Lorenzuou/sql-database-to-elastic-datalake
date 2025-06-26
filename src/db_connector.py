import logging
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, inspect, text
import src.config as config

logger = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(self):
        self.db_engine = self._create_db_engine()
        self.inspector = inspect(self.db_engine)
        self.db_type = config.DB_CONFIG['db_type']
        self.tables_to_sync = [
            "Ticket", "TicketStatus", "TicketLabel", 
            "Status", "Label", "Module", "User", "DataSource"
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
    
    def verify_database_schema(self):
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
            
    def get_data_sources(self):
        """Get all data sources from the database."""
        try:
            quoted_table = self._quote_table_name("DataSource")
            query = f"""
            SELECT *
            FROM {quoted_table}
            WHERE "deletedAt" IS NULL
            """
            
            with self.db_engine.connect() as connection:
                df = pd.read_sql(query, connection)
                logger.info(f"Retrieved {len(df)} data sources from database")
                return df
        except Exception as e:
            logger.error(f"Error fetching data sources: {str(e)}")
            return None

    def get_users(self):
        """Get all users from the database."""
        try:
            quoted_table = self._quote_table_name("User")
            query = f"""
            SELECT *
            FROM {quoted_table}
            WHERE "deletedAt" IS NULL
            """
            
            with self.db_engine.connect() as connection:
                df = pd.read_sql(query, connection)
                logger.info(f"Retrieved {len(df)} users from database")
                return df
        except Exception as e:
            logger.error(f"Error fetching users: {str(e)}")
            return None

    def get_modules(self):
        """Get all modules from the database."""
        try:
            quoted_table = self._quote_table_name("Module")
            query = f"""
            SELECT *
            FROM {quoted_table}
            WHERE "deletedAt" IS NULL
            """
            
            with self.db_engine.connect() as connection:
                df = pd.read_sql(query, connection)
                logger.info(f"Retrieved {len(df)} modules from database")
                return df
        except Exception as e:
            logger.error(f"Error fetching modules: {str(e)}")
            return None

    def get_statuses(self):
        """Get all statuses from the database."""
        try:
            quoted_table = self._quote_table_name("Status")
            query = f"""
            SELECT *
            FROM {quoted_table}
            WHERE "deletedAt" IS NULL
            """
            
            with self.db_engine.connect() as connection:
                df = pd.read_sql(query, connection)
                logger.info(f"Retrieved {len(df)} statuses from database")
                return df
        except Exception as e:
            logger.error(f"Error fetching statuses: {str(e)}")
            return None

    def get_labels(self):
        """Get all labels from the database."""
        try:
            quoted_table = self._quote_table_name("Label")
            query = f"""
            SELECT *
            FROM {quoted_table}
            WHERE "deletedAt" IS NULL
            """
            
            with self.db_engine.connect() as connection:
                df = pd.read_sql(query, connection)
                logger.info(f"Retrieved {len(df)} labels from database")
                return df
        except Exception as e:
            logger.error(f"Error fetching labels: {str(e)}")
            return None

    def get_tickets_and_labels(self, ticket_id=None):
        """Get tickets and labels data from database.
        
        Args:
            ticket_id (str, optional): If provided, only fetch data for this specific ticket.
        """
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
        """
        
        # Add ticket_id filter if provided
        if ticket_id:
            query += f" AND t.id = '{ticket_id}'"
        
        query += " ORDER BY t.\"number\""
        
        logger.info(f"Executing SQL query:\n{query}")
        
        # Execute the query and get all tickets
        try:
            with self.db_engine.connect() as connection:
                # First try a test query to verify connection
                test_query = f"SELECT COUNT(*) FROM {table_prefix}\"Ticket\""
                if ticket_id:
                    test_query += f" WHERE id = '{ticket_id}'"
                try:
                    result = connection.execute(text(test_query))
                    ticket_count = result.fetchone()[0]
                    logger.info(f"Test query successful. Found {ticket_count} tickets in database.")
                    if ticket_count == 0:
                        logger.warning("No tickets in database. Nothing to sync.")
                        return None, None
                except Exception as test_err:
                    logger.error(f"Test query failed: {str(test_err)}")
                    # Try with public schema
                    try:
                        test_query = "SELECT COUNT(*) FROM \"Ticket\""
                        if ticket_id:
                            test_query += f" WHERE id = '{ticket_id}'"
                        result = connection.execute(text(test_query))
                        ticket_count = result.fetchone()[0]
                        logger.info(f"Test query with public schema successful. Found {ticket_count} tickets.")
                        # Update the query to use public schema
                        query = query.replace(f"{table_prefix}", "")
                    except Exception as e:
                        logger.error(f"Test query with public schema failed: {str(e)}")
                        logger.error("Cannot connect to database tables. Check schema and table names.")
                        return None, None
                
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
                            if ticket_id and table == "Ticket":
                                count_query += f" WHERE id = '{ticket_id}'"
                            result = connection.execute(text(count_query))
                            count = result.fetchone()[0]
                            logger.info(f"Table {table}: {count} records")
                        except Exception as e:
                            logger.warning(f"Could not check count for {table}: {str(e)}")
                    return None, None
                
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
                
                # Fetch all labels for all tickets
                labels_query = f"""
                SELECT tl."ticketId", l.id as label_id, l."name" as label_name, l.color
                FROM {table_prefix}"TicketLabel" tl
                JOIN {table_prefix}"Label" l ON tl."labelId" = l.id
                WHERE tl."deletedAt" IS NULL
                """
                
                # Add ticket_id filter if provided
                if ticket_id:
                    labels_query += f" AND tl.\"ticketId\" = '{ticket_id}'"
                
                logger.info(f"Executing labels query:\n{labels_query}")
                df_labels = pd.read_sql(labels_query, connection)
                logger.info(f"Retrieved {len(df_labels)} label records")
                
                return df_tickets, df_labels
                
        except Exception as e:
            logger.error(f"Error executing database query: {str(e)}")
            return None, None
