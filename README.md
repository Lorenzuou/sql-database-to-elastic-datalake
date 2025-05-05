# Data Lake Sync Application

This application synchronizes your database tables with Elasticsearch, creating a searchable data lake where you can easily search and visualize your data using Kibana.

## Features

- Automatically detects and syncs all tables from your database
- Supports PostgreSQL and MySQL databases
- Creates appropriate Elasticsearch mappings based on column types
- Progress tracking with tqdm
- Batch processing for efficient data transfer
- Error handling and logging
- Docker support for easy deployment
- Kibana integration for data visualization

## Prerequisites

- Python 3.8 or higher
- Docker and Docker Compose
- PostgreSQL or MySQL database
- Required Python packages (see requirements.txt)

## Installation

1. Clone this repository
2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Create a `.env` file in the project root with the following variables:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password
DB_TYPE=postgresql  # or mysql

# Elasticsearch Configuration (Docker)
ES_HOST=localhost
ES_PORT=9200
ES_SCHEME=http
```

## Running with Docker

1. Start Elasticsearch and Kibana:
   ```bash
   docker-compose up -d
   ```

2. Wait for the services to start (about 30 seconds)

3. Run the sync application:
   ```bash
   python data_lake_sync.py
   ```

4. Access Kibana at http://localhost:5601

## Visualizing Data in Kibana

Once the sync is complete, follow these steps to visualize your data:

1. Open Kibana in your browser (http://localhost:5601)

2. Create an index pattern:
   - Go to "Stack Management" > "Index Patterns"
   - Click "Create index pattern"
   - Enter `data_lake_*` as the pattern
   - Click "Next step"
   - Select a time field if available, or click "Create index pattern"

   Note: When searching for specific tables, remember that index names are lowercase,
   regardless of the original table name casing.

3. Explore your data:
   - Go to "Discover" to view and search your data
   - Use "Visualize" to create charts and graphs
   - Create dashboards to combine multiple visualizations

4. Example visualizations you can create:
   - Bar charts for categorical data
   - Line charts for time series data
   - Pie charts for distribution analysis
   - Data tables for detailed views
   - Maps for geographical data

## Notes

- Each table is synced to a separate Elasticsearch index with the prefix `data_lake_`
- Table names are converted to lowercase for Elasticsearch indices due to Elasticsearch naming requirements
- The application handles data in batches to manage memory usage
- Progress is shown for each table being synced
- Errors are logged but won't stop the entire process
- The Docker setup uses a single-node Elasticsearch configuration for development
- Data is persisted in a Docker volume named `elasticsearch-data`# sql-database-to-elastic-datalake
