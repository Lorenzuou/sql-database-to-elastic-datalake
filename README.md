# Data Lake Sync Application

A comprehensive data synchronization system that creates a searchable data lake by syncing your database tables with Elasticsearch. This application provides both automated data synchronization and a REST API for real-time data operations, making it easy to search and visualize your data using Kibana.

## 🚀 Features

- **Multi-Database Support**: Works with PostgreSQL and MySQL databases
- **Elasticsearch Integration**: Automatic indexing with optimized mappings
- **REST API**: Full CRUD operations for tickets, users, modules, and more
- **Real-time Sync**: Incremental updates and batch processing
- **Search Capabilities**: Advanced search with Elasticsearch queries
- **Docker Support**: Easy deployment with Docker Compose
- **Data Validation**: Comprehensive data sanitization and validation
- **Historical Tracking**: Maintains data lineage and change history

## 📋 Prerequisites

- Python 3.8+
- Docker and Docker Compose
- PostgreSQL or MySQL database
- Elasticsearch 8.11.0
- Kibana 8.11.0

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd simplelake
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Environment Configuration

Create a `.env` file in the root directory:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password
DB_TYPE=postgresql  # or mysql

# Elasticsearch Configuration
ES_HOST=localhost
ES_PORT=9200
ES_SCHEME=http

# Optional: Enable Search API
SEARCH_API_ENABLED=true
```

### 4. Start Infrastructure with Docker

```bash
docker-compose up -d
```

This will start:
- Elasticsearch on port 9200
- Kibana on port 5601

## 🏗️ Project Structure

```
simplelake/
├── src/
│   ├── config.py              # Configuration management
│   ├── db_connector.py        # Database connection and queries
│   ├── es_connector.py        # Elasticsearch operations
│   ├── data_sync.py           # Main synchronization logic
│   ├── document_utils.py      # Document processing utilities
│   ├── json_encoder.py        # Custom JSON serialization
│   ├── utils.py               # Shared utilities
│   └── routes/                # REST API endpoints
│       ├── ticket_routes.py   # Ticket operations
│       ├── user_routes.py     # User operations
│       ├── module_routes.py   # Module operations
│       ├── status_routes.py   # Status operations
│       ├── label_routes.py    # Label operations
│       └── data_source_routes.py # Data source operations
├── tests/                     # Test suite
├── data_lake_sync.py          # Legacy sync script
├── server.py                  # Flask application server
├── docker-compose.yml         # Docker infrastructure
├── requirements.txt           # Python dependencies
└── schema.sql                # Database schema
```

## 🚀 Usage

### Starting the Application

```bash
python server.py
```

The server will start on port 5000 with the following endpoints:

- `GET /health` - Health check
- `POST /tickets/tickets` - Add new ticket
- `POST /tickets/batch` - Add multiple tickets
- `POST /tickets/sync` - Sync tickets from database
- `GET /users/users` - Get all users
- `POST /users/sync` - Sync users from database
- `GET /modules/modules` - Get all modules
- `POST /modules/sync` - Sync modules from database
- And more...

### Data Synchronization

#### Manual Sync

```bash
python -c "
from src.data_sync import DataLakeSync
sync = DataLakeSync()
sync.sync_all_tables()
"
```

#### Individual Table Sync

```python
from src.data_sync import DataLakeSync

sync = DataLakeSync()

# Sync specific tables
sync.sync_data_sources()
sync.sync_users()
sync.sync_modules()
sync.sync_statuses()
sync.sync_labels()
```

### API Examples

#### Add a New Ticket

```bash
curl -X POST http://localhost:5000/tickets/tickets \
  -H "Content-Type: application/json" \
  -d '{
    "ticket_id": "123",
    "title": "Sample Ticket",
    "description": "This is a sample ticket",
    "status": "open",
    "priority": "high"
  }'
```

#### Search Tickets

```bash
curl -X POST http://localhost:5000/tickets/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "title": "sample"
      }
    }
  }'
```

## 🔧 Configuration

### Database Configuration

The application supports both PostgreSQL and MySQL:

```python
# PostgreSQL
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password

# MySQL
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password
```

### Elasticsearch Configuration

```python
ES_HOST=localhost
ES_PORT=9200
ES_SCHEME=http  # or https for secure connections
```

### Sync Configuration

```python
SYNC_CONFIG = {
    'batch_size': 1000,        # Documents per batch
    'index_prefix': 'data_lake_',  # Index naming prefix
    'refresh_interval': '1s'   # Index refresh interval
}
```

## 📊 Data Model

The application syncs the following entities:

### Core Entities

- **Tickets**: Main workflow items with labels and status
- **Users**: System users with roles and preferences
- **Modules**: System modules with hierarchical structure
- **Statuses**: Workflow status definitions
- **Labels**: Categorization labels for tickets
- **Data Sources**: External data source configurations

### Database Schema

The application expects tables in a `copy` schema (or default schema) with the following structure:

- `Ticket` - Main ticket data
- `User` - User information
- `Module` - Module definitions
- `Status` - Status definitions
- `Label` - Label definitions
- `DataSource` - Data source configurations
- `TicketLabel` - Many-to-many relationship
- `TicketStatus` - Status tracking

## 🔍 Search Capabilities

### Basic Search

```python
# Simple text search
{
    "query": {
        "match": {
            "title": "search term"
        }
    }
}
```

### Advanced Search

```python
# Complex queries with filters
{
    "query": {
        "bool": {
            "must": [
                {"match": {"title": "urgent"}},
                {"term": {"status": "open"}}
            ],
            "filter": [
                {"range": {"created_at": {"gte": "2024-01-01"}}}
            ]
        }
    }
}
```

## 🧪 Testing

Run the test suite:

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run tests
pytest

# Run with coverage
pytest --cov=src
```

## 🐳 Docker Deployment

### Development

```bash
# Start infrastructure
docker-compose up -d

# Run application
python server.py
```

### Production

Create a production Dockerfile:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 5000

CMD ["python", "server.py"]
```

## 📈 Monitoring

### Health Check

```bash
curl http://localhost:5000/health
```

### Elasticsearch Status

```bash
curl http://localhost:9200/_cluster/health
```

### Index Statistics

```bash
curl http://localhost:9200/_cat/indices?v
```

## 🔒 Security Considerations

- Use environment variables for sensitive configuration
- Implement proper authentication for production use
- Enable Elasticsearch security features in production
- Use HTTPS for external communications
- Regularly update dependencies

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:

1. Check the existing issues
2. Create a new issue with detailed information
3. Include logs and error messages
4. Provide steps to reproduce the problem

## 🔄 Version History

- **v0.1.0**: Initial release with basic sync functionality
- Added REST API endpoints
- Multi-database support
- Docker deployment
- Comprehensive testing suite
