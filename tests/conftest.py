import pytest
import pandas as pd
from datetime import datetime
from src.db_connector import DatabaseConnector
from src.es_connector import ElasticsearchConnector
from src.config import SYNC_CONFIG

@pytest.fixture
def mock_db_connector(mocker):
    """Mock database connector for unit tests."""
    mock_connector = mocker.Mock(spec=DatabaseConnector)
    
    # Mock sample data
    mock_tickets = pd.DataFrame({
        'ticket_id': ['1', '2'],
        'ticket_number': [1001, 1002],
        'ticket_data': ['{"key": "value1"}', '{"key": "value2"}'],
        'ticket_createdAt': [datetime.now(), datetime.now()],
        'ticket_updatedAt': [datetime.now(), datetime.now()],
        'status_id': ['status1', 'status2'],
        'status_name': ['Open', 'Closed'],
        'module_id': ['module1', 'module2'],
        'module_name': ['Module A', 'Module B']
    })
    
    mock_labels = pd.DataFrame({
        'ticketId': ['1', '1', '2'],
        'label_id': ['label1', 'label2', 'label3'],
        'label_name': ['Bug', 'High Priority', 'Feature'],
        'color': ['red', 'yellow', 'green']
    })
    
    # Setup mock methods with filtering
    def get_tickets_and_labels(ticket_id=None):
        if ticket_id:
            filtered_tickets = mock_tickets[mock_tickets['ticket_id'] == ticket_id]
            filtered_labels = mock_labels[mock_labels['ticketId'] == ticket_id]
            return filtered_tickets, filtered_labels
        return mock_tickets, mock_labels
    
    mock_connector.get_tickets_and_labels.side_effect = get_tickets_and_labels
    mock_connector.get_data_sources.return_value = pd.DataFrame({
        'id': ['ds1', 'ds2'],
        'name': ['Source 1', 'Source 2']
    })
    mock_connector.get_users.return_value = pd.DataFrame({
        'id': ['user1', 'user2'],
        'name': ['User 1', 'User 2']
    })
    mock_connector.get_modules.return_value = pd.DataFrame({
        'id': ['module1', 'module2'],
        'name': ['Module A', 'Module B']
    })
    mock_connector.get_statuses.return_value = pd.DataFrame({
        'id': ['status1', 'status2'],
        'name': ['Open', 'Closed']
    })
    mock_connector.get_labels.return_value = pd.DataFrame({
        'id': ['label1', 'label2'],
        'name': ['Bug', 'Feature']
    })
    
    return mock_connector

@pytest.fixture
def mock_es_connector(mocker):
    """Mock elasticsearch connector for unit tests."""
    mock_connector = mocker.Mock(spec=ElasticsearchConnector)
    
    # Setup mock methods
    mock_connector.create_index.return_value = True
    mock_connector.bulk_index.return_value = (2, [])  # (success_count, failed_items)
    mock_connector.get_document_count.return_value = 2
    
    return mock_connector

@pytest.fixture
def test_config():
    """Test configuration."""
    return {
        'index_prefix': 'test_',
        'refresh_interval': '1s',
        'batch_size': 50
    } 