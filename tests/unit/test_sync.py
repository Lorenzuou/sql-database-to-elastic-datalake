import pytest
from datetime import datetime
from src.data_sync import DataLakeSync


def test_sync_data_sources(mock_db_connector, mock_es_connector, test_config, mocker):
    """Test syncing data sources to Elasticsearch."""
    # Mock config
    mocker.patch('src.config.SYNC_CONFIG', test_config)
    
    # Create sync instance with mocked connectors
    sync = DataLakeSync()
    sync.db_connector = mock_db_connector
    sync.es_connector = mock_es_connector
    
    # Test sync
    sync.sync_data_sources()
    
    # Verify Elasticsearch operations
    mock_es_connector.create_index.assert_called_once()
    mock_es_connector.bulk_index.assert_called()
    mock_es_connector.get_document_count.assert_called()


def test_sync_users(mock_db_connector, mock_es_connector, test_config, mocker):
    """Test syncing users to Elasticsearch."""
    # Mock config
    mocker.patch('src.config.SYNC_CONFIG', test_config)
    
    # Create sync instance with mocked connectors
    sync = DataLakeSync()
    sync.db_connector = mock_db_connector
    sync.es_connector = mock_es_connector
    
    # Test sync
    sync.sync_users()
    
    # Verify Elasticsearch operations
    mock_es_connector.create_index.assert_called_once()
    mock_es_connector.bulk_index.assert_called()
    mock_es_connector.get_document_count.assert_called()


def test_sync_modules(mock_db_connector, mock_es_connector, test_config, mocker):
    """Test syncing modules to Elasticsearch."""
    # Mock config
    mocker.patch('src.config.SYNC_CONFIG', test_config)
    
    # Create sync instance with mocked connectors
    sync = DataLakeSync()
    sync.db_connector = mock_db_connector
    sync.es_connector = mock_es_connector
    
    # Test sync
    sync.sync_modules()
    
    # Verify Elasticsearch operations
    mock_es_connector.create_index.assert_called_once()
    mock_es_connector.bulk_index.assert_called()
    mock_es_connector.get_document_count.assert_called()


def test_sync_statuses(mock_db_connector, mock_es_connector, test_config, mocker):
    """Test syncing statuses to Elasticsearch."""
    # Mock config
    mocker.patch('src.config.SYNC_CONFIG', test_config)
    
    # Create sync instance with mocked connectors
    sync = DataLakeSync()
    sync.db_connector = mock_db_connector
    sync.es_connector = mock_es_connector
    
    # Test sync
    sync.sync_statuses()
    
    # Verify Elasticsearch operations
    mock_es_connector.create_index.assert_called_once()
    mock_es_connector.bulk_index.assert_called()
    mock_es_connector.get_document_count.assert_called()


def test_sync_labels(mock_db_connector, mock_es_connector, test_config, mocker):
    """Test syncing labels to Elasticsearch."""
    # Mock config
    mocker.patch('src.config.SYNC_CONFIG', test_config)
    
    # Create sync instance with mocked connectors
    sync = DataLakeSync()
    sync.db_connector = mock_db_connector
    sync.es_connector = mock_es_connector
    
    # Test sync
    sync.sync_labels()
    
    # Verify Elasticsearch operations
    mock_es_connector.create_index.assert_called_once()
    mock_es_connector.bulk_index.assert_called()
    mock_es_connector.get_document_count.assert_called()


def test_sync_all_tables(mock_db_connector, mock_es_connector, test_config, mocker):
    """Test syncing all tables to Elasticsearch."""
    # Mock config
    mocker.patch('src.config.SYNC_CONFIG', test_config)
    
    # Create sync instance with mocked connectors
    sync = DataLakeSync()
    sync.db_connector = mock_db_connector
    sync.es_connector = mock_es_connector
    
    # Test sync
    sync.sync_all_tables()
    
    # Verify all sync methods were called
    assert mock_es_connector.create_index.call_count == 5  # One for each type
    assert mock_es_connector.bulk_index.call_count >= 5  # At least one batch per type
    assert mock_es_connector.get_document_count.call_count >= 5  # At least one count per type 