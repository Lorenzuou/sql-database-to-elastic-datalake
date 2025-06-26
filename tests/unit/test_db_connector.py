import pytest
import pandas as pd
from datetime import datetime
from src.db_connector import DatabaseConnector

def test_get_tickets_and_labels(mock_db_connector):
    """Test getting tickets and labels from database."""
    # Test getting all tickets
    tickets, labels = mock_db_connector.get_tickets_and_labels()
    assert isinstance(tickets, pd.DataFrame)
    assert isinstance(labels, pd.DataFrame)
    assert len(tickets) == 2
    assert len(labels) == 3
    
    # Test getting specific ticket
    tickets, labels = mock_db_connector.get_tickets_and_labels(ticket_id='1')
    assert len(tickets) == 1
    assert tickets.iloc[0]['ticket_id'] == '1'
    assert len(labels) == 2  # Two labels for ticket 1

def test_get_data_sources(mock_db_connector):
    """Test getting data sources from database."""
    data_sources = mock_db_connector.get_data_sources()
    assert isinstance(data_sources, pd.DataFrame)
    assert len(data_sources) == 2
    assert list(data_sources['id']) == ['ds1', 'ds2']
    assert list(data_sources['name']) == ['Source 1', 'Source 2']

def test_get_users(mock_db_connector):
    """Test getting users from database."""
    users = mock_db_connector.get_users()
    assert isinstance(users, pd.DataFrame)
    assert len(users) == 2
    assert list(users['id']) == ['user1', 'user2']
    assert list(users['name']) == ['User 1', 'User 2']

def test_get_modules(mock_db_connector):
    """Test getting modules from database."""
    modules = mock_db_connector.get_modules()
    assert isinstance(modules, pd.DataFrame)
    assert len(modules) == 2
    assert list(modules['id']) == ['module1', 'module2']
    assert list(modules['name']) == ['Module A', 'Module B']

def test_get_statuses(mock_db_connector):
    """Test getting statuses from database."""
    statuses = mock_db_connector.get_statuses()
    assert isinstance(statuses, pd.DataFrame)
    assert len(statuses) == 2
    assert list(statuses['id']) == ['status1', 'status2']
    assert list(statuses['name']) == ['Open', 'Closed']

def test_get_labels(mock_db_connector):
    """Test getting labels from database."""
    labels = mock_db_connector.get_labels()
    assert isinstance(labels, pd.DataFrame)
    assert len(labels) == 2
    assert list(labels['id']) == ['label1', 'label2']
    assert list(labels['name']) == ['Bug', 'Feature'] 