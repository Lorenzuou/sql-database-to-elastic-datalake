import logging
from src.data_sync import DataLakeSync

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def sync_all_data():
    """Sync all data to the data lake."""
    logger.info("Starting data sync")
    try:
        sync = DataLakeSync()
        sync.sync_all_tables()
        logger.info("Completed data sync")
    except Exception as e:
        logger.error(f"Error during data sync: {str(e)}")


if __name__ == "__main__":
    sync_all_data() 