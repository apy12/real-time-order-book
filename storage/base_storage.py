# storage/base_storage.py
from abc import ABC, abstractmethod
from utils.logger import logger

class BaseStorage(ABC):
    def __init__(self, db_config: dict):
        self.db_config = db_config
        logger.info("Initialized BaseStorage.")

    @abstractmethod
    def connect(self):
        """Establishes a database connection."""
        pass

    @abstractmethod
    def disconnect(self):
        """Closes the database connection."""
        pass

    @abstractmethod
    def create_tables(self):
        """Creates necessary tables if they don't exist."""
        pass

    @abstractmethod
    def insert_order_book_snapshot(self, snapshot_data: dict):
        """Inserts an order book snapshot and its bids/asks into the database."""
        pass