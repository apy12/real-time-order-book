# collector/base_collector.py
from abc import ABC, abstractmethod
from utils.logger import logger

class BaseCollector(ABC):
    def __init__(self, exchange_id: str, symbols: list):
        self.exchange_id = exchange_id
        self.symbols = symbols
        logger.info(f"Initialized BaseCollector for {exchange_id} with symbols: {symbols}")

    @abstractmethod
    async def fetch_order_book(self, symbol: str):
        """
        Fetches the order book for a given symbol from the exchange.
        Should return data in the specified format.
        """
        pass