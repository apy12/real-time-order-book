# collector/bitfinex_collector.py
import ccxt.async_support as ccxt
import asyncio
from collector.base_collector import BaseCollector
from utils.logger import logger

class BitfinexCollector(BaseCollector):
    def __init__(self, exchange_id: str, symbols: list):
        super().__init__(exchange_id, symbols)
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,
            # Add API keys if needed, but public order book usually doesn't require them
            # 'apiKey': 'YOUR_BITFINEX_API_KEY',
            # 'secret': 'YOUR_BITFINEX_SECRET',
        })
        logger.info(f"BitfinexCollector initialized for {exchange_id}")

    async def fetch_order_book(self, symbol: str):
        try:
            orderbook = await self.exchange.fetch_order_book(symbol)
            # Ensure the orderbook includes all required fields
            orderbook['symbol'] = symbol
            orderbook['exchange_id'] = self.exchange_id
            if 'timestamp' not in orderbook:
                orderbook['timestamp'] = self.exchange.milliseconds()
            if 'datetime' not in orderbook:
                orderbook['datetime'] = self.exchange.iso8601(orderbook['timestamp'])

            logger.debug(f"Fetched order book for {symbol} on {self.exchange_id}")
            return orderbook
        except ccxt.NetworkError as e:
            logger.error(f"Network error fetching {symbol} from {self.exchange_id}: {e}")
            return None
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error fetching {symbol} from {self.exchange_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching {symbol} from {self.exchange_id}: {e}")
            return None
        finally:
            await self.exchange.close() # Close connection after fetching