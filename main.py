# main.py
import nest_asyncio
nest_asyncio.apply()

import asyncio
from config.database import DB_CONFIG
from config.exchanges import EXCHANGE_CONFIG
from collector.bitfinex_collector import BitfinexCollector
from storage.mysql_storage import MySQLStorage
from scheduler.data_scheduler import DataScheduler
from utils.logger import logger

async def main():
    logger.info("Starting the Real-Time Order Book Data Collection System.")

    # Initialize components
    bitfinex_symbols = EXCHANGE_CONFIG["bitfinex"]["symbols"]
    collector = BitfinexCollector("bitfinex", bitfinex_symbols)
    storage = MySQLStorage(DB_CONFIG)
    scheduler = DataScheduler(collector, storage, interval_seconds=1)

    try:
        await scheduler.start()
    except KeyboardInterrupt:
        logger.info("System interrupted by user. Shutting down...")
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main: {e}", exc_info=True)
    finally:
        scheduler.stop()
        logger.info("System shut down successfully.")

if __name__ == "__main__":
    asyncio.run(main())