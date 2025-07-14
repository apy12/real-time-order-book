# scheduler/data_scheduler.py
import asyncio
from utils.logger import logger

class DataScheduler:
    def __init__(self, collector, storage, interval_seconds: int = 1):
        self.collector = collector
        self.storage = storage
        self.interval_seconds = interval_seconds
        self._running = False
        logger.info(f"DataScheduler initialized with {interval_seconds}-second interval.")

    async def _collect_and_store(self, symbol: str):
        snapshot = await self.collector.fetch_order_book(symbol)
        if snapshot:
            self.storage.insert_order_book_snapshot(snapshot)

    async def start(self):
        logger.info("Starting data collection scheduler...")
        self._running = True
        self.storage.connect()
        self.storage.create_tables()

        tasks = []
        for symbol in self.collector.symbols:
            async def periodic_task(s):
                while self._running:
                    await self._collect_and_store(s)
                    await asyncio.sleep(self.interval_seconds)
            tasks.append(periodic_task(symbol))

        await asyncio.gather(*tasks)

    def stop(self):
        logger.info("Stopping data collection scheduler...")
        self._running = False
        self.storage.disconnect()