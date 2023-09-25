import asyncio
from datetime import datetime
import logging

from analyzer.analyzer import Analyzer
from fetcher.fetcher import Fetcher


class Controller:
    def __init__(
        self,
        logger: logging.Logger,
        fetcher: Fetcher,
        analyzer: Analyzer,
        startTime: datetime,
        endTime: datetime,
    ):
        self.logger = logger
        self.fetcher = fetcher
        self.analyzer = analyzer
        self.startTime = startTime
        self.endTime = endTime

    async def run(self):
        self.logger.info(f"=== Run analysis from {self.startTime} to {self.endTime} ===")
        await asyncio.gather(
            self.fetcher.fetch(self.startTime, self.endTime), self._analyze()
        )

    async def _analyze(self):
        while not self.fetcher.isFinished():
            data = await self.getData()
            if data is None:
                continue
            self.analyzer.analyze(data)
        self.logger.info("analyzData completed")
        result = self.analyzer.getResult()
        self.logger.info(result)
