import asyncio
import logging
from datetime import datetime

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
        self.logger.info(
            f"=== Run analysis from {self.startTime} to {self.endTime} ==="
        )
        await asyncio.gather(
            self.fetcher.fetch(self.startTime, self.endTime), self._analyze()
        )

    async def _analyze(self) -> None:
        self.logger.info(f"_analyze: {self.fetcher.isFinished()}")
        while True:
            data = await self.fetcher.getData()
            if data is None:
                self.logger.info("data is None")
                break
            else:
                self.logger.info(f"analyzing data ${data}")
                await self.analyzer.analyze(data)

        self.logger.info("analyzData completed")
        self.analyzer.dumpResult()
