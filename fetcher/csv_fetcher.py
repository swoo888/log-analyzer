import abc
import asyncio
import csv
import logging
from datetime import datetime
from typing import Generic, TypeVar

from fetcher.fetcher import Fetcher

T = TypeVar("T")


class CsvFetcher(Fetcher, Generic[T]):
    def __init__(
        self,
        logger: logging.Logger,
        resultQueue: asyncio.Queue,
        filePath: str,
    ) -> None:
        super().__init__(logger, resultQueue)
        self.logger = logger
        self.filePath = filePath
        # increase csv field size limit so it works with large data fields
        csv.field_size_limit(100000000)

    async def fetch(
        self,
        startTime: datetime,
        endTime: datetime,
    ) -> None:
        with open(self.filePath) as f:
            csv_reader = csv.DictReader(f)

            for line in csv_reader:
                lineData = self.getLineData(line)
                self.logger.info(f"putting data {lineData}")
                await self.resultQueue.put(lineData)
        await self.done()

    @abc.abstractclassmethod
    def getLineData(self, line: dict[str, str]) -> T:
        pass
