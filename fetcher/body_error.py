import asyncio
import csv
import logging
from datetime import datetime

from fetcher.fetcher import Fetcher


class BodyError:
    def __init__(self, date, attributeError, bodyMessage):
        self.date = date
        self.attributeError = attributeError
        self.bodyMessage = bodyMessage


class BodyErrorFetcher(Fetcher):
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
                lineData = self.getBodyData(line)
                self.logger.info(f"putting data {lineData}")
                await self.resultQueue.put(lineData)
        await self.done()

    def getBodyData(self, line: dict[str, str]) -> BodyError:
        date = line.get("Date", "")
        attributeError = line.get("@Body.Attributes.metadata.error", "")
        bodyMessage = line.get("@Body.message", "")
        return BodyError(
            date,
            attributeError,
            bodyMessage,
        )
