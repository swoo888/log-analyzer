import abc
import asyncio
import datetime
import logging
from typing import Any


class Fetcher:
    def __init__(
        self,
        logger: logging.Logger,
        resultQueue: asyncio.Queue,
        maxConcurrency: int = 8,
        maxRetries: int = 3,
    ) -> None:
        """Instantiate a fetcher with async Queue.

        Args:
            resultQueue (asyncio.Queue): Async queue to hold fetched data
            maxConcurrency (int, optional): Max concurrency. Defaults to 8.
            maxRetries (int, optional): Max retries. Defaults to 3.
        """
        self.logger = logger
        self.resulQueue = resultQueue
        self.maxConcurrency = maxConcurrency
        self.maxRetries = maxRetries
        self.finished = False

    def isFinished(self) -> bool:
        """Check if data fetch is complete.

        Returns:
            bool: Return true if all data were fetched.
        """
        return self.finished

    def getUtcStr(self, utcTime: datetime) -> str:
        """Get utc time string format output.

        Args:
            utcTime (datetime): A datetime object to get its string outpout.

        Returns:
            str: UTC format output.
        """
        return utcTime.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    async def getData(self) -> Any:
        """Get data from async queue.

        Returns:
            Any: Data from async queue.
        """
        return self.resultQueue.get()

    @abc.abstractclassmethod
    async def fetch(self, startTime: datetime, endTime: datetime) -> None:
        """Fetch data between startTime and endTime onto async resultQueue.

        Args:
            startTime (datetime): Start time.
            endTime (datetime): End time.
        """
        pass

    async def done(self) -> None:
        """Signal fetching of data completed.
        """
        await self.resultQueue.put(None)  # signal all fetching is complete
        self.finished = True
        self.logger.info("=== Finished data Fetch ===")
