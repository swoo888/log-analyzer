import asyncio
import logging
import os
import unittest
from datetime import datetime, timedelta, timezone

from fetcher.loggly import LogglyFetcher


class TestLoglyFetcher(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.logger = logging.getLogger(__name__)
        self.baseUri = os.getenv("fetcherBaseUri")
        self.queryParam = os.getenv("fetcherQueryParam")
        self.endTime = datetime.now(timezone.utc)
        self.startTime = self.endTime - timedelta(seconds=15)
        self.authToken = os.getenv("fetcherToken")
        self.sourceGroup = os.getenv("fetcherSourceGroup")
        self.resultQueue = asyncio.Queue()

        self.fetcher = LogglyFetcher(
            self.logger,
            self.resultQueue,
            self.baseUri,
            self.queryParam,
            self.authToken,
            self.sourceGroup,
        )

    async def test_fetch(self):
        await self.fetcher.fetch(self.startTime, self.endTime)
        self.assertFalse(self.resultQueue.empty())
