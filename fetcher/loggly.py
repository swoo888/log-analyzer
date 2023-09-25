import asyncio
import logging
from datetime import datetime, timedelta

from aiohttp import ClientSession, TCPConnector

from fetcher.fetcher import Fetcher


class LogglyFetcher(Fetcher):
    MAX_RECORD_SIZE = 1000  # loggly's max fetch size limit
    FETCH_INTERVAL_SECS = 5 * 60  # loggly data fetch interval

    def __init__(
        self,
        logger: logging.Logger,
        resultQueue: asyncio.Queue,
        baseUri: str,
        queryParam: str,
        authToken: str,
        sourceGroup: str,
    ) -> None:
        super.__init__(logger, resultQueue)
        self.logger = logger
        self.baseUri = baseUri  # "http://companyName.loggly.com/apiv2"
        self.queryParam = queryParam
        self.authToken = authToken
        self.sourceGroup = sourceGroup
        self.headers = {
            "Authorization": "bearer " + authToken,
            "Content-Type": "application/json",
        }
        self.intervalSecs = self.FETCH_INTERVAL_SECS,

    async def _fetchJson(
        self,
        session: ClientSession,
        url: str,
        params: dict,
    ):
        tries = 1
        while tries < self.maxRetries:
            try:
                resp = await session.get(url=url, params=params)
                resp.raise_for_status()
                jsonData = await resp.json()
                return jsonData
            except Exception as ex:
                tries = tries + 1
                self.logger.error(ex)
                await asyncio.sleep(1)
        return None

    async def _fetchTimeRange(
        self,
        session: ClientSession,
        timeFrom: datetime,
        timeTo: datetime,
    ) -> None:
        params = {
            "q": self.queryParam,
            "size": self.MAX_RECORD_SIZE,
            "from": self.getUtcStr(timeFrom),
            "until": self.getUtcStr(timeTo),
        }
        url = self.baseUri + "events/iterate"
        while url:
            jsonData = await self._fetchJson(session, url, params)
            if jsonData is None:
                raise Exception("fetching data failed with max tries")
            await self._putData(jsonData)
            url = jsonData.get("next", "")

    async def _putData(self, jsonData: dict) -> None:
        events = jsonData["events"]
        self.logger.info(f"Search data received {len(events)}")
        await self.resultQueue.put(events)

    async def fetch(
        self,
        startTime: datetime,
        endTime: datetime,
    ) -> None:
        start = startTime
        deltaSecs = timedelta(seconds=self.intervalSecs)
        tasks = []
        tcpConn = TCPConnector(limit=self.maxConcurrency)
        async with ClientSession(headers=self.headers, connector=tcpConn) as session:
            while start < endTime:
                end = min(start + deltaSecs, endTime)
                self.logger.info(f"fetching {start}, {end}")
                tasks.append(self._fetchTimeRange(session, start, end))
                start = start + deltaSecs
                if len(tasks) >= self.maxConcurrency:
                    await asyncio.gather(*tasks)
                    self.logger.info(
                        f"finished batch of {self.maxConcurrency} at {endTime}"
                    )
                    tasks = []
            if len(tasks) > 0:
                await asyncio.gather(*tasks)
        await self.done()
