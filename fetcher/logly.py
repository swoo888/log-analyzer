import asyncio
import logging
from datetime import datetime, timedelta

from aiohttp import ClientSession, TCPConnector


class LoglyFetcher:
    MAX_RECORD_SIZE = 1000  # loggly's max fetch size limit
    FETCH_INTERVAL_SECS = 5 * 60  # loggly data fetch interval

    def __init__(
        self,
        baseUri: str,
        queryParam: str,
        authToken: str,
        sourceGroup: str,
        resultQueue: asyncio.Queue,
        maxConcurrency: int = 20,
        maxRetries: int = 100,
    ) -> None:
        self.baseUri = baseUri  # "http://companyName.loggly.com/apiv2"
        self.queryParam = queryParam
        self.authToken = authToken
        self.sourceGroup = sourceGroup
        self.resultQueue = resultQueue
        self.logger = logging.getLogger(LoglyFetcher.__name__)
        self.headers = {
            "Authorization": "bearer " + authToken,
            "Content-Type": "application/json",
        }
        self.finished = False
        self.maxConcurrency = maxConcurrency
        self.maxRetries = maxRetries

    def getUtcStr(self, utcTime: datetime) -> str:
        return utcTime.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def isFinished(self) -> bool:
        return self.finished

    async def fetchJson(
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

    async def fetchTimeRange(
        self,
        session: ClientSession,
        timeFrom: datetime,
        timeTo: datetime,
    ) -> None:
        params = {
            "q": self.queryParam,
            "size": LoglyFetcher.MAX_RECORD_SIZE,
            "from": self.getUtcStr(timeFrom),
            "until": self.getUtcStr(timeTo),
        }
        url = self.baseUri + "events/iterate"
        while url:
            jsonData = await self.fetchJson(session, url, params)
            if jsonData is None:
                raise Exception("fetching data failed with max tries")
            await self.putData(jsonData)
            url = jsonData.get("next", "")

    async def putData(self, jsonData: dict) -> None:
        events = jsonData["events"]
        self.logger.info(f"Search data received {len(events)}")
        await self.resultQueue.put(events)

    async def fetch(
        self, startTime: datetime, endTime: datetime, intervalSecs: int = FETCH_INTERVAL_SECS
    ) -> None:
        start = startTime
        deltaSecs = timedelta(seconds=intervalSecs)
        tasks = []
        tcpConn = TCPConnector(limit=self.maxConcurrency)
        async with ClientSession(headers=self.headers, connector=tcpConn) as session:
            while start < endTime:
                end = min(start + deltaSecs, endTime)
                self.logger.info(f"fetching {start}, {end}")
                tasks.append(self.fetchTimeRange(session, start, end))
                start = start + deltaSecs
                if len(tasks) >= self.maxConcurrency:
                    await asyncio.gather(*tasks)
                    self.logger.info(f"finished batch of {self.maxConcurrency} at {endTime}")
                    tasks = []
            if len(tasks) > 0:
                await asyncio.gather(*tasks)
        await self.resultQueue.put(None)  # Allow analyzer await function to exist
        self.finished = True
        self.logger.info("=== Finished data Fetch ===")
