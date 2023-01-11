import asyncio
import logging
from datetime import datetime, timedelta

from aiohttp import ClientSession


class LoglyFetcher:
    MAX_RECORD_SIZE = 5000  # logly has 5000 records limit each fetch

    def __init__(
        self,
        baseUri: str,
        queryParam: str,
        authToken: str,
        sourceGroup: str,
        resultQueue: asyncio.Queue,
    ) -> None:
        self.baseUri = baseUri  # "http://companyName.loggly.com/apiv2"
        self.queryParam = queryParam
        self.authToken = authToken
        self.sourceGroup = sourceGroup
        self.resultQueue = resultQueue
        self.intervalSecs = 5  # default time interval to fetch data
        self.logger = logging.getLogger(LoglyFetcher.__name__)
        self.headers = {
            "Authorization": "bearer " + authToken,
            "Content-Type": "application/json",
        }

    def getUtcStr(self, utcTime: datetime) -> str:
        return utcTime.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    async def fetchTimeRange(self, timeFrom: datetime, timeTo: datetime) -> None:
        params = {
            "q": self.queryParam,
            "size": LoglyFetcher.MAX_RECORD_SIZE,
            "from": self.getUtcStr(timeFrom),
            "until": self.getUtcStr(timeTo),
            "source_group": self.sourceGroup,
        }
        async with ClientSession(headers=self.headers) as session:
            searchUri = self.baseUri + "/search"
            searchResp = await session.get(url=searchUri, params=params)
            searchResp.raise_for_status()
            # data = await searchResp.json()
            data = await searchResp.json()
            rsid = data["rsid"]["id"]
            self.logger.info("Search finished " + str(rsid))

            eventsUri = self.baseUri + "/events"
            params = {"rsid": rsid}
            eventsResp = await session.get(url=eventsUri, params=params)
            eventsResp.raise_for_status()
            jsonData = await eventsResp.json()
            await self.putData(jsonData)

    async def putData(self, jsonData: dict) -> None:
        totalEvents = jsonData["total_events"]
        self.logger.info(f"Search data received {totalEvents}")
        if totalEvents > self.MAX_RECORD_SIZE:
            self.logger.warn(
                f"total events is larger than {self.MAX_RECORD_SIZE}! Use smaller time interval."
            )
        await self.resultQueue.put(jsonData["events"])

    async def fetch(
        self, startTime: datetime, endTime: datetime, intervalSecs: int = 5
    ) -> None:
        start = startTime
        deltaSecs = timedelta(seconds=intervalSecs)
        while start < endTime:
            end = min(start + deltaSecs, endTime)
            await self.fetchTimeRange(start, end)
            start = start + deltaSecs
