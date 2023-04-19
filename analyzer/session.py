import asyncio
import logging
from datetime import datetime

from fetcher.logly import LoglyFetcher


class SessionAnalyzer:
    def __init__(
        self,
        baseUri: str,
        queryParam: str,
        authToken: str,
        sourceGroup: str,
    ) -> None:
        self.baseUri = baseUri  # "http://companyName.loggly.com/apiv2"
        self.queryParam = queryParam
        self.authToken = authToken
        self.sourceGroup = sourceGroup
        self.resultQueue = asyncio.Queue()
        self.sids = {}
        self.psidSids = {}
        self.channelSids = {}
        self.logger = logging.getLogger(SessionAnalyzer.__name__)

    async def analyze(self, timeFrom: datetime, timeTo: datetime) -> None:
        self.logger.info(f"=== Start analyzing data from {timeFrom} to {timeTo} ===")
        fetcher = LoglyFetcher(
            self.baseUri,
            self.queryParam,
            self.authToken,
            self.sourceGroup,
            self.resultQueue,
        )
        await asyncio.gather(fetcher.fetch(timeFrom, timeTo, intervalSecs=3), self.analyzeData(fetcher))

    async def analyzeData(self, fetcher: LoglyFetcher):
        while not fetcher.isFinished():
            data = await self.resultQueue.get()
            if not data:
                continue
            for item in data:
                if not item:
                    continue
                queryParams = (
                    item.get("event", {})
                    .get("json", {})
                    .get("req", {})
                    .get("queryParams", {})
                )
                if not queryParams:
                    continue
                sid = queryParams.get("sid", "")
                deviceId = queryParams.get("deviceId", "")
                sidKey = sid
                self.sids[sidKey] = self.sids.get(sidKey, 0) + 1
                if deviceId == "{PSID}":
                    self.psidSids[sidKey] = self.psidSids.get(sidKey, 0) + 1
                elif deviceId == "channel":
                    self.channelSids[sidKey] = self.channelSids.get(sidKey, 0) + 1
        self.outputResult()
        self.logger.info("analyzData completed")

    def outputResult(self):
        labels = ["All sid", "deviceId=PSID", "deviceId=channel"]
        data = [self.sids, self.psidSids, self.channelSids]
        for i in range(len(data)):
            label = labels[i]
            dataDict = data[i]
            descData = sorted(dataDict.items(), key=lambda x: x[1], reverse=True)
            self.logger.info(f"result for {label}: ")
            count = len(descData)
            countIncludingEmpty = count + dataDict.get("", 0)
            self.logger.info(
                f"Items Count: {count}, Count including empty: {countIncludingEmpty}"
            )
            # self.logger.info(pprint.pformat(descData))
