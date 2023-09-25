import json
import logging
from typing import Any

from analyzer.analyzer import Analyzer


class SessionAnalyzer(Analyzer):
    def __init__(
        self,
        logger: logging.Logger,
    ) -> None:
        self.logger = logger
        self.sids = {}
        self.psidSids = {}
        self.channelSids = {}

    async def analyze(self, data: list[Any]):
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
            sidKey = str(sid)
            self.sids[sidKey] = self.sids.get(sidKey, 0) + 1
            if deviceId == "{PSID}":
                self.psidSids[sidKey] = self.psidSids.get(sidKey, 0) + 1
            elif deviceId == "channel":
                self.channelSids[sidKey] = self.channelSids.get(sidKey, 0) + 1

    def getResult(self):
        labels = ["All sid", "deviceId=PSID", "deviceId=channel"]
        data = [self.sids, self.psidSids, self.channelSids]
        result = {}
        for i in range(len(data)):
            label = labels[i]
            dataDict = data[i]
            descData = sorted(dataDict.items(), key=lambda x: x[1], reverse=True)
            self.logger.info(f"result for {label}: ")
            count = len(descData)
            countIncludingEmpty = count + dataDict.get("", 0)
            result[label] = {count, countIncludingEmpty}
        return json.dumps(result)
