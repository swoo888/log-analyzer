import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

import pytz

from analyzer.session import SessionAnalyzer


async def main():
    baseUri = os.getenv("fetcherBaseUri")
    queryParam = os.getenv("fetcherQueryParam")
    # endTime = datetime.now(timezone.utc)
    tz = pytz.timezone("US/Pacific")
    endTime = datetime(2023, 4, 18, 15, 0, 0, 0)
    endTime = tz.localize(endTime)
    startTime = endTime - timedelta(hours=2)
    # startTime = endTime - timedelta(days=30)
    authToken = os.getenv("fetcherToken")
    sourceGroup = os.getenv("fetcherSourceGroup")

    analalyzer = SessionAnalyzer(baseUri, queryParam, authToken, sourceGroup)
    await analalyzer.analyze(startTime, endTime)


# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)
asyncio.run(main())
