import asyncio
import logging
import os
from datetime import datetime, timedelta

import pytz

from analyzer.session import SessionAnalyzer


async def main():
    baseUri = os.getenv("fetcherBaseUri")
    queryParam = os.getenv("fetcherQueryParam")
    authToken = os.getenv("fetcherToken")
    sourceGroup = os.getenv("fetcherSourceGroup")
    tz = pytz.timezone("US/Pacific")
    # endTime = datetime(2023, 4, 18, 15, 0, 0, 0)
    endTime = datetime.now()
    endTime = tz.localize(endTime)
    daysOffset = 0
    endTime = endTime - timedelta(days=daysOffset)
    durationDays = 30
    startTime = endTime - timedelta(days=durationDays)

    analalyzer = SessionAnalyzer(baseUri, queryParam, authToken, sourceGroup)
    await analalyzer.analyze(startTime, endTime)


# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
timeStart = datetime.now()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
asyncio.run(main())
timeSpent = datetime.now() - timeStart
print(f"total time spent: {timeSpent}")
