import asyncio
import logging
import os
from datetime import datetime, timedelta

import pytz

from analyzer.session import SessionAnalyzer
from controller.controller import Controller
from fetcher.loggly import LogglyFetcher
from utils.loggingConfig import LoggerConfig


async def analyze_session():
    logger = logging.getLogger(__name__)
    resultQueue = asyncio.Queue()

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

    fetcher = LogglyFetcher(resultQueue, baseUri, queryParam, authToken, sourceGroup)
    analyzer = SessionAnalyzer(logger)
    controller = Controller(logger, fetcher, analyzer, startTime, endTime)
    result = await controller.run()
    logger.info(result)


timeStart = datetime.now()
LoggerConfig.setUpBasicLogging()

asyncio.run(analyze_session())

timeSpent = datetime.now() - timeStart
print(f"total time spent: {timeSpent}")
