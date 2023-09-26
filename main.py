import asyncio
import csv
import json
import logging
import os
from datetime import datetime, timedelta

import pytz

from analyzer.body_error import BodyErrorAnalyzer
from analyzer.session import SessionAnalyzer
from controller.controller import Controller
from fetcher.body_error import BodyErrorFetcher
from fetcher.loggly import LogglyFetcher
from utils.logging_config import LoggerConfig


async def analyzeSession():
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


async def analyzeBodyError():
    logger = logging.getLogger(__name__)
    resultQueue = asyncio.Queue()
    csvFile = os.getenv("bodyErrorCsv")
    fetcher = BodyErrorFetcher(logger, resultQueue, csvFile)
    analyzer = BodyErrorAnalyzer(logger)
    controller = Controller(logger, fetcher, analyzer, datetime.min, datetime.max)
    await controller.run()


timeStart = datetime.now()
LoggerConfig.setUpBasicLogging()

# asyncio.run(analyzeSession())
asyncio.run(analyzeBodyError())

timeSpent = datetime.now() - timeStart
print(f"total time spent: {timeSpent}")
