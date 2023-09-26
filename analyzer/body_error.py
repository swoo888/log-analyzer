import csv
import json
import logging
import os

from analyzer.analyzer import Analyzer
from fetcher.body_error import BodyError


class BodyErrorAnalyzer(Analyzer):
    def __init__(
        self,
        logger: logging.Logger,
    ) -> None:
        self.logger = logger
        self.errors = {}

    async def analyze(self, data: BodyError):
        if not data:
            return
        attributeError = self.normalize(data.attributeError)
        bodyMessage = self.normalize(data.bodyMessage)
        error = attributeError if attributeError else bodyMessage
        if error:
            self.putError(error)

    def putError(self, error: str) -> None:
        error = error.strip("\"")
        cnt = self.errors.get(error, 0)
        self.errors[error] = cnt + 1

    def normalize(self, error: str) -> str:
        if not error:
            return ""
        error = error.strip("\"")
        strs = error.split(":")
        return strs[0]

    def getResult(self):
        sortedErrors = sorted(self.errors.items(), key=lambda x: x[1], reverse=True)
        self._writeCsv(sortedErrors)
        return json.dumps(sortedErrors)

    def _writeCsv(self, sortedErrors: list[tuple]) -> None:
        fieldNames = ["error", "count"]
        bodyErrorsDict = [{"error": x[0], "count": x[1]} for x in sortedErrors]
        outputFile = os.getenv("bodyErrorCsvOut")
        with open(outputFile, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldNames)
            writer.writeheader()
            writer.writerows(bodyErrorsDict)
