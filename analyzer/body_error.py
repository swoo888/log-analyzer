import csv
import json
import logging
from typing import Union

from analyzer.analyzer import Analyzer
from fetcher.body_error import BodyError
from fetcher.daily_totals import DailyTotal


class BodyErrorAnalyzer(Analyzer):
    def __init__(
        self,
        logger: logging.Logger,
        csvErrorsOutputFile: str,
        csvErrDateCntsOutputFile: str,
        csvErrDatePercentageOutputFile: str,
    ) -> None:
        self.logger = logger
        self.csvErrorsOutputFile = csvErrorsOutputFile
        self.csvErrDateCntsOutputFile = csvErrDateCntsOutputFile
        self.csvErrDatePercentageOutputFile = csvErrDatePercentageOutputFile
        self.errors = {}
        # holds error type daily counts. {"error1": {"2023-09-08": 10, "2023-09-09": 13}, }
        self.ErrDateCnts = {}
        #  holds all the unique error dates
        self.errorDates = set()
        # holds daily totals for all errors. {"2023-09-08": 100, "2023-09-09": 200}
        self.DateTotals = {}
        # holds daily submit totals
        self.DateSumbitTotals = {}

    async def analyze(self, data: Union[BodyError, DailyTotal]):
        if not data:
            return
        if isinstance(data, BodyError):
            dateStr = self.getDate(data.date)
            attributeError = self.normalize(data.attributeError)
            bodyMessage = self.normalize(data.bodyMessage)
            error = attributeError if attributeError else bodyMessage
            if error:
                self.putError(dateStr, error)
        elif isinstance(data, DailyTotal):
            dateStr = self.getDate(data.date)
            total = data.total
            self.DateSumbitTotals[dateStr] = total
        else:
            raise Exception("invalid data type")

    def putError(self, dateStr: str, error: str) -> None:
        # There are 2 types of error: Error or error.  We group them here
        if error.lower() == "error":
            error = "error"
        cnt = self.errors.get(error, 0)
        self.errors[error] = cnt + 1

        # update error daily count
        self.errorDates.add(dateStr)
        dateErrCnts = self.ErrDateCnts.get(error, {})
        dateErrCnt = dateErrCnts.get(dateStr, 0)
        dateErrCnts[dateStr] = dateErrCnt + 1
        self.ErrDateCnts[error] = dateErrCnts

        # update error daily total
        dateTotal = self.DateTotals.get(dateStr, 0)
        dateTotal = dateTotal + 1
        self.DateTotals[dateStr] = dateTotal

    def getDate(self, dateStr: str) -> str:
        """Get date part from a date string in format of 2023-09-22T17:53:44.362Z

        Args:
            dateStr (str): Date string to extract the date part

        Returns:
            str: Date string
        """
        if not dateStr:
            return ""
        dateStr = dateStr.strip('"')
        strs = dateStr.split("T")
        return strs[0]

    def normalize(self, error: str) -> str:
        if not error:
            return ""
        error = error.strip('"')
        strs = error.split(":")
        return strs[0]

    def _getPercent(self, dateCnts: tuple[str, int], dateTotals: {str: int}) -> float:
        dateStr = dateCnts[0]
        cnt = dateCnts[1]
        dateTotal = dateTotals.get(dateStr, 0)
        if cnt == 0 or dateTotal == 0:
            return 0
        return round(cnt / dateTotal, 2)

    def dumpResult(self) -> None:
        # errors is {"error1": cnt, "error2": cnt}
        # sortedErrors is [["error1", cnt], ["error2", cnt]]
        sortedErrors = sorted(self.errors.items(), key=lambda x: x[1], reverse=True)
        fieldNames = ["error", "count"]
        bodyErrorRows = [{"error": x[0], "count": x[1]} for x in sortedErrors]
        # output error category and count
        with open(self.csvErrorsOutputFile, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldNames)
            writer.writeheader()
            writer.writerows(bodyErrorRows)

        # process daily errors
        # errorDates is set of dates
        sortedErrorDates = list(sorted(self.errorDates))
        errTypeFieldName = "error type/datetime"
        fieldNames = [errTypeFieldName]
        fieldNames.extend(sortedErrorDates)
        dailyErrorRows = []
        dailyErrorPercentageRows = []
        for sortedErr in sortedErrors:
            err = sortedErr[0]
            dateCnts = self.ErrDateCnts.get(err, {})
            # sort by date. output is [[date, cnt], [date, cnt]]
            sortedDateCnts = sorted(dateCnts.items(), key=lambda x: x[0])
            dailyErrsDict = {x[0]: x[1] for x in sortedDateCnts}
            dailyErrsDict[errTypeFieldName] = err
            dailyErrorRows.append(dailyErrsDict)

            # daily error percentage to total submitted jobs
            dailyErrsPercentDict = {
                x[0]: self._getPercent(x, self.DateSumbitTotals) for x in sortedDateCnts
            }
            dailyErrsPercentDict[errTypeFieldName] = err
            dailyErrorPercentageRows.append(dailyErrsPercentDict)

        # dailyErrorRows is
        # [ {"error type/datetime": error, "2023-09-08": 32, "2023-09-09": 101},]
        self.logger.info(json.dumps(dailyErrorRows))
        with open(self.csvErrDateCntsOutputFile, "w") as csvDailyfile:
            writer = csv.DictWriter(csvDailyfile, fieldnames=fieldNames)
            writer.writeheader()
            writer.writerows(dailyErrorRows)

        self.logger.info("date totals:")
        self.logger.info(
            json.dumps(sorted(self.DateTotals.items(), key=lambda x: x[0]))
        )

        # dailyErrorPercentageRows is
        # [ {"error type/datetime": error, "2023-09-08": 12.01, "2023-09-09": 10.02},]
        self.logger.info(json.dumps(dailyErrorPercentageRows))
        with open(self.csvErrDatePercentageOutputFile, "w") as csvDailyPercentagefile:
            writer = csv.DictWriter(csvDailyPercentagefile, fieldnames=fieldNames)
            writer.writeheader()
            writer.writerows(dailyErrorPercentageRows)
