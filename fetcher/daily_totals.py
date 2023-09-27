from fetcher.csv_fetcher import CsvFetcher


class DailyTotal:
    def __init__(self, date: str, total: int):
        self.date = date
        self.total = total


class DailyTotalFetcher(CsvFetcher[DailyTotal]):
    def getLineData(self, line: dict[str, str]) -> DailyTotal:
        date = line.get("time", "")
        total = int(line.get("value", "0"))
        return DailyTotal(
            date,
            total
        )
