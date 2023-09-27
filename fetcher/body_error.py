from fetcher.csv_fetcher import CsvFetcher


class BodyError:
    def __init__(self, date: str, attributeError: str, bodyMessage: str):
        self.date = date
        self.attributeError = attributeError
        self.bodyMessage = bodyMessage


class BodyErrorFetcher(CsvFetcher[BodyError]):
    def getLineData(self, line: dict[str, str]) -> BodyError:
        date = line.get("Date", "")
        attributeError = line.get("@Body.Attributes.metadata.error", "")
        bodyMessage = line.get("@Body.message", "")
        return BodyError(
            date,
            attributeError,
            bodyMessage,
        )
