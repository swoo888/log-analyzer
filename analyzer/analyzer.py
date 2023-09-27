import abc
from typing import Any


class Analyzer:
    @abc.abstractclassmethod
    async def analyze(self, data: Any) -> None:
        """Analyze data fetched.

        Args:
            data (any): Data retrieved in each fetch.  This function is called for each data fetched.
        """
        pass

    @abc.abstractclassmethod
    def dumpResult(self) -> None:
        """Dump the full analysis result after all data are completely analyzed.
        """
        pass
