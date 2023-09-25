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
    def getResult(self) -> str:
        """Get the full analysis result after all data are completely analyzed.

        Returns:
            str: Analysis result after all data are fetched and analyzed.
        """
        pass
