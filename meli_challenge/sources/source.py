from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame


class Source(metaclass=ABCMeta):
    """Base class to read input data and return a dataframe."""

    @abstractmethod
    def read(self, schema_file: str) -> DataFrame:
        pass
