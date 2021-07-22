from meli_challenge.sources.source import Source
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing import List


class ObjectSource(Source):
    """Reads a list or list of dicts to a dataframe.

    Args:
        spark_session (SparkSession): Spark session provided by the user.
        data_object (List[dict]): List containing the data.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        data_object: List[dict],
    ):
        self._spark_session = spark_session
        self._data_object = data_object

    def read(self, schema_file: str) -> DataFrame:
        """Reads a list into a dataframe.

        Args:
            schema_file (str): File containing the schema to be applied into the list.

        Returns:
            DataFrame: Dataframe with the list data and the desired schema.
        """

        return self._spark_session.createDataFrame(
            self._spark_session.sparkContext.parallelize(self._data_object),
            schema_file_to_schema_object(schema_file),
        )
