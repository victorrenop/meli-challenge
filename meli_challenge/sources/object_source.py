from pyspark.sql.dataframe import DataFrame
from meli_challenge.sources.source import Source
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from pyspark.sql import SparkSession
from typing import List


class ObjectSource(Source):
    def __init__(
        self,
        spark_session: SparkSession,
        data_object: List[dict],
    ):
        self._spark_session = spark_session
        self._data_object = data_object

    def read(self, schema_file: str) -> DataFrame:
        return self._spark_session.createDataFrame(
            self._spark_session.sparkContext.parallelize(self._data_object),
            schema_file_to_schema_object(schema_file),
        )
