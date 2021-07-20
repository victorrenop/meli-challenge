from pyspark.sql.dataframe import DataFrame
from meli_challenge.sources.source import Source
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from pyspark.sql import SparkSession
from typing import List, Union


class CSVSource(Source):
    def __init__(
        self,
        spark_session: SparkSession,
        source_files: Union[List[str], str],
        base_path: str = None,
        read_options: dict = None,
    ):
        self._spark_session = spark_session
        self._source_files = source_files
        self._base_path = base_path
        self._read_options = read_options if read_options else {}

    def read(self, schema_file: str) -> DataFrame:
        csv_data_reader = self._spark_session.read
        if self._base_path:
            csv_data_reader = csv_data_reader.option("basePath", self._base_path)
        csv_data_reader = csv_data_reader.options(**self._read_options)
        csv_data_reader = csv_data_reader.schema(
            schema_file_to_schema_object(schema_file)
        )

        return csv_data_reader.csv(self._source_files)
