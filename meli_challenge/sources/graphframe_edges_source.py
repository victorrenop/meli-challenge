from meli_challenge.sources.source import Source
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from meli_challenge.utils.exceptions import InvalidSchema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


class EdgesSource(Source):
    def __init__(
        self,
        spark_session: SparkSession,
        input_df: DataFrame,
        source_column: str = "source",
        target_column: str = "target",
        weight_column: str = "weight",
    ):
        self._spark_session = spark_session
        self._input_df = input_df
        self._source_column = source_column
        self._target_column = target_column
        self._weight_column = weight_column

    def read(self, schema_file: str) -> DataFrame:
        self._validate_input_schema()
        result_df = self._spark_session.createDataFrame(
            self._spark_session.sparkContext.emptyRDD(),
            schema_file_to_schema_object(schema_file),
        )
        valid_rows = self._input_df.filter("%s > 0" % (self._weight_column))
        extra_columns = [
            column
            for column in valid_rows.columns
            if column != self._source_column and column != self._target_column
        ]
        result_df = result_df.union(
            valid_rows.select(
                col(self._source_column),
                col(self._target_column),
                *extra_columns,
            )
        )
        result_df = result_df.union(
            valid_rows.select(
                col(self._target_column),
                col(self._source_column),
                *extra_columns,
            )
        )

        return result_df.distinct()

    def _validate_input_schema(self) -> None:
        input_columns = set(self._input_df.columns)
        input_schema = self._input_df.schema
        desired_columns = {
            self._source_column,
            self._target_column,
            self._weight_column,
        }
        columns_diff = desired_columns - input_columns
        if len(columns_diff) > 0:
            raise InvalidSchema(
                "Input schema doesn't have the required columns!\n"
                + "The required columns are: %s\n" % (", ".join(desired_columns))
                + "Input schema: %s\n" % (input_schema)
            )

        desired_columns = {self._source_column, self._target_column}
        null_columns = [
            column_name
            for column_name in desired_columns
            if self._input_df.where(col(column_name).isNull()).count() > 0
        ]
        if len(null_columns) > 0:
            raise InvalidSchema(
                "Core columns have null values!\n"
                + "The core columns are: %s\n" % (", ".join(desired_columns))
                + "Core columns that have null values: %s\n" % (", ".join(null_columns))
            )
