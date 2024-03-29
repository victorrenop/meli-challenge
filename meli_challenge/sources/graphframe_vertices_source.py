from meli_challenge.sources.source import Source
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from meli_challenge.utils.exceptions import InvalidSchema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import List


class VerticesSource(Source):
    """Builds a graph vertices dataframe using the input dataframe.

    The vertices dataframe is built by getting all unique values of both source and
    and target columns of the input dataframe. The output dataframe only has 1 column
    for all the unique vertices called "id". This column is needed by the Graph Frames
    package to correctly create a graph.

    Args:
        spark_session (SparkSession): Spark session provided by the user
        input_df (DataFrame): Dataframe containing the original data. Must have a source
            and target columns describing a relationship between vertices.
        id_columns (List[str], optional): List containing the source and target columns.
            Defaults to None.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        input_df: DataFrame,
        id_columns: List[str] = None,
    ):
        self._spark_session = spark_session
        self._input_df = input_df
        self._id_columns = id_columns if id_columns else ["id"]

    def read(self, schema_file: str) -> DataFrame:
        """Reads the input dataframe into a vertices dataframe.

        Args:
            schema_file (str): File containing the schema of the output vertices
                dataframe.

        Raises:
            InvalidSchema: If the input doesn't have the source and target columns or if
                source and target columns have null values.

        Returns:
            DataFrame: Dataframe with only one column called "id" that contains all
                unique vertices.
        """

        self._validate_input_schema()
        result_df = self._spark_session.createDataFrame(
            self._spark_session.sparkContext.emptyRDD(),
            schema_file_to_schema_object(schema_file),
        )
        for id_column in self._id_columns:
            result_df = result_df.union(self._input_df.select(id_column))

        return result_df.distinct()

    def _validate_input_schema(self) -> None:
        """Validates if the input dataframe has a valid schema"""

        input_columns = set(self._input_df.columns)
        input_schema = self._input_df.schema
        desired_columns = set(self._id_columns)
        columns_diff = desired_columns - input_columns
        if len(columns_diff) > 0:
            raise InvalidSchema(
                "Input schema doesn't have the required id columns!\n"
                + "The required columns are: %s\n" % (", ".join(desired_columns))
                + "Input schema: %s\n" % (input_schema)
            )

        null_columns = [
            column_name
            for column_name in desired_columns
            if self._input_df.where(col(column_name).isNull()).count() > 0
        ]
        if len(null_columns) > 0:
            raise InvalidSchema(
                "Id columns have null values!\n"
                + "The id columns are: %s\n" % (", ".join(desired_columns))
                + "Core columns that have null values: %s\n" % (", ".join(null_columns))
            )
