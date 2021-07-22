from meli_challenge.sources.source import Source
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from meli_challenge.utils.exceptions import InvalidSchema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


class EdgesSource(Source):
    """Builds a graph edges dataframe using the input dataframe.

    The edge dataframe is built using some rules to comply with the GraphFrames package
    requirements and other additional requirements.

    The output dataframe has always 3 core columns: "src" and "dst" that defines a bi-
    directional edge betwen both columns and a "weight" column that defines the weight
    of this edge. Rows from the input dataframe with weight less than zero are
    discarded, following the challenge rules that an edge is represented only by weights
    greater than zero.

    The output will always have reversal duplicates because Graph Frames only has
    directed graphs but the challenge graph is intrinsically an undirected graph and to
    behave like that, edges on both directions of a relationship are added. For example,
    if the input data has the following row "a, b, 1, 1", the output df will have the
    rows "a, b, 1, 1" and "b, a, 1, 1".

    Args:
        spark_session (SparkSession): Spark session provided by the user.
        input_df (DataFrame): Dataframe containing the original data. Must have a source
            and target columns describing a relationship between vertices. This
            relationship is interpreted as being bi-directional.
        source_column (str, optional): Name of the source vertex column. Defaults to
            "source".
        target_column (str, optional): Name of the target edge column. Defaults to
            "target".
        weight_column (str, optional): Name of the edge weight column. Defaults to
            "weight".
    """

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
        """Reads the input dataframe into an edges dataframe.

        Args:
            schema_file (str): File containing the schema of the edges output dataframe.

        Raises:
            InvalidSchema: If the input dataframe doesn't have the source, target or
                weight columns or if the source and target columns have null values.

        Returns:
            Dataframe: Dataframe with the edges represented by the columns "src", "dst"
                and "weight" (as the GraphFrame class needs). The other columns of the
                input dataframe are also present in this dataframe, used as edge data.
        """

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
        """Checks if the input dataframe schema is valid"""

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
