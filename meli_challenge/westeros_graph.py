from meli_challenge.sessions import CustomSparkSession
from meli_challenge.sources.source import Source
from meli_challenge.sources import EdgesSource, VerticesSource
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from pyspark.sql.functions import col, coalesce, lit, sum, collect_set
from pyspark.sql import DataFrame
from functools import reduce
from operator import add

INPUT_SCHEMA = "assets/schemas/input_schema.json"
EDGES_SCHEMA = "assets/schemas/edges_schema.json"
VERTICES_SCHEMA = "assets/schemas/vertices_schema.json"
BOOKS = ["1", "2", "3"]


class WesterosGraph:
    def __init__(self, session: CustomSparkSession, source_reader: Source = None):
        # This import is needed because the graphframes package is only initialized
        # after the spark session has been created
        from graphframes import GraphFrame  # noqa

        self._session = session

        input_df = self._session.spark_session.createDataFrame(
            self._session.spark_context.emptyRDD(),
            schema_file_to_schema_object(INPUT_SCHEMA),
        )
        if source_reader:
            input_df = source_reader.read(INPUT_SCHEMA)
        vertices_builder = VerticesSource(
            self._session.spark_session, input_df, ["character_1", "character_2"]
        )
        edges_builder = EdgesSource(
            self._session.spark_session, input_df, "character_1", "character_2"
        )
        self._graph = GraphFrame(
            vertices_builder.read(VERTICES_SCHEMA),
            edges_builder.read(EDGES_SCHEMA),
        )

    def get_aggregated_interactions(self) -> DataFrame:
        agg_results = (
            self._graph.edges.select("src", "weight", "book")
            .groupBy("src", "book")
            .agg(sum("weight").alias("weight_sum"))
        )
        pivoted_result = (
            agg_results.groupBy("src")
            .pivot("book", BOOKS)
            .agg(sum("weight_sum").alias("book"))
            .na.fill(0)
        )
        total_sum_result = pivoted_result.withColumn(
            "total", reduce(add, [coalesce(col(name), lit(0)) for name in BOOKS])
        )
        renamed_df = reduce(
            lambda df, col_name: df.withColumnRenamed(col_name, "book_" + col_name),
            BOOKS,
            total_sum_result,
        )
        renamed_df = renamed_df.withColumnRenamed("src", "character")
        result_df = renamed_df.orderBy(col("total").desc())

        return result_df

    def get_mutual_friends(self, character_1: str, character_2: str) -> DataFrame:
        mutual_friends = self._graph.find("(a)-[]->(b); (a)-[]->(c)")
        mutual_friends = mutual_friends.select(
            col("a.id").alias("mutual_friend"),
            col("b.id").alias("character_1"),
            col("c.id").alias("character_2"),
        )
        mutual_friends = (
            mutual_friends.filter(
                (col("character_1") == character_1)
                & (col("character_2") == character_2)
            )
            .groupBy("character_1", "character_2")
            .agg(collect_set("mutual_friend").alias("mutual_friends"))
        )

        return mutual_friends
