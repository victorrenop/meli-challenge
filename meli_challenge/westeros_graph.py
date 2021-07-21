from meli_challenge.sessions import CustomSparkSession
from meli_challenge.sources.source import Source
from meli_challenge.sources import EdgesSource, VerticesSource
from meli_challenge.utils.common_functions import schema_file_to_schema_object
from meli_challenge.utils.constants import (
    INPUT_SCHEMA,
    EDGES_SCHEMA,
    VERTICES_SCHEMA,
    BOOKS,
)
from pyspark.sql.functions import col, coalesce, lit, sum, collect_set, when, lower
from pyspark.sql import DataFrame
from functools import reduce
from operator import add


class WesterosGraph:
    def __init__(self, session: CustomSparkSession, source_reader: Source = None):
        self._session = session

        input_df = self._session.spark_session.createDataFrame(
            self._session.spark_context.emptyRDD(),
            schema_file_to_schema_object(INPUT_SCHEMA),
        )
        if source_reader:
            input_df = source_reader.read(INPUT_SCHEMA)
        self._graph = None
        self.build_graph(input_df)

    def build_graph(self, input_df: DataFrame, source_target: tuple = None) -> object:
        # This import is needed because the graphframes package is only initialized
        # after the spark session has been created
        from graphframes import GraphFrame  # noqa

        if not source_target:
            source_target = ("character_1", "character_2")
        vertices_builder = VerticesSource(
            self._session.spark_session, input_df, source_target
        )
        edges_builder = EdgesSource(
            self._session.spark_session, input_df, *source_target
        )
        new_vertices = vertices_builder.read(VERTICES_SCHEMA)
        new_edges = edges_builder.read(EDGES_SCHEMA)
        if self._graph:
            new_vertices = new_vertices.union(self._graph.vertices).distinct()
            new_edges = new_edges.union(self._graph.edges).distinct()
        self._graph = GraphFrame(new_vertices, new_edges)

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
        mutual_friends = self._mutual_friends_motif()
        return (
            mutual_friends.filter(
                (lower(col("character_1")) == character_1.lower())
                & (lower(col("character_2")) == character_2.lower())
            )
            .groupBy("character_1", "character_2")
            .agg(collect_set("mutual_friend").alias("mutual_friends"))
        )

    def get_all_characters_mutual_friends(self) -> DataFrame:
        mutual_friends = self._mutual_friends_motif()
        return (
            mutual_friends.filter(col("character_1") != col("character_2"))
            .groupBy("character_1", "character_2")
            .agg(collect_set("mutual_friend").alias("mutual_friends"))
            .select(
                when(col("character_1") > col("character_2"), col("character_2"))
                .otherwise(col("character_1"))
                .alias("character_1"),
                when(col("character_1") < col("character_2"), col("character_2"))
                .otherwise(col("character_2"))
                .alias("character_2"),
                "mutual_friends",
            )
            .filter(col("character_1") != col("character_2"))
        ).distinct()

    def _mutual_friends_motif(self) -> DataFrame:
        return self._graph.find("(a)-[]->(b); (a)-[]->(c)").select(
            col("a.id").alias("mutual_friend"),
            col("b.id").alias("character_1"),
            col("c.id").alias("character_2"),
        )
