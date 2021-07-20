from meli_challenge.sources import EdgesSource
from meli_challenge.utils.exceptions import InvalidSchema
from pyspark.sql import Row
import pytest


class TestEdgesSource:
    def test_edges_df_creation(self, spark_session):
        input_data = [
            {"id1": "1", "id2": "2", "weight": 1},
            {"id1": "1", "id2": "4", "weight": 1},
            {"id1": "2", "id2": "1", "weight": 1},
            {"id1": "3", "id2": "2", "weight": 1},
        ]
        input_df = spark_session.sparkContext.parallelize(input_data).toDF()
        edges_source = EdgesSource(spark_session, input_df, "id1", "id2")
        expected_result = [
            Row(src="1", dst="2", weight=1),
            Row(src="1", dst="4", weight=1),
            Row(src="2", dst="1", weight=1),
            Row(src="2", dst="3", weight=1),
            Row(src="3", dst="2", weight=1),
            Row(src="4", dst="1", weight=1),
        ]

        actual_result = edges_source.read(
            "tests/unit/sources/test_data/graph_data/edges_schema.json"
        ).collect()
        actual_result.sort(key=lambda x: (x["src"], x["dst"]))

        assert expected_result == actual_result

    def test_missing_core_columns(self, spark_session):
        input_data = [
            {"id1": "1", "weight": 1},
            {"id1": "2", "weight": 1},
        ]
        input_df = spark_session.sparkContext.parallelize(input_data).toDF()
        edges_source = EdgesSource(spark_session, input_df, "id1", "id2")
        with pytest.raises(InvalidSchema):
            _ = edges_source.read(
                "tests/unit/sources/test_data/graph_data/edges_schema.json"
            )

    def test_null_core_columns(self, spark_session):
        input_data = [
            {"id1": "1", "id2": 2, "weight": 1},
            {"id1": "2", "weight": 1},
        ]
        input_df = spark_session.sparkContext.parallelize(input_data).toDF()
        edges_source = EdgesSource(spark_session, input_df, "id1", "id2")
        with pytest.raises(InvalidSchema):
            _ = edges_source.read(
                "tests/unit/sources/test_data/graph_data/edges_schema.json"
            )

    def test_filter_out_zero_weight(self, spark_session):
        input_data = [
            {"id1": "1", "id2": "2", "weight": 1},
            {"id1": "1", "id2": "4", "weight": 0},
        ]
        input_df = spark_session.sparkContext.parallelize(input_data).toDF()
        edges_source = EdgesSource(spark_session, input_df, "id1", "id2")
        expected_result = [
            Row(src="1", dst="2", weight=1),
            Row(src="2", dst="1", weight=1),
        ]

        actual_result = edges_source.read(
            "tests/unit/sources/test_data/graph_data/edges_schema.json"
        ).collect()

        assert expected_result == actual_result
