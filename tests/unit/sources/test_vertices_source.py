from meli_challenge.sources import VerticesSource
from meli_challenge.utils.exceptions import InvalidSchema
from pyspark.sql import Row
import pytest


class TestVerticesSource:
    @pytest.mark.parametrize(
        "id_columns,expected_result",
        [
            (["id1"], [Row(id="1"), Row(id="2"), Row(id="3")]),
            (["id1", "id2"], [Row(id="1"), Row(id="2"), Row(id="3"), Row(id="4")]),
        ],
    )
    def test_vertices_df_creation(self, spark_session, id_columns, expected_result):
        input_data = [
            {"id1": "1", "id2": "2", "weight": 1},
            {"id1": "1", "id2": "4", "weight": 1},
            {"id1": "2", "id2": "1", "weight": 1},
            {"id1": "3", "id2": "2", "weight": 1},
        ]
        input_df = spark_session.sparkContext.parallelize(input_data).toDF()
        vertices_source = VerticesSource(spark_session, input_df, id_columns)

        actual_result = vertices_source.read(
            "tests/unit/sources/test_data/graph_data/vertices_schema.json"
        ).collect()
        actual_result.sort(key=lambda x: x["id"])

        assert expected_result == actual_result

    def test_missing_id_columns(self, spark_session):
        input_data = [
            {"mock": "1"},
        ]
        input_df = spark_session.sparkContext.parallelize(input_data).toDF()
        vertices_source = VerticesSource(spark_session, input_df, ["id1"])
        with pytest.raises(InvalidSchema):
            _ = vertices_source.read(
                "tests/unit/sources/test_data/graph_data/vertices_schema.json"
            )

    def test_null_id_columns(self, spark_session):
        input_data = [
            {"id1": "1", "id2": 2, "weight": 1},
            {"id1": "2", "weight": 1},
        ]
        input_df = spark_session.sparkContext.parallelize(input_data).toDF()
        vertices_source = VerticesSource(spark_session, input_df, ["id1", "id2"])
        with pytest.raises(InvalidSchema):
            _ = vertices_source.read(
                "tests/unit/sources/test_data/graph_data/vertices_schema.json"
            )
