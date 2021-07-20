from meli_challenge.sources import ObjectSource
from datetime import datetime


class TestObjectSource:
    def test_read_data_as_list(self, spark_session, expected_result):
        input_object = [
            {"id": 1, "data": "abc", "date": datetime(2021, 1, 1).date()},
            {"id": 2, "data": "def", "date": datetime(2021, 1, 2).date()},
        ]
        obj_source = ObjectSource(spark_session, input_object)

        actual_result = obj_source.read(
            "tests/unit/sources/test_data/csv_data/base_schema.json"
        ).collect()
        actual_result.sort(key=lambda x: x["id"])

        assert expected_result == actual_result
