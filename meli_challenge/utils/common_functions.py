from pyspark.sql.types import StructType
import json


def schema_file_to_schema_object(schema_file: str) -> StructType:
    with open(schema_file, "r") as file_to_read:
        return StructType.fromJson(json.load(file_to_read))
