from pyspark.sql.types import StructType
import json


def schema_file_to_schema_object(schema_file: str) -> StructType:
    with open(schema_file, "r") as file_to_read:
        return StructType.fromJson(json.load(file_to_read))


def check_json_object_schema(json_object: dict, schema_file: str) -> bool:
    with open(schema_file, "r") as f:
        schema = json.load(f)
        for key, val in json_object.items():
            if key not in schema or type(val).__name__ != schema[key]:
                return False
    return True
