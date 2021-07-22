from pyspark.sql.types import StructType
import json


def schema_file_to_schema_object(schema_file: str) -> StructType:
    with open(schema_file, "r") as file_to_read:
        return StructType.fromJson(json.load(file_to_read))


def check_json_object_schema(json_object: dict, schema_file: str) -> bool:
    with open(schema_file, "r") as f:
        schema = json.load(f)
        for key, key_type in schema.items():
            if (
                key not in json_object
                or str(key_type) != type(json_object[key]).__name__  # noqa E721
            ):
                return False
    return True
