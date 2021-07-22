from pyspark.sql.types import StructType
import json


def schema_file_to_schema_object(schema_file: str) -> StructType:
    """Reads a schema from a json file into a StructType object.

    Args:
        schema_file (str): Path to the schema file.

    Returns:
        StructType: Pyspark object with the read schema.
    """

    with open(schema_file, "r") as file_to_read:
        return StructType.fromJson(json.load(file_to_read))


def check_json_object_schema(json_object: dict, schema_file: str) -> bool:
    """Checks if the input dictionary has the required schema.

    The schema is read from the provided schema file that doesn't follow the json schema
    pattern, but rather is more of a type mapping where a key is the expected column
    name and the key's value is the expected type.

    If there are missing keys or type mismatch, the "json_object"'s schema is deemed
    invalid.

    Args:
        json_object (dict): Dictionary that needs to be schema checked.
        schema_file (str): Path to file that contains the schema mapping.

    Returns:
        bool: True if the schema is valid, False if not.
    """

    with open(schema_file, "r") as f:
        schema = json.load(f)
        for key, key_type in schema.items():
            if (
                key not in json_object
                or str(key_type) != type(json_object[key]).__name__  # noqa E721
            ):
                return False
    return True
