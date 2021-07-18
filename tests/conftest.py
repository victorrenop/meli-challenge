from pyspark.sql import SparkSession
from typing import List
import pytest


@pytest.fixture
def spark_session(app_name: str = "testing_session", jars: List[str] = None):
    jars = jars if jars else []
    spark_session = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", ",".join(jars))
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("INFO")

    return spark_session
