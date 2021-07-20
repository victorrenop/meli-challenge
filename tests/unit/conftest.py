from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark_session():
    spark_session = SparkSession.builder.appName("testing_session").getOrCreate()

    return spark_session
