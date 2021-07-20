from meli_challenge.sessions import CustomSparkSession
from pyspark.sql import SparkSession
from pyspark.context import SparkContext


class TestCustomSparkSession:
    def test_already_initialized_session(self, spark_session):
        custom_session = CustomSparkSession(spark_session=spark_session)

        assert custom_session.spark_session == spark_session

    def test_correct_spark_session_creation(self):
        custom_session = CustomSparkSession()

        assert isinstance(custom_session.spark_session, SparkSession)

    def test_correct_spark_context_creation(self):
        custom_session = CustomSparkSession()

        assert isinstance(custom_session.spark_context, SparkContext)
