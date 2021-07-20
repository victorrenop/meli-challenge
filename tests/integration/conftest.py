from meli_challenge.sessions.custom_spark_session import CustomSparkSession
import pytest


@pytest.fixture
def custom_session():
    return CustomSparkSession(
        app_name="integration_test_session",
        configs=[("spark.jars", "assets/jars/graphframes-0.8.1-spark3.0-s_2.12.jar")],
        py_files=["assets/jars/graphframes-0.8.1-spark3.0-s_2.12.jar"],
        log_level="WARN",
    )
