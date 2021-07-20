from pyspark import SparkConf
from pyspark.sql import SparkSession
from typing import List, Tuple


class CustomSparkSession:
    """Custom session that initializes the Spark Session and Spark Context.

    Initializes the Spark Session using the defined configurations passed by the user
    or simply utilizes an already initialized Spark Session passed by the user.

    Also creates a class attribute to access the Spark Context object.
    """

    def __init__(
        self,
        app_name: str = "meli_challenge",
        configs: List[Tuple] = None,
        py_files: List[str] = None,
        log_level: str = "ERROR",
        spark_session: SparkSession = None,
    ):
        self.spark_session = spark_session
        configs = configs if configs else []
        py_files = py_files if py_files else []
        if not spark_session:
            spark_conf = SparkConf()
            spark_conf.setAll(configs if configs else [])
            self.spark_session = (
                SparkSession.builder.appName(app_name)
                .config(conf=spark_conf)
                .getOrCreate()
            )
            for file in py_files:
                self.spark_session.sparkContext.addPyFile(file)
            self.spark_session.sparkContext.setLogLevel(log_level)
        self.spark_context = self.spark_session.sparkContext
