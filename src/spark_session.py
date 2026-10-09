"""Creates the single local SparkSession used by every pipeline stage."""
import os
import sys

from pyspark.sql import SparkSession

from src import config


def get_spark(app_name: str = config.APP_NAME) -> SparkSession:
    # Make sure Spark workers use the same Python as this script
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
