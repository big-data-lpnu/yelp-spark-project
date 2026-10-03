import os

import pytest
from pyspark.sql import SparkSession

# The Linux-only -XX:-UseContainerSupport in src.spark.session would stop the
# JVM elsewhere; tests use their own tiny session instead.
os.environ.setdefault("JAVA_TOOL_OPTIONS", "-XX:+IgnoreUnrecognizedVMOptions")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("yelp-ml-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # Same semantics as production (Spark 4 default): bad casts throw.
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
