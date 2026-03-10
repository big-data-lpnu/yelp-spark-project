from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "yelp-spark-project") -> SparkSession:
    """
    Creates and configures a new SparkSession.

    Args:
        app_name: The name to assign to the Spark application. Defaults to
        'yelp-spark-project'.

    Returns:
        SparkSession: An initialized SparkSession with the specified
        configurations.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    return spark