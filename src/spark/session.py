from pyspark.sql import SparkSession
import os

# UseContainerSupport exists only on Linux JVMs; IgnoreUnrecognizedVMOptions
# keeps the JVM from refusing to start on macOS/Windows.
_JVM_OPTIONS = (
    "-XX:+IgnoreUnrecognizedVMOptions -Xmx8g -XX:+UseG1GC "
    "-XX:-UseContainerSupport"
)

if "JAVA_TOOL_OPTIONS" not in os.environ:
    os.environ["JAVA_TOOL_OPTIONS"] = _JVM_OPTIONS
else:
    os.environ["JAVA_TOOL_OPTIONS"] += " " + _JVM_OPTIONS


def create_spark_session(app_name: str = "yelp-spark-project") -> SparkSession:
    """
    Creates and configures a new SparkSession.

    Defaults are conservative for single-machine / laptop use (4 local threads,
    8g driver heap). Tune with SPARK_MAX_CORES, SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY, SPARK_SQL_SHUFFLE_PARTITIONS before calling.
    Config changes apply only to a new SparkContext. If you already have a
    session, call ``spark.stop()`` then create_spark_session again (kernel restart
    not required). Exception: some JVM options may still need a fresh Python
    process in odd setups.

    GPU: Vanilla Spark runs DataFrame work on the CPU. NVIDIA GPU acceleration is
    usually added separately via the RAPIDS Accelerator for Apache Spark (CUDA,
    plugin JARs, spark.rapids.* configs). The limits above only cap local threads
    and heap for laptop use; they do not prevent you from raising them and
    attaching RAPIDS on a GPU host when you wire that up.

    Args:
        app_name: The name to assign to the Spark application. Defaults to
        'yelp-spark-project'.

    Returns:
        SparkSession: An initialized SparkSession with the specified
        configurations.
    """

    cores = os.environ.get("SPARK_MAX_CORES", "4")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "8g")
    executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY", "8g")
    shuffle_partitions = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "200")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        # Give them enough heap space.
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        # Increase the fraction of heap used for execution/storage (default 0.6).
        .config("spark.memory.fraction", "0.8")
        # Of that fraction, reserve less for cached data so execution tasks have
        # more room to work (default 0.5 — halved here to reduce spill pressure).
        .config("spark.memory.storageFraction", "0.3")
        # Limit the max size of a single collect result to avoid driver OOM.
        .config("spark.driver.maxResultSize", "2g")
        # More shuffle partitions spread the data more finely, keeping each
        # partition smaller and less likely to exhaust per-task memory.
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        # Avoid broadcasting large tables; let Spark sort-merge join instead.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )

    return spark
