from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from src.constants import PROJECT_ROOT

# On some Linux + cgroup v2 setups, JDK 17 can NPE inside CgroupV2Subsystem when
# Spark initializes executor metrics (OperatingSystemMXBean → container metrics).
# Disabling JVM container support avoids that path for local development.
_SPARK_EXTRA_JAVA_OPTS = "-XX:-UseContainerSupport"


def _env_positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _env_memory(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _spark_local_dir_from_env() -> str | None:
    """
    Optional override for shuffle/spill scratch (comma-separated list allowed).
    If unset, create_spark_session uses a project-local dir (see _resolve_spark_local_dir).
    """
    for key in ("SPARK_LOCAL_DIRS", "SPARK_LOCAL_DIR"):
        raw = os.environ.get(key, "").strip()
        if raw:
            return raw
    return None


def _default_spark_local_scratch() -> Path:
    """Under project artifacts/ (gitignored) — avoids /tmp tmpfs and small quotas."""
    return PROJECT_ROOT / "artifacts" / "spark_local_scratch"


def _resolve_spark_local_dir() -> str:
    env = _spark_local_dir_from_env()
    if env:
        _ensure_spark_local_dirs(env)
        return env
    p = _default_spark_local_scratch()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def _ensure_spark_local_dirs(local_dir: str) -> None:
    for segment in local_dir.split(","):
        s = segment.strip()
        if not s:
            continue
        Path(s).expanduser().mkdir(parents=True, exist_ok=True)


def _spark_resource_settings() -> tuple[int, str, str, int]:
    """
    Defaults favor laptops: few threads and modest heap to reduce CPU/memory
    pressure and swap thrashing (which freezes the desktop).

    Override before create_spark_session:
      SPARK_MAX_CORES — local[N] thread count (default 2)
      SPARK_DRIVER_MEMORY — driver heap, e.g. 1g, 768m (default 2g)
      SPARK_EXECUTOR_MEMORY — same in local mode; usually match driver (default 1g)
      SPARK_SQL_SHUFFLE_PARTITIONS — shuffle partitions (default max(4, cores*2), cap 16)
      SPARK_LOCAL_DIRS / SPARK_LOCAL_DIR — override scratch for shuffle/spill (else artifacts/spark_local_scratch)
      SPARK_DRIVER_BIND_ADDRESS / SPARK_DRIVER_HOST — for local[*] only; default 127.0.0.1
        (set bind/host to empty to skip binding — useful when not using loopback)
    """
    cores = _env_positive_int("SPARK_MAX_CORES", 2)
    driver_mem = _env_memory("SPARK_DRIVER_MEMORY", "2g")
    executor_mem = _env_memory("SPARK_EXECUTOR_MEMORY", driver_mem)
    shuffle_raw = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "").strip()
    if shuffle_raw:
        try:
            shuffle = max(2, int(shuffle_raw))
        except ValueError:
            shuffle = max(4, min(16, cores * 2))
    else:
        shuffle = max(4, min(16, cores * 2))
    return cores, driver_mem, executor_mem, shuffle


def _jdk_home_has_java(home: str) -> bool:
    return bool(home) and (Path(home) / "bin" / "java").is_file()


def _ensure_java_home_for_spark() -> None:
    """
    Hadoop (local FS) uses javax.security.auth.Subject; on JDK 24+ getSubject()
    throws UnsupportedOperationException, breaking Spark. PySpark starts the JVM
    using JAVA_HOME — not YELP_JAVA17_HOME — so we must set JAVA_HOME to a
    supported JDK (11–21; this project standardizes on 17).
    """
    yelp = os.environ.get("YELP_JAVA17_HOME", "").strip()
    if _jdk_home_has_java(yelp):
        os.environ["JAVA_HOME"] = yelp
        return
    try:
        r = subprocess.run(
            ["mise", "where", "java@17"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        path = (r.stdout or "").strip()
        if _jdk_home_has_java(path):
            os.environ["JAVA_HOME"] = path
            os.environ.setdefault("YELP_JAVA17_HOME", path)
    except (OSError, subprocess.SubprocessError):
        pass


def create_spark_session(app_name: str = "yelp-spark-project") -> SparkSession:
    """
    Creates and configures a new SparkSession.

    Defaults are conservative for single-machine / laptop use (2 local threads,
    2g driver heap). Tune with SPARK_MAX_CORES, SPARK_DRIVER_MEMORY,
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
    _ensure_java_home_for_spark()
    # Import after JAVA_HOME is set — PySpark may read the env at import time.
    from pyspark.sql import SparkSession

    cores, driver_mem, executor_mem, shuffle_parts = _spark_resource_settings()
    master = f"local[{cores}]"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_mem)
        .config("spark.executor.memory", executor_mem)
        .config("spark.sql.shuffle.partitions", str(shuffle_parts))
        .config("spark.default.parallelism", str(cores))
        .config("spark.sql.files.maxPartitionBytes", "67108864")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.extraJavaOptions", _SPARK_EXTRA_JAVA_OPTS)
        .config("spark.executor.extraJavaOptions", _SPARK_EXTRA_JAVA_OPTS)
        .config("spark.local.dir", _resolve_spark_local_dir())
    )

    # Local mode: bind driver to loopback so block/shuffle traffic does not use the
    # machine hostname (e.g. Docker/LAN mismatches → TaskResultLost, idle timeouts).
    # Override with SPARK_DRIVER_BIND_ADDRESS / SPARK_DRIVER_HOST (empty = skip).
    if master.startswith("local"):
        bind = os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1").strip()
        host = os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1").strip()
        if bind:
            builder = builder.config("spark.driver.bindAddress", bind)
        if host:
            builder = builder.config("spark.driver.host", host)

    spark = builder.getOrCreate()

    return spark