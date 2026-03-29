"""Orchestrate preprocessing: clean -> flatten -> transform -> reduce per table."""

import os
import threading
import time
import warnings
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from src.preprocessing.clean import clean
from src.preprocessing.config import PreprocessConfig, default_config
from src.preprocessing.flatten import flatten_table
from src.preprocessing.reduce import reduce_df
from src.preprocessing.transform import transform

# Heartbeat interval (seconds) for progress during long steps
_HEARTBEAT_INTERVAL = 60
_current_step: str | None = None
_heartbeat_stop = threading.Event()


def _log(msg: str) -> None:
    """Print a timestamped progress line (flushed so it appears in notebooks)."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _heartbeat_worker() -> None:
    """Background thread: print current step every _HEARTBEAT_INTERVAL until stopped."""
    while not _heartbeat_stop.wait(timeout=_HEARTBEAT_INTERVAL):
        if _current_step:
            _log(f"  ... still running: {_current_step}")


def _run_with_heartbeat(step_name: str, fn, *args, **kwargs):
    """Run fn(*args, **kwargs) while a background thread logs progress every minute."""
    global _current_step
    _current_step = step_name
    _heartbeat_stop.clear()
    t = threading.Thread(target=_heartbeat_worker, daemon=True)
    t.start()
    try:
        return fn(*args, **kwargs)
    finally:
        _heartbeat_stop.set()
        _current_step = None


def preprocess_business(
    spark: SparkSession,
    raw_df: DataFrame,
    config: PreprocessConfig | None = None,
) -> DataFrame:
    """Full preprocessing for business table."""
    cfg = config or default_config()["business"]
    df = clean(raw_df, "business", cfg)
    df = flatten_table(df, "business")
    df = transform(df, cfg, "business")
    return reduce_df(df, cfg)


def preprocess_review(
    spark: SparkSession,
    raw_df: DataFrame,
    config: PreprocessConfig | None = None,
) -> DataFrame:
    """Full preprocessing for review table."""
    cfg = config or default_config()["review"]
    df = clean(raw_df, "review", cfg)
    df = flatten_table(df, "review")
    df = transform(df, cfg, "review")
    return reduce_df(df, cfg)


def preprocess_user(
    spark: SparkSession,
    raw_df: DataFrame,
    config: PreprocessConfig | None = None,
) -> DataFrame:
    """Full preprocessing for user table."""
    cfg = config or default_config()["user"]
    df = clean(raw_df, "user", cfg)
    df = flatten_table(df, "user")
    df = transform(df, cfg, "user")
    return reduce_df(df, cfg)


def preprocess_checkin(
    spark: SparkSession,
    raw_df: DataFrame,
    config: PreprocessConfig | None = None,
) -> DataFrame:
    """Full preprocessing for checkin table."""
    cfg = config or default_config()["checkin"]
    df = clean(raw_df, "checkin", cfg)
    df = flatten_table(df, "checkin")
    df = transform(df, cfg, "checkin")
    return reduce_df(df, cfg)


def preprocess_tip(
    spark: SparkSession,
    raw_df: DataFrame,
    config: PreprocessConfig | None = None,
) -> DataFrame:
    """Full preprocessing for tip table."""
    cfg = config or default_config()["tip"]
    df = clean(raw_df, "tip", cfg)
    df = flatten_table(df, "tip")
    df = transform(df, cfg, "tip")
    return reduce_df(df, cfg)


def preprocess_photo(
    spark: SparkSession,
    raw_df: DataFrame,
    config: PreprocessConfig | None = None,
) -> DataFrame:
    """Full preprocessing for photo table."""
    cfg = config or default_config()["photo"]
    df = clean(raw_df, "photo", cfg)
    df = flatten_table(df, "photo")
    df = transform(df, cfg, "photo")
    return reduce_df(df, cfg)


PREPROCESSORS = {
    "business": preprocess_business,
    "review": preprocess_review,
    "user": preprocess_user,
    "checkin": preprocess_checkin,
    "tip": preprocess_tip,
    "photo": preprocess_photo,
}


def _available_dataset_names():
    """Names of datasets whose files exist."""
    from src.spark.load_data import DATASETS

    out = []
    for name in PREPROCESSORS:
        _, path = DATASETS[name]
        if path.exists():
            out.append(name)
        else:
            warnings.warn(
                f"Dataset '{name}' skipped: file not found at {path}.",
                UserWarning,
                stacklevel=2,
            )
    return out


def load_all_raw(spark: SparkSession) -> dict[str, DataFrame]:
    """
    Stage 1: Load raw DataFrames for all available datasets.
    Logs progress per dataset. Skip datasets whose file is missing.
    """
    from src.spark.load_data import DATASETS, load_dataset

    names = _available_dataset_names()
    _log(f"Stage 1: Loading raw data ({len(names)} datasets)")
    result = {}
    for i, name in enumerate(names, 1):
        t0 = time.perf_counter()
        _log(f"  [{i}/{len(names)}] {name}: loading...")
        raw = load_dataset(spark, name)
        n = raw.count()
        elapsed = time.perf_counter() - t0
        _log(f"  [{i}/{len(names)}] {name}: done ({n} rows, {elapsed:.1f}s)")
        result[name] = raw
    _log("Stage 1: finished.")
    return result


def clean_all(
    spark: SparkSession,
    raw: dict[str, DataFrame],
    config_per_table: dict[str, PreprocessConfig] | None = None,
) -> dict[str, DataFrame]:
    """Stage 2: Clean (nulls, duplicates) for each dataset. Logs progress."""
    configs = config_per_table or default_config()
    names = list(raw)
    _log(f"Stage 2: Cleaning ({len(names)} datasets)")
    result = {}
    for i, name in enumerate(names, 1):
        t0 = time.perf_counter()
        _log(f"  [{i}/{len(names)}] {name}: cleaning...")

        def _clean():
            cfg = configs.get(name) or default_config().get(name) or PreprocessConfig()
            return clean(raw[name], name, cfg)

        df = _run_with_heartbeat(f"clean {name}", _clean)
        n = df.count()
        elapsed = time.perf_counter() - t0
        _log(f"  [{i}/{len(names)}] {name}: done ({n} rows, {elapsed:.1f}s)")
        result[name] = df
    _log("Stage 2: finished.")
    return result


def flatten_all(
    cleaned: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """Stage 3: Flatten nested columns for each dataset. Logs progress."""
    names = list(cleaned)
    _log(f"Stage 3: Flattening ({len(names)} datasets)")
    result = {}
    for i, name in enumerate(names, 1):
        t0 = time.perf_counter()
        _log(f"  [{i}/{len(names)}] {name}: flattening...")
        df = flatten_table(cleaned[name], name)
        n = df.count()
        elapsed = time.perf_counter() - t0
        _log(f"  [{i}/{len(names)}] {name}: done ({n} rows, {elapsed:.1f}s)")
        result[name] = df
    _log("Stage 3: finished.")
    return result


def transform_all(
    spark: SparkSession,
    flattened: dict[str, DataFrame],
    config_per_table: dict[str, PreprocessConfig] | None = None,
) -> dict[str, DataFrame]:
    """Stage 4: Transform (scale, parse dates, drop cols) for each dataset. Logs progress."""
    configs = config_per_table or default_config()
    names = list(flattened)
    _log(f"Stage 4: Transforming ({len(names)} datasets)")
    result = {}
    for i, name in enumerate(names, 1):
        t0 = time.perf_counter()
        _log(f"  [{i}/{len(names)}] {name}: transforming...")

        def _transform():
            cfg = configs.get(name) or default_config().get(name) or PreprocessConfig()
            return transform(flattened[name], cfg, name)

        df = _run_with_heartbeat(f"transform {name}", _transform)
        n = df.count()
        elapsed = time.perf_counter() - t0
        _log(f"  [{i}/{len(names)}] {name}: done ({n} rows, {elapsed:.1f}s)")
        result[name] = df
    _log("Stage 4: finished.")
    return result


def reduce_all(
    spark: SparkSession,
    transformed: dict[str, DataFrame],
    config_per_table: dict[str, PreprocessConfig] | None = None,
) -> dict[str, DataFrame]:
    """Stage 5: Optional sampling/reduction for each dataset. Logs progress."""
    configs = config_per_table or default_config()
    names = list(transformed)
    _log(f"Stage 5: Reduce ({len(names)} datasets)")
    result = {}
    for i, name in enumerate(names, 1):
        t0 = time.perf_counter()
        _log(f"  [{i}/{len(names)}] {name}: reduce...")
        cfg = configs.get(name) or default_config().get(name) or PreprocessConfig()
        df = reduce_df(transformed[name], cfg)
        n = df.count()
        elapsed = time.perf_counter() - t0
        _log(f"  [{i}/{len(names)}] {name}: done ({n} rows, {elapsed:.1f}s)")
        result[name] = df
    _log("Stage 5: finished.")
    return result


def _write_partition_count(spark: SparkSession) -> int:
    """
    Fewer partitions = fewer concurrent write tasks on low-RAM machines.
    Override with YELP_WRITE_PARTITIONS (integer).
    """
    raw = os.environ.get("YELP_WRITE_PARTITIONS", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    dp = spark.sparkContext.defaultParallelism
    # More files, smaller per-task sorts during shuffle → less spill (helps when /tmp is tiny).
    return max(8, min(64, dp * 8))


def write_processed(processed: dict[str, DataFrame], base_path=None) -> None:
    """
    Write processed DataFrames to Parquet. Logs progress.
    Repartitions before write to limit memory per task and avoid spill/OOM.

    If you see ``Disk quota exceeded`` during spill: point scratch space at a
    large filesystem — ``os.environ["SPARK_LOCAL_DIRS"] = "/path/with/space"``
    (or ``SPARK_LOCAL_DIR``), then ``spark.stop()`` and ``create_spark_session``
    again. You do not need a kernel restart, but you must re-run the pipeline
    from load after a new session (old DataFrames are invalid). Optionally raise
    ``YELP_WRITE_PARTITIONS`` (e.g. 32) to shrink per-task sorts.
    """
    from pathlib import Path

    from src.constants import PROCESSED_DIR

    base = Path(base_path) if base_path else PROCESSED_DIR
    base.mkdir(parents=True, exist_ok=True)
    names = list(processed)
    if not names:
        _log("Writing Parquet: nothing to write (no datasets).")
        return
    spark = next(iter(processed.values())).sparkSession
    n_parts = _write_partition_count(spark)
    _log(f"Writing Parquet ({len(names)} datasets) to {base} ({n_parts} partitions each)")
    for i, name in enumerate(names, 1):
        t0 = time.perf_counter()
        _log(f"  [{i}/{len(names)}] {name}: writing...")
        out_path = base / name
        df = processed[name].repartition(n_parts)
        df.write.mode("overwrite").parquet(str(out_path))
        elapsed = time.perf_counter() - t0
        _log(f"  [{i}/{len(names)}] {name}: done ({elapsed:.1f}s)")
    _log("Writing: finished.")


def preprocess_all(
    spark: SparkSession,
    config_per_table: dict[str, PreprocessConfig] | None = None,
    load_raw: bool = True,
) -> dict[str, DataFrame]:
    """
    Load each dataset, run its preprocessor, return dict of processed DataFrames.
    Logs progress per dataset and a heartbeat every minute during long steps.
    For more control and less memory use, run the staged pipeline instead:
    load_all_raw -> clean_all -> flatten_all -> transform_all -> reduce_all.
    """
    if not load_raw:
        raise ValueError("preprocess_all with load_raw=False requires raw dict")

    from src.spark.load_data import DATASETS, load_dataset

    configs = config_per_table or default_config()
    names = _available_dataset_names()
    _log(f"Preprocessing {len(names)} datasets (heartbeat every {_HEARTBEAT_INTERVAL}s)")
    result = {}
    for i, name in enumerate(names, 1):
        _, path = DATASETS[name]
        t0 = time.perf_counter()
        _log(f"  [{i}/{len(names)}] {name}: loading + processing...")

        def _run():
            raw = load_dataset(spark, name)
            cfg = configs.get(name)
            return PREPROCESSORS[name](spark, raw, cfg)

        df = _run_with_heartbeat(name, _run)
        n = df.count()
        elapsed = time.perf_counter() - t0
        _log(f"  [{i}/{len(names)}] {name}: done ({n} rows, {elapsed:.1f}s)")
        result[name] = df
    _log("Preprocessing: all finished.")
    return result
