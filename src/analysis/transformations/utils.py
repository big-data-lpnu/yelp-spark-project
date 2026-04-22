"""Shared helpers for the transformation layer."""

from pathlib import Path

from pyspark.sql import DataFrame


def save_csv(df: DataFrame, dir: Path, name: str) -> None:
    """Write *df* to CSV files inside *dir*."""
    out = str(dir / name)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)
    print(f"  -> saved: {out}")
