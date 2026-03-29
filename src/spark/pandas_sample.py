"""Sample Spark DataFrames to Pandas for notebooks (plots) without OOM."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def to_pandas_sample(
    df: DataFrame,
    fraction: float = 0.15,
    seed: int = 42,
    max_rows: int = 50_000,
    columns: list[str] | None = None,
):
    """
    Sample then collect at most ``max_rows`` rows to Pandas.

    For text-heavy tables (e.g. reviews, tips), pass ``columns`` so Spark never
    materializes large string columns (``text``, etc.) when you only need a few
    numeric fields for charts.

    Avoids ``coalesce(1)`` before collect, which merges all sampled rows into one
    partition and can OOM the executor heap.
    """
    if columns is not None:
        use = [c for c in columns if c in df.columns]
        if not use:
            raise ValueError(
                f"None of the requested columns exist on the DataFrame: {columns!r}"
            )
        base = df.select(*use)
    else:
        base = df
    sampled = base.sample(withReplacement=False, fraction=fraction, seed=seed)
    return sampled.limit(max_rows).toPandas()
