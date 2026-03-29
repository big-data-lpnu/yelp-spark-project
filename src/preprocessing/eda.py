"""Exploratory helpers for preprocessing notebooks: overview, nulls, duplicates, numeric describe."""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
)

from src.preprocessing.clean import TABLE_KEYS

_NUMERIC_TYPES = (
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    ShortType,
    ByteType,
    DecimalType,
)


def _maybe_sample(df: DataFrame, sample_fraction: float | None) -> DataFrame:
    if sample_fraction is None or sample_fraction >= 1.0:
        return df
    return df.sample(withReplacement=False, fraction=float(sample_fraction), seed=42)


def dataset_overview(dfs: dict[str, DataFrame]) -> pd.DataFrame:
    """Row and column counts per table (full scan per table for row count)."""
    rows = []
    for name, df in dfs.items():
        n = df.count()
        rows.append({"table": name, "rows": n, "columns": len(df.columns)})
    return pd.DataFrame(rows)


def numeric_columns(df: DataFrame) -> list[str]:
    """Names of numeric (non-ID heuristic: include all numeric types in schema)."""
    out = []
    for f in df.schema.fields:
        if f.name not in df.columns:
            continue
        if isinstance(f.dataType, _NUMERIC_TYPES):
            out.append(f.name)
    return out


def numeric_describe(
    df: DataFrame,
    sample_fraction: float | None = None,
    exclude_suffixes: tuple[str, ...] = (),
) -> pd.DataFrame:
    """
    Spark summary stats for numeric columns only.
    exclude_suffixes: e.g. ('_year', '_month', '_day_of_week') to focus on raw-scale metrics.
    """
    work = _maybe_sample(df, sample_fraction)
    cols = numeric_columns(work)
    for suf in exclude_suffixes:
        cols = [c for c in cols if not c.endswith(suf)]
    if not cols:
        return pd.DataFrame()
    return work.select(*cols).summary().toPandas()


def null_report(
    df: DataFrame,
    table: str,
    *,
    key_columns_only: bool = False,
    sample_fraction: float | None = None,
) -> pd.DataFrame:
    """
    Per-column null counts and fractions. Uses optional sampling for large tables.
    """
    work = _maybe_sample(df, sample_fraction)
    n = work.count()
    if n == 0:
        return pd.DataFrame(
            columns=["column", "n_rows", "null_count", "null_fraction", "table"]
        )

    if key_columns_only:
        keys = TABLE_KEYS.get(table, [])
        colnames = [c for c in keys if c in work.columns]
        if not colnames:
            colnames = list(work.columns)
    else:
        colnames = list(work.columns)

    agg_exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in colnames]
    row = work.agg(*agg_exprs).first()
    rows_out = []
    for c in colnames:
        nc = row[c] if row[c] is not None else 0
        frac = float(nc) / float(n) if n else 0.0
        rows_out.append(
            {
                "column": c,
                "n_rows": n,
                "null_count": int(nc),
                "null_fraction": round(frac, 6),
                "table": table,
            }
        )
    return pd.DataFrame(rows_out).sort_values("null_fraction", ascending=False)


def duplicate_report(df: DataFrame, table: str) -> pd.DataFrame:
    """
    Row count vs distinct-key count; duplicate_rows = total - distinct (same keys as clean.drop_duplicates).
    """
    keys = TABLE_KEYS.get(table)
    if not keys or not all(c in df.columns for c in keys):
        n = df.count()
        return pd.DataFrame(
            [
                {
                    "table": table,
                    "total_rows": n,
                    "distinct_key_rows": n,
                    "duplicate_rows": 0,
                    "keys": None,
                }
            ]
        )
    total = df.count()
    distinct = df.dropDuplicates(keys).count()
    dup = total - distinct
    return pd.DataFrame(
        [
            {
                "table": table,
                "total_rows": total,
                "distinct_key_rows": distinct,
                "duplicate_rows": dup,
                "keys": ",".join(keys),
            }
        ]
    )


def duplicate_report_all(dfs: dict[str, DataFrame]) -> pd.DataFrame:
    """Stack duplicate_report for every table."""
    parts = [duplicate_report(dfs[name], name) for name in dfs]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
