"""Detect and drop uninformative columns (all-null or constant)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Never drop primary keys / join keys
_ID_COLUMNS = frozenset(
    {
        "business_id",
        "user_id",
        "review_id",
        "photo_id",
    }
)


def find_uninformative_columns(
    df: DataFrame,
    extra_exclude: frozenset[str] | set[str] | None = None,
) -> list[str]:
    """
    Columns where COUNT(DISTINCT col) <= 1 (all null or a single distinct value).
    ID columns are always kept.
    """
    ex = set(_ID_COLUMNS)
    if extra_exclude:
        ex |= set(extra_exclude)
    cols = [c for c in df.columns if c not in ex]
    if not cols:
        return []
    aliases = [f"_dist_{i}" for i in range(len(cols))]
    aggs = [F.countDistinct(F.col(c)).alias(aliases[i]) for i, c in enumerate(cols)]
    row = df.agg(*aggs).first()
    out: list[str] = []
    for i, c in enumerate(cols):
        d = row[aliases[i]]
        if d is None or int(d) <= 1:
            out.append(c)
    return sorted(out)


def drop_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for c in columns:
        if c in df.columns:
            df = df.drop(c)
    return df


def drop_uninformative(
    df: DataFrame,
    columns: list[str],
) -> DataFrame:
    """Drop listed columns if present."""
    return drop_columns(df, columns)
