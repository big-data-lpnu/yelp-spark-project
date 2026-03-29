"""Data transformation: normalization, scaling, date parsing."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from src.preprocessing.config import PreprocessConfig


def _numeric_columns(df: DataFrame) -> list[str]:
    """Column names that are numeric (int or double)."""
    out = []
    for f in df.schema.fields:
        if f.name in df.columns and isinstance(
            f.dataType, (DoubleType, IntegerType)
        ):
            out.append(f.name)
    return out


def parse_dates(df: DataFrame, date_columns: list[str] | None = None) -> DataFrame:
    """Parse string date columns to date type; add year, month, day_of_week."""
    if date_columns is None:
        date_columns = ["date", "yelping_since"]
    for col_name in date_columns:
        if col_name not in df.columns:
            continue
        c = F.trim(F.col(col_name))
        # Yelp review/tip "date" is "yyyy-MM-dd HH:mm:ss". Do not use to_timestamp/to_date
        # with "yyyy-MM-dd" on the full string — ANSI mode throws. Take yyyy-MM-dd via
        # substring, then try_to_date only (never strict to_timestamp on unparsed tails).
        head_date = F.substring(c, 1, 10)
        parsed = F.coalesce(
            F.try_to_date(head_date, "yyyy-MM-dd"),
            F.try_to_date(c, "yyyy-MM-dd"),
            F.try_to_date(c, "yyyy-MM"),
        )
        df = df.withColumn(col_name + "_parsed", parsed)
        df = df.withColumn(
            col_name + "_year",
            F.year(F.col(col_name + "_parsed")),
        )
        df = df.withColumn(
            col_name + "_month",
            F.month(F.col(col_name + "_parsed")),
        )
        df = df.withColumn(
            col_name + "_day_of_week",
            F.dayofweek(F.col(col_name + "_parsed")),
        )
    return df


def scale_numeric(
    df: DataFrame,
    method: str = "minmax",
    exclude_columns: set[str] | None = None,
) -> DataFrame:
    """
    Scale numeric columns to 0-1 (minmax) or zero mean unit variance (standard).
    Excludes ID-like and key columns by default.
    """
    exclude_columns = exclude_columns or set()
    # Also exclude IDs and keys
    exclude_columns = exclude_columns | {
        "business_id",
        "user_id",
        "review_id",
        "photo_id",
        "is_open",
        "is_elite",
    }
    numeric = [
        c
        for c in _numeric_columns(df)
        if c not in exclude_columns
        and not c.endswith("_year")
        and not c.endswith("_month")
        and not c.endswith("_day_of_week")
    ]
    if not numeric:
        return df

    if method == "none":
        return df

    for col_name in numeric:
        if method == "minmax":
            stats = df.agg(
                F.min(F.col(col_name)).alias("min"),
                F.max(F.col(col_name)).alias("max"),
            ).first()
            min_val = stats["min"]
            max_val = stats["max"]
            if min_val is None or max_val is None:
                continue
            span = max_val - min_val
            if span == 0:
                df = df.withColumn(col_name + "_scaled", F.lit(0.0))
            else:
                df = df.withColumn(
                    col_name + "_scaled",
                    (F.col(col_name) - min_val) / span,
                )
        elif method == "standard":
            stats = df.agg(
                F.mean(F.col(col_name)).alias("mean"),
                F.stddev(F.col(col_name)).alias("std"),
            ).first()
            mean_val = stats["mean"]
            std_val = stats["std"]
            if mean_val is None or std_val is None:
                continue
            if std_val == 0 or std_val is None:
                df = df.withColumn(col_name + "_scaled", F.lit(0.0))
            else:
                df = df.withColumn(
                    col_name + "_scaled",
                    (F.col(col_name) - mean_val) / std_val,
                )

    # Replace original numeric columns with scaled values
    for col_name in numeric:
        scaled = col_name + "_scaled"
        if scaled in df.columns:
            df = df.drop(col_name)
            df = df.withColumn(col_name, F.col(scaled))
            df = df.drop(scaled)

    return df


def drop_columns(df: DataFrame, to_drop: list[str]) -> DataFrame:
    """Drop columns that exist."""
    for c in to_drop:
        if c in df.columns:
            df = df.drop(c)
    return df


def prune_after_flatten(
    df: DataFrame,
    config: PreprocessConfig,
    _table: str,
) -> DataFrame:
    """Drop redundant nested/array columns once flatten-derived fields exist."""
    if not config.columns_to_prune_after_flatten:
        return df
    return drop_columns(df, config.columns_to_prune_after_flatten)


def transform(
    df: DataFrame,
    config: PreprocessConfig,
    table: str,
) -> DataFrame:
    """Apply date parsing and scaling; then drop configured columns."""
    if config.parse_dates:
        df = parse_dates(df)
    if config.scale_method and config.scale_method != "none":
        df = scale_numeric(df, method=config.scale_method)
    if config.columns_to_drop:
        df = drop_columns(df, config.columns_to_drop)
    return df
