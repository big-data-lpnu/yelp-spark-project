"""Data cleaning: missing values, de-duplication, optional outlier handling."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

from src.preprocessing.config import PreprocessConfig

# Primary keys per dataset for dropDuplicates
TABLE_KEYS = {
    "business": ["business_id"],
    "review": ["review_id"],
    "user": ["user_id"],
    "checkin": ["business_id", "date"],
    "tip": ["user_id", "business_id", "date"],
    "photo": ["photo_id"],
}


def drop_duplicates(df: DataFrame, table: str) -> DataFrame:
    """Drop duplicate rows by primary key."""
    keys = TABLE_KEYS.get(table)
    if not keys or not all(c in df.columns for c in keys):
        return df
    return df.dropDuplicates(keys)


def _numeric_columns(df: DataFrame) -> list[str]:
    """List numeric columns (excluding arrays/maps)."""
    out = []
    for f in df.schema.fields:
        if f.name in df.columns and isinstance(
            f.dataType, (DoubleType, IntegerType)
        ):
            out.append(f.name)
    return out


def _string_columns(df: DataFrame) -> list[str]:
    """List string columns (excluding arrays/maps)."""
    out = []
    for f in df.schema.fields:
        if f.name in df.columns and isinstance(f.dataType, StringType):
            out.append(f.name)
    return out


def handle_nulls(
    df: DataFrame,
    config: PreprocessConfig,
    table: str,
) -> DataFrame:
    """
    Apply null strategy: drop rows with nulls in key columns, then impute or
    drop for the rest.
    """
    keys = TABLE_KEYS.get(table, [])
    key_cols = [c for c in keys if c in df.columns]
    if key_cols:
        df = df.dropna(subset=key_cols)

    if config.null_strategy == "drop":
        return df.dropna(how="any")

    # Impute: numeric -> median/mean, string -> "Unknown"
    numeric = _numeric_columns(df)
    string = _string_columns(df)
    # Exclude key columns from imputation (already dropped nulls)
    numeric = [c for c in numeric if c not in key_cols]
    string = [c for c in string if c not in key_cols]

    for col_name in numeric:
        if col_name not in df.columns:
            continue
        stat = df.stat.approxQuantile(col_name, [0.5], 0.01)
        fill = float(stat[0]) if stat and stat[0] is not None else 0.0
        if config.impute_numeric == "mean":
            fill = df.agg(F.mean(col_name)).first()[0] or 0.0
        df = df.fillna({col_name: fill})

    for col_name in string:
        if col_name not in df.columns:
            continue
        df = df.fillna({col_name: config.impute_categorical})

    return df


def clean(df: DataFrame, table: str, config: PreprocessConfig) -> DataFrame:
    """Run cleaning: handle nulls and drop duplicates."""
    df = handle_nulls(df, config, table)
    if config.drop_duplicates:
        df = drop_duplicates(df, table)
    return df
