"""
Data validation module for PySpark DataFrames.

Provides comprehensive validation and profiling of loaded datasets, including:
- Schema inspection
- Row and column statistics
- Null value analysis
- Summary statistics for numeric and string columns
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    when,
    min as spark_min,
    max as spark_max,
    mean,
    stddev,
)
from pyspark.sql.types import NumericType, StringType


def _print_report_header(dataset_name: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"📊 DATA VALIDATION REPORT: {dataset_name.upper()}")
    print(f"{'=' * 80}")


def _analyze_null_values(df: DataFrame, columns: list[str], row_count: int) -> dict:
    print("\n🔍 NULL VALUE ANALYSIS")
    null_analysis = {}

    null_counts = df.select(
        [count(when(col(c).isNull(), 1)).alias(c) for c in columns]
    ).collect()[0]

    has_nulls = False
    for col_name in columns:
        null_count = null_counts[col_name]
        null_percentage = (null_count / row_count * 100) if row_count > 0 else 0

        null_analysis[col_name] = {
            "null_count": null_count,
            "null_percentage": round(null_percentage, 2),
        }

        if null_count > 0:
            has_nulls = True
            print(
                f"  • {col_name}: {null_count:,} nulls ({null_percentage:.2f}%)"
            )

    if not has_nulls:
        print("  • No null values detected ✓")

    return null_analysis


def _collect_numeric_stats(df: DataFrame, numeric_columns: list[str]) -> dict:
    if not numeric_columns:
        return {}

    print("\n🔢 NUMERIC COLUMN STATISTICS")
    numeric_stats = {}

    agg_expressions = []
    for col_name in numeric_columns:
        agg_expressions.extend(
            [
                spark_min(col(col_name)).alias(f"{col_name}_min"),
                spark_max(col(col_name)).alias(f"{col_name}_max"),
                mean(col(col_name)).alias(f"{col_name}_mean"),
                stddev(col(col_name)).alias(f"{col_name}_stddev"),
            ]
        )

    stats_row = df.select(*agg_expressions).collect()[0]

    for col_name in numeric_columns:
        min_val = stats_row[f"{col_name}_min"]
        max_val = stats_row[f"{col_name}_max"]
        mean_val = stats_row[f"{col_name}_mean"]
        stddev_val = stats_row[f"{col_name}_stddev"]

        numeric_stats[col_name] = {
            "min": min_val,
            "max": max_val,
            "mean": round(mean_val, 4) if mean_val is not None else None,
            "stddev": round(stddev_val, 4) if stddev_val is not None else None,
        }

        mean_text = f"{mean_val:.4f}" if mean_val is not None else "None"
        stddev_text = (
            f"{stddev_val:.4f}" if stddev_val is not None else "None"
        )
        print(
            f"  • {col_name}:"
            f" min={min_val}, max={max_val}, mean={mean_text}, stddev={stddev_text}"
        )

    return numeric_stats


def _collect_string_stats(df: DataFrame, string_columns: list[str]) -> dict:
    if not string_columns:
        return {}

    print("\n📝 STRING COLUMN STATISTICS")
    string_stats = {}

    for col_name in string_columns:
        distinct_count = df.select(col_name).distinct().count()
        string_stats[col_name] = {
            "distinct_count": distinct_count,
        }
        print(f"  • {col_name}: {distinct_count:,} distinct values")

    return string_stats


def validate_dataframe(df: DataFrame, dataset_name: str) -> dict:
    """
    Performs comprehensive validation and profiling on a Spark DataFrame.

    Parameters:
    -----------
    df : DataFrame
        The Spark DataFrame to validate
    dataset_name : str
        The name of the dataset (used for reporting)

    Returns:
    --------
    dict
        A dictionary containing validation report with keys:
        - 'dataset_name': Name of the dataset
        - 'row_count': Total number of rows
        - 'column_count': Number of columns
        - 'columns': List of column names
        - 'schema': DataFrame schema
        - 'null_analysis': Dictionary mapping column names to null counts/percentages
        - 'numeric_stats': Dictionary with statistics for numeric columns
        - 'string_stats': Dictionary with statistics for string columns
    """

    # Handle empty DataFrames
    if not df.columns:
        print(f"\n⚠️  Dataset '{dataset_name}' has no columns!")
        return {
            "dataset_name": dataset_name,
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "schema": None,
            "error": "DataFrame has no columns",
        }

    _print_report_header(dataset_name)

    # 1. Basic DataFrame Info
    row_count = df.count()
    column_count = len(df.columns)
    columns = df.columns

    print("\n📈 BASIC STATISTICS")
    print(f"  • Rows: {row_count:,}")
    print(f"  • Columns: {column_count}")
    print(f"  • Columns: {', '.join(columns)}")

    # 2. Schema Information
    print("\n📋 SCHEMA")
    df.printSchema()

    # 3. Null Value Analysis
    null_analysis = _analyze_null_values(df, columns, row_count)

    # 4. Numeric Column Statistics
    numeric_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    ]

    numeric_stats = _collect_numeric_stats(df, numeric_columns)

    # 5. String Column Statistics
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    string_stats = _collect_string_stats(df, string_columns)

    # 6. Summary
    print(f"\n{'=' * 80}")
    print(f"✅ Validation complete for '{dataset_name}'")
    print(f"{'=' * 80}\n")

    # Return validation report
    return {
        "dataset_name": dataset_name,
        "row_count": row_count,
        "column_count": column_count,
        "columns": columns,
        "schema": df.schema,
        "null_analysis": null_analysis,
        "numeric_stats": numeric_stats,
        "string_stats": string_stats,
    }
