"""Configuration for preprocessing pipeline: null handling, scaling, reduction."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PreprocessConfig:
    """Per-table or global preprocessing options."""

    # Cleaning
    null_strategy: str = "impute"  # "impute" | "drop"
    impute_numeric: str = "median"  # "mean" | "median"
    impute_categorical: str = "Unknown"
    drop_duplicates: bool = True

    # Columns to drop after flattening or for ML (table-specific)
    columns_to_drop: list[str] = field(default_factory=list)

    # Transformation
    scale_method: str = "minmax"  # "minmax" | "standard" | "none"
    parse_dates: bool = True

    # Reduction (optional)
    sample_fraction: float | None = None  # e.g. 0.1 for 10%
    sample_seed: int = 42
    do_pca: bool = False
    pca_components: int | None = None

    # Discretization (optional)
    discretize: bool = False
    bin_columns: dict[str, list[float]] = field(default_factory=dict)


def default_config() -> dict[str, PreprocessConfig]:
    """Default config per dataset; columns_to_drop are minimal (redundant after flatten)."""
    return {
        "business": PreprocessConfig(
            columns_to_drop=["attributes", "hours"],  # flattened
        ),
        "review": PreprocessConfig(
            columns_to_drop=["date"],  # raw string is yyyy-MM-dd HH:mm:ss; use date_parsed
        ),
        "user": PreprocessConfig(
            columns_to_drop=["friends", "elite"],  # replaced by _count
        ),
        "checkin": PreprocessConfig(
            columns_to_drop=["date"],  # replaced by checkin_count etc.
        ),
        "tip": PreprocessConfig(
            columns_to_drop=["date"],
        ),
        "photo": PreprocessConfig(),
    }
