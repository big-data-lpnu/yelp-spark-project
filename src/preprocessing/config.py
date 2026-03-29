"""Configuration for preprocessing pipeline: null handling, scaling, reduction."""

from dataclasses import dataclass, field
@dataclass
class PreprocessConfig:
    """Per-table or global preprocessing options."""

    # Cleaning
    null_strategy: str = "impute"  # "impute" | "drop"
    impute_numeric: str = "median"  # "mean" | "median"
    impute_categorical: str = "Unknown"
    drop_duplicates: bool = True

    # Columns to drop immediately after flatten (redundant vs derived columns).
    columns_to_prune_after_flatten: list[str] = field(default_factory=list)

    # Columns to drop in transform after parse_dates (e.g. raw date string once parsed).
    columns_to_drop: list[str] = field(default_factory=list)

    # After prune: drop columns with COUNT(DISTINCT) <= 1 (all null or constant).
    drop_uninformative: bool = True

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
    """Default config: prune drops redundant nested/arrays after flatten; columns_to_drop removes raw dates after parsing."""
    return {
        "business": PreprocessConfig(
            columns_to_prune_after_flatten=["attributes", "hours", "categories"],
            columns_to_drop=[],
        ),
        "review": PreprocessConfig(
            columns_to_prune_after_flatten=[],
            columns_to_drop=["date"],
        ),
        "user": PreprocessConfig(
            columns_to_prune_after_flatten=["friends", "elite"],
            columns_to_drop=[],
        ),
        "checkin": PreprocessConfig(
            columns_to_prune_after_flatten=[],
            columns_to_drop=["date"],
        ),
        "tip": PreprocessConfig(
            columns_to_prune_after_flatten=[],
            columns_to_drop=["date"],
        ),
        "photo": PreprocessConfig(),
    }
