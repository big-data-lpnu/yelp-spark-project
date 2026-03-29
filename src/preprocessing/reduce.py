"""Optional data reduction: sampling, PCA stub."""

from pyspark.sql import DataFrame

from src.preprocessing.config import PreprocessConfig


def sample(
    df: DataFrame,
    fraction: float | None,
    seed: int = 42,
) -> DataFrame:
    """Return sampled DataFrame if fraction is set, else unchanged."""
    if fraction is None or fraction >= 1.0:
        return df
    return df.sample(withReplacement=False, fraction=fraction, seed=seed)


def reduce_df(
    df: DataFrame,
    config: PreprocessConfig,
) -> DataFrame:
    """Apply optional sampling."""
    return sample(df, config.sample_fraction, config.sample_seed)
