"""
Spark ML preprocessing: feature selection per task, train/validation/test
split and the learned preprocessing stages.

Every stage that learns from data (Imputer medians, StandardScaler mean and
std) is part of a Pipeline that is fitted on the training split only, so no
statistics of the validation or test rows leak into the model.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.constants import ML_SEED
from src.ml.features import (
    CLASSIFICATION_EXTRA_FEATURES,
    CLASSIFICATION_LABEL,
    DIAGNOSTIC_COLUMNS,
    ID_COLUMN,
    REGRESSION_EXTRA_FEATURES,
    REGRESSION_LABEL,
    SHARED_FEATURES,
)

FEATURES_COL = "features"
SPLIT_COL = "split"
BUCKET_COL = "bucket"
SPLITS = ("train", "validation", "test")
TRAIN_BUCKETS = 70
VALIDATION_BUCKETS = 15
TRAINING_PARTITIONS = 16

REGRESSION = "regression"
CLASSIFICATION = "classification"


@dataclass(frozen=True)
class TaskSpec:
    """What to predict and from which columns."""

    name: str
    kind: str  # REGRESSION | CLASSIFICATION
    label: str
    numeric_features: tuple[str, ...]

    @property
    def is_classification(self) -> bool:
        return self.kind == CLASSIFICATION


def regression_task() -> TaskSpec:
    """log(1 + fans) from activity, votes, compliments and elite status."""
    return TaskSpec(
        name=REGRESSION,
        kind=REGRESSION,
        label=REGRESSION_LABEL,
        numeric_features=(*SHARED_FEATURES, *REGRESSION_EXTRA_FEATURES),
    )


def classification_task() -> TaskSpec:
    """Has the user ever been elite? Nothing derived from ``elite`` is used."""
    return TaskSpec(
        name=CLASSIFICATION,
        kind=CLASSIFICATION,
        label=CLASSIFICATION_LABEL,
        numeric_features=(*SHARED_FEATURES, *CLASSIFICATION_EXTRA_FEATURES),
    )


def assign_split(df: DataFrame, seed: int = ML_SEED) -> DataFrame:
    """
    Add ``bucket`` (0-99) and ``split`` columns from a seeded hash of the
    user id: buckets 0-69 train, 70-84 validation, 85-99 test.

    The assignment depends only on the id, so it is identical for both tasks
    and every re-run, regardless of how the data is partitioned
    (``randomSplit`` is not). Smaller training subsets for the learning
    curve are nested: bucket < 7 is contained in bucket < 35, etc. With ~2M
    rows the class ratio of each split matches the overall one closely, so
    no explicit stratification is needed.
    """
    bucket = F.pmod(F.xxhash64(F.col(ID_COLUMN), F.lit(seed)), F.lit(100))
    return df.withColumn(BUCKET_COL, bucket.cast("int")).withColumn(
        SPLIT_COL,
        F.when(F.col(BUCKET_COL) < TRAIN_BUCKETS, "train")
        .when(
            F.col(BUCKET_COL) < TRAIN_BUCKETS + VALIDATION_BUCKETS,
            "validation",
        )
        .otherwise("test"),
    )


def preprocessing_pipeline(task: TaskSpec) -> Pipeline:
    """Imputer (median) -> VectorAssembler -> StandardScaler."""
    numeric = list(task.numeric_features)
    imputed = [f"{c}__imp" for c in numeric]
    return Pipeline(
        stages=[
            # Median of the training split; a safety net (the table has no
            # NULLs today), robust to heavy tails.
            Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
            VectorAssembler(inputCols=imputed, outputCol="features_raw"),
            # Zero mean / unit variance: required by the neural network and
            # factorization machine (gradient-based), makes linear
            # coefficients comparable; trees are unaffected.
            StandardScaler(
                inputCol="features_raw",
                outputCol=FEATURES_COL,
                withMean=True,
                withStd=True,
            ),
        ]
    )


def feature_names(task: TaskSpec) -> list[str]:
    """Slot i of the feature vector is the i-th numeric feature."""
    return list(task.numeric_features)


@dataclass
class PreparedData:
    """Train/validation/test DataFrames with the fitted preprocessing."""

    task: TaskSpec
    preprocessing: PipelineModel
    train: DataFrame
    validation: DataFrame
    test: DataFrame
    feature_names: list[str]


def prepare(features: DataFrame, task: TaskSpec) -> PreparedData:
    """Split, fit preprocessing on train, transform all three splits."""
    df = assign_split(features)
    keep = [ID_COLUMN, task.label, SPLIT_COL, BUCKET_COL, *DIAGNOSTIC_COLUMNS]
    model = preprocessing_pipeline(task).fit(
        df.filter(F.col(SPLIT_COL) == "train")
    )
    prepared = model.transform(df).select(*keep, FEATURES_COL)

    # Fixed, id-based partitioning: Random Forest bootstraps per partition,
    # so a stable layout keeps results reproducible across runs.
    parts = {
        name: prepared.filter(F.col(SPLIT_COL) == name)
        .drop(SPLIT_COL)
        .repartition(TRAINING_PARTITIONS, ID_COLUMN)
        .cache()
        for name in SPLITS
    }
    for part in parts.values():
        part.count()  # materialise the cache once

    return PreparedData(
        task=task,
        preprocessing=model,
        train=parts["train"],
        validation=parts["validation"],
        test=parts["test"],
        feature_names=feature_names(task),
    )
