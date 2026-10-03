"""
Spark ML preprocessing: feature selection per task, train/validation/test
split and the learned preprocessing stages.

Every stage that learns from data (Imputer medians, StringIndexer levels,
CountVectorizer vocabulary, StandardScaler std) is part of a Pipeline that is
fitted on the training split only, so no statistics of the validation or test
rows leak into the model.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    CountVectorizer,
    CountVectorizerModel,
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.constants import ML_SEED
from src.ml.features import (
    CATEGORICAL_FEATURES,
    CATEGORY_ARRAY_FEATURE,
    CLASSIFICATION_LABEL,
    ENGAGEMENT_NUMERIC_FEATURES,
    ID_COLUMN,
    PROFILE_NUMERIC_FEATURES,
    RECENCY_FEATURES,
    REGRESSION_LABEL,
)

FEATURES_COL = "features"
WEIGHT_COL = "class_weight"
SPLIT_COL = "split"
BUCKET_COL = "bucket"
SPLITS = ("train", "validation", "test")
TRAIN_BUCKETS = 70
VALIDATION_BUCKETS = 15
TRAINING_PARTITIONS = 8

# Raw columns carried next to the feature vector for baselines and error
# analysis. They are never part of the model input unless the task lists
# them as features.
DIAGNOSTIC_COLUMNS = [
    "state",
    CATEGORY_ARRAY_FEATURE,
    "log_review_count",
    "days_since_last_review",
]

# Business categories kept as binary indicators (most frequent in train).
CATEGORY_VOCAB_SIZE = 60


@dataclass(frozen=True)
class TaskSpec:
    """What to predict and from which columns."""

    name: str
    label: str
    numeric_features: tuple[str, ...]
    categorical_features: tuple[str, ...] = tuple(CATEGORICAL_FEATURES)
    use_categories: bool = True


def regression_task() -> TaskSpec:
    """Predict business star rating. The other label is a legit feature."""
    return TaskSpec(
        name="regression",
        label=REGRESSION_LABEL,
        numeric_features=(
            *PROFILE_NUMERIC_FEATURES,
            *ENGAGEMENT_NUMERIC_FEATURES,
            *RECENCY_FEATURES,
            CLASSIFICATION_LABEL,
        ),
    )


def regression_profile_task() -> TaskSpec:
    """Ablation: only what the listing says about itself (no activity)."""
    return TaskSpec(
        name="regression_profile",
        label=REGRESSION_LABEL,
        numeric_features=(*PROFILE_NUMERIC_FEATURES, CLASSIFICATION_LABEL),
    )


def classification_task(include_recency: bool = True) -> TaskSpec:
    """Predict whether a business is closed (positive class = closed)."""
    recency = tuple(RECENCY_FEATURES) if include_recency else ()
    return TaskSpec(
        name="classification_recency" if include_recency else "classification",
        label=CLASSIFICATION_LABEL,
        numeric_features=(
            *PROFILE_NUMERIC_FEATURES,
            *ENGAGEMENT_NUMERIC_FEATURES,
            *recency,
            REGRESSION_LABEL,
        ),
    )


def assign_split(df: DataFrame, seed: int = ML_SEED) -> DataFrame:
    """
    Add ``bucket`` (0-99) and ``split`` columns from a seeded hash of the
    business id: buckets 0-69 train, 70-84 validation, 85-99 test.

    The assignment depends only on the id, so it is identical for both tasks,
    every ablation and every re-run, regardless of how the data is
    partitioned (``randomSplit`` is not). Smaller training subsets for the
    learning curve are nested: bucket < 7 is contained in bucket < 35, etc.
    With 150k rows the class ratio of each split stays within a few tenths of
    a percent of the overall one, so no explicit stratification is needed.
    """
    bucket = F.pmod(F.xxhash64(F.col(ID_COLUMN), F.lit(seed)), F.lit(100))
    return df.withColumn(BUCKET_COL, bucket.cast("int")).withColumn(
        SPLIT_COL,
        F.when(F.col(BUCKET_COL) < TRAIN_BUCKETS, "train")
        .when(
            F.col(BUCKET_COL) < TRAIN_BUCKETS + VALIDATION_BUCKETS, "validation"
        )
        .otherwise("test"),
    )


def add_class_weights(df: DataFrame, label: str) -> DataFrame:
    """
    Balanced class weights computed on the training rows:
    w_c = n_train / (2 * n_train_c). Applied to every row; only training uses
    them.
    """
    counts = {
        float(r[label]): r["count"]
        for r in df.filter(F.col(SPLIT_COL) == "train")
        .groupBy(label)
        .count()
        .collect()
    }
    total = sum(counts.values())
    weight = F.lit(1.0)
    for value, n in counts.items():
        weight = F.when(F.col(label) == value, total / (2.0 * n)).otherwise(
            weight
        )
    return df.withColumn(WEIGHT_COL, weight)


def preprocessing_pipeline(task: TaskSpec) -> Pipeline:
    """Imputer -> StringIndexer/OHE -> CountVectorizer -> assembler -> std."""
    numeric = list(task.numeric_features)
    categorical = list(task.categorical_features)
    imputed = [f"{c}__imp" for c in numeric]
    indexed = [f"{c}__idx" for c in categorical]
    encoded = [f"{c}__ohe" for c in categorical]

    stages = [
        # Median of the training split; robust to the heavy-tailed counts.
        Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
        # Unseen levels at validation/test time go to an extra index.
        StringIndexer(
            inputCols=categorical,
            outputCols=indexed,
            handleInvalid="keep",
            stringOrderType="frequencyDesc",
        ),
        OneHotEncoder(
            inputCols=indexed, outputCols=encoded, handleInvalid="keep"
        ),
    ]
    assembled = imputed + encoded
    if task.use_categories:
        stages.append(
            CountVectorizer(
                inputCol=CATEGORY_ARRAY_FEATURE,
                outputCol="category_vec",
                vocabSize=CATEGORY_VOCAB_SIZE,
                minDF=50,
                binary=True,
            )
        )
        assembled.append("category_vec")

    stages += [
        VectorAssembler(inputCols=assembled, outputCol="features_raw"),
        # Unit variance so linear-model coefficients are comparable; trees are
        # invariant to this per-feature affine transform.
        StandardScaler(
            inputCol="features_raw",
            outputCol=FEATURES_COL,
            withMean=False,
            withStd=True,
        ),
    ]
    return Pipeline(stages=stages)


def feature_names(
    transformed: DataFrame, model: PipelineModel | None = None
) -> list[str]:
    """
    Human-readable name of every slot of the feature vector, read from the
    VectorAssembler's ``ml_attr`` metadata (the scaler does not carry it, but
    scaling keeps the slot order).

    CountVectorizer leaves its slots unnamed (``category_vec_<i>``); slot i
    is ``vocabulary[i]`` of the fitted model, so pass ``model`` to get the
    category words.
    """
    attrs = transformed.schema["features_raw"].metadata.get("ml_attr", {})
    slots: dict[int, str] = {}
    for group in attrs.get("attrs", {}).values():
        for attr in group:
            slots[attr["idx"]] = attr["name"]
    size = attrs.get("num_attrs", max(slots) + 1 if slots else 0)
    vocabulary = next(
        (
            stage.vocabulary
            for stage in (model.stages if model else [])
            if isinstance(stage, CountVectorizerModel)
        ),
        [],
    )
    return [
        _pretty_feature_name(slots.get(i, f"f{i}"), vocabulary)
        for i in range(size)
    ]


def _pretty_feature_name(name: str, vocabulary: list[str]) -> str:
    if name.startswith("category_vec_"):
        index = int(name.removeprefix("category_vec_"))
        term = vocabulary[index] if index < len(vocabulary) else index
        return f"category={term}"
    return name.replace("__imp", "").replace("__ohe_", "=")


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
    df = add_class_weights(df, task.label)

    keep = [
        ID_COLUMN,
        task.label,
        WEIGHT_COL,
        SPLIT_COL,
        BUCKET_COL,
        *DIAGNOSTIC_COLUMNS,
    ]
    model = preprocessing_pipeline(task).fit(
        df.filter(F.col(SPLIT_COL) == "train")
    )
    transformed = model.transform(df)
    prepared = transformed.select(*keep, FEATURES_COL)

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
        feature_names=feature_names(transformed, model),
    )
