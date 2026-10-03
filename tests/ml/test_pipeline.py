import pytest
from pyspark.sql import functions as F

from src.ml import pipeline as pl
from src.ml.features import (
    CLASSIFICATION_LABEL,
    RECENCY_FEATURES,
    REGRESSION_LABEL,
)


def test_split_is_disjoint_complete_and_deterministic(spark):
    ids = spark.range(20_000).select(
        F.col("id").cast("string").alias("business_id")
    )
    first = pl.assign_split(ids)
    second = pl.assign_split(ids.repartition(7))

    counts = {
        r.split: r["count"] for r in first.groupBy("split").count().collect()
    }
    assert sum(counts.values()) == 20_000
    assert counts["train"] / 20_000 == pytest.approx(0.70, abs=0.02)
    assert counts["validation"] / 20_000 == pytest.approx(0.15, abs=0.02)
    assert counts["test"] / 20_000 == pytest.approx(0.15, abs=0.02)

    joined = first.alias("a").join(second.alias("b"), "business_id")
    assert joined.filter(F.col("a.split") != F.col("b.split")).count() == 0


def test_class_weights_are_balanced_on_train(spark):
    df = spark.createDataFrame(
        [(str(i), float(i < 20), "train") for i in range(100)]
        + [("v", 1.0, "validation")],
        "business_id string, is_closed double, split string",
    )
    weighted = pl.add_class_weights(df, "is_closed")
    train = weighted.filter("split = 'train'")
    by_class = {
        r.is_closed: r.w
        for r in train.groupBy("is_closed")
        .agg(F.first(pl.WEIGHT_COL).alias("w"))
        .collect()
    }
    assert by_class[1.0] == pytest.approx(100 / (2 * 20))
    assert by_class[0.0] == pytest.approx(100 / (2 * 80))
    # Mean weight 1 keeps regularisation strength comparable.
    assert train.agg(F.avg(pl.WEIGHT_COL)).first()[0] == pytest.approx(1.0)


def test_tasks_do_not_feed_their_own_label():
    regression = pl.regression_task()
    assert REGRESSION_LABEL not in regression.numeric_features
    assert not [f for f in regression.numeric_features if "star" in f]

    for include_recency in (True, False):
        task = pl.classification_task(include_recency=include_recency)
        assert CLASSIFICATION_LABEL not in task.numeric_features
        assert "is_open" not in task.numeric_features
        has_recency = set(RECENCY_FEATURES) & set(task.numeric_features)
        assert bool(has_recency) == include_recency

    for task in (regression, pl.classification_task()):
        assert "business_id" not in task.numeric_features


def test_preprocessing_learns_only_from_train(spark):
    task = pl.TaskSpec(
        name="toy",
        label="y",
        numeric_features=("x",),
        categorical_features=("c",),
        use_categories=False,
    )
    df = spark.createDataFrame(
        [
            (1.0, 1.0, "a", "train"),
            (2.0, 2.0, "a", "train"),
            (3.0, 3.0, "b", "train"),
            # Huge test value must not move the imputed median.
            (1000.0, 0.0, "a", "test"),
            (None, 0.0, "zzz", "test"),
        ],
        "x double, y double, c string, split string",
    )
    model = pl.preprocessing_pipeline(task).fit(df.filter("split = 'train'"))
    imputer = model.stages[0]
    assert imputer.surrogateDF.first()["x"] == 2.0

    out = model.transform(df.filter("split = 'test'"))
    names = pl.feature_names(out, model)
    assert names[0] == "x"
    assert any(n.startswith("c=") for n in names)

    # Materialise the vectors (count() alone would prune the transformers).
    rows = {
        r.c: r.features_raw.toArray()
        for r in out.select("c", "features_raw").collect()
    }
    assert rows["a"][0] == 1000.0  # raw test value kept as is
    assert rows["zzz"][0] == 2.0  # null -> TRAIN median, not test median
    # Unseen level lands in the "unknown" slot instead of failing.
    assert rows["zzz"][names.index("c=__unknown")] == 1.0


def test_category_slots_are_named_by_vocabulary(spark):
    task = pl.TaskSpec(
        name="toy",
        label="y",
        numeric_features=("x",),
        categorical_features=(),
        use_categories=True,
    )
    rows = [
        (float(i), 1.0, ["pizza"] if i % 2 else ["bars"]) for i in range(120)
    ]
    df = spark.createDataFrame(
        rows, "x double, y double, category_list array<string>"
    )
    model = pl.preprocessing_pipeline(task).fit(df)
    names = pl.feature_names(model.transform(df), model)
    assert {"category=pizza", "category=bars"} <= set(names)
