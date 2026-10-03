import pytest
from pyspark.sql import functions as F

from src.ml import pipeline as pl
from src.ml.features import CLASSIFICATION_LABEL, REGRESSION_LABEL


def test_split_is_disjoint_complete_and_deterministic(spark):
    ids = spark.range(20_000).select(
        F.col("id").cast("string").alias("user_id")
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

    joined = first.alias("a").join(second.alias("b"), "user_id")
    assert joined.filter(F.col("a.split") != F.col("b.split")).count() == 0


def test_tasks_do_not_feed_their_own_target():
    regression = pl.regression_task()
    assert REGRESSION_LABEL not in regression.numeric_features
    assert not [f for f in regression.numeric_features if "fans" in f]

    classification = pl.classification_task()
    assert CLASSIFICATION_LABEL not in classification.numeric_features
    assert not [f for f in classification.numeric_features if "elite" in f]

    for task in (regression, classification):
        assert "user_id" not in task.numeric_features


def test_preprocessing_learns_only_from_train(spark):
    task = pl.TaskSpec(
        name="toy", kind=pl.REGRESSION, label="y", numeric_features=("x",)
    )
    df = spark.createDataFrame(
        [
            (1.0, 1.0, "train"),
            (2.0, 2.0, "train"),
            (3.0, 3.0, "train"),
            # Huge test value must not move the imputed median or the scale.
            (1000.0, 0.0, "test"),
            (None, 0.0, "test"),
        ],
        "x double, y double, split string",
    )
    model = pl.preprocessing_pipeline(task).fit(df.filter("split = 'train'"))
    assert model.stages[0].surrogateDF.first()["x"] == 2.0
    scaler = model.stages[-1]
    assert scaler.mean.toArray()[0] == pytest.approx(2.0)
    assert scaler.std.toArray()[0] == pytest.approx(1.0)

    rows = sorted(
        r.features[0]
        for r in model.transform(df.filter("split = 'test'")).collect()
    )
    # null -> train median 2 -> (2 - 2) / 1 = 0; 1000 -> (1000 - 2) / 1.
    assert rows == pytest.approx([0.0, 998.0])
