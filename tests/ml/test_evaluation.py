import numpy as np
import pytest
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors

from src.ml import evaluation as ev


def test_metrics_are_for_the_positive_class():
    # 10 positives, 90 negatives; model finds 6 with 4 false alarms.
    m = ev.metrics_from_confusion(tp=6, fp=4, tn=86, fn=4)
    assert m["accuracy"] == pytest.approx(0.92)
    assert m["precision"] == pytest.approx(0.6)
    assert m["recall"] == pytest.approx(0.6)
    assert m["f1"] == pytest.approx(0.6)
    # The open class alone would score far higher, so the macro average
    # must lie strictly between the two.
    f1_open = 2 * 86 / (2 * 86 + 4 + 4)
    assert m["macro_f1"] == pytest.approx((0.6 + f1_open) / 2)


def test_majority_baseline_has_zero_positive_class_scores():
    m = ev.metrics_from_confusion(tp=0, fp=0, tn=80, fn=20)
    assert m["accuracy"] == pytest.approx(0.8)
    assert m["precision"] == m["recall"] == m["f1"] == 0.0


def test_spark_and_numpy_threshold_metrics_agree(spark):
    rng = np.random.default_rng(0)
    y = (rng.random(500) < 0.2).astype(float)
    pred = np.where(rng.random(500) < 0.8, y, 1 - y)
    df = spark.createDataFrame(
        [(float(a), float(b)) for a, b in zip(y, pred)],
        "label double, prediction double",
    )
    spark_m = ev.classification_metrics(df, "label", score_col=None)
    np_m = ev.np_threshold_metrics(y, pred)
    for key in np_m:
        assert spark_m[key] == pytest.approx(np_m[key])


def test_numpy_auc_matches_spark_evaluator(spark):
    rng = np.random.default_rng(1)
    y = (rng.random(400) < 0.25).astype(float)
    # Rounded scores create ties, the tricky case for curve construction.
    score = np.round(np.clip(y * 0.3 + rng.random(400) * 0.7, 0, 1), 2)
    df = spark.createDataFrame(
        [(float(a), Vectors.dense([1 - s, s])) for a, s in zip(y, score)],
        ["label", "rawPrediction"],
    )
    evaluator = BinaryClassificationEvaluator(labelCol="label")
    assert ev.roc_auc(y, score) == pytest.approx(
        evaluator.evaluate(df, {evaluator.metricName: "areaUnderROC"}), abs=1e-9
    )
    assert ev.pr_auc(y, score) == pytest.approx(
        evaluator.evaluate(df, {evaluator.metricName: "areaUnderPR"}), abs=1e-9
    )


def test_regression_metrics_spark_vs_numpy(spark):
    fans = np.array([0, 1, 3, 10, 100])
    pred_fans = np.array([0, 0, 4, 10, 50])
    y, pred = np.log1p(fans), np.log1p(pred_fans)
    df = spark.createDataFrame(
        [(float(a), float(b)) for a, b in zip(y, pred)],
        "log_fans double, prediction double",
    )
    spark_m = ev.regression_metrics(df, "log_fans")
    np_m = ev.np_regression_metrics(y, pred)
    for key in np_m:
        assert spark_m[key] == pytest.approx(np_m[key])
    # Back on the fan scale: |0| + |-1| + |1| + |0| + |-50| = 52 over 5 users.
    assert np_m["mae_fans"] == pytest.approx(52 / 5)


def test_best_threshold():
    y = np.array([0, 0, 0, 1, 1], dtype=float)
    score = np.array([0.1, 0.2, 0.6, 0.7, 0.9])
    # Any cut in (0.6, 0.7] separates the classes perfectly.
    assert 0.6 < ev.best_threshold(y, score) <= 0.7


def test_bootstrap_interval_brackets_the_estimate():
    rng = np.random.default_rng(3)
    y = rng.normal(3.5, 1.0, 500)
    pred = y + rng.normal(0, 0.7, 500)
    point = ev.np_regression_metrics(y, pred)["rmse"]
    low, high = ev.bootstrap(ev.np_regression_metrics, (y, pred))["rmse"]
    assert low < point < high
    assert high - low > 0.01


def test_paired_bootstrap_direction():
    rng = np.random.default_rng(4)
    y = rng.normal(0, 1, 300)
    good, bad = y + rng.normal(0, 0.1, 300), y + rng.normal(0, 1.0, 300)

    def rmse(t, p):
        return float(np.sqrt(np.mean((p - t) ** 2)))

    out = ev.paired_bootstrap_diff(rmse, y, good, bad, higher_is_better=False)
    assert out["diff"] < 0
    assert out["share_a_better"] == 1.0


def test_tree_auc_uses_probabilities_not_leaf_counts(spark):
    """
    A weighted decision tree's rawPrediction holds leaf class counts; AUC
    must rank by P(positive) like the NumPy curves and bootstrap CIs do.
    """
    from pyspark.ml.classification import DecisionTreeClassifier
    from pyspark.ml.feature import VectorAssembler

    from src.ml.pipeline import CLASSIFICATION, TaskSpec
    from src.ml.training import PROBABILITY_COL, predict, score

    rng = np.random.default_rng(5)
    x1, x2 = rng.random(3000), rng.random(3000)
    y = (rng.random(3000) < 0.1 + 0.6 * x1 * x2).astype(float)
    df = VectorAssembler(
        inputCols=["x1", "x2"], outputCol="features"
    ).transform(
        spark.createDataFrame(
            [
                (float(a), float(b), float(c), float(1.0 + 3.0 * c))
                for a, b, c in zip(x1, x2, y)
            ],
            "x1 double, x2 double, is_elite double, class_weight double",
        )
    )
    model = DecisionTreeClassifier(
        labelCol="is_elite", weightCol="class_weight", maxDepth=6, seed=1
    ).fit(df)
    task = TaskSpec(
        name="t",
        kind=CLASSIFICATION,
        label="is_elite",
        numeric_features=("x1",),
    )
    metrics = score(model, df, task)
    scored = predict(model, df, task).select("is_elite", PROBABILITY_COL)
    pdf = scored.toPandas()
    labels = pdf["is_elite"].to_numpy()
    probs = pdf[PROBABILITY_COL].to_numpy()
    assert metrics["roc_auc"] == pytest.approx(
        ev.roc_auc(labels, probs), abs=1e-9
    )
    assert metrics["pr_auc"] == pytest.approx(
        ev.pr_auc(labels, probs), abs=1e-9
    )
