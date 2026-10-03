"""
Training, model selection and training-process analysis.

Protocol (identical for every model):

1. Every grid point is fitted on the TRAIN split; the fit time and the
   train and validation metrics are recorded.
2. The best grid point is chosen on VALIDATION (regression: lowest RMSE,
   classification: highest PR-AUC, which does not depend on a threshold).
3. That train-only model is scored once on TEST. There is no refit on
   train+validation, so the curves, the selected threshold and the test
   numbers all describe the same fitted model.

Classification: elite users are 4.6% of the data. Instead of class weights
(the neural network does not support them) every classifier gets its own
decision threshold, chosen on VALIDATION by maximising F1, and applied
unchanged to TEST. Metrics at the default 0.5 are reported too.

GBT: one long fit per depth is scored after every boosting iteration on train
and validation (``evaluateEachIteration``); the iteration with the lowest
validation loss becomes ``maxIter`` of the selected model.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from pyspark.ml import Model
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.constants import ML_CHECKPOINT_DIR
from src.ml.evaluation import (
    best_threshold,
    bootstrap,
    classification_metrics,
    np_regression_metrics,
    np_threshold_metrics,
    paired_bootstrap_diff,
    pr_auc,
    regression_metrics,
    roc_auc,
)
from src.ml.models import (
    FM,
    GBT,
    GBT_MAX_ITER,
    LINEAR,
    LOGISTIC,
    MLP,
    RANDOM_FOREST,
    ModelSpec,
    classification_models,
    regression_models,
)
from src.ml.pipeline import (
    BUCKET_COL,
    TRAIN_BUCKETS,
    PreparedData,
    TaskSpec,
    classification_task,
    prepare,
    regression_task,
)

# Nested training subsets for the learning curve, as hash-bucket limits.
# The full training split (TRAIN_BUCKETS) is the selected model itself.
LEARNING_CURVE_BUCKETS = (1, 3, 7, 17, 35)

PROBABILITY_COL = "p_positive"


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def configure_spark_for_training(spark: SparkSession) -> None:
    """GBT checkpoints its growing lineage every ``checkpointInterval``."""
    ML_CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(ML_CHECKPOINT_DIR))


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def predict(model: Model, df: DataFrame, task: TaskSpec) -> DataFrame:
    """Predictions; P(positive) for classifiers, log fans clipped at 0."""
    out = model.transform(df)
    if task.is_classification:
        return out.withColumn(
            PROBABILITY_COL, vector_to_array("probability").getItem(1)
        )
    # log(1 + fans) can not be negative.
    return out.withColumn("prediction", F.greatest("prediction", F.lit(0.0)))


def score(model: Model, df: DataFrame, task: TaskSpec) -> dict[str, float]:
    predictions = predict(model, df, task).cache()
    try:
        if task.is_classification:
            return classification_metrics(predictions, task.label)
        return regression_metrics(predictions, task.label)
    finally:
        predictions.unpersist()


def selection_metric(task: TaskSpec) -> tuple[str, bool]:
    """(metric name, higher is better) used to pick hyperparameters."""
    if task.is_classification:
        return "pr_auc", True
    return "rmse", False


def _is_better(task: TaskSpec, a: float, b: float | None) -> bool:
    _, higher = selection_metric(task)
    return b is None or (a > b if higher else a < b)


def collect_scores(model: Model, df: DataFrame, task: TaskSpec) -> pd.DataFrame:
    """Label, prediction (and P(positive)) plus raw counts, as pandas."""
    columns = [task.label, "prediction", "fans", "review_count"]
    if task.is_classification:
        columns.append(PROBABILITY_COL)
    return predict(model, df, task).select(*columns).toPandas()


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass
class ModelResult:
    """Everything recorded for one model on one task."""

    spec: ModelSpec
    trials: list[dict[str, Any]]
    best_params: dict[str, Any]
    model: Model
    fit_seconds: float
    train_metrics: dict[str, float]
    validation_metrics: dict[str, float]
    test_metrics: dict[str, float]
    curves: dict[str, Any] = field(default_factory=dict)
    importances: list[tuple[str, float]] = field(default_factory=list)
    validation_scores: pd.DataFrame | None = None
    test_scores: pd.DataFrame | None = None
    test_ci: dict[str, tuple[float, float]] = field(default_factory=dict)
    # Classification only: threshold chosen on validation and the test
    # metrics at that threshold (the headline numbers).
    threshold: float | None = None
    test_tuned: dict[str, float] = field(default_factory=dict)


@dataclass
class TaskResult:
    task: TaskSpec
    data: PreparedData
    models: list[ModelResult]
    baselines: dict[str, dict[str, float]] = field(default_factory=dict)
    learning_curve: list[dict[str, Any]] = field(default_factory=list)
    extras: dict[str, Any] = field(default_factory=dict)
    split_summary: list[dict[str, Any]] = field(default_factory=list)

    @property
    def best(self) -> ModelResult:
        name, higher = selection_metric(self.task)
        sign = 1 if higher else -1
        return max(self.models, key=lambda m: sign * m.validation_metrics[name])


# ---------------------------------------------------------------------------
# Fitting one model
# ---------------------------------------------------------------------------


def _timed_fit(estimator, df: DataFrame):
    start = time.perf_counter()
    model = estimator.fit(df)
    return model, time.perf_counter() - start


def _gbt_curves(model, data: PreparedData) -> dict[str, list]:
    """Per-iteration train/validation RMSE of a fitted GBT regressor."""

    def rmse(df):
        return [
            math.sqrt(v) for v in model.evaluateEachIteration(df, "squared")
        ]

    return {"train": rmse(data.train), "validation": rmse(data.validation)}


def _importances(model, names: list[str]) -> list[tuple[str, float]]:
    """Tree importances, or coefficients on standardised features."""
    if hasattr(model, "featureImportances"):
        values = model.featureImportances.toArray()
    elif hasattr(model, "coefficients"):
        values = model.coefficients.toArray()
    elif hasattr(model, "linear"):  # factorization machine: linear part
        values = model.linear.toArray()
    else:  # neural network: no per-feature weights to report
        return []
    pairs = list(zip(names, (float(v) for v in values)))
    return sorted(pairs, key=lambda p: abs(p[1]), reverse=True)


def _objective_history(model) -> list[float]:
    """Loss per optimiser iteration from the model's training summary."""
    try:
        summary = model.summary
        # PySpark exposes ``summary`` as a property on Linear/Logistic
        # models but as a method on MultilayerPerceptron.
        if callable(summary):
            summary = summary()
        history = [float(v) for v in summary.objectiveHistory]
    except Exception:  # no training summary for this model type
        return []
    # Tree ensembles report a placeholder history of [0.0].
    return history if len(history) > 1 else []


def fit_model(spec: ModelSpec, data: PreparedData) -> ModelResult:
    """Fit every grid point, keep the best on validation, score it on test."""
    task = data.task
    metric, _ = selection_metric(task)
    trials: list[dict[str, Any]] = []
    best: tuple | None = None
    curves: dict[str, Any] = {}

    for params in spec.grid:
        model, seconds = _timed_fit(spec.make(params), data.train)
        params = dict(params)
        # Total fitting work for this grid point (GBT may fit twice).
        search_seconds = seconds

        if spec.key == GBT:
            fitted_iters = params.get("maxIter", GBT_MAX_ITER)
            curve = _gbt_curves(model, data)
            best_iter = int(np.argmin(curve["validation"])) + 1
            curves.setdefault("gbt_iterations", {})[
                f"maxDepth={params['maxDepth']}"
            ] = {
                **curve,
                "best_iter": best_iter,
                # Minimum at the budget edge: still improving.
                "censored": best_iter == fitted_iters,
            }
            params["maxIter"] = best_iter
            if best_iter < fitted_iters:
                # GBT without subsampling is deterministic: the refit is the
                # same sequence of trees, stopped at the best iteration.
                model, seconds = _timed_fit(spec.make(params), data.train)
                search_seconds += seconds

        history = _objective_history(model)
        if history:
            curves.setdefault("objective_history", {})[
                _params_label(params)
            ] = history

        train_m = score(model, data.train, task)
        val_m = score(model, data.validation, task)
        trials.append(
            {
                "model": spec.name,
                **params,
                "fit_seconds": round(seconds, 2),
                "search_seconds": round(search_seconds, 2),
                **{f"train_{k}": v for k, v in train_m.items()},
                **{f"val_{k}": v for k, v in val_m.items()},
            }
        )
        _log(
            f"  {spec.name} {params} fit {seconds:.1f}s "
            f"train {metric}={train_m[metric]:.4f} "
            f"val {metric}={val_m[metric]:.4f}"
        )
        if best is None or _is_better(task, val_m[metric], best[0]):
            best = (val_m[metric], params, model, seconds, train_m, val_m)

    _, params, model, seconds, train_m, val_m = best
    result = ModelResult(
        spec=spec,
        trials=trials,
        best_params=params,
        model=model,
        fit_seconds=seconds,
        train_metrics=train_m,
        validation_metrics=val_m,
        test_metrics=score(model, data.test, task),
        curves=curves,
        importances=_importances(model, data.feature_names),
        validation_scores=collect_scores(model, data.validation, task),
        test_scores=collect_scores(model, data.test, task),
    )
    if task.is_classification:
        _tune_threshold(result, task)
    result.test_ci = confidence_intervals(result, task)
    _log(
        f"  -> best {spec.name} {params}: val {metric}="
        f"{val_m[metric]:.4f}, test {metric}={result.test_metrics[metric]:.4f}"
    )
    return result


def _params_label(params: dict[str, Any]) -> str:
    return ", ".join(f"{k}={v}" for k, v in params.items())


def _tune_threshold(result: ModelResult, task: TaskSpec) -> None:
    """F1-optimal threshold on validation, applied unchanged to test."""
    val, test = result.validation_scores, result.test_scores
    result.threshold = best_threshold(
        val[task.label].to_numpy(dtype=float),
        val[PROBABILITY_COL].to_numpy(dtype=float),
    )
    y = test[task.label].to_numpy(dtype=float)
    p = test[PROBABILITY_COL].to_numpy(dtype=float)
    result.test_tuned = {
        **np_threshold_metrics(y, (p >= result.threshold) * 1.0),
        "roc_auc": result.test_metrics["roc_auc"],
        "pr_auc": result.test_metrics["pr_auc"],
    }


# ---------------------------------------------------------------------------
# Uncertainty
# ---------------------------------------------------------------------------


def confidence_intervals(result: ModelResult, task: TaskSpec) -> dict:
    """Bootstrap 95% intervals of the test metrics (row resamples)."""
    s = result.test_scores
    y = s[task.label].to_numpy(dtype=float)
    if not task.is_classification:
        pred = s["prediction"].to_numpy(dtype=float)
        return bootstrap(np_regression_metrics, (y, pred))

    p = s[PROBABILITY_COL].to_numpy(dtype=float)
    threshold = result.threshold

    def metrics(y_, p_):
        return {
            **np_threshold_metrics(y_, (p_ >= threshold) * 1.0),
            "pr_auc": pr_auc(y_, p_),
            "roc_auc": roc_auc(y_, p_),
        }

    return bootstrap(metrics, (y, p))


def compare_top_two(result: TaskResult) -> dict[str, Any]:
    """Paired bootstrap of the test-metric difference of the two best."""
    metric, higher = selection_metric(result.task)
    ranked = sorted(
        result.models,
        key=lambda m: m.validation_metrics[metric],
        reverse=higher,
    )
    a, b = ranked[0], ranked[1]
    y = a.test_scores[result.task.label].to_numpy(dtype=float)
    column = PROBABILITY_COL if result.task.is_classification else "prediction"
    pa = a.test_scores[column].to_numpy(dtype=float)
    pb = b.test_scores[column].to_numpy(dtype=float)

    def rmse(y_, p_):
        return float(np.sqrt(np.mean((p_ - y_) ** 2)))

    fn = pr_auc if result.task.is_classification else rmse
    diff = paired_bootstrap_diff(fn, y, pa, pb, higher_is_better=higher)
    return {"a": a.spec.name, "b": b.spec.name, "metric": metric, **diff}


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------


def regression_baselines(data: PreparedData) -> dict[str, dict[str, float]]:
    """Train mean of log fans, and 'nobody has fans' (the median user)."""
    label = data.task.label
    mean = data.train.agg(F.avg(label)).first()[0]
    return {
        "Baseline: train mean": regression_metrics(
            data.test.withColumn("prediction", F.lit(mean)), label
        ),
        "Baseline: 0 fans (median)": regression_metrics(
            data.test.withColumn("prediction", F.lit(0.0)), label
        ),
    }


def classification_baselines(data: PreparedData) -> dict[str, dict[str, float]]:
    """Majority class, and 'elite if review_count >= N' (N on validation)."""
    label = data.task.label
    val = data.validation.select(label, "review_count").toPandas()
    y = val[label].to_numpy(dtype=float)
    reviews = val["review_count"].to_numpy(dtype=float)
    candidates = np.unique(np.quantile(reviews, np.linspace(0.5, 0.999, 200)))
    cut = max(
        candidates,
        key=lambda c: np_threshold_metrics(y, (reviews >= c) * 1.0)["f1"],
    )
    rule = data.test.withColumn(
        "prediction", (F.col("review_count") >= cut).cast("double")
    )
    return {
        "Baseline: majority (nobody elite)": classification_metrics(
            data.test.withColumn("prediction", F.lit(0.0)),
            label,
            score_col=None,
        ),
        f"Baseline: review_count >= {cut:.0f}": classification_metrics(
            rule, label, score_col=None
        ),
    }


# ---------------------------------------------------------------------------
# Training-process extras
# ---------------------------------------------------------------------------


def learning_curve(best: ModelResult, data: PreparedData) -> list[dict]:
    """Best model/params refitted on nested fractions of the train split."""
    task = data.task
    rows = []
    for limit in LEARNING_CURVE_BUCKETS:
        subset = data.train.filter(F.col(BUCKET_COL) < limit).cache()
        n = subset.count()
        model, seconds = _timed_fit(best.spec.make(best.best_params), subset)
        rows.append(
            _curve_row(
                limit,
                n,
                seconds,
                score(model, subset, task),
                score(model, data.validation, task),
            )
        )
        subset.unpersist()
        _log(f"  learning curve {limit / TRAIN_BUCKETS:.1%} ({n} rows) done")
    # The full training split is the selected model itself.
    rows.append(
        _curve_row(
            TRAIN_BUCKETS,
            data.train.count(),
            best.fit_seconds,
            best.train_metrics,
            best.validation_metrics,
        )
    )
    return rows


def _curve_row(limit, n, seconds, train_m, val_m) -> dict[str, Any]:
    return {
        "train_fraction": limit / TRAIN_BUCKETS,
        "train_rows": n,
        "fit_seconds": round(seconds, 2),
        **{f"train_{k}": v for k, v in train_m.items()},
        **{f"val_{k}": v for k, v in val_m.items()},
    }


def split_summary(data: PreparedData) -> list[dict[str, Any]]:
    label = data.task.label
    rows = []
    for name, df in [
        ("train", data.train),
        ("validation", data.validation),
        ("test", data.test),
    ]:
        row = df.agg(
            F.count(F.lit(1)).alias("rows"), F.avg(label).alias("mean")
        ).first()
        rows.append(
            {"split": name, "rows": row["rows"], f"mean_{label}": row["mean"]}
        )
    return rows


# ---------------------------------------------------------------------------
# Task drivers
# ---------------------------------------------------------------------------


def run_task(
    features: DataFrame, task: TaskSpec, specs: list[ModelSpec], quick: bool
) -> TaskResult:
    _log(f"Task {task.name}: preparing data")
    data = prepare(features, task)
    _log(
        f"Task {task.name}: {len(data.feature_names)} features, "
        f"train/val/test = {data.train.count()}/{data.validation.count()}/"
        f"{data.test.count()}"
    )
    models = []
    for spec in specs:
        _log(
            f"Task {task.name}: tuning {spec.name} ({len(spec.grid)} settings)"
        )
        models.append(fit_model(spec, data))

    result = TaskResult(task=task, data=data, models=models)
    result.split_summary = split_summary(data)
    if not quick:
        _log(f"Task {task.name}: learning curve for {result.best.spec.name}")
        result.learning_curve = learning_curve(result.best, data)
    result.extras["top_two"] = compare_top_two(result)
    return result


def run_regression(features: DataFrame, quick: bool = False) -> TaskResult:
    task = regression_task()
    specs = regression_models(task.label)
    result = run_task(features, task, _quick(specs) if quick else specs, quick)
    result.baselines = regression_baselines(result.data)
    return result


def run_classification(features: DataFrame, quick: bool = False) -> TaskResult:
    task = classification_task()
    specs = classification_models(task.label, len(task.numeric_features))
    result = run_task(features, task, _quick(specs) if quick else specs, quick)
    result.baselines = classification_baselines(result.data)
    return result


# Development mode: one short setting per model.
QUICK_SETTINGS = {
    LINEAR: {"regParam": 0.01},
    FM: {"stepSize": 0.01, "maxIter": 10},
    GBT: {"maxDepth": 3, "maxIter": 20},
    LOGISTIC: {"regParam": 0.01},
    RANDOM_FOREST: {"numTrees": 10, "maxDepth": 8},
    MLP: {"hidden": [16], "maxIter": 20},
}


def _quick(specs: list[ModelSpec]) -> list[ModelSpec]:
    for spec in specs:
        spec.grid = [QUICK_SETTINGS[spec.key]]
    return specs


def release(data: PreparedData) -> None:
    for df in (data.train, data.validation, data.test):
        df.unpersist()
