"""
Training, model selection and training-process analysis.

Protocol (identical for every model family):

1. Every grid point is fitted on the TRAIN split; the fit time and the
   train and validation metrics are recorded.
2. The best grid point is chosen on VALIDATION (regression: lowest RMSE,
   classification: highest PR-AUC, which does not depend on a threshold).
3. That train-only model is scored once on TEST. There is no refit on
   train+validation, so the curves, the selected threshold and the test
   numbers all describe the same fitted model.

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
    GBT,
    GBT_MAX_ITER,
    LINEAR,
    ModelSpec,
    classification_models,
    regression_models,
)
from src.ml.pipeline import (
    BUCKET_COL,
    TRAIN_BUCKETS,
    WEIGHT_COL,
    PreparedData,
    TaskSpec,
    prepare,
)

# Lowest / highest possible star rating; regression output is clipped to it.
STAR_RANGE = (1.0, 5.0)

# Nested training subsets for the learning curve, as hash-bucket limits
# (TRAIN_BUCKETS = 70 is the full training split).
LEARNING_CURVE_BUCKETS = (4, 7, 17, 35, TRAIN_BUCKETS)

PROBABILITY_COL = "p_closed"


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def configure_spark_for_training(spark: SparkSession) -> None:
    """GBT checkpoints its growing lineage every ``checkpointInterval``."""
    ML_CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(ML_CHECKPOINT_DIR))


# ---------------------------------------------------------------------------
# Task-specific scoring
# ---------------------------------------------------------------------------


def _is_classification(task: TaskSpec) -> bool:
    return task.label == "is_closed"


def predict(model: Model, df: DataFrame, task: TaskSpec) -> DataFrame:
    """Model predictions; regression output clipped to the 1-5 star range."""
    out = model.transform(df)
    if _is_classification(task):
        return out.withColumn(
            PROBABILITY_COL, vector_to_array("probability").getItem(1)
        )
    return out.withColumn(
        "prediction",
        F.least(
            F.greatest("prediction", F.lit(STAR_RANGE[0])), F.lit(STAR_RANGE[1])
        ),
    )


def score(model: Model, df: DataFrame, task: TaskSpec) -> dict[str, float]:
    predictions = predict(model, df, task).cache()
    try:
        if _is_classification(task):
            return classification_metrics(predictions, task.label)
        return regression_metrics(predictions, task.label)
    finally:
        predictions.unpersist()


def selection_metric(task: TaskSpec) -> tuple[str, bool]:
    """(metric name, higher is better) used to pick hyperparameters."""
    if _is_classification(task):
        return "pr_auc", True
    return "rmse", False


def _is_better(task: TaskSpec, a: float, b: float | None) -> bool:
    _, higher = selection_metric(task)
    return b is None or (a > b if higher else a < b)


def collect_scores(model: Model, df: DataFrame, task: TaskSpec) -> pd.DataFrame:
    """Label, prediction (and P(closed)) plus diagnostics as pandas."""
    columns = [task.label, "prediction", "log_review_count", "state"]
    if _is_classification(task):
        columns.append(PROBABILITY_COL)
    return predict(model, df, task).select(*columns).toPandas()


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass
class FamilyResult:
    """Everything recorded for one model family on one task."""

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


@dataclass
class TaskResult:
    task: TaskSpec
    data: PreparedData
    families: list[FamilyResult]
    baselines: dict[str, dict[str, float]] = field(default_factory=dict)
    learning_curve: list[dict[str, Any]] = field(default_factory=list)
    extras: dict[str, Any] = field(default_factory=dict)
    split_summary: list[dict[str, Any]] = field(default_factory=list)

    @property
    def best(self) -> FamilyResult:
        name, higher = selection_metric(self.task)
        key = (
            (lambda f: f.validation_metrics[name])
            if higher
            else (lambda f: -f.validation_metrics[name])
        )
        return max(self.families, key=key)


# ---------------------------------------------------------------------------
# Fitting one family
# ---------------------------------------------------------------------------


def _timed_fit(estimator, df: DataFrame):
    start = time.perf_counter()
    model = estimator.fit(df)
    return model, time.perf_counter() - start


def _unweighted(df: DataFrame) -> DataFrame:
    """Evaluation view: weight 1 for every row (GBT loss reads weightCol)."""
    return df.withColumn(WEIGHT_COL, F.lit(1.0))


def _gbt_curves(model, data: PreparedData, task: TaskSpec) -> dict[str, list]:
    """Per-iteration train/validation loss of a fitted GBT model."""
    train, val = _unweighted(data.train), _unweighted(data.validation)
    if _is_classification(task):
        # Spark's GBT log-loss: 2*log(1 + exp(-2*y*F)), y in {-1, 1}.
        return {
            "train": list(model.evaluateEachIteration(train)),
            "validation": list(model.evaluateEachIteration(val)),
            "loss": "log-loss",
        }
    # "squared" returns the MSE after each iteration -> RMSE.
    return {
        "train": [
            math.sqrt(v) for v in model.evaluateEachIteration(train, "squared")
        ],
        "validation": [
            math.sqrt(v) for v in model.evaluateEachIteration(val, "squared")
        ],
        "loss": "rmse",
    }


def _importances(model, names: list[str]) -> list[tuple[str, float]]:
    if hasattr(model, "featureImportances"):
        values = model.featureImportances.toArray()
    elif hasattr(model, "coefficients"):
        # Features are standardised, so coefficients are comparable.
        values = model.coefficients.toArray()
    else:
        return []
    pairs = list(zip(names, (float(v) for v in values)))
    return sorted(pairs, key=lambda p: abs(p[1]), reverse=True)


def fit_family(spec: ModelSpec, data: PreparedData) -> FamilyResult:
    """Fit every grid point, keep the best on validation, score it on test."""
    task = data.task
    metric, _ = selection_metric(task)
    trials: list[dict[str, Any]] = []
    best: tuple | None = None  # (val metric, params, model, fit time)
    curves: dict[str, Any] = {}

    for params in spec.grid:
        model, seconds = _timed_fit(spec.make(params), data.train)
        params = dict(params)
        # Total fitting work for this grid point (GBT may fit twice).
        search_seconds = seconds

        if spec.key == GBT:
            fitted_iters = params.get("maxIter", GBT_MAX_ITER)
            gbt_curve = _gbt_curves(model, data, task)
            best_iter = int(np.argmin(gbt_curve["validation"])) + 1
            name = f"maxDepth={params['maxDepth']}, step={params['stepSize']}"
            curves.setdefault("gbt_iterations", {})[name] = {
                **gbt_curve,
                "best_iter": best_iter,
                # Minimum at the budget edge: still improving, not converged.
                "censored": best_iter == fitted_iters,
            }
            params["maxIter"] = best_iter
            if best_iter < fitted_iters:
                # GBT without subsampling is deterministic: the refit is the
                # same sequence of trees, stopped at the best iteration.
                model, seconds = _timed_fit(spec.make(params), data.train)
                search_seconds += seconds

        train_m = score(model, _unweighted(data.train), task)
        val_m = score(model, data.validation, task)
        trials.append(
            {
                "model": spec.name,
                **{k: params[k] for k in params},
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
    test_m = score(model, data.test, task)

    if spec.key == LINEAR:
        curves["objective_history"] = list(model.summary.objectiveHistory)

    result = FamilyResult(
        spec=spec,
        trials=trials,
        best_params=params,
        model=model,
        fit_seconds=seconds,
        train_metrics=train_m,
        validation_metrics=val_m,
        test_metrics=test_m,
        curves=curves,
        importances=_importances(model, data.feature_names),
        validation_scores=collect_scores(model, data.validation, task),
        test_scores=collect_scores(model, data.test, task),
    )
    result.test_ci = test_confidence_intervals(result.test_scores, task)
    _log(
        f"  -> best {spec.name} {params}: val {metric}={val_m[metric]:.4f}, "
        f"test {metric}={test_m[metric]:.4f}"
    )
    return result


# ---------------------------------------------------------------------------
# Uncertainty
# ---------------------------------------------------------------------------


def _np_classification(y, pred, p):
    return {
        **np_threshold_metrics(y, pred),
        "pr_auc": pr_auc(y, p),
        "roc_auc": roc_auc(y, p),
    }


def test_confidence_intervals(scores: pd.DataFrame, task: TaskSpec) -> dict:
    """Bootstrap 95% intervals of the test metrics (1000 row resamples)."""
    y = scores[task.label].to_numpy(dtype=float)
    pred = scores["prediction"].to_numpy(dtype=float)
    if _is_classification(task):
        p = scores[PROBABILITY_COL].to_numpy(dtype=float)
        return bootstrap(_np_classification, (y, pred, p))
    return bootstrap(np_regression_metrics, (y, pred))


def compare_top_two(result: TaskResult) -> dict[str, Any]:
    """Paired bootstrap of the test-metric difference of the two best models."""
    metric, higher = selection_metric(result.task)
    ranked = sorted(
        result.families,
        key=lambda f: f.validation_metrics[metric],
        reverse=higher,
    )
    a, b = ranked[0], ranked[1]
    y = a.test_scores[result.task.label].to_numpy(dtype=float)
    if _is_classification(result.task):
        pa = a.test_scores[PROBABILITY_COL].to_numpy(dtype=float)
        pb = b.test_scores[PROBABILITY_COL].to_numpy(dtype=float)
        diff = paired_bootstrap_diff(pr_auc, y, pa, pb, higher_is_better=True)
        name = "pr_auc"
    else:
        pa = a.test_scores["prediction"].to_numpy(dtype=float)
        pb = b.test_scores["prediction"].to_numpy(dtype=float)

        def rmse(y_, p_):
            return float(np.sqrt(np.mean((p_ - y_) ** 2)))

        diff = paired_bootstrap_diff(rmse, y, pa, pb, higher_is_better=False)
        name = "rmse"
    return {"a": a.spec.name, "b": b.spec.name, "metric": name, **diff}


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------


def regression_baselines(data: PreparedData) -> dict[str, dict[str, float]]:
    """Train mean, and mean stars per (state, first category) from train."""
    label = data.task.label
    mean = data.train.agg(F.avg(label)).first()[0]
    # F.get: NULL for businesses without categories (element_at throws).
    test = data.test.withColumn("primary_category", F.get("category_list", 0))
    train = data.train.withColumn("primary_category", F.get("category_list", 0))
    by_group = (
        train.groupBy("state", "primary_category")
        .agg(F.avg(label).alias("group_mean"), F.count(F.lit(1)).alias("n"))
        .filter(F.col("n") >= 5)
    )
    by_state = train.groupBy("state").agg(F.avg(label).alias("state_mean"))
    group_pred = (
        test.join(by_group, ["state", "primary_category"], "left")
        .join(by_state, "state", "left")
        .withColumn(
            "prediction",
            F.coalesce("group_mean", "state_mean", F.lit(mean)),
        )
    )
    return {
        "Baseline: train mean": regression_metrics(
            test.withColumn("prediction", F.lit(mean)), label
        ),
        "Baseline: mean by state x category": regression_metrics(
            group_pred, label
        ),
    }


def classification_baselines(data: PreparedData) -> dict[str, dict[str, float]]:
    """Majority class, and (if recency is used) a one-feature threshold rule."""
    label = data.task.label
    baselines = {
        "Baseline: majority (all open)": classification_metrics(
            data.test.withColumn("prediction", F.lit(0.0)),
            label,
            score_col=None,
        )
    }
    if "days_since_last_review" in data.task.numeric_features:
        val = data.validation.select(label, "days_since_last_review").toPandas()
        y = val[label].to_numpy(dtype=float)
        days = val["days_since_last_review"].to_numpy(dtype=float)
        # Cut-off chosen on validation by F1 over a grid of day quantiles.
        candidates = np.unique(np.quantile(days, np.linspace(0.5, 0.995, 100)))
        cut_days = max(
            candidates,
            key=lambda c: np_threshold_metrics(y, (days >= c) * 1.0)["f1"],
        )
        rule = data.test.withColumn(
            "prediction",
            (F.col("days_since_last_review") >= cut_days).cast("double"),
        )
        baselines[f"Baseline: days since last review >= {cut_days:.0f}"] = (
            classification_metrics(rule, label, score_col=None)
        )
    return baselines


# ---------------------------------------------------------------------------
# Training-process extras
# ---------------------------------------------------------------------------


def learning_curve(
    best: FamilyResult, data: PreparedData
) -> list[dict[str, Any]]:
    """Best family/params refitted on nested fractions of the train split."""
    rows = []
    for limit in LEARNING_CURVE_BUCKETS:
        subset = data.train.filter(F.col(BUCKET_COL) < limit).cache()
        n = subset.count()
        model, seconds = _timed_fit(best.spec.make(best.best_params), subset)
        train_m = score(model, _unweighted(subset), data.task)
        val_m = score(model, data.validation, data.task)
        subset.unpersist()
        rows.append(
            {
                "train_fraction": limit / TRAIN_BUCKETS,
                "train_rows": n,
                "fit_seconds": round(seconds, 2),
                **{f"train_{k}": v for k, v in train_m.items()},
                **{f"val_{k}": v for k, v in val_m.items()},
            }
        )
        _log(f"  learning curve {limit / TRAIN_BUCKETS:.0%} ({n} rows) done")
    return rows


def threshold_analysis(best: FamilyResult, task: TaskSpec) -> dict[str, Any]:
    """Pick the F1-optimal threshold on validation and apply it to test."""
    val, test = best.validation_scores, best.test_scores
    t = best_threshold(
        val[task.label].to_numpy(dtype=float),
        val[PROBABILITY_COL].to_numpy(dtype=float),
    )
    y = test[task.label].to_numpy(dtype=float)
    p = test[PROBABILITY_COL].to_numpy(dtype=float)
    return {
        "threshold": t,
        "test_at_0.5": np_threshold_metrics(y, (p >= 0.5) * 1.0),
        "test_at_tuned": np_threshold_metrics(y, (p >= t) * 1.0),
    }


def class_weight_comparison(
    best: FamilyResult, data: PreparedData
) -> list[dict[str, Any]]:
    """{weighted, unweighted} x {threshold 0.5, tuned on validation}."""
    task = data.task
    unweighted_spec = next(
        s
        for s in classification_models(task.label, weighted=False)
        if s.key == best.spec.key
    )
    model, _ = _timed_fit(unweighted_spec.make(best.best_params), data.train)
    rows = []
    for name, val_s, test_s in [
        ("weighted", best.validation_scores, best.test_scores),
        (
            "unweighted",
            collect_scores(model, data.validation, task),
            collect_scores(model, data.test, task),
        ),
    ]:
        t = best_threshold(
            val_s[task.label].to_numpy(dtype=float),
            val_s[PROBABILITY_COL].to_numpy(dtype=float),
        )
        y = test_s[task.label].to_numpy(dtype=float)
        p = test_s[PROBABILITY_COL].to_numpy(dtype=float)
        for label, threshold in [("0.5", 0.5), ("tuned", t)]:
            rows.append(
                {
                    "class_weights": name,
                    "threshold": threshold,
                    "threshold_kind": label,
                    **np_threshold_metrics(y, (p >= threshold) * 1.0),
                    "pr_auc": pr_auc(y, p),
                }
            )
    return rows


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
    features: DataFrame,
    task: TaskSpec,
    specs: list[ModelSpec],
    with_learning_curve: bool = True,
) -> TaskResult:
    _log(f"Task {task.name}: preparing data")
    data = prepare(features, task)
    _log(
        f"Task {task.name}: {len(data.feature_names)} features, "
        f"train/val/test = {data.train.count()}/{data.validation.count()}/"
        f"{data.test.count()}"
    )
    families = []
    for spec in specs:
        _log(
            f"Task {task.name}: tuning {spec.name} ({len(spec.grid)} settings)"
        )
        families.append(fit_family(spec, data))

    result = TaskResult(task=task, data=data, families=families)
    result.split_summary = split_summary(data)
    if with_learning_curve:
        _log(f"Task {task.name}: learning curve for {result.best.spec.name}")
        result.learning_curve = learning_curve(result.best, data)
    result.extras["top_two"] = compare_top_two(result)
    return result


def run_regression(features: DataFrame, quick: bool = False) -> TaskResult:
    from src.ml.pipeline import regression_profile_task, regression_task

    task = regression_task()
    specs = regression_models(task.label)
    if quick:
        specs = _quick(specs)
    result = run_task(features, task, specs, with_learning_curve=not quick)
    result.baselines = regression_baselines(result.data)

    # Ablation: best family/params using only the listing's own profile.
    profile = regression_profile_task()
    _log("Ablation: profile-only features")
    pdata = prepare(features, profile)
    best = result.best
    spec = next(
        s for s in regression_models(profile.label) if s.key == best.spec.key
    )
    model, seconds = _timed_fit(spec.make(best.best_params), pdata.train)
    result.extras["profile_only"] = {
        "model": spec.name,
        "params": best.best_params,
        "n_features": len(pdata.feature_names),
        "fit_seconds": seconds,
        "validation": score(model, pdata.validation, profile),
        "test": score(model, pdata.test, profile),
    }
    release(pdata)
    return result


def run_classification(
    features: DataFrame,
    include_recency: bool,
    quick: bool = False,
    reuse_params_from: TaskResult | None = None,
) -> TaskResult:
    """
    Classify closed businesses.

    The primary run (no recency features) searches the full grids. The
    "+recency" variant can reuse each family's selected hyperparameters
    (``reuse_params_from``) so it costs one fit per family; GBT still picks
    its own number of iterations from a fresh GBT_MAX_ITER-iteration curve.
    """
    from src.ml.pipeline import classification_task

    task = classification_task(include_recency=include_recency)
    specs = classification_models(task.label)
    if reuse_params_from is not None:
        chosen = {f.spec.key: f.best_params for f in reuse_params_from.families}
        for spec in specs:
            params = dict(chosen[spec.key])
            if spec.key == GBT:
                params.pop("maxIter", None)
                if quick:
                    params["maxIter"] = QUICK_GBT_ITER
            spec.grid = [params]
    elif quick:
        specs = _quick(specs)
    learning = not quick and reuse_params_from is None
    result = run_task(features, task, specs, with_learning_curve=learning)
    result.baselines = classification_baselines(result.data)
    best = result.best
    result.extras["threshold"] = threshold_analysis(best, task)
    if not quick and reuse_params_from is None:
        _log("Class weights vs. threshold comparison")
        result.extras["class_weights"] = class_weight_comparison(
            best, result.data
        )
    return result


# Development mode: boosting iterations per GBT fit.
QUICK_GBT_ITER = 50


def _quick(specs: list[ModelSpec]) -> list[ModelSpec]:
    """Development mode: two settings per family, one short GBT."""
    for spec in specs:
        if spec.key == GBT:
            spec.grid = [
                {"maxDepth": 3, "stepSize": 0.3, "maxIter": QUICK_GBT_ITER}
            ]
        else:
            spec.grid = [spec.grid[0], spec.grid[len(spec.grid) // 2]]
    return specs


def release(data: PreparedData) -> None:
    for df in (data.train, data.validation, data.test):
        df.unpersist()
