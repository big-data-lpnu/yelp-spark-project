"""Quality metrics for the regression and classification models."""

from __future__ import annotations

import numpy as np
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

POSITIVE_LABEL = 1.0


def regression_metrics(
    predictions: DataFrame, label_col: str, prediction_col: str = "prediction"
) -> dict[str, float]:
    """
    RMSE, R² and MAE on the model's scale (log(1 + fans)), plus MAE and RMSE
    after converting back to fan counts (expm1) for interpretation.
    """
    evaluator = RegressionEvaluator(
        labelCol=label_col, predictionCol=prediction_col
    )
    error = F.expm1(F.col(prediction_col)) - F.expm1(F.col(label_col))
    row = predictions.select(
        F.avg(F.abs(error)).alias("mae_fans"),
        F.sqrt(F.avg(error * error)).alias("rmse_fans"),
    ).first()
    return {
        "rmse": evaluator.evaluate(predictions, {evaluator.metricName: "rmse"}),
        "r2": evaluator.evaluate(predictions, {evaluator.metricName: "r2"}),
        "mae": evaluator.evaluate(predictions, {evaluator.metricName: "mae"}),
        "mae_fans": float(row["mae_fans"]),
        "rmse_fans": float(row["rmse_fans"]),
    }


def confusion_counts(
    predictions: DataFrame, label_col: str, prediction_col: str = "prediction"
) -> dict[str, int]:
    """TP/FP/TN/FN with label 1.0 as the positive class."""
    row = predictions.select(
        *[
            F.sum(
                (
                    (F.col(label_col) == actual)
                    & (F.col(prediction_col) == predicted)
                ).cast("long")
            ).alias(name)
            for name, actual, predicted in [
                ("tp", 1.0, 1.0),
                ("fp", 0.0, 1.0),
                ("tn", 0.0, 0.0),
                ("fn", 1.0, 0.0),
            ]
        ]
    ).first()
    return {k: int(row[k] or 0) for k in ("tp", "fp", "tn", "fn")}


def _prf(tp: float, fp: float, fn: float) -> tuple[float, float, float]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall
        else 0.0
    )
    return precision, recall, f1


def metrics_from_confusion(tp: int, fp: int, tn: int, fn: int) -> dict:
    """
    Accuracy and Precision/Recall/F1 of the positive class (closed), plus
    macro and support-weighted F1 over both classes.

    Spark's MulticlassClassificationEvaluator defaults to metricLabel=0 and a
    *weighted* "f1", which would silently describe the majority class; all
    numbers here come from one confusion matrix instead.
    """
    total = tp + fp + tn + fn
    precision, recall, f1 = _prf(tp, fp, fn)
    # The negative class seen as "positive": its TP=tn, FP=fn, FN=fp.
    _, _, f1_neg = _prf(tn, fn, fp)
    n_pos, n_neg = tp + fn, tn + fp
    return {
        "accuracy": (tp + tn) / total if total else 0.0,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "macro_f1": (f1 + f1_neg) / 2,
        "weighted_f1": (f1 * n_pos + f1_neg * n_neg) / total if total else 0.0,
    }


def classification_metrics(
    predictions: DataFrame,
    label_col: str,
    prediction_col: str = "prediction",
    score_col: str | None = "probability",
) -> dict[str, float]:
    """
    Threshold metrics from the confusion matrix plus ROC-AUC / PR-AUC when the
    model exposes scores.

    AUCs rank rows by P(closed) (``probability``), not ``rawPrediction``:
    for a decision tree the raw column holds the leaf's weighted class
    counts, which do not order rows by their probability. ``numBins=0``
    uses the exact curve (no down-sampling), as the NumPy helpers do.

    No sample weights are used: class weights only steer training, the
    evaluation data is scored as it is distributed in reality.
    """
    counts = confusion_counts(predictions, label_col, prediction_col)
    metrics: dict[str, float] = {
        **metrics_from_confusion(**counts),
        **{k: float(v) for k, v in counts.items()},
    }
    if score_col and score_col in predictions.columns:
        binary = BinaryClassificationEvaluator(
            labelCol=label_col, rawPredictionCol=score_col, numBins=0
        )
        metrics["roc_auc"] = binary.evaluate(
            predictions, {binary.metricName: "areaUnderROC"}
        )
        metrics["pr_auc"] = binary.evaluate(
            predictions, {binary.metricName: "areaUnderPR"}
        )
    return metrics


# ---------------------------------------------------------------------------
# NumPy versions on collected predictions (curves, thresholds, bootstrap).
# Test/validation splits are ~22k rows, small enough to collect.
# ---------------------------------------------------------------------------


def np_regression_metrics(y: np.ndarray, pred: np.ndarray) -> dict[str, float]:
    """Same metrics as ``regression_metrics`` from log-scale arrays."""
    err = pred - y
    ss_res = float(np.sum(err**2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    err_fans = np.expm1(pred) - np.expm1(y)
    return {
        "rmse": float(np.sqrt(np.mean(err**2))),
        "r2": 1.0 - ss_res / ss_tot if ss_tot else 0.0,
        "mae": float(np.mean(np.abs(err))),
        "mae_fans": float(np.mean(np.abs(err_fans))),
        "rmse_fans": float(np.sqrt(np.mean(err_fans**2))),
    }


def np_threshold_metrics(y: np.ndarray, pred: np.ndarray) -> dict[str, float]:
    """Same metrics as ``metrics_from_confusion`` from 0/1 arrays."""
    return metrics_from_confusion(
        tp=int(np.sum((pred == 1) & (y == 1))),
        fp=int(np.sum((pred == 1) & (y == 0))),
        tn=int(np.sum((pred == 0) & (y == 0))),
        fn=int(np.sum((pred == 0) & (y == 1))),
    )


def _ranked_counts(y: np.ndarray, score: np.ndarray):
    """Cumulative TP/FP at every distinct score threshold (descending)."""
    order = np.argsort(-score, kind="mergesort")
    y_sorted, s_sorted = y[order], score[order]
    last_of_group = np.r_[np.diff(s_sorted) != 0, True]
    tp = np.cumsum(y_sorted)[last_of_group]
    fp = np.cumsum(1 - y_sorted)[last_of_group]
    return tp, fp, s_sorted[last_of_group]


def roc_curve(y: np.ndarray, score: np.ndarray):
    """(fpr, tpr) with (0,0) and (1,1) end points, as Spark builds it."""
    tp, fp, _ = _ranked_counts(y, score)
    pos, neg = max(tp[-1], 1), max(fp[-1], 1)
    return np.r_[0.0, fp / neg, 1.0], np.r_[0.0, tp / pos, 1.0]


def pr_curve(y: np.ndarray, score: np.ndarray):
    """(recall, precision) starting at (0, first precision), as in Spark."""
    tp, fp, _ = _ranked_counts(y, score)
    precision = tp / np.maximum(tp + fp, 1)
    recall = tp / max(tp[-1], 1)
    return np.r_[0.0, recall], np.r_[precision[0], precision]


def roc_auc(y: np.ndarray, score: np.ndarray) -> float:
    fpr, tpr = roc_curve(y, score)
    return float(np.trapezoid(tpr, fpr))


def pr_auc(y: np.ndarray, score: np.ndarray) -> float:
    recall, precision = pr_curve(y, score)
    return float(np.trapezoid(precision, recall))


def threshold_sweep(
    y: np.ndarray, score: np.ndarray, thresholds: np.ndarray | None = None
) -> list[dict[str, float]]:
    """Positive-class metrics for every threshold (predict 1 if score >= t)."""
    if thresholds is None:
        thresholds = np.round(np.arange(0.05, 0.96, 0.01), 2)
    return [
        {"threshold": float(t), **np_threshold_metrics(y, (score >= t) * 1.0)}
        for t in thresholds
    ]


def best_threshold(y: np.ndarray, score: np.ndarray) -> float:
    """Threshold that maximises positive-class F1 (pick on validation!)."""
    sweep = threshold_sweep(y, score)
    return max(sweep, key=lambda r: r["f1"])["threshold"]


def bootstrap(
    metric_fn,
    arrays: tuple[np.ndarray, ...],
    n_resamples: int = 300,
    seed: int = 42,
) -> dict[str, tuple[float, float]]:
    """
    95% percentile intervals of every metric returned by ``metric_fn``
    over row resamples of ``arrays`` (all of the same length).
    """
    rng = np.random.default_rng(seed)
    n = len(arrays[0])
    samples: dict[str, list[float]] = {}
    for _ in range(n_resamples):
        idx = rng.integers(0, n, n)
        for key, value in metric_fn(*(a[idx] for a in arrays)).items():
            samples.setdefault(key, []).append(value)
    return {
        key: (
            float(np.percentile(values, 2.5)),
            float(np.percentile(values, 97.5)),
        )
        for key, values in samples.items()
    }


def paired_bootstrap_diff(
    metric_fn,
    y: np.ndarray,
    pred_a: np.ndarray,
    pred_b: np.ndarray,
    higher_is_better: bool,
    n_resamples: int = 300,
    seed: int = 42,
) -> dict[str, float]:
    """
    metric(a) - metric(b) on the same resampled rows: point difference, its
    95% interval and ``share_a_better``, the share of resamples in which
    model a beats model b (direction given by ``higher_is_better``).
    """
    rng = np.random.default_rng(seed)
    n = len(y)
    diffs = []
    for _ in range(n_resamples):
        idx = rng.integers(0, n, n)
        diffs.append(
            metric_fn(y[idx], pred_a[idx]) - metric_fn(y[idx], pred_b[idx])
        )
    diffs = np.asarray(diffs)
    better = diffs > 0 if higher_is_better else diffs < 0
    return {
        "diff": float(metric_fn(y, pred_a) - metric_fn(y, pred_b)),
        "ci_low": float(np.percentile(diffs, 2.5)),
        "ci_high": float(np.percentile(diffs, 97.5)),
        "share_a_better": float(np.mean(better)),
    }
