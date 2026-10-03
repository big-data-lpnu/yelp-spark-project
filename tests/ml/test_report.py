"""Every figure and table renders from (synthetic) results, no Spark."""

import json
from types import SimpleNamespace

import matplotlib
import numpy as np
import pandas as pd

from src.ml import report
from src.ml.evaluation import np_threshold_metrics
from src.ml.models import classification_models, regression_models
from src.ml.pipeline import (
    PreparedData,
    classification_task,
    regression_task,
)
from src.ml.training import (
    PROBABILITY_COL,
    ModelResult,
    TaskResult,
    _tune_threshold,
    compare_top_two,
    confidence_intervals,
)

matplotlib.use("Agg")
N = 2000


def _scores(task, rng):
    fans = rng.negative_binomial(0.3, 0.1, N)
    review_count = rng.integers(1, 500, N)
    if not task.is_classification:
        y = np.log1p(fans)
        pred = np.clip(y + rng.normal(0, 0.3, N), 0, None)
        return pd.DataFrame(
            {
                task.label: y,
                "prediction": pred,
                "fans": fans,
                "review_count": review_count,
            }
        )
    y = (rng.random(N) < 0.05).astype(float)
    p = np.clip(0.6 * y + rng.random(N) * 0.5, 0, 1)
    return pd.DataFrame(
        {
            task.label: y,
            "prediction": (p >= 0.5) * 1.0,
            PROBABILITY_COL: p,
            "fans": fans,
            "review_count": review_count,
        }
    )


def _metrics(task):
    if not task.is_classification:
        return {
            "rmse": 0.3,
            "r2": 0.8,
            "mae": 0.2,
            "mae_fans": 1.5,
            "rmse_fans": 9.0,
        }
    return {
        **np_threshold_metrics(np.array([1.0, 0.0]), np.array([1.0, 0.0])),
        "pr_auc": 0.9,
        "roc_auc": 0.97,
    }


def _result(task, specs, rng):
    models = []
    for spec in specs:
        metrics = _metrics(task)
        trials = []
        for params in spec.grid:
            params = {
                **params,
                **({"maxIter": 80} if spec.key == "gbt" else {}),
            }
            trials.append(
                {
                    "model": spec.name,
                    **params,
                    "fit_seconds": 3.0,
                    "search_seconds": 5.0,
                    **{f"train_{k}": v for k, v in metrics.items()},
                    **{f"val_{k}": v for k, v in metrics.items()},
                }
            )
        curves = {
            "objective_history": {"a": list(np.exp(-np.arange(20) / 5) + 0.1)}
        }
        if spec.key == "gbt":
            curve = list(1 / np.arange(1, 101) + 0.2)
            curves["gbt_iterations"] = {
                "maxDepth=3": {
                    "train": curve,
                    "validation": curve,
                    "best_iter": 100,
                    "censored": True,
                },
                "maxDepth=5": {
                    "train": curve,
                    "validation": curve,
                    "best_iter": 60,
                    "censored": False,
                },
            }
        is_tree = spec.key in ("gbt", "random_forest")
        model = ModelResult(
            spec=spec,
            trials=trials,
            best_params=dict(spec.grid[0]),
            model=SimpleNamespace(featureImportances=1) if is_tree else None,
            fit_seconds=2.0,
            train_metrics=metrics,
            validation_metrics=metrics,
            test_metrics=metrics,
            curves=curves,
            importances=[
                (f, float(rng.normal())) for f in task.numeric_features
            ],
            validation_scores=_scores(task, rng),
            test_scores=_scores(task, rng),
        )
        if task.is_classification:
            _tune_threshold(model, task)
        model.test_ci = confidence_intervals(model, task)
        models.append(model)

    data = PreparedData(
        task=task,
        preprocessing=SimpleNamespace(stages=["Imputer", "Scaler"]),
        train=None,
        validation=None,
        test=None,
        feature_names=list(task.numeric_features),
    )
    result = TaskResult(task=task, data=data, models=models)
    result.split_summary = [{"split": "train", "rows": 1, "mean": 0.1}]
    metrics = _metrics(task)
    result.learning_curve = [
        {
            "train_fraction": f,
            "train_rows": int(f * 1e6),
            "fit_seconds": 1.0,
            **{f"train_{k}": v for k, v in metrics.items()},
            **{f"val_{k}": v for k, v in metrics.items()},
        }
        for f in (0.01, 0.1, 1.0)
    ]
    result.baselines = {"Baseline: a": metrics, "Baseline: majority": metrics}
    result.extras["top_two"] = compare_top_two(result)
    return result


def test_all_figures_and_tables_render(tmp_path):
    rng = np.random.default_rng(0)
    reg_task, cls_task = regression_task(), classification_task()
    reg = _result(reg_task, regression_models(reg_task.label), rng)
    cls = _result(
        cls_task,
        classification_models(cls_task.label, len(cls_task.numeric_features)),
        rng,
    )
    fan_counts = pd.DataFrame(
        {
            "fans": [0, 0, 1, 5, 60, 3000],
            "is_elite": [0.0, 1, 0, 1, 1, 1],
            "count": [1500, 10, 200, 50, 3, 1],
        }
    )
    tables = {r.task.name: report.write_task(r, tmp_path) for r in (reg, cls)}
    paths = report.write_figures(reg, cls, fan_counts, tmp_path / "fig")
    report.write_digest([reg, cls], tables, tmp_path / "r.md", "1. x")
    json.dumps(report.summary([reg, cls]), default=str)

    assert all(p.exists() and p.stat().st_size > 0 for p in paths)
    assert len(paths) == 21
    tuning = tables["classification"]["tuning"]
    # Fit times are columns, never hyperparameters.
    assert not tuning["params"].str.contains("seconds").any()
