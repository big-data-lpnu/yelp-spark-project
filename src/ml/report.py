"""
Persist ML results: CSV tables, a markdown digest of all tables and the
figures. The Ukrainian interpretation in ``src/reports/ml/README.md`` is
written by hand from these numbers; nothing here generates prose.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.ml import plots
from src.ml.models import FM, GBT, LINEAR, LOGISTIC, RANDOM_FOREST
from src.ml.training import TaskResult

REGRESSION_COLUMNS = ["rmse", "r2", "mae", "mae_fans", "rmse_fans"]
CLASSIFICATION_COLUMNS = ["accuracy", "precision", "recall", "f1"]


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        return "—" if pd.isna(value) else f"{value:.4f}"
    return str(value)


def to_markdown(df: pd.DataFrame) -> str:
    """Plain GitHub markdown table (no tabulate dependency)."""
    header = "| " + " | ".join(map(str, df.columns)) + " |"
    sep = "|" + "|".join(" --- " for _ in df.columns) + "|"
    rows = [
        "| " + " | ".join(_fmt(v) for v in row) + " |"
        for row in df.itertuples(index=False)
    ]
    return "\n".join([header, sep, *rows])


def _ci(model, metric: str) -> str:
    low, high = model.test_ci.get(metric, (float("nan"), float("nan")))
    return f"[{low:.3f}; {high:.3f}]"


def comparison_table(result: TaskResult) -> pd.DataFrame:
    """Test metrics of baselines and models (the main comparison)."""
    rows = []
    if result.task.is_classification:
        for name, m in result.baselines.items():
            rows.append(
                {"model": name, **{c: m[c] for c in CLASSIFICATION_COLUMNS}}
            )
        for m in result.models:
            rows.append(
                {
                    "model": m.spec.name,
                    "threshold": m.threshold,
                    **{c: m.test_tuned[c] for c in CLASSIFICATION_COLUMNS},
                    "f1_95ci": _ci(m, "f1"),
                    "pr_auc": m.test_metrics["pr_auc"],
                    "pr_auc_95ci": _ci(m, "pr_auc"),
                    "roc_auc": m.test_metrics["roc_auc"],
                    "fit_seconds": round(m.fit_seconds, 1),
                    "best_params": json.dumps(m.best_params),
                }
            )
        return pd.DataFrame(rows)

    for name, m in result.baselines.items():
        rows.append({"model": name, **{c: m[c] for c in REGRESSION_COLUMNS}})
    for m in result.models:
        rows.append(
            {
                "model": m.spec.name,
                **{c: m.test_metrics[c] for c in REGRESSION_COLUMNS},
                "rmse_95ci": _ci(m, "rmse"),
                "r2_95ci": _ci(m, "r2"),
                "fit_seconds": round(m.fit_seconds, 1),
                "best_params": json.dumps(m.best_params),
            }
        )
    return pd.DataFrame(rows)


def default_threshold_table(result: TaskResult) -> pd.DataFrame:
    """Classifiers at the default 0.5 threshold, for transparency."""
    return pd.DataFrame(
        [
            {
                "model": m.spec.name,
                **{c: m.test_metrics[c] for c in CLASSIFICATION_COLUMNS},
            }
            for m in result.models
        ]
    )


def generalisation_table(result: TaskResult) -> pd.DataFrame:
    """Train / validation / test of the selected models (overfitting gap)."""
    metrics = (
        ["pr_auc", "roc_auc"]
        if result.task.is_classification
        else ["rmse", "r2"]
    )
    rows = []
    for m in result.models:
        row = {"model": m.spec.name}
        for k in metrics:
            row[f"train_{k}"] = m.train_metrics[k]
            row[f"val_{k}"] = m.validation_metrics[k]
            row[f"test_{k}"] = m.test_metrics[k]
        rows.append(row)
    return pd.DataFrame(rows)


def tuning_table(result: TaskResult) -> pd.DataFrame:
    """
    Every grid point: its parameters, fit times, train/validation metric.
    Classification F1 here is at the default 0.5 threshold (thresholds are
    tuned only for the selected models); selection uses PR-AUC.
    """
    metrics = (
        ["pr_auc", "f1"] if result.task.is_classification else ["rmse", "r2"]
    )
    meta = {"model", "fit_seconds", "search_seconds"}
    rows = []
    for m in result.models:
        for trial in m.trials:
            params = {
                k: v
                for k, v in trial.items()
                if k not in meta and not k.startswith(("train_", "val_"))
            }
            rows.append(
                {
                    "model": trial["model"],
                    "params": json.dumps(params),
                    "fit_seconds": trial["fit_seconds"],
                    "search_seconds": trial["search_seconds"],
                    **{
                        f"{p}_{k}{'@0.5' if k == 'f1' else ''}": trial[
                            f"{p}_{k}"
                        ]
                        for k in metrics
                        for p in ("train", "val")
                    },
                }
            )
    return pd.DataFrame(rows)


def importance_table(result: TaskResult, top: int = 15) -> pd.DataFrame:
    """Top features of every model that exposes per-feature weights."""
    rows = []
    for m in result.models:
        for rank, (name, value) in enumerate(m.importances[:top], 1):
            rows.append(
                {
                    "model": m.spec.name,
                    "rank": rank,
                    "feature": name,
                    "value": value,
                }
            )
    return pd.DataFrame(rows)


def write_task(result: TaskResult, out_dir: Path) -> dict[str, pd.DataFrame]:
    """Write every table of one task as CSV; return them for the digest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = {
        "splits": pd.DataFrame(result.split_summary),
        "comparison": comparison_table(result),
        "generalisation": generalisation_table(result),
        "tuning": tuning_table(result),
        "importance": importance_table(result),
    }
    if result.task.is_classification:
        tables["threshold_0_5"] = default_threshold_table(result)
    if result.learning_curve:
        tables["learning_curve"] = pd.DataFrame(result.learning_curve)
    for name, df in tables.items():
        df.to_csv(out_dir / f"{result.task.name}_{name}.csv", index=False)
    return tables


def write_figures(
    regression: TaskResult,
    classification: TaskResult,
    fan_counts: pd.DataFrame,
    fig_dir: Path,
) -> list[Path]:
    """Every report figure (the notebook draws the same ones step by step)."""
    reg, cls = regression, classification
    paths = [
        plots.target_distributions(fan_counts, fig_dir / "targets.png"),
        # Regression
        plots.regression_comparison(reg, fig_dir / "reg_comparison.png"),
        plots.objective_history(
            reg,
            LINEAR,
            fig_dir / "reg_linear_convergence.png",
            "LinearRegression: збіжність L-BFGS",
        ),
        plots.param_curve(
            reg,
            LINEAR,
            "regParam",
            "rmse",
            fig_dir / "reg_linear_regularization.png",
            "LinearRegression: сила регуляризації",
            "regParam",
            log_x=True,
        ),
        plots.param_curve(
            reg,
            FM,
            "stepSize",
            "rmse",
            fig_dir / "reg_fm_learning_rate.png",
            "FMRegressor: крок навчання AdamW",
            "stepSize (learning rate)",
            log_x=True,
            log_y=True,
        ),
        plots.gbt_iterations(reg, fig_dir / "reg_gbt_iterations.png"),
        plots.fit_times(reg, fig_dir / "reg_fit_times.png"),
        plots.feature_importance(
            reg, [LINEAR, GBT], fig_dir / "reg_feature_importance.png"
        ),
        plots.regression_predictions(reg, fig_dir / "reg_predictions.png"),
        # Classification
        plots.classification_comparison(cls, fig_dir / "cls_comparison.png"),
        plots.objective_history(
            cls,
            LOGISTIC,
            fig_dir / "cls_logistic_convergence.png",
            "LogisticRegression: збіжність L-BFGS",
        ),
        plots.param_curve(
            cls,
            LOGISTIC,
            "regParam",
            "pr_auc",
            fig_dir / "cls_logistic_regularization.png",
            "LogisticRegression: сила регуляризації",
            "regParam",
            log_x=True,
        ),
        plots.forest_settings(cls, fig_dir / "cls_forest.png"),
        plots.mlp_training(cls, fig_dir / "cls_mlp_training.png"),
        plots.fit_times(cls, fig_dir / "cls_fit_times.png"),
        plots.roc_pr_curves(cls, fig_dir / "cls_roc_pr.png"),
        plots.threshold_curves(cls, fig_dir / "cls_threshold.png"),
        plots.confusion_matrices(cls, fig_dir / "cls_confusion.png"),
        plots.feature_importance(
            cls,
            [LOGISTIC, RANDOM_FOREST],
            fig_dir / "cls_feature_importance.png",
        ),
    ]
    if reg.learning_curve:
        paths.append(
            plots.learning_curve(
                reg, "rmse", fig_dir / "reg_learning_curve.png"
            )
        )
    if cls.learning_curve:
        paths.append(
            plots.learning_curve(
                cls, "pr_auc", fig_dir / "cls_learning_curve.png"
            )
        )
    return paths


def summary(results: list[TaskResult]) -> dict[str, Any]:
    """JSON-friendly digest of everything (for writing the report)."""

    def clean(obj):
        if isinstance(obj, dict):
            return {str(k): clean(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [clean(v) for v in obj]
        if hasattr(obj, "item"):
            return obj.item()
        return obj

    out = {}
    for r in results:
        out[r.task.name] = clean(
            {
                "n_features": len(r.data.feature_names),
                "splits": r.split_summary,
                "best": r.best.spec.name,
                "baselines": r.baselines,
                "models": {
                    m.spec.name: {
                        "best_params": m.best_params,
                        "fit_seconds": m.fit_seconds,
                        "train": m.train_metrics,
                        "validation": m.validation_metrics,
                        "test": m.test_metrics,
                        "threshold": m.threshold,
                        "test_tuned": m.test_tuned,
                        "test_ci": m.test_ci,
                        "trials": m.trials,
                        "gbt": {
                            k: {
                                "best_iter": v["best_iter"],
                                "censored": v["censored"],
                            }
                            for k, v in m.curves.get(
                                "gbt_iterations", {}
                            ).items()
                        },
                        "objective_iterations": {
                            k: len(v)
                            for k, v in m.curves.get(
                                "objective_history", {}
                            ).items()
                        },
                        "top_features": m.importances[:15],
                    }
                    for m in r.models
                },
                "learning_curve": r.learning_curve,
                "extras": r.extras,
            }
        )
    return out


TITLES = {
    "splits": "Розбиття на вибірки",
    "comparison": "Порівняння моделей на тестовій вибірці",
    "threshold_0_5": "Класифікатори при порозі 0,5 (тест)",
    "generalisation": "Train / validation / test обраних моделей",
    "tuning": "Підбір гіперпараметрів (усі спроби)",
    "importance": "Найважливіші ознаки моделей",
    "learning_curve": "Крива навчання найкращої моделі",
}
EXTRA_TITLES = {"top_two": "Парний bootstrap: дві найкращі моделі на тесті"}


def write_digest(
    results: list[TaskResult],
    tables: dict[str, dict[str, pd.DataFrame]],
    path: Path,
    preprocessing_stages: str,
) -> Path:
    """All tables in one markdown file (appendix of the report)."""
    lines = [
        "# Результати етапу машинного навчання (згенеровано автоматично)",
        "",
        "Файл перезаписується скриптом `uv run -m src.ml.run`;"
        " інтерпретація — у `README.md` поруч.",
        "",
        "## Етапи конвеєра попередньої обробки (Spark ML PipelineModel)",
        "",
        "```",
        preprocessing_stages,
        "```",
    ]
    for r in results:
        lines += ["", f"## Задача `{r.task.name}`", ""]
        for name, df in tables[r.task.name].items():
            lines += [f"### {TITLES.get(name, name)}", "", to_markdown(df), ""]
        for key, value in r.extras.items():
            lines += [
                f"### {EXTRA_TITLES.get(key, key)}",
                "",
                "```json",
                json.dumps(value, indent=2, default=str),
                "```",
                "",
            ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
