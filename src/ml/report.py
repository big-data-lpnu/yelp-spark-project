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
from src.ml.training import TaskResult

REGRESSION_COLUMNS = ["rmse", "r2", "mae", "within_half_star"]
CLASSIFICATION_COLUMNS = ["accuracy", "precision", "recall", "f1"]
CLASSIFICATION_EXTRA = ["macro_f1", "weighted_f1", "roc_auc", "pr_auc"]


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


def _ci(fam, metric: str) -> str:
    low, high = fam.test_ci.get(metric, (float("nan"), float("nan")))
    return f"[{low:.3f}; {high:.3f}]"


def comparison_table(result: TaskResult) -> pd.DataFrame:
    """Test metrics of baselines and every family (main comparison)."""
    is_cls = result.task.label == "is_closed"
    columns = (
        CLASSIFICATION_COLUMNS + CLASSIFICATION_EXTRA
        if is_cls
        else REGRESSION_COLUMNS
    )
    rows = []
    for name, metrics in result.baselines.items():
        rows.append(
            {
                "model": name,
                **{c: metrics.get(c, float("nan")) for c in columns},
            }
        )
    for fam in result.families:
        row = {
            "model": fam.spec.name,
            **{c: fam.test_metrics[c] for c in columns},
        }
        key = "f1" if is_cls else "rmse"
        row[f"{key}_95ci"] = _ci(fam, key)
        if is_cls:
            row["pr_auc_95ci"] = _ci(fam, "pr_auc")
        else:
            row["r2_95ci"] = _ci(fam, "r2")
        row["fit_seconds"] = round(fam.fit_seconds, 1)
        row["best_params"] = json.dumps(fam.best_params)
        rows.append(row)
    return pd.DataFrame(rows)


def generalisation_table(result: TaskResult) -> pd.DataFrame:
    """Train / validation / test of the selected models (overfitting gap)."""
    is_cls = result.task.label == "is_closed"
    metrics = ["f1", "pr_auc"] if is_cls else ["rmse", "r2"]
    rows = []
    for fam in result.families:
        row = {"model": fam.spec.name}
        for m in metrics:
            row[f"train_{m}"] = fam.train_metrics[m]
            row[f"val_{m}"] = fam.validation_metrics[m]
            row[f"test_{m}"] = fam.test_metrics[m]
        rows.append(row)
    return pd.DataFrame(rows)


def tuning_table(result: TaskResult) -> pd.DataFrame:
    """Every grid point: its parameters, fit times, train/validation metric."""
    is_cls = result.task.label == "is_closed"
    metrics = ["f1", "pr_auc"] if is_cls else ["rmse", "r2"]
    meta = {"model", "fit_seconds", "search_seconds"}
    rows = []
    for fam in result.families:
        for trial in fam.trials:
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
                    # GBT: the long 200-iteration fit plus the refit.
                    "search_seconds": trial["search_seconds"],
                    **{
                        f"{p}_{m}": trial[f"{p}_{m}"]
                        for m in metrics
                        for p in ("train", "val")
                    },
                }
            )
    return pd.DataFrame(rows)


def importance_table(result: TaskResult, top: int = 20) -> pd.DataFrame:
    fam = result.best
    return pd.DataFrame(
        [
            {"feature": n, "group": plots.feature_group(n), "importance": v}
            for n, v in fam.importances[:top]
        ]
    )


def write_task(result: TaskResult, out_dir: Path) -> dict[str, pd.DataFrame]:
    """Write every table of one task as CSV; return them for the digest."""
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = {
        "splits": pd.DataFrame(result.split_summary),
        "comparison": comparison_table(result),
        "generalisation": generalisation_table(result),
        "tuning": tuning_table(result),
        "importance": importance_table(result),
        "group_importance": plots.grouped_importances(result.best.importances)
        .rename("share")
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"index": "group"}),
    }
    if result.learning_curve:
        tables["learning_curve"] = pd.DataFrame(result.learning_curve)
    if "class_weights" in result.extras:
        tables["class_weights"] = pd.DataFrame(result.extras["class_weights"])
    for name, df in tables.items():
        df.to_csv(out_dir / f"{result.task.name}_{name}.csv", index=False)
    return tables


def write_figures(
    regression: TaskResult,
    classification: TaskResult,
    classification_recency: TaskResult,
    features_sample: pd.DataFrame,
    fig_dir: Path,
) -> list[Path]:
    reg, cls, rec = regression, classification, classification_recency
    paths = [
        plots.target_distributions(
            features_sample, fig_dir / "target_distributions.png"
        ),
        # Regression
        plots.regression_comparison(reg, fig_dir / "reg_comparison.png"),
        plots.convergence(reg, fig_dir / "reg_linear_convergence.png"),
        plots.regularization_path(
            reg, "rmse", fig_dir / "reg_linear_regularization.png"
        ),
        plots.tree_depth_curve(reg, "rmse", fig_dir / "reg_tree_depth.png"),
        plots.forest_curves(reg, "rmse", fig_dir / "reg_forest.png"),
        plots.gbt_iterations(reg, fig_dir / "reg_gbt_iterations.png"),
        plots.fit_times(reg, fig_dir / "reg_fit_times.png"),
        plots.feature_importance(reg, fig_dir / "reg_feature_importance.png"),
        plots.predictions_by_star(reg, fig_dir / "reg_predictions_by_star.png"),
        plots.error_by_review_count(
            reg, fig_dir / "reg_error_by_review_count.png"
        ),
        # Classification
        plots.classification_comparison(
            [cls, rec],
            ["Без ознак давності активності", "З ознаками давності активності"],
            fig_dir / "cls_comparison.png",
        ),
        plots.convergence(cls, fig_dir / "cls_linear_convergence.png"),
        plots.regularization_path(
            cls, "pr_auc", fig_dir / "cls_linear_regularization.png"
        ),
        plots.tree_depth_curve(cls, "pr_auc", fig_dir / "cls_tree_depth.png"),
        plots.forest_curves(cls, "pr_auc", fig_dir / "cls_forest.png"),
        plots.gbt_iterations(cls, fig_dir / "cls_gbt_iterations.png"),
        plots.fit_times(cls, fig_dir / "cls_fit_times.png"),
        plots.roc_pr_curves(cls, fig_dir / "cls_roc_pr.png"),
        plots.roc_pr_curves(rec, fig_dir / "cls_recency_roc_pr.png"),
        plots.confusion_matrices(cls, fig_dir / "cls_confusion.png"),
        plots.threshold_curve(cls, fig_dir / "cls_threshold.png"),
        plots.feature_importance(cls, fig_dir / "cls_feature_importance.png"),
        plots.feature_importance(
            rec, fig_dir / "cls_recency_feature_importance.png"
        ),
        plots.group_importance_comparison(
            [cls, rec],
            ["без давності", "з давністю"],
            fig_dir / "cls_group_importance.png",
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
                "families": {
                    f.spec.name: {
                        "best_params": f.best_params,
                        "fit_seconds": f.fit_seconds,
                        "train": f.train_metrics,
                        "validation": f.validation_metrics,
                        "test": f.test_metrics,
                        "test_ci": f.test_ci,
                        "gbt_best_iters": {
                            k: v["best_iter"]
                            for k, v in f.curves.get(
                                "gbt_iterations", {}
                            ).items()
                        },
                        "objective_iterations": len(
                            f.curves.get("objective_history", [])
                        ),
                        "top_features": f.importances[:15],
                    }
                    for f in r.families
                },
                "learning_curve": r.learning_curve,
                "extras": r.extras,
            }
        )
    return out


EXTRA_TITLES = {
    "top_two": "Парний bootstrap: дві найкращі моделі на тесті",
    "profile_only": "Абляція: лише ознаки профілю бізнесу",
    "threshold": "Поріг класифікації, обраний на валідації",
}


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
    titles = {
        "splits": "Розбиття на вибірки",
        "comparison": "Порівняння моделей на тестовій вибірці",
        "generalisation": "Train / validation / test обраних моделей",
        "tuning": "Підбір гіперпараметрів (усі спроби)",
        "importance": "Топ-20 ознак найкращої моделі",
        "group_importance": "Важливість груп ознак найкращої моделі",
        "learning_curve": "Крива навчання найкращої моделі",
        "class_weights": "Ваги класів × поріг (найкраща модель)",
    }
    for r in results:
        lines += ["", f"## Задача `{r.task.name}`", ""]
        for name, df in tables[r.task.name].items():
            lines += [f"### {titles.get(name, name)}", "", to_markdown(df), ""]
        for key, value in r.extras.items():
            if key == "class_weights":
                continue
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
