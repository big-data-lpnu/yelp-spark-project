"""
Figures for the ML report (Ukrainian labels, like the team report).

Every function draws one figure, saves it as PNG and closes it, so the module
works both from the CLI (Agg backend) and from the notebook, which displays
the saved files. No backend is forced here.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.ml.evaluation import pr_curve, roc_curve, threshold_sweep
from src.ml.models import FM, GBT, LINEAR, LOGISTIC, MLP, RANDOM_FOREST
from src.ml.training import PROBABILITY_COL, ModelResult, TaskResult

COLORS = {
    LINEAR: "#4C72B0",
    FM: "#8172B3",
    GBT: "#C44E52",
    LOGISTIC: "#4C72B0",
    RANDOM_FOREST: "#55A868",
    MLP: "#DD8452",
}
BASELINE_COLOR = "#9A9A9A"

METRIC_LABELS = {
    "rmse": "RMSE (log(1+fans))",
    "r2": "R²",
    "mae": "MAE (log(1+fans))",
    "accuracy": "Accuracy",
    "precision": "Precision",
    "recall": "Recall",
    "f1": "F1",
    "pr_auc": "PR-AUC",
    "roc_auc": "ROC-AUC",
}


def _save(fig, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def _model(result: TaskResult, key: str) -> ModelResult:
    return next(m for m in result.models if m.spec.key == key)


def _trials(result: TaskResult, key: str) -> pd.DataFrame:
    return pd.DataFrame(_model(result, key).trials)


# ---------------------------------------------------------------------------
# Data / targets
# ---------------------------------------------------------------------------


def target_distributions(fan_counts: pd.DataFrame, path: Path) -> Path:
    """
    Fans (raw and log scale) and the elite class balance, from a small
    aggregate with columns fans, is_elite, count (one row per pair).
    """
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 4))
    by_fans = fan_counts.groupby("fans")["count"].sum()
    total = by_fans.sum()
    clipped = by_fans.groupby(np.minimum(by_fans.index.to_numpy(), 50)).sum()
    ax1.bar(clipped.index, clipped.values, width=1.0, color=COLORS[LINEAR])
    ax1.set_yscale("log")
    ax1.set_title(
        f"fans (50 = «50 і більше»); 0 фанів: {by_fans[0] / total:.0%}"
    )
    ax1.set_xlabel("Кількість фанів")
    ax1.set_ylabel("Користувачів (лог. шкала)")

    ax2.hist(
        np.log1p(by_fans.index.to_numpy(dtype=float)),
        bins=50,
        weights=by_fans.values,
        color=COLORS[FM],
    )
    ax2.set_yscale("log")
    ax2.set_title("Ціль регресії: log(1 + fans)")
    ax2.set_xlabel("log(1 + fans)")

    elite = fan_counts.groupby("is_elite")["count"].sum().sort_index()
    ax3.bar(
        ["Не еліт (0)", "Еліт (1)"],
        elite.values,
        color=[COLORS[RANDOM_FOREST], COLORS[GBT]],
    )
    for i, v in enumerate(elite.values):
        ax3.text(
            i, v, f"{v:,}\n({v / elite.sum():.1%})", ha="center", va="bottom"
        )
    ax3.set_ylim(0, elite.max() * 1.2)
    ax3.set_title("Ціль класифікації: is_elite")
    return _save(fig, path)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _ci_err(m: ModelResult, metric: str, value: float):
    low, high = m.test_ci.get(metric, (value, value))
    return [[max(value - low, 0)], [max(high - value, 0)]]


def regression_comparison(result: TaskResult, path: Path) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ax, metric in zip(axes, ["rmse", "r2"]):
        names, values, colors, errs = [], [], [], []
        for name, m in result.baselines.items():
            names.append(name.replace("Baseline: ", "Базова: "))
            values.append(m[metric])
            colors.append(BASELINE_COLOR)
            errs.append([[0], [0]])
        for m in result.models:
            v = m.test_metrics[metric]
            names.append(m.spec.name)
            values.append(v)
            colors.append(COLORS[m.spec.key])
            errs.append(_ci_err(m, metric, v))
        err = np.hstack(errs)
        ax.barh(names, values, color=colors, xerr=err, capsize=3)
        for y, v in enumerate(values):
            ax.text(max(v, 0), y, f" {v:.3f}", va="center")
        ax.invert_yaxis()
        ax.set_xlim(min(0, min(values) * 1.1), max(values) * 1.25)
        ax.set_title(f"{METRIC_LABELS[metric]}, тест")
    axes[1].set_yticklabels([])
    fig.suptitle(
        "Регресія log(1 + fans): порівняння моделей (95% bootstrap CI)"
    )
    return _save(fig, path)


def classification_comparison(result: TaskResult, path: Path) -> Path:
    """Accuracy/Precision/Recall/F1 at each model's validation threshold."""
    metrics = ["accuracy", "precision", "recall", "f1"]
    rows = [
        (name.replace("Baseline: ", "Базова: "), m, None, BASELINE_COLOR, "//")
        for name, m in result.baselines.items()
        if "majority" not in name
    ] + [
        (
            f"{m.spec.name} (поріг {m.threshold:.2f})",
            m.test_tuned,
            m,
            COLORS[m.spec.key],
            None,
        )
        for m in result.models
    ]
    fig, ax = plt.subplots(figsize=(11, 5))
    width = 0.8 / len(rows)
    x = np.arange(len(metrics))
    for i, (name, values, model, color, hatch) in enumerate(rows):
        heights = [values[k] for k in metrics]
        err = None
        if model is not None:
            err = np.hstack([_ci_err(model, k, values[k]) for k in metrics])
        ax.bar(
            x + i * width,
            heights,
            width,
            yerr=err,
            capsize=2,
            label=name,
            color=color,
            hatch=hatch,
        )
    ax.set_xticks(x + width * (len(rows) - 1) / 2)
    ax.set_xticklabels([METRIC_LABELS[m] for m in metrics])
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("Значення метрики (клас «еліт»)")
    ax.set_title(
        "Класифікація is_elite: тест, поріг кожної моделі обрано на валідації"
    )
    ax.legend(
        fontsize=8,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=2,
        frameon=False,
    )
    return _save(fig, path)


# ---------------------------------------------------------------------------
# Training process
# ---------------------------------------------------------------------------


def objective_history(
    result: TaskResult, key: str, path: Path, title: str
) -> Path:
    """Loss after every optimiser iteration, one line per grid point."""
    m = _model(result, key)
    fig, ax = plt.subplots(figsize=(7.5, 4))
    for label, history in m.curves.get("objective_history", {}).items():
        ax.plot(range(1, len(history) + 1), history, label=label)
    ax.set_xlabel("Ітерація оптимізатора (L-BFGS)")
    ax.set_ylabel("Значення функції втрат")
    ax.set_yscale("log")
    ax.set_title(title)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    return _save(fig, path)


def param_curve(
    result: TaskResult,
    key: str,
    param: str,
    metric: str,
    path: Path,
    title: str,
    xlabel: str,
    log_x: bool = False,
    log_y: bool = False,
) -> Path:
    """Train vs validation metric across one hyperparameter of the grid."""
    df = _trials(result, key)
    df["x"] = df[param].astype(str) if df[param].dtype == object else df[param]
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(
        df["x"], df[f"train_{metric}"], ls="--", marker="o", label="тренування"
    )
    ax.plot(df["x"], df[f"val_{metric}"], marker="s", label="валідація")
    if log_x:
        ax.set_xscale("log")
    if log_y:
        ax.set_yscale("log")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(METRIC_LABELS[metric])
    ax.set_title(title)
    ax.legend()
    ax.grid(alpha=0.3)
    return _save(fig, path)


def forest_settings(result: TaskResult, path: Path) -> Path:
    df = _trials(result, RANDOM_FOREST)
    labels = [
        f"{int(t)} дерев\nглибина {int(d)}"
        for t, d in zip(df["numTrees"], df["maxDepth"])
    ]
    x = np.arange(len(df))
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(
        x - 0.2, df["train_pr_auc"], 0.4, label="тренування", color="#A6CEE3"
    )
    ax.bar(
        x + 0.2,
        df["val_pr_auc"],
        0.4,
        label="валідація",
        color=COLORS[RANDOM_FOREST],
    )
    for i, (tr, va) in enumerate(zip(df["train_pr_auc"], df["val_pr_auc"])):
        ax.text(i - 0.2, tr, f"{tr:.3f}", ha="center", va="bottom", fontsize=8)
        ax.text(i + 0.2, va, f"{va:.3f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x, labels)
    lo = min(df["val_pr_auc"].min(), df["train_pr_auc"].min())
    ax.set_ylim(lo - 0.05, 1.0)
    ax.set_ylabel("PR-AUC")
    ax.set_title("RandomForestClassifier: кількість і глибина дерев")
    ax.legend()
    return _save(fig, path)


def mlp_training(result: TaskResult, path: Path) -> Path:
    m = _model(result, MLP)
    df = _trials(result, MLP)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.2))
    for label, history in m.curves.get("objective_history", {}).items():
        ax1.plot(range(1, len(history) + 1), history, label=label)
    ax1.set_xlabel("Ітерація L-BFGS")
    ax1.set_ylabel("Log-loss на тренуванні")
    ax1.set_title("Нейронна мережа: функція втрат під час навчання")
    if ax1.lines:
        ax1.legend(fontsize=8)
    ax1.grid(alpha=0.3)

    labels = [str(h) for h in df["hidden"]]
    x = np.arange(len(df))
    ax2.bar(
        x - 0.2, df["train_pr_auc"], 0.4, label="тренування", color="#FDBF6F"
    )
    ax2.bar(
        x + 0.2, df["val_pr_auc"], 0.4, label="валідація", color=COLORS[MLP]
    )
    for i, (tr, va) in enumerate(zip(df["train_pr_auc"], df["val_pr_auc"])):
        ax2.text(i - 0.2, tr, f"{tr:.3f}", ha="center", va="bottom", fontsize=8)
        ax2.text(i + 0.2, va, f"{va:.3f}", ha="center", va="bottom", fontsize=8)
    ax2.set_xticks(x, [f"приховані шари\n{h}" for h in labels])
    ax2.set_ylim(
        min(df["val_pr_auc"].min(), df["train_pr_auc"].min()) - 0.05, 1.0
    )
    ax2.set_ylabel("PR-AUC")
    ax2.set_title("Розмір мережі: тренування vs валідація")
    ax2.legend()
    return _save(fig, path)


def gbt_iterations(result: TaskResult, path: Path) -> Path:
    m = _model(result, GBT)
    curves = m.curves.get("gbt_iterations", {})
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for i, (name, c) in enumerate(curves.items()):
        color = plt.cm.viridis(i / max(len(curves) - 1, 1))
        it = np.arange(1, len(c["train"]) + 1)
        ax.plot(
            it,
            c["train"],
            ls="--",
            color=color,
            alpha=0.8,
            label=f"{name}: тренування",
        )
        suffix = (
            " (ще спадає)"
            if c.get("censored")
            else f" (мін. на {c['best_iter']})"
        )
        ax.plot(
            it, c["validation"], color=color, label=f"{name}: валідація{suffix}"
        )
        if not c.get("censored"):
            ax.axvline(c["best_iter"], color=color, ls=":", alpha=0.7)
    ax.set_xlabel("Ітерація бустингу (кількість дерев)")
    ax.set_ylabel(METRIC_LABELS["rmse"])
    ax.set_title("GBTRegressor: криві навчання по ітераціях")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    return _save(fig, path)


def learning_curve(result: TaskResult, metric: str, path: Path) -> Path:
    df = pd.DataFrame(result.learning_curve)
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(
        df["train_rows"],
        df[f"train_{metric}"],
        ls="--",
        marker="o",
        label="тренування",
    )
    ax.plot(
        df["train_rows"], df[f"val_{metric}"], marker="s", label="валідація"
    )
    ax.set_xscale("log")
    ax.set_xlabel("Розмір тренувальної вибірки (рядків, лог. шкала)")
    ax.set_ylabel(METRIC_LABELS[metric])
    ax.set_title(f"Крива навчання: {result.best.spec.name}")
    ax.legend()
    ax.grid(alpha=0.3)
    return _save(fig, path)


def fit_times(result: TaskResult, path: Path) -> Path:
    df = pd.concat([pd.DataFrame(m.trials) for m in result.models])
    totals = (
        df.groupby("model")
        .agg(mean=("fit_seconds", "mean"), sum=("search_seconds", "sum"))
        .reindex([m.spec.name for m in result.models])
    )
    fig, ax = plt.subplots(figsize=(7.5, 3))
    ax.barh(
        totals.index,
        totals["mean"],
        color=[COLORS[m.spec.key] for m in result.models],
    )
    for y, (mean, total) in enumerate(zip(totals["mean"], totals["sum"])):
        ax.text(mean, y, f" {mean:.1f} с (усього {total:.0f} с)", va="center")
    ax.invert_yaxis()
    ax.set_xlim(0, totals["mean"].max() * 1.7)
    ax.set_xlabel("Середній час одного навчання, с")
    ax.set_title("Час навчання моделей (1,4 млн рядків)")
    return _save(fig, path)


# ---------------------------------------------------------------------------
# Interpretation
# ---------------------------------------------------------------------------


def feature_importance(
    result: TaskResult, keys: list[str], path: Path, top: int = 12
) -> Path:
    """Side-by-side importances / standardised coefficients."""
    fig, axes = plt.subplots(1, len(keys), figsize=(6.5 * len(keys), 5))
    for ax, key in zip(np.atleast_1d(axes), keys):
        m = _model(result, key)
        items = m.importances[:top][::-1]
        colors = [COLORS[key] if v >= 0 else "#999999" for _, v in items]
        ax.barh([n for n, _ in items], [v for _, v in items], color=colors)
        ax.axvline(0, color="black", lw=0.8)
        is_tree = hasattr(m.model, "featureImportances")
        ax.set_xlabel(
            "Важливість (зменшення impurity)"
            if is_tree
            else "Коефіцієнт на стандартизованих ознаках"
        )
        ax.set_title(m.spec.name)
    return _save(fig, path)


def regression_predictions(result: TaskResult, path: Path) -> Path:
    m = result.best
    s = m.test_scores
    bins = [-0.5, 0.5, 1.5, 5.5, 20.5, 100.5, np.inf]
    labels = ["0", "1", "2–5", "6–20", "21–100", ">100"]
    s = s.assign(bucket=pd.cut(s["fans"], bins=bins, labels=labels))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    groups = [np.expm1(s.loc[s["bucket"] == b, "prediction"]) for b in labels]
    ax1.boxplot(groups, tick_labels=labels, showfliers=False)
    ax1.set_yscale("symlog", linthresh=1)
    ax1.set_xlabel("Фактична кількість фанів")
    ax1.set_ylabel("Прогноз, фанів (symlog)")
    ax1.set_title(f"{m.spec.name}: прогноз за групами користувачів")

    residual = s["prediction"] - s[result.task.label]
    ax2.hist(residual, bins=80, color=COLORS[m.spec.key])
    ax2.set_yscale("log")
    ax2.axvline(0, color="black", lw=1)
    ax2.set_xlabel("Залишок у log(1 + fans) (прогноз − факт)")
    ax2.set_ylabel("Користувачів (лог. шкала)")
    ax2.set_title(
        f"Залишки: середнє {residual.mean():+.3f}, σ {residual.std():.3f}"
    )
    return _save(fig, path)


def roc_pr_curves(result: TaskResult, path: Path) -> Path:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    label = result.task.label
    prevalence = None
    for m in result.models:
        s = m.test_scores
        y = s[label].to_numpy(dtype=float)
        p = s[PROBABILITY_COL].to_numpy(dtype=float)
        prevalence = y.mean()
        fpr, tpr = roc_curve(y, p)
        rec, prec = pr_curve(y, p)
        color = COLORS[m.spec.key]
        ax1.plot(
            fpr,
            tpr,
            color=color,
            label=f"{m.spec.name} (AUC {m.test_metrics['roc_auc']:.3f})",
        )
        ax2.plot(
            rec,
            prec,
            color=color,
            label=f"{m.spec.name} (AUC {m.test_metrics['pr_auc']:.3f})",
        )
    ax1.plot([0, 1], [0, 1], "k:", label="випадковий")
    ax1.set_xlabel("False Positive Rate")
    ax1.set_ylabel("True Positive Rate (Recall)")
    ax1.set_title("ROC-криві (тест)")
    ax2.axhline(
        prevalence, color="k", ls=":", label=f"випадковий ({prevalence:.3f})"
    )
    ax2.set_xlabel("Recall")
    ax2.set_ylabel("Precision")
    ax2.set_title("Precision-Recall криві (тест)")
    for ax in (ax1, ax2):
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
    return _save(fig, path)


def threshold_curves(result: TaskResult, path: Path) -> Path:
    """F1 vs threshold on validation for every classifier."""
    fig, ax = plt.subplots(figsize=(7.5, 4))
    for m in result.models:
        s = m.validation_scores
        sweep = pd.DataFrame(
            threshold_sweep(
                s[result.task.label].to_numpy(dtype=float),
                s[PROBABILITY_COL].to_numpy(dtype=float),
            )
        )
        color = COLORS[m.spec.key]
        ax.plot(
            sweep["threshold"], sweep["f1"], color=color, label=f"{m.spec.name}"
        )
        ax.axvline(m.threshold, color=color, ls=":", alpha=0.8)
    ax.set_xlabel("Поріг ймовірності класу «еліт»")
    ax.set_ylabel("F1 на валідації")
    ax.set_title("Вибір порогу класифікації (пунктир — обраний поріг)")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    return _save(fig, path)


def confusion_matrices(result: TaskResult, path: Path) -> Path:
    m = result.best
    s = m.test_scores
    y = s[result.task.label].to_numpy(dtype=float)
    p = s[PROBABILITY_COL].to_numpy(dtype=float)
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2))
    for ax, (title, threshold) in zip(
        axes,
        [
            ("поріг 0.5", 0.5),
            (f"поріг {m.threshold:.2f} (валідація)", m.threshold),
        ],
    ):
        pred = (p >= threshold) * 1.0
        cm = np.array(
            [
                [
                    np.sum((y == 0) & (pred == 0)),
                    np.sum((y == 0) & (pred == 1)),
                ],
                [
                    np.sum((y == 1) & (pred == 0)),
                    np.sum((y == 1) & (pred == 1)),
                ],
            ]
        )
        ax.imshow(cm, cmap="Blues")
        for i in range(2):
            for j in range(2):
                ax.text(
                    j,
                    i,
                    f"{cm[i, j]:,}\n({cm[i, j] / cm[i].sum():.1%})",
                    ha="center",
                    va="center",
                    color="white" if cm[i, j] > cm.max() / 2 else "black",
                )
        ax.set_xticks([0, 1], ["не еліт", "еліт"])
        ax.set_yticks([0, 1], ["не еліт", "еліт"])
        ax.set_xlabel("Прогноз")
        ax.set_ylabel("Факт")
        ax.set_title(f"{m.spec.name}, {title}")
    return _save(fig, path)
