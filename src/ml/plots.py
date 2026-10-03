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
from src.ml.models import DECISION_TREE, GBT, LINEAR, RANDOM_FOREST
from src.ml.training import PROBABILITY_COL, TaskResult

COLORS = {
    LINEAR: "#4C72B0",
    DECISION_TREE: "#DD8452",
    RANDOM_FOREST: "#55A868",
    GBT: "#C44E52",
}
BASELINE_COLOR = "#9A9A9A"

METRIC_LABELS = {
    "rmse": "RMSE",
    "r2": "R²",
    "mae": "MAE",
    "accuracy": "Accuracy",
    "precision": "Precision",
    "recall": "Recall",
    "f1": "F1",
    "pr_auc": "PR-AUC",
    "roc_auc": "ROC-AUC",
}

FEATURE_GROUP_LABELS = {
    "location": "Розташування",
    "popularity": "Кількість відгуків",
    "categories": "Категорії",
    "attributes": "Атрибути",
    "hours": "Години роботи",
    "reviews": "Відгуки (без оцінок)",
    "tips_checkins_photos": "Поради / відвідування / фото",
    "recency": "Давність активності",
    "other_label": "Інша ціль (stars / is_closed)",
}


def _save(fig, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def _family(result: TaskResult, key: str):
    return next(f for f in result.families if f.spec.key == key)


# ---------------------------------------------------------------------------
# Feature groups
# ---------------------------------------------------------------------------


def feature_group(name: str) -> str:
    """Family of a (pretty) feature name, for grouped importances."""
    from src.ml.features import RECENCY_FEATURES

    if name in RECENCY_FEATURES:
        return "recency"
    if name in ("latitude", "longitude") or name.startswith("state="):
        return "location"
    if name == "log_review_count":
        return "popularity"
    if name.startswith("category=") or name == "n_categories":
        return "categories"
    if name in ("stars", "is_closed"):
        return "other_label"
    if (
        name.startswith(
            ("attr_", "ambience_", "parking_", "meal_", "wifi=", "noise_level=")
        )
        or name.startswith(("alcohol=", "attire="))
        or name
        in (
            "price_range",
            "n_attributes",
        )
    ):
        return "attributes"
    if name in (
        "has_hours",
        "n_open_days",
        "weekly_open_hours",
        "open_weekend",
        "open_late",
        "opens_early",
        "hours_zero_format",
    ):
        return "hours"
    if name.startswith("avg_review_") or name == "business_age_days":
        return "reviews"
    return "tips_checkins_photos"


def grouped_importances(importances: list[tuple[str, float]]) -> pd.Series:
    """Sum of |importance| per feature group, normalised to 1."""
    totals: dict[str, float] = {}
    for name, value in importances:
        group = feature_group(name)
        totals[group] = totals.get(group, 0.0) + abs(value)
    series = pd.Series(totals)
    return series / series.sum() if series.sum() else series


# ---------------------------------------------------------------------------
# Data / target
# ---------------------------------------------------------------------------


def target_distributions(features: pd.DataFrame, path: Path) -> Path:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))
    counts = features["stars"].value_counts().sort_index()
    ax1.bar(counts.index.astype(str), counts.values, color=COLORS[LINEAR])
    ax1.set_title("Регресія: розподіл цільової змінної stars")
    ax1.set_xlabel("Рейтинг бізнесу (stars)")
    ax1.set_ylabel("Кількість бізнесів")

    closed = features["is_closed"].value_counts().sort_index()
    labels = ["Працює (0)", "Закритий (1)"]
    ax2.bar(labels, closed.values, color=[COLORS[RANDOM_FOREST], COLORS[GBT]])
    for i, v in enumerate(closed.values):
        ax2.text(
            i, v, f"{v:,}\n({v / closed.sum():.1%})", ha="center", va="bottom"
        )
    ax2.set_title("Класифікація: баланс класів is_closed")
    ax2.set_ylabel("Кількість бізнесів")
    ax2.set_ylim(0, closed.max() * 1.18)
    return _save(fig, path)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _metric_rows(result: TaskResult, metrics: list[str]) -> pd.DataFrame:
    rows = []
    for name, m in result.baselines.items():
        rows.append(
            {
                "model": name.replace("Baseline: ", "Базова: "),
                "kind": "baseline",
                **m,
            }
        )
    for fam in result.families:
        row = {"model": fam.spec.name, "kind": fam.spec.key, **fam.test_metrics}
        for metric in metrics:
            low, high = fam.test_ci.get(metric, (np.nan, np.nan))
            row[f"{metric}_low"], row[f"{metric}_high"] = low, high
        rows.append(row)
    return pd.DataFrame(rows)


def regression_comparison(result: TaskResult, path: Path) -> Path:
    df = _metric_rows(result, ["rmse", "r2"])
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, metric in zip(axes, ["rmse", "r2"]):
        colors = [COLORS.get(k, BASELINE_COLOR) for k in df["kind"]]
        err = None
        if f"{metric}_low" in df:
            low = (df[metric] - df[f"{metric}_low"]).clip(lower=0).fillna(0)
            high = (df[f"{metric}_high"] - df[metric]).clip(lower=0).fillna(0)
            err = [low.to_numpy(), high.to_numpy()]
        ax.barh(df["model"], df[metric], color=colors, xerr=err, capsize=3)
        for y, v in enumerate(df[metric]):
            ax.text(v, y, f" {v:.3f}", va="center")
        ax.set_title(f"{METRIC_LABELS[metric]} на тестовій вибірці")
        ax.invert_yaxis()
        ax.set_xlim(0, max(df[metric].max() * 1.2, 0.05))
    axes[1].set_yticklabels([])
    fig.suptitle("Порівняння регресійних моделей (95% bootstrap CI)")
    return _save(fig, path)


def classification_comparison(
    results: list[TaskResult], titles: list[str], path: Path
) -> Path:
    metrics = ["accuracy", "precision", "recall", "f1"]
    fig, axes = plt.subplots(
        1, len(results), figsize=(7 * len(results), 5.8), sharey=True
    )
    axes = np.atleast_1d(axes)
    for ax, result, title in zip(axes, results, titles):
        df = _metric_rows(result, metrics + ["pr_auc"])
        df = df[~df["model"].str.contains("majority")]
        width = 0.8 / len(df)
        x = np.arange(len(metrics))
        for i, (_, row) in enumerate(df.iterrows()):
            values = [row[m] for m in metrics]
            err = None
            if f"{metrics[0]}_low" in row and not np.isnan(
                row.get(f"{metrics[0]}_low", np.nan)
            ):
                err = [
                    [max(row[m] - row[f"{m}_low"], 0) for m in metrics],
                    [max(row[f"{m}_high"] - row[m], 0) for m in metrics],
                ]
            ax.bar(
                x + i * width,
                values,
                width,
                yerr=err,
                capsize=2,
                label=row["model"],
                color=COLORS.get(row["kind"], BASELINE_COLOR),
                hatch="//" if row["kind"] == "baseline" else None,
            )
        ax.set_xticks(x + width * (len(df) - 1) / 2)
        ax.set_xticklabels([METRIC_LABELS[m] for m in metrics])
        ax.set_ylim(0, 1.05)
        ax.set_title(title)
        ax.legend(
            fontsize=8,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.08),
            ncol=2,
            frameon=False,
        )
    axes[0].set_ylabel("Значення метрики (клас «закритий»)")
    fig.suptitle("Порівняння класифікаторів на тестовій вибірці (поріг 0.5)")
    return _save(fig, path)


# ---------------------------------------------------------------------------
# Training process
# ---------------------------------------------------------------------------


def convergence(result: TaskResult, path: Path) -> Path:
    fam = _family(result, LINEAR)
    history = fam.curves.get("objective_history", [])
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(
        range(1, len(history) + 1), history, marker=".", color=COLORS[LINEAR]
    )
    ax.set_xlabel("Ітерація оптимізатора (L-BFGS / OWL-QN)")
    ax.set_ylabel("Значення цільової функції (loss + регуляризація)")
    ax.set_title(f"Збіжність {fam.spec.name} {fam.best_params}")
    ax.grid(alpha=0.3)
    return _save(fig, path)


def regularization_path(result: TaskResult, metric: str, path: Path) -> Path:
    fam = _family(result, LINEAR)
    df = pd.DataFrame(fam.trials)
    fig, ax = plt.subplots(figsize=(7, 4))
    for mix, part in df.groupby("elasticNetParam"):
        part = part.sort_values("regParam")
        ax.plot(
            part["regParam"],
            part[f"val_{metric}"],
            marker="o",
            label=f"elasticNet={mix} (валідація)",
        )
        ax.plot(
            part["regParam"],
            part[f"train_{metric}"],
            ls="--",
            alpha=0.6,
            label=f"elasticNet={mix} (тренування)",
        )
    ax.set_xscale("log")
    ax.set_xlabel("regParam (сила регуляризації)")
    ax.set_ylabel(METRIC_LABELS[metric])
    ax.set_title(f"{fam.spec.name}: вплив регуляризації")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    return _save(fig, path)


def tree_depth_curve(result: TaskResult, metric: str, path: Path) -> Path:
    fam = _family(result, DECISION_TREE)
    df = pd.DataFrame(fam.trials)
    fig, ax = plt.subplots(figsize=(7, 4))
    for min_inst, part in df.groupby("minInstancesPerNode"):
        part = part.sort_values("maxDepth")
        ax.plot(
            part["maxDepth"],
            part[f"train_{metric}"],
            ls="--",
            marker="o",
            label=f"тренування, minInstancesPerNode={min_inst}",
        )
        ax.plot(
            part["maxDepth"],
            part[f"val_{metric}"],
            marker="s",
            label=f"валідація, minInstancesPerNode={min_inst}",
        )
    ax.set_xlabel("maxDepth (глибина дерева)")
    ax.set_ylabel(METRIC_LABELS[metric])
    ax.set_title(f"{fam.spec.name}: недонавчання vs перенавчання")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    return _save(fig, path)


def forest_curves(result: TaskResult, metric: str, path: Path) -> Path:
    fam = _family(result, RANDOM_FOREST)
    df = pd.DataFrame(fam.trials)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))
    trees = df[df["maxDepth"] == 12].sort_values("numTrees")
    ax1.plot(
        trees["numTrees"],
        trees[f"train_{metric}"],
        ls="--",
        marker="o",
        label="тренування",
    )
    ax1.plot(
        trees["numTrees"], trees[f"val_{metric}"], marker="s", label="валідація"
    )
    ax1.set_xlabel("numTrees (maxDepth=12)")
    depth = df[df["numTrees"] == 60].sort_values("maxDepth")
    ax2.plot(
        depth["maxDepth"],
        depth[f"train_{metric}"],
        ls="--",
        marker="o",
        label="тренування",
    )
    ax2.plot(
        depth["maxDepth"], depth[f"val_{metric}"], marker="s", label="валідація"
    )
    ax2.set_xlabel("maxDepth (numTrees=60)")
    for ax in (ax1, ax2):
        ax.set_ylabel(METRIC_LABELS[metric])
        ax.legend()
        ax.grid(alpha=0.3)
    fig.suptitle(f"{fam.spec.name}: кількість і глибина дерев")
    return _save(fig, path)


def gbt_iterations(result: TaskResult, path: Path) -> Path:
    fam = _family(result, GBT)
    curves = fam.curves.get("gbt_iterations", {})
    fig, ax = plt.subplots(figsize=(8, 4.5))
    loss_name = "RMSE"
    for i, (name, c) in enumerate(curves.items()):
        loss_name = "RMSE" if c["loss"] == "rmse" else "Spark log-loss"
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
        censored = c.get("censored", False)
        suffix = " (ще спадає)" if censored else f" (мін. на {c['best_iter']})"
        ax.plot(
            it, c["validation"], color=color, label=f"{name}: валідація{suffix}"
        )
        if not censored:
            ax.axvline(c["best_iter"], color=color, ls=":", alpha=0.7)
    ax.set_xlabel("Ітерація бустингу (кількість дерев)")
    ax.set_ylabel(f"{loss_name} (без ваг класів)")
    ax.set_title(
        f"{fam.spec.name}: криві навчання по ітераціях\n"
        "(пунктир — найкраща ітерація на валідації)"
    )
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
    df = pd.concat([pd.DataFrame(f.trials) for f in result.families])
    # "Total" counts all fitting work of the search (GBT: long fit + refit).
    df["work"] = df["search_seconds"].fillna(df["fit_seconds"])
    totals = (
        df.groupby("model")
        .agg(mean=("fit_seconds", "mean"), sum=("work", "sum"))
        .reindex([f.spec.name for f in result.families])
    )
    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.barh(
        totals.index,
        totals["mean"],
        color=[COLORS[f.spec.key] for f in result.families],
    )
    for y, (mean, total) in enumerate(zip(totals["mean"], totals["sum"])):
        ax.text(mean, y, f" {mean:.1f} с (усього {total:.0f} с)", va="center")
    ax.invert_yaxis()
    ax.set_xlim(0, totals["mean"].max() * 1.6)
    ax.set_xlabel("Середній час одного навчання, с")
    ax.set_title("Час навчання моделей")
    return _save(fig, path)


# ---------------------------------------------------------------------------
# Interpretation
# ---------------------------------------------------------------------------


def feature_importance(result: TaskResult, path: Path, top: int = 15) -> Path:
    fam = result.best
    top_items = fam.importances[:top][::-1]
    groups = grouped_importances(fam.importances).sort_values()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5))
    ax1.barh(
        [n for n, _ in top_items],
        [v for _, v in top_items],
        color=COLORS[fam.spec.key],
    )
    ax1.set_title(f"Топ-{top} ознак: {fam.spec.name}")
    ax1.set_xlabel(
        "Важливість (сума зменшення impurity)"
        if fam.spec.key != LINEAR
        else "Стандартизований коефіцієнт"
    )
    ax2.barh(
        [FEATURE_GROUP_LABELS.get(g, g) for g in groups.index],
        groups.values,
        color="#8172B3",
    )
    ax2.set_title("Частка важливості за групами ознак")
    ax2.set_xlabel("Частка сумарної важливості")
    return _save(fig, path)


def group_importance_comparison(
    results: list[TaskResult], titles: list[str], path: Path
) -> Path:
    frames = {
        t: grouped_importances(r.best.importances)
        for r, t in zip(results, titles)
    }
    df = pd.DataFrame(frames).fillna(0.0)
    df = df.loc[df.max(axis=1).sort_values().index]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    y = np.arange(len(df))
    h = 0.8 / len(df.columns)
    for i, col in enumerate(df.columns):
        ax.barh(y + i * h, df[col], h, label=col)
    ax.set_yticks(y + h * (len(df.columns) - 1) / 2)
    ax.set_yticklabels([FEATURE_GROUP_LABELS.get(g, g) for g in df.index])
    ax.set_xlabel("Частка сумарної важливості")
    ax.set_title("Важливість груп ознак у найкращих класифікаторах")
    ax.legend()
    return _save(fig, path)


def predictions_by_star(result: TaskResult, path: Path) -> Path:
    fam = result.best
    s = fam.test_scores
    stars = sorted(s["stars"].unique())
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    ax1.boxplot(
        [s.loc[s["stars"] == v, "prediction"] for v in stars],
        tick_labels=[str(v) for v in stars],
        showfliers=False,
    )
    ax1.plot(range(1, len(stars) + 1), stars, "r--", label="ідеальний прогноз")
    ax1.set_xlabel("Фактичний рейтинг (stars)")
    ax1.set_ylabel("Прогноз моделі")
    ax1.set_title(f"{fam.spec.name}: прогноз для кожного рівня рейтингу")
    ax1.legend()

    residual = s["prediction"] - s["stars"]
    ax2.hist(residual, bins=60, color=COLORS[fam.spec.key], alpha=0.85)
    ax2.axvline(0, color="black", lw=1)
    ax2.set_xlabel("Залишок (прогноз − факт)")
    ax2.set_ylabel("Кількість бізнесів")
    ax2.set_title(
        f"Розподіл залишків: середнє {residual.mean():+.3f},"
        f" σ {residual.std():.3f}"
    )
    return _save(fig, path)


def error_by_review_count(result: TaskResult, path: Path) -> Path:
    s = result.best.test_scores.copy()
    s["reviews"] = np.expm1(s["log_review_count"]).round()
    bins = [0, 10, 20, 50, 100, 250, np.inf]
    labels = ["5–10", "11–20", "21–50", "51–100", "101–250", ">250"]
    s["bucket"] = pd.cut(s["reviews"], bins=bins, labels=labels)
    g = s.groupby("bucket", observed=True).apply(
        lambda d: pd.Series(
            {
                "rmse": np.sqrt(np.mean((d["prediction"] - d["stars"]) ** 2)),
                "n": len(d),
            }
        ),
        include_groups=False,
    )
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(g.index.astype(str), g["rmse"], color=COLORS[result.best.spec.key])
    for i, (r, n) in enumerate(zip(g["rmse"], g["n"])):
        ax.text(
            i, r, f"{r:.2f}\nn={int(n)}", ha="center", va="bottom", fontsize=8
        )
    ax.set_ylim(0, g["rmse"].max() * 1.25)
    ax.set_xlabel("Кількість відгуків бізнесу")
    ax.set_ylabel("RMSE на тесті")
    ax.set_title("Помилка залежить від кількості відгуків")
    return _save(fig, path)


def roc_pr_curves(result: TaskResult, path: Path) -> Path:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    label = result.task.label
    prevalence = None
    for fam in result.families:
        s = fam.test_scores
        y = s[label].to_numpy(dtype=float)
        p = s[PROBABILITY_COL].to_numpy(dtype=float)
        prevalence = y.mean()
        fpr, tpr = roc_curve(y, p)
        rec, prec = pr_curve(y, p)
        ax1.plot(
            fpr,
            tpr,
            color=COLORS[fam.spec.key],
            label=f"{fam.spec.name} (AUC {fam.test_metrics['roc_auc']:.3f})",
        )
        ax2.plot(
            rec,
            prec,
            color=COLORS[fam.spec.key],
            label=f"{fam.spec.name} (AUC {fam.test_metrics['pr_auc']:.3f})",
        )
    ax1.plot([0, 1], [0, 1], "k:", label="випадковий")
    ax1.set_xlabel("False Positive Rate")
    ax1.set_ylabel("True Positive Rate (Recall)")
    ax1.set_title("ROC-криві (тест)")
    ax2.axhline(
        prevalence, color="k", ls=":", label=f"випадковий ({prevalence:.2f})"
    )
    ax2.set_xlabel("Recall")
    ax2.set_ylabel("Precision")
    ax2.set_title("Precision-Recall криві (тест)")
    for ax in (ax1, ax2):
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
    return _save(fig, path)


def confusion_matrices(result: TaskResult, path: Path) -> Path:
    fam = result.best
    t = result.extras["threshold"]["threshold"]
    s = fam.test_scores
    y = s[result.task.label].to_numpy(dtype=float)
    p = s[PROBABILITY_COL].to_numpy(dtype=float)
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2))
    for ax, (title, threshold) in zip(
        axes, [("поріг 0.5", 0.5), (f"поріг {t:.2f} (обрано на валідації)", t)]
    ):
        pred = (p >= threshold) * 1.0
        m = np.array(
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
        ax.imshow(m, cmap="Blues")
        for i in range(2):
            for j in range(2):
                ax.text(
                    j,
                    i,
                    f"{m[i, j]:,}\n({m[i, j] / m[i].sum():.1%})",
                    ha="center",
                    va="center",
                    color="white" if m[i, j] > m.max() / 2 else "black",
                )
        ax.set_xticks([0, 1], ["працює", "закритий"])
        ax.set_yticks([0, 1], ["працює", "закритий"])
        ax.set_xlabel("Прогноз")
        ax.set_ylabel("Факт")
        ax.set_title(f"{fam.spec.name}, {title}")
    return _save(fig, path)


def threshold_curve(result: TaskResult, path: Path) -> Path:
    fam = result.best
    s = fam.validation_scores
    sweep = pd.DataFrame(
        threshold_sweep(
            s[result.task.label].to_numpy(dtype=float),
            s[PROBABILITY_COL].to_numpy(dtype=float),
        )
    )
    t = result.extras["threshold"]["threshold"]
    fig, ax = plt.subplots(figsize=(7, 4))
    for metric in ["precision", "recall", "f1", "accuracy"]:
        ax.plot(sweep["threshold"], sweep[metric], label=METRIC_LABELS[metric])
    ax.axvline(t, color="k", ls=":", label=f"обраний поріг {t:.2f}")
    ax.set_xlabel("Поріг ймовірності класу «закритий»")
    ax.set_ylabel("Значення метрики (валідація)")
    ax.set_title(f"{fam.spec.name}: вибір порогу класифікації")
    ax.legend()
    ax.grid(alpha=0.3)
    return _save(fig, path)
