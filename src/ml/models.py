"""
The six models (three per task) and their small hyperparameter grids.

Regression (log fans):      LinearRegression, FMRegressor, GBTRegressor
Classification (is elite):  LogisticRegression, RandomForestClassifier,
                            MultilayerPerceptronClassifier

Each grid is short on purpose: its points are chosen so that the trials also
show how training behaves (regularisation strength, learning rate, number
and depth of trees, network size). GBT ``maxIter`` is not in the grid: one
long fit per depth is scored after every boosting iteration and the best
iteration count is picked from that curve (see ``src.ml.training``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from pyspark.ml import Estimator
from pyspark.ml.classification import (
    LogisticRegression,
    MultilayerPerceptronClassifier,
    RandomForestClassifier,
)
from pyspark.ml.regression import FMRegressor, GBTRegressor, LinearRegression

from src.constants import ML_SEED
from src.ml.pipeline import FEATURES_COL

LINEAR = "linear"
FM = "fm"
GBT = "gbt"
LOGISTIC = "logistic"
RANDOM_FOREST = "random_forest"
MLP = "mlp"

# Boosting iterations per GBT fit.
GBT_MAX_ITER = 100

# Full-batch AdamW iterations per factorization machine fit.
FM_MAX_ITER = 50


@dataclass
class ModelSpec:
    """One model: estimator factory plus the settings to try."""

    key: str
    name: str
    make: Callable[[dict[str, Any]], Estimator]
    grid: list[dict[str, Any]]
    # Parameters shared by every grid point, shown in the report.
    fixed: dict[str, Any] = field(default_factory=dict)


def regression_models(label: str) -> list[ModelSpec]:
    common = {"featuresCol": FEATURES_COL, "labelCol": label}
    return [
        ModelSpec(
            key=LINEAR,
            name="LinearRegression",
            # l-bfgs (not the default normal equation) so the training
            # summary exposes the loss value of every iteration.
            fixed={"solver": "l-bfgs", "maxIter": 100, "elasticNetParam": 0},
            make=lambda p: LinearRegression(
                **common, **{"solver": "l-bfgs", "maxIter": 100, **p}
            ),
            grid=[{"regParam": r} for r in (0.001, 0.01, 0.1)],
        ),
        ModelSpec(
            key=FM,
            name="FMRegressor",
            # Learning-rate sweep: too small learns slowly, too large
            # diverges. AdamW has no per-iteration history in Spark, so
            # this sweep is the training-process view of the FM.
            fixed={"factorSize": 8, "maxIter": FM_MAX_ITER, "solver": "adamW"},
            make=lambda p: FMRegressor(
                **common,
                seed=ML_SEED,
                **{"factorSize": 8, "maxIter": FM_MAX_ITER, **p},
            ),
            grid=[{"stepSize": s} for s in (0.001, 0.01, 0.1)],
        ),
        ModelSpec(
            key=GBT,
            name="GBTRegressor",
            fixed={"maxIter": GBT_MAX_ITER, "stepSize": 0.1},
            make=lambda p: GBTRegressor(
                **common,
                seed=ML_SEED,
                stepSize=0.1,
                lossType="squared",
                # GBT lineage grows by one stage per iteration; checkpoint
                # it (needs SparkContext.setCheckpointDir).
                checkpointInterval=10,
                **{"maxIter": GBT_MAX_ITER, **p},
            ),
            grid=[{"maxDepth": d} for d in (3, 5)],
        ),
    ]


def classification_models(label: str, n_features: int) -> list[ModelSpec]:
    common = {"featuresCol": FEATURES_COL, "labelCol": label}

    def mlp(params: dict[str, Any]) -> Estimator:
        params = dict(params)
        hidden = params.pop("hidden")
        return MultilayerPerceptronClassifier(
            **common,
            layers=[n_features, *hidden, 2],
            seed=ML_SEED,
            **{"maxIter": 100, **params},
        )

    return [
        ModelSpec(
            key=LOGISTIC,
            name="LogisticRegression",
            fixed={"maxIter": 100, "elasticNetParam": 0},
            make=lambda p: LogisticRegression(
                **common, **{"maxIter": 100, **p}
            ),
            grid=[{"regParam": r} for r in (0.001, 0.01, 0.1)],
        ),
        ModelSpec(
            key=RANDOM_FOREST,
            name="RandomForestClassifier",
            fixed={"featureSubsetStrategy": "sqrt", "subsamplingRate": 0.8},
            make=lambda p: RandomForestClassifier(
                **common,
                seed=ML_SEED,
                featureSubsetStrategy="sqrt",
                subsamplingRate=0.8,
                maxMemoryInMB=512,
                **p,
            ),
            grid=[
                {"numTrees": 20, "maxDepth": 8},
                {"numTrees": 60, "maxDepth": 8},
                {"numTrees": 60, "maxDepth": 14},
            ],
        ),
        ModelSpec(
            key=MLP,
            name="MultilayerPerceptronClassifier",
            fixed={"maxIter": 100, "solver": "l-bfgs", "activation": "sigmoid"},
            make=mlp,
            grid=[{"hidden": h} for h in ([16], [32, 16], [64, 32])],
        ),
    ]
