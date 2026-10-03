"""
Model families and their hyperparameter grids.

Grids are explicit lists of parameter dicts (not a full cartesian product) so
each family covers exactly the settings the training analysis needs, e.g.
a maxDepth sweep for the decision tree and a numTrees sweep for the forest.
GBT ``maxIter`` is not in the grid: one long fit per depth is scored after
every boosting iteration and the best iteration count is picked from that
curve (see ``src.ml.training``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from pyspark.ml import Estimator
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)

from src.constants import ML_SEED
from src.ml.pipeline import FEATURES_COL, WEIGHT_COL

LINEAR = "linear"
DECISION_TREE = "decision_tree"
RANDOM_FOREST = "random_forest"
GBT = "gbt"

# Iteration budget per GBT fit. At stepSize 0.1 the validation loss is still
# (slowly) falling at 200 for shallow trees -> reported as "censored"; the
# stepSize 0.3 grid point reaches its minimum well inside the budget.
GBT_MAX_ITER = 200

TREE_DEFAULTS = {
    "seed": ML_SEED,
    # Spark's default 256 MB histogram budget makes deep forests on ~170
    # features fall back to many passes over the data.
    "maxMemoryInMB": 512,
    # Cache node ids instead of re-walking deep trees for every row.
    "cacheNodeIds": True,
    # GBT lineage grows by one stage per iteration; checkpoint it (needs
    # SparkContext.setCheckpointDir, set in src.ml.training).
    "checkpointInterval": 10,
}


@dataclass
class ModelSpec:
    """One model family: estimator factory plus the settings to try."""

    key: str
    name: str
    make: Callable[[dict[str, Any]], Estimator]
    grid: list[dict[str, Any]]
    # Parameters shared by every grid point, shown in the report.
    fixed: dict[str, Any] = field(default_factory=dict)


def _depth_grid() -> list[dict[str, Any]]:
    return [
        {"maxDepth": depth, "minInstancesPerNode": min_instances}
        for depth in (2, 4, 6, 8, 10, 12, 15)
        for min_instances in (1, 20)
    ]


def _forest_grid() -> list[dict[str, Any]]:
    # numTrees sweep at a fixed depth + maxDepth sweep at a fixed size.
    sweep_trees = [{"numTrees": n, "maxDepth": 12} for n in (10, 30, 60, 100)]
    sweep_depth = [{"numTrees": 60, "maxDepth": d} for d in (6, 9, 15)]
    return sweep_trees + sweep_depth


def _linear_grid() -> list[dict[str, Any]]:
    # No regParam=0: the one-hot blocks form a full dummy set that is
    # collinear with the intercept, so some regularisation is required.
    return [
        {"regParam": reg, "elasticNetParam": mix}
        for reg in (0.0005, 0.005, 0.05)
        for mix in (0.0, 0.5, 1.0)
    ]


def _gbt_grid() -> list[dict[str, Any]]:
    # Depth sweep at a small learning rate, plus one fast learner whose
    # validation loss turns upwards within the iteration budget.
    slow = [{"maxDepth": depth, "stepSize": 0.1} for depth in (3, 5, 7)]
    return slow + [{"maxDepth": 5, "stepSize": 0.3}]


def regression_models(label: str) -> list[ModelSpec]:
    common = {"featuresCol": FEATURES_COL, "labelCol": label}
    tree = {**common, **TREE_DEFAULTS}
    return [
        ModelSpec(
            key=LINEAR,
            name="LinearRegression",
            # l-bfgs (not the default normal equation) so the training
            # summary exposes the loss value of every iteration.
            fixed={"solver": "l-bfgs", "maxIter": 200},
            make=lambda p: LinearRegression(
                **common, solver="l-bfgs", maxIter=200, **p
            ),
            grid=_linear_grid(),
        ),
        ModelSpec(
            key=DECISION_TREE,
            name="DecisionTreeRegressor",
            make=lambda p: DecisionTreeRegressor(**tree, **p),
            grid=_depth_grid(),
        ),
        ModelSpec(
            key=RANDOM_FOREST,
            name="RandomForestRegressor",
            fixed={"subsamplingRate": 0.8, "featureSubsetStrategy": "onethird"},
            make=lambda p: RandomForestRegressor(
                **tree,
                subsamplingRate=0.8,
                featureSubsetStrategy="onethird",
                **p,
            ),
            grid=_forest_grid(),
        ),
        ModelSpec(
            key=GBT,
            name="GBTRegressor",
            fixed={"maxIter": GBT_MAX_ITER, "lossType": "squared"},
            make=lambda p: GBTRegressor(
                **tree, lossType="squared", **{"maxIter": GBT_MAX_ITER, **p}
            ),
            grid=_gbt_grid(),
        ),
    ]


def classification_models(label: str, weighted: bool = True) -> list[ModelSpec]:
    """Classifiers trained with balanced class weights (``class_weight``)."""
    common = {"featuresCol": FEATURES_COL, "labelCol": label}
    if weighted:
        common["weightCol"] = WEIGHT_COL
    tree = {**common, **TREE_DEFAULTS}
    return [
        ModelSpec(
            key=LINEAR,
            name="LogisticRegression",
            fixed={"maxIter": 200},
            make=lambda p: LogisticRegression(**common, maxIter=200, **p),
            grid=_linear_grid(),
        ),
        ModelSpec(
            key=DECISION_TREE,
            name="DecisionTreeClassifier",
            make=lambda p: DecisionTreeClassifier(**tree, **p),
            grid=_depth_grid(),
        ),
        ModelSpec(
            key=RANDOM_FOREST,
            name="RandomForestClassifier",
            fixed={"subsamplingRate": 0.8, "featureSubsetStrategy": "sqrt"},
            make=lambda p: RandomForestClassifier(
                **tree,
                subsamplingRate=0.8,
                featureSubsetStrategy="sqrt",
                **p,
            ),
            grid=_forest_grid(),
        ),
        ModelSpec(
            key=GBT,
            name="GBTClassifier",
            fixed={"maxIter": GBT_MAX_ITER},
            make=lambda p: GBTClassifier(
                **tree, **{"maxIter": GBT_MAX_ITER, **p}
            ),
            grid=_gbt_grid(),
        ),
    ]
