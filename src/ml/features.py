"""
User-level feature table for the ML stage.

One row per Yelp user (~2M rows) built from the ``user`` table:

* regression target: ``log_fans`` = log(1 + fans). Fans are extremely
  skewed (median 0, 99th percentile 26, a few users with thousands), so the
  model learns relative differences instead of chasing a handful of
  "celebrities";
* classification target: ``is_elite``, the user has been in Yelp's Elite
  Squad in at least one year (~4.6% of users).

Only label-free, row-wise transformations happen here; everything that
learns statistics from the data (imputation, scaling) lives in the Spark ML
pipeline in ``src.ml.pipeline`` and is fitted on the training split only.

Spark 4 runs with ANSI mode on, so string -> number/date conversions use
``try_cast`` / ``try_to_date`` and divisions use ``try_divide``: the strict
variants throw on dirty values instead of returning NULL.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.constants import ML_FEATURES_PATH
from src.spark.load_data import load_dataset

ID_COLUMN = "user_id"
REGRESSION_LABEL = "log_fans"
CLASSIFICATION_LABEL = "is_elite"

# compliment_funny is left out: in the Yelp dump it is an exact copy of
# compliment_cool for every user, so it would enter the model twice.
COMPLIMENTS = [
    "hot",
    "more",
    "profile",
    "cute",
    "list",
    "note",
    "plain",
    "cool",
    "writer",
    "photos",
]

# How active the user is and how others react to their reviews.
ACTIVITY_FEATURES = [
    "log_review_count",
    "years_on_yelp",
    "log_friends",
    "average_stars",
]
VOTE_FEATURES = [
    "log_useful",
    "log_funny",
    "log_cool",
    # Votes per review: quality rather than volume.
    "useful_per_review",
    "funny_per_review",
    "cool_per_review",
]
COMPLIMENT_FEATURES = [
    *[f"log_compliment_{c}" for c in COMPLIMENTS],
    "log_compliments_total",
    "compliments_per_review",
]
SHARED_FEATURES = [*ACTIVITY_FEATURES, *VOTE_FEATURES, *COMPLIMENT_FEATURES]

# The other task's target is a legitimate input (it is not derived from
# this task's target); anything derived from the task's own target is not.
REGRESSION_EXTRA_FEATURES = ["is_elite", "n_elite_years"]
CLASSIFICATION_EXTRA_FEATURES = ["log_fans"]

# Raw columns kept next to the features for reporting (never model inputs
# of the task they describe).
DIAGNOSTIC_COLUMNS = ["fans", "review_count"]


def _log1p(col_name: str) -> Column:
    return F.log1p(F.greatest(F.col(col_name), F.lit(0)).cast("double"))


def _per_review(col_name: str) -> Column:
    return F.coalesce(
        F.try_divide(F.col(col_name).cast("double"), F.col("review_count")),
        F.lit(0.0),
    )


def elite_years(elite: Column) -> Column:
    """
    Distinct elite years. The raw data stores 2020 as "20,20"
    (e.g. "2019,20,20,2021"), so every lone "20" token means 2020.
    """
    tokens = F.transform(
        F.split(F.coalesce(elite, F.lit("")), ","),
        lambda t: F.when(F.trim(t) == "20", F.lit("2020")).otherwise(F.trim(t)),
    )
    return F.array_distinct(
        F.filter(tokens, lambda t: (t != "") & (t != "None"))
    )


def friend_count(friends: Column) -> Column:
    """'None' or a comma-separated list of user ids -> number of friends."""
    trimmed = F.trim(F.coalesce(friends, F.lit("None")))
    return F.when(trimmed.isin("None", ""), F.lit(0)).otherwise(
        F.size(F.split(trimmed, r"\s*,\s*"))
    )


def snapshot_date(user: DataFrame):
    """Latest sign-up date: the 'today' of the dataset snapshot."""
    since = F.try_to_date(F.substring("yelping_since", 1, 10), "yyyy-MM-dd")
    return user.select(F.max(since).alias("d")).first()["d"]


def build_user_features(user: DataFrame) -> DataFrame:
    """One row per user with both targets and all candidate features."""
    snapshot = snapshot_date(user)
    since = F.try_to_date(F.substring("yelping_since", 1, 10), "yyyy-MM-dd")
    compliments_total = sum(
        (F.col(f"compliment_{c}") for c in COMPLIMENTS), F.lit(0)
    )
    years = elite_years(F.col("elite"))

    base = (
        user.dropDuplicates([ID_COLUMN])
        .filter(F.col(ID_COLUMN).isNotNull())
        .fillna(0)
        .withColumn("compliments_total", compliments_total)
    )
    return base.select(
        ID_COLUMN,
        # Targets
        _log1p("fans").alias(REGRESSION_LABEL),
        (F.size(years) > 0).cast("double").alias(CLASSIFICATION_LABEL),
        # Raw values for reporting
        F.col("fans").cast("long").alias("fans"),
        F.col("review_count").cast("long").alias("review_count"),
        F.size(years).cast("double").alias("n_elite_years"),
        # Activity
        _log1p("review_count").alias("log_review_count"),
        (F.datediff(F.lit(snapshot), since) / 365.25).alias("years_on_yelp"),
        F.log1p(friend_count(F.col("friends")).cast("double")).alias(
            "log_friends"
        ),
        F.col("average_stars").cast("double").alias("average_stars"),
        # Votes
        _log1p("useful").alias("log_useful"),
        _log1p("funny").alias("log_funny"),
        _log1p("cool").alias("log_cool"),
        _per_review("useful").alias("useful_per_review"),
        _per_review("funny").alias("funny_per_review"),
        _per_review("cool").alias("cool_per_review"),
        # Compliments
        *[
            _log1p(f"compliment_{c}").alias(f"log_compliment_{c}")
            for c in COMPLIMENTS
        ],
        _log1p("compliments_total").alias("log_compliments_total"),
        _per_review("compliments_total").alias("compliments_per_review"),
    )


def check_features(features: DataFrame) -> None:
    """Fail fast on duplicate ids or impossible values."""
    row = features.select(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(ID_COLUMN).alias("ids"),
        F.min("years_on_yelp").alias("min_years"),
    ).first()
    if row["rows"] != row["ids"]:
        raise ValueError("Feature table has duplicate user ids")
    if row["min_years"] is not None and row["min_years"] < 0:
        raise ValueError("Sign-up date after the snapshot date")


def load_or_build_features(
    spark: SparkSession,
    path: Path = ML_FEATURES_PATH,
    rebuild: bool = False,
) -> DataFrame:
    """
    Read the cached feature table, building it first if missing.

    Building scans the 3.4 GB user file once (most of it is the friend
    lists), so the result is persisted as Parquet under
    artifacts/processed/ml/.
    """
    if rebuild or not (path / "_SUCCESS").exists():
        features = build_user_features(load_dataset(spark, "user"))
        features.write.mode("overwrite").parquet(str(path))
    features = spark.read.parquet(str(path))
    check_features(features)
    return features
