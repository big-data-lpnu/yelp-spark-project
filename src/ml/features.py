"""
Business-level feature table for the ML stage.

One row per business, built from the raw business table plus aggregates of
reviews, tips, check-ins and photos. Only label-free, row-wise or per-business
transformations happen here; everything that learns statistics from the data
(imputation, indexing, scaling, category vocabulary) lives in the Spark ML
pipeline in ``src.ml.pipeline`` and is fitted on the training split only.

Spark 4 runs with ANSI mode on, so string -> number conversions use
``try_cast``, array access uses ``get`` and division uses ``try_divide``:
the strict variants throw on dirty values instead of returning NULL.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.constants import ML_FEATURES_PATH
from src.spark.load_data import load_dataset

# ---------------------------------------------------------------------------
# Raw attribute groups
# ---------------------------------------------------------------------------

# Attributes stored as python-repr booleans ("True" / "False" / "None").
BOOLEAN_ATTRIBUTES = [
    "BusinessAcceptsCreditCards",
    "BikeParking",
    "RestaurantsTakeOut",
    "RestaurantsDelivery",
    "GoodForKids",
    "OutdoorSeating",
    "RestaurantsReservations",
    "HasTV",
    "RestaurantsGoodForGroups",
    "ByAppointmentOnly",
    "Caters",
    "WheelchairAccessible",
    "RestaurantsTableService",
    "DogsAllowed",
    "BusinessAcceptsBitcoin",
    "HappyHour",
    "DriveThru",
]

# Attributes stored as quoted strings (u'free', 'no', ...) -> categorical.
CATEGORICAL_ATTRIBUTES = {
    "WiFi": "wifi",
    "NoiseLevel": "noise_level",
    "Alcohol": "alcohol",
    "RestaurantsAttire": "attire",
}

# Attributes stored as python-repr dicts -> one tri-state flag per sub-key.
NESTED_ATTRIBUTES = {
    "Ambience": (
        "ambience",
        [
            "casual",
            "classy",
            "romantic",
            "intimate",
            "trendy",
            "upscale",
            "hipster",
            "divey",
            "touristy",
        ],
    ),
    "BusinessParking": (
        "parking",
        ["garage", "street", "validated", "lot", "valet"],
    ),
    "GoodForMeal": (
        "meal",
        ["dessert", "latenight", "lunch", "dinner", "brunch", "breakfast"],
    ),
}

HOURS_DAYS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

PHOTO_LABELS = ["food", "inside", "outside", "drink", "menu"]

# States with fewer businesses than this are folded into "OTHER" (the raw
# data has ~13 junk states with 1-4 rows each).
MIN_STATE_SIZE = 100

# ---------------------------------------------------------------------------
# Feature groups consumed by src.ml.pipeline
# ---------------------------------------------------------------------------

ID_COLUMN = "business_id"
REGRESSION_LABEL = "stars"
CLASSIFICATION_LABEL = "is_closed"

ATTRIBUTE_FLAG_FEATURES = [f"attr_{a}" for a in BOOLEAN_ATTRIBUTES] + [
    f"{prefix}_{key}"
    for prefix, keys in NESTED_ATTRIBUTES.values()
    for key in keys
]

# Describe what the business is: location, size, category, amenities, hours.
PROFILE_NUMERIC_FEATURES = [
    "log_review_count",
    "latitude",
    "longitude",
    "n_categories",
    "n_attributes",
    "price_range",
    "has_hours",
    "n_open_days",
    "weekly_open_hours",
    "open_weekend",
    "open_late",
    "opens_early",
    "hours_zero_format",
    *ATTRIBUTE_FLAG_FEATURES,
]

# Describe how customers interact with the business (no star ratings!).
ENGAGEMENT_NUMERIC_FEATURES = [
    "avg_review_useful",
    "avg_review_funny",
    "avg_review_cool",
    "avg_review_length",
    "log_n_tips",
    "avg_tip_compliments",
    "log_n_checkins",
    "log_n_photos",
    *[f"photo_share_{label}" for label in PHOTO_LABELS],
    # Exposure time: snapshot minus FIRST review. Not cut short by closure.
    "business_age_days",
]

# Everything that looks at the LAST activity date. A closed business stops
# receiving reviews/tips/check-ins, so these are largely a consequence of the
# label rather than a cause: the classification task is reported both with
# them ("stale listing detection") and without them.
RECENCY_FEATURES = [
    "days_since_last_review",
    "share_reviews_last_year",
    "days_since_last_tip",
    "days_since_last_checkin",
    "share_checkins_last_year",
]

CATEGORICAL_FEATURES = ["state", *CATEGORICAL_ATTRIBUTES.values()]

# Array<string> column turned into a binary bag of categories by a
# CountVectorizer fitted on the training split.
CATEGORY_ARRAY_FEATURE = "category_list"


# ---------------------------------------------------------------------------
# Business table parsing
# ---------------------------------------------------------------------------


def _strip_quotes(value: Column) -> Column:
    """u'free' / 'free' / free -> free (lower-cased, trimmed)."""
    return F.lower(F.trim(F.regexp_replace(value, r"^u?'(.*)'$", "$1")))


def _tri_state(is_true: Column, is_false: Column) -> Column:
    """True -> 1.0, False -> -1.0, missing/unknown -> 0.0."""
    return (
        F.when(is_true, F.lit(1.0))
        .when(is_false, F.lit(-1.0))
        .otherwise(F.lit(0.0))
    )


def parse_attributes(df: DataFrame) -> DataFrame:
    """Expand the ``attributes`` map into flat numeric/categorical columns."""
    attrs = F.col("attributes")
    cols = {
        "n_attributes": F.coalesce(F.size(F.map_keys(attrs)), F.lit(0)).cast(
            "double"
        )
    }

    for key in BOOLEAN_ATTRIBUTES:
        value = attrs.getItem(key)
        cols[f"attr_{key}"] = _tri_state(value == "True", value == "False")

    price = F.expr("try_cast(attributes['RestaurantsPriceRange2'] AS INT)")
    cols["price_range"] = F.when(price.between(1, 4), price.cast("double"))

    for key, name in CATEGORICAL_ATTRIBUTES.items():
        raw = attrs.getItem(key)
        value = _strip_quotes(raw)
        # Bare None is "unknown"; quoted 'none' is a real level
        # (e.g. Alcohol u'none' = no alcohol served).
        cols[name] = F.when(
            raw.isNull() | (raw == "None") | (value == ""), F.lit("missing")
        ).otherwise(value)

    for key, (prefix, sub_keys) in NESTED_ATTRIBUTES.items():
        value = attrs.getItem(key)
        for sub in sub_keys:
            cols[f"{prefix}_{sub}"] = _tri_state(
                F.coalesce(value.rlike(rf"'{sub}':\s*True"), F.lit(False)),
                F.coalesce(value.rlike(rf"'{sub}':\s*False"), F.lit(False)),
            )
    return df.withColumns(cols)


def _hhmm_to_minutes(sql_time: str) -> Column:
    """SQL string expression 'H:M' -> minutes after midnight (NULL if bad)."""
    return F.expr(
        f"try_cast(get(split({sql_time}, ':'), 0) AS INT) * 60"
        f" + try_cast(get(split({sql_time}, ':'), 1) AS INT)"
    )


def parse_hours(df: DataFrame) -> DataFrame:
    """
    Opening-hours features from the ``hours`` map ("8:0-18:30").

    "0:0-0:0" is ambiguous in Yelp data (24 hours vs. not set) and is far more
    common on recently edited listings, so it is excluded from the hours
    arithmetic and exposed as its own ``hours_zero_format`` flag. A closing
    time before the opening time means the business closes after midnight.
    """
    day_hours, late_flags, early_flags, zero_flags = [], [], [], []
    for day in HOURS_DAYS:
        is_zero = F.col("hours").getItem(day) == "0:0-0:0"
        open_min = _hhmm_to_minutes(f"get(split(hours['{day}'], '-'), 0)")
        close_min = _hhmm_to_minutes(f"get(split(hours['{day}'], '-'), 1)")
        open_min = F.when(~is_zero, open_min)
        close_adj = F.when(close_min <= open_min, close_min + 1440).otherwise(
            close_min
        )
        day_hours.append((close_adj - open_min) / 60.0)
        late_flags.append(F.coalesce(close_adj > 22 * 60, F.lit(False)))
        early_flags.append(F.coalesce(open_min < 8 * 60, F.lit(False)))
        zero_flags.append(F.coalesce(is_zero, F.lit(False)))

    weekly = sum((F.coalesce(h, F.lit(0.0)) for h in day_hours), F.lit(0.0))
    open_days = sum(
        (F.when(h.isNotNull(), 1.0).otherwise(0.0) for h in day_hours),
        F.lit(0.0),
    )
    hours = F.col("hours")
    return df.withColumns(
        {
            "has_hours": F.coalesce(F.size(hours) > 0, F.lit(False)).cast(
                "double"
            ),
            "n_open_days": open_days,
            "weekly_open_hours": weekly,
            "open_weekend": (
                hours.getItem("Saturday").isNotNull()
                | hours.getItem("Sunday").isNotNull()
            ).cast("double"),
            "open_late": F.greatest(*late_flags).cast("double"),
            "opens_early": F.greatest(*early_flags).cast("double"),
            "hours_zero_format": F.greatest(*zero_flags).cast("double"),
        }
    )


def parse_categories(df: DataFrame) -> DataFrame:
    """'Restaurants, Pizza' -> ['restaurants', 'pizza'] + category count."""
    category_list = F.filter(
        F.split(F.lower(F.trim(F.col("categories"))), r"\s*,\s*"),
        lambda c: c != "",
    )
    category_list = F.coalesce(category_list, F.array().cast("array<string>"))
    return df.withColumn(CATEGORY_ARRAY_FEATURE, category_list).withColumn(
        "n_categories", F.size(CATEGORY_ARRAY_FEATURE).cast("double")
    )


def fold_rare_states(
    df: DataFrame, min_size: int = MIN_STATE_SIZE
) -> DataFrame:
    """Replace states with fewer than ``min_size`` businesses by 'OTHER'."""
    big_states = (
        df.groupBy("state")
        .count()
        .filter(F.col("count") >= min_size)
        .select("state", F.lit(True).alias("_big"))
    )
    return (
        df.join(F.broadcast(big_states), "state", "left")
        .withColumn(
            "state",
            F.when(F.col("_big"), F.col("state")).otherwise(F.lit("OTHER")),
        )
        .drop("_big")
    )


# ---------------------------------------------------------------------------
# Per-business aggregates of the activity tables
# ---------------------------------------------------------------------------


def _to_date(col_name: str) -> Column:
    """'2018-07-07 22:09:11' -> date (NULL if malformed)."""
    return F.try_to_date(F.substring(F.col(col_name), 1, 10), "yyyy-MM-dd")


def snapshot_date(review: DataFrame) -> Column:
    """Latest review date: the 'today' of the dataset snapshot."""
    return review.select(F.max(_to_date("date")).alias("d")).first()["d"]


def review_aggregates(review: DataFrame, snapshot) -> DataFrame:
    """
    Per-business review statistics. Review stars are deliberately NOT used:
    business stars is (a rounded mean of) them, so any star aggregate would
    leak the regression label.
    """
    last_year = F.date_sub(F.lit(snapshot), 365)
    d = _to_date("date")
    return review.groupBy(ID_COLUMN).agg(
        F.count(F.lit(1)).alias("n_reviews"),
        F.avg("useful").alias("avg_review_useful"),
        F.avg("funny").alias("avg_review_funny"),
        F.avg("cool").alias("avg_review_cool"),
        F.avg(F.length("text")).alias("avg_review_length"),
        F.min(d).alias("first_review_date"),
        F.max(d).alias("last_review_date"),
        F.sum(F.when(d > last_year, 1).otherwise(0)).alias(
            "n_reviews_last_year"
        ),
    )


def tip_aggregates(tip: DataFrame) -> DataFrame:
    return tip.groupBy(ID_COLUMN).agg(
        F.count(F.lit(1)).alias("n_tips"),
        F.avg("compliment_count").alias("avg_tip_compliments"),
        F.max(_to_date("date")).alias("last_tip_date"),
    )


def checkin_aggregates(checkin: DataFrame, snapshot) -> DataFrame:
    """Check-in 'date' is one comma-separated string of timestamps."""
    dates = F.transform(
        F.split(F.col("date"), r"\s*,\s*"),
        lambda s: F.try_to_date(F.substring(F.trim(s), 1, 10), "yyyy-MM-dd"),
    )
    last_year = F.date_sub(F.lit(snapshot), 365)
    per_row = checkin.select(
        ID_COLUMN,
        F.size(dates).alias("n"),
        F.array_max(dates).alias("last"),
        F.size(F.filter(dates, lambda x: x > last_year)).alias("n_last_year"),
    )
    return per_row.groupBy(ID_COLUMN).agg(
        F.sum("n").alias("n_checkins"),
        F.max("last").alias("last_checkin_date"),
        F.sum("n_last_year").alias("n_checkins_last_year"),
    )


def photo_aggregates(photo: DataFrame) -> DataFrame:
    return photo.groupBy(ID_COLUMN).agg(
        F.count(F.lit(1)).alias("n_photos"),
        *[
            F.sum(F.when(F.col("label") == label, 1).otherwise(0)).alias(
                f"n_photos_{label}"
            )
            for label in PHOTO_LABELS
        ],
    )


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def _days_since(snapshot, col_name: str) -> Column:
    return F.datediff(F.lit(snapshot), F.col(col_name)).cast("double")


def build_business_features(
    business: DataFrame,
    review: DataFrame,
    tip: DataFrame,
    checkin: DataFrame,
    photo: DataFrame,
) -> DataFrame:
    """Join business profile with activity aggregates into one ML table."""
    snapshot = snapshot_date(review)

    base = business.dropDuplicates([ID_COLUMN]).filter(
        F.col(ID_COLUMN).isNotNull()
        & F.col("stars").isNotNull()
        & F.col("is_open").isin(0, 1)
    )
    base = parse_categories(parse_hours(parse_attributes(base)))
    base = fold_rare_states(base.fillna({"state": "OTHER"}))

    df = (
        base.join(review_aggregates(review, snapshot), ID_COLUMN, "left")
        .join(tip_aggregates(tip), ID_COLUMN, "left")
        .join(checkin_aggregates(checkin, snapshot), ID_COLUMN, "left")
        .join(photo_aggregates(photo), ID_COLUMN, "left")
        .fillna(
            0,
            subset=[
                "n_reviews",
                "n_reviews_last_year",
                "n_tips",
                "n_checkins",
                "n_checkins_last_year",
                "n_photos",
                *[f"n_photos_{label}" for label in PHOTO_LABELS],
            ],
        )
    )

    def share(part: str, total: str) -> Column:
        """part / total, 0 when the total is 0 (nothing happened)."""
        return F.coalesce(
            F.try_divide(F.col(part).cast("double"), F.col(total)), F.lit(0.0)
        )

    age = _days_since(snapshot, "first_review_date")

    def days_since_or_age(col_name: str) -> Column:
        """Days since last event; 'never' -> as old as the business."""
        return F.coalesce(_days_since(snapshot, col_name), age)

    return df.select(
        ID_COLUMN,
        F.col("stars").cast("double").alias(REGRESSION_LABEL),
        (1 - F.col("is_open")).cast("double").alias(CLASSIFICATION_LABEL),
        "state",
        "latitude",
        "longitude",
        F.log1p(F.col("review_count")).alias("log_review_count"),
        CATEGORY_ARRAY_FEATURE,
        "n_categories",
        "n_attributes",
        "price_range",
        *CATEGORICAL_ATTRIBUTES.values(),
        *ATTRIBUTE_FLAG_FEATURES,
        "has_hours",
        "n_open_days",
        "weekly_open_hours",
        "open_weekend",
        "open_late",
        "opens_early",
        "hours_zero_format",
        "avg_review_useful",
        "avg_review_funny",
        "avg_review_cool",
        "avg_review_length",
        F.log1p("n_tips").alias("log_n_tips"),
        F.coalesce("avg_tip_compliments", F.lit(0.0)).alias(
            "avg_tip_compliments"
        ),
        F.log1p("n_checkins").alias("log_n_checkins"),
        F.log1p("n_photos").alias("log_n_photos"),
        *[
            share(f"n_photos_{label}", "n_photos").alias(f"photo_share_{label}")
            for label in PHOTO_LABELS
        ],
        age.alias("business_age_days"),
        _days_since(snapshot, "last_review_date").alias(
            "days_since_last_review"
        ),
        share("n_reviews_last_year", "n_reviews").alias(
            "share_reviews_last_year"
        ),
        days_since_or_age("last_tip_date").alias("days_since_last_tip"),
        days_since_or_age("last_checkin_date").alias("days_since_last_checkin"),
        share("n_checkins_last_year", "n_checkins").alias(
            "share_checkins_last_year"
        ),
    )


def load_or_build_features(
    spark: SparkSession,
    path: Path = ML_FEATURES_PATH,
    rebuild: bool = False,
) -> DataFrame:
    """
    Read the cached feature table, building it first if missing.

    Building scans the 5 GB review file once (~1-2 min on a laptop), so the
    result is persisted as Parquet under artifacts/processed/ml/.
    """
    if rebuild or not (path / "_SUCCESS").exists():
        features = build_business_features(
            load_dataset(spark, "business"),
            load_dataset(spark, "review"),
            load_dataset(spark, "tip"),
            load_dataset(spark, "checkin"),
            load_dataset(spark, "photo"),
        )
        features.coalesce(4).write.mode("overwrite").parquet(str(path))
    features = spark.read.parquet(str(path))
    check_features(features)
    return features


def check_features(features: DataFrame) -> None:
    """Fail fast on impossible values (e.g. activity after the snapshot)."""
    day_cols = [
        "business_age_days",
        "days_since_last_review",
        "days_since_last_tip",
        "days_since_last_checkin",
    ]
    row = features.select(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(ID_COLUMN).alias("ids"),
        *[F.min(c).alias(c) for c in day_cols],
    ).first()
    if row["rows"] != row["ids"]:
        raise ValueError("Feature table has duplicate business ids")
    negative = [c for c in day_cols if row[c] is not None and row[c] < 0]
    if negative:
        raise ValueError(f"Activity after the snapshot date in {negative}")
