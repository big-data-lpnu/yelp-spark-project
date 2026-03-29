"""Flatten nested JSON/map/array columns into dataset columns."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Common Yelp business attribute keys (subset to avoid column explosion)
COMMON_ATTRIBUTE_KEYS = [
    "GoodForKids",
    "RestaurantsPriceRange2",
    "BikeParking",
    "WiFi",
    "BusinessParking",
    "Ambience",
    "RestaurantsGoodForGroups",
    "OutdoorSeating",
    "HasTV",
    "RestaurantsReservations",
    "Alcohol",
    "RestaurantsDelivery",
    "RestaurantsTakeOut",
    "GoodForMeal",
    "ByAppointmentOnly",
    "RestaurantsTableService",
    "Caters",
    "NoiseLevel",
    "RestaurantsAttire",
    "GoodForDancing",
    "DriveThru",
    "Smoking",
    "WheelchairAccessible",
    "AcceptCreditCards",
    "Open24Hours",
    "DogsAllowed",
    "BYOB",
    "Corkage",
    "BYOBCorkage",
    "HairSpecializesIn",
    "RestaurantsCounterService",
    "AgesAllowed",
    "Music",
]

# Hours map keys
HOURS_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _safe_col_name(key: str) -> str:
    """Turn attribute key into safe column name."""
    return "attr_" + key.replace(".", "_").replace(" ", "_").replace("-", "_")[:50]


def flatten_business(df: DataFrame) -> DataFrame:
    """
    Flatten business: attributes map -> one column per common key, hours map ->
    open_days_count + columns per day, categories -> category_count.
    """
    if "attributes" not in df.columns:
        return df

    # Attributes: add column for each common key
    for key in COMMON_ATTRIBUTE_KEYS:
        col_name = _safe_col_name(key)
        df = df.withColumn(
            col_name,
            F.col("attributes").getItem(key).cast(StringType()),
        )

    # Hours: open_days_count and optionally per-day (e.g. has_hours_Monday)
    if "hours" in df.columns:
        df = df.withColumn(
            "open_days_count",
            F.size(F.map_keys(F.col("hours"))),
        )
        for day in HOURS_DAYS:
            safe = "hours_" + day.lower()[:3]
            df = df.withColumn(
                safe,
                F.col("hours").getItem(day),
            )

    # Categories: count and keep array (optional: one-hot top N in pipeline)
    if "categories" in df.columns:
        df = df.withColumn(
            "category_count",
            F.when(F.col("categories").isNull(), 0).otherwise(
                F.size(F.col("categories"))
            ),
        )

    return df


def flatten_user(df: DataFrame) -> DataFrame:
    """Flatten user: friends -> friend_count, elite -> elite_count and is_elite."""
    if "friends" in df.columns:
        df = df.withColumn(
            "friend_count",
            F.when(F.col("friends").isNull(), 0).otherwise(
                F.size(F.col("friends"))
            ),
        )
    if "elite" in df.columns:
        df = df.withColumn(
            "elite_count",
            F.when(F.col("elite").isNull(), 0).otherwise(
                F.size(F.col("elite"))
            ),
        )
        df = df.withColumn(
            "is_elite",
            F.when(F.col("elite_count") > 0, 1).otherwise(0),
        )
    return df


def flatten_checkin(df: DataFrame) -> DataFrame:
    """
    Flatten checkin: date is comma-separated list of timestamps -> checkin_count
    and optionally day-of-week counts.
    """
    if "date" not in df.columns:
        return df

    # Split by comma and count
    df = df.withColumn(
        "date_array",
        F.split(F.trim(F.col("date")), r"\s*,\s*"),
    )
    df = df.withColumn(
        "checkin_count",
        F.size(F.col("date_array")),
    )
    # Optionally explode and get hour/day distribution; for simplicity keep count
    df = df.drop("date_array")
    return df


def flatten_table(df: DataFrame, table: str) -> DataFrame:
    """Dispatch flattening by table name."""
    if table == "business":
        return flatten_business(df)
    if table == "user":
        return flatten_user(df)
    if table == "checkin":
        return flatten_checkin(df)
    return df
