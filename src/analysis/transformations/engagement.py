"""
Engagement Analytics
========================================
Six business questions focused on check-ins, tips, and photos:
  Q1  filter              High-compliment tips written after 2020
  Q2  group by            Top 20 most checked-in businesses
  Q3  window              Rank businesses by tip count within each city
  Q4  join   + group by   Businesses with both tips and photos - counts per business
  Q5  join   + filter     Open businesses (stars ≥ 4.0) with at least 10 tips
  Q6  window              Lag between consecutive tips per user (days gap)
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum as _sum,
    desc,
    rank,
    lag,
    datediff,
    to_date,
    split,
    size,
    lit,
)
from pyspark.sql.window import Window

from src.analysis.transformations.utils import save_csv


# ---------------------------------------------------------------------------
# Q1 - filter
# Tips with compliment_count ≥ 3 written on or after 2020-01-01
# ---------------------------------------------------------------------------
def q1_recent_popular_tips(df_tip: DataFrame) -> DataFrame:
    """
    Business question:
        Which tips written since 2020 have received the most compliments -
        showing what kind of advice the community finds most valuable recently?

    Operations used: filter, orderBy, limit
    """
    result = (
        df_tip.filter(
            (col("compliment_count") >= 3) & (col("date") >= lit("2020-01-01"))
        )
        .select("user_id", "business_id", "compliment_count", "date", "text")
        .orderBy(desc("compliment_count"))
        .limit(20)
    )

    print(
        "\n=== Q1 Execution Plan: Recent popular tips (2020+, ≥3 compliments) ==="
    )
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q2 - group by
# Top 20 businesses by total check-in count
# The 'date' field in checkin is a comma-separated string of timestamps
# ---------------------------------------------------------------------------
def q2_most_visited_businesses(
    df_checkin: DataFrame, df_business: DataFrame
) -> DataFrame:
    """
    Business question:
        Which 20 businesses have been checked into the most times on Yelp -
        the most physically visited places in the dataset?

    Operations used: withColumn (split+size), groupBy, join, orderBy, limit
    """
    checkin_counts = (
        df_checkin.withColumn(
            "single_checkin_count", size(split(col("date"), ","))
        )
        .groupBy("business_id")
        .agg(_sum("single_checkin_count").alias("total_checkins"))
    )

    result = (
        checkin_counts.join(
            df_business.select(
                "business_id", "name", "city", "state", "is_open"
            ),
            "business_id",
        )
        .orderBy(desc("total_checkins"))
        .limit(20)
    )

    print("\n=== Q2 Execution Plan: Top 20 most checked-in businesses ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q3 - window
# Top 3 businesses by tip count within each city
# ---------------------------------------------------------------------------
def q3_top_tipped_businesses_per_city(
    df_tip: DataFrame, df_business: DataFrame
) -> DataFrame:
    """
    Business question:
        In each city, which businesses have received the most tips from visitors?
        Show the top 3 per city.

    Operations used: groupBy, join, window (rank), filter, orderBy, limit
    """
    tip_counts = df_tip.groupBy("business_id").agg(
        count("text").alias("tip_count")
    )

    window_city = Window.partitionBy("city").orderBy(desc("tip_count"))

    result = (
        tip_counts.join(
            df_business.select("business_id", "name", "city", "state"),
            "business_id",
        )
        .withColumn("rank_in_city", rank().over(window_city))
        .filter(col("rank_in_city") <= 3)
        .select("city", "state", "name", "tip_count", "rank_in_city")
        .orderBy("city", "rank_in_city")
        .limit(60)
    )

    print("\n=== Q3 Execution Plan: Top 3 tipped businesses per city ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q4 - join + group by
# Businesses with both tips and photos - side-by-side engagement counts
# ---------------------------------------------------------------------------
def q4_businesses_with_tips_and_photos(
    df_tip: DataFrame, df_photo: DataFrame, df_business: DataFrame
) -> DataFrame:
    """
    Business question:
        Which businesses have generated both written tips and photo uploads,
        and what are the counts of each engagement type per business?

    Operations used: groupBy, join (multiple), orderBy, limit
    """
    tip_counts = df_tip.groupBy("business_id").agg(
        count("text").alias("tip_count")
    )

    photo_counts = df_photo.groupBy("business_id").agg(
        count("photo_id").alias("photo_count")
    )

    result = (
        tip_counts.join(photo_counts, "business_id")
        .join(
            df_business.select("business_id", "name", "city", "state", "stars"),
            "business_id",
        )
        .orderBy(desc("tip_count"))
        .limit(20)
    )

    print("\n=== Q4 Execution Plan: Businesses with tips and photos ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q5 - join + filter
# Currently open, highly-rated businesses (stars ≥ 4.0) with ≥ 10 tips
# ---------------------------------------------------------------------------
def q5_open_quality_businesses_with_tips(
    df_tip: DataFrame, df_business: DataFrame
) -> DataFrame:
    """
    Business question:
        Which currently open businesses with a rating of 4.0 or higher have
        also received at least 10 tips from visitors?

    Operations used: filter, groupBy, join, orderBy, limit
    """
    tip_counts = df_tip.groupBy("business_id").agg(
        count("text").alias("tip_count")
    )

    result = (
        df_business.filter((col("is_open") == 1) & (col("stars") >= 4.0))
        .join(tip_counts, "business_id")
        .filter(col("tip_count") >= 10)
        .select("name", "city", "state", "stars", "review_count", "tip_count")
        .orderBy(desc("tip_count"))
        .limit(20)
    )

    print("\n=== Q5 Execution Plan: Open quality businesses with 10+ tips ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q6 - window
# Days between consecutive tips left by the same user (lag function)
# ---------------------------------------------------------------------------
def q6_tip_frequency_per_user(df_tip: DataFrame) -> DataFrame:
    """
    Business question:
        How frequently do users leave tips? For each tip, how many days passed
        since the same user's previous tip?

    Operations used: window (lag), datediff, filter (nulls), orderBy, limit
    """
    window_user = Window.partitionBy("user_id").orderBy("date")

    result = (
        df_tip.withColumn("prev_tip_date", lag("date", 1).over(window_user))
        .withColumn(
            "days_since_prev_tip",
            datediff(to_date(col("date")), to_date(col("prev_tip_date"))),
        )
        .filter(col("prev_tip_date").isNotNull())
        .select(
            "user_id",
            "business_id",
            "date",
            "prev_tip_date",
            "days_since_prev_tip",
        )
        .orderBy("user_id", "date")
        .limit(30)
    )

    print("\n=== Q6 Execution Plan: Days between consecutive tips per user ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Wrapper - run every question and persist results
# ---------------------------------------------------------------------------
def run_all(
    df_business: DataFrame,
    df_checkin: DataFrame,
    df_tip: DataFrame,
    df_photo: DataFrame,
    results_dir: Path,
) -> None:
    """Run all six questions sequentially and save results as CSV."""
    out = results_dir / "engagement"
    out.mkdir(parents=True, exist_ok=True)

    save_csv(q1_recent_popular_tips(df_tip), out, "q1_recent_popular_tips")
    save_csv(
        q2_most_visited_businesses(df_checkin, df_business),
        out,
        "q2_most_visited_businesses",
    )
    save_csv(
        q3_top_tipped_businesses_per_city(df_tip, df_business),
        out,
        "q3_top_tipped_businesses_per_city",
    )
    save_csv(
        q4_businesses_with_tips_and_photos(df_tip, df_photo, df_business),
        out,
        "q4_businesses_with_tips_and_photos",
    )
    save_csv(
        q5_open_quality_businesses_with_tips(df_tip, df_business),
        out,
        "q5_open_quality_businesses_with_tips",
    )
    save_csv(
        q6_tip_frequency_per_user(df_tip), out, "q6_tip_frequency_per_user"
    )
