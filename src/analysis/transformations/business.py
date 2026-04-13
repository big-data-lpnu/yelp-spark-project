"""
Business Analytics
====================================
Six business questions focused on business-level insights:
  Q1  filter + group by   Top 10 cities by number of open businesses
  Q2  filter + window     Top 5 businesses by review count per state (open only)
  Q3  join   + group by   Average review stars per state
  Q4  join   + filter     Top businesses in Pennsylvania with ≥ 100 reviews
  Q5  window              Businesses vs. their city average stars (delta)
  Q6  filter              Open restaurants rated ≥ 4.5 with ≥ 100 reviews
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    avg,
    desc,
    rank,
)
from pyspark.sql.window import Window

from src.analysis.transformations.utils import save_csv


# ---------------------------------------------------------------------------
# Q1 - filter + group by
# Top 10 cities by number of currently open businesses
# ---------------------------------------------------------------------------
def q1_top_cities_by_open_businesses(df_business: DataFrame) -> DataFrame:
    """
    Business question:
        Which 10 cities have the most currently open businesses on Yelp?

    Operations used: filter, groupBy, orderBy, limit
    """
    result = (
        df_business.filter(col("is_open") == 1)
        .groupBy("city")
        .agg(count("business_id").alias("open_business_count"))
        .orderBy(desc("open_business_count"))
        .limit(10)
    )

    print("\n=== Q1 Execution Plan: Top 10 cities by open businesses ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q2 - filter + window
# Top 5 businesses by review count per state (open businesses only)
# ---------------------------------------------------------------------------
def q2_top_businesses_per_state(df_business: DataFrame) -> DataFrame:
    """
    Business question:
        Among currently open businesses, which are the top 5 most-reviewed
        in each US state?

    Operations used: filter, window (rank), orderBy
    """
    window_spec = Window.partitionBy("state").orderBy(desc("review_count"))

    result = (
        df_business.filter(col("is_open") == 1)
        .withColumn("rank_in_state", rank().over(window_spec))
        .filter(col("rank_in_state") <= 5)
        .select(
            "state", "name", "city", "stars", "review_count", "rank_in_state"
        )
        .orderBy("state", "rank_in_state")
    )

    print("\n=== Q2 Execution Plan: Top 5 businesses per state (open) ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q3 - join + group by
# Average review stars per state (business + review join)
# ---------------------------------------------------------------------------
def q3_avg_review_stars_per_state(
    df_business: DataFrame, df_review: DataFrame
) -> DataFrame:
    """
    Business question:
        What is the average review rating given to businesses in each US state,
        and how many reviews have been written per state?

    Operations used: join, groupBy, orderBy
    """
    result = (
        df_business.select("business_id", "state")
        .join(
            df_review.select("business_id", "stars", "review_id"), "business_id"
        )
        .groupBy("state")
        .agg(
            avg("stars").alias("avg_review_stars"),
            count("review_id").alias("total_reviews"),
        )
        .orderBy(desc("total_reviews"))
    )

    print("\n=== Q3 Execution Plan: Average review stars per state ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q4 - join + filter
# Top 20 most-reviewed businesses in Pennsylvania (PA)
# ---------------------------------------------------------------------------
def q4_top_businesses_in_pennsylvania(
    df_business: DataFrame, df_review: DataFrame
) -> DataFrame:
    """
    Business question:
        Which businesses in Pennsylvania have received at least 100 reviews,
        and what is their average rating from those reviews?

    Operations used: filter, join, groupBy, orderBy, limit
    """
    pa_businesses = df_business.filter(col("state") == "PA").select(
        "business_id", "name", "city"
    )

    result = (
        df_review.select("business_id", "review_id", "stars")
        .join(pa_businesses, "business_id")
        .groupBy("business_id", "name", "city")
        .agg(
            count("review_id").alias("total_reviews"),
            avg("stars").alias("avg_stars"),
        )
        .filter(col("total_reviews") >= 100)
        .orderBy(desc("total_reviews"))
        .limit(20)
    )

    print("\n=== Q4 Execution Plan: Top businesses in Pennsylvania ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q5 - window
# Each business star rating compared to its city's average (delta)
# ---------------------------------------------------------------------------
def q5_business_vs_city_avg_stars(df_business: DataFrame) -> DataFrame:
    """
    Business question:
        For each business, how does its star rating compare to the average
        rating of all businesses in the same city?

    Operations used: window (avg), orderBy, limit
    """
    window_city = Window.partitionBy("city")

    result = (
        df_business.withColumn("city_avg_stars", avg("stars").over(window_city))
        .withColumn("diff_from_city_avg", col("stars") - col("city_avg_stars"))
        .select(
            "name",
            "city",
            "state",
            "stars",
            "city_avg_stars",
            "diff_from_city_avg",
        )
        .orderBy(desc("diff_from_city_avg"))
        .limit(30)
    )

    print("\n=== Q5 Execution Plan: Business stars vs city average ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q6 - filter
# Open restaurants rated ≥ 4.5 with ≥ 100 reviews
# ---------------------------------------------------------------------------
def q6_top_open_restaurants(df_business: DataFrame) -> DataFrame:
    """
    Business question:
        Which currently open restaurants have a rating of 4.5 stars or higher
        and at least 100 reviews - the best and most popular places to eat?

    Operations used: filter, orderBy
    """
    result = (
        df_business.filter(
            (col("is_open") == 1)
            & (col("stars") >= 4.5)
            & (col("review_count") >= 100)
            & col("categories").contains("Restaurants")
        )
        .select("name", "city", "state", "stars", "review_count")
        .orderBy(desc("review_count"))
    )

    print(
        "\n=== Q6 Execution Plan: Top open restaurants (4.5+, 100+ reviews) ==="
    )
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Wrapper - run every question and persist results
# ---------------------------------------------------------------------------
def run_all(
    df_business: DataFrame, df_review: DataFrame, results_dir: Path
) -> None:
    """Run all six questions sequentially and save results as CSV."""
    out = results_dir / "business"
    out.mkdir(parents=True, exist_ok=True)

    save_csv(
        q1_top_cities_by_open_businesses(df_business),
        out,
        "q1_top_cities_by_open_businesses",
    )
    save_csv(
        q2_top_businesses_per_state(df_business),
        out,
        "q2_top_businesses_per_state",
    )
    save_csv(
        q3_avg_review_stars_per_state(df_business, df_review),
        out,
        "q3_avg_review_stars_per_state",
    )
    save_csv(
        q4_top_businesses_in_pennsylvania(df_business, df_review),
        out,
        "q4_top_businesses_in_pennsylvania",
    )
    save_csv(
        q5_business_vs_city_avg_stars(df_business),
        out,
        "q5_business_vs_city_avg_stars",
    )
    save_csv(
        q6_top_open_restaurants(df_business), out, "q6_top_open_restaurants"
    )
