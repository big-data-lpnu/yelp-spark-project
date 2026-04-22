"""
Review Analytics
===================================
Six business questions focused on review trends and quality:
  Q1  filter              Highly-voted reviews (useful+funny+cool > 10, stars ≥ 4)
  Q2  group by            Review volume and average stars by year-month
  Q3  window              Most recent review per business (row_number)
  Q4  join   + group by   Top 20 businesses by total reviews and average score
  Q5  join   + filter     5-star reviews for businesses in the "Food" category
  Q6  window              Cumulative review count per business over time
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    avg,
    desc,
    row_number,
    date_format,
    to_date,
)
from pyspark.sql.window import Window

from src.analysis.transformations.utils import save_csv


# ---------------------------------------------------------------------------
# Q1 - filter
# Highly-voted reviews (useful + funny + cool > 10) with stars ≥ 4
# ---------------------------------------------------------------------------
def q1_highly_voted_reviews(df_review: DataFrame) -> DataFrame:
    """
    Business question:
        Which reviews were most appreciated by the community - having
        combined useful, funny, and cool votes above 10 with a high star rating?

    Operations used: filter, orderBy, limit
    """
    result = (
        df_review.filter(
            (col("useful") + col("funny") + col("cool") > 10)
            & (col("stars") >= 4)
        )
        .select(
            "review_id",
            "business_id",
            "user_id",
            "stars",
            "useful",
            "funny",
            "cool",
            "date",
        )
        .orderBy(desc("useful"))
        .limit(20)
    )

    print("\n=== Q1 Execution Plan: Highly-voted positive reviews ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q2 - group by
# Review volume and average stars grouped by year-month
# ---------------------------------------------------------------------------
def q2_reviews_by_month(df_review: DataFrame) -> DataFrame:
    """
    Business question:
        How has review activity and average rating changed month-over-month
        throughout the history of Yelp data?

    Operations used: withColumn (date formatting), groupBy, agg, orderBy
    """
    result = (
        df_review.withColumn(
            "year_month", date_format(to_date(col("date")), "yyyy-MM")
        )
        .groupBy("year_month")
        .agg(
            count("review_id").alias("review_count"),
            avg("stars").alias("avg_stars"),
        )
        .orderBy("year_month")
    )

    print("\n=== Q2 Execution Plan: Reviews by year-month ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q3 - window
# Most recent review per business (row_number, ordered by date desc)
# ---------------------------------------------------------------------------
def q3_latest_review_per_business(df_review: DataFrame) -> DataFrame:
    """
    Business question:
        What is the single most recent review for each business on Yelp?

    Operations used: window (row_number), filter, orderBy, limit
    """
    window_biz = Window.partitionBy("business_id").orderBy(desc("date"))

    result = (
        df_review.withColumn("recency_rank", row_number().over(window_biz))
        .filter(col("recency_rank") == 1)
        .select(
            "business_id",
            "review_id",
            "user_id",
            "stars",
            "date",
            "recency_rank",
        )
        .orderBy("date")
        .limit(30)
    )

    print("\n=== Q3 Execution Plan: Most recent review per business ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q4 - join + group by
# Top 20 businesses by total number of reviews and their average score
# ---------------------------------------------------------------------------
def q4_top_businesses_by_reviews(
    df_business: DataFrame, df_review: DataFrame
) -> DataFrame:
    """
    Business question:
        Which 20 businesses have attracted the most reviews, and what is the
        community's average rating for each of them?

    Operations used: groupBy, join, filter, orderBy, limit
    """
    review_stats = df_review.groupBy("business_id").agg(
        count("review_id").alias("total_reviews"),
        avg("stars").alias("avg_review_stars"),
    )

    result = (
        review_stats.join(
            df_business.select("business_id", "name", "city", "state", "stars"),
            "business_id",
        )
        .filter(col("total_reviews") >= 50)
        .orderBy(desc("total_reviews"))
        .limit(20)
    )

    print("\n=== Q4 Execution Plan: Top 20 businesses by review count ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q5 - join + filter
# 5-star reviews for businesses categorised under "Food"
# ---------------------------------------------------------------------------
def q5_five_star_food_reviews(
    df_business: DataFrame, df_review: DataFrame
) -> DataFrame:
    """
    Business question:
        Which 5-star reviews were written for businesses in the "Food" category,
        and how useful were those reviews to other users?

    Operations used: filter, join, orderBy, limit
    """
    food_businesses = df_business.filter(
        col("categories").contains("Food")
    ).select("business_id", "name", "city", "state")

    result = (
        df_review.filter(col("stars") == 5)
        .join(food_businesses, "business_id")
        .select(
            "name", "city", "state", "stars", "useful", "funny", "cool", "date"
        )
        .orderBy(desc("useful"))
        .limit(30)
    )

    print("\n=== Q5 Execution Plan: 5-star reviews for Food businesses ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q6 - window
# Cumulative review count per business ordered by review date
# ---------------------------------------------------------------------------
def q6_cumulative_reviews_per_business(df_review: DataFrame) -> DataFrame:
    """
    Business question:
        How has the cumulative number of reviews grown over time for each
        business - showing the running total ordered by review date?

    Operations used: window (count with rowsBetween), orderBy, limit
    """
    window_running = (
        Window.partitionBy("business_id")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    result = (
        df_review.withColumn(
            "cumulative_reviews", count("review_id").over(window_running)
        )
        .select(
            "business_id", "review_id", "date", "stars", "cumulative_reviews"
        )
        .orderBy("business_id", "date")
        .limit(30)
    )

    print("\n=== Q6 Execution Plan: Cumulative reviews per business ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Wrapper - run every question and persist results
# ---------------------------------------------------------------------------
def run_all(
    df_business: DataFrame, df_review: DataFrame, results_dir: Path
) -> None:
    """Run all six questions sequentially and save results as CSV."""
    out = results_dir / "review"
    out.mkdir(parents=True, exist_ok=True)

    save_csv(q1_highly_voted_reviews(df_review), out, "q1_highly_voted_reviews")
    save_csv(q2_reviews_by_month(df_review), out, "q2_reviews_by_month")
    save_csv(
        q3_latest_review_per_business(df_review),
        out,
        "q3_latest_review_per_business",
    )
    save_csv(
        q4_top_businesses_by_reviews(df_business, df_review),
        out,
        "q4_top_businesses_by_reviews",
    )
    save_csv(
        q5_five_star_food_reviews(df_business, df_review),
        out,
        "q5_five_star_food_reviews",
    )
    save_csv(
        q6_cumulative_reviews_per_business(df_review),
        out,
        "q6_cumulative_reviews_per_business",
    )
