"""
User Analytics
================================
Six business questions focused on user behaviour and influence:
  Q1  filter              Elite users with ≥ 100 fans and ≥ 500 reviews
  Q2  group by            Users grouped by join year - average activity stats
  Q3  window              Rank users by useful votes within their join year
  Q4  join   + group by   Top 20 most active tip writers (tips + user data)
  Q5  join   + filter     Reviews by elite users with stars ≥ 4 and useful ≥ 5
  Q6  window              Percentile rank of users by total compliments received
"""

from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as _sum,
    desc,
    rank,
    percent_rank,
    year,
    to_timestamp,
)
from pyspark.sql.window import Window

from src.analysis.transformations.utils import save_csv


# ---------------------------------------------------------------------------
# Q1 - filter
# Elite users with high fan count and review activity
# ---------------------------------------------------------------------------
def q1_top_elite_users(df_user: DataFrame) -> DataFrame:
    """
    Business question:
        Who are the most influential elite Yelp users - those with at least
        100 fans and 500 reviews written?

    Operations used: filter, orderBy, limit
    """
    result = (
        df_user.filter(
            (col("elite").isNotNull() & (col("elite") != ""))
            & (col("fans") >= 100)
            & (col("review_count") >= 500)
        )
        .select("name", "review_count", "fans", "average_stars", "useful")
        .orderBy(desc("fans"))
        .limit(20)
    )

    print(
        "\n=== Q1 Execution Plan: Top elite users (100+ fans, 500+ reviews) ==="
    )
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q2 - group by
# Average user activity stats grouped by the year they joined Yelp
# ---------------------------------------------------------------------------
def q2_user_stats_by_join_year(df_user: DataFrame) -> DataFrame:
    """
    Business question:
        How do the review habits of Yelp users differ depending on the year
        they joined the platform?

    Operations used: withColumn (year extraction), groupBy, agg, orderBy
    """
    result = (
        df_user.withColumn(
            "join_year", year(to_timestamp(col("yelping_since")))
        )
        .groupBy("join_year")
        .agg(
            count("user_id").alias("user_count"),
            avg("review_count").alias("avg_reviews"),
            avg("fans").alias("avg_fans"),
            avg("average_stars").alias("avg_rating_given"),
            avg("useful").alias("avg_useful_votes"),
        )
        .orderBy("join_year")
    )

    print("\n=== Q2 Execution Plan: User activity stats by join year ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q3 - window
# Top 5 users by useful votes in each year they joined Yelp
# ---------------------------------------------------------------------------
def q3_top_users_by_useful_per_year(df_user: DataFrame) -> DataFrame:
    """
    Business question:
        For each Yelp joining cohort (year), who are the top 5 users that
        received the most 'useful' votes on their reviews?

    Operations used: window (rank), filter, orderBy
    """
    window_year = Window.partitionBy("join_year").orderBy(desc("useful"))

    result = (
        df_user.withColumn(
            "join_year", year(to_timestamp(col("yelping_since")))
        )
        .withColumn("rank_in_cohort", rank().over(window_year))
        .filter(col("rank_in_cohort") <= 5)
        .select(
            "name",
            "join_year",
            "useful",
            "review_count",
            "fans",
            "rank_in_cohort",
        )
        .orderBy("join_year", "rank_in_cohort")
    )

    print(
        "\n=== Q3 Execution Plan: Top 5 users by useful votes per join year ==="
    )
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q4 - join + group by
# Top 20 most active tip writers (enriched with user profile data)
# ---------------------------------------------------------------------------
def q4_top_tip_writers(df_user: DataFrame, df_tip: DataFrame) -> DataFrame:
    """
    Business question:
        Which users have contributed the most tips, and how many compliments
        did those tips receive in total?

    Operations used: groupBy, join, orderBy, limit
    """
    tip_stats = df_tip.groupBy("user_id").agg(
        count("text").alias("tip_count"),
        _sum("compliment_count").alias("total_compliments_on_tips"),
    )

    result = (
        tip_stats.join(
            df_user.select("user_id", "name", "review_count", "fans"),
            "user_id",
        )
        .orderBy(desc("tip_count"))
        .limit(20)
    )

    print("\n=== Q4 Execution Plan: Top 20 tip writers ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q5 - join + filter
# Reviews by elite users rated ≥ 4 stars and marked useful ≥ 5 times
# ---------------------------------------------------------------------------
def q5_useful_reviews_by_elite_users(
    df_user: DataFrame, df_review: DataFrame
) -> DataFrame:
    """
    Business question:
        What are the most useful, highly-rated reviews written by elite Yelp
        users (stars ≥ 4 and useful ≥ 5)?

    Operations used: filter, join, orderBy, limit
    """
    elite_users = df_user.filter(
        col("elite").isNotNull() & (col("elite") != "")
    ).select("user_id", "name")

    result = (
        df_review.filter((col("stars") >= 4) & (col("useful") >= 5))
        .join(elite_users, "user_id")
        .select(
            "name", "business_id", "stars", "useful", "funny", "cool", "date"
        )
        .orderBy(desc("useful"))
        .limit(20)
    )

    print("\n=== Q5 Execution Plan: Useful reviews by elite users ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Q6 - window
# Percentile rank of users by total compliments received across all types
# ---------------------------------------------------------------------------
def q6_user_compliment_percentile(df_user: DataFrame) -> DataFrame:
    """
    Business question:
        How do users rank against each other in terms of total compliments
        received? Who sits in the top percentiles of community appreciation?

    Operations used: window (percent_rank), filter, orderBy, limit
    """
    compliment_cols = [
        "compliment_hot",
        "compliment_more",
        "compliment_profile",
        "compliment_cute",
        "compliment_list",
        "compliment_note",
        "compliment_plain",
        "compliment_cool",
        "compliment_funny",
        "compliment_writer",
        "compliment_photos",
    ]
    total_expr = reduce(lambda a, b: a + b, (col(c) for c in compliment_cols))

    window_all = Window.orderBy(desc("total_compliments"))

    result = (
        df_user.withColumn("total_compliments", total_expr)
        .filter(col("total_compliments") > 0)
        .withColumn("percentile_rank", percent_rank().over(window_all))
        .select(
            "name",
            "fans",
            "review_count",
            "total_compliments",
            "percentile_rank",
        )
        .orderBy(desc("total_compliments"))
        .limit(30)
    )

    print("\n=== Q6 Execution Plan: User compliment percentile rank ===")
    result.explain(extended=True)
    return result


# ---------------------------------------------------------------------------
# Wrapper - run every question and persist results
# ---------------------------------------------------------------------------
def run_all(
    df_user: DataFrame,
    df_review: DataFrame,
    df_tip: DataFrame,
    results_dir: Path,
) -> None:
    """Run all six questions sequentially and save results as CSV."""
    out = results_dir / "user"
    out.mkdir(parents=True, exist_ok=True)

    save_csv(q1_top_elite_users(df_user), out, "q1_top_elite_users")
    save_csv(
        q2_user_stats_by_join_year(df_user), out, "q2_user_stats_by_join_year"
    )
    save_csv(
        q3_top_users_by_useful_per_year(df_user),
        out,
        "q3_top_users_by_useful_per_year",
    )
    save_csv(q4_top_tip_writers(df_user, df_tip), out, "q4_top_tip_writers")
    save_csv(
        q5_useful_reviews_by_elite_users(df_user, df_review),
        out,
        "q5_useful_reviews_by_elite_users",
    )
    save_csv(
        q6_user_compliment_percentile(df_user),
        out,
        "q6_user_compliment_percentile",
    )
