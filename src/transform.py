"""
Transformation Phase — Main Orchestrator
==========================================
Loads all datasets, runs the 24 business-question transformations across
four team members, and writes results to CSV under /results/.
"""

from pathlib import Path

from src.spark.load_data import load_dataset
from src.constants import RESULTS_DIR

import src.analysis.transformations.business as t1
import src.analysis.transformations.user as t2
import src.analysis.transformations.review as t3
import src.analysis.transformations.engagement as t4


def run_transformations(spark) -> None:
    """Load datasets, run all questions, and save CSV results."""
    print("Loading datasets...")
    business = load_dataset(spark, "business").cache()
    review   = load_dataset(spark, "review").cache()
    user     = load_dataset(spark, "user").cache()
    checkin  = load_dataset(spark, "checkin").cache()
    tip      = load_dataset(spark, "tip").cache()
    photo    = load_dataset(spark, "photo").cache()
    print("All datasets loaded and cached.\n")

    results_root = Path(RESULTS_DIR)
    results_root.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 80)
    print("Business Analytics")
    print("=" * 80)
    t1.run_all(business, review, results_root)

    print("\n" + "=" * 80)
    print("User Analytics")
    print("=" * 80)
    t2.run_all(user, review, tip, results_root)

    print("\n" + "=" * 80)
    print("Review Analytics")
    print("=" * 80)
    t3.run_all(business, review, results_root)

    print("\n" + "=" * 80)
    print("Engagement Analytics")
    print("=" * 80)
    t4.run_all(business, checkin, tip, photo, results_root)

    print("\n" + "=" * 80)
    print(f"TRANSFORMATION PHASE COMPLETE — results in: {results_root}")
    print("=" * 80)
