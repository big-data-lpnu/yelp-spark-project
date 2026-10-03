"""
Machine learning stage entry point.

    uv run -m src.ml.run            # full run (~30-40 min on 8 cores)
    uv run -m src.ml.run --quick    # 2 settings per model, no extras

Builds (or reuses) the business feature table, trains 4 regression and
4 classification model families, and writes tables to results/ml/ and the
report artefacts (figures, CSV, markdown digest) to src/reports/ml/.
The notebook src/notebooks/ml_models.ipynb runs the same functions step by
step with inline output.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

os.environ.setdefault("MPLBACKEND", "Agg")  # headless: never open windows

from src.constants import ML_REPORT_DIR, ML_RESULTS_DIR  # noqa: E402
from src.ml import report  # noqa: E402
from src.ml.features import load_or_build_features  # noqa: E402
from src.ml.training import (  # noqa: E402
    configure_spark_for_training,
    release,
    run_classification,
    run_regression,
)
from src.spark.session import create_spark_session  # noqa: E402


def describe_stages(result) -> str:
    return "\n".join(
        f"{i + 1}. {stage}"
        for i, stage in enumerate(result.data.preprocessing.stages)
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="tiny grids")
    parser.add_argument(
        "--rebuild-features",
        action="store_true",
        help="rebuild the feature parquet from raw JSON",
    )
    parser.add_argument(
        "--report-dir",
        default=str(ML_REPORT_DIR),
        help="where figures, CSV tables and the digest go",
    )
    args = parser.parse_args(argv)

    start = time.perf_counter()
    spark = create_spark_session("yelp-ml")
    spark.sparkContext.setLogLevel("ERROR")
    configure_spark_for_training(spark)

    features = load_or_build_features(
        spark, rebuild=args.rebuild_features
    ).cache()
    print(
        f"Feature table: {features.count()} businesses,"
        f" {len(features.columns)} columns"
    )

    regression = run_regression(features, quick=args.quick)
    release(regression.data)
    classification = run_classification(
        features, include_recency=False, quick=args.quick
    )
    release(classification.data)
    classification_recency = run_classification(
        features,
        include_recency=True,
        quick=args.quick,
        reuse_params_from=classification,
    )
    release(classification_recency.data)

    results = [regression, classification, classification_recency]
    report_dir = Path(args.report_dir)
    tables = {
        r.task.name: report.write_task(r, report_dir / "tables")
        for r in results
    }
    sample = features.select("stars", "is_closed").toPandas()
    report.write_figures(
        regression,
        classification,
        classification_recency,
        sample,
        report_dir / "figures",
    )
    report.write_digest(
        results, tables, report_dir / "results.md", describe_stages(regression)
    )

    ML_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary = report.summary(results)
    summary["runtime_minutes"] = (time.perf_counter() - start) / 60
    (ML_RESULTS_DIR / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    print(f"Done in {summary['runtime_minutes']:.1f} min -> {report_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
