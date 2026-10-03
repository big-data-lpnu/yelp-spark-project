"""
Machine learning stage entry point.

    uv run -m src.ml.run            # full run, all cores
    uv run -m src.ml.run --quick    # one short setting per model, no extras

Builds (or reuses) the user feature table, trains 3 regression models
(log fans) and 3 classification models (elite users), and writes the report
artefacts (figures, CSV tables, results.md) to src/reports/ml/ and a JSON
digest to results/ml/summary.json. A --quick run writes everything to
results/ml/quick/ instead, so it never overwrites the published report.
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
        default=None,
        help="where figures, CSV tables and the digest go "
        "(default: src/reports/ml, or results/ml/quick with --quick)",
    )
    args = parser.parse_args(argv)
    if args.report_dir:
        report_dir = Path(args.report_dir)
    else:
        report_dir = ML_RESULTS_DIR / "quick" if args.quick else ML_REPORT_DIR
    summary_dir = report_dir if args.quick else ML_RESULTS_DIR

    # Same core count as the notebook, so fit times are comparable.
    os.environ.setdefault("SPARK_MAX_CORES", str(os.cpu_count()))
    start = time.perf_counter()
    spark = create_spark_session("yelp-ml")
    spark.sparkContext.setLogLevel("ERROR")
    configure_spark_for_training(spark)

    features = load_or_build_features(
        spark, rebuild=args.rebuild_features
    ).cache()
    print(
        f"Feature table: {features.count()} users,"
        f" {len(features.columns)} columns"
    )

    regression = run_regression(features, quick=args.quick)
    release(regression.data)
    classification = run_classification(features, quick=args.quick)
    release(classification.data)

    results = [regression, classification]
    tables = {
        r.task.name: report.write_task(r, report_dir / "tables")
        for r in results
    }
    fan_counts = features.groupBy("fans", "is_elite").count().toPandas()
    report.write_figures(
        regression, classification, fan_counts, report_dir / "figures"
    )
    report.write_digest(
        results, tables, report_dir / "results.md", describe_stages(regression)
    )

    summary_dir.mkdir(parents=True, exist_ok=True)
    summary = report.summary(results)
    summary["runtime_minutes"] = (time.perf_counter() - start) / 60
    (summary_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    print(f"Done in {summary['runtime_minutes']:.1f} min -> {report_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
