import subprocess

from pyspark import __version__ as pyspark_version

from src.spark.session import create_spark_session
from src.spark.load_data import load_dataset
from src.analysis.data_validation import validate_dataframe
from src.transform import run_transformations


def main():
    print(
        f"Hello from yelp-spark-project!\n"
        f"Using PySpark version {pyspark_version}\n"
    )

    print("Java version:")
    print(
        subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT
        ).decode()
    )

    spark = create_spark_session()

    # ------------------------------------------------------------------
    # Extraction phase - load and validate every dataset
    # ------------------------------------------------------------------
    datasets = ["business", "review", "user", "checkin", "tip"]

    validation_reports = {}
    for dataset_name in datasets:
        try:
            print(f"Loading dataset: {dataset_name}...")
            df = load_dataset(spark, dataset_name)

            print(f"Validating dataset: {dataset_name}...")
            report = validate_dataframe(df, dataset_name)
            validation_reports[dataset_name] = report

        except Exception as e:
            print(f"Error processing '{dataset_name}': {str(e)}")
            validation_reports[dataset_name] = {"error": str(e)}

    print(f"\n{'=' * 80}")
    print("EXTRACTION PHASE SUMMARY")
    print(f"{'=' * 80}")

    successful = sum(1 for r in validation_reports.values() if "error" not in r)
    failed = len(validation_reports) - successful
    print(
        f"Successfully loaded and validated: {successful}/{len(datasets)} datasets"
    )

    if failed > 0:
        print(f"Failed datasets: {failed}")
        for name, report in validation_reports.items():
            if "error" in report:
                print(f"   • {name}: {report['error']}")

    # ------------------------------------------------------------------
    # Transformation phase - run all 24 business questions
    # ------------------------------------------------------------------
    print(f"\n{'=' * 80}")
    print("TRANSFORMATION PHASE")
    print(f"{'=' * 80}")
    run_transformations(spark)

    spark.stop()


if __name__ == "__main__":
    main()
