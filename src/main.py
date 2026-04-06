from pyspark import __version__ as pyspark_version

from src.spark.session import create_spark_session
from src.spark.load_data import load_dataset
from src.analysis.data_validation import validate_dataframe

import subprocess


def main():
    print(
        f"Hello from yelp-spark-project!\n"
        f"Using PySpark version {pyspark_version}\n"
    )

    print("Java version:")
    print(subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode())

    # Create Spark session
    spark = create_spark_session()

    # List of datasets to load and validate
    datasets = ["business", "review", "user", "checkin", "tip"]

    # Load and validate each dataset
    validation_reports = {}
    for dataset_name in datasets:
        try:
            print(f"Loading dataset: {dataset_name}...")
            df = load_dataset(spark, dataset_name)

            print(f"Validating dataset: {dataset_name}...")
            report = validate_dataframe(df, dataset_name)
            validation_reports[dataset_name] = report

        except Exception as e:
            print(f"❌ Error processing '{dataset_name}': {str(e)}")
            validation_reports[dataset_name] = {"error": str(e)}

    # Summary
    print(f"\n{'=' * 80}")
    print("📊 EXTRACTION PHASE SUMMARY")
    print(f"{'=' * 80}")

    successful = sum(
        1 for report in validation_reports.values() if "error" not in report
    )
    failed = len(validation_reports) - successful

    print(
        f"\n✅ Successfully loaded and validated: {successful}/{len(datasets)} datasets"
    )

    if failed > 0:
        print(f"❌ Failed datasets: {failed}")
        for dataset_name, report in validation_reports.items():
            if "error" in report:
                print(f"   • {dataset_name}: {report['error']}")

    else:
        print("🎉 All datasets loaded and validated successfully!")

    spark.stop()


if __name__ == "__main__":
    main()
