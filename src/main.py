from pyspark import __version__ as pyspark_version


def main():
    print(
        f"Hello from yelp-spark-project!\n"
        f"Using PySpark version {pyspark_version}"
    )


if __name__ == "__main__":
    main()
