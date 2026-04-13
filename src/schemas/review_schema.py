from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

review_schema = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        # Yelp JSON stores stars as a float (e.g. 4.0) - DoubleType parses cleanly.
        # IntegerType silently nullifies float-encoded integers during JSON read.
        StructField("stars", DoubleType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
        StructField("useful", IntegerType(), True),
        StructField("funny", IntegerType(), True),
        StructField("cool", IntegerType(), True),
    ]
)
