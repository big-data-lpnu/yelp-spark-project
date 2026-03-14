from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),

    StructField("stars", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("text", StringType(), True),

    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),
])