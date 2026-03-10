from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)


tip_schema = StructType([
    StructField("text", StringType(), True),
    StructField("date", StringType(), True),
    StructField("compliment_count", IntegerType(), True),
    StructField("business_id", StringType(), True),
    StructField("user_id", StringType(), True)
])