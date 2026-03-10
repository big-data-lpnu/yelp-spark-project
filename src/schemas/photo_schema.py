from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


photo_schema = StructType([
    StructField("photo_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("caption", StringType(), True),
    StructField("label", StringType(), True)
])