from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


checkin_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("date", StringType(), True)
])