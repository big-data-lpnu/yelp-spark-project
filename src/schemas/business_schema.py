from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    MapType,
)


business_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),

    StructField("stars", DoubleType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("is_open", IntegerType(), True),

    StructField("attributes", MapType(StringType(), StringType()), True),

    # Actual data stores categories as a comma-separated string,
    # e.g. "Restaurants, Pizza, Italian". Filter with .contains("X").
    StructField("categories", StringType(), True),

    StructField("hours", MapType(StringType(), StringType()), True)
])