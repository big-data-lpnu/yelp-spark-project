from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),

    StructField("review_count", IntegerType(), True),
    StructField("yelping_since", StringType(), True),

    # Actual data: comma-separated user_id string, e.g. "id1,id2,id3"
    StructField("friends", StringType(), True),

    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),

    StructField("fans", IntegerType(), True),

    # Actual data: comma-separated years string, e.g. "2012,2013,2014"
    StructField("elite", StringType(), True),

    StructField("average_stars", DoubleType(), True),

    StructField("compliment_hot", IntegerType(), True),
    StructField("compliment_more", IntegerType(), True),
    StructField("compliment_profile", IntegerType(), True),
    StructField("compliment_cute", IntegerType(), True),
    StructField("compliment_list", IntegerType(), True),
    StructField("compliment_note", IntegerType(), True),
    StructField("compliment_plain", IntegerType(), True),
    StructField("compliment_cool", IntegerType(), True),
    StructField("compliment_funny", IntegerType(), True),
    StructField("compliment_writer", IntegerType(), True),
    StructField("compliment_photos", IntegerType(), True),
])