from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    ArrayType
)


user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),

    StructField("review_count", IntegerType(), True),
    StructField("yelping_since", StringType(), True),

    StructField("friends", ArrayType(StringType()), True),

    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),

    StructField("fans", IntegerType(), True),

    StructField("elite", ArrayType(IntegerType()), True),

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