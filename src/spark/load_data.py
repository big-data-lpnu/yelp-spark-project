from src.schemas.business_schema import business_schema
from src.schemas.review_schema import review_schema
from src.schemas.user_schema import user_schema
from src.schemas.checkin_schema import checkin_schema
from src.schemas.tip_schema import tip_schema
from src.schemas.photo_schema import photo_schema

from src.constants import (
    BUSINESS_PATH,
    REVIEW_PATH,
    USER_PATH,
    CHECKIN_PATH,
    TIP_PATH,
    PHOTO_PATH,
)


DATASETS = {
    "business": (business_schema, BUSINESS_PATH),
    "review": (review_schema, REVIEW_PATH),
    "user": (user_schema, USER_PATH),
    "checkin": (checkin_schema, CHECKIN_PATH),
    "tip": (tip_schema, TIP_PATH),
    "photo": (photo_schema, PHOTO_PATH),
}


def load_dataset(spark, name):
    """
    Loads a dataset using Spark based on the provided name.

    Parameters:
    name : str
        The name of the dataset to be loaded, which corresponds to a key in the
        `DATASETS` dictionary.
    spark
        The SparkSession object used to read the dataset.

    Returns:
    DataFrame
        A Spark DataFrame containing the loaded dataset.
    """
    schema, path = DATASETS[name]
    df = spark.read.schema(schema).json(str(path))

    return df