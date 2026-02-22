# Created at Sunday, 2026-02-22 18:11
# Please maintain this file as the source of truth for all constants used in the project.

from logging import DEBUG as DEBUG_LOGGING_LEVEL

# Artifacts
ARTIFACTS_DIR = "artifacts"
DATASETS_DIR = f"{ARTIFACTS_DIR}/datasets"

# Dataset URLs (at the time of writing, these were the URLs for the Yelp dataset page)
# If these URLs change, please update them here.
# https://www.yelp.com/dataset
YELP_DATASET_JSON_URL = (
    "https://business.yelp.com/external-assets/files/Yelp-JSON.zip"
)
YELP_DATASET_PHOTOS_URL = (
    "https://business.yelp.com/external-assets/files/Yelp-Photos.zip"
)

# Logging configuration
LOGGING_LEVEL = DEBUG_LOGGING_LEVEL
