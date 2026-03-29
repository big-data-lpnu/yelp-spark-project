# Created at Sunday, 2026-02-22 18:11
# Please maintain this file as the source of truth for all constants used in the project.

from logging import DEBUG as DEBUG_LOGGING_LEVEL
from pathlib import Path

# Artifacts
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
PROCESSED_DIR = ARTIFACTS_DIR / "processed"
DATASETS_DIR = ARTIFACTS_DIR / "datasets" / "yelp_json" / "Yelp JSON" / "yelp_dataset"

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

# Dataset paths
BUSINESS_PATH = DATASETS_DIR / "yelp_academic_dataset_business.json"
REVIEW_PATH = DATASETS_DIR / "yelp_academic_dataset_review.json"
USER_PATH = DATASETS_DIR / "yelp_academic_dataset_user.json"
CHECKIN_PATH = DATASETS_DIR / "yelp_academic_dataset_checkin.json"
TIP_PATH = DATASETS_DIR / "yelp_academic_dataset_tip.json"
# Photo metadata is not in Yelp-JSON.zip; it lives in Yelp-Photos.zip as photos.json.
PHOTO_PATH = (
    ARTIFACTS_DIR
    / "datasets"
    / "yelp_photos"
    / "Yelp Photos"
    / "yelp_photos"
    / "photos.json"
)