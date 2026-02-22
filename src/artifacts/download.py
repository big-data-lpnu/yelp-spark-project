from src.constants import (
    YELP_DATASET_JSON_URL,
    YELP_DATASET_PHOTOS_URL,
    DATASETS_DIR,
    LOGGING_LEVEL,
)
from src.utils.network import stream_download
from src.utils.archive_files import (
    is_supported_archive,
    unpack_file,
    recursive_unpack,
)

import os
import shutil
import logging
from datetime import datetime


def download_dataset(
    url: str,
    output_path: str,
    truncate_existing: bool = False,
    cleanup_archive: bool = True,
    chunk_size_bytes: int = 10 * 1024 * 1024,
) -> None:
    """
    Download a dataset from the specified URL and save it to the given output path.

    :param url: The URL of the dataset to download.

    :param output_path: The local file path where
    the downloaded dataset will be saved.

    :param truncate_existing: Whether to delete existing files
    at the output path before downloading. Defaults to False.

    :param cleanup_archive: Whether to delete
    the downloaded archive file after extraction. Defaults to True.

    :param chunk_size_bytes: The size of each chunk
    to download in bytes. Defaults to 10 MB.
    """
    logger = logging.getLogger("ArtifactsDownloader")
    logger.debug(f"Downloading dataset from {url} to {output_path}...")

    target_dir = os.path.dirname(output_path)
    if truncate_existing:
        shutil.rmtree(target_dir, ignore_errors=True)
    os.makedirs(target_dir, exist_ok=True)

    if os.path.exists(output_path) or os.path.exists(
        output_path.replace(".zip", "")
    ):
        logger.debug(
            f"Output path {output_path} already exists. Skipping download."
        )
        return

    stream_download(url, output_path, chunk_size_bytes=chunk_size_bytes)
    if is_supported_archive(output_path):
        unpack_file(output_path, cleanup_archive=cleanup_archive)

        search_path = output_path.rsplit(".", 1)[0]
        unpacked_paths = recursive_unpack(
            search_path, cleanup_archive=cleanup_archive
        )

        logger.info(
            f"Dataset downloaded and unpacked successfully.\n"
            f"Unpacked paths:\n{output_path}\n"
            f"{'\n'.join(unpacked_paths)}"
        )

    logger.debug(f"Dataset downloaded successfully and saved to {output_path}")


if __name__ == "__main__":
    logging.basicConfig(
        level=LOGGING_LEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("ArtifactsDownloader")

    start_time = datetime.now()
    logger.info(f"Starting dataset download at {start_time}...")

    try:
        download_dataset(YELP_DATASET_JSON_URL, f"{DATASETS_DIR}/yelp_json.zip")
        download_dataset(
            YELP_DATASET_PHOTOS_URL, f"{DATASETS_DIR}/yelp_photos.zip"
        )
        logger.info(
            f"All datasets downloaded successfully at {datetime.now()}.\n"
            f"Time elapsed: {datetime.now() - start_time}"
        )
    except Exception as e:
        logger.error(f"An error occurred while downloading datasets: {e}")
