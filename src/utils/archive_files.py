import os
import shutil
import logging


SUPPORTED_FORMATS = [
    fmt
    for formats in shutil.get_unpack_formats()
    for fmt in formats[1]  # ('name', ['extensions'], 'description')
]


def is_supported_archive(file_path: str) -> bool:
    """
    Check if the given file path points to a supported archive format.

    :param file_path: The path to the file to check.
    :return: True if the file is a supported archive, False otherwise.
    """
    if not os.path.isfile(file_path):
        return False

    # Skip metadata files that may be present in the directory
    file_name = os.path.basename(file_path)
    if file_name.startswith(".") or file_name.startswith("_"):
        return False

    ext = f".{file_path.rsplit(".", 1)[-1].lower()}"
    return ext in SUPPORTED_FORMATS


def unpack_file(
    zip_path: str, extract_to: str | None = None, cleanup_archive: bool = True
) -> None:
    """
    Unpack a supported archive file (e.g., zip, tar.gz) to the specified directory.

    :param zip_path: The path to the archive file to unpack.

    :param extract_to: The directory to extract the contents to.
    If None, it will be extracted to a directory with
    the same name as the archive (without extension).

    :param cleanup_archive: Whether to delete the archive file after extraction.
    Defaults to True.
    """

    logger = logging.getLogger("ArtifactsDownloader")

    if extract_to is None:
        extract_to = zip_path.rsplit(".", 1)[0]

    logger.debug(f"Extracting dataset from {zip_path} to {extract_to}...")
    shutil.unpack_archive(zip_path, extract_to)

    # Remove the zip file after extraction to save space
    if cleanup_archive:
        logger.debug(f"Cleaning up zip file {zip_path}...")
        os.remove(zip_path)

    logger.debug(f"Extracted dataset from {zip_path} to {extract_to}")


def recursive_unpack(path: str, cleanup_archive: bool = True) -> list[str]:
    """
    Recursively unpack all supported archive files
    found in the given directory path.


    This function will traverse the directory structure
    starting from the specified path, identify any supported
    archive files, and unpack them in place.
    If an archive is found within a subdirectory, it will also be unpacked,
    allowing for nested archives to be handled.

    :param path: The directory path to start searching for archive files.

    :param cleanup_archive: Whether to delete the archive file after unpacking.
    Defaults to True.

    :return: A list of paths to the directories where the archives were unpacked.
    """
    if not os.path.isdir(path):
        return []

    unpacked_paths = []
    for root, _, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            if is_supported_archive(file_path):
                unpack_file(file_path, cleanup_archive=cleanup_archive)
                unpacked_paths.append(root)

    return unpacked_paths
