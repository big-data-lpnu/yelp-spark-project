import requests
from threading import Lock
from tqdm import tqdm


def stream_download(
    url: str,
    output_path: str,
    chunk_size_bytes: int,
    tqdm_position: int = 0,
) -> None:
    """
    Download a file from the specified URL in streaming mode, writing it to the given output path.
    This method is memory-efficient and suitable for large files.

    :param url: The URL to download the file from.
    :param output_path: The local file path to save the downloaded file.
    :param chunk_size_bytes: The size of each chunk to download in bytes.
    :param tqdm_position: The position of the tqdm progress bar (useful for multiple downloads).
    """
    response = requests.get(url, stream=True, headers={
        "User-Agent": "Mozilla/5.0 (compatible; YelpSparkProject/1.0)"
    })
    response.raise_for_status()
    total_size = int(response.headers.get("content-length", 0))

    with tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        desc=output_path.split("/")[-1],
        position=tqdm_position,
        leave=True,
        miniters=1,
    ) as pbar:
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size_bytes):
                f.write(chunk)
                pbar.update(len(chunk))
