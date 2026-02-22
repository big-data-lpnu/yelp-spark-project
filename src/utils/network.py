import requests
from tqdm import tqdm


def stream_download(url: str, output_path: str, chunk_size_bytes: int) -> None:
    """
    Download a file from the specified URL in streaming mode, writing it to the given output path.
    This method is memory-efficient and suitable for large files.
    
    :param url: The URL to download the file from.
    :param output_path: The local file path to save the downloaded file.
    :param chunk_size_bytes: The size of each chunk to download in bytes.
    """
    with requests.get(
        url,
        stream=True,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; YelpSparkProject/1.0)"
        },
    ) as response:
        response.raise_for_status()  # Check if the request was successful
        total_size = int(response.headers.get("content-length", 0))
        with open(output_path, "wb") as file:
            for chunk in tqdm(
                response.iter_content(chunk_size=chunk_size_bytes),
                total=total_size // chunk_size_bytes + 1,
                unit="chunk",
            ):
                if chunk:  # Filter out keep-alive chunks
                    file.write(chunk)
