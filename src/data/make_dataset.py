import logging
import os
import zipfile

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

URL = "https://download.inep.gov.br/microdados/microdados_enem_2017.zip"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def download() -> str:
    """
    Downloads the ENEM 2017 microdata zip file from a specified URL and saves it 
    to the RAW_DIR directory relative to the project root if it does not already exist.

    Process:
      - Constructs the full path to the destination file (microdados_enem_2017.zip)
        within RAW_DIR.
      - Checks if the file already exists:
          - If it exists, logs that the file is already present and returns its path.
          - If not, initiates a download using a requests session.
      - Configures the session to retry the request up to 5 times (with exponential
        backoff) for transient HTTP status codes (429, 500, 502, 503, 504).
      - Performs a streaming GET request with a timeout of 10 seconds.
      - Writes the response content in chunks (8192 bytes) to the destination file.
      - Logs completion or errors encountered during the download.

    Returns:
      str: The absolute path to the downloaded zip file.

    Raises:
      requests.RequestException: If an error occurs during the downloading process 
      (e.g., network issues or an invalid HTTP response).
    """
    zip_path = os.path.join(RAW_DIR, "microdados_enem_2017.zip")
    if not os.path.exists(zip_path):
        logging.info("Downloading microdata from %s...", URL)
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        try:
            r = session.get(URL, stream=True, timeout=10)
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info("Download completed: %s", zip_path)
        except requests.RequestException as e:
            logging.error("Download failed: %s", e)
            raise
    else:
        logging.info("File already exists: %s", zip_path)
    return zip_path


def extract(zip_path: str) -> None:
    """
    Extracts the contents of the provided zip file into the RAW_DIR directory.

    Process:
      - Opens the zip file using Python's zipfile module.
      - Extracts all files contained within the zip archive to RAW_DIR.
      - Logs both the beginning and completion of the extraction process.

    Args:
      zip_path (str): The path to the zip file that is to be extracted.

    Raises:
      zipfile.BadZipFile: If the provided zip file is corrupt or not a valid zip archive.
    """  # noqa: E501
    logging.info("Extracting zip file: %s", zip_path)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(RAW_DIR)
    logging.info("Extraction completed to directory: %s", RAW_DIR)