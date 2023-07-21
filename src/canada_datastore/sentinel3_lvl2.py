import logging
import os
from ftplib import FTP
from pathlib import Path
from urllib.parse import urlparse

from retrying import retry

logger = logging.getLogger("canada_datastore")

URL = (
    "ftp://ftp_ompc_esl@ftp.adwaiseo-services.com/"
    + "sentinel-3/To_KLC/Canadian_Campaign/"
)


def download_files_from_ftp(remote_url: str, local_directory: str) -> None:
    # Parse the remote URL to get the FTP server and file path
    parsed_url = urlparse(remote_url)
    server_address = parsed_url.hostname
    username = parsed_url.username
    password = parsed_url.password
    remote_file_path = parsed_url.path.lstrip("/")
    files = []
    # Connect to the FTP server
    ftp = FTP()
    ftp.connect(server_address)
    ftp.login(username, password)
    ftp.cwd(remote_file_path)

    # Get the list of files in the remote directory
    remote_files = ftp.nlst()

    # Create the local directory if it does not exist
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    @retry(wait_fixed=2000, stop_max_attempt_number=3)
    def download_file(remote_filename: str, local_filename: str) -> None:
        # Check if the file is already present locally
        local_file_path = os.path.join(local_directory, local_filename)
        if os.path.exists(local_file_path):
            logger.info(f"{local_filename} already exists. Skipping...")
            return

        # Download the file
        with open(local_file_path, "wb") as local_file:
            ftp.retrbinary(f"RETR {remote_filename}", local_file.write)
        logger.info(f"Downloaded {local_filename}")
        files.append(local_filename)

    # Download files one by one
    for remote_filename in remote_files:
        local_filename = os.path.basename(remote_filename)
        download_file(remote_filename, local_filename)

    # Close the FTP connection
    ftp.quit()


def get_s3_lvl2_products(local_dir: str | Path):
    logger.info("Starting SEN3 Level 2 mirroring")
    local_dir = Path(local_dir)
    if not local_dir.exists():
        local_dir.mkdir(parents=True, exist_ok=True)
    for folder in ["NRT-like_products", "operational_products"]:
        remote_url = f"{URL}/{folder}"
        download_files_from_ftp(remote_url, local_dir.as_posix())
    logger.info("Done with SEN3 Level 2 mirroring")
