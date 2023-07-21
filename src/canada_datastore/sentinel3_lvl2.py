import ftplib
import logging
from pathlib import Path
from urllib.parse import urlparse

from retrying import retry

logger = logging.getLogger("canada_datastore")

URL = (
    "sftp://ftp_ompc_esl:phae2eTo@ftp.adwaiseo-services.com/"
    + "sentinel-3/To_KLC/Canadian_Campaign/"
)


def download_files_from_ftps(remote_url: str, local_directory: str) -> None:
    # Parse the remote URL to get the FTPS server and file path
    parsed_url = urlparse(remote_url)
    server_address = parsed_url.hostname
    username = parsed_url.username
    password = parsed_url.password
    remote_file_path = parsed_url.path

    # Create an FTP_TLS object and set the required protocol to FTPS
    ftps = ftplib.FTP_TLS()
    ftps.connect(server_address)
    ftps.login(username, password)
    ftps.prot_p()

    # Change to the remote directory
    ftps.cwd(remote_file_path)

    # Create the local directory if it does not exist
    local_directory_path = Path(local_directory)
    local_directory_path.mkdir(parents=True, exist_ok=True)

    @retry(wait_fixed=2000, stop_max_attempt_number=3)
    def download_directory(
        remote_directory: str, local_directory: str
    ) -> None:
        # Change to the remote directory
        ftps.cwd(remote_directory)
        # Get the list of files and subdirectories in the remote directory
        remote_files = []
        ftps.retrlines("LIST", remote_files.append)
        for line in remote_files:
            # Parse the line to extract the file/directory name
            tokens = line.split(None, 8)
            if len(tokens) < 9:
                continue

            file_name = tokens[-1].lstrip()
            if tokens[0].startswith("d"):  # Directory
                # Create the corresponding local subdirectory
                # and continue recursion
                local_subdirectory = local_directory_path / file_name
                local_subdirectory.mkdir(parents=True, exist_ok=True)
                download_directory(file_name, local_subdirectory)
            else:
                # Download the file
                local_file_path = local_directory / file_name
                if not local_file_path.exists():
                    with local_file_path.open("wb") as local_file:
                        ftps.retrbinary(f"RETR {file_name}", local_file.write)
                    logger.info(f"Downloaded {file_name}")
                else:
                    logger.info(f"{file_name} already exists. Skipping...")

        # Return to the parent directory after downloading
        # all files/subdirectories
        ftps.cwd("..")

    # Start the recursive download process from the root directory
    download_directory(remote_file_path, local_directory)

    # Close the FTPS connection
    ftps.quit()


def get_s3_lvl2_products(local_dir: str | Path):
    logger.info("Starting SEN3 Level 2 mirroring")
    local_dir = Path(local_dir)
    if not local_dir.exists():
        local_dir.mkdir(parents=True, exist_ok=True)
    for folder in ["NRT-like_products", "operational_products"]:
        remote_url = f"{URL}/{folder}"
        download_files_from_ftps(remote_url, local_dir.as_posix())
    logger.info("Done with SEN3 Level 2 mirroring")
