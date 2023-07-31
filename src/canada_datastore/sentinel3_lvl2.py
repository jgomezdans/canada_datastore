import datetime as dt
import ftplib
import json
import logging
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import xarray as xr
from retrying import retry

from .utils import dataframe_to_geojson

logger = logging.getLogger("canada_datastore")

URL = (
    "ftp://ftp_ompc_esl:phae2eTo@ftp.adwaiseo-services.com/"
    + "sentinel-3/To_KLC/Canadian_Campaign/"
)


def nc_to_dataframe(fname, to_folder):
    to_folder = Path(to_folder)
    ds = xr.open_dataset(fname)
    if len(ds.fires) > 0:
        logger.info(f"{len(ds.fires)} fires in {fname}")

        tag = fname.stem.split("_")[1]
        columns = [
            k for k, v in ds.variables.items() if ds[k].values.ndim == 1
        ]
        values = (ds[k].values for k in columns)
        df = pd.DataFrame(dict(zip(columns, values)))
        geojson = dataframe_to_geojson(df)
        df.to_csv(to_folder / (fname.parent.stem + f"_{tag}.csv"))
        with open(
            to_folder / (fname.parent.stem + f"_{tag}.geojson"), mode="w"
        ) as fp:
            json.dump(geojson, fp, indent=True)
    else:
        logger.info(f"No fires in {fname}")


def download_files_from_ftps(
    remote_url: str,
    local_directory: str,
    start_date: None | dt.datetime = None,
) -> None:
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
    print(remote_file_path)
    ftps.cwd(remote_file_path)

    # Create the local directory if it does not exist
    local_directory_path = Path(local_directory)
    local_directory_path.mkdir(parents=True, exist_ok=True)
    dloaded_files = []

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
                try:
                    date = dt.datetime.strptime(file_name, "%Y%m%d")
                    if date < start_date:
                        continue
                except ValueError:
                    pass
                if file_name.find("SL_1_RBT") >= 0:
                    continue
                local_subdirectory.mkdir(parents=True, exist_ok=True)
                logger.info(f"Going into {file_name}")
                download_directory(file_name, local_subdirectory)
            else:
                # Download the file
                local_file_path = local_directory / file_name
                if not local_file_path.exists():
                    with local_file_path.open("wb") as local_file:
                        ftps.retrbinary(f"RETR {file_name}", local_file.write)
                    logger.debug(f"Downloaded {local_file_path}")
                    dloaded_files.append(local_file)
                else:
                    logger.debug(f"{file_name} already exists. Skipping...")

        # Return to the parent directory after downloading
        # all files/subdirectories
        ftps.cwd("..")

    # Start the recursive download process from the root directory
    download_directory(remote_file_path, local_directory)

    # Close the FTPS connection
    ftps.quit()
    return dloaded_files


def get_s3_lvl2_products(
    local_dir: str | Path, start_date: dt.datetime | None = None
):
    logger.info("Starting SEN3 Level 2 mirroring")
    local_dir = Path(local_dir)
    if not local_dir.exists():
        local_dir.mkdir(parents=True, exist_ok=True)
    folder = "NRT_like_data"
    remote_url = f"{URL}/{folder}"
    dloaded_files = download_files_from_ftps(
        remote_url, local_dir.as_posix(), start_date=start_date
    )
    print(dloaded_files)
    logger.info("Done with SEN3 Level 2 mirroring")
    return dloaded_files


def process_lv2_products(local_dir: str | Path, output_dir: str):
    folder = Path(local_dir)
    outfolder = folder / output_dir
    if not outfolder.exists():
        outfolder.mkdir(parents=True, exist_ok=True)
    fnames = [f for f in folder.rglob("**/FRP*nc")]
    for fname in fnames:
        nc_to_dataframe(fname, outfolder)
