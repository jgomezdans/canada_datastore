"""A Sentinel 3 download object for NRT Level 1b products"""

import eumdac
import datetime as dt
import os
from pathlib import Path
import requests
import shutil
import logging
import zipfile
from tqdm.contrib.concurrent import thread_map

from .sentinel3_gridder import find_files

logger = logging.getLogger("canada_datastore")


def access_token(
    key: str | None = None, secret: str | None = None
) -> eumdac.AccessToken:
    if key is None:
        try:
            consumer_key = os.environ["EUMETSAT_KEY"]
        except KeyError:
            raise KeyError(
                "You did not provide an EUMETSAT API key, "
                "and one isn't defined in the environment"
            )
    if secret is None:
        try:
            consumer_secret = os.environ["EUMETSAT_SECRET"]
        except KeyError:
            raise KeyError(
                "You did not provide an EUMETSAT SECRET key, "
                "and one isn't defined in the environment"
            )
    logger.info("Getting access token")

    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)

    try:
        logger.info(f"This token '{token}' expires {token.expiration}")
    except requests.exceptions.HTTPError as exc:
        logger.info(f"Error when trying the request to the server: '{exc}'")
    return token


def set_up_collection(collection_name: str) -> eumdac.DataStore.collections:
    token = access_token()
    datastore = eumdac.DataStore(token)
    return datastore.get_collection(collection_name)


def download_product(product, output_folder: str | Path):
    output_folder = Path(output_folder)
    output_file = output_folder / (f"{product._id}.zip")
    if not output_file.exists():
        with product.open() as fsrc, open(output_file, mode="wb") as fdst:
            shutil.copyfileobj(fsrc, fdst)

        logger.info(f"Download of product {product} finished.")
    return output_file


def process_granule(product, output_folder: str | Path):
    output_folder = Path(output_folder)
    loc = output_folder / (product.name.replace(".zip", ""))
    with zipfile.ZipFile(product, "r") as zip_ref:
        logger.info(f"Uncompressing {product} to {loc.parent}")
        zip_ref.extractall(loc.parent)

    find_files(loc)


def select_product_filter(
    collection_name: str,
    geometry: list,
    start_time: dt.datetime,
    end_time: dt.datetime,
    output_folder: str | Path = None,
):
    if output_folder is None:
        output_folder = Path.cwd()
    elif isinstance(output_folder, str):
        output_folder = Path(output_folder)

    if not output_folder.exists():
        output_folder.mkdir(parents=True, exist_ok=True)

    collection = set_up_collection(collection_name)
    poly = "POLYGON(({}))".format(
        ",".join(["{} {}".format(*coord) for coord in geometry])
    )
    products = collection.search(geo=poly, dtstart=start_time, dtend=end_time)
    logger.info(f"Will download {len(products)} products")

    def downloader(product):
        return download_product(product, output_folder)

    def process(product):
        return process_granule(product, output_folder)

    fnames = thread_map(downloader, products, max_workers=4)
    thread_map(process, fnames)


if __name__ == "__main__":
    PICKLE_LAKE = [
        [-97.65797267768923, 55.86011801824668],
        [-97.65797267768923, 48.68922685204274],
        [-82.99305913386435, 48.68922685204274],
        [-82.99305913386435, 55.86011801824668],
        [-97.65797267768923, 55.86011801824668],
    ]

    SLSTR_NRT = "EO:EUM:DAT:0411"

    select_product_filter(
        SLSTR_NRT,
        PICKLE_LAKE,
        dt.datetime(2023, 7, 1),
        dt.datetime(2023, 7, 2),
        output_folder="/home/jose/data/Canada_datastore/Sentinel3",
    )
