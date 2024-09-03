import datetime as dt
import logging
import os
from pathlib import Path

import click

from canada_datastore import (
    get_firms_date,
    get_s3_lvl2_products,
    process_lv2_products,
    select_product_filter,
)

from .utils import create_all_project_files, get_folder
from .utils import rsync_files

logger = logging.getLogger("canada_datastore")


# PICKLE_LAKE = [
#     [-102, 56],
#     [-102, 46],
#     [-82, 46],
#     [-82, 56],
#     [-102, 56],
# ]

# Fort McMurray
PICKLE_LAKE = [
    [-115.2178, 59.4289],  # Northwest corner
    [-107.5422, 59.4289],  # Northeast corner
    [-107.5422, 54.0235],  # Southeast corner
    [-115.2178, 54.0235],  # Southwest corner
    [-115.2178, 59.4289],  # Back to the Northwest corner to close
    # the polygon
]


@click.command()
@click.option(
    "-f1", "--lvl1folder", default=None, help="Level1 product folder"
)
@click.option(
    "-f2", "--lvl2folder", default=None, help="Level2 product folder"
)
@click.option(
    "-ff", "--firmsfolder", default=None, help="FIRMS product folder"
)
@click.option("-d", "--date", default=None, help="Level 1 start date")
def main(lvl1folder, lvl2folder, firmsfolder, date):
    lvl1folder = get_folder(lvl1folder)
    lvl2folder = get_folder(lvl2folder)
    firmsfolder = get_folder(firmsfolder)

    if date is not None:
        try:
            start_day = dt.datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            logger.error("Dates should be given as YYYY-MM-DD format")
    else:
        start_day = dt.datetime.today() - dt.timedelta(days=1)
    day1 = dt.timedelta(days=1)
    today = start_day
    if lvl2folder is not None:
        logger.info("Downloading Level2 data...")
        get_s3_lvl2_products(lvl2folder.as_posix(), start_date=start_day)

    while today <= dt.datetime.now():
        if lvl2folder is not None:
            process_lv2_products(
                lvl2folder, "processed_output", start_date=today
            )
        if firmsfolder is not None:
            logger.info(f"Downloading FIRMS data {today}...")
            retval = get_firms_date(today, output_folder=firmsfolder, range=1)
            logger.info(f"FIRMS: got {retval.keys()}")
        if lvl1folder is not None:
            logger.info(
                f"Downloading Level1 data for {today.strftime('%Y-%m-%d')}"
            )
            select_product_filter(
                "EO:EUM:DAT:0411",
                PICKLE_LAKE,
                today,
                today + day1,
                output_folder=lvl1folder.as_posix(),
            )
        today = today + day1
    logger.info("Downloaded and converted files")
    logger.info("Creating QGIS project files")
    logger.info("Syncing to JASMIN...")
    jasmin_rsync = (
        "jlgomezdans@xfer1.jasmin.ac.uk:/home/users/"
        + "jlgomezdans/global_fire_models/public/"
    )
    data_folders = [
        folder.as_posix()
        for folder in [lvl1folder, lvl2folder, firmsfolder]
        if folder is not None
    ]

    common_parent = Path(os.path.commonpath(data_folders))
    rsync_files(common_parent, jasmin_rsync)

    # Create output folder if it doesn't exist
    (common_parent / "qgis_projects").mkdir(exist_ok=True)
    create_all_project_files(
        lvl1folder,
        lvl2folder,
        firmsfolder,
        common_parent / "qgis_projects",
    )
    # Now sync the QGIS projects
    rsync_files(common_parent, jasmin_rsync)


main()
