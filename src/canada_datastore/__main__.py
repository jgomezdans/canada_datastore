import datetime as dt
import logging

import click

from canada_datastore import (
    get_firms_date,
    get_s3_lvl2_products,
    process_lv2_products,
    select_product_filter,
)

from .utils import create_all_project_files, get_folder

logger = logging.getLogger("canada_datastore")


PICKLE_LAKE = [
    [-102, 56],
    [-102, 46],
    [-82, 46],
    [-82, 56],
    [-102, 56],
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
    if lvl2folder is not None:
        logger.info("Downloading Level2 data...")
        get_s3_lvl2_products(lvl2folder.as_posix(), start_date=start_day)
        process_lv2_products(lvl2folder, "processed_output")
    day1 = dt.timedelta(days=1)
    today = start_day
    while today <= dt.datetime.now():
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

    create_all_project_files(
        lvl1folder,
        lvl2folder,
        firmsfolder,
        lvl1folder.parent / "qgis_projects",
    )


main()
