import click
from .canada_datastore import select_product_filter
from .canada_datastore import get_s3_lvl2_products
import datetime as dt
import logging
from pathlib import Path

logger = logging.getLogger("canada_datastore")


PICKLE_LAKE = [
    [-98, 56],
    [-98, 46],
    [-82, 46],
    [-82, 56],
    [-98, 56],
]


@click.command()
@click.option(
    "-f1", "--lvl1folder", default="./", help="Level1 product folder"
)
@click.option(
    "-f2", "--lvl2folder", default="./", help="Level2 product folder"
)
@click.option("-d", "--date", default=None, help="Date to gather data")
def main(lvl1folder, lvl2folder, date):
    lvl1folder = Path(lvl1folder)
    lvl2folder = Path(lvl2folder)
    if not lvl1folder.exists():
        raise IOError("Can't find lvl1 product folder")
    if not lvl2folder.exists():
        raise IOError("Can't find lvl2 product folder")

    get_s3_lvl2_products(lvl2folder.as_posix_path())
    if date is not None:
        try:
            date = dt.datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            logger.error("Dates should be given as YYYY-MM-DD format")
    else:
        start_day = dt.datetime(2023, 7, 1)
    day1 = dt.timedelta(days=1)
    today = start_day
    while today <= dt.datetime.now():
        select_product_filter(
            "EO:EUM:DAT:0411",
            PICKLE_LAKE,
            today,
            today + day1,
            output_folder=lvl1folder.as_posix(),
        )
        today = today + day1


main()
