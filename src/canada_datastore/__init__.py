import logging
import sys

from .sentinel3_dloader import select_product_filter  # noqa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
# Create a logger for the package
logger = logging.getLogger("canada_datastore")
