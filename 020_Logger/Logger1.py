import os
import logging
from datetime import datetime
from pytz import timezone

logger = logging.getLogger(__name__)

def main():
    logger.debug("this is debug ")
    logger.info("this is info")
    logger.warning("this is warning")
    logger.error("this is error")
    logger.critical("this is critical")

if __name__ == "__main__":
    main()