import os
import logging
from logging import StreamHandler, FileHandler, Formatter
from datetime import datetime
from pytz import timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(ROOT,datetime.now(timezone('Asia/Tokyo')).strftime('%Y%m%d%H%M')+"_log.txt")

# ログ出力設定
logger = logging.getLogger(__name__)
handler_stream = StreamHandler()
handler_stream.setFormatter(Formatter("%(asctime)s@ %(name)s [%(levelname)s] %(funcName)s: %(message)s"))
handler_file = FileHandler(filename=LOGS, encoding='utf-8')
handler_file.setFormatter(Formatter("%(asctime)s@ %(name)s [%(levelname)s] %(funcName)s: %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler_stream, handler_file])

def main():
    logger.debug("this is debug ")
    logger.info("this is info")
    logger.warning("this is warning")
    logger.error("this is error")
    logger.critical("this is critical")

if __name__ == "__main__":
    main()