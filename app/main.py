import asyncio
import logging.config

from libs.logger_config import LOGGING_CONFIG
from libs.config_loader import Config
from sources.factories import get_data_source
from repos.factories import get_db_store
from downloader import Application
from processor import DataProcessor


config = Config()
logging.config.dictConfig(LOGGING_CONFIG)

app = Application(
    config=config,
    data_source=get_data_source(config=config),
    db_store=get_db_store(config=config),
    processor=DataProcessor(config=config),
)

asyncio.run(app.run())
