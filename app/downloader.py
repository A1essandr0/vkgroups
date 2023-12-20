import logging
import asyncio

from libs.config_loader import Config
from repos.interfaces import DbStoreRepo
from sources.interfaces import DataSource
from processor import DataProcessor


class Application:
    def __init__(
        self,
        config: Config,
        data_source: DataSource,
        processor: DataProcessor,
        db_store: DbStoreRepo,
    ):
        self.config = config
        self.data_source = data_source
        self.db = db_store
        self.processor = processor


    async def run(self):
        start_await_delay = int(self.config.get("APPLICATION_START_AWAIT_SECONDS"))
        data_update_delay = int(self.config.get("DATA_UPDATE_PERIOD_HOURS")) * 60 * 60 *24

        logging.info(f"Started application, starting delay {start_await_delay} secs...")
        # logging.info(f"Config: {self.config.to_dict()}")
        await asyncio.sleep(start_await_delay)

        while True:
            try:
                should_update_db = self.db.should_update_data()
                if not should_update_db:
                    logging.info(f"DB doesn't need to be updated")
                    await asyncio.sleep(data_update_delay)
                    continue

                should_load_from_source = self.data_source.should_get_data()
                if should_load_from_source:
                    logging.info(f"Started getting data from {self.data_source.name}")
                    await self.data_source.get_data_from_source()

                final_table = self.processor.process()

                exc = await self.db.batch_upload_data(final_table)
                if exc:
                    logging.error(f"There was an error while uploading data to DB; {exc}")
                else:
                    logging.info(f"Data uploaded successfully")

                await asyncio.sleep(data_update_delay)

            except asyncio.exceptions.CancelledError:
                logging.info("Gracefully stopping application...")
                await self.data_source.close()
                break