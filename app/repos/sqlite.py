from os import path
from typing import Optional
from sqlite3 import connect

import pandas as pd

from libs.config_loader import Config
from libs.singleton import MetaSingleton


class SqliteDbStore(metaclass=MetaSingleton):
    def __init__(self, config: Config):
        self.config = config
        self.db_file_had_to_be_created = not path.exists(self.config.get("SQLITE_PATH"))
        self.connection = connect(self.config.get("SQLITE_PATH"))

        create_fields_query = ', '.join(
            [field_name + ' TEXT' for field_name in config.get("USER_DATA_FIELDS")]
        ) + ', group_subscribed TEXT'
        if ";" in create_fields_query:
            raise Exception("Error during DB init: possible SQL injection")
        
        self.connection.execute("CREATE TABLE if not exists Subscribers ({0})".format(create_fields_query))


    def should_update_data(self) -> bool:
        if self.db_file_had_to_be_created:
            self.db_file_had_to_be_created = False
            return True

        db_file_path = self.config.get("SQLITE_PATH")
        # if date.fromtimestamp(path.getctime(db_file_path)) == date.today():
        #     return False

        return True
        

    async def batch_upload_data(self, data: pd.DataFrame) -> Optional[Exception]:
        try:
            data.to_sql("Subscribers", self.connection, index=False, if_exists='replace')
        except Exception as exc:
            return exc

        return None