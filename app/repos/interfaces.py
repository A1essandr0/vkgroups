from typing import Protocol, Optional

import pandas as pd


class DbStoreRepo(Protocol):
    """ Relational DB repository """

    def should_update_data(self) -> bool:
        """ Should db data be updated"""

    async def batch_upload_data(self, data: pd.DataFrame) -> Optional[Exception]:
        """ Upload multiple data records"""

