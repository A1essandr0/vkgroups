from typing import Optional

import pandas as pd

from libs.singleton import MetaSingleton


class MockDbStore(metaclass=MetaSingleton):
    def should_update_data(self) -> bool:
        return True

    async def batch_upload_data(self, data: pd.DataFrame) -> Optional[Exception]:
        return None