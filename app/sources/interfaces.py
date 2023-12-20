from typing import Protocol


class DataSource(Protocol):
    name: str

    def should_get_data(self) -> bool:
        """ Should get data from source """

    async def get_data_from_source(self):
        """ Get data from source """
    
    async def close(self):
        """ Close sessions and release resources """