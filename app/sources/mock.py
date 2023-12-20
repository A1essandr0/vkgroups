
from libs.singleton import MetaSingleton


class MockDataSource(metaclass=MetaSingleton):
    def __init__(self):
        self.name = "MOCK"


    def should_get_data(self) -> bool:
        return True

    async def get_data_from_source(self):
        pass

    async def close(self):
        pass