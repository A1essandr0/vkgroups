
from models.exceptions import DataSourceNotImplemented
from libs.config_loader import Config
from sources.interfaces import DataSource
from sources.vk import VkDataSource
from sources.mock import MockDataSource


def get_data_source(config: Config, source_type="vk") -> DataSource:
    if source_type == "vk":
        return VkDataSource(config=config)
    
    if source_type == "mock":
        return MockDataSource()

    raise DataSourceNotImplemented(f"Not implemented data source: {source_type}")