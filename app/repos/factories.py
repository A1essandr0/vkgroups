
from models.exceptions import StoreNotImplemented
from libs.config_loader import Config
from repos.interfaces import DbStoreRepo
from repos.sqlite import SqliteDbStore
from repos.mock import MockDbStore


def get_db_store(config: Config, store_type="sqlite") -> DbStoreRepo:
    if store_type == "sqlite":
        return SqliteDbStore(config=config)

    if store_type == "mock":
        return MockDbStore()

    raise StoreNotImplemented(f"Not implemented store type: {store_type}")