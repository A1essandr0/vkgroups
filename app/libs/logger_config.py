
from libs.config_loader import Config

config = Config()

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "class": "logging.Formatter",
            "format": config.get("LOGGING_FORMAT"),
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "level": config.get("LOGGING_LEVEL"),
            "formatter": "standard",
        },
    },
    "loggers": {
        "root": {
            "handlers": ["default"],
            "level": config.get("LOGGING_LEVEL"),
        }
    },
}