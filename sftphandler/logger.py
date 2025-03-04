import logging
import logging.config
import functools


config_initial = {
    "version": 1,
    "formatters": {
        "detailed": {
            "class": "logging.Formatter",
            "format": "%(asctime)s %(levelname)-8s %(processName)-10s %(message)s",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": logging.DEBUG,
            "formatter": "detailed",
        },
    },
    "root": {"level": logging.DEBUG, "handlers": ["console"]}
}

logging.config.dictConfig(config_initial)


def loggerFactory(name, level=logging.DEBUG):
    """Get a logger foa n module
    loggerFactory(__name__)
    """
    logger = logging.getLogger(name)
    import coloredlogs

    coloredlogs.install(level=level, logger=logger)
    logger.setLevel(level)
    return logger