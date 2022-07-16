import logging


LOGGER_FORMAT = "%(asctime)s %(message)s"
logging.basicConfig(format=LOGGER_FORMAT, datefmt="[%H:%M:%S]")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
