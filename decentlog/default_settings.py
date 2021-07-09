from easydict import EasyDict

DEFAULT_TIMEOUT = 2000000


LOG_LEVEL = 'DEBUG'
LOG_DIR = "logs"
LOG_HANDLER = EasyDict(
    to_dweet=True, to_kafka=True,
    to_stream=True, to_file=True,
)

# General log configuration
LOG_FORMATTER = EasyDict(
    to_json=True,
    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
    # format='%(relativeCreated)6.1f %(threadName)12s: %(levelname).1s %(module)8.8s:%(lineno)-4d %(message)s',
    log_max_bytes=1024 * 1024 * 10,
    log_backups=5,
    relay_stdout=True,
)


# Kafka Configuration
KAFKA = EasyDict(
    BOOTSTRAP_SERVER="localhost:9094",
    TIMEOUT=DEFAULT_TIMEOUT,
    GROUP_ID=None,
    HOSTS=None,
    TOPIC="default-logs",
    KAFKA_RETRY_TIME=5,
)

# Dweet Configuration
DWEET = EasyDict(
    BASE_URL="dweet.io",
    THINGS_NAME='',
    TIMEOUT=DEFAULT_TIMEOUT,
)
