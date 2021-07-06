from easydict import EasyDict

_c = EasyDict()

DEFAULT_TIMEOUT = 2000000


_c.log_level = 'DEBUG'
_c.log_dir = "logs"
_c.DWEET_HANDLER = 'decentlog.publisher.DweetPublisher'
_c.KAFKA_HANDLER = 'decentlog.publisher.KafkaPublisher'
_c.STREAM_HANDLER = 'logging.StreamHandler'

_c.stream_to_logger = True
_c.output_to_json = True

# General log configuration
_c.LOG_MAX_BYTES = 1024 * 1024 * 10
_c.LOG_BACKUPS = 5
_c.LOG_FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
# c.LOG_FORMAT = '%(relativeCreated)6.1f %(threadName)12s: %(levelname).1s %(module)8.8s:%(lineno)-4d %(message)s'

# Kafka Configuration
_c.KAFKA = EasyDict(
    BOOTSTRAP_SERVER="localhost:9094",
    TIMEOUT=DEFAULT_TIMEOUT,
    GROUP_ID=None,
    HOSTS=None,
    TOPIC="default-logs",
    KAFKA_RETRY_TIME=5,
)

# Dweet Configuration
_c.DWEET = EasyDict(
    BASE_URL="dweet.io",
    THINGS_NAME='',
    TIMEOUT=DEFAULT_TIMEOUT,
)


settings = _c
