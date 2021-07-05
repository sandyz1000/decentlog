from easydict import EasyDict

_c = EasyDict()

_c.LOG_LEVEL = 'DEBUG'
_c.LOG_STDOUT = True
_c.LOG_JSON = False
_c.LOG_DIR = "logs"
_c.LOG_MAX_BYTES = 1024 * 1024 * 10
_c.LOG_BACKUPS = 5
_c.TO_KAFKA = False
_c.KAFKA_HOSTS = "192.168.200.90:9092"
_c.TOPIC = "jay-cluster-logs"

_c._FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
# _FORMAT = '%(relativeCreated)6.1f %(threadName)12s: %(levelname).1s %(module)8.8s:%(lineno)-4d %(message)s'


default_settings = _c

