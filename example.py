# %%
from decentlog import init_decentlog
init_decentlog("this_is_a_channel")

# %%
print("Testing logging from kaggle !!!")

# %%
import os
import errno
from decentlog.dclogger import LogObject
from concurrent_log_handler import ConcurrentRotatingFileHandler
my_dir = "logs"
try:
    os.makedirs(my_dir, exist_ok=True)
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise
logger = LogObject(name="test_name")
logger.set_handler(
    ConcurrentRotatingFileHandler(
        os.path.join(my_dir, "test.log"),
        backupCount=5,
        maxBytes=10240))
logger.info("this is a log. ")

# %%
from decentlog import KafkaHandler
settings = {"KAFKA_HOSTS": "192.168.200.90:9092", "TOPIC": "jay-cluster-logs"}
logger = LogObject(name="test_name", json=True)
kafka_handler = KafkaHandler(settings)
logger.set_handler(kafka_handler)
logger.info("this is a log. ")

# %%
import sys
import logging
from decentlog import StreamPublisher

logger = LogObject(name="test_name")
logger.set_handler(StreamPublisher(log_level=logging.DEBUG))
logger.info("this is a log. ")

# %%
