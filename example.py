# %%
from decentlog import init_decentlog
init_decentlog("this_is_a_channel")

# %%
print("Testing logging from kaggle !!!")

# %%
# %%
from decentlog import DecentLogError as Logger


class MyClass(Logger):
    name = "log_name"

    def __init__(self, settings_file):
        super(MyClass, self).__init__(settings_file)


MC = MyClass("default_settings.py")
MC.set_logger()
MC.logger.debug("....")

# %%
import os
import errno
from decentlog.logger import LogFactory
from concurrent_log_handler import ConcurrentRotatingFileHandler
my_dir = "logs"
try:
    os.makedirs(my_dir)
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise
logger = LogFactory.get_instance(name="test_name")
logger.set_handler(
    ConcurrentRotatingFileHandler(
        os.path.join(my_dir, "test.log"),
        backupCount=5,
        maxBytes=10240))
logger.info("this is a log. ")

# %%
from decentlog import LogFactory, KafkaHandler
settings = {"KAFKA_HOSTS": "192.168.200.90:9092", "TOPIC": "jay-cluster-logs"}
logger = LogFactory.get_instance(name="test_name", json=True)
kafka_handler = KafkaHandler(settings)
logger.set_handler(kafka_handler)
logger.info("this is a log. ")

# %%
import sys
import logging
from decentlog import LogFactory
logger = LogFactory.get_instance(name="test_name")
logger.set_handler(logging.StreamHandler(sys.stdout))
logger.info("this is a log. ")

# %%
# my_dir = settings.get("LOG_DIR")
# try:
#     os.makedirs(my_dir)
# except OSError as exception:
#     if exception.errno != errno.EEXIST:
#         raise
# logger = CustomLogFactory.get_instance(name="test_name")
# logger.set_handler(
#     ConcurrentRotatingFileHandler(
#         os.path.join(my_dir, "test.log"),
#         backupCount=5,
#         maxBytes=10240))
# logger.info("this is a log. ")
#################################################
# logger = CustomLogFactory.get_instance(name="test_name", json=True)
# kafka_handler = KafkaHandler(settings)
# logger.set_handler(kafka_handler)
# logger.info("this is a log. ")
#################################################
# logger = CustomLogFactory.get_instance(name="test_name")
# logger.set_handler(logging.StreamHandler(sys.stdout))
# logger.info("this is a log. ")
#################################################
obj = Logger("defaut_settings.py")
obj.logger.info("this is a log. ")
