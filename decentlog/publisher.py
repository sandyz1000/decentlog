import sys
import logging
import threading
import dweepy
import random_name
from psutil import cpu_count
import time
from datetime import datetime
from queue import Queue
from functools import wraps
from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
from kafka.errors import FailedPayloadsError
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
_FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
# _FORMAT = '%(relativeCreated)6.1f %(threadName)12s: %(levelname).1s %(module)8.8s:%(lineno)-4d %(message)s'
BASE_URL = 'https://dweet.io'


class read_from_q:
    def __init__(self, q, block=False, timeout=None):
        """
         :param Queue.Queue q:
         :param bool block:
         :param timeout:
        """
        self.q = q
        self.block = block
        self.timeout = timeout

    def __enter__(self):
        return self.q.get(self.block, self.timeout)

    def __exit__(self, _type, _value, _traceback):
        self.q.task_done()


class dweet_publisher:
    def __init__(self, logger: logging.Logger = None, level: int = None,
                 channel: str = None, max_workers: int = cpu_count(logical=False)):
        self.logger = logger
        self.level = level
        self.terminal = sys.stdout
        self.channel = channel
        self._buffer = ''
        self._prevtime = datetime.now()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        print(f"Initialized publisher with channel: {self.channel}")

    def write(self, message, offset=0.1):
        """ Write log message to dweet channel

        :param msg: [description]
        :type msg: [type]
        """
        if message == '\n':
            return

        curtime = datetime.now()
        self._buffer += message + "\n"
        if (curtime - self._prevtime).seconds > offset:
            payload = {'msg': self._buffer}
            self.terminal.write(self._buffer)
            future = self.executor.submit(self._write_to_dweet, payload, self.channel)
            future.result()
            self._buffer = ''
            self._prevtime = curtime

    @staticmethod
    def _write_to_dweet(row, channel, dweet_timeout=0.5):
        try:
            dweepy.dweet_for(channel, row)
            time.sleep(dweet_timeout)
        except dweepy.DweepyError:
            pass

    def flush(self):
        pass


class dweet_queue_publisher:
    def __init__(self, channel=None):
        self.channel = channel
        self._buffer = ''
        self._prevtime = datetime.now()
        threading.Thread(target=self.send_queue, args=(self.channel), kwargs={}, daemon=True).start()
        self.qque = Queue(-1)
        print(f"Initialized publisher with channel: {self.channel}")

    def _queue_rows(self, block=False, timeout=None):
        """
        :param Queue.Queue q:
        :param bool block:
        :param int timeout:
        """
        while not self.qque.empty():
            with read_from_q(self.qque, block, timeout) as row:
                yield row

    def write(self, message):
        if message == '\n':
            return

        curtime = datetime.now()
        self._buffer += message
        if (curtime - self._prevtime).seconds > 1:
            payload = {'msg': self._buffer}
            self.qque.put_nowait(payload)
            self._buffer = ''
            self._prevtime = curtime
        self._wfor_completion()

    def send_queue(self, channel, block=False, timeout=None, dweet_timeout=0.5):
        for row in self._queue_rows():
            try:
                dweepy.dweet_for(channel, row)
                time.sleep(dweet_timeout)
            except dweepy.DweepyError:
                pass

    def flush(self):
        pass

    def _wfor_completion(self):
        self.qque.join()


def init_decentlog(channel=None):
    """ Init decentralized logging """
    try:
        channel = random_name.generate_name() if channel is None else channel
        logging.basicConfig(level=logging.DEBUG, format=_FORMAT, filemode='a')
        logger = dweet_publisher(logger=logging.getLogger('STDOUT'), level=logging.INFO, channel=channel)
        sys.stdout = logger
        sys.stderr = logger
        return logger

    except Exception as e:
        raise e


class DweetHandler(logging.Handler):
    def __init__(self, settings: defaultdict):
        pass

    def emit(self, record):
        pass
    
    def close(self):
        pass


class KafkaHandler(logging.Handler):

    def __init__(self, settings: defaultdict):
        self.settings = settings
        self.consumer = KafkaConsumer(
            settings.get("KAFKA_HOSTS"),
            group_id=settings.get('GROUP_ID', None),
            bootstrap_servers=settings.get('BOOTSTRAP_SERVER', 'localhost:9092')
        )
        self.producer = KafkaProducer(
            bootstrap_servers=settings.get('BOOTSTRAP_SERVER', 'localhost:9092')
        )
        self.producer.send = self.__failedpayloads_wrapper(
            self.producer.send,
            settings.get("KAFKA_RETRY_TIME", 5)
        )
        super(KafkaHandler, self).__init__()

    def __failedpayloads_wrapper(self, func: typing.Callable, max_iter_times: int,
                                 _raise: bool = False):
        @wraps(func)
        def _wrapper(*args):
            count = 0
            while count <= max_iter_times:
                try:
                    func(*args)
                    break
                except Exception as e:
                    if _raise and not isinstance(e, FailedPayloadsError):
                        raise e
                    count += 1
                    if count > max_iter_times and _raise:
                        raise
                    time.sleep(0.1)

        return _wrapper

    def emit(self, record):
        # TODO: Check if kafka topic exists
        buf = self.formatter.format(record)
        if hasattr(buf, "encode"):
            buf = buf.encode(sys.getdefaultencoding())
        self.producer.send(self.settings.get("TOPIC"), buf)

    def close(self):
        self.acquire()
        super(KafkaHandler, self).close()
        self.consumer.close()
        self.release()
