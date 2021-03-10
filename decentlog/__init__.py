import sys
import logging
import threading
import dweepy
import random_name
import time
from queue import Queue
import requests
from concurrent.futures import ThreadPoolExecutor
_FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
# _FORMAT = '%(relativeCreated)6.1f %(threadName)12s: %(levelname).1s %(module)8.8s:%(lineno)-4d %(message)s'


class DecentLogError(Exception):
    pass


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


class Publisher:
    def __init__(self, logger=None, level=None, channel=None):
        self.logger = logger
        self.level = level
        self.channel = channel
        self.dweet_timeout = 1
        self.executor = ThreadPoolExecutor(max_workers=4)
        print(f"Initialized publisher with channel: {self.channel}")

    def write(self, message):
        """ Write log message to dweet channel

        :param msg: [description]
        :type msg: [type]
        """
        def _write_to_dweet(row):
            try:
                dweepy.dweet_for(self.channel, row)
                time.sleep(self.dweet_timeout)
            except dweepy.DweepyError:
                pass

        if message != '\n':
            payload = {'msg': message}
            future = self.executor.submit(_write_to_dweet, payload)
            future.result()

    def flush(self):
        pass


class QueuePublisher:
    def __init__(self, channel=None):
        self.channel = channel
        self.dweet_timeout = 1
        threading.Thread(target=self.send_queue, args=(), kwargs={}, daemon=True).start()
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
        if message != '\n':
            payload = {'msg': message}
            self.qque.put_nowait(payload)
            
    def send_queue(self, block=False, timeout=None):
        for row in self._queue_rows():
            try:
                dweepy.dweet_for(self.channel, row)
                time.sleep(self.dweet_timeout)
            except dweepy.DweepyError:
                pass

    def flush(self):
        pass

    def wfor_completion(self):
        self.qque.join()


def listen_channel_output(channel, timeout=20000, **kwargs):
    """ Listen to log publish in the dweet channel from remote server

    :raises Exception: [description]
    :return: [description]
    :rtype: [type]
    """
    try:
        for dweet in dweepy.listen_for_dweets_from(channel, timeout=timeout):
            msg, created, _ = dweet['content'].get('msg', None), dweet['created'], dweet['thing']
            yield f"{created} :: {msg}"
    except requests.ConnectionError:
        print("Connection timeout, connect again")
    except Exception:
        pass


def init_decentlog(channel=None):
    """ Init decentralized logging """
    try:
        channel = random_name.generate_name() if channel is None else channel
        logging.basicConfig(level=logging.DEBUG, format=_FORMAT, filemode='a')
        logger = Publisher(logger=logging.getLogger('STDOUT'), level=logging.INFO, channel=channel)
        sys.stdout = logger
        sys.stderr = logger
        return logger

    except Exception as e:
        raise e

