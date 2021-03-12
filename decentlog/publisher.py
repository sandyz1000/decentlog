import sys
import logging
import threading
import dweepy
import random_name
import time
from datetime import datetime
from queue import Queue
from .polling import poll_dweet_things_from
from .streaming import listen_for_dweets_from
from concurrent.futures import ThreadPoolExecutor
_FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
# _FORMAT = '%(relativeCreated)6.1f %(threadName)12s: %(levelname).1s %(module)8.8s:%(lineno)-4d %(message)s'
BASE_URL = 'https://dweet.io'


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


class dweet_publisher:
    def __init__(self, logger=None, level=None, channel=None):
        self.logger = logger
        self.level = level
        self.channel = channel
        self._buffer = ''
        self._prevtime = datetime.now()
        self.executor = ThreadPoolExecutor(max_workers=4)
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


def listen_channel_output(channel, timeout=200000, longpoll=False, **kwargs):
    """ Listen to log publish in the dweet channel from remote server

    :raises Exception: [description]
    :return: [description]
    :rtype: [type]
    """
    func_dweet = poll_dweet_things_from if longpoll else listen_for_dweets_from
    try:
        for dweet in func_dweet(channel, timeout=timeout):
            msg, _, _ = dweet['content'].get('msg', None), dweet['created'], dweet['thing']
            yield msg
    except Exception as e:
        raise e


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

