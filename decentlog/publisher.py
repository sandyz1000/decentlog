import sys
import logging
import threading
import dweepy
import typing
import random_name
from psutil import cpu_count
import time
from datetime import datetime
from queue import Queue
from functools import wraps
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
from kafka.errors import FailedPayloadsError
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from abc import ABCMeta, abstractmethod


class DweetPublisher(logging.Handler):
    """ Dweet Log Handler that will publish message to dweet channel
    """

    def __init__(self, settings: defaultdict):
        super().__init__()
        self.settings = settings
        if 'DWEET' not in settings:
            raise AttributeError("Key with named 'DWEET' not found")

        self.init_var()
        self.executor = ThreadPoolExecutor(max_workers=settings.get('max_workers'))
        print(f"Initialized publisher with channel: {self.channel}")

    def init_var(self):
        self.level = self.settings.get('log_level', 0)
        super().__init__(level=self.level)
        self.terminal = sys.stdout
        self.channel = self.settings.get('channel', None)
        if not self.channel:
            self.channel = random_name.generate_name()
        self._buffer = ''
        self._prevtime = datetime.now()

    def __submit_task(self, payload):
        self.terminal.write(self._buffer)
        future = self.executor.submit(self._write_to_dweet, payload, self.channel)
        future.result()

    def _write_to_dweet(self, row, channel, dweet_timeout=0.5):
        try:
            dweepy.dweet_for(channel, row)
            time.sleep(dweet_timeout)
        except dweepy.DweepyError:
            pass

    def emit(self, message: str, offset: float = 0.1):
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
            self.__submit_task(payload)
            self._buffer = ''
            self._prevtime = curtime

    def close(self):
        self.executor.shutdown()


class DweetQueuePublisher(DweetPublisher):
    def __init__(self, settings: defaultdict):
        self.settings = settings
        if 'DWEET' not in settings:
            raise AttributeError("Key with named 'DWEET' not found")
        super(DweetQueuePublisher, self).init_var()
        t = threading.Thread(target=self.send_queue, args=(self.channel), kwargs={}, daemon=True)
        t.start()
        self.qque = Queue(-1)

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

        self.__read_from_q = read_from_q
        print(f"Initialized publisher with channel: {self.channel}")
        self.__submit_task = (lambda payload: self.qque.put_nowait(payload))

    def send_queue(self, channel, block=True, timeout=None, dweet_timeout=0.5) -> bool:
        # TODO: Fix this code
        if not block:
            try:
                for row in self._queue_rows():
                    dweepy.dweet_for(channel, row)
                    time.sleep(dweet_timeout)
            except (dweepy.DweepyError, StopIteration):
                pass
        else:
            # Execute below code when block commit is True
            items, count = [], 0
            while 1:
                try:
                    row = next(self._queue_rows())
                    items.append(row)
                    count += 1
                except StopIteration:
                    break

            try:
                # TODO: Send dweet in batch
                time.sleep(dweet_timeout)
            except dweepy.DweepyError:
                pass
        return True

    def _queue_rows(self, block=False, timeout=None):
        """
        :param bool block:
        :param int timeout:
        """
        if self.qque.empty():
            raise StopIteration("Queue is empty")

        while not self.qque.empty():
            with self.__read_from_q(self.qque, block, timeout) as row:
                yield row

    def emit(self, message: str, offset: float):
        super().emit(message, offset=offset)
        self.__wfor_completion()

    def __wfor_completion(self):
        self.qque.join()

    def close(self):
        pass


class KafkaPublisher(logging.Handler):

    def __init__(self, settings: typing.Dict[str, typing.Any]):
        if 'KAFKA' not in settings:
            raise AttributeError("Key with named 'KAFKA' not found")
        self.level = self.settings.log_level
        super(KafkaPublisher, self).__init__(level=self.level)
        self.settings = settings
        kafka_cfg = settings['KAFKA']
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_cfg.get('BOOTSTRAP_SERVER', 'localhost:9092')
        )
        self.producer.send = self.__failedpayloads_wrapper(self.producer.send,
                                                           kafka_cfg["KAFKA_RETRY_TIME"])
        self.topic = kafka_cfg['TOPIC']

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

    def emit(self, record: typing.AnyStr):
        buf = self.formatter.format(record)
        if hasattr(buf, "encode"):
            buf = buf.encode(sys.getdefaultencoding())
        self.producer.send(self.topic, buf)

    def close(self):
        self.acquire()
        self.consumer.close()
        self.release()


class StreamPublisher(logging.Handler):
    """
    A publisher class which writes logging records, appropriately formatted,
    to a stream. Note that this class does not close the stream, as
    sys.stdout or sys.stderr may be used.
    """

    terminator = '\n'

    def __init__(self, dc_stdout=None, dc_stderr=None, log_level=None):
        """
        Initialize the handler with stdout and stderr.
        If stream is not specified, default sys.stderr and sys.stdout is used.
        """
        super(StreamPublisher, self).__init__(level=log_level)
        self.level = log_level if log_level else logging.DEBUG
        self.stderr = sys.stderr if not dc_stderr else dc_stderr
        self.stdout = sys.stdout if not dc_stdout else dc_stdout

    def get_stream(self, level):
        return self.stderr if level < max(self.min_level, logging.WARNING) else self.stdout
        
    def flush(self, stream):
        """ Flushes the stream.
        """
        self.acquire()
        try:
            if stream and hasattr(stream, "flush"):
                stream.flush()
        finally:
            self.release()

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """
        try:
            msg = self.format(record)
            stream = self.get_stream(record.level)
            # issue 35046: merged two stream.writes into one.
            stream.write(msg + self.terminator)
            self.flush(stream)
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)

    def setStream(self, _stdout=None, _stderr=None):
        """
        Sets the StreamHandler's stream to the specified value,
        if it is different.

        Returns the old stream, if the stream was changed, or None
        if it wasn't.
        """
        if _stderr is self.stderr and _stdout is self.stdout:
            return self.stdout, self.stderr
        
        prev_stdout, prev_stderr = self.stdout, self.stderr
        self.acquire()
        try:
            self.flush(self.stdout)
            self.flush(self.stderr)
            self.stdout, self.stderr = _stdout, _stderr
        finally:
            self.release()
        return prev_stdout, prev_stderr

    def __repr__(self):
        stream = self.get_stream(self.level)
        level = logging.getLevelName(self.level)
        name = getattr(stream, 'name', '')
        #  bpo-36015: name can be an int
        name = str(name)
        if name:
            name += ' '
        return '<%s %s(%s)>' % (self.__class__.__name__, name, level)

