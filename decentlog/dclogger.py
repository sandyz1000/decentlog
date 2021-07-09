import copy
import datetime
import errno
import io
import logging
import os
import sys
import typing
from functools import wraps
from abc import ABCMeta, abstractmethod
from portalocker import lock, unlock, LOCK_EX
from concurrent_log_handler import ConcurrentRotatingFileHandler, NullLogRecord
from pythonjsonlogger import jsonlogger


class FixedConcurrentRotatingFileHandler(ConcurrentRotatingFileHandler):

    def acquire(self):
        """ Acquire thread and file locks.  Re-opening log for 'degraded' mode.
        """
        # handle thread lock
        logging.Handler.acquire(self)
        # Issue a file lock.  (This is inefficient for multiple active threads
        # within a single process. But if you're worried about high-performance,
        # you probably aren't using this log handler.)
        if self.stream_lock:
            # If stream_lock=None, then assume close() was called or something
            # else weird and ignore all file-level locks.
            if self.stream_lock.closed:
                # Daemonization can close all open file descriptors, see
                # https://bugzilla.redhat.com/show_bug.cgi?id=952929
                # Try opening the lock file again.  Should we warn() here?!?
                try:
                    self._open_lockfile()
                except Exception:
                    self.handleError(NullLogRecord())
                    # Don't try to open the stream lock again
                    self.stream_lock = None
                    return
            unlock(self.stream_lock)
            lock(self.stream_lock, LOCK_EX)


class singleton(type):
    """Singleton metaclass to be inherited by child logger
    """
    _instance = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super(singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance[cls]


class LogObject(metaclass=singleton):
    """ This class will the add functionality to log.Handler instance"""

    def __init__(self, json=False, name='scrapy-cluster', level='INFO',
                 file_format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                 propagate=False):
        self.logger = logging.getLogger(name)
        root = logging.getLogger()
        for log in root.manager.loggerDict.keys():
            root.getChild(log).setLevel(getattr(logging, level, 10))
        self.logger.propagate = propagate
        self.json = json
        self.name = name
        self.format_string = file_format
        self.getRootLogger = (lambda self: self.root)
        
    def __wrapper__(self, func: typing.Callable, item):
        @wraps(func)
        def _wraps(*args, **kwargs):
            if len(args) > 2:
                extras = args[1]
            else:
                extras = kwargs.pop("extras", {})
            extras = self.add_extras(extras, item)
            return func(args[0], extra=extras)
        return _wraps

    def set_handler(self, handler):
        handler.setLevel(logging.DEBUG)
        formatter = self._get_formatter(self.json)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.debug("Logging to %s" % handler.__class__.__name__)

    def __getattr__(self, item):
        if item.upper() in logging._nameToLevel:
            func = getattr(self.logger, item)
            return self.__wrapper__(func, item)
        raise AttributeError

    def _get_formatter(self, json):
        if json:
            return jsonlogger.JsonFormatter()
        else:
            return logging.Formatter(self.format_string)

    def add_extras(self, dict, level):
        my_copy = copy.deepcopy(dict)
        if 'level' not in my_copy:
            my_copy['level'] = level
        if 'timestamp' not in my_copy:
            my_copy['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if 'logger' not in my_copy:
            my_copy['logger'] = self.name
        return my_copy


class BaseDecentLogger(ABCMeta):
    """ Decentralized logger base class that return the instance of logger, however it cannot be
    instatiated. Implementment child class to complete the functionality """
    name = "root"

    def __init__(self, settings: typing.Union[typing.Dict] = None):
        self.settings = settings
        self.my_dir = self.settings.get('log_dir', 'logs')
        os.makedirs(self.my_dir, exist_ok=True)

    @abstractmethod
    def resolve_handler(self) -> None:
        """" To be implemented by child class """

    def relay_stdoutput(self):
        # - Initialize decent logger for relay log to stdout and stderr
        # i.e. print statement to log
        sys.stdout = StreamToLogger(logger=self.logger, log_level=logging.INFO)
        sys.stderr = StreamToLogger(logger=self.logger, log_level=logging.ERROR)

    def set_logger(self, logger=None) -> LogObject:
        """Initialize to set logger to global scope

        :param logger: [description], defaults to None
        :type logger: [type], optional
        :return: [description]
        :rtype: [type]
        """
        if logger:
            self.logger = logger
            return self.logger
        my_level = self.settings['LOG_FORMATTER'].get('LOG_LEVEL', 'INFO')
        my_name = self.name
        my_json = self.settings['LOG_FORMATTER'].get('LOG_JSON', True)
        if self.settings['LOG_FORMATTER'].get('relay_stdout', False):
            self.relay_stdoutput()
        self.logger = LogObject(json=my_json, name=my_name, level=my_level)
        self.logger.set_handler(self.resolve_handler())
        return self.logger


class StreamToLogger(io.TextIOWrapper):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

    def __init__(
        self,
        logger,
        log_level=logging.INFO,
        buffer='',
        encoding=None,
        errors=None,
        newline='\n',
        line_buffering=False,
        write_through=False,
    ):
        super(StreamToLogger, self).__init__(
            buffer, encoding=encoding, errors=errors,
            newline=newline, line_buffering=line_buffering, write_through=write_through
        )
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf: typing.AnyStr):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def writelines(self, lines: typing.Iterable[str]):
        for line in lines:
            self.logger.log(self.log_level, line.rstrip())
